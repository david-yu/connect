// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// StreamConfig holds configuration for the CDC streaming phase.
type StreamConfig struct {
	Schema          string
	Tables          []string
	AsnCDCSchema    string // CDC control schema, defaults to "ASNCDC"
	BackoffInterval time.Duration
	PollBatchSize   int
	StartingCSN     CSN
	// CommitSeqByteLen is the byte length of IBMSNAP_COMMITSEQ in the CD tables.
	// DB2 ≤ 11.x uses CHAR(10) FOR BIT DATA (10 bytes); DB2 12.1+ uses CHAR(16).
	// Zero means auto-detect during Initialize (recommended).
	CommitSeqByteLen int
	// afterIntentSeq is the last IBMSNAP_INTENTSEQ seen at StartingCSN.
	// Used for composite (CSN, IntentSeq) pagination to resume mid-CSN.
	afterIntentSeq int64
}

// asncdcSchema returns the CDC control schema, defaulting to "ASNCDC".
func (c *StreamConfig) asncdcSchema() string {
	if c.AsnCDCSchema != "" {
		return c.AsnCDCSchema
	}
	return "ASNCDC"
}

// Streamer polls DB2 change tables and emits ChangeEvents in CSN order.
//
// On each poll iteration the Streamer:
//  1. Queries MAX(SYNCHPOINT) from ASNCDC.IBMSNAP_REGISTER as a safe upper
//     bound (avoids reading rows inserted by in-progress transactions).
//  2. Issues one SELECT per change table for rows in the window
//     (currentCSN, upperCSN].
//  3. Merges and sorts all events by (CSN, IBMSNAP_INTENTSEQ) before delivery.
//  4. Advances its watermark to the highest CSN seen.
//
// When no new rows are found the loop backs off for StreamConfig.BackoffInterval.
type Streamer struct {
	db           *sql.DB
	config       StreamConfig
	version      Version
	changeTables map[string]string // monitored table name -> change table qualified name
}

// NewStreamer creates a new Streamer.
func NewStreamer(db *sql.DB, config StreamConfig, version Version) *Streamer {
	return &Streamer{
		db:           db,
		config:       config,
		version:      version,
		changeTables: make(map[string]string),
	}
}

// Initialize discovers the change tables for all monitored tables from IBMSNAP_REGISTER.
func (s *Streamer) Initialize(ctx context.Context) error {
	cdcSchema := s.config.asncdcSchema()

	// Schema is validated as uppercase alphanumeric + underscore at config-parse time
	// (isValidDB2Identifier in input_db2_cdc.go), so direct embedding is safe.
	query := fmt.Sprintf(`
		SELECT SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE
		FROM %s.IBMSNAP_REGISTER
		WHERE SOURCE_OWNER = '%s'
	`, cdcSchema, s.config.Schema)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying CDC registration: %w", err)
	}
	defer rows.Close()

	registered := make(map[string]bool)

	for rows.Next() {
		var sourceOwner, sourceTable, cdOwner, cdTable string
		if err := rows.Scan(&sourceOwner, &sourceTable, &cdOwner, &cdTable); err != nil {
			return fmt.Errorf("scanning registration row: %w", err)
		}

		sourceTable = strings.TrimSpace(sourceTable)
		cdTable = strings.TrimSpace(cdTable)

		for _, monitoredTable := range s.config.Tables {
			if strings.EqualFold(sourceTable, monitoredTable) {
				changeTableName := fmt.Sprintf("%s.%s", strings.TrimSpace(cdOwner), cdTable)
				s.changeTables[monitoredTable] = changeTableName
				registered[monitoredTable] = true
				break
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating registration rows: %w", err)
	}

	for _, table := range s.config.Tables {
		if !registered[table] {
			return fmt.Errorf("table %s.%s is not registered for CDC (run ASNCDC.ADDTABLE)", s.config.Schema, table)
		}
	}

	// Auto-detect IBMSNAP_COMMITSEQ byte length from the registered CD tables.
	if s.config.CommitSeqByteLen <= 0 {
		if byteLen, err := s.detectCommitSeqByteLen(ctx); err == nil {
			s.config.CommitSeqByteLen = byteLen
		} else {
			s.config.CommitSeqByteLen = 10 // safe default for older DB2
		}
	}

	return nil
}

// detectCommitSeqByteLen queries SYSCAT.COLUMNS to determine the actual byte
// length of IBMSNAP_COMMITSEQ in the registered CD tables.
// DB2 ≤ 11.x uses CHAR(10) FOR BIT DATA; DB2 12.1+ uses CHAR(16) FOR BIT DATA.
func (s *Streamer) detectCommitSeqByteLen(ctx context.Context) (int, error) {
	for _, cdTableFull := range s.changeTables {
		parts := strings.SplitN(cdTableFull, ".", 2)
		if len(parts) != 2 {
			continue
		}
		var length int
		err := s.db.QueryRowContext(ctx,
			"SELECT LENGTH FROM SYSCAT.COLUMNS WHERE TABSCHEMA=? AND TABNAME=? AND COLNAME='IBMSNAP_COMMITSEQ'",
			strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]),
		).Scan(&length)
		if err == nil && length > 0 {
			return length, nil
		}
	}
	return 0, errors.New("could not detect IBMSNAP_COMMITSEQ byte length from registered CD tables")
}

// Stream continuously polls change tables and sends events to handler until ctx is cancelled.
func (s *Streamer) Stream(ctx context.Context, handler func(event ChangeEvent) error) error {
	currentCSN := s.config.StartingCSN
	afterIntentSeq := s.config.afterIntentSeq

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		events, maxCSN, maxIntent, err := s.pollChanges(ctx, currentCSN, afterIntentSeq)
		if err != nil {
			return fmt.Errorf("polling changes: %w", err)
		}

		for _, event := range events {
			if err := handler(event); err != nil {
				return fmt.Errorf("handler error: %w", err)
			}
		}

		if len(events) > 0 {
			currentCSN = maxCSN
			afterIntentSeq = maxIntent
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.config.BackoffInterval):
			}
		}
	}
}

// tableResult holds one table's poll output.
type tableResult struct {
	events []ChangeEvent
	full   bool // true when len(events) == PollBatchSize
}

// computeSafeCSN returns the highest CSN that is safe to advance to across all
// tables in a single poll round.
//
// For each full table (exactly PollBatchSize rows), the batch may have been
// truncated mid-transaction. The safe per-table ceiling is the penultimate
// distinct CSN within that table's own batch; events at the max CSN must be
// re-fetched. Edge case: all rows in the batch share one CSN — advance to
// tableMax anyway (at-least-once; caller deduplicates on csn+intentSeq).
//
// Non-full tables impose no constraint: all rows up to upperCSN were returned.
//
// The global safe ceiling is the minimum of all per-table ceilings, starting
// from upperCSN (no full table → no narrowing).
func computeSafeCSN(results []tableResult, upperCSN CSN, batchSize int) CSN {
	safeCSN := upperCSN
	for _, r := range results {
		if len(r.events) != batchSize {
			continue // not full — no constraint from this table
		}
		tableMax := r.events[len(r.events)-1].CSN
		// Default: edge case where all rows share one CSN — advance to tableMax.
		tableSafe := tableMax
		for i := len(r.events) - 2; i >= 0; i-- {
			if !r.events[i].CSN.Equal(tableMax) {
				tableSafe = r.events[i].CSN // found penultimate distinct CSN
				break
			}
		}
		if tableSafe.Less(safeCSN) {
			safeCSN = tableSafe
		}
	}
	return safeCSN
}

// computeReturnCSN caps safeCSN at the actual max event CSN so that sparse
// event windows do not advance the watermark past the last observed event.
// When no table is full, safeCSN equals upperCSN which may be much higher than
// any event in the batch; returning upperCSN would silently skip future events
// that land before the next poll window opens.
func computeReturnCSN(safeCSN CSN, sorted []ChangeEvent) CSN {
	if len(sorted) == 0 {
		return safeCSN
	}
	maxEventCSN := sorted[len(sorted)-1].CSN
	if maxEventCSN.Less(safeCSN) {
		return maxEventCSN
	}
	return safeCSN
}

// pollChanges polls all change tables and merges events ordered by CSN.
//
// Watermark safety: IBMSNAP_COMMITSEQ identifies a DB2 transaction; all rows
// from that transaction share the same CSN.  If any change table returned
// exactly PollBatchSize rows the batch may have been truncated in the middle
// of a transaction, so we must NOT advance currentCSN past that last CSN — the
// remaining rows would be permanently skipped by the `> afterCSN` predicate.
//
// We compute the safe ceiling per-table (not globally) and take the minimum.
// Using the global penultimate would cause events from a full table at a lower
// CSN to be permanently skipped when another table has events at a higher CSN.
//
// afterIntentSeq enables composite (CSN, IntentSeq) pagination so that a
// PollBatchSize cutoff mid-transaction does not lose the remaining rows at
// the same CSN on the next poll.
func (s *Streamer) pollChanges(ctx context.Context, afterCSN CSN, afterIntentSeq int64) ([]ChangeEvent, CSN, int64, error) {
	upperCSN, err := s.getUpperBound(ctx)
	if err != nil {
		return nil, CSN{}, 0, fmt.Errorf("getting upper bound: %w", err)
	}
	// Skip the poll when the capture daemon hasn't advanced past our watermark,
	// but only when afterIntentSeq is zero (no pending rows at the current CSN).
	// If afterIntentSeq > 0 we are mid-transaction and must continue polling to
	// drain the remaining rows at exactly afterCSN even though upperCSN == afterCSN.
	if !upperCSN.Greater(afterCSN) && afterIntentSeq == 0 {
		return nil, afterCSN, afterIntentSeq, nil
	}

	results := make([]tableResult, 0, len(s.changeTables))
	allEvents := make([]ChangeEvent, 0, len(s.changeTables)*s.config.PollBatchSize)

	for tableName, changeTableName := range s.changeTables {
		events, err := s.pollChangeTable(ctx, tableName, changeTableName, afterCSN, afterIntentSeq, upperCSN)
		if err != nil {
			return nil, CSN{}, 0, fmt.Errorf("polling change table %s: %w", changeTableName, err)
		}
		results = append(results, tableResult{events: events, full: len(events) == s.config.PollBatchSize})
		allEvents = append(allEvents, events...)
	}

	sorted := s.sortEventsByCSN(allEvents)
	if len(sorted) == 0 {
		return nil, afterCSN, afterIntentSeq, nil
	}

	safeCSN := computeSafeCSN(results, upperCSN, s.config.PollBatchSize)

	// Trim events above the safe ceiling when any table was full and narrowed it.
	if sorted[len(sorted)-1].CSN.Greater(safeCSN) {
		trimIdx := len(sorted)
		for i := len(sorted) - 1; i >= 0; i-- {
			if !sorted[i].CSN.Greater(safeCSN) {
				trimIdx = i + 1
				break
			}
		}
		sorted = sorted[:trimIdx]
	}

	if len(sorted) == 0 {
		return nil, afterCSN, afterIntentSeq, nil
	}

	returnCSN := computeReturnCSN(safeCSN, sorted)
	var returnIntentSeq int64
	// Track the max intentSeq at the return CSN for composite pagination.
	for i := len(sorted) - 1; i >= 0; i-- {
		if sorted[i].CSN.Equal(returnCSN) {
			if sorted[i].IntentSeq > returnIntentSeq {
				returnIntentSeq = sorted[i].IntentSeq
			}
		} else {
			break
		}
	}
	return sorted, returnCSN, returnIntentSeq, nil
}

// getUpperBound returns the highest log position the DB2 capture daemon has
// durably written, using MAX over both CD_NEW_SYNCHPOINT (set at ADDTABLE time,
// always non-null) and SYNCHPOINT (updated after each captured transaction).
// This prevents reading change rows that belong to an in-progress transaction.
func (s *Streamer) getUpperBound(ctx context.Context) (CSN, error) {
	cdcSchema := s.config.asncdcSchema()
	// SYNCHPOINT is NULL immediately after ASNCDC.ADDTABLE; CD_NEW_SYNCHPOINT is
	// always set at registration time. Union both so we get a non-null upper bound
	// even before the capture daemon processes its first change.
	query := fmt.Sprintf(`
		SELECT MAX(t.SYNCHPOINT) FROM (
			SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT
			  FROM %s.IBMSNAP_REGISTER
			 WHERE SOURCE_OWNER = '%s'
			UNION ALL
			SELECT SYNCHPOINT
			  FROM %s.IBMSNAP_REGISTER
			 WHERE SOURCE_OWNER = '%s'
		) t`,
		cdcSchema, s.config.Schema,
		cdcSchema, s.config.Schema,
	)

	var synchpointBytes []byte
	err := s.db.QueryRowContext(ctx, query).Scan(&synchpointBytes)
	if err != nil {
		return CSN{}, fmt.Errorf("getting SYNCHPOINT: %w", err)
	}

	if len(synchpointBytes) == 0 {
		return NullCSN(), nil
	}

	return NewCSNFromDBValue(synchpointBytes), nil
}

// buildPollQuery returns the SQL used to fetch a batch of change rows.
//
// IBMSNAP_COMMITSEQ is typed CHAR(n) FOR BIT DATA — a raw binary column.
// Reliable parameter binding for binary columns via SQL_C_BINARY would require
// exact n-byte buffers and careful length handling. To avoid this complexity,
// CSN bounds are embedded directly as hex literals sized to match the column
// (X'<2n chars>'). This is safe because CSN values come from our own state,
// never from user input.
//
// When afterIntentSeq > 0 the query uses composite (CSN, IntentSeq) pagination
// so that a PollBatchSize cutoff mid-CSN resumes from where it left off rather
// than re-reading or skipping the tail of the same transaction.
func (s *Streamer) buildPollQuery(changeTableName string, afterCSN CSN, afterIntentSeq int64, upperCSN CSN) string {
	byteLen := s.config.CommitSeqByteLen
	if byteLen <= 0 {
		byteLen = 10
	}
	afterHex := afterCSN.SQLHex(byteLen)
	upperHex := upperCSN.SQLHex(byteLen)

	var afterPredicate string
	if afterIntentSeq > 0 {
		// Composite pagination: resume within the same CSN using IntentSeq.
		afterPredicate = fmt.Sprintf(
			"(IBMSNAP_COMMITSEQ > X'%s' OR (IBMSNAP_COMMITSEQ = X'%s' AND IBMSNAP_INTENTSEQ > %d))",
			afterHex, afterHex, afterIntentSeq,
		)
	} else {
		afterPredicate = fmt.Sprintf("IBMSNAP_COMMITSEQ > X'%s'", afterHex)
	}

	return fmt.Sprintf(`
		SELECT *
		FROM %s
		WHERE IBMSNAP_OPERATION IN ('I', 'D')
		  AND %s
		  AND IBMSNAP_COMMITSEQ <= X'%s'
		ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ
		FETCH FIRST %d ROWS ONLY
	`, changeTableName, afterPredicate, upperHex, s.config.PollBatchSize)
}

// pollChangeTable queries a single change table for events in the CSN window (afterCSN, upperCSN].
func (s *Streamer) pollChangeTable(ctx context.Context, tableName, changeTableName string, afterCSN CSN, afterIntentSeq int64, upperCSN CSN) ([]ChangeEvent, error) {
	query := s.buildPollQuery(changeTableName, afterCSN, afterIntentSeq, upperCSN)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying change table %s: %w", changeTableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns for %s: %w", changeTableName, err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("getting column types for %s: %w", changeTableName, err)
	}

	// Locate metadata columns by name; collect data column indices.
	opIdx, csnIdx, intentSeqIdx, tsIdx := -1, -1, -1, -1
	dataColIdxs := make([]int, 0)
	dataColNames := make([]string, 0)

	for i, col := range columns {
		switch col {
		case "IBMSNAP_OPERATION":
			opIdx = i
		case "IBMSNAP_COMMITSEQ":
			csnIdx = i
		case "IBMSNAP_INTENTSEQ":
			intentSeqIdx = i
		case "IBMSNAP_LOGMARKER":
			tsIdx = i
		default:
			if !strings.HasPrefix(col, "IBMSNAP_") {
				dataColIdxs = append(dataColIdxs, i)
				dataColNames = append(dataColNames, col)
			}
		}
	}

	if opIdx < 0 || csnIdx < 0 || intentSeqIdx < 0 {
		return nil, fmt.Errorf("change table %s is missing required IBMSNAP_ columns (found: %v)", changeTableName, columns)
	}

	var events []ChangeEvent

	for rows.Next() {
		scanDest := make([]any, len(columns))
		for i := range scanDest {
			scanDest[i] = new(any)
		}

		if err := rows.Scan(scanDest...); err != nil {
			return nil, fmt.Errorf("scanning row from %s: %w", changeTableName, err)
		}

		operation := getString(scanDest[opIdx])
		csnBytes := getBytes(scanDest[csnIdx])
		intentSeq := getInt64(scanDest[intentSeqIdx])

		var timestamp time.Time
		if tsIdx >= 0 {
			timestamp = getTime(scanDest[tsIdx])
		}

		csn := NewCSNFromDBValue(csnBytes)

		opType, err := FromDB2Op(operation)
		if err != nil {
			// Skip unknown operation types rather than failing the whole batch.
			continue
		}

		data := make(map[string]any, len(dataColNames))
		for i, idx := range dataColIdxs {
			value := *(scanDest[idx].(*any))
			data[dataColNames[i]] = convertDB2Value(value, columnTypes[idx])
		}

		events = append(events, ChangeEvent{
			Schema:    s.config.Schema,
			Table:     tableName,
			Operation: opType,
			CSN:       csn,
			IntentSeq: intentSeq,
			Timestamp: timestamp,
			Data:      data,
		})
	}

	return events, rows.Err()
}

// sortEventsByCSN sorts events using a min-heap ordered by (CSN, IntentSeq).
func (*Streamer) sortEventsByCSN(events []ChangeEvent) []ChangeEvent {
	if len(events) == 0 {
		return events
	}

	h := &eventHeap{}
	heap.Init(h)
	for _, event := range events {
		heap.Push(h, event)
	}

	sorted := make([]ChangeEvent, 0, len(events))
	for h.Len() > 0 {
		sorted = append(sorted, heap.Pop(h).(ChangeEvent))
	}

	return sorted
}

// eventHeap is a min-heap of ChangeEvents ordered by (CSN, IntentSeq).
type eventHeap []ChangeEvent

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	cmp := h[i].CSN.Compare(h[j].CSN)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].IntentSeq < h[j].IntentSeq
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) { *h = append(*h, x.(ChangeEvent)) }

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// getString extracts a string value from a scanned any pointer.
func getString(dest any) string {
	value := *(dest.(*any))
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// getBytes extracts raw bytes from a scanned any pointer.
func getBytes(dest any) []byte {
	value := *(dest.(*any))
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return nil
	}
}

// getInt64 extracts an int64 from a scanned any pointer.
func getInt64(dest any) int64 {
	value := *(dest.(*any))
	if value == nil {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case string:
		// Some drivers return numeric columns as strings.
		var n int64
		_, _ = fmt.Sscanf(v, "%d", &n)
		return n
	default:
		return 0
	}
}

// getTime extracts a time.Time from a scanned any pointer.
// The DB2 CLI driver returns TIMESTAMP columns as strings via SQL_C_CHAR
// binding, so we fall back to string parsing when the native time.Time
// assertion fails.
func getTime(dest any) time.Time {
	value := *(dest.(*any))
	if value == nil {
		return time.Time{}
	}
	if t, ok := value.(time.Time); ok {
		return t
	}
	if s, ok := value.(string); ok {
		// DB2 native format: "2006-01-02 15:04:05.999999"
		if t, err := time.Parse("2006-01-02 15:04:05.999999", s); err == nil {
			return t
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
