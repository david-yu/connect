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
	"regexp"
	"strings"
	"time"
)

// db2IdentifierRE matches valid DB2 identifiers that are safe to embed in SQL
// without quoting: non-empty, first char letter or underscore, rest alphanumeric or underscore.
var db2IdentifierRE = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

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
	// TableFilter narrows which tables to stream. When nil all tables in Tables
	// (or all registered tables when Tables is empty) are streamed.
	TableFilter func(string) bool
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

	// pendingBeforeByTable holds the trailing opTypeUpdateBefore event (opcode 3)
	// when a PollBatchSize boundary falls between the D and I rows of an update
	// pair. The matching opTypeUpdateAfter row will arrive in the next poll and
	// will be prepended before pairOpcodeEvents merges the pair.
	pendingBeforeByTable map[string]*ChangeEvent
}

// NewStreamer creates a new Streamer.
func NewStreamer(db *sql.DB, config StreamConfig, version Version) *Streamer {
	return &Streamer{
		db:                   db,
		config:               config,
		version:              version,
		changeTables:         make(map[string]string),
		pendingBeforeByTable: make(map[string]*ChangeEvent),
	}
}

// isValidDB2IdentifierInternal reports whether s is safe to embed in SQL strings:
// non-empty, first char letter or underscore, remaining chars alphanumeric or underscore.
func isValidDB2IdentifierInternal(s string) bool {
	return db2IdentifierRE.MatchString(s)
}

// Initialize discovers the change tables for all monitored tables from IBMSNAP_REGISTER.
// When config.Tables is empty, all registered tables for the schema are discovered dynamically.
// config.TableFilter (if set) narrows the discovered or configured set.
func (s *Streamer) Initialize(ctx context.Context) error {
	if !isValidDB2IdentifierInternal(s.config.Schema) {
		return fmt.Errorf("invalid schema %q: must be non-empty uppercase alphanumeric+underscore", s.config.Schema)
	}

	cdcSchema := s.config.asncdcSchema()

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

	dynamicDiscovery := len(s.config.Tables) == 0

	registered := make(map[string]bool)

	for rows.Next() {
		var sourceOwner, sourceTable, cdOwner, cdTable string
		if err := rows.Scan(&sourceOwner, &sourceTable, &cdOwner, &cdTable); err != nil {
			return fmt.Errorf("scanning registration row: %w", err)
		}

		sourceTable = strings.TrimSpace(sourceTable)
		cdTable = strings.TrimSpace(cdTable)

		if dynamicDiscovery {
			// Accept all registered tables, apply filter below.
			if s.config.TableFilter == nil || s.config.TableFilter(sourceTable) {
				changeTableName := fmt.Sprintf("%s.%s", strings.TrimSpace(cdOwner), strings.TrimSpace(cdTable))
				s.changeTables[sourceTable] = changeTableName
				// Force a new backing array to avoid aliasing the caller's slice
				// (on reconnect, the same StreamConfig is reused with spare capacity).
				s.config.Tables = append(s.config.Tables[:len(s.config.Tables):len(s.config.Tables)], sourceTable)
				registered[sourceTable] = true
			}
		} else {
			for _, monitoredTable := range s.config.Tables {
				if strings.EqualFold(sourceTable, monitoredTable) {
					if s.config.TableFilter == nil || s.config.TableFilter(monitoredTable) {
						changeTableName := fmt.Sprintf("%s.%s", strings.TrimSpace(cdOwner), strings.TrimSpace(cdTable))
						s.changeTables[monitoredTable] = changeTableName
					}
					registered[monitoredTable] = true
					break
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating registration rows: %w", err)
	}

	if !dynamicDiscovery {
		for _, table := range s.config.Tables {
			if !registered[table] {
				return fmt.Errorf("table %s.%s is not registered for CDC (run ASNCDC.ADDTABLE)", s.config.Schema, table)
			}
		}
	}

	if len(s.changeTables) == 0 {
		return fmt.Errorf("no CDC-registered tables found for schema %s (check ASNCDC.IBMSNAP_REGISTER)", s.config.Schema)
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
	// intentSeqByTable tracks the last IBMSNAP_INTENTSEQ seen per table at currentCSN,
	// enabling composite (CSN, IntentSeq) pagination when a batch boundary lands
	// mid-transaction at the same CSN across multiple tables.
	intentSeqByTable := make(map[string]int64)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		events, maxCSN, newIntentSeqs, err := s.pollChanges(ctx, currentCSN, intentSeqByTable)
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
			intentSeqByTable = newIntentSeqs
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
	tableName string
	events    []ChangeEvent
	// full is true when the raw SQL row count equalled PollBatchSize (before
	// pairOpcodeEvents merging) OR when the table has a pending before-image
	// that must be matched next poll. Either condition means the watermark must
	// not advance past this table's last event CSN.
	full bool
}

// computeSafeCSN returns the highest CSN that is safe to advance to across all
// tables in a single poll round.
//
// For each full table (raw row count == PollBatchSize, or pending before-image
// held), the batch may have been truncated mid-transaction. The safe per-table
// ceiling is the penultimate distinct CSN within that table's own batch; events
// at the max CSN must be re-fetched. Edge case: all rows in the batch share one
// CSN — advance to tableMax anyway (at-least-once; caller deduplicates on
// csn+intentSeq).
//
// Non-full tables impose no constraint: all rows up to upperCSN were returned.
//
// The global safe ceiling is the minimum of all per-table ceilings, starting
// from upperCSN (no full table → no narrowing).
func computeSafeCSN(results []tableResult, upperCSN CSN) CSN {
	safeCSN := upperCSN
	for _, r := range results {
		if !r.full {
			continue // not full — no constraint from this table
		}
		if len(r.events) == 0 {
			continue // pending-only full: no events to bound against
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
// intentSeqByTable holds the per-table last IBMSNAP_INTENTSEQ seen at afterCSN.
// When any table has a non-zero entry, composite (CSN, IntentSeq) pagination is
// used for that table so the batch does not re-deliver already-seen rows.
// A global afterIntentSeq would incorrectly skip table B's events when table A
// defines the high-water mark for a shared CSN.
func (s *Streamer) pollChanges(ctx context.Context, afterCSN CSN, intentSeqByTable map[string]int64) ([]ChangeEvent, CSN, map[string]int64, error) {
	upperCSN, err := s.getUpperBound(ctx)
	if err != nil {
		return nil, CSN{}, nil, fmt.Errorf("getting upper bound: %w", err)
	}
	// Skip the poll when the capture daemon hasn't advanced past our watermark,
	// but only when no table has mid-CSN pending rows (intentSeq > 0).
	// Any table with intentSeq > 0 means we are mid-transaction and must continue.
	anyPending := false
	for _, seq := range intentSeqByTable {
		if seq > 0 {
			anyPending = true
			break
		}
	}
	if !upperCSN.Greater(afterCSN) && !anyPending {
		return nil, afterCSN, intentSeqByTable, nil
	}

	results := make([]tableResult, 0, len(s.changeTables))
	allEvents := make([]ChangeEvent, 0, len(s.changeTables)*s.config.PollBatchSize)

	for tableName, changeTableName := range s.changeTables {
		tableIntentSeq := intentSeqByTable[tableName]
		events, rawCount, err := s.pollChangeTable(ctx, tableName, changeTableName, afterCSN, tableIntentSeq, upperCSN)
		if err != nil {
			return nil, CSN{}, nil, fmt.Errorf("polling change table %s: %w", changeTableName, err)
		}
		// full=true when: raw row count hit the batch limit (batch was truncated),
		// OR a pending before-image is held for this table (matching I row must
		// arrive next poll — watermark must not advance past the pending CSN).
		full := rawCount == s.config.PollBatchSize || s.pendingBeforeByTable[tableName] != nil
		results = append(results, tableResult{tableName: tableName, events: events, full: full})
		allEvents = append(allEvents, events...)
	}

	sorted := s.sortEventsByCSN(allEvents)
	if len(sorted) == 0 {
		return nil, afterCSN, intentSeqByTable, nil
	}

	safeCSN := computeSafeCSN(results, upperCSN)

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
		return nil, afterCSN, intentSeqByTable, nil
	}

	returnCSN := computeReturnCSN(safeCSN, sorted)

	// Build per-table max intentSeq at returnCSN for composite pagination.
	// If returnCSN advanced past afterCSN, return an empty map (intent seqs reset).
	var newIntentSeqs map[string]int64
	if returnCSN.Greater(afterCSN) {
		newIntentSeqs = make(map[string]int64) // CSN advanced: all intent seqs reset
	} else {
		newIntentSeqs = make(map[string]int64, len(intentSeqByTable))
		for i := len(sorted) - 1; i >= 0; i-- {
			ev := sorted[i]
			if !ev.CSN.Equal(returnCSN) {
				break
			}
			if ev.IntentSeq > newIntentSeqs[ev.Table] {
				newIntentSeqs[ev.Table] = ev.IntentSeq
			}
		}
	}
	return sorted, returnCSN, newIntentSeqs, nil
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
		// Note: if SYNCHPOINT is null (capture daemon not yet written), returns NullCSN.
		// buildPollQuery with NullCSN upper bound emits X'00...' which safely returns 0 rows.
		// This is correct-by-accident; a future hardening pass should add an explicit nil check.
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
			"(cdc.IBMSNAP_COMMITSEQ > X'%s' OR (cdc.IBMSNAP_COMMITSEQ = X'%s' AND cdc.IBMSNAP_INTENTSEQ > %d))",
			afterHex, afterHex, afterIntentSeq,
		)
	} else {
		afterPredicate = fmt.Sprintf("cdc.IBMSNAP_COMMITSEQ > X'%s'", afterHex)
	}

	// LEAD/LAG window functions classify each D/I row within a COMMITSEQ:
	//   OPCODE 1 = standalone DELETE
	//   OPCODE 2 = standalone INSERT
	//   OPCODE 3 = before-image of UPDATE (D row with an immediately following I)
	//   OPCODE 4 = after-image of UPDATE  (I row with an immediately preceding D)
	// Ported from Debezium LuwPlatform.java CHANGE_TABLE_DATA_COLUMNS_QUERY.
	return fmt.Sprintf(`
		SELECT CASE
		  WHEN cdc.IBMSNAP_OPERATION = 'D'
		       AND LEAD(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ) = 'I'
		       THEN 3
		  WHEN cdc.IBMSNAP_OPERATION = 'I'
		       AND LAG(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ) = 'D'
		       THEN 4
		  WHEN cdc.IBMSNAP_OPERATION = 'D' THEN 1
		  WHEN cdc.IBMSNAP_OPERATION = 'I' THEN 2
		END AS IBMSNAP_OPCODE,
		cdc.*
		FROM %s cdc
		WHERE %s
		  AND cdc.IBMSNAP_COMMITSEQ <= X'%s'
		ORDER BY cdc.IBMSNAP_COMMITSEQ, cdc.IBMSNAP_INTENTSEQ
		FETCH FIRST %d ROWS ONLY
	`, changeTableName, afterPredicate, upperHex, s.config.PollBatchSize)
}

// pollChangeTable queries a single change table for events in the CSN window (afterCSN, upperCSN].
//
// Returns the merged events, the raw SQL row count (before pairOpcodeEvents merging), and any error.
// The raw row count is used by the caller to determine whether the batch was full (truncated).
//
// Pending before-image handling: when the LEAD/LAG query places a D+I pair at the PollBatchSize
// boundary, the opTypeUpdateBefore (D) row is saved in s.pendingBeforeByTable[tableName] instead
// of being emitted as a phantom delete. On the next poll, it is prepended to the raw events before
// pairOpcodeEvents processes the complete pair.
func (s *Streamer) pollChangeTable(ctx context.Context, tableName, changeTableName string, afterCSN CSN, afterIntentSeq int64, upperCSN CSN) ([]ChangeEvent, int, error) {
	query := s.buildPollQuery(changeTableName, afterCSN, afterIntentSeq, upperCSN)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("querying change table %s: %w", changeTableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, fmt.Errorf("getting columns for %s: %w", changeTableName, err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, 0, fmt.Errorf("getting column types for %s: %w", changeTableName, err)
	}

	// Locate metadata columns by name; collect data column indices.
	opIdx, csnIdx, intentSeqIdx, tsIdx, opcodeIdx := -1, -1, -1, -1, -1
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
		case "IBMSNAP_OPCODE":
			opcodeIdx = i
		default:
			if !strings.HasPrefix(col, "IBMSNAP_") {
				dataColIdxs = append(dataColIdxs, i)
				dataColNames = append(dataColNames, col)
			}
		}
	}

	if opIdx < 0 || csnIdx < 0 || intentSeqIdx < 0 {
		return nil, 0, fmt.Errorf("change table %s is missing required IBMSNAP_ columns (found: %v)", changeTableName, columns)
	}

	var rawEvents []ChangeEvent
	var rawCount int

	// Reuse scanDest and scanPtrs across rows to avoid one allocation per column per row.
	scanDest := make([]any, len(columns))
	scanPtrs := make([]any, len(columns))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		rawCount++
		// Clear previous row values before scanning (avoids stale data on nil columns).
		for i := range scanDest {
			scanDest[i] = nil
		}

		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, 0, fmt.Errorf("scanning row from %s: %w", changeTableName, err)
		}

		csnBytes := getBytes(&scanDest[csnIdx])
		intentSeq := getInt64(&scanDest[intentSeqIdx])

		var timestamp time.Time
		if tsIdx >= 0 {
			timestamp = getTime(&scanDest[tsIdx])
		}

		csn := NewCSNFromDBValue(csnBytes)

		var opType OpType
		var opErr error
		if opcodeIdx >= 0 {
			code := getInt64(&scanDest[opcodeIdx])
			opType, opErr = fromOpcodeInt(code)
		} else {
			operation := getString(&scanDest[opIdx])
			opType, opErr = FromDB2Op(operation)
		}
		if opErr != nil {
			// Skip unknown operation types rather than failing the whole batch.
			continue
		}

		data := make(map[string]any, len(dataColNames))
		for i, idx := range dataColIdxs {
			data[dataColNames[i]] = convertDB2Value(scanDest[idx], columnTypes[idx])
		}

		rawEvents = append(rawEvents, ChangeEvent{
			Schema:    s.config.Schema,
			Table:     tableName,
			Operation: opType,
			CSN:       csn,
			IntentSeq: intentSeq,
			Timestamp: timestamp,
			Data:      data,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, rawCount, err
	}

	// Cross-poll D+I pairing: inject any pending before-image from the previous
	// poll at the front of rawEvents when it matches the first after-image.
	if pending := s.pendingBeforeByTable[tableName]; pending != nil {
		if len(rawEvents) > 0 &&
			rawEvents[0].Operation == opTypeUpdateAfter &&
			rawEvents[0].CSN.Equal(pending.CSN) {
			rawEvents = append([]ChangeEvent{*pending}, rawEvents...)
		}
		// Whether matched or not, clear the pending state now — it either
		// contributed to a pair or the matching I never arrived (shouldn't
		// happen with DB2 SQL Replication, but we must not hold stale state).
		delete(s.pendingBeforeByTable, tableName)
	}

	// If the last raw event is an unmatched opTypeUpdateBefore, hold it for
	// the next poll. pairOpcodeEvents will see a complete pair next time.
	if len(rawEvents) > 0 && rawEvents[len(rawEvents)-1].Operation == opTypeUpdateBefore {
		pending := rawEvents[len(rawEvents)-1]
		s.pendingBeforeByTable[tableName] = &pending
		rawEvents = rawEvents[:len(rawEvents)-1]
	}

	return pairOpcodeEvents(rawEvents), rawCount, nil
}

// pairOpcodeEvents merges consecutive opTypeUpdateBefore + opTypeUpdateAfter pairs
// (produced by the LEAD/LAG query) into a single OpTypeUpdate event with BeforeData
// populated. Pairs must be consecutive and share the same CSN (guaranteed by the
// LEAD/LAG window function and computeSafeCSN pagination).
//
// Cross-batch D+I pairs are handled upstream: pollChangeTable injects the
// pending before-image at the front and strips the trailing before-image before
// calling this function, so pairOpcodeEvents only sees complete pairs or
// non-update events.
func pairOpcodeEvents(events []ChangeEvent) []ChangeEvent {
	if len(events) == 0 {
		return events
	}
	out := make([]ChangeEvent, 0, len(events))
	for i := 0; i < len(events); i++ {
		ev := events[i]
		if ev.Operation == opTypeUpdateBefore && i+1 < len(events) {
			next := events[i+1]
			if next.Operation == opTypeUpdateAfter && next.CSN.Equal(ev.CSN) {
				out = append(out, ChangeEvent{
					Schema:     next.Schema,
					Table:      next.Table,
					Operation:  OpTypeUpdate,
					CSN:        next.CSN,
					IntentSeq:  next.IntentSeq,
					Timestamp:  next.Timestamp,
					Data:       next.Data,
					BeforeData: ev.Data,
				})
				i++ // skip the after-image row
				continue
			}
		}
		out = append(out, ev)
	}
	return out
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
// Note (MI-8): returns 0 for unknown types. IntentSeq=0 is a valid DB2 value;
// if IBMSNAP_INTENTSEQ contains an unexpected type (e.g. float64 from a mock
// driver), the 0 return can satisfy intentSeq > 0 guards and trigger infinite
// pagination. Full fix (type assertions + error return) is out of scope.
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
