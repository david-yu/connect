// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SnapshotConfig holds configuration for the initial snapshot phase.
type SnapshotConfig struct {
	Schema         string
	Tables         []string
	AsnCDCSchema   string // CDC control schema, defaults to "ASNCDC"
	BatchSize      int
	IsolationLevel string // e.g. "REPEATABLE READ"
	// TableFilter narrows which tables to snapshot. When nil all Tables are snapshotted.
	TableFilter func(string) bool
}

// asncdcSchema returns the CDC control schema, defaulting to "ASNCDC".
func (c *SnapshotConfig) asncdcSchema() string {
	if c.AsnCDCSchema != "" {
		return c.AsnCDCSchema
	}
	return "ASNCDC"
}

// Snapshotter performs the initial full-table read of all monitored tables
// before the CDC streaming phase begins.
//
// All tables are read inside a single read-only REPEATABLE READ transaction so
// that rows from different tables are consistent with each other. The CDC log
// position (CSN) is captured from ASNCDC.IBMSNAP_REGISTER *before* the first
// row is read; streaming resumes from this CSN, ensuring no changes are missed.
//
// Large tables are paginated using keyset pagination (ORDER BY pk / WHERE pk >
// lastKey FETCH FIRST N ROWS ONLY), which is efficient regardless of table size
// and avoids OFFSET-based scans.
type Snapshotter struct {
	db      *sql.DB
	config  SnapshotConfig
	version Version
}

// NewSnapshotter creates a new Snapshotter.
func NewSnapshotter(db *sql.DB, config SnapshotConfig, version Version) *Snapshotter {
	return &Snapshotter{
		db:      db,
		config:  config,
		version: version,
	}
}

// Snapshot reads all rows from each configured table and returns the CSN that
// streaming should resume from (captured before the first table is read).
func (s *Snapshotter) Snapshot(ctx context.Context, handler func(event ChangeEvent) error) (CSN, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: s.parseIsolationLevel(),
		ReadOnly:  true,
	})
	if err != nil {
		return CSN{}, fmt.Errorf("beginning snapshot transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	startCSN, err := s.captureCurrentCSN(ctx, tx)
	if err != nil {
		return CSN{}, fmt.Errorf("capturing starting CSN: %w", err)
	}

	if err = s.snapshotTablesSequential(ctx, tx, handler); err != nil {
		return CSN{}, fmt.Errorf("snapshot failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return CSN{}, fmt.Errorf("committing snapshot transaction: %w", err)
	}

	return startCSN, nil
}

// captureCurrentCSN reads MAX(SYNCHPOINT) from ASNCDC.IBMSNAP_REGISTER for the
// monitored schema. SYNCHPOINT is the CDC log position the capture daemon has
// confirmed writing; streaming will resume from this value after the snapshot.
//
// SYNCHPOINT (not CURRENT TIMESTAMP or a DB2 log-position function) is used
// because it reflects the capture daemon's confirmed write position — the same
// value that change-table poll queries compare against.
func (s *Snapshotter) captureCurrentCSN(ctx context.Context, tx *sql.Tx) (CSN, error) {
	cdcSchema := s.config.asncdcSchema()

	// Schema is validated as uppercase alphanumeric + underscore at config-parse time
	// (isValidDB2Identifier in input_db2_cdc.go), so direct embedding is safe.
	query := fmt.Sprintf(
		"SELECT MAX(SYNCHPOINT) FROM %s.IBMSNAP_REGISTER WHERE SOURCE_OWNER = '%s'",
		cdcSchema, s.config.Schema,
	)

	var synchpointBytes []byte
	err := tx.QueryRowContext(ctx, query).Scan(&synchpointBytes)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "not found") {
			// CDC tables not yet set up — start from CSN 0.
			return NewCSN(0), nil
		}
		return CSN{}, fmt.Errorf("capturing CSN from IBMSNAP_REGISTER: %w", err)
	}

	if len(synchpointBytes) == 0 {
		return NewCSN(0), nil
	}

	return NewCSNFromDBValue(synchpointBytes), nil
}

// snapshotTablesSequential snapshots tables one at a time.
func (s *Snapshotter) snapshotTablesSequential(ctx context.Context, tx *sql.Tx, handler func(event ChangeEvent) error) error {
	for _, tableName := range s.config.Tables {
		if s.config.TableFilter != nil && !s.config.TableFilter(tableName) {
			continue
		}
		if err := s.snapshotTable(ctx, tx, tableName, handler); err != nil {
			return fmt.Errorf("snapshotting table %s: %w", tableName, err)
		}
	}
	return nil
}

// snapshotTable reads all rows of one table using keyset pagination.
func (s *Snapshotter) snapshotTable(ctx context.Context, tx *sql.Tx, tableName string, handler func(event ChangeEvent) error) error {
	pks, err := s.discoverPrimaryKeys(ctx, tx, tableName)
	if err != nil {
		return fmt.Errorf("discovering primary keys: %w", err)
	}

	if len(pks) == 0 {
		return fmt.Errorf("table %s has no primary key (required for CDC snapshot)", tableName)
	}

	columns, err := s.getTableColumns(ctx, tx, tableName)
	if err != nil {
		return fmt.Errorf("getting table columns: %w", err)
	}

	var lastKeyValues []any

	for {
		batchRows, err := s.fetchBatch(ctx, tx, tableName, columns, pks, lastKeyValues)
		if err != nil {
			return fmt.Errorf("fetching batch: %w", err)
		}

		if len(batchRows) == 0 {
			break
		}

		for _, row := range batchRows {
			event := ChangeEvent{
				Schema:    s.config.Schema,
				Table:     tableName,
				Operation: OpTypeRead,
				CSN:       NullCSN(),
				Data:      row,
			}

			if err := handler(event); err != nil {
				return fmt.Errorf("handler error: %w", err)
			}
		}

		lastRow := batchRows[len(batchRows)-1]
		lastKeyValues = make([]any, len(pks))
		for i, pk := range pks {
			lastKeyValues[i] = lastRow[pk]
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

// discoverPrimaryKeys returns the primary key column names for a table, ordered by sequence.
func (s *Snapshotter) discoverPrimaryKeys(ctx context.Context, tx *sql.Tx, tableName string) ([]string, error) {
	query := `
		SELECT COLNAME
		FROM SYSCAT.KEYCOLUSE
		WHERE TABSCHEMA = ?
		  AND TABNAME = ?
		  AND CONSTNAME = (
		    SELECT CONSTNAME
		    FROM SYSCAT.TABCONST
		    WHERE TABSCHEMA = ?
		      AND TABNAME = ?
		      AND TYPE = 'P'
		  )
		ORDER BY COLSEQ
	`

	rows, err := tx.QueryContext(ctx, query, s.config.Schema, tableName, s.config.Schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying primary keys: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		pks = append(pks, strings.TrimSpace(colName))
	}

	return pks, rows.Err()
}

// getTableColumns returns all column names for a table, in column order.
func (s *Snapshotter) getTableColumns(ctx context.Context, tx *sql.Tx, tableName string) ([]string, error) {
	query := `
		SELECT COLNAME
		FROM SYSCAT.COLUMNS
		WHERE TABSCHEMA = ?
		  AND TABNAME = ?
		ORDER BY COLNO
	`

	rows, err := tx.QueryContext(ctx, query, s.config.Schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		columns = append(columns, strings.TrimSpace(colName))
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s has no columns", s.config.Schema, tableName)
	}

	return columns, rows.Err()
}

// fetchBatch fetches one page of rows using keyset pagination.
func (s *Snapshotter) fetchBatch(ctx context.Context, tx *sql.Tx, tableName string, columns, pks []string, lastKeyValues []any) ([]map[string]any, error) {
	// Quote each column name to handle names with special characters.
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = `"` + strings.ReplaceAll(col, `"`, `""`) + `"`
	}
	selectClause := strings.Join(quotedCols, ", ")

	quotedPKs := make([]string, len(pks))
	for i, pk := range pks {
		quotedPKs[i] = `"` + strings.ReplaceAll(pk, `"`, `""`) + `"`
	}
	orderByClause := strings.Join(quotedPKs, ", ")

	query := fmt.Sprintf(
		`SELECT %s FROM "%s"."%s"`,
		selectClause,
		strings.ReplaceAll(s.config.Schema, `"`, `""`),
		strings.ReplaceAll(tableName, `"`, `""`),
	)

	var args []any

	if len(lastKeyValues) > 0 {
		pkList := strings.Join(quotedPKs, ", ")
		placeholders := make([]string, len(pks))
		for i := range pks {
			placeholders[i] = "?"
		}
		query += fmt.Sprintf(" WHERE (%s) > (%s)", pkList, strings.Join(placeholders, ", "))
		args = lastKeyValues
	}

	query += fmt.Sprintf(" ORDER BY %s FETCH FIRST %d ROWS ONLY", orderByClause, s.config.BatchSize)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing batch query: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("getting column types: %w", err)
	}

	var result []map[string]any

	// Reuse scanDest and scanPtrs across rows to avoid one allocation per column per row.
	scanDest := make([]any, len(columns))
	scanPtrs := make([]any, len(columns))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		// Clear previous row values before scanning (avoids stale data on nil columns).
		for i := range scanDest {
			scanDest[i] = nil
		}

		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		rowMap := make(map[string]any, len(columns))
		for i, col := range columns {
			rowMap[col] = convertDB2Value(scanDest[i], columnTypes[i])
		}

		result = append(result, rowMap)
	}

	return result, rows.Err()
}

// parseIsolationLevel converts a string isolation level to sql.IsolationLevel.
func (s *Snapshotter) parseIsolationLevel() sql.IsolationLevel {
	switch strings.ToUpper(s.config.IsolationLevel) {
	case "READ UNCOMMITTED":
		return sql.LevelReadUncommitted
	case "READ COMMITTED":
		return sql.LevelReadCommitted
	case "REPEATABLE READ":
		return sql.LevelRepeatableRead
	case "SERIALIZABLE":
		return sql.LevelSerializable
	default:
		return sql.LevelRepeatableRead
	}
}

// convertDB2Value converts a DB2 driver value to a JSON-serializable Go type.
func convertDB2Value(value any, colType *sql.ColumnType) any {
	if value == nil {
		return nil
	}

	if b, ok := value.([]byte); ok {
		dbType := strings.ToUpper(colType.DatabaseTypeName())
		if strings.Contains(dbType, "CHAR") || strings.Contains(dbType, "CLOB") || strings.Contains(dbType, "TEXT") {
			return string(b)
		}
		// Binary types: return raw bytes (JSON-marshalled as base64).
		return b
	}

	return value
}
