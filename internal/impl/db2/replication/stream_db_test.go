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
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helper function tests (no DB)
// ---------------------------------------------------------------------------

func TestGetStringHelper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"[]byte", []byte("world"), "world"},
		{"int64", int64(42), "42"},
		{"float64", float64(3.14), "3.14"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := tc.value
			dest := &v
			assert.Equal(t, tc.want, getString(dest))
		})
	}
}

func TestGetTimeHelper(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second)

	tests := []struct {
		name     string
		value    any
		wantZero bool
		wantTime time.Time
	}{
		{"nil", nil, true, time.Time{}},
		{"time.Time", now, false, now},
		{"string (not a time)", "not-a-time", true, time.Time{}},
		{"int64 (not a time)", int64(12345), true, time.Time{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := tc.value
			result := getTime(&v)
			if tc.wantZero {
				assert.True(t, result.IsZero())
			} else {
				assert.Equal(t, tc.wantTime, result)
			}
		})
	}
}

func TestStreamConfigAsncdcSchema(t *testing.T) {
	t.Parallel()

	c := &StreamConfig{}
	assert.Equal(t, "ASNCDC", c.asncdcSchema(), "default schema")

	c.AsnCDCSchema = "CUSTOM"
	assert.Equal(t, "CUSTOM", c.asncdcSchema(), "explicit schema")
}

// ---------------------------------------------------------------------------
// DB-dependent tests
// ---------------------------------------------------------------------------

func TestGetUpperBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rows     [][]driver.Value
		wantCSN  uint64
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "normal SYNCHPOINT",
			rows:    [][]driver.Value{{[]byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}}}, // 12345
			wantCSN: 12345,
		},
		{
			name:     "nil SYNCHPOINT returns NullCSN",
			rows:     [][]driver.Value{{nil}},
			wantNull: true,
		},
		{
			name:    "query error propagates",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.wantErr {
						return nil, nil, fmt.Errorf("SYNCHPOINT query failed")
					}
					return []string{"MAX(SYNCHPOINT)"}, tc.rows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{Schema: "MYSCHEMA"}, Version{})
			csn, err := s.getUpperBound(context.Background())

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.wantNull {
				assert.True(t, csn.IsNull())
			} else {
				assert.Equal(t, tc.wantCSN, csn.Uint64())
			}
		})
	}
}

func TestInitialize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rows      [][]driver.Value
		tables    []string
		wantErr   bool
		errSubstr string
	}{
		{
			name:   "single table registered",
			rows:   [][]driver.Value{{"MYSCHEMA", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"}},
			tables: []string{"EMPLOYEES"},
		},
		{
			name:   "case-insensitive match",
			rows:   [][]driver.Value{{"MYSCHEMA", "employees", "ASNCDC", "EMPLOYEES_CT"}},
			tables: []string{"EMPLOYEES"},
		},
		{
			name: "multiple tables",
			rows: [][]driver.Value{
				{"MYSCHEMA", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"},
				{"MYSCHEMA", "ORDERS", "ASNCDC", "ORDERS_CT"},
			},
			tables: []string{"EMPLOYEES", "ORDERS"},
		},
		{
			name:      "table not registered returns error",
			rows:      [][]driver.Value{{"MYSCHEMA", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"}},
			tables:    []string{"EMPLOYEES", "MISSING_TABLE"},
			wantErr:   true,
			errSubstr: "MISSING_TABLE",
		},
		{
			name:    "query error propagates",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.wantErr && tc.rows == nil {
						return nil, nil, fmt.Errorf("registration query failed")
					}
					return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"}, tc.rows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schema: "MYSCHEMA",
				Tables: tc.tables,
			}, Version{})

			err := s.Initialize(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				if tc.errSubstr != "" {
					assert.Contains(t, err.Error(), tc.errSubstr)
				}
				return
			}
			require.NoError(t, err)

			// Verify change tables were populated.
			for _, table := range tc.tables {
				_, ok := s.changeTables[table]
				assert.True(t, ok, "change table for %s should be registered", table)
			}
		})
	}
}

func TestPollChangeTable(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)

	tests := []struct {
		name      string
		rows      [][]driver.Value
		columns   []string
		wantCount int
		wantErr   bool
		errSubstr string
	}{
		{
			name: "one insert event",
			columns: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"EMP_ID", "EMP_NAME",
			},
			rows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}, // CSN=12345
				int64(1),                             // INTENTSEQ
				"I",                                  // INSERT
				ts,                                   // LOGMARKER
				int64(42),                            // EMP_ID
				"Alice",                              // EMP_NAME
			}},
			wantCount: 1,
		},
		{
			name: "delete event",
			columns: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"ID",
			},
			rows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0, 1},
				int64(1),
				"D",
				ts,
				int64(1),
			}},
			wantCount: 1,
		},
		{
			name: "unknown operation skipped",
			columns: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"ID",
			},
			rows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0, 1},
				int64(1),
				"Z", // unknown operation
				ts,
				int64(1),
			}},
			wantCount: 0,
		},
		{
			name:      "missing IBMSNAP_ columns returns error",
			columns:   []string{"COL1", "COL2"},
			rows:      [][]driver.Value{{"val1", "val2"}},
			wantErr:   true,
			errSubstr: "missing required IBMSNAP_",
		},
		{
			name:    "query error propagates",
			wantErr: true,
		},
		{
			name:      "empty result set",
			columns:   []string{"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER"},
			rows:      nil,
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.wantErr && tc.columns == nil {
						return nil, nil, fmt.Errorf("change table query failed")
					}
					return tc.columns, tc.rows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schema:        "MYSCHEMA",
				PollBatchSize: 100,
			}, Version{})

			events, err := s.pollChangeTable(context.Background(), "EMPLOYEES", "ASNCDC.EMPLOYEES_CT",
				NewCSN(0), 0, NewCSN(99999))

			if tc.wantErr {
				require.Error(t, err)
				if tc.errSubstr != "" {
					assert.Contains(t, err.Error(), tc.errSubstr)
				}
				return
			}
			require.NoError(t, err)
			assert.Len(t, events, tc.wantCount)
		})
	}
}

func TestPollChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		afterCSN   CSN
		upperCSN   []byte // bytes returned for SYNCHPOINT; nil = NullCSN
		changeRows [][]driver.Value
		changeCols []string
		wantCount  int
		wantErr    bool
	}{
		{
			name:      "no new changes (upper <= after)",
			afterCSN:  NewCSN(100),
			upperCSN:  []byte{0, 0, 0, 0, 0, 0, 0, 99}, // 99 < 100
			wantCount: 0,
		},
		{
			name:     "new events returned",
			afterCSN: NewCSN(0),
			upperCSN: []byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}, // 12345
			changeCols: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"ID",
			},
			changeRows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0, 50},
				int64(1),
				"I",
				time.Now(),
				int64(1),
			}},
			wantCount: 1,
		},
		{
			name:     "upper bound query error",
			afterCSN: NewCSN(0),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			callCount := 0
			db := openFakeDB(t, &replFakeHandlers{
				query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					callCount++
					if tc.wantErr && callCount == 1 {
						return nil, nil, fmt.Errorf("SYNCHPOINT error")
					}
					if strings.Contains(q, "SYNCHPOINT") {
						return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{tc.upperCSN}}, nil
					}
					// Change table query
					return tc.changeCols, tc.changeRows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schema:        "MYSCHEMA",
				PollBatchSize: 100,
			}, Version{})

			if !tc.wantErr {
				// Register a fake change table.
				s.changeTables["EMPLOYEES"] = "ASNCDC.EMPLOYEES_CT"
			}

			events, maxCSN, _, err := s.pollChanges(context.Background(), tc.afterCSN, 0)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, events, tc.wantCount)
			if tc.wantCount > 0 {
				assert.True(t, maxCSN.Greater(tc.afterCSN) || maxCSN.Equal(tc.afterCSN))
			}
		})
	}
}

func TestStreamContextCancel(t *testing.T) {
	t.Parallel()

	// Stream should exit cleanly when context is cancelled.
	callCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			callCount++
			// Return a SYNCHPOINT that's always less than afterCSN to avoid
			// actually processing events. Stream will back off and then exit via ctx.
			return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())

	s := NewStreamer(db, StreamConfig{
		Schema:          "MYSCHEMA",
		BackoffInterval: 10 * time.Millisecond,
		PollBatchSize:   100,
		StartingCSN:     NewCSN(0),
	}, Version{})
	s.changeTables["T"] = "ASNCDC.T_CT"

	done := make(chan error, 1)
	go func() {
		done <- s.Stream(ctx, func(_ ChangeEvent) error { return nil })
	}()

	// Cancel after a short delay to let the Stream loop spin at least once.
	time.Sleep(30 * time.Millisecond)
	cancel()

	err := <-done
	assert.ErrorIs(t, err, context.Canceled)
}

// TestPollChangeTableDeleteEvent verifies that a 'D' operation row is decoded
// into a ChangeEvent with OpTypeDelete and the row data in the Data map.
// In DB2 LUW SQL Replication, DELETE rows are written to the change table with
// IBMSNAP_OPERATION='D'. The before-image column values are present in the row.
func TestPollChangeTableDeleteEvent(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	csnBytes := []byte{0, 0, 0, 0, 0, 0, 0, 42} // CSN = 42

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"EMP_ID", "EMP_NAME",
				}, [][]driver.Value{{
					csnBytes, int64(1), "D", ts,
					int64(99), "Alice",
				}}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schema: "MYSCHEMA", PollBatchSize: 100}, Version{})
	events, err := s.pollChangeTable(context.Background(), "EMPLOYEES", "ASNCDC.EMPLOYEES_CT",
		NewCSN(0), 0, NewCSN(99999))

	require.NoError(t, err)
	require.Len(t, events, 1)

	evt := events[0]
	assert.Equal(t, OpTypeDelete, evt.Operation)
	assert.Equal(t, uint64(42), evt.CSN.Uint64())
	assert.Equal(t, int64(1), evt.IntentSeq)
	assert.Equal(t, ts, evt.Timestamp)
	assert.Equal(t, "MYSCHEMA", evt.Schema)
	assert.Equal(t, "EMPLOYEES", evt.Table)

	// Data map must contain row columns but not any IBMSNAP_* control columns.
	assert.Equal(t, int64(99), evt.Data["EMP_ID"])
	assert.Equal(t, "Alice", evt.Data["EMP_NAME"])
	_, hasIBM := evt.Data["IBMSNAP_OPERATION"]
	assert.False(t, hasIBM, "IBMSNAP_* control columns must not appear in Data")
}

// TestPollChangeTableUpdateAsDIPair verifies that pollChangeTable correctly maps
// IBMSNAP_OPERATION='D' and 'I' rows to OpTypeDelete and OpTypeInsert when no
// IBMSNAP_OPCODE column is present (the non-LEAD/LAG fallback path).
//
// In production the buildPollQuery LEAD/LAG subquery emits IBMSNAP_OPCODE 3/4 for
// update pairs; pairOpcodeEvents then merges them into a single OpTypeUpdate event.
// This test validates the fallback: when the result set has no IBMSNAP_OPCODE column,
// pollChangeTable maps raw D/I IBMSNAP_OPERATION codes to OpTypeDelete/OpTypeInsert.
func TestPollChangeTableUpdateAsDIPair(t *testing.T) {
	t.Parallel()

	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 77} // both rows share this CSN
	ts := time.Now().Truncate(time.Second)

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID", "SALARY",
				}, [][]driver.Value{
					// D row: before-image (intentseq=1, old salary)
					{sharedCSN, int64(1), "D", ts, int64(5), int64(50000)},
					// I row: after-image (intentseq=2, new salary)
					{sharedCSN, int64(2), "I", ts, int64(5), int64(60000)},
				}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schema: "MYSCHEMA", PollBatchSize: 100}, Version{})
	events, err := s.pollChangeTable(context.Background(), "EMPLOYEES", "ASNCDC.EMPLOYEES_CT",
		NewCSN(0), 0, NewCSN(99999))

	require.NoError(t, err)
	require.Len(t, events, 2, "UPDATE must produce exactly 2 events (D + I)")

	// First event: delete with before-image salary.
	assert.Equal(t, OpTypeDelete, events[0].Operation)
	assert.Equal(t, uint64(77), events[0].CSN.Uint64())
	assert.Equal(t, int64(1), events[0].IntentSeq)
	assert.Equal(t, int64(50000), events[0].Data["SALARY"])

	// Second event: insert with after-image salary.
	assert.Equal(t, OpTypeInsert, events[1].Operation)
	assert.Equal(t, uint64(77), events[1].CSN.Uint64())
	assert.Equal(t, int64(2), events[1].IntentSeq)
	assert.Equal(t, int64(60000), events[1].Data["SALARY"])

	// Both events must share the same CSN — this is what identifies them as
	// the two halves of a single UPDATE operation.
	assert.True(t, events[0].CSN.Equal(events[1].CSN),
		"D and I events for an UPDATE must share the same IBMSNAP_COMMITSEQ")
}

// TestPollChangesMultiTableCSNOrdering verifies that events from multiple change
// tables are merged and returned in strict (CSN, IntentSeq) order regardless of
// which table they originated from.
//
// Two change tables (ORDERS_CT with rows at CSN=10,30 and EMPLOYEES_CT with a
// row at CSN=20) must be merged so the output order is CSN 10 → 20 → 30.
func TestPollChangesMultiTableCSNOrdering(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 50}}, // upper bound = 50
				}, nil
			}
			cols := []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER", "ID",
			}
			if strings.Contains(q, "ORDERS_CT") {
				return cols, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 10}, int64(1), "I", ts, int64(1)},
					{[]byte{0, 0, 0, 0, 0, 0, 0, 30}, int64(1), "I", ts, int64(3)},
				}, nil
			}
			// EMPLOYEES_CT
			return cols, [][]driver.Value{
				{[]byte{0, 0, 0, 0, 0, 0, 0, 20}, int64(1), "I", ts, int64(2)},
			}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schema:        "MYSCHEMA",
		PollBatchSize: 100,
	}, Version{})
	s.changeTables["ORDERS"] = "ASNCDC.ORDERS_CT"
	s.changeTables["EMPLOYEES"] = "ASNCDC.EMPLOYEES_CT"

	events, maxCSN, _, err := s.pollChanges(context.Background(), NewCSN(0), 0)

	require.NoError(t, err)
	require.Len(t, events, 3)

	assert.Equal(t, uint64(10), events[0].CSN.Uint64(), "first event must have lowest CSN")
	assert.Equal(t, uint64(20), events[1].CSN.Uint64(), "second event must have middle CSN")
	assert.Equal(t, uint64(30), events[2].CSN.Uint64(), "third event must have highest CSN")
	assert.Equal(t, uint64(30), maxCSN.Uint64())
}

// TestInitializeCaseSensitiveTableNames verifies that Initialize handles mixed-
// case table names from IBMSNAP_REGISTER correctly. DB2 may store table names
// in the register with different casing; the connector matches them
// case-insensitively against the configured tables list.
func TestInitializeCaseSensitiveTableNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		registeredAs   string // value in SOURCE_TABLE column
		configuredAs   string // value in StreamConfig.Tables
		wantRegistered bool
	}{
		{
			name:           "exact match uppercase",
			registeredAs:   "EMPLOYEES",
			configuredAs:   "EMPLOYEES",
			wantRegistered: true,
		},
		{
			name:           "registry lowercase, config uppercase",
			registeredAs:   "employees",
			configuredAs:   "EMPLOYEES",
			wantRegistered: true,
		},
		{
			name:           "registry uppercase, config lowercase",
			registeredAs:   "EMPLOYEES",
			configuredAs:   "employees",
			wantRegistered: true,
		},
		{
			name:           "mixed case in registry",
			registeredAs:   "Employees",
			configuredAs:   "EMPLOYEES",
			wantRegistered: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"},
						[][]driver.Value{
							{"MYSCHEMA", tc.registeredAs, "ASNCDC", "EMPLOYEES_CT"},
						}, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schema: "MYSCHEMA",
				Tables: []string{tc.configuredAs},
			}, Version{})

			err := s.Initialize(context.Background())
			if tc.wantRegistered {
				require.NoError(t, err)
				assert.Len(t, s.changeTables, 1)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestInitializeCustomCDCSchema verifies that a non-default CDC schema
// (AsnCDCSchema config field) is used in the IBMSNAP_REGISTER query instead of
// the hardcoded default. This allows operators who installed SQL Replication
// under a custom schema to use the connector.
func TestInitializeCustomCDCSchema(t *testing.T) {
	t.Parallel()

	var capturedQueries []string

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			capturedQueries = append(capturedQueries, q)
			if strings.Contains(q, "SOURCE_OWNER") {
				return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"},
					[][]driver.Value{
						{"MYSCHEMA", "EMPLOYEES", "CDCSCHEMA", "EMPLOYEES_CT"},
					}, nil
			}
			// detectCommitSeqByteLen queries SYSCAT.COLUMNS — return a valid length.
			return []string{"LENGTH"}, [][]driver.Value{{int64(16)}}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schema:       "MYSCHEMA",
		Tables:       []string{"EMPLOYEES"},
		AsnCDCSchema: "CDCSCHEMA", // non-default CDC schema
	}, Version{})

	err := s.Initialize(context.Background())
	require.NoError(t, err)

	allQueries := strings.Join(capturedQueries, "\n")
	assert.Contains(t, allQueries, "CDCSCHEMA.IBMSNAP_REGISTER",
		"Initialize must query the custom CDC schema, not the hardcoded default")
	assert.NotContains(t, allQueries, "ASNCDC.IBMSNAP_REGISTER",
		"default schema name must not appear when a custom schema is configured")
}

func TestStreamHandlerError(t *testing.T) {
	t.Parallel()

	ts := time.Now()
	callCount := 0

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			callCount++
			if strings.Contains(q, "SYNCHPOINT") {
				// Return a large SYNCHPOINT so pollChanges sees new events.
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0xFF, 0xFF}},
				}, nil
			}
			// Change table returns one event.
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID",
				}, [][]driver.Value{{
					[]byte{0, 0, 0, 0, 0, 0, 0, 50},
					int64(1),
					"I",
					ts,
					int64(1),
				}}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schema:          "MYSCHEMA",
		BackoffInterval: time.Millisecond,
		PollBatchSize:   100,
		StartingCSN:     NewCSN(0),
	}, Version{})
	s.changeTables["T"] = "ASNCDC.T_CT"

	err := s.Stream(context.Background(), func(_ ChangeEvent) error {
		return fmt.Errorf("handler refused event")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler refused event")
}
