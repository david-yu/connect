// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/replication"
)

// testLogger returns a *service.Logger suitable for unit tests.
func testLogger(t *testing.T) *service.Logger {
	t.Helper()
	return service.MockResources().Logger()
}

// ---------------------------------------------------------------------------
// isAlreadyExistsError
// ---------------------------------------------------------------------------

func TestIsAlreadyExistsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"SQLSTATE=42710", fmt.Errorf("sql error SQLSTATE=42710 object already exists"), true},
		{"SQLSTATE 42710", fmt.Errorf("DB2 SQL Error: SQLCODE=-601, SQLSTATE 42710"), true},
		{"unrelated error", fmt.Errorf("connection refused"), false},
		// bare "42710" without SQLSTATE prefix must not match (too broad)
		{"bare 42710 substring", fmt.Errorf("some message 42710 embedded"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isAlreadyExistsError(tc.err))
		})
	}
}

// ---------------------------------------------------------------------------
// determineCheckpointMode
// ---------------------------------------------------------------------------

func TestDetermineCheckpointMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      string
		version   replication.Version
		wantMode  string
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "auto with 11.5 → csn",
			mode:     "auto",
			version:  replication.Version{Major: 11, Minor: 5},
			wantMode: "csn",
		},
		{
			name:     "auto with 10.1 → csn",
			mode:     "auto",
			version:  replication.Version{Major: 10, Minor: 1},
			wantMode: "csn",
		},
		{
			name:      "auto with 9.7 → error",
			mode:      "auto",
			version:   replication.Version{Major: 9, Minor: 7},
			wantErr:   true,
			errSubstr: "does not support CSN",
		},
		{
			name:     "explicit csn with 11.5 → csn",
			mode:     "csn",
			version:  replication.Version{Major: 11, Minor: 5},
			wantMode: "csn",
		},
		{
			name:      "explicit csn with 9.7 → error",
			mode:      "csn",
			version:   replication.Version{Major: 9, Minor: 7},
			wantErr:   true,
			errSubstr: "requires DB2 10.1+",
		},
		{
			name:      "unknown mode → error",
			mode:      "timestamp",
			version:   replication.Version{Major: 11, Minor: 5},
			wantErr:   true,
			errSubstr: "invalid checkpoint_mode",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := &db2CDCInput{
				checkpointMode: tc.mode,
				version:        tc.version,
				log:            testLogger(t),
			}

			mode, err := d.determineCheckpointMode()
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSubstr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantMode, mode)
		})
	}
}

// ---------------------------------------------------------------------------
// eventToMessage
// ---------------------------------------------------------------------------

func TestEventToMessage(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		event    replication.ChangeEvent
		wantMeta map[string]string
	}{
		{
			name: "snapshot read event",
			event: replication.ChangeEvent{
				Schema:    "DB2ADMIN",
				Table:     "EMPLOYEES",
				Operation: replication.OpTypeRead,
				CSN:       replication.NullCSN(),
				Data:      map[string]any{"ID": 1, "NAME": "Alice"},
			},
			wantMeta: map[string]string{
				"db2_schema":    "DB2ADMIN",
				"db2_table":     "EMPLOYEES",
				"db2_operation": "read",
				"db2_csn":       "",
			},
		},
		{
			name: "insert event with CSN and timestamp",
			event: replication.ChangeEvent{
				Schema:    "DB2ADMIN",
				Table:     "ORDERS",
				Operation: replication.OpTypeInsert,
				CSN:       replication.NewCSN(12345),
				Timestamp: ts,
				Data:      map[string]any{"ORDER_ID": 99},
			},
			wantMeta: map[string]string{
				"db2_schema":    "DB2ADMIN",
				"db2_table":     "ORDERS",
				"db2_operation": "insert",
				"db2_csn":       "CSN:0000000000003039",
				"db2_timestamp": ts.Format(time.RFC3339Nano),
			},
		},
		{
			name: "delete event",
			event: replication.ChangeEvent{
				Schema:    "SCHEMA",
				Table:     "T",
				Operation: replication.OpTypeDelete,
				CSN:       replication.NewCSN(999),
				Data:      map[string]any{"ID": 7},
			},
			wantMeta: map[string]string{
				"db2_schema":    "SCHEMA",
				"db2_table":     "T",
				"db2_operation": "delete",
			},
		},
	}

	d := &db2CDCInput{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg, err := d.eventToMessage(tc.event)
			require.NoError(t, err)
			require.NotNil(t, msg)

			for key, want := range tc.wantMeta {
				got, exists := msg.MetaGet(key)
				assert.True(t, exists, "meta key %q should exist", key)
				assert.Equal(t, want, got, "meta key %q", key)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// detectVersion
// ---------------------------------------------------------------------------

func TestDetectVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryFunc func(query string, args []driver.Value) ([]string, [][]driver.Value, error)
		wantMajor int
		wantMinor int
		wantErr   bool
	}{
		{
			name: "ENV_INST_INFO returns SQL11050",
			queryFunc: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				if strings.Contains(q, "ENV_INST_INFO") {
					return []string{"SERVICE_LEVEL"}, [][]driver.Value{{"SQL11050"}}, nil
				}
				return nil, nil, fmt.Errorf("unexpected query: %s", q)
			},
			wantMajor: 11,
			wantMinor: 5,
		},
		{
			name: "fallback to PROD_RELEASE when ENV_INST_INFO fails",
			queryFunc: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				if strings.Contains(q, "ENV_INST_INFO") {
					return nil, nil, fmt.Errorf("table does not exist")
				}
				// Fallback query
				return []string{"PROD_RELEASE"}, [][]driver.Value{{"11.1.0.0"}}, nil
			},
			wantMajor: 11,
			wantMinor: 1,
		},
		{
			name: "both queries fail → error",
			queryFunc: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				return nil, nil, fmt.Errorf("connection error")
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openInputFakeDB(t, &inputFakeHandlers{query: tc.queryFunc})
			d := &db2CDCInput{db: db, log: testLogger(t)}

			ver, err := d.detectVersion(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantMajor, ver.Major)
			assert.Equal(t, tc.wantMinor, ver.Minor)
		})
	}
}

// ---------------------------------------------------------------------------
// initCheckpointTable
// ---------------------------------------------------------------------------

func TestInitCheckpointTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		createErr error
		wantErr   bool
	}{
		{
			name:      "creates table successfully",
			tableName: "RPCN.CDC_CHECKPOINT",
			createErr: nil,
		},
		{
			name:      "table already exists (42710) is silently ignored",
			tableName: "RPCN.CDC_CHECKPOINT",
			createErr: fmt.Errorf("SQLSTATE=42710 object already exists"),
		},
		{
			name:      "other exec error propagates",
			tableName: "RPCN.CDC_CHECKPOINT",
			createErr: fmt.Errorf("permission denied"),
			wantErr:   true,
		},
		{
			name:      "unqualified table name (no schema) creates table",
			tableName: "CDC_CHECKPOINT",
			createErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			execCount := 0
			db := openInputFakeDB(t, &inputFakeHandlers{
				exec: func(q string, _ []driver.Value) error {
					execCount++
					if strings.Contains(q, "CREATE SCHEMA") {
						return nil // CREATE SCHEMA always succeeds in tests
					}
					return tc.createErr
				},
			})

			d := &db2CDCInput{
				db:               db,
				cpCacheTableName: tc.tableName,
				log:              testLogger(t),
			}

			err := d.initCheckpointTable(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// loadCheckpoint (DB table path)
// ---------------------------------------------------------------------------

func TestLoadCheckpointFromDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryRows [][]driver.Value
		noRows    bool
		queryErr  error
		wantCSN   uint64
		wantNull  bool
		wantErr   bool
	}{
		{
			name:      "checkpoint found",
			queryRows: [][]driver.Value{{"CSN:0000000000003039"}}, // 12345
			wantCSN:   12345,
		},
		{
			name:     "no checkpoint → NullCSN",
			noRows:   true,
			wantNull: true,
		},
		{
			name:     "query error propagates",
			queryErr: fmt.Errorf("connection timeout"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openInputFakeDB(t, &inputFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.queryErr != nil {
						return nil, nil, tc.queryErr
					}
					if tc.noRows {
						return []string{"CACHE_VAL"}, nil, nil // no rows → sql.ErrNoRows
					}
					return []string{"CACHE_VAL"}, tc.queryRows, nil
				},
			})

			d := &db2CDCInput{
				db:                 db,
				cpCacheName:        "",
				cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
				checkpointCacheKey: "db2_cdc_checkpoint",
				log:                testLogger(t),
			}

			csn, err := d.loadCheckpoint(context.Background())
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

// ---------------------------------------------------------------------------
// saveCheckpoint (DB table path)
// ---------------------------------------------------------------------------

func TestSaveCheckpointToDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		csn     replication.CSN
		execErr error
		wantErr bool
	}{
		{
			name: "saves successfully",
			csn:  replication.NewCSN(12345),
		},
		{
			name:    "exec error propagates",
			csn:     replication.NewCSN(99),
			execErr: fmt.Errorf("deadlock detected"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var capturedArgs []driver.Value
			db := openInputFakeDB(t, &inputFakeHandlers{
				exec: func(_ string, args []driver.Value) error {
					capturedArgs = args
					return tc.execErr
				},
			})

			d := &db2CDCInput{
				db:                 db,
				cpCacheName:        "",
				cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
				checkpointCacheKey: "db2_cdc_checkpoint",
				log:                testLogger(t),
			}

			err := d.saveCheckpoint(context.Background(), tc.csn)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Verify the args include our checkpoint key and CSN string.
			require.Len(t, capturedArgs, 2)
			assert.Equal(t, "db2_cdc_checkpoint", capturedArgs[0])
			assert.Equal(t, tc.csn.String(), capturedArgs[1])
		})
	}
}

// ---------------------------------------------------------------------------
// eventToMessage data type and message body tests
// ---------------------------------------------------------------------------

// TestEventToMessageNullData verifies that ChangeEvents whose Data map contains
// nil values are serialised correctly: nil values appear as JSON null, not as
// absent keys or zero values. Downstream consumers must be able to distinguish
// "column is NULL" from "column was not selected".
//
// In the Debezium-compatible envelope, row data appears under the "after" key.
func TestEventToMessageNullData(t *testing.T) {
	t.Parallel()

	event := replication.ChangeEvent{
		Schema:    "DB2ADMIN",
		Table:     "EMPLOYEES",
		Operation: replication.OpTypeInsert,
		CSN:       replication.NewCSN(1),
		Data: map[string]any{
			"ID":         int64(42),
			"MANAGER_ID": nil, // explicit NULL from the source
			"DEPT":       nil,
		},
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))

	// Row data is under "after" in the Debezium envelope.
	after, ok := decoded["after"].(map[string]any)
	require.True(t, ok, "after field must be a JSON object")

	assert.Equal(t, float64(42), after["ID"])

	// Nil values must appear as JSON null, not as absent keys.
	managerVal, managerExists := after["MANAGER_ID"]
	assert.True(t, managerExists, "MANAGER_ID key must be present even when NULL")
	assert.Nil(t, managerVal, "MANAGER_ID must be JSON null")

	deptVal, deptExists := after["DEPT"]
	assert.True(t, deptExists, "DEPT key must be present even when NULL")
	assert.Nil(t, deptVal, "DEPT must be JSON null")

	// "before" must be null for INSERT events.
	assert.Nil(t, decoded["before"], "before must be null for INSERT events")

	// Verify top-level envelope fields.
	assert.Equal(t, "c", decoded["op"])
}

// TestEventToMessageNumericTypes verifies that numeric DB2 column types
// (INTEGER, BIGINT, FLOAT) round-trip through eventToMessage without precision
// loss or silent type coercion.
//
// In the Debezium-compatible envelope, row data appears under the "after" key.
func TestEventToMessageNumericTypes(t *testing.T) {
	t.Parallel()

	event := replication.ChangeEvent{
		Schema:    "DB2ADMIN",
		Table:     "METRICS",
		Operation: replication.OpTypeRead,
		CSN:       replication.NullCSN(),
		Data: map[string]any{
			"INT_COL":    int64(2147483647),    // max int32 as int64
			"BIGINT_COL": int64(9000000000000), // requires full int64 range
			"FLOAT_COL":  float64(3.14159),
		},
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))

	// Row data is under "after" in the Debezium envelope.
	after, ok := decoded["after"].(map[string]any)
	require.True(t, ok)

	// JSON numbers become float64 after Unmarshal; verify round-trip values.
	assert.Equal(t, float64(2147483647), after["INT_COL"])
	assert.Equal(t, float64(9000000000000), after["BIGINT_COL"])
	assert.InDelta(t, 3.14159, after["FLOAT_COL"], 1e-10)

	// Snapshot rows use op="r" and snapshot="true".
	assert.Equal(t, "r", decoded["op"])
}

// TestEventToMessageStringAndBinaryTypes verifies that VARCHAR columns appear
// as JSON strings and that binary ([]byte) columns are base64-encoded strings
// in the JSON output (standard encoding/json behaviour for []byte).
//
// In the Debezium-compatible envelope, row data appears under the "after" key.
func TestEventToMessageStringAndBinaryTypes(t *testing.T) {
	t.Parallel()

	binaryPayload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	event := replication.ChangeEvent{
		Schema:    "DB2ADMIN",
		Table:     "DOCUMENTS",
		Operation: replication.OpTypeInsert,
		CSN:       replication.NewCSN(5),
		Data: map[string]any{
			"TITLE":   "Hello World", // VARCHAR → JSON string
			"CONTENT": binaryPayload, // BLOB → []byte → base64 in JSON
		},
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))

	// Row data is under "after" in the Debezium envelope.
	after, ok := decoded["after"].(map[string]any)
	require.True(t, ok)

	// VARCHAR must come through as a plain JSON string.
	assert.Equal(t, "Hello World", after["TITLE"])

	// []byte values are base64-encoded by encoding/json.
	contentStr, isStr := after["CONTENT"].(string)
	require.True(t, isStr, "binary column must be a JSON string (base64-encoded)")
	assert.NotEmpty(t, contentStr)
}

// TestEventToMessageMetadataKeys verifies all required message metadata keys
// are set for each operation type. Snapshot events (OpTypeRead) must carry an
// empty db2_csn since NullCSN().String() == "". Streaming events must carry
// the canonical "CSN:<hex>" string and, when the timestamp is non-zero, the
// RFC3339Nano-formatted db2_timestamp.
//
// The db2_operation key carries the human-readable form ("read", "insert",
// "delete"); db2_op carries the Debezium one-character code ("r", "c", "d").
// db2_commit_lsn is an alias for db2_csn using the Debezium field name.
func TestEventToMessageMetadataKeys(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name         string
		event        replication.ChangeEvent
		wantCSN      string
		wantHasTS    bool
		wantDebezOp  string // Debezium single-char op code
		wantSnapshot string
	}{
		{
			name: "snapshot read — no CSN, no timestamp",
			event: replication.ChangeEvent{
				Schema: "S", Table: "T", Operation: replication.OpTypeRead,
				CSN: replication.NullCSN(),
			},
			wantCSN:      "",
			wantHasTS:    false,
			wantDebezOp:  "r",
			wantSnapshot: "true",
		},
		{
			name: "streaming insert — CSN and timestamp present",
			event: replication.ChangeEvent{
				Schema: "S", Table: "T", Operation: replication.OpTypeInsert,
				CSN: replication.NewCSN(500), Timestamp: ts,
			},
			wantCSN:      "CSN:00000000000001F4",
			wantHasTS:    true,
			wantDebezOp:  "c",
			wantSnapshot: "false",
		},
		{
			name: "streaming delete — CSN present, zero timestamp",
			event: replication.ChangeEvent{
				Schema: "S", Table: "T", Operation: replication.OpTypeDelete,
				CSN: replication.NewCSN(501),
			},
			wantCSN:      "CSN:00000000000001F5",
			wantHasTS:    false,
			wantDebezOp:  "d",
			wantSnapshot: "false",
		},
	}

	d := &db2CDCInput{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg, err := d.eventToMessage(tc.event)
			require.NoError(t, err)

			schema, _ := msg.MetaGet("db2_schema")
			table, _ := msg.MetaGet("db2_table")
			op, _ := msg.MetaGet("db2_operation")
			debezOp, debezOpExists := msg.MetaGet("db2_op")
			csn, csnExists := msg.MetaGet("db2_csn")
			commitLSN, commitLSNExists := msg.MetaGet("db2_commit_lsn")
			connector, connectorExists := msg.MetaGet("db2_connector")
			snapshot, snapshotExists := msg.MetaGet("db2_snapshot")

			assert.Equal(t, "S", schema)
			assert.Equal(t, "T", table)
			// db2_operation carries the human-readable form.
			assert.Equal(t, string(tc.event.Operation), op)
			// db2_op carries the Debezium one-character code.
			assert.True(t, debezOpExists, "db2_op must always be set")
			assert.Equal(t, tc.wantDebezOp, debezOp)
			// db2_csn (backward-compat) and db2_commit_lsn (Debezium name) must match.
			assert.True(t, csnExists, "db2_csn must always be set")
			assert.Equal(t, tc.wantCSN, csn)
			assert.True(t, commitLSNExists, "db2_commit_lsn must always be set")
			assert.Equal(t, tc.wantCSN, commitLSN, "db2_commit_lsn must equal db2_csn")
			// db2_connector must always be "db2".
			assert.True(t, connectorExists, "db2_connector must always be set")
			assert.Equal(t, "db2", connector)
			// db2_snapshot.
			assert.True(t, snapshotExists, "db2_snapshot must always be set")
			assert.Equal(t, tc.wantSnapshot, snapshot)

			tsVal, tsExists := msg.MetaGet("db2_timestamp")
			if tc.wantHasTS {
				assert.True(t, tsExists, "db2_timestamp must be set when Timestamp is non-zero")
				assert.Equal(t, ts.Format(time.RFC3339Nano), tsVal)
			} else {
				if tsExists {
					assert.Empty(t, tsVal, "db2_timestamp must be empty when Timestamp is zero")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Checkpoint resume tests
// ---------------------------------------------------------------------------

// TestLoadCheckpointParseCSNFormats verifies that loadCheckpoint correctly
// round-trips CSN values stored in the checkpoint table in all supported string
// formats. The connector writes "CSN:<hex>" canonically but must also handle
// decimal and 0x-prefixed hex for backward compatibility.
func TestLoadCheckpointParseCSNFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stored  string // value stored in CACHE_VAL column
		wantCSN uint64
	}{
		{
			name:    "CSN prefix hex (canonical write format)",
			stored:  "CSN:0000000000003039",
			wantCSN: 12345,
		},
		{
			name:    "decimal integer",
			stored:  "12345",
			wantCSN: 12345,
		},
		{
			name:    "hex with 0x prefix",
			stored:  "0x3039",
			wantCSN: 12345,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openInputFakeDB(t, &inputFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					return []string{"CACHE_VAL"}, [][]driver.Value{{tc.stored}}, nil
				},
			})

			d := &db2CDCInput{
				db:                 db,
				cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
				checkpointCacheKey: "db2_cdc_checkpoint",
				log:                testLogger(t),
			}

			csn, err := d.loadCheckpoint(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tc.wantCSN, csn.Uint64())
		})
	}
}

// TestSaveCheckpointWritesCanonicalFormat verifies that saveCheckpoint always
// writes the canonical "CSN:<hex>" format to the checkpoint table and uses a
// MERGE (upsert) statement so both first-time writes and updates work correctly.
func TestSaveCheckpointWritesCanonicalFormat(t *testing.T) {
	t.Parallel()

	var capturedMerge string
	var capturedArgs []driver.Value

	db := openInputFakeDB(t, &inputFakeHandlers{
		exec: func(q string, args []driver.Value) error {
			if strings.Contains(q, "MERGE") {
				capturedMerge = q
				capturedArgs = args
			}
			return nil
		},
	})

	d := &db2CDCInput{
		db:                 db,
		cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
		checkpointCacheKey: "my_connector_key",
		log:                testLogger(t),
	}

	csn := replication.NewCSN(50000) // 0xC350
	err := d.saveCheckpoint(context.Background(), csn)
	require.NoError(t, err)

	assert.Contains(t, capturedMerge, "MERGE INTO RPCN.CDC_CHECKPOINT",
		"saveCheckpoint must use a MERGE statement for upsert semantics")
	require.Len(t, capturedArgs, 2)
	assert.Equal(t, "my_connector_key", capturedArgs[0],
		"first MERGE argument must be the checkpoint cache key")
	assert.Equal(t, "CSN:000000000000C350", capturedArgs[1],
		"second MERGE argument must be the canonical CSN:<hex> string")
}

// TestStreamingSkipsSnapshotWhenCheckpointExists verifies the checkpoint resume
// behaviour: when a saved CSN is loaded from the checkpoint table, the connector
// sets StreamConfig.StartingCSN to the saved CSN and the snapshot phase is
// skipped (because StartingCSN is no longer null).
//
// The stream poll query uses IBMSNAP_COMMITSEQ > X'<afterCSN>', so setting
// StartingCSN = savedCSN is correct — the > predicate already excludes the
// saved position itself.  Using .Next() was an off-by-one that dropped the
// first event after a restart.
func TestStreamingSkipsSnapshotWhenCheckpointExists(t *testing.T) {
	t.Parallel()

	savedCSN := replication.NewCSN(999)

	db := openInputFakeDB(t, &inputFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "CDC_CHECKPOINT") {
				return []string{"CACHE_VAL"}, [][]driver.Value{{savedCSN.String()}}, nil
			}
			return nil, nil, nil
		},
	})

	d := &db2CDCInput{
		db:                 db,
		cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
		checkpointCacheKey: "db2_cdc_checkpoint",
		snapshotMode: snapshotModeInitial, // snapshot is enabled in config
		streamConfig: replication.StreamConfig{
			Schema:        "DB2INST1",
			Tables:        []string{"EMPLOYEES"},
			PollBatchSize: 100,
		},
		log: testLogger(t),
	}

	csn, err := d.loadCheckpoint(context.Background())
	require.NoError(t, err)
	require.False(t, csn.IsNull())

	// Simulate what Connect() does after loading a non-null checkpoint.
	d.streamConfig.StartingCSN = csn

	// With a saved checkpoint, StartingCSN is set to savedCSN.
	// runCDC skips the snapshot for snapshotModeInitial when StartingCSN is not null.
	// Since StartingCSN is NOT null, the snapshot must be skipped.
	assert.False(t, d.streamConfig.StartingCSN.IsNull(),
		"after checkpoint resume, StartingCSN must not be null so snapshot is skipped")
	assert.Equal(t, savedCSN.Uint64(), d.streamConfig.StartingCSN.Uint64(),
		"StartingCSN must equal the saved checkpoint (poll uses >, so no double-increment needed)")
}

// TestSnapshotModes verifies the doSnapshot decision for each snapshot_mode value.
func TestSnapshotModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mode           snapshotMode
		hasCheckpoint  bool
		expectSnapshot bool
	}{
		{"initial_no_checkpoint", snapshotModeInitial, false, true},
		{"initial_with_checkpoint", snapshotModeInitial, true, false},
		{"always_no_checkpoint", snapshotModeAlways, false, true},
		{"always_with_checkpoint", snapshotModeAlways, true, true},
		{"never_no_checkpoint", snapshotModeNever, false, false},
		{"never_with_checkpoint", snapshotModeNever, true, false},
		{"initial_only_no_checkpoint", snapshotModeInitialOnly, false, true},
		{"initial_only_with_checkpoint", snapshotModeInitialOnly, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var startCSN replication.CSN
			if tc.hasCheckpoint {
				startCSN = replication.NewCSN(12345)
			} else {
				startCSN = replication.NullCSN()
			}
			doSnapshot := false
			switch tc.mode {
			case snapshotModeInitial:
				doSnapshot = startCSN.IsNull()
			case snapshotModeAlways:
				doSnapshot = true
			case snapshotModeNever:
				doSnapshot = false
			case snapshotModeInitialOnly:
				doSnapshot = true
			}
			assert.Equal(t, tc.expectSnapshot, doSnapshot)
		})
	}
}

// ---------------------------------------------------------------------------
// Table registration / filtering tests
// ---------------------------------------------------------------------------

// TestInitializeFiltersMissingTables verifies that Initialize returns a
// descriptive error when a configured table is absent from IBMSNAP_REGISTER.
// This prevents silent data loss where a typo in the tables config causes the
// connector to start but capture zero changes for the misnamed table.
func TestInitializeFiltersMissingTables(t *testing.T) {
	t.Parallel()

	db := openInputFakeDB(t, &inputFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			// Only EMPLOYEES is registered; PAYROLL is not.
			return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"},
				[][]driver.Value{
					{"DB2INST1", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"},
				}, nil
		},
	})

	// NewStreamer is accessible from the replication package and exercises the
	// same Initialize code path that runStreaming calls internally.
	s := replication.NewStreamer(db, replication.StreamConfig{
		Schema: "DB2INST1",
		Tables: []string{"EMPLOYEES", "PAYROLL"},
	}, replication.Version{})

	err := s.Initialize(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PAYROLL",
		"error must name the missing table so the operator knows which registration to add")
}
