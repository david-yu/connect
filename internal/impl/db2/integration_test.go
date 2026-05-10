// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	// Register the db2_cdc input.
	_ "github.com/redpanda-data/connect/v4/internal/impl/db2"
	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2test"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// TestIntegrationDB2CDCDriver tests basic SQL driver connectivity and query execution.
func TestIntegrationDB2CDCDriver(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Drop from any previous run (ignore "not found" errors).
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.INTEGRATION_TEST`)

	// Create a test table and insert rows.
	db.MustExecContext(ctx, `
		CREATE TABLE DB2INST1.INTEGRATION_TEST (
			ID   INTEGER NOT NULL PRIMARY KEY,
			NAME VARCHAR(100)
		)
	`)

	for i := 1; i <= 10; i++ {
		db.MustExecContext(ctx,
			"INSERT INTO DB2INST1.INTEGRATION_TEST (ID, NAME) VALUES (?, ?)",
			i, fmt.Sprintf("row-%d", i),
		)
	}

	// Query rows back and verify the driver returns correct data.
	rows, err := db.QueryContext(ctx, "SELECT ID, NAME FROM DB2INST1.INTEGRATION_TEST ORDER BY ID")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var id int
		var name string
		require.NoError(t, rows.Scan(&id, &name))
		t.Logf("row: id=%d name=%s", id, name)
		assert.Equal(t, count+1, id)
		assert.Equal(t, fmt.Sprintf("row-%d", count+1), name)
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 10, count)
}

// TestIntegrationDB2CDCSnapshotAndStreaming verifies the full snapshot → streaming flow.
func TestIntegrationDB2CDCSnapshotAndStreaming(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Drop all CDC objects from any previous run (ignore "not found" errors).
	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_CDC_TEST_EMPLOYEES"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CDC_TEST_EMPLOYEES`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CDC_CHECKPOINT`)

	// Create the test table.
	db.MustExecContext(ctx, `
		CREATE TABLE DB2INST1.CDC_TEST_EMPLOYEES (
			EMP_ID   INTEGER NOT NULL PRIMARY KEY,
			EMP_NAME VARCHAR(100)
		)
	`)

	// Pre-populate for snapshot.
	for i := 1; i <= 5; i++ {
		db.MustExecContext(ctx,
			"INSERT INTO DB2INST1.CDC_TEST_EMPLOYEES (EMP_ID, EMP_NAME) VALUES (?, ?)",
			i, fmt.Sprintf("employee-%d", i),
		)
	}

	// Enable ASNCDC capture on this table.
	db.EnableASNCDC("DB2INST1", []string{"CDC_TEST_EMPLOYEES"})

	// Build and run the connector.
	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["CDC_TEST_EMPLOYEES"]
  stream_snapshot: true
  snapshot_max_batch_size: 10
  poll_batch_size: 100
  stream_backoff_interval: 500ms
  checkpoint_cache_table_name: "DB2INST1.CDC_CHECKPOINT"
`, db.DSN)

	var (
		received   []string
		receivedMu sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		for _, msg := range batch {
			b, err := msg.AsBytes()
			if err != nil {
				continue
			}
			op, _ := msg.MetaGet("db2_operation")
			table, _ := msg.MetaGet("db2_table")
			csn, _ := msg.MetaGet("db2_csn")
			t.Logf("CDC event [%s] %s csn=%s: %s", op, table, csn, b)
			received = append(received, string(b))
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream.Run(ctx)
	}()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	// Wait for snapshot rows (5 pre-populated rows).
	assert.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		return len(received) >= 5
	}, 2*time.Minute, 500*time.Millisecond, "snapshot: expected at least 5 events")

	// Log CT table and register state before streaming inserts.
	{
		var ctCount int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ASNCDC."CDC_DB2INST1_CDC_TEST_EMPLOYEES"`).Scan(&ctCount)
		var synchHex []byte
		_ = db.QueryRowContext(ctx,
			"SELECT SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'",
		).Scan(&synchHex)
		t.Logf("before streaming inserts: CT_count=%d synchpoint=%X asncap_pid=%s",
			ctCount, synchHex, asncapPID())
	}

	// Insert streaming rows while the connector is running.
	for i := 6; i <= 10; i++ {
		db.MustExecContext(ctx,
			"INSERT INTO DB2INST1.CDC_TEST_EMPLOYEES (EMP_ID, EMP_NAME) VALUES (?, ?)",
			i, fmt.Sprintf("employee-%d", i),
		)
	}

	// Log CT table state a few seconds after inserts — if asncap is working,
	// the CT table should have rows by now.
	time.Sleep(5 * time.Second)
	{
		var ctCount int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ASNCDC."CDC_DB2INST1_CDC_TEST_EMPLOYEES"`).Scan(&ctCount)
		var synchHex []byte
		_ = db.QueryRowContext(ctx,
			"SELECT SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='CDC_TEST_EMPLOYEES'",
		).Scan(&synchHex)
		t.Logf("5s after streaming inserts: CT_count=%d synchpoint=%X asncap_pid=%s",
			ctCount, synchHex, asncapPID())
		if ctCount == 0 {
			t.Logf("WARNING: CT table is empty — asncap may not be capturing; printing asncap log")
			if logOut, err := os.ReadFile("/tmp/asncap.log"); err == nil && len(logOut) > 0 {
				tail := logOut
				if len(tail) > 3000 {
					tail = tail[len(tail)-3000:]
				}
				t.Logf("asncap log:\n%s", tail)
			}
		}
	}

	// Wait for streaming rows (5 snapshot + 5 streaming = 10 total).
	assert.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		return len(received) >= 10
	}, 2*time.Minute, 500*time.Millisecond, "streaming: expected at least 10 events total")
}

// asncapPID returns the PID of the running asncap process for TESTDB, or "none".
func asncapPID() string {
	out, _ := exec.Command("pgrep", "-f", "capture_server=TESTDB").Output()
	pid := strings.TrimSpace(string(out))
	if pid == "" {
		return "none"
	}
	return pid
}

// TestIntegrationDB2CDCUpdateBeforeImage verifies that UPDATE operations
// produce a single event with op="u", before=old row, after=new row.
func TestIntegrationDB2CDCUpdateBeforeImage(t *testing.T) {
	integration.CheckSkip(t)

	db := db2test.SetupTest(t)
	ctx := t.Context()

	// Cleanup from any previous run.
	_, _ = db.ExecContext(ctx, `DROP TABLE ASNCDC."CDC_DB2INST1_CDC_UPDATE_TEST"`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_PRUNCNTL WHERE SOURCE_TABLE='CDC_UPDATE_TEST'`)
	_, _ = db.ExecContext(ctx, `DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_TABLE='CDC_UPDATE_TEST'`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CDC_UPDATE_TEST`)
	_, _ = db.ExecContext(ctx, `DROP TABLE DB2INST1.CDC_UPDATE_CHECKPOINT`)

	db.MustExecContext(ctx, `CREATE TABLE DB2INST1.CDC_UPDATE_TEST (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(100))`)
	db.MustExecContext(ctx, `INSERT INTO DB2INST1.CDC_UPDATE_TEST (ID, NAME) VALUES (1, 'original')`)
	db.EnableASNCDC("DB2INST1", []string{"CDC_UPDATE_TEST"})

	connectorYAML := fmt.Sprintf(`
db2_cdc:
  dsn: %q
  schema: "DB2INST1"
  tables: ["CDC_UPDATE_TEST"]
  stream_snapshot: false
  poll_batch_size: 100
  stream_backoff_interval: 200ms
  checkpoint_cache_table_name: "DB2INST1.CDC_UPDATE_CHECKPOINT"
`, db.DSN)

	type cdcEvent struct {
		op     string
		before map[string]any
		after  map[string]any
	}
	var (
		received   []cdcEvent
		receivedMu sync.Mutex
	)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddInputYAML(connectorYAML))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		for _, msg := range batch {
			b, err := msg.AsBytes()
			if err != nil {
				continue
			}
			t.Logf("CDC event: %s", b)
			var env map[string]any
			if jsonErr := json.Unmarshal(b, &env); jsonErr != nil {
				continue
			}
			op, _ := env["op"].(string)
			var before, after map[string]any
			if v, ok := env["before"]; ok && v != nil {
				before, _ = v.(map[string]any)
			}
			if v, ok := env["after"]; ok && v != nil {
				after, _ = v.(map[string]any)
			}
			received = append(received, cdcEvent{op: op, before: before, after: after})
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- stream.Run(ctx)
	}()
	t.Cleanup(func() {
		if err := stream.StopWithin(10 * time.Second); err != nil {
			t.Logf("stream stop: %v", err)
		}
		if err := <-streamDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	})

	// Let streaming stabilize.
	time.Sleep(5 * time.Second)

	// Perform an UPDATE — DB2 SQL Replication captures this as a D+I pair in the CD table.
	db.MustExecContext(ctx, `UPDATE DB2INST1.CDC_UPDATE_TEST SET NAME = 'updated' WHERE ID = 1`)

	assert.Eventually(t, func() bool {
		receivedMu.Lock()
		defer receivedMu.Unlock()
		for _, e := range received {
			if e.op == "u" && e.before != nil && e.after != nil {
				name, _ := e.before["NAME"].(string)
				newName, _ := e.after["NAME"].(string)
				return name == "original" && newName == "updated"
			}
		}
		return false
	}, 2*time.Minute, 500*time.Millisecond,
		"expected UPDATE event with op='u', before.NAME='original', after.NAME='updated'")
}
