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
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/replication"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	db2CDCFieldDSN                      = "dsn"
	db2CDCFieldSchema                   = "schema"
	db2CDCFieldTables                   = "tables"
	db2CDCFieldCDCSchema                = "cdc_schema"
	db2CDCFieldStreamSnapshot           = "stream_snapshot"
	db2CDCFieldSnapshotMaxBatchSize     = "snapshot_max_batch_size"
	db2CDCFieldCheckpointMode           = "checkpoint_mode"
	db2CDCFieldCheckpointCache          = "checkpoint_cache"
	db2CDCFieldCheckpointCacheKey       = "checkpoint_cache_key"
	db2CDCFieldCheckpointCacheTableName = "checkpoint_cache_table_name"
	db2CDCFieldCheckpointLimit          = "checkpoint_limit"
	db2CDCFieldStreamBackoffInterval    = "stream_backoff_interval"
	db2CDCFieldPollBatchSize            = "poll_batch_size"

	// db2CDCEventChanBuf is the number of change events buffered between the
	// background CDC goroutine and ReadBatch. Sized to allow one poll batch to be
	// queued without back-pressure so that the CDC loop does not stall waiting for
	// the consumer.
	db2CDCEventChanBuf = 100
)

func db2CDCConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.80.0").
		Categories("Services").
		Summary("Consumes change data capture (CDC) events from IBM DB2 using SQL Replication.").
		Description(`Streams change events from IBM DB2 tables using SQL Replication (the ASNCDC schema).

This connector uses *pure Go* with `+"`github.com/ebitengine/purego`"+` for DB2 CLI bindings — no CGO or C toolchain required at build time.

== How it works

DB2 SQL Replication maintains a set of *change tables* (also called CD tables) alongside your source tables. A capture daemon reads the DB2 transaction log and writes INSERT, UPDATE, and DELETE operations into those tables. This input polls those change tables and converts each row into a Redpanda Connect message.

The connector operates in two phases:

1. *Snapshot* (when `+"`stream_snapshot: true`"+`): reads all existing rows from each monitored table inside a single `+"`REPEATABLE READ`"+` read-only transaction. The CDC position (CSN) is captured *before* the first row is read, so no changes are missed while the snapshot is in progress.
2. *Streaming*: polls each change table for new rows with `+"`IBMSNAP_COMMITSEQ`"+` greater than the last checkpoint. The maximum `+"`SYNCHPOINT`"+` from `+"`ASNCDC.IBMSNAP_REGISTER`"+` is used as an upper bound on every poll to avoid reading uncommitted rows.

== UPDATE representation

DB2 LUW SQL Replication encodes UPDATE operations as a DELETE record followed by an INSERT record that share the same `+"`IBMSNAP_COMMITSEQ`"+` value. This connector detects these pairs using window functions and merges them into a single `+"`op: u`"+` (update) event with the `+"`before`"+` field populated with the old row data and `+"`after`"+` with the new row data.

== Message format (Debezium-compatible)

The message body matches the https://debezium.io/documentation/reference/stable/connectors/db2.html[Debezium DB2 connector^] envelope format, making this connector a drop-in replacement for existing Debezium consumers.

`+"```json"+`
{
  "before": null,
  "after":  { "ID": 42, "NAME": "Alice", "SALARY": 75000 },
  "source": {
    "connector":  "db2",
    "name":       "db2_cdc",
    "schema":     "DB2ADMIN",
    "table":      "EMPLOYEES",
    "commit_lsn": "CSN:000000000000C350",
    "change_lsn": null,
    "snapshot":   "false",
    "ts_ms":      1705315845000
  },
  "op":    "c",
  "ts_ms": 1705315845000
}
`+"```"+`

`+"**`op` codes**"+` match Debezium: `+"`c`"+` = create/insert, `+"`u`"+` = update, `+"`d`"+` = delete, `+"`r`"+` = read (snapshot).

`+"**`before` field**"+`: Populated per Debezium semantics. For `+"`op: u`"+` (update) events, `+"`before`"+` contains the old row data and `+"`after`"+` contains the new row data. For `+"`op: d`"+` (delete) events, `+"`before`"+` contains the deleted row and `+"`after`"+` is `+"`null`"+`. For `+"`op: c`"+` (insert) and `+"`op: r`"+` (snapshot read) events, `+"`before`"+` is `+"`null`"+`.

`+"**`change_lsn`**"+`: DB2 LUW SQL Replication exposes only `+"`IBMSNAP_COMMITSEQ`"+` (the commit LSN). The intra-transaction LSN (`+"`change_lsn`"+` in Debezium) is not available and is always `+"`null`"+`.

The following metadata keys are set on every message:

- `+"`db2_schema`"+`: source table schema
- `+"`db2_table`"+`: source table name
- `+"`db2_operation`"+`: human-readable op — `+"`read`"+`, `+"`insert`"+`, `+"`update`"+`, or `+"`delete`"+`
- `+"`db2_op`"+`: Debezium op code — `+"`r`"+`, `+"`c`"+`, `+"`u`"+`, or `+"`d`"+`
- `+"`db2_csn`"+`: commit sequence number string (empty for snapshot events; backward-compat alias for `+"`db2_commit_lsn`"+`)
- `+"`db2_commit_lsn`"+`: same as `+"`db2_csn`"+` (Debezium field name)
- `+"`db2_connector`"+`: always `+"`db2`"+`
- `+"`db2_snapshot`"+`: `+"`true`"+` for snapshot rows, `+"`false`"+` for streaming rows
- `+"`db2_timestamp`"+`: `+"`IBMSNAP_LOGMARKER`"+` timestamp from the change table (RFC3339Nano; omitted for snapshot events)

== Prerequisites

1. IBM DB2 10.1 or later with SQL Replication installed.
2. The DB2 CLI shared library must be present at runtime:
   - Linux: `+"`libdb2.so.1`"+`
   - macOS: `+"`libdb2.dylib`"+`
   - Windows: `+"`db2cli.dll`"+`
3. SQL Replication must be configured and the capture daemon running:
`+"```sql"+`
-- Start the CDC capture daemon
CALL ASNCDC.ASNCDCSERVICES('start', 'asncdc');

-- Register each table you want to capture (repeat for each table)
CALL ASNCDC.ADDTABLE('MYSCHEMA', 'EMPLOYEES');
CALL ASNCDC.ADDTABLE('MYSCHEMA', 'ORDERS');
`+"```"+`

== Checkpoint persistence

The connector tracks the highest processed `+"`IBMSNAP_COMMITSEQ`"+` so it can resume after a restart without replaying already-delivered events. By default a `+"`RPCN.CDC_CHECKPOINT`"+` table is created in DB2 (the `+"`RPCN`"+` schema must already exist). Set `+"`checkpoint_cache`"+` to use an external https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] instead.
`).
		Fields(
			service.NewStringField(db2CDCFieldDSN).
				Description("DB2 connection string in the keyword=value format accepted by SQLDriverConnect. "+
					"Common keywords: `DATABASE` (database alias), `HOSTNAME`, `PORT` (default 50000), "+
					"`PROTOCOL` (`TCPIP`), `UID`, `PWD`. The DB2 CLI shared library must be installed "+
					"separately — this connector does not bundle it.").
				Example("DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"),

			service.NewStringField(db2CDCFieldSchema).
				Description("DB2 schema (TABSCHEMA) that owns the monitored tables. "+
					"Maps to SOURCE_OWNER in ASNCDC.IBMSNAP_REGISTER. "+
					"Case-insensitive — automatically normalized to uppercase.").
				Example("DB2ADMIN"),

			service.NewStringListField(db2CDCFieldTables).
				Description("List of table names (without schema prefix) to capture changes from. "+
					"Each table must already be registered with SQL Replication via ASNCDC.ADDTABLE before the connector starts. "+
					"The connector will fail at startup if any listed table is missing from ASNCDC.IBMSNAP_REGISTER.").
				Example([]string{"EMPLOYEES", "ORDERS"}),

			service.NewStringField(db2CDCFieldCDCSchema).
				Description("Schema that owns the SQL Replication control tables (IBMSNAP_REGISTER and the generated change tables). "+
					"Defaults to `ASNCDC`, which is the standard installation schema. "+
					"Change this only if SQL Replication was installed under a custom schema.").
				Default("ASNCDC").
				Advanced(),

			service.NewBoolField(db2CDCFieldStreamSnapshot).
				Description("When true, an initial full-table snapshot is performed before streaming CDC changes. "+
					"All existing rows are read inside a single `REPEATABLE READ` read-only transaction. "+
					"The CDC position (CSN) is captured before the first row is read so that no changes are missed during the snapshot. "+
					"Set to false to skip the snapshot and start streaming from the current DB2 log position.").
				Default(true),

			service.NewIntField(db2CDCFieldSnapshotMaxBatchSize).
				Description("Number of rows fetched per round-trip during the initial snapshot. "+
					"Rows are retrieved using keyset pagination (ordered by primary key), so reducing this value "+
					"lowers memory usage at the cost of more DB2 round-trips. "+
					"Increase for large tables with small rows to improve snapshot throughput.").
				Default(1000).
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointMode).
				Description("Checkpoint tracking mode. `auto` (default) detects the DB2 version and "+
					"selects CSN-based checkpointing on DB2 10.1+. Set to `csn` to require CSN "+
					"mode explicitly and fail fast on older DB2 versions.").
				Default("auto").
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointCache).
				Description("Name of a https://www.docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] to use for storing the checkpoint CSN. "+
					"If not set, the connector automatically creates and manages a `"+db2CDCFieldCheckpointCacheTableName+"` table in DB2. "+
					"Use an external cache (e.g. Redis) when you want checkpoint state to survive a DB2 rebuild "+
					"or when the connector user lacks DDL privileges.").
				Example("my_redis_cache").
				Optional().
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointCacheKey).
				Description("Key used to store and retrieve the checkpoint in the cache or DB2 checkpoint table. "+
					"Change this value if you run multiple DB2 CDC connectors that share the same cache resource "+
					"or the same DB2 instance, to avoid checkpoint collisions.").
				Default("db2_cdc_checkpoint").
				Advanced(),

			service.NewStringField(db2CDCFieldCheckpointCacheTableName).
				Description("Fully-qualified DB2 table name for the built-in checkpoint store. "+
					"The table is created automatically if it does not exist (SQLSTATE 42710 is silently ignored). "+
					"The connector will attempt to CREATE the schema if it does not already exist; "+
					"if the connector user lacks CREATE SCHEMA privileges, the schema must be created manually beforehand. "+
					"Only used when `"+db2CDCFieldCheckpointCache+"` is not set.").
				Default("RPCN.CDC_CHECKPOINT").
				Advanced(),

			service.NewIntField(db2CDCFieldCheckpointLimit).
				Description("Maximum number of messages that can be in-flight (sent but not yet acknowledged) at any one time. "+
					"The CSN checkpoint is only advanced once all messages at a given position are acknowledged, "+
					"preserving at-least-once delivery guarantees. "+
					"Increase this value to allow larger output batches and higher throughput; "+
					"decrease it to reduce the number of re-delivered messages after a restart.").
				Default(1024).
				Advanced(),

			service.NewDurationField(db2CDCFieldStreamBackoffInterval).
				Description("How long to wait between change-table polls when no new rows are found. "+
					"Note that the DB2 capture daemon itself introduces latency — reducing this below the capture "+
					"daemon's commit interval has no benefit. "+
					"Increase for low-traffic tables to reduce unnecessary DB2 queries.").
				Default("5s").
				Advanced(),

			service.NewIntField(db2CDCFieldPollBatchSize).
				Description("Maximum number of change rows fetched per change-table query. "+
					"Each poll issues one query per monitored table. "+
					"Reduce this value if individual change events are large (e.g. wide rows). "+
					"Increase it to improve throughput for high-velocity tables.").
				Default(1000).
				Advanced(),

			service.NewAutoRetryNacksToggleField(),
			service.NewBatchPolicyField("batching"),
		)
}

func init() {
	service.MustRegisterBatchInput("db2_cdc", db2CDCConfigSpec(), newDB2CDCInput)
}

//------------------------------------------------------------------------------

type db2CDCInput struct {
	db             *sql.DB
	dsn            string
	schema         string
	tables         []string
	asnCDCSchema   string
	streamSnapshot bool
	checkpointMode string

	checkpointCacheKey string
	checkpointLimit    int
	cpCacheName        string // external cache resource name; empty = use DB2 table
	cpCacheTableName   string

	snapshotConfig replication.SnapshotConfig
	streamConfig   replication.StreamConfig

	capped    *checkpoint.Capped[replication.CSN]
	eventChan chan replication.ChangeEvent
	errChan   chan error

	res     *service.Resources
	shutSig *shutdown.Signaller
	wg      sync.WaitGroup
	mu      sync.Mutex
	closed  bool

	version replication.Version
	log     *service.Logger
}

func newDB2CDCInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	dsn, err := conf.FieldString(db2CDCFieldDSN)
	if err != nil {
		return nil, err
	}

	schema, err := conf.FieldString(db2CDCFieldSchema)
	if err != nil {
		return nil, err
	}
	schema = strings.ToUpper(schema)
	if !isValidDB2Identifier(schema) {
		return nil, fmt.Errorf("schema %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", schema)
	}

	tables, err := conf.FieldStringList(db2CDCFieldTables)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, errors.New("at least one table must be specified")
	}
	for i, t := range tables {
		tables[i] = strings.ToUpper(t)
		if !isValidDB2Identifier(tables[i]) {
			return nil, fmt.Errorf("tables[%d] %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", i, t)
		}
	}

	asnCDCSchema, err := conf.FieldString(db2CDCFieldCDCSchema)
	if err != nil {
		return nil, err
	}
	asnCDCSchema = strings.ToUpper(asnCDCSchema)
	if !isValidDB2Identifier(asnCDCSchema) {
		return nil, fmt.Errorf("cdc_schema %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", asnCDCSchema)
	}

	streamSnapshot, err := conf.FieldBool(db2CDCFieldStreamSnapshot)
	if err != nil {
		return nil, err
	}

	snapshotMaxBatchSize, err := conf.FieldInt(db2CDCFieldSnapshotMaxBatchSize)
	if err != nil {
		return nil, err
	}
	if snapshotMaxBatchSize < 1 {
		return nil, errors.New("snapshot_max_batch_size must be at least 1")
	}

	checkpointMode, err := conf.FieldString(db2CDCFieldCheckpointMode)
	if err != nil {
		return nil, err
	}

	var cpCacheName string
	if conf.Contains(db2CDCFieldCheckpointCache) {
		if cpCacheName, err = conf.FieldString(db2CDCFieldCheckpointCache); err != nil {
			return nil, err
		}
	}

	checkpointCacheKey, err := conf.FieldString(db2CDCFieldCheckpointCacheKey)
	if err != nil {
		return nil, err
	}

	cpCacheTableName, err := conf.FieldString(db2CDCFieldCheckpointCacheTableName)
	if err != nil {
		return nil, err
	}
	cpCacheTableName = strings.ToUpper(cpCacheTableName)
	if err := validateQualifiedIdentifier(cpCacheTableName); err != nil {
		return nil, fmt.Errorf("checkpoint_cache_table_name %q: %w", cpCacheTableName, err)
	}

	checkpointLimit, err := conf.FieldInt(db2CDCFieldCheckpointLimit)
	if err != nil {
		return nil, err
	}

	streamBackoffInterval, err := conf.FieldDuration(db2CDCFieldStreamBackoffInterval)
	if err != nil {
		return nil, err
	}

	pollBatchSize, err := conf.FieldInt(db2CDCFieldPollBatchSize)
	if err != nil {
		return nil, err
	}
	if pollBatchSize < 1 {
		return nil, errors.New("poll_batch_size must be at least 1")
	}

	d := &db2CDCInput{
		dsn:                dsn,
		schema:             schema,
		tables:             tables,
		asnCDCSchema:       asnCDCSchema,
		streamSnapshot:     streamSnapshot,
		checkpointMode:     checkpointMode,
		checkpointCacheKey: checkpointCacheKey,
		checkpointLimit:    checkpointLimit,
		cpCacheName:        cpCacheName,
		cpCacheTableName:   cpCacheTableName,
		snapshotConfig: replication.SnapshotConfig{
			Schema:         schema,
			Tables:         tables,
			AsnCDCSchema:   asnCDCSchema,
			BatchSize:      snapshotMaxBatchSize,
			IsolationLevel: "REPEATABLE READ",
		},
		streamConfig: replication.StreamConfig{
			Schema:          schema,
			Tables:          tables,
			AsnCDCSchema:    asnCDCSchema,
			BackoffInterval: streamBackoffInterval,
			PollBatchSize:   pollBatchSize,
			StartingCSN:     replication.NullCSN(),
		},
		capped:    checkpoint.NewCapped[replication.CSN](int64(checkpointLimit)),
		eventChan: make(chan replication.ChangeEvent, db2CDCEventChanBuf),
		errChan:   make(chan error, 1),
		shutSig:   shutdown.NewSignaller(),
		res:       mgr,
		log:       mgr.Logger(),
	}

	batchInput, err := service.AutoRetryNacksBatchedToggled(conf, d)
	if err != nil {
		return nil, err
	}

	return conf.WrapBatchInputExtractTracingSpanMapping("db2_cdc", batchInput)
}

func (d *db2CDCInput) Connect(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db != nil {
		return nil
	}

	db, err := sql.Open("db2-cli", d.dsn)
	if err != nil {
		return fmt.Errorf("opening DB2 connection: %w", err)
	}

	// Limit to a single connection — CDC logic assumes stable connection state.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("pinging DB2: %w", err)
	}

	d.db = db

	version, err := d.detectVersion(ctx)
	if err != nil {
		d.db.Close()
		d.db = nil
		return fmt.Errorf("detecting DB2 version: %w", err)
	}
	d.version = version
	d.log.Infof("Connected to DB2 %s", version)

	actualMode, err := d.determineCheckpointMode()
	if err != nil {
		d.db.Close()
		d.db = nil
		return err
	}
	d.checkpointMode = actualMode

	// Initialize checkpoint persistence.
	if d.cpCacheName == "" {
		if err := d.initCheckpointTable(ctx); err != nil {
			d.db.Close()
			d.db = nil
			return fmt.Errorf("initializing checkpoint table: %w", err)
		}
		d.log.Infof("Using DB2 table %q for checkpoint persistence", d.cpCacheTableName)
	} else {
		d.log.Infof("Using external cache %q for checkpoint persistence", d.cpCacheName)
	}

	startingCSN, err := d.loadCheckpoint(ctx)
	if err != nil {
		d.db.Close()
		d.db = nil
		return fmt.Errorf("loading checkpoint: %w", err)
	}

	if !startingCSN.IsNull() {
		d.log.Infof("Resuming from checkpoint CSN: %s", startingCSN)
		// The stream poll query uses IBMSNAP_COMMITSEQ > X'<afterCSN>', so
		// setting StartingCSN = savedCSN resumes correctly from the next event.
		// Calling .Next() here was a double-increment that silently dropped
		// events at exactly checkpoint+1.
		d.streamConfig.StartingCSN = startingCSN
	} else {
		d.log.Info("No checkpoint found, starting from the beginning")
	}

	// Derive a long-lived context from the shutdown signaller, not from the Connect
	// ctx which callers may cancel once Connect returns.
	cdcCtx, _ := d.shutSig.SoftStopCtx(context.Background())

	d.wg.Add(1)
	go d.runCDC(cdcCtx)

	return nil
}

func (d *db2CDCInput) determineCheckpointMode() (string, error) {
	switch d.checkpointMode {
	case "csn":
		if !d.version.SupportsCDC() {
			return "", fmt.Errorf(
				"checkpoint_mode='csn' requires DB2 10.1+, found version %s; upgrade or use checkpoint_mode='auto'",
				d.version,
			)
		}
		d.log.Info("Using CSN checkpoint mode [EXPLICIT]")
		return "csn", nil

	case "auto":
		if d.version.AtLeast(10, 1) {
			d.log.Infof("Auto-detected CSN support (DB2 %s) — using CSN checkpoint mode", d.version)
			return "csn", nil
		}
		return "", fmt.Errorf("DB2 version %s does not support CSN (requires 10.1+); upgrade DB2", d.version)

	default:
		return "", fmt.Errorf("invalid checkpoint_mode %q: must be 'csn' or 'auto'", d.checkpointMode)
	}
}

func (d *db2CDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case event := <-d.eventChan:
		msg, err := d.eventToMessage(event)
		if err != nil {
			return nil, nil, err
		}

		// Track the CSN before delivering the message so the Capped checkpoint knows it is in-flight.
		var resolveFn func() *replication.CSN
		if !event.CSN.IsNull() {
			if resolveFn, err = d.capped.Track(ctx, event.CSN, 1); err != nil {
				return nil, nil, fmt.Errorf("tracking CSN: %w", err)
			}
		}

		ackFunc := func(ctx context.Context, ackErr error) error {
			if resolveFn == nil {
				return nil
			}
			// Always release the in-flight slot to avoid deadlocking the Capped
			// tracker. Only persist the checkpoint when the ack succeeded.
			if highestCSN := resolveFn(); highestCSN != nil && ackErr == nil {
				if err := d.saveCheckpoint(ctx, *highestCSN); err != nil {
					d.log.Warnf("failed to save checkpoint: %v", err)
				}
			}
			return nil
		}

		return service.MessageBatch{msg}, ackFunc, nil

	case err := <-d.errChan:
		return nil, nil, err

	case <-ctx.Done():
		return nil, nil, ctx.Err()

	case <-d.shutSig.SoftStopChan():
		return nil, nil, service.ErrEndOfInput
	}
}

func (d *db2CDCInput) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()

	d.shutSig.TriggerSoftStop()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		d.log.Warnf("context cancelled while waiting for CDC goroutines to stop: %v", ctx.Err())
		d.shutSig.TriggerHardStop()
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db != nil {
		if err := d.db.Close(); err != nil {
			d.log.Errorf("error closing DB2 connection: %v", err)
		}
		d.db = nil
	}

	return nil
}

// runCDC executes the two-phase CDC loop: initial snapshot (optional) → streaming.
func (d *db2CDCInput) runCDC(ctx context.Context) {
	defer d.wg.Done()

	if d.streamSnapshot && d.streamConfig.StartingCSN.IsNull() {
		d.log.Info("Starting snapshot phase")
		if err := d.runSnapshot(ctx); err != nil {
			if ctx.Err() == nil && !d.shutSig.IsSoftStopSignalled() {
				select {
				case d.errChan <- fmt.Errorf("snapshot failed: %w", err):
				default:
				}
			}
			return
		}
		d.log.Info("Snapshot complete")
	}

	d.log.Info("Starting streaming phase")
	if err := d.runStreaming(ctx); err != nil {
		if ctx.Err() == nil && !d.shutSig.IsSoftStopSignalled() {
			select {
			case d.errChan <- fmt.Errorf("streaming failed: %w", err):
			default:
			}
		}
	}
}

func (d *db2CDCInput) runSnapshot(ctx context.Context) error {
	snapshotter := replication.NewSnapshotter(d.db, d.snapshotConfig, d.version)

	handler := func(event replication.ChangeEvent) error {
		select {
		case d.eventChan <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-d.shutSig.SoftStopChan():
			return service.ErrEndOfInput
		}
	}

	startingCSN, err := snapshotter.Snapshot(ctx, handler)
	if err != nil {
		return err
	}

	d.streamConfig.StartingCSN = startingCSN
	d.log.Infof("Snapshot captured at CSN: %s", startingCSN)

	return nil
}

func (d *db2CDCInput) runStreaming(ctx context.Context) error {
	streamer := replication.NewStreamer(d.db, d.streamConfig, d.version)

	if err := streamer.Initialize(ctx); err != nil {
		return fmt.Errorf("initializing streamer: %w", err)
	}

	handler := func(event replication.ChangeEvent) error {
		select {
		case d.eventChan <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-d.shutSig.SoftStopChan():
			return service.ErrEndOfInput
		}
	}

	return streamer.Stream(ctx, handler)
}

// debeziumOp maps a replication.OpType to the single-character Debezium operation code
// used in the "op" field of the Debezium envelope and in the db2_op metadata key.
//
// Debezium codes: c=create (insert), u=update, d=delete, r=read (snapshot).
func debeziumOp(op replication.OpType) string {
	switch op {
	case replication.OpTypeInsert:
		return "c"
	case replication.OpTypeUpdate:
		return "u"
	case replication.OpTypeDelete:
		return "d"
	case replication.OpTypeRead:
		return "r"
	default:
		return string(op)
	}
}

// debeziumSnapshotValue returns the Debezium snapshot field value for the
// given event. Debezium uses "true" during a snapshot, "last" for the final
// snapshot event, and "false" (or absent) during streaming. Because this
// connector does not track which snapshot row is the last, all snapshot rows
// emit "true"; streaming rows emit "false".
func debeziumSnapshotValue(op replication.OpType) string {
	if op == replication.OpTypeRead {
		return "true"
	}
	return "false"
}

// eventToMessage converts a ChangeEvent to a Redpanda Connect message whose
// body matches the Debezium DB2 connector envelope format for drop-in
// compatibility with existing Debezium consumers.
//
// Envelope structure:
//
//	{
//	  "before":  null | { <column>: <value>, ... },
//	  "after":   null | { <column>: <value>, ... },
//	  "source":  { "connector": "db2", "name": "db2_cdc", "schema": "...",
//	               "table": "...", "commit_lsn": "CSN:...", "change_lsn": null,
//	               "snapshot": "true"|"false", "ts_ms": <epoch-ms>, ... },
//	  "op":      "c"|"u"|"d"|"r",
//	  "ts_ms":   <epoch-ms>
//	}
//
// The before/after fields follow Debezium semantics:
//   - INSERT (op="c"): before=null, after=new row
//   - UPDATE (op="u"): before=old row data, after=new row data
//   - DELETE (op="d"): before=deleted row data, after=null
//   - READ   (op="r"): before=null, after=snapshot row
//
// Metadata keys set on the message:
//
//   - db2_schema        — source table schema (TABSCHEMA)
//   - db2_table         — source table name
//   - db2_operation     — human-readable op: "read", "insert", "delete", "update"
//   - db2_op            — Debezium op code: "r", "c", "d", "u"
//   - db2_csn           — commit sequence number string (backward-compat alias)
//   - db2_commit_lsn    — same as db2_csn (Debezium naming)
//   - db2_connector     — always "db2"
//   - db2_snapshot      — "true" for snapshot rows, "false" for streaming rows
//   - db2_timestamp     — IBMSNAP_LOGMARKER timestamp (RFC3339Nano; omitted if zero)
func (*db2CDCInput) eventToMessage(event replication.ChangeEvent) (*service.Message, error) {
	// Determine source timestamp in epoch-milliseconds.
	var tsMs int64
	if !event.Timestamp.IsZero() {
		tsMs = event.Timestamp.UnixMilli()
	}

	// Build the source block to match Debezium's SourceInfo struct.
	// change_lsn is not tracked separately by DB2 LUW SQL Replication
	// (it only exposes IBMSNAP_COMMITSEQ = commit_lsn), so it is null.
	source := map[string]any{
		"connector":  "db2",
		"name":       "db2_cdc",
		"schema":     event.Schema,
		"table":      event.Table,
		"commit_lsn": event.CSN.String(), // "" for snapshot rows (NullCSN)
		"change_lsn": nil,                // not available from DB2 LUW SQL Replication
		"snapshot":   debeziumSnapshotValue(event.Operation),
		"ts_ms":      tsMs,
	}

	// Populate before/after fields per Debezium semantics:
	//   INSERT (c): before=null,            after=new row
	//   UPDATE (u): before=old row data,    after=new row data
	//   DELETE (d): before=deleted row data, after=null
	//   READ   (r): before=null,            after=snapshot row
	var before any
	var after any
	switch event.Operation {
	case replication.OpTypeDelete:
		before = event.Data
		after = nil
	case replication.OpTypeUpdate:
		before = event.BeforeData
		after = event.Data
	default:
		before = nil
		after = event.Data
	}

	envelope := map[string]any{
		"before": before,
		"after":  after,
		"source": source,
		"op":     debeziumOp(event.Operation),
		"ts_ms":  tsMs,
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(envelope)

	// Metadata — standard Debezium-named keys plus backward-compat aliases.
	msg.MetaSetMut("db2_schema", event.Schema)
	msg.MetaSetMut("db2_table", event.Table)
	msg.MetaSetMut("db2_operation", string(event.Operation))
	msg.MetaSetMut("db2_op", debeziumOp(event.Operation))
	csnStr := event.CSN.String()
	msg.MetaSetMut("db2_csn", csnStr)        // backward-compat alias
	msg.MetaSetMut("db2_commit_lsn", csnStr) // Debezium naming
	msg.MetaSetMut("db2_connector", "db2")
	msg.MetaSetMut("db2_snapshot", debeziumSnapshotValue(event.Operation))
	if !event.Timestamp.IsZero() {
		msg.MetaSetMut("db2_timestamp", event.Timestamp.Format(time.RFC3339Nano))
	}

	return msg, nil
}

// detectVersion queries DB2 for its version string.
func (d *db2CDCInput) detectVersion(ctx context.Context) (replication.Version, error) {
	var versionStr string
	err := d.db.QueryRowContext(ctx,
		"SELECT SERVICE_LEVEL FROM SYSIBMADM.ENV_INST_INFO FETCH FIRST 1 ROW ONLY",
	).Scan(&versionStr)
	if err != nil {
		// Fallback for older DB2 versions.
		if err2 := d.db.QueryRowContext(ctx, "VALUES (PROD_RELEASE)").Scan(&versionStr); err2 != nil {
			return replication.Version{}, fmt.Errorf("detecting DB2 version (ENV_INST_INFO: %v; PROD_RELEASE: %w)", err, err2)
		}
	}

	return replication.ParseVersion(versionStr)
}

// initCheckpointTable creates the checkpoint table in DB2 if it does not exist.
func (d *db2CDCInput) initCheckpointTable(ctx context.Context) error {
	// Try to create the schema (derived from the table name); ignore "already exists".
	if parts := strings.SplitN(d.cpCacheTableName, ".", 2); len(parts) == 2 {
		_, _ = d.db.ExecContext(ctx, "CREATE SCHEMA "+parts[0])
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			CACHE_KEY VARCHAR(255) NOT NULL,
			CACHE_VAL VARCHAR(255) NOT NULL,
			PRIMARY KEY (CACHE_KEY)
		)`, d.cpCacheTableName)

	_, err := d.db.ExecContext(ctx, createSQL)
	if err != nil && !isAlreadyExistsError(err) {
		return fmt.Errorf("create checkpoint table %s: %w", d.cpCacheTableName, err)
	}

	return nil
}

// loadCheckpoint reads the last saved CSN from the checkpoint store.
func (d *db2CDCInput) loadCheckpoint(ctx context.Context) (replication.CSN, error) {
	if d.cpCacheName != "" {
		var cacheVal []byte
		var cacheErr error
		if err := d.res.AccessCache(ctx, d.cpCacheName, func(c service.Cache) {
			cacheVal, cacheErr = c.Get(ctx, d.checkpointCacheKey)
		}); err != nil {
			return replication.NullCSN(), fmt.Errorf("accessing checkpoint cache: %w", err)
		}
		if errors.Is(cacheErr, service.ErrKeyNotFound) {
			return replication.NullCSN(), nil
		}
		if cacheErr != nil {
			return replication.NullCSN(), fmt.Errorf("reading checkpoint: %w", cacheErr)
		}
		return replication.ParseCSN(string(cacheVal))
	}

	var cacheVal string
	err := d.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT CACHE_VAL FROM %s WHERE CACHE_KEY = ?", d.cpCacheTableName),
		d.checkpointCacheKey,
	).Scan(&cacheVal)

	if errors.Is(err, sql.ErrNoRows) {
		return replication.NullCSN(), nil
	}
	if err != nil {
		return replication.NullCSN(), fmt.Errorf("loading checkpoint from DB2 table: %w", err)
	}

	return replication.ParseCSN(cacheVal)
}

// saveCheckpoint persists the highest fully-processed CSN to the checkpoint store.
func (d *db2CDCInput) saveCheckpoint(ctx context.Context, csn replication.CSN) error {
	if d.cpCacheName != "" {
		var cacheErr error
		if err := d.res.AccessCache(ctx, d.cpCacheName, func(c service.Cache) {
			cacheErr = c.Set(ctx, d.checkpointCacheKey, []byte(csn.String()), nil)
		}); err != nil {
			return fmt.Errorf("accessing checkpoint cache: %w", err)
		}
		return cacheErr
	}

	// DB2 MERGE — upsert into checkpoint table.
	// Copy d.db under the mutex to avoid a race with Close, which sets d.db = nil.
	d.mu.Lock()
	db := d.db
	d.mu.Unlock()
	if db == nil {
		return nil
	}

	mergeSQL := fmt.Sprintf(`
		MERGE INTO %s AS T
		USING (VALUES (?, ?)) AS S(CACHE_KEY, CACHE_VAL)
		ON T.CACHE_KEY = S.CACHE_KEY
		WHEN MATCHED THEN UPDATE SET T.CACHE_VAL = S.CACHE_VAL
		WHEN NOT MATCHED THEN INSERT (CACHE_KEY, CACHE_VAL) VALUES (S.CACHE_KEY, S.CACHE_VAL)
	`, d.cpCacheTableName)

	_, err := db.ExecContext(ctx, mergeSQL, d.checkpointCacheKey, csn.String())
	if err != nil {
		return fmt.Errorf("saving checkpoint to DB2 table: %w", err)
	}

	return nil
}

// isAlreadyExistsError returns true for DB2 SQLSTATE 42710 (object already exists).
// The substring "42710" can appear in many places; we only want to match the
// SQLSTATE token specifically.
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLSTATE=42710") || strings.Contains(msg, "SQLSTATE 42710")
}

// isValidDB2Identifier returns true if s contains only uppercase letters, digits, and
// underscores — the character set required for DB2 schema/table identifiers that are
// safe to embed directly in SQL strings without quoting or parameter binding.
func isValidDB2Identifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

// validateQualifiedIdentifier validates a possibly-qualified identifier of the form
// "TABLE" or "SCHEMA.TABLE". Each part must satisfy isValidDB2Identifier.
func validateQualifiedIdentifier(s string) error {
	parts := strings.SplitN(s, ".", 2)
	for _, part := range parts {
		if !isValidDB2Identifier(part) {
			return fmt.Errorf("identifier part %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", part)
		}
	}
	return nil
}
