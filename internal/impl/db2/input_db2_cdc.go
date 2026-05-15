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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/confx"
	"github.com/redpanda-data/connect/v4/internal/impl/db2/replication"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	db2CDCFieldDSN                      = "dsn"
	db2CDCFieldSchema                   = "schema"
	db2CDCFieldTables                   = "tables"
	db2CDCFieldTableIncludeRegex        = "table_include_regex"
	db2CDCFieldTableExcludeRegex        = "table_exclude_regex"
	db2CDCFieldCDCSchema                = "cdc_schema"
	db2CDCFieldSnapshotMode             = "snapshot_mode"
	db2CDCFieldSnapshotMaxBatchSize     = "snapshot_max_batch_size"
	db2CDCFieldCheckpointMode           = "checkpoint_mode"
	db2CDCFieldCheckpointCache          = "checkpoint_cache"
	db2CDCFieldCheckpointCacheKey       = "checkpoint_cache_key"
	db2CDCFieldCheckpointCacheTableName = "checkpoint_cache_table_name"
	db2CDCFieldCheckpointLimit          = "checkpoint_limit"
	db2CDCFieldStreamBackoffInterval    = "stream_backoff_interval"
	db2CDCFieldPollBatchSize            = "poll_batch_size"
	db2CDCFieldHeartbeatInterval        = "heartbeat_interval"
	db2CDCFieldEmitSchemaChanges        = "emit_schema_changes"
	db2CDCFieldSignalTable              = "signal_table"

	// db2CDCEventChanBuf sizes the event channel. A larger buffer allows the
	// streamer to run ahead of ReadBatch consumers; drain logic caps batch size
	// at PollBatchSize so this needs to hold at least one full poll round.
	db2CDCEventChanBuf = 1000
)

type snapshotMode string

const (
	snapshotModeInitial     snapshotMode = "initial"
	snapshotModeAlways      snapshotMode = "always"
	snapshotModeNever       snapshotMode = "never"
	snapshotModeInitialOnly snapshotMode = "initial_only"
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

1. *Snapshot* (when `+"`snapshot_mode: initial`"+` or `+"`always`"+`): reads all existing rows from each monitored table inside a single `+"`REPEATABLE READ`"+` read-only transaction. The CDC position (CSN) is captured *before* the first row is read, so no changes are missed while the snapshot is in progress.
2. *Streaming*: polls each change table for new rows with `+"`IBMSNAP_COMMITSEQ`"+` greater than the last checkpoint. The maximum `+"`SYNCHPOINT`"+` from `+"`ASNCDC.IBMSNAP_REGISTER`"+` is used as an upper bound on every poll to avoid reading uncommitted rows.

== UPDATE representation

DB2 LUW SQL Replication encodes UPDATE operations as a DELETE record followed by an INSERT record that share the same `+"`IBMSNAP_COMMITSEQ`"+` value. This connector detects these pairs using LEAD/LAG window functions (ported from the Debezium DB2 connector) and merges them into a single `+"`op: u`"+` (update) event. The `+"`before`"+` field contains the old row data and `+"`after`"+` contains the new row data.

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

`+"**`op` codes**"+` match Debezium: `+"`c`"+` = create/insert, `+"`u`"+` = update, `+"`d`"+` = delete, `+"`r`"+` = read (snapshot), `+"`hb`"+` = heartbeat, `+"`schema_change`"+` = schema change.

`+"**`before` field**"+`: Populated per Debezium semantics. For `+"`op: u`"+` (update) events, `+"`before`"+` contains the old row data and `+"`after`"+` contains the new row data. For `+"`op: d`"+` (delete) events, `+"`before`"+` contains the deleted row and `+"`after`"+` is `+"`null`"+`. For `+"`op: c`"+` (insert) and `+"`op: r`"+` (snapshot read) events, `+"`before`"+` is `+"`null`"+`.

`+"**`change_lsn`**"+`: DB2 LUW SQL Replication exposes only `+"`IBMSNAP_COMMITSEQ`"+` (the commit LSN). The intra-transaction LSN (`+"`change_lsn`"+` in Debezium) is not available and is always `+"`null`"+`.

The following metadata keys are set on every message:

- `+"`db2_schema`"+`: source table schema
- `+"`db2_table`"+`: source table name
- `+"`db2_operation`"+`: human-readable op — `+"`read`"+`, `+"`insert`"+`, `+"`update`"+`, `+"`delete`"+`, `+"`heartbeat`"+`, or `+"`schema_change`"+`
- `+"`db2_op`"+`: Debezium op code — `+"`r`"+`, `+"`c`"+`, `+"`u`"+`, `+"`d`"+`, `+"`hb`"+`, or `+"`schema_change`"+`
- `+"`db2_csn`"+`: commit sequence number string (empty for snapshot events; backward-compat alias for `+"`db2_commit_lsn`"+`)
- `+"`db2_commit_lsn`"+`: same as `+"`db2_csn`"+` (Debezium field name)
- `+"`db2_connector`"+`: always `+"`db2`"+`
- `+"`db2_snapshot`"+`: `+"`true`"+` for snapshot rows, `+"`false`"+` for streaming rows
- `+"`db2_timestamp`"+`: `+"`IBMSNAP_LOGMARKER`"+` timestamp from the change table (RFC3339Nano; omitted for snapshot events)

== Prerequisites

1. IBM DB2 10.1 or later with SQL Replication installed.
2. The DB2 CLI shared library must be present at runtime:
   - Linux: `+"`libdb2.so.1`"+` (from the IBM Data Server Driver package)
   - macOS: `+"`libdb2.dylib`"+`
   - Windows: `+"`db2cli.dll`"+`

=== Installing on Debian/Ubuntu

Download the IBM Data Server Driver Package (dsdriver) from the IBM support site and run:

`+"```sh"+`
tar xzf ibm_data_server_driver_package_linuxx64.tar.gz
cd dsdriver && bash installDSDriver
export LD_LIBRARY_PATH=/opt/ibm/dsdriver/lib:$LD_LIBRARY_PATH
`+"```"+`

=== Installing on RHEL/CentOS

`+"```sh"+`
rpm -ivh ibm-datasrvrmgr-*.rpm
export LD_LIBRARY_PATH=/opt/ibm/dsdriver/lib:$LD_LIBRARY_PATH
`+"```"+`

=== Kubernetes init container

`+"```yaml"+`
initContainers:
  - name: install-db2-driver
    image: ibmcom/db2:11.5.9.0
    command: ["/bin/sh", "-c", "cp /opt/ibm/db2/V11.5/lib64/libdb2.so.1 /shared/lib/"]
    volumeMounts:
      - name: db2-lib
        mountPath: /shared/lib
volumes:
  - name: db2-lib
    emptyDir: {}
`+"```"+`

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
					"The connector will fail at startup if any listed table is missing from ASNCDC.IBMSNAP_REGISTER. "+
					"When empty, all CDC-registered tables in the schema are discovered dynamically from ASNCDC.IBMSNAP_REGISTER.").
				Example([]string{"EMPLOYEES", "ORDERS"}).
				Optional(),

			service.NewStringListField(db2CDCFieldTableIncludeRegex).
				Description("Optional list of regular expressions; only tables whose names match at least one pattern are captured. "+
					"Applied after `tables`. When `tables` is empty, all CDC-registered tables in the schema are discovered first and this filter narrows the set. "+
					"Patterns are matched against the bare table name (without schema prefix).").
				Example([]string{"^EMP", "^ORDER"}).
				Optional().
				Advanced(),

			service.NewStringListField(db2CDCFieldTableExcludeRegex).
				Description("Optional list of regular expressions; tables whose names match any pattern are excluded from capture. "+
					"Applied after `table_include_regex`.").
				Example([]string{"_TEST$", "_STAGING$"}).
				Optional().
				Advanced(),

			service.NewStringField(db2CDCFieldCDCSchema).
				Description("Schema that owns the SQL Replication control tables (IBMSNAP_REGISTER and the generated change tables). "+
					"Defaults to `ASNCDC`, which is the standard installation schema. "+
					"Change this only if SQL Replication was installed under a custom schema.").
				Default("ASNCDC").
				Advanced(),

			service.NewStringEnumField(db2CDCFieldSnapshotMode,
				string(snapshotModeInitial),
				string(snapshotModeAlways),
				string(snapshotModeNever),
				string(snapshotModeInitialOnly)).
				Description("Controls when an initial full-table snapshot is performed:\n\n"+
					"- `initial` (default): snapshot only on first start (when no checkpoint exists). Skipped on restart.\n"+
					"- `always`: snapshot on every start, regardless of existing checkpoint.\n"+
					"- `never`: skip snapshot; start streaming from the current log position.\n"+
					"- `initial_only`: snapshot then stop (no streaming phase).").
				Default(string(snapshotModeInitial)),

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

			service.NewDurationField(db2CDCFieldHeartbeatInterval).
				Description("When set to a positive duration, a heartbeat message (`op=hb`) is emitted at this interval "+
					"even when no CDC changes are available. "+
					"Use this to keep downstream consumers alive on low-traffic tables. "+
					"Set to 0 to disable (default).").
				Default("0s").
				Advanced(),

			service.NewBoolField(db2CDCFieldEmitSchemaChanges).
				Description("When true, emit a schema change event (`op=schema_change`) whenever a new table is added to SQL Replication "+
					"(i.e., when `ASNCDC.ADDTABLE` is called while the connector is running). "+
					"The connector polls `ASNCDC.IBMSNAP_REGISTER` for new entries on each backoff interval.").
				Default(false).
				Advanced(),

			service.NewStringField(db2CDCFieldSignalTable).
				Description("Fully-qualified DB2 table name for the signal channel (e.g. `DB2INST1.CDC_SIGNALS`). "+
					"When set, the connector polls this table for `execute-snapshot` signals. "+
					"An `execute-snapshot` signal triggers an ad-hoc snapshot of the specified tables without restarting the connector. "+
					"Create the table with: "+
					"`CREATE TABLE <schema>.<table> (ID VARCHAR(255) NOT NULL, TYPE VARCHAR(64) NOT NULL, DATA VARCHAR(2048), PRIMARY KEY (ID))`").
				Optional().
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
	auxDB          *sql.DB // dedicated connection for signals, schema changes, incremental snapshots
	dsn            string
	schema         string
	tables         []string
	tableFilter    *confx.RegexpFilter
	asnCDCSchema   string
	snapshotMode   snapshotMode
	checkpointMode string

	checkpointCacheKey string
	checkpointLimit    int
	cpCacheName        string // external cache resource name; empty = use DB2 table
	cpCacheTableName   string

	heartbeatInterval time.Duration
	emitSchemaChanges bool
	lastSeenSynchCSN  replication.CSN
	signalTable       string

	snapshotConfig replication.SnapshotConfig
	streamConfig   replication.StreamConfig

	capped    *checkpoint.Capped[replication.CSN]
	eventChan chan replication.ChangeEvent
	errChan   chan error

	res       *service.Resources
	shutSig   *shutdown.Signaller
	cdcCancel context.CancelFunc // cancels cdcCtx on hard stop
	wg        sync.WaitGroup
	mu        sync.Mutex
	closed    bool

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

	var tables []string
	if conf.Contains(db2CDCFieldTables) {
		if tables, err = conf.FieldStringList(db2CDCFieldTables); err != nil {
			return nil, err
		}
	}
	for i, t := range tables {
		tables[i] = strings.ToUpper(t)
		if !isValidDB2Identifier(tables[i]) {
			return nil, fmt.Errorf("tables[%d] %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", i, t)
		}
	}

	var includePatterns, excludePatterns []string
	if conf.Contains(db2CDCFieldTableIncludeRegex) {
		if includePatterns, err = conf.FieldStringList(db2CDCFieldTableIncludeRegex); err != nil {
			return nil, err
		}
	}
	if conf.Contains(db2CDCFieldTableExcludeRegex) {
		if excludePatterns, err = conf.FieldStringList(db2CDCFieldTableExcludeRegex); err != nil {
			return nil, err
		}
	}

	tableIncludes, err := confx.ParseRegexpPatterns(includePatterns)
	if err != nil {
		return nil, fmt.Errorf("table_include_regex: %w", err)
	}
	tableExcludes, err := confx.ParseRegexpPatterns(excludePatterns)
	if err != nil {
		return nil, fmt.Errorf("table_exclude_regex: %w", err)
	}

	tableFilter := &confx.RegexpFilter{Include: tableIncludes, Exclude: tableExcludes}

	if len(tables) == 0 && len(includePatterns) == 0 {
		return nil, errors.New("either tables or table_include_regex must be specified")
	}

	asnCDCSchema, err := conf.FieldString(db2CDCFieldCDCSchema)
	if err != nil {
		return nil, err
	}
	asnCDCSchema = strings.ToUpper(asnCDCSchema)
	if !isValidDB2Identifier(asnCDCSchema) {
		return nil, fmt.Errorf("cdc_schema %q contains invalid characters: only uppercase letters, digits, and underscores are allowed", asnCDCSchema)
	}

	snapshotModeStr, err := conf.FieldString(db2CDCFieldSnapshotMode)
	if err != nil {
		return nil, err
	}
	mode := snapshotMode(snapshotModeStr)

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
	if streamBackoffInterval <= 0 {
		return nil, fmt.Errorf("stream_backoff_interval must be positive, got %v", streamBackoffInterval)
	}

	pollBatchSize, err := conf.FieldInt(db2CDCFieldPollBatchSize)
	if err != nil {
		return nil, err
	}
	if pollBatchSize < 1 {
		return nil, errors.New("poll_batch_size must be at least 1")
	}

	heartbeatInterval, err := conf.FieldDuration(db2CDCFieldHeartbeatInterval)
	if err != nil {
		return nil, err
	}

	emitSchemaChanges, err := conf.FieldBool(db2CDCFieldEmitSchemaChanges)
	if err != nil {
		return nil, err
	}

	var signalTable string
	if conf.Contains(db2CDCFieldSignalTable) {
		if signalTable, err = conf.FieldString(db2CDCFieldSignalTable); err != nil {
			return nil, err
		}
		signalTable = strings.ToUpper(signalTable)
		if err := validateQualifiedIdentifier(signalTable); err != nil {
			return nil, fmt.Errorf("signal_table %q: %w", signalTable, err)
		}
	}

	tableFilterFn := tableFilter.Matches

	d := &db2CDCInput{
		dsn:                dsn,
		schema:             schema,
		tables:             tables,
		tableFilter:        tableFilter,
		heartbeatInterval:  heartbeatInterval,
		emitSchemaChanges:  emitSchemaChanges,
		lastSeenSynchCSN:   replication.NullCSN(),
		signalTable:        signalTable,
		asnCDCSchema:       asnCDCSchema,
		snapshotMode:       mode,
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
			TableFilter:    tableFilterFn,
		},
		streamConfig: replication.StreamConfig{
			Schema:          schema,
			Tables:          tables,
			AsnCDCSchema:    asnCDCSchema,
			BackoffInterval: streamBackoffInterval,
			PollBatchSize:   pollBatchSize,
			StartingCSN:     replication.NullCSN(),
			TableFilter:     tableFilterFn,
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

	// Open a dedicated auxiliary connection for out-of-band operations (signal
	// polling, schema change detection, incremental snapshots). These run
	// concurrently with the main CDC streaming loop, so they need their own
	// connection pool to avoid starving d.db's single slot.
	if d.signalTable != "" || d.emitSchemaChanges {
		auxDB, err := sql.Open("db2-cli", d.dsn)
		if err != nil {
			d.db.Close()
			d.db = nil
			return fmt.Errorf("opening auxiliary DB2 connection: %w", err)
		}
		auxDB.SetMaxOpenConns(1)
		auxDB.SetMaxIdleConns(1)
		if err := auxDB.PingContext(ctx); err != nil {
			auxDB.Close()
			d.db.Close()
			d.db = nil
			return fmt.Errorf("pinging auxiliary DB2 connection: %w", err)
		}
		d.auxDB = auxDB
	}

	// Initialize signal table if configured.
	if d.signalTable != "" {
		if err := d.initSignalTable(ctx); err != nil {
			if d.auxDB != nil {
				d.auxDB.Close()
				d.auxDB = nil
			}
			d.db.Close()
			d.db = nil
			return fmt.Errorf("initializing signal table: %w", err)
		}
		d.log.Infof("Using DB2 table %q for incremental snapshot signals", d.signalTable)
	}

	startingCSN, err := d.loadCheckpoint(ctx)
	if err != nil {
		if d.auxDB != nil {
			d.auxDB.Close()
			d.auxDB = nil
		}
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
	cdcCtx, cdcCancel := d.shutSig.SoftStopCtx(context.Background())
	d.cdcCancel = cdcCancel

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
	// Block until at least one event is available.
	var firstEvent replication.ChangeEvent
	select {
	case firstEvent = <-d.eventChan:
	case err := <-d.errChan:
		return nil, nil, err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-d.shutSig.SoftStopChan():
		return nil, nil, service.ErrEndOfInput
	}

	// Drain additional events non-blocking up to PollBatchSize.
	rawEvents := make([]replication.ChangeEvent, 0, d.streamConfig.PollBatchSize)
	rawEvents = append(rawEvents, firstEvent)
drainLoop:
	for len(rawEvents) < d.streamConfig.PollBatchSize {
		select {
		case ev := <-d.eventChan:
			rawEvents = append(rawEvents, ev)
		default:
			break drainLoop
		}
	}

	// Convert events to messages and track each CSN.
	// All-or-nothing: if ackErr != nil, no resolveFn is called so the Capped
	// watermark cannot advance past unprocessed messages. AutoRetryNacks
	// redelivers by calling the same ackFunc with nil on retry.
	batch := make(service.MessageBatch, 0, len(rawEvents))
	resolveFns := make([]func() *replication.CSN, 0, len(rawEvents))

	for _, event := range rawEvents {
		msg, err := d.eventToMessage(event)
		if err != nil {
			return nil, nil, err
		}
		var resolveFn func() *replication.CSN
		if !event.CSN.IsNull() {
			if resolveFn, err = d.capped.Track(ctx, event.CSN, 1); err != nil {
				return nil, nil, fmt.Errorf("tracking CSN: %w", err)
			}
		}
		batch = append(batch, msg)
		resolveFns = append(resolveFns, resolveFn)
	}

	ackFunc := func(ctx context.Context, ackErr error) error {
		if ackErr != nil {
			// All-or-nothing: do not advance any Capped slot on batch failure.
			// AutoRetryNacks will redeliver the entire batch.
			return nil
		}
		// Call resolveFns in order; checkpoint only the highest resolved CSN.
		var highest *replication.CSN
		for _, fn := range resolveFns {
			if fn == nil {
				continue
			}
			if h := fn(); h != nil {
				highest = h
			}
		}
		if highest != nil {
			if err := d.saveCheckpoint(ctx, *highest); err != nil {
				d.log.Warnf("failed to save checkpoint: %v", err)
			}
		}
		return nil
	}

	return batch, ackFunc, nil
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
		if d.cdcCancel != nil {
			d.cdcCancel()
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.auxDB != nil {
		if err := d.auxDB.Close(); err != nil {
			d.log.Errorf("error closing auxiliary DB2 connection: %v", err)
		}
		d.auxDB = nil
	}

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

	doSnapshot := false
	switch d.snapshotMode {
	case snapshotModeInitial:
		doSnapshot = d.streamConfig.StartingCSN.IsNull()
	case snapshotModeAlways:
		doSnapshot = true
	case snapshotModeNever:
		doSnapshot = false
	case snapshotModeInitialOnly:
		doSnapshot = true
	}

	if doSnapshot {
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
		if d.snapshotMode == snapshotModeInitialOnly {
			d.log.Info("snapshot_mode=initial_only: stopping after snapshot")
			return
		}
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

	// Pre-register all sub-goroutine counts before launching any. This prevents
	// a race where Close()'s wg.Wait() completes between individual wg.Add+go pairs.
	addCount := 0
	if d.heartbeatInterval > 0 {
		addCount++
	}
	if d.emitSchemaChanges {
		addCount++
	}
	if d.signalTable != "" {
		addCount++
	}
	if addCount > 0 {
		d.wg.Add(addCount)
	}

	if d.heartbeatInterval > 0 {
		go d.runHeartbeat(ctx)
	}

	if d.emitSchemaChanges {
		go d.pollSchemaChanges(ctx)
	}

	if d.signalTable != "" {
		go d.pollSignals(ctx)
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

func (d *db2CDCInput) runHeartbeat(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			hb := replication.ChangeEvent{
				Operation: replication.OpTypeHeartbeat,
				Timestamp: time.Now().UTC(),
			}
			select {
			case d.eventChan <- hb:
			case <-ctx.Done():
				return
			case <-d.shutSig.SoftStopChan():
				return
			}
		case <-ctx.Done():
			return
		case <-d.shutSig.SoftStopChan():
			return
		}
	}
}

func (d *db2CDCInput) pollSchemaChanges(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.streamConfig.BackoffInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := d.checkForSchemaChanges(ctx); err != nil {
				d.log.Warnf("schema change poll: %v", err)
			}
		case <-ctx.Done():
			return
		case <-d.shutSig.SoftStopChan():
			return
		}
	}
}

func (d *db2CDCInput) checkForSchemaChanges(ctx context.Context) error {
	// CommitSeqByteLen is 10 for DB2 ≤11.x and 16 for DB2 12.1+.
	// Using CommitSeqByteLen ensures the hex literal matches the column width.
	byteLen := d.streamConfig.CommitSeqByteLen
	if byteLen <= 0 {
		byteLen = 10
	}
	lastHex := d.lastSeenSynchCSN.SQLHex(byteLen)

	// Ported from Debezium LuwPlatform.java getListOfNewCdcEnabledTablesQuery.
	// Finds tables added to SQL Replication after the last known SYNCHPOINT.
	query := fmt.Sprintf(`
		SELECT r.SOURCE_OWNER, r.SOURCE_TABLE, r.CD_NEW_SYNCHPOINT
		FROM %s.IBMSNAP_REGISTER r
		WHERE r.SOURCE_OWNER = '%s'
		  AND r.CD_NEW_SYNCHPOINT > X'%s'
		ORDER BY r.CD_NEW_SYNCHPOINT
	`, d.asnCDCSchema, d.schema, lastHex)

	rows, err := d.auxDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying schema changes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var srcOwner, srcTable string
		var synchBytes []byte
		if err := rows.Scan(&srcOwner, &srcTable, &synchBytes); err != nil {
			d.log.Warnf("scanning schema change row: %v", err)
			continue
		}
		csn := replication.NewCSNFromDBValue(synchBytes)
		d.lastSeenSynchCSN = csn

		event := replication.ChangeEvent{
			Schema:    strings.TrimSpace(srcOwner),
			Table:     strings.TrimSpace(srcTable),
			Operation: replication.OpTypeSchemaChange,
			CSN:       csn,
			Timestamp: time.Now().UTC(),
		}
		// Note: s.changeTables is not updated here. Tables added via ASNCDC.ADDTABLE
		// are reported as schema_change events but not polled until the connector
		// restarts. A future Streamer.AddTable() API would enable hot-add without restart.
		select {
		case d.eventChan <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

// initSignalTable creates the signal table in DB2 if it does not exist.
func (d *db2CDCInput) initSignalTable(ctx context.Context) error {
	if parts := strings.SplitN(d.signalTable, ".", 2); len(parts) == 2 {
		if _, err := d.auxDB.ExecContext(ctx, "CREATE SCHEMA "+parts[0]); err != nil && !isAlreadyExistsError(err) {
			return fmt.Errorf("creating schema %q: %w", parts[0], err)
		}
	}
	_, err := d.auxDB.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			ID   VARCHAR(255) NOT NULL,
			TYPE VARCHAR(64)  NOT NULL,
			DATA VARCHAR(2048),
			PRIMARY KEY (ID)
		)`, d.signalTable))
	if err != nil && !isAlreadyExistsError(err) {
		return fmt.Errorf("create signal table %s: %w", d.signalTable, err)
	}
	return nil
}

func (d *db2CDCInput) pollSignals(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.streamConfig.BackoffInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := d.processSignals(ctx); err != nil {
				d.log.Warnf("signal poll: %v", err)
			}
		case <-ctx.Done():
			return
		case <-d.shutSig.SoftStopChan():
			return
		}
	}
}

func (d *db2CDCInput) processSignals(ctx context.Context) error {
	if d.auxDB == nil {
		return nil
	}

	// Collect all pending signals into memory first, then close the result set
	// before calling runIncrementalSnapshot. This avoids a single-connection
	// deadlock: the SELECT would hold the auxDB connection open while the
	// snapshot transaction needs that same connection.
	type pendingSignal struct {
		id, data string
	}
	var pending []pendingSignal

	rows, err := d.auxDB.QueryContext(ctx,
		fmt.Sprintf("SELECT ID, DATA FROM %s WHERE TYPE = 'execute-snapshot' FETCH FIRST 10 ROWS ONLY",
			d.signalTable))
	if err != nil {
		return fmt.Errorf("querying signals: %w", err)
	}
	for rows.Next() {
		var id string
		var data sql.NullString
		if err := rows.Scan(&id, &data); err != nil {
			d.log.Warnf("scanning signal row: %v", err)
			continue
		}
		pending = append(pending, pendingSignal{id: id, data: data.String})
	}
	scanErr := rows.Err()
	rows.Close() // release auxDB connection before snapshot
	if scanErr != nil {
		return scanErr
	}

	for _, sig := range pending {
		tables := parseSnapshotSignalTables(sig.data)
		d.log.Infof("received execute-snapshot signal id=%s tables=%v", sig.id, tables)
		if err := d.runIncrementalSnapshot(ctx, tables); err != nil {
			d.log.Warnf("incremental snapshot for signal %s: %v", sig.id, err)
		}
		_, _ = d.auxDB.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE ID = ? AND TYPE = 'execute-snapshot'", d.signalTable), sig.id)
	}
	return nil
}

func (d *db2CDCInput) runIncrementalSnapshot(ctx context.Context, tables []string) error {
	cfg := replication.SnapshotConfig{
		Schema:         d.schema,
		Tables:         tables,
		AsnCDCSchema:   d.asnCDCSchema,
		BatchSize:      d.snapshotConfig.BatchSize,
		IsolationLevel: "REPEATABLE READ",
	}
	snapshotter := replication.NewSnapshotter(d.auxDB, cfg, d.version)
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
	_, err := snapshotter.Snapshot(ctx, handler)
	return err
}

// parseSnapshotSignalTables parses the DATA field of an execute-snapshot signal.
// Accepts JSON {"data-collections":["SCHEMA.TABLE",...]} or bare table names.
// Schema prefixes are stripped; the caller sets Schema separately in SnapshotConfig.
func parseSnapshotSignalTables(data string) []string {
	type signalData struct {
		DataCollections []string `json:"data-collections"`
	}
	var sd signalData
	if err := json.Unmarshal([]byte(data), &sd); err == nil && len(sd.DataCollections) > 0 {
		tables := make([]string, 0, len(sd.DataCollections))
		for _, t := range sd.DataCollections {
			t = strings.ToUpper(t)
			if parts := strings.SplitN(t, ".", 2); len(parts) == 2 {
				tables = append(tables, parts[1])
			} else {
				tables = append(tables, t)
			}
		}
		return tables
	}
	// Fallback: comma-separated table names.
	if data == "" {
		return nil
	}
	parts := strings.Split(data, ",")
	tables := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.ToUpper(p))
		if p == "" {
			continue
		}
		if dotParts := strings.SplitN(p, ".", 2); len(dotParts) == 2 {
			tables = append(tables, dotParts[1])
		} else {
			tables = append(tables, p)
		}
	}
	return tables
}

// debeziumOp maps a replication.OpType to the Debezium operation code used in the
// "op" field of the Debezium envelope and in the db2_op metadata key.
//
// Codes: c=insert, u=update, d=delete, r=snapshot read, hb=heartbeat.
// schema_change and any unrecognised type fall through to the string value of op.
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
	case replication.OpTypeHeartbeat:
		return "hb"
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
	if event.Operation == replication.OpTypeHeartbeat {
		tsMs := event.Timestamp.UnixMilli()
		envelope := map[string]any{
			"op":    "hb",
			"ts_ms": tsMs,
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(envelope)
		msg.MetaSetMut("db2_operation", "heartbeat")
		msg.MetaSetMut("db2_op", "hb")
		msg.MetaSetMut("db2_schema", "")
		msg.MetaSetMut("db2_table", "")
		msg.MetaSetMut("db2_csn", "")
		msg.MetaSetMut("db2_commit_lsn", "")
		msg.MetaSetMut("db2_connector", "db2")
		msg.MetaSetMut("db2_snapshot", "false")
		if !event.Timestamp.IsZero() {
			msg.MetaSetMut("db2_timestamp", event.Timestamp.Format(time.RFC3339Nano))
		}
		return msg, nil
	}

	if event.Operation == replication.OpTypeSchemaChange {
		tsMs := event.Timestamp.UnixMilli()
		envelope := map[string]any{
			"op": "schema_change",
			"source": map[string]any{
				"schema": event.Schema,
				"table":  event.Table,
				"csn":    event.CSN.String(),
				"ts_ms":  tsMs,
			},
			"ts_ms": tsMs,
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(envelope)
		msg.MetaSetMut("db2_operation", "schema_change")
		msg.MetaSetMut("db2_schema", event.Schema)
		msg.MetaSetMut("db2_table", event.Table)
		msg.MetaSetMut("db2_op", "schema_change")
		csnStr := event.CSN.String()
		msg.MetaSetMut("db2_csn", csnStr)
		msg.MetaSetMut("db2_commit_lsn", csnStr)
		msg.MetaSetMut("db2_connector", "db2")
		msg.MetaSetMut("db2_snapshot", "false")
		if !event.Timestamp.IsZero() {
			msg.MetaSetMut("db2_timestamp", event.Timestamp.Format(time.RFC3339Nano))
		}
		return msg, nil
	}

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

// isAlreadyExistsError returns true when the DB2 error message contains SQLSTATE
// 42710 (object already exists). Uses substring matching because DB2 CLI error
// strings embed the SQLSTATE in two formats: "SQLSTATE=42710" and "SQLSTATE 42710".
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
