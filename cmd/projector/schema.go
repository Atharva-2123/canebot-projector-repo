package main

// DDL for the two databases the projector owns.
//
// The source database (the controller's config.db) is NEVER written to. It is opened
// read-only and only ever SELECTed from.
//
// ---------------------------------------------------------------------------
// Source-branch compatibility
// ---------------------------------------------------------------------------
// Three controller branches are in flight and their schemas differ:
//
//	main                  7 tables (the base set)
//	test/tracking_fsm     + Audit, + fsm_step_runs
//	test/maintenance      + Audit, + order_checkpoint
//
// Every shared table is column-identical across all three, so the projector runs against
// any of them. Of the three extra tables only fsm_step_runs is replicated:
//
//	fsm_step_runs     one row per completed step, with sensor snapshots and per-step
//	                  actuator segments. Strictly better than deriving dwells ourselves,
//	                  so it is used when present and derived from fsm_events when absent.
//	Audit             a JSON mirror of faults and orders inserts. Both are already
//	                  projected, so replicating it would duplicate them in the cloud.
//	order_checkpoint  a single overwritten row (CHECK id = 1) holding the power-loss
//	                  resume point. Operational state, not history — same category as
//	                  fsm_resume_state, which is likewise excluded.
//
// ---------------------------------------------------------------------------
// Rules baked into the replica schema, all from the ilyama edge contract
// ---------------------------------------------------------------------------
//   - every table has an explicit INTEGER PRIMARY KEY (rowid-only tables are rejected)
//   - every table has a fixed-width RFC3339 `event_ts`, which is the EMIT/CLOSE time,
//     never the start time (the agent's cursor only moves forward, so a row written late
//     carrying an earlier timestamp would silently never ship)
//   - domain timestamps are additionally carried as integer *_at_ms columns
//   - only INTEGER / REAL / TEXT affinities; no BLOB (excluded by allowed_affinities)
//   - `cycles` is the FK parent for everything scoped, so it is created first
//
// ---------------------------------------------------------------------------
// Denormalization
// ---------------------------------------------------------------------------
// Child rows carry their parent cycle's context (recipe_id, is_production, cycle_result)
// plus pre-computed time buckets (hour_bucket_ms, date_utc) so admin dashboard queries
// never join and never compute a bucket expression. This is safe here specifically because
// rows are immutable and flushed cycle-atomically — parent and children are written in one
// transaction, so the copied values cannot drift.
//
// Timestamps are UTC throughout; date_utc is the UTC calendar date. There is no shift
// dimension because the controller has no shift concept to derive one from.
const replicaSchema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

-- ═══════════ cycles — the FK parent, a continuous partition of machine time ═══════════

CREATE TABLE IF NOT EXISTS cycles (
    id                     INTEGER PRIMARY KEY,
    event_ts               TEXT    NOT NULL,   -- interval END
    started_at_ms          INTEGER NOT NULL,
    ended_at_ms            INTEGER NOT NULL,
    duration_ms            INTEGER NOT NULL,
    order_id              TEXT    NOT NULL UNIQUE,
    is_production          INTEGER NOT NULL,   -- 1 real order, 0 synthetic interval
    result                 TEXT    NOT NULL,   -- completed | faulted_recoverable
                                               -- | faulted_non_recoverable | aborted
                                               -- | idle | maintenance | manual | error
    recipe_id              INTEGER,
    glass_count            INTEGER,
    terminal_state         TEXT,
    terminal_step          INTEGER,
    fault_count            INTEGER,
    dominant_fault_type    TEXT,
    first_fault_at_ms      INTEGER,
    last_fault_at_ms       INTEGER,
    fsm_event_count        INTEGER,
    step_event_count       INTEGER,
    state_transition_count INTEGER,
    unique_state_count     INTEGER,
    outcome                TEXT    NOT NULL DEFAULT '',
    close_reason           TEXT    NOT NULL DEFAULT '', -- step19 | fault | next_order | cap
                                                       -- | state_change | shutdown
    bucket_1m_ms           INTEGER NOT NULL DEFAULT 0,
    bucket_5m_ms           INTEGER NOT NULL DEFAULT 0,
    bucket_15m_ms          INTEGER NOT NULL DEFAULT 0,
    date_utc               TEXT    NOT NULL
);

-- ═══════════ derived — what the dashboards read ═══════════

CREATE TABLE IF NOT EXISTS step_dwells (
    id                  INTEGER PRIMARY KEY,
    event_ts            TEXT    NOT NULL,      -- dwell END
    started_at_ms       INTEGER NOT NULL,
    ended_at_ms         INTEGER NOT NULL,
    duration_ms         INTEGER NOT NULL,
    order_id           TEXT    NOT NULL REFERENCES cycles(order_id),
    lane                TEXT    NOT NULL,      -- main | tilter | crusher
    state               TEXT    NOT NULL,
    step                INTEGER,
    step_title          TEXT,
    seq_index           INTEGER NOT NULL,      -- unique within (order_id, lane)
    previous_state      TEXT,                  -- only when sourced from fsm_step_runs
    previous_step       INTEGER,
    event_count         INTEGER,
    io_event_count      INTEGER,
    transition_count    INTEGER,
    fault_count         INTEGER,
    sensors_start_bits  TEXT,
    sensors_end_bits    TEXT,
    door_closed         INTEGER,
    cip_bypass          INTEGER,
    sensors_start_json  TEXT,                  -- only when sourced from fsm_step_runs
    sensors_end_json    TEXT,
    -- Every input edge during the step, and the full per-output actuator breakdown, both
    -- carried verbatim from the firmware. step_actuators flattens actuators_json into rows
    -- but keeps only the first segment's recipe_step / recipe_origin_state, so the raw blob
    -- is what preserves multi-pulse detail. sensors_trace_json has no flattened form at all.
    actuators_json      TEXT,
    source_kind         TEXT    NOT NULL,      -- step_runs | derived
    -- The fault that interrupted this step, denormalised by the firmware onto the step run
    -- (only when sourced from fsm_step_runs). fault_events stays the fault ledger; these are
    -- here so "which step fails most" needs no join.
    fault_type          TEXT,
    fault_message       TEXT,
    recipe_id           INTEGER,
    is_production       INTEGER NOT NULL,
    cycle_result        TEXT,
    cycle_started_at_ms INTEGER,
    bucket_1m_ms   INTEGER NOT NULL DEFAULT 0,
    bucket_5m_ms   INTEGER NOT NULL DEFAULT 0,
    bucket_15m_ms  INTEGER NOT NULL DEFAULT 0,
    date_utc            TEXT    NOT NULL
);


CREATE TABLE IF NOT EXISTS state_durations (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,           -- exited
    entered_at_ms  INTEGER NOT NULL,
    exited_at_ms   INTEGER NOT NULL,
    duration_ms    INTEGER NOT NULL,
    order_id      TEXT    NOT NULL REFERENCES cycles(order_id),
    state          TEXT    NOT NULL,
    entry_reason   TEXT,
    exit_reason    TEXT,
    is_downtime    INTEGER NOT NULL,           -- 1 when Error or Maintenance
    is_production  INTEGER NOT NULL,
    date_utc       TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS fault_events (
    id                  INTEGER PRIMARY KEY,
    event_ts            TEXT    NOT NULL,      -- cleared, or raised if still open
    raised_at_ms        INTEGER NOT NULL,
    cleared_at_ms       INTEGER,
    downtime_ms         INTEGER,
    order_id           TEXT    NOT NULL REFERENCES cycles(order_id),
    fault_key           TEXT    NOT NULL UNIQUE,
    fault_type          TEXT    NOT NULL,
    severity            TEXT    NOT NULL,      -- device-classified
    recovered           INTEGER,
    recovery_at_ms      INTEGER,
    state               TEXT,
    step                INTEGER,
    step_title          TEXT,
    message             TEXT,
    dwell_seq_index     INTEGER,
    recipe_id           INTEGER,
    is_production       INTEGER NOT NULL,
    cycle_result        TEXT,
    cycle_started_at_ms INTEGER,
    bucket_1m_ms   INTEGER NOT NULL DEFAULT 0,
    bucket_5m_ms   INTEGER NOT NULL DEFAULT 0,
    bucket_15m_ms  INTEGER NOT NULL DEFAULT 0,
    date_utc            TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS actuator_intervals (
    id                  INTEGER PRIMARY KEY,
    event_ts            TEXT    NOT NULL,      -- ended
    started_at_ms       INTEGER NOT NULL,
    ended_at_ms         INTEGER NOT NULL,
    duration_ms         INTEGER NOT NULL,      -- never COALESCEd to 0
    order_id           TEXT    NOT NULL REFERENCES cycles(order_id),
    src_id              INTEGER NOT NULL,
    revision            INTEGER NOT NULL DEFAULT 0,
    output_id           TEXT    NOT NULL,
    output_name         TEXT    NOT NULL,
    started_state       TEXT,
    started_step        INTEGER,
    started_send_ok     INTEGER,
    ended_state         TEXT,
    ended_step          INTEGER,
    ended_send_ok       INTEGER,
    fault_type          TEXT,
    fault_message       TEXT,
    fault_raised_at_ms  INTEGER,
    fault_cleared_at_ms INTEGER,
    seq_index           INTEGER,               -- the dwell this pulse fell inside; attributed
                                               -- after both are written, since pulses and
                                               -- dwells arrive on separate cursors
    recipe_id           INTEGER,
    is_production       INTEGER NOT NULL,
    cycle_result        TEXT,
    date_utc            TEXT    NOT NULL
);

-- One row per door open→close episode, emitted when the door closes.
--
-- Both signals come from the input stream the controller already records:
--   X0.0  main door switch — true means CLOSED, so open is the true→false edge
--   X0.7  labelled "CIP bypass switch", but its RISING edge is what the firmware treats
--         as the fault reset (fsm/error.go). A rise while the door is open therefore means
--         someone reset a fault during that episode.
CREATE TABLE IF NOT EXISTS door_events (
    id                   INTEGER PRIMARY KEY,
    event_ts             TEXT    NOT NULL,     -- door CLOSED (episode end)
    opened_at_ms         INTEGER NOT NULL,
    closed_at_ms         INTEGER NOT NULL,
    duration_ms          INTEGER NOT NULL,     -- how long the door stood open
    order_id            TEXT    NOT NULL REFERENCES cycles(order_id),
    fault_reset_during   INTEGER NOT NULL,     -- 1 if X0.7 rose while the door was open
    fault_reset_count    INTEGER NOT NULL,     -- more than one means repeated attempts
    first_reset_at_ms    INTEGER,
    last_reset_at_ms     INTEGER,
    ms_to_first_reset    INTEGER,              -- open → first reset; how long the operator took
    state_at_open        TEXT,
    step_at_open         INTEGER,
    state_at_close       TEXT,
    step_at_close        INTEGER,
    opened_in_error      INTEGER NOT NULL,     -- 1 if the machine was already faulted
    recipe_id            INTEGER,
    is_production        INTEGER NOT NULL,
    cycle_result         TEXT,
    date_utc             TEXT    NOT NULL
);

-- One row per fault incident, emitted once the machine has recovered from it.
--
-- An incident runs from a fault until the machine is back to normal work: it enters HomeIdle,
-- or a drink reaches AutoCycle step 19. Every fault raised in between belongs to it, which is
-- what folds the re-raises a door close without a reset produces (seven fault_events rows on
-- 18 Sep, one breakdown) into the single stretch of downtime they actually were.
--
-- fault_events cannot carry this. Its rows ship once, when the fault is raised, and both
-- destinations append rather than upsert — so a resolution written back onto a fault row would
-- never leave the device, and re-emitting the row would duplicate the fault in the cloud.
--
-- No foreign key to cycles: an incident longer than the replica retention would outlive its
-- cycle row, and a failing insert here would stall the whole tick.
CREATE TABLE IF NOT EXISTS fault_incidents (
    id                     INTEGER PRIMARY KEY,
    event_ts               TEXT    NOT NULL,   -- recovered
    raised_at_ms           INTEGER NOT NULL,   -- the first fault
    recovered_at_ms        INTEGER NOT NULL,
    downtime_ms            INTEGER NOT NULL,   -- raised → recovered; the MTTR numerator
    recovered_via          TEXT    NOT NULL,   -- home_idle | cycle_complete
    order_id               TEXT,               -- the first fault's
    fault_key              TEXT    NOT NULL UNIQUE,  -- the first fault's
    fault_type             TEXT    NOT NULL,
    severity               TEXT    NOT NULL,
    fault_count            INTEGER NOT NULL,   -- faults raised before recovery, re-raises included
    last_raised_at_ms      INTEGER NOT NULL,
    first_reset_at_ms      INTEGER,            -- first X0.7 rising edge
    reset_count            INTEGER NOT NULL,
    first_door_open_at_ms  INTEGER,            -- response time = this − raised_at_ms
    door_open_count        INTEGER NOT NULL,   -- per-episode times are in door_events
    door_open_ms           INTEGER NOT NULL,
    -- Where the downtime went. Spans are clipped to the incident, so these four plus any
    -- projector gap sum to downtime_ms.
    error_ms               INTEGER NOT NULL,
    manual_ms              INTEGER NOT NULL,
    maintenance_ms         INTEGER NOT NULL,
    run_ms                 INTEGER NOT NULL,   -- AutoCycle resumed and re-faulted
    date_utc               TEXT    NOT NULL    -- of raised_at_ms
);

CREATE TABLE IF NOT EXISTS cip_runs (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,           -- ended
    started_at_ms  INTEGER NOT NULL,
    ended_at_ms    INTEGER NOT NULL,
    duration_ms    INTEGER NOT NULL,
    order_id      TEXT    NOT NULL REFERENCES cycles(order_id),
    completed      INTEGER NOT NULL,
    fault_count    INTEGER,
    date_utc       TEXT    NOT NULL
);


CREATE TABLE IF NOT EXISTS sensor_toggles (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,
    event_at_ms    INTEGER NOT NULL,
    order_id      TEXT    NOT NULL REFERENCES cycles(order_id),
    src_id         INTEGER NOT NULL,
    input_id       TEXT    NOT NULL,
    input_name     TEXT    NOT NULL,
    value_from     INTEGER NOT NULL,
    value_to       INTEGER NOT NULL,
    current_state  TEXT,
    current_step   INTEGER,
    is_production  INTEGER NOT NULL,
    bucket_1m_ms   INTEGER NOT NULL DEFAULT 0,
    bucket_5m_ms   INTEGER NOT NULL DEFAULT 0,
    bucket_15m_ms  INTEGER NOT NULL DEFAULT 0,
    date_utc       TEXT    NOT NULL
);

-- The source config table is a mutable KV with no timestamps, so the projector diffs
-- against its own snapshot; changed_at_ms is detection time, not change time.
CREATE TABLE IF NOT EXISTS config_history (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,
    changed_at_ms  INTEGER NOT NULL,
    order_id      TEXT    NOT NULL REFERENCES cycles(order_id),
    config_key     TEXT    NOT NULL,
    old_value      TEXT,
    new_value      TEXT,
    date_utc       TEXT    NOT NULL
);

-- ═══════════ rollups — pre-aggregated, two axes ═══════════
--
-- One table replaces hourly_rollups, daily_rollups, hourly_fault_counts and
-- hourly_step_stats. They differed only in their grain and in what they were grouped by, so
-- they are now grain and (dim_kind, dim_key) on a single table.
--
-- Fewer tables is not cosmetic here: every table is one more omega snapshot cursor, and a
-- table created empty and populated later is silently skipped by the agent — a failure that
-- has already cost all four of these once.
--
-- EVERY measure is additive: sums and counts, divided at render time, never a stored average.
-- That is what lets an hour re-aggregate into a day, and a day into a quarter.
--
-- The in-progress bucket has no row by design. Render it as pending, never as zero.
CREATE TABLE IF NOT EXISTS rollups (
    id               INTEGER PRIMARY KEY,
    event_ts         TEXT    NOT NULL,         -- bucket end
    grain            TEXT    NOT NULL,         -- hour | day
    bucket_start_ms  INTEGER NOT NULL,
    date_utc         TEXT    NOT NULL,
    dim_kind         TEXT    NOT NULL,         -- machine | fault_type | step | recipe | sensor
    dim_key          TEXT,                     -- NULL for machine; else the fault type,
                                               -- step, recipe id or input_id

    -- machine measures (dim_kind = 'machine')
    glasses          INTEGER,
    orders_started   INTEGER,
    orders_completed INTEGER,
    orders_faulted   INTEGER,
    fault_count      INTEGER,
    cip_runs         INTEGER,
    cycle_ms_sum     INTEGER,                  -- sum + count, never a pre-divided average
    cycle_count      INTEGER,
    run_ms           INTEGER,
    error_ms         INTEGER,
    maintenance_ms   INTEGER,
    idle_ms          INTEGER,

    -- dimensional measures (dim_kind <> 'machine')
    occurrences      INTEGER,
    duration_ms_sum  INTEGER,
    duration_ms_max  INTEGER,
    severity         TEXT,                     -- fault_type rows only
    lane             TEXT,                     -- step rows only

    UNIQUE(grain, bucket_start_ms, dim_kind, dim_key)
);


CREATE TABLE IF NOT EXISTS gaps (
    id            INTEGER PRIMARY KEY,
    event_ts      TEXT    NOT NULL,            -- gap END
    started_at_ms INTEGER NOT NULL,
    ended_at_ms   INTEGER,
    duration_ms   INTEGER,
    order_id     TEXT REFERENCES cycles(order_id),
    reason        TEXT,
    version       TEXT,                        -- projector build that covered this period
    source_branch TEXT                         -- which controller schema it detected
);

-- The live fault and door state, for the platform's alarm rules. One row per change plus a
-- heartbeat, so the cloud's latest row per machine is always the current state. See
-- alarmstate.go. A cleared row keeps the fault's type, message and raised time: the alarm
-- rule keys its instance on them, so the clear has to land on the same instance.
CREATE TABLE IF NOT EXISTS alarm_state (
    id                   INTEGER PRIMARY KEY,
    event_ts             TEXT    NOT NULL,   -- when the row was written, strictly increasing
    fault_active         INTEGER NOT NULL,
    fault_type           TEXT,               -- the current, or last, fault
    fault_message        TEXT,
    fault_raised_at_ms   INTEGER,
    fault_cleared_at_ms  INTEGER,
    clear_reason         TEXT,               -- x07_reset | home_idle
    door_open            INTEGER,            -- NULL: not yet known
    door_opened_at_ms    INTEGER,            -- the current, or last, opening
    machine_state        TEXT,
    reason               TEXT    NOT NULL,   -- startup | change | heartbeat
    date_utc             TEXT    NOT NULL
);

-- Indexes are excluded from the schema fingerprint, so these are free to add or change.
-- order_id and the time buckets are indexed everywhere: they are the admin dashboard's
-- primary filters.
CREATE INDEX IF NOT EXISTS idx_cycles_ts              ON cycles(event_ts);
CREATE INDEX IF NOT EXISTS idx_cycles_date            ON cycles(date_utc, is_production);
CREATE INDEX IF NOT EXISTS idx_step_dwells_ts         ON step_dwells(event_ts);
CREATE INDEX IF NOT EXISTS idx_step_dwells_order      ON step_dwells(order_id, lane, seq_index);
CREATE INDEX IF NOT EXISTS idx_state_durations_ts     ON state_durations(event_ts);
CREATE INDEX IF NOT EXISTS idx_state_durations_order  ON state_durations(order_id);
CREATE INDEX IF NOT EXISTS idx_fault_events_ts        ON fault_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_fault_events_order     ON fault_events(order_id);
CREATE INDEX IF NOT EXISTS idx_actuator_intervals_ts  ON actuator_intervals(event_ts);
CREATE INDEX IF NOT EXISTS idx_actuator_intervals_ord ON actuator_intervals(order_id, output_id);
CREATE INDEX IF NOT EXISTS idx_cip_runs_order         ON cip_runs(order_id);
CREATE INDEX IF NOT EXISTS idx_door_events_ts         ON door_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_door_events_order      ON door_events(order_id);
CREATE INDEX IF NOT EXISTS idx_door_events_opened     ON door_events(opened_at_ms);
CREATE INDEX IF NOT EXISTS idx_fault_incidents_ts     ON fault_incidents(event_ts);
CREATE INDEX IF NOT EXISTS idx_fault_events_raised    ON fault_events(raised_at_ms);
CREATE INDEX IF NOT EXISTS idx_state_durations_enter  ON state_durations(state, entered_at_ms);
CREATE INDEX IF NOT EXISTS idx_sensor_toggles_ts      ON sensor_toggles(event_ts);
CREATE INDEX IF NOT EXISTS idx_sensor_toggles_order   ON sensor_toggles(order_id, event_at_ms);
CREATE INDEX IF NOT EXISTS idx_config_history_order   ON config_history(order_id);
CREATE INDEX IF NOT EXISTS idx_rollups_lookup          ON rollups(grain, dim_kind, bucket_start_ms);
`

// fsmLogsSchema is the controller's stdout log (fsm.log), one row per line.
//
// Deliberately NOT part of replicaSchema. Every omega reading the replica must list fsm_logs
// under table_strategies before the table exists: an unlisted table defaults to the `rows`
// strategy, which installs triggers that a later switch to `telemetry` never removes. So the
// table is only created once -fsm-log is set, which lets the omega configs roll out first.
//
// It is also created in the same transaction as its first row. A table the agent sees empty is
// marked snapshot-done and its cursor fast-forwarded past whatever lands next — the bug that has
// already cost the rollup tables once.
//
// event_ts carries a sequence, not sub-second time. The controller logs whole seconds and up to
// ~50 lines share one, while the platform drops the agent's row id — so the n-th line of a
// second is stamped n milliseconds into it, to keep the order in the one column every query
// sorts on. Milliseconds because the platform stores DateTime64(3). The real second is
// logged_at_ms.
const fsmLogsSchema = `
CREATE TABLE IF NOT EXISTS fsm_logs (
    id               INTEGER PRIMARY KEY,
    event_ts         TEXT    NOT NULL,
    logged_at_ms     INTEGER NOT NULL,
    ts_source        TEXT    NOT NULL,   -- line | clock | inherited | tailer | digest
    file_gen         TEXT    NOT NULL,   -- one log file lifetime: dev:inode:first_seen_ms
    byte_offset      INTEGER NOT NULL,   -- where the line starts; -1 for tailer rows
    line_no          INTEGER NOT NULL,   -- within file_gen
    logger           TEXT,               -- db_hook | step_run | actuator_run | sensor_toggle
    level            TEXT    NOT NULL,   -- error | warn | info
    category         TEXT    NOT NULL,   -- modbus | fsm_step | fsm_state | observability
                                         -- | tailer | other
    tag              TEXT,               -- first [TAG] of the message
    tag2             TEXT,               -- second [TAG], e.g. [TILTER-HOLD-GUARD] [Homing]
    message          TEXT    NOT NULL,   -- without the date or logger prefix
    fingerprint      TEXT    NOT NULL,   -- the message with its numbers blanked
    repeat_count     INTEGER NOT NULL DEFAULT 1,
    first_seen_at_ms INTEGER,            -- digest rows: the repeats this row stands for
    last_seen_at_ms  INTEGER,
    is_continuation  INTEGER NOT NULL DEFAULT 0,
    truncated        INTEGER NOT NULL DEFAULT 0,
    bucket_1m_ms     INTEGER NOT NULL,
    date_utc         TEXT    NOT NULL,
    UNIQUE(file_gen, byte_offset)
);
CREATE INDEX IF NOT EXISTS idx_fsm_logs_ts    ON fsm_logs(event_ts);
CREATE INDEX IF NOT EXISTS idx_fsm_logs_level ON fsm_logs(level, bucket_1m_ms);
`

// stateSchema is projector_state.db — the projector's own bookkeeping.
//
// Deliberately a SEPARATE FILE from the replica: the edge agent auto-enrols every
// non-system table it finds in the database it is pointed at and silently defaults unlisted
// tables to the `rows` strategy, which installs triggers. Bookkeeping tables sitting in the
// replica would therefore get triggers installed and replicate themselves to the cloud.
const stateSchema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = FULL;

-- Per-source-table read watermark. Keyed on the source id PRIMARY KEY, never on a
-- timestamp: ts_utc is RFC3339Nano, which strips trailing zeros, so its lexical order
-- is not time order.
CREATE TABLE IF NOT EXISTS source_cursors (
    source_table   TEXT PRIMARY KEY,
    last_src_id    INTEGER NOT NULL DEFAULT 0,
    last_closed_ms INTEGER,
    updated_at_ms  INTEGER NOT NULL
);

-- In-flight work, so a restart mid-cycle resumes rather than dropping the cycle.
CREATE TABLE IF NOT EXISTS open_segments (
    kind          TEXT    NOT NULL,
    segment_key   TEXT    NOT NULL,
    started_at_ms INTEGER NOT NULL,
    payload_json  TEXT,
    PRIMARY KEY (kind, segment_key)
);

-- Monotonic guard. The writer refuses to emit a row whose event_ts is earlier than the
-- last emitted value for that table; going backwards loses rows permanently and silently.
CREATE TABLE IF NOT EXISTS emit_watermark (
    table_name    TEXT PRIMARY KEY,
    last_event_ts TEXT    NOT NULL,
    last_id       INTEGER NOT NULL
);

-- Last observed value per config key, so changes can be detected without timestamps
-- (the source config table is a mutable KV with no timestamp columns at all).
CREATE TABLE IF NOT EXISTS config_snapshot (
    config_key TEXT PRIMARY KEY,
    value      TEXT
);

-- Liveness heartbeat, written every tick whether or not there was anything to read.
-- Gap detection cannot use the cursor timestamps for this: those only advance when new
-- rows arrive, so an idle-but-healthy projector would look absent and fabricate a gap.
CREATE TABLE IF NOT EXISTS projector_meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

-- The alarm_state tracker, persisted as one JSON document so the state and the fsm_events id
-- it is accurate as of are always written together. See alarmstate.go.
CREATE TABLE IF NOT EXISTS alarm_tracker (
    id    INTEGER PRIMARY KEY CHECK (id = 1),
    state TEXT NOT NULL
);

-- Read position in a tailed log file. dev/inode catch a replaced file, and the hash of its
-- first bytes catches one truncated and regrown past byte_offset between two reads — a size
-- check alone would resume mid-line in unrelated content.
CREATE TABLE IF NOT EXISTS log_cursors (
    path              TEXT PRIMARY KEY,
    file_gen          TEXT    NOT NULL,
    dev               INTEGER NOT NULL,
    inode             INTEGER NOT NULL,
    byte_offset       INTEGER NOT NULL,
    line_no           INTEGER NOT NULL,
    head_len          INTEGER NOT NULL,
    head_sha256       TEXT    NOT NULL,
    last_logged_at_ms INTEGER NOT NULL DEFAULT 0,
    updated_at_ms     INTEGER NOT NULL,
    -- Bytes pruned off the file's start in this generation. byte_offset is in the file as it
    -- is now; rows carry base_offset + byte_offset, so a line never reuses a pruned one's key.
    base_offset       INTEGER NOT NULL DEFAULT 0,
    -- A prune about to happen: its length, and the hash of the bytes the file will start with
    -- once it has. A restart that finds these compares, and so knows whether the cut landed.
    pending_prune_len INTEGER NOT NULL DEFAULT 0,
    pending_head_len  INTEGER NOT NULL DEFAULT 0,
    pending_head_sha  TEXT    NOT NULL DEFAULT '',
    -- The file's size just before that prune. When the bytes after the cut repeat the bytes
    -- before it, the hash cannot tell; a file now shorter than this can (it is only appended).
    pending_size      INTEGER NOT NULL DEFAULT 0
);
`

// logCursorColumns are the log_cursors columns added after the table first shipped, so an
// existing projector_state.db gains them on open. CREATE TABLE IF NOT EXISTS does not.
var logCursorColumns = []struct{ name, ddl string }{
	{"base_offset", `ALTER TABLE log_cursors ADD COLUMN base_offset INTEGER NOT NULL DEFAULT 0`},
	{"pending_prune_len", `ALTER TABLE log_cursors ADD COLUMN pending_prune_len INTEGER NOT NULL DEFAULT 0`},
	{"pending_head_len", `ALTER TABLE log_cursors ADD COLUMN pending_head_len INTEGER NOT NULL DEFAULT 0`},
	{"pending_head_sha", `ALTER TABLE log_cursors ADD COLUMN pending_head_sha TEXT NOT NULL DEFAULT ''`},
	{"pending_size", `ALTER TABLE log_cursors ADD COLUMN pending_size INTEGER NOT NULL DEFAULT 0`},
}
