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
    order_key              TEXT    NOT NULL UNIQUE,
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
    bucket_1m_ms           INTEGER NOT NULL DEFAULT 0,
    bucket_5m_ms           INTEGER NOT NULL DEFAULT 0,
    bucket_15m_ms          INTEGER NOT NULL DEFAULT 0,
    hour_bucket_ms         INTEGER NOT NULL,
    date_utc               TEXT    NOT NULL
);

-- ═══════════ derived — what the dashboards read ═══════════

CREATE TABLE IF NOT EXISTS step_dwells (
    id                  INTEGER PRIMARY KEY,
    event_ts            TEXT    NOT NULL,      -- dwell END
    started_at_ms       INTEGER NOT NULL,
    ended_at_ms         INTEGER NOT NULL,
    duration_ms         INTEGER NOT NULL,
    order_key           TEXT    NOT NULL REFERENCES cycles(order_key),
    lane                TEXT    NOT NULL,      -- main | tilter | crusher
    state               TEXT    NOT NULL,
    step                INTEGER,
    step_title          TEXT,
    seq_index           INTEGER NOT NULL,      -- unique within (order_key, lane)
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
    sensors_trace_json  TEXT,
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
    hour_bucket_ms      INTEGER NOT NULL,
    date_utc            TEXT    NOT NULL
);

-- Per-output run time within one step. Only available from fsm_step_runs, which carries
-- actuators_json; recipe_step / recipe_origin_state disambiguate e.g. AutoCycle step 10
-- from Tilter step 10 when a step boundary splits one physical ON pulse.
CREATE TABLE IF NOT EXISTS step_actuators (
    id                  INTEGER PRIMARY KEY,
    event_ts            TEXT    NOT NULL,      -- parent step END
    step_started_at_ms  INTEGER NOT NULL,
    step_ended_at_ms    INTEGER NOT NULL,
    order_key           TEXT    NOT NULL REFERENCES cycles(order_key),
    lane                TEXT    NOT NULL,
    state               TEXT    NOT NULL,
    step                INTEGER,
    step_title          TEXT,
    seq_index           INTEGER NOT NULL,
    output_id           TEXT    NOT NULL,     -- coil address, e.g. Y0.1 (the actuators_json map key)
    output_name         TEXT    NOT NULL,
    total_run_ms        INTEGER NOT NULL,
    segment_count       INTEGER NOT NULL,
    recipe_step         INTEGER,
    recipe_origin_state TEXT,
    recipe_id           INTEGER,
    is_production       INTEGER NOT NULL,
    cycle_result        TEXT,
    hour_bucket_ms      INTEGER NOT NULL,
    date_utc            TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS state_durations (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,           -- exited
    entered_at_ms  INTEGER NOT NULL,
    exited_at_ms   INTEGER NOT NULL,
    duration_ms    INTEGER NOT NULL,
    order_key      TEXT    NOT NULL REFERENCES cycles(order_key),
    state          TEXT    NOT NULL,
    entry_reason   TEXT,
    exit_reason    TEXT,
    is_downtime    INTEGER NOT NULL,           -- 1 when Error or Maintenance
    is_production  INTEGER NOT NULL,
    hour_bucket_ms INTEGER NOT NULL,
    date_utc       TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS fault_events (
    id                  INTEGER PRIMARY KEY,
    event_ts            TEXT    NOT NULL,      -- cleared, or raised if still open
    raised_at_ms        INTEGER NOT NULL,
    cleared_at_ms       INTEGER,
    downtime_ms         INTEGER,
    order_key           TEXT    NOT NULL REFERENCES cycles(order_key),
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
    hour_bucket_ms      INTEGER NOT NULL,
    date_utc            TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS actuator_intervals (
    id                  INTEGER PRIMARY KEY,
    event_ts            TEXT    NOT NULL,      -- ended
    started_at_ms       INTEGER NOT NULL,
    ended_at_ms         INTEGER NOT NULL,
    duration_ms         INTEGER NOT NULL,      -- never COALESCEd to 0
    order_key           TEXT    NOT NULL REFERENCES cycles(order_key),
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
    recipe_id           INTEGER,
    is_production       INTEGER NOT NULL,
    cycle_result        TEXT,
    hour_bucket_ms      INTEGER NOT NULL,
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
    order_key            TEXT    NOT NULL REFERENCES cycles(order_key),
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
    hour_bucket_ms       INTEGER NOT NULL,
    date_utc             TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS cip_runs (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,           -- ended
    started_at_ms  INTEGER NOT NULL,
    ended_at_ms    INTEGER NOT NULL,
    duration_ms    INTEGER NOT NULL,
    order_key      TEXT    NOT NULL REFERENCES cycles(order_key),
    completed      INTEGER NOT NULL,
    trigger_source TEXT,                       -- auto | manual ("trigger" is reserved)
    fault_count    INTEGER,
    hour_bucket_ms INTEGER NOT NULL,
    date_utc       TEXT    NOT NULL
);

-- ═══════════ raw mirrors — drill-down ═══════════

CREATE TABLE IF NOT EXISTS fsm_events (
    id               INTEGER PRIMARY KEY,
    event_ts         TEXT    NOT NULL,
    event_at_ms      INTEGER NOT NULL,
    order_key        TEXT    NOT NULL REFERENCES cycles(order_key),
    src_id           INTEGER NOT NULL,
    event_kind       TEXT    NOT NULL,
    state_from       TEXT,
    state_to         TEXT,
    current_state    TEXT,
    step_from        INTEGER,
    step_to          INTEGER,
    modbus_order_reg INTEGER,                  -- renamed from source order_id
    input_id         TEXT,
    input_value      INTEGER,
    event_type       TEXT,
    source           TEXT,
    trace_id         TEXT,
    payload_json     TEXT,
    sensors_json     TEXT,
    -- The same snapshot as one character per input, in the order documented in sensors.go.
    -- 32 bytes with nothing to escape, against ~820 on the wire for sensors_json — this is
    -- the column that ships; sensors_json stays local for inspection.
    sensors_bits     TEXT,
    is_production    INTEGER NOT NULL,
    hour_bucket_ms   INTEGER NOT NULL,
    date_utc         TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_toggles (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,
    event_at_ms    INTEGER NOT NULL,
    order_key      TEXT    NOT NULL REFERENCES cycles(order_key),
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
    hour_bucket_ms INTEGER NOT NULL,
    date_utc       TEXT    NOT NULL
);

-- The source config table is a mutable KV with no timestamps, so the projector diffs
-- against its own snapshot; changed_at_ms is detection time, not change time.
CREATE TABLE IF NOT EXISTS config_history (
    id             INTEGER PRIMARY KEY,
    event_ts       TEXT    NOT NULL,
    changed_at_ms  INTEGER NOT NULL,
    order_key      TEXT    NOT NULL REFERENCES cycles(order_key),
    config_key     TEXT    NOT NULL,
    old_value      TEXT,
    new_value      TEXT,
    hour_bucket_ms INTEGER NOT NULL,
    date_utc       TEXT    NOT NULL
);

-- ═══════════ rollups — every column additive, so buckets re-aggregate ═══════════

CREATE TABLE IF NOT EXISTS hourly_rollups (
    id               INTEGER PRIMARY KEY,
    event_ts         TEXT    NOT NULL,         -- bucket end
    bucket_start_ms  INTEGER NOT NULL,
    date_utc         TEXT    NOT NULL,
    recipe_id        INTEGER,
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
    idle_ms          INTEGER
);

CREATE TABLE IF NOT EXISTS hourly_fault_counts (
    id              INTEGER PRIMARY KEY,
    event_ts        TEXT    NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    date_utc        TEXT    NOT NULL,
    fault_type      TEXT    NOT NULL,
    severity        TEXT    NOT NULL,
    occurrences     INTEGER NOT NULL,
    downtime_ms_sum INTEGER
);

CREATE TABLE IF NOT EXISTS hourly_step_stats (
    id              INTEGER PRIMARY KEY,
    event_ts        TEXT    NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    date_utc        TEXT    NOT NULL,
    lane            TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    step            INTEGER,
    step_title      TEXT,
    dwell_count     INTEGER NOT NULL,
    duration_ms_sum INTEGER NOT NULL,
    duration_ms_max INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_rollups (
    id               INTEGER PRIMARY KEY,
    event_ts         TEXT    NOT NULL,
    date_utc         TEXT    NOT NULL,
    recipe_id        INTEGER,
    glasses          INTEGER,
    orders_completed INTEGER,
    orders_faulted   INTEGER,
    fault_count      INTEGER,
    cycle_ms_sum     INTEGER,
    cycle_count      INTEGER,
    run_ms           INTEGER,
    error_ms         INTEGER,
    maintenance_ms   INTEGER,
    idle_ms          INTEGER
);

-- ═══════════ collector health ═══════════

CREATE TABLE IF NOT EXISTS projector_runs (
    id            INTEGER PRIMARY KEY,
    event_ts      TEXT    NOT NULL,
    started_at_ms INTEGER NOT NULL,
    stopped_at_ms INTEGER,
    version       TEXT,
    source_branch TEXT,                        -- which controller schema was detected
    note          TEXT
);

CREATE TABLE IF NOT EXISTS gaps (
    id            INTEGER PRIMARY KEY,
    event_ts      TEXT    NOT NULL,            -- gap END
    started_at_ms INTEGER NOT NULL,
    ended_at_ms   INTEGER,
    duration_ms   INTEGER,
    order_key     TEXT REFERENCES cycles(order_key),
    reason        TEXT
);

-- Indexes are excluded from the schema fingerprint, so these are free to add or change.
-- order_key and the time buckets are indexed everywhere: they are the admin dashboard's
-- primary filters.
CREATE INDEX IF NOT EXISTS idx_cycles_ts              ON cycles(event_ts);
CREATE INDEX IF NOT EXISTS idx_cycles_bucket          ON cycles(hour_bucket_ms);
CREATE INDEX IF NOT EXISTS idx_cycles_date            ON cycles(date_utc, is_production);
CREATE INDEX IF NOT EXISTS idx_step_dwells_ts         ON step_dwells(event_ts);
CREATE INDEX IF NOT EXISTS idx_step_dwells_order      ON step_dwells(order_key, lane, seq_index);
CREATE INDEX IF NOT EXISTS idx_step_dwells_bucket     ON step_dwells(hour_bucket_ms, lane, step);
CREATE INDEX IF NOT EXISTS idx_step_actuators_ts      ON step_actuators(event_ts);
CREATE INDEX IF NOT EXISTS idx_step_actuators_order   ON step_actuators(order_key, seq_index);
CREATE INDEX IF NOT EXISTS idx_step_actuators_bucket  ON step_actuators(hour_bucket_ms, output_name);
CREATE INDEX IF NOT EXISTS idx_state_durations_ts     ON state_durations(event_ts);
CREATE INDEX IF NOT EXISTS idx_state_durations_order  ON state_durations(order_key);
CREATE INDEX IF NOT EXISTS idx_state_durations_down   ON state_durations(hour_bucket_ms, is_downtime);
CREATE INDEX IF NOT EXISTS idx_fault_events_ts        ON fault_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_fault_events_order     ON fault_events(order_key);
CREATE INDEX IF NOT EXISTS idx_fault_events_bucket    ON fault_events(hour_bucket_ms, fault_type);
CREATE INDEX IF NOT EXISTS idx_actuator_intervals_ts  ON actuator_intervals(event_ts);
CREATE INDEX IF NOT EXISTS idx_actuator_intervals_ord ON actuator_intervals(order_key, output_id);
CREATE INDEX IF NOT EXISTS idx_cip_runs_order         ON cip_runs(order_key);
CREATE INDEX IF NOT EXISTS idx_door_events_ts         ON door_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_door_events_order      ON door_events(order_key);
CREATE INDEX IF NOT EXISTS idx_door_events_reset      ON door_events(hour_bucket_ms, fault_reset_during);
CREATE INDEX IF NOT EXISTS idx_fsm_events_ts          ON fsm_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_fsm_events_order       ON fsm_events(order_key, event_at_ms);
CREATE INDEX IF NOT EXISTS idx_sensor_toggles_ts      ON sensor_toggles(event_ts);
CREATE INDEX IF NOT EXISTS idx_sensor_toggles_order   ON sensor_toggles(order_key, event_at_ms);
CREATE INDEX IF NOT EXISTS idx_config_history_order   ON config_history(order_key);
CREATE INDEX IF NOT EXISTS idx_hourly_rollups_bucket  ON hourly_rollups(bucket_start_ms);
CREATE INDEX IF NOT EXISTS idx_hourly_faults_bucket   ON hourly_fault_counts(bucket_start_ms, fault_type);
CREATE INDEX IF NOT EXISTS idx_hourly_steps_bucket    ON hourly_step_stats(bucket_start_ms, lane, step);
CREATE INDEX IF NOT EXISTS idx_daily_rollups_date     ON daily_rollups(date_utc);
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
`
