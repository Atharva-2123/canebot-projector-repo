package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"canebot-fsm/fsm"
)

// Output row types. These mirror the replica schema; the writer stamps `event_ts` and the
// denormalized cycle context at flush time, which is why none of them carry those fields.

type outFSMEvent struct {
	eventAtMS      int64
	orderKey       string
	srcID          int64
	eventKind      string
	stateFrom      string
	stateTo        string
	currentState   string
	stepFrom       *int64
	stepTo         *int64
	modbusOrderReg *int64
	inputID        string
	inputValue     *int64
	eventType      string
	source         string
	traceID        string
	payloadJSON    string
	sensorsJSON    string
}

type outStateDuration struct {
	enteredAtMS int64
	exitedAtMS  int64
	durationMS  int64
	orderKey    string
	state       string
	entryReason string
	exitReason  string
}

type outStepDwell struct {
	startedAtMS     int64
	endedAtMS       int64
	durationMS      int64
	orderKey        string
	lane            string
	state           string
	step            *int64
	seqIndex        int64
	previousState   string
	previousStep    *int64
	eventCount      int
	ioEventCount    int
	transitionCount int
	faultCount      int
	sensorsStart    string
	sensorsEnd      string
	sensorsTrace    string // every input edge during the step, verbatim
	actuatorsJSON   string // full per-output breakdown, verbatim
	sourceKind      string // step_runs | derived
	faultType       string // the fault that interrupted this step; step_runs-sourced only
	faultMessage    string
}

type outStepActuator struct {
	stepStartedAtMS   int64
	stepEndedAtMS     int64
	orderKey          string
	lane              string
	state             string
	step              *int64
	seqIndex          int64
	outputID          string
	outputName        string
	totalRunMS        int64
	segmentCount      int
	recipeStep        *int64
	recipeOriginState string
}

type outFaultEvent struct {
	raisedAtMS    int64
	clearedAtMS   *int64
	downtimeMS    *int64
	orderKey      string
	faultKey      string
	faultType     string
	severity      string
	recovered     *int64
	recoveryAtMS  *int64
	state         string
	step          int64
	message       string
	dwellSeqIndex *int64
}

type outSensorToggle struct {
	eventAtMS    int64
	orderKey     string
	srcID        int64
	inputID      string
	inputName    string
	valueFrom    int64
	valueTo      int64
	currentState string
	currentStep  *int64
}

type outDoorEvent struct {
	openedAtMS      int64
	closedAtMS      int64
	durationMS      int64
	orderKey        string
	faultResetCount int
	firstResetAtMS  *int64
	lastResetAtMS   *int64
	msToFirstReset  *int64
	stateAtOpen     string
	stepAtOpen      *int64
	stateAtClose    string
	stepAtClose     *int64
	openedInError   bool
}

type outConfigChange struct {
	changedAtMS int64
	orderKey    string
	configKey   string
	oldValue    string
	newValue    string
}

type outActuatorInterval struct {
	startedAtMS      int64
	endedAtMS        int64
	durationMS       int64
	orderKey         string
	srcID            int64
	revision         int64
	outputID         string
	outputName       string
	startedState     string
	startedStep      *int64
	startedSendOK    *int64
	endedState       string
	endedStep        *int64
	endedSendOK      *int64
	faultType        string
	faultMessage     string
	faultRaisedAtMS  *int64
	faultClearedAtMS *int64
}

// cycleContext is the parent's denormalized attributes, copied onto every child row so the
// admin dashboard never joins.
type cycleContext struct {
	recipeID     *int64
	isProduction int64
	result       string
	startedAtMS  int64
}

// ---------------------------------------------------------------------------
// Derived helpers
// ---------------------------------------------------------------------------

// stateByName reverses fsm.State.String(). Tilter and Crusher are deliberately absent:
// they are parallel sub-FSMs the firmware reports through CurrentState, not members of the
// State enum, so they have no step metadata to look up.
var stateByName = map[string]fsm.State{
	"HomeIdle":    fsm.StateHomeIdle,
	"Manual":      fsm.StateManual,
	"Error":       fsm.StateError,
	"Maintenance": fsm.StateMaintenance,
	"AutoCycle":   fsm.StateAutoCycle,
}

// stepTitle resolves a human label from the firmware's own metadata, so the label can never
// drift from the machine's behaviour the way a hand-copied frontend table does.
func stepTitle(state string, step *int64) string {
	if step == nil {
		return ""
	}
	s, ok := stateByName[state]
	if !ok {
		return ""
	}
	return fsm.GetStepDescription(s, int(*step))
}

// downtimeStates are the states that count against availability.
func isDowntimeState(state string) int64 {
	switch state {
	case "Error", "Maintenance":
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

type replicaWriter struct {
	db *sql.DB
	st *stateStore
}

func openReplica(path string, st *stateStore) (*replicaWriter, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open replica: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(replicaSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init replica schema: %w", err)
	}
	if err := migrateReplica(db); err != nil {
		db.Close()
		return nil, err
	}
	return &replicaWriter{db: db, st: st}, nil
}

func migrateReplica(db *sql.DB) error {
	migrations := []struct{ table, column, ddl, backfill string }{
		{"step_dwells", "fault_type", `ALTER TABLE step_dwells ADD COLUMN fault_type TEXT`, ""},
		{"step_dwells", "fault_message", `ALTER TABLE step_dwells ADD COLUMN fault_message TEXT`, ""},

		// The step's sensor snapshot, as bits. The JSON forms are held back from replication
		// at ~820 B/row, so without these the cloud has no sensor state for a step at all —
		// and fsm_events, which carries the only other copy, is the table this schema is
		// working towards not replicating.
		{"step_dwells", "sensors_start_bits",
			`ALTER TABLE step_dwells ADD COLUMN sensors_start_bits TEXT`, ""},
		{"step_dwells", "sensors_end_bits",
			`ALTER TABLE step_dwells ADD COLUMN sensors_end_bits TEXT`, ""},
		{"step_dwells", "door_closed",
			`ALTER TABLE step_dwells ADD COLUMN door_closed INTEGER`, ""},
		{"step_dwells", "cip_bypass",
			`ALTER TABLE step_dwells ADD COLUMN cip_bypass INTEGER`, ""},

		// The outcome the charts stack, as a column rather than eight result values the
		// dashboard has to collapse itself. QueryScript has no CASE, so without this every
		// orders chart reimplements the mapping in JavaScript and the copies drift.
		{"cycles", "outcome",
			`ALTER TABLE cycles ADD COLUMN outcome TEXT NOT NULL DEFAULT ''`,
			`UPDATE cycles SET outcome = CASE
			     WHEN is_production = 0                 THEN 'non_production'
			     WHEN result = 'completed'              THEN 'success'
			     WHEN result = 'faulted_recoverable'    THEN 'recovered'
			     ELSE 'failed'
			 END
			 WHERE outcome = ''`},
	}

	// Bucket keys, so a time series is a GROUP BY rather than a client-side loop.
	// QueryScript has no time_bucket(); these are the widths the dashboard's presets use.
	//
	// Backfilled from each row's own start timestamp — a DEFAULT of 0 would put every
	// pre-existing row in the same bucket at the epoch, which is worse than not having the
	// column at all.
	for _, b := range []struct{ table, startCol string }{
		{"cycles", "started_at_ms"},
		{"fault_events", "raised_at_ms"},
		{"step_dwells", "started_at_ms"},
		{"sensor_toggles", "event_at_ms"},
	} {
		for _, w := range []struct {
			col     string
			widthMS int64
		}{
			{"bucket_1m_ms", 60_000},
			{"bucket_5m_ms", 5 * 60_000},
			{"bucket_15m_ms", 15 * 60_000},
		} {
			migrations = append(migrations, struct{ table, column, ddl, backfill string }{
				table:  b.table,
				column: w.col,
				ddl: fmt.Sprintf(
					`ALTER TABLE %s ADD COLUMN %s INTEGER NOT NULL DEFAULT 0`, b.table, w.col),
				backfill: fmt.Sprintf(
					`UPDATE %s SET %s = %s - (%s %% %d) WHERE %s = 0`,
					b.table, w.col, b.startCol, b.startCol, w.widthMS, w.col),
			})
		}
	}

	// The four rollup tables became one. An existing replica is migrated by copying its rows
	// across, not by dropping and re-deriving: the source rows behind old buckets may already
	// have been pruned from the controller, so a re-derivation would silently lose history.
	//
	// The copy runs in the same step that creates the table, which also sidesteps the agent
	// bug that has already cost these four tables once — a table created empty and populated
	// later is skipped, its cursor fast-forwarded past everything that arrives afterwards.
	// Created-and-populated in one go, there is no empty window to observe.
	if err := migrateRollups(db); err != nil {
		return err
	}

	for _, m := range migrations {
		has, err := replicaHasColumn(db, m.table, m.column)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		if _, err := db.Exec(m.ddl); err != nil {
			return fmt.Errorf("migrate replica %s.%s: %w", m.table, m.column, err)
		}
		if m.backfill == "" {
			continue
		}
		if _, err := db.Exec(m.backfill); err != nil {
			return fmt.Errorf("backfill replica %s.%s: %w", m.table, m.column, err)
		}
	}
	return nil
}

// migrateRollups folds hourly_rollups, daily_rollups, hourly_fault_counts and
// hourly_step_stats into the single rollups table.
//
// Each source is checked for independently rather than assuming all four are present. That is
// not defensive padding: the agent bug in §11 has already left one of these tables missing
// from a live deployment, so "hourly_rollups exists" says nothing about the other three.
func migrateRollups(db *sql.DB) error {
	copies := []struct{ from, sql string }{
		{"hourly_rollups", `INSERT OR IGNORE INTO rollups
		     (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
		      glasses, orders_started, orders_completed, orders_faulted, fault_count, cip_runs,
		      cycle_ms_sum, cycle_count, run_ms, error_ms, maintenance_ms, idle_ms)
		 SELECT event_ts, 'hour', bucket_start_ms, date_utc, 'machine', NULL,
		      glasses, orders_started, orders_completed, orders_faulted, fault_count, cip_runs,
		      cycle_ms_sum, cycle_count, run_ms, error_ms, maintenance_ms, idle_ms
		 FROM hourly_rollups`},

		{"daily_rollups", `INSERT OR IGNORE INTO rollups
		     (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
		      glasses, orders_completed, orders_faulted, fault_count,
		      cycle_ms_sum, cycle_count, run_ms, error_ms, maintenance_ms, idle_ms)
		 SELECT event_ts, 'day', CAST(strftime('%s', date_utc) AS INTEGER) * 1000, date_utc,
		      'machine', NULL,
		      glasses, orders_completed, orders_faulted, fault_count,
		      cycle_ms_sum, cycle_count, run_ms, error_ms, maintenance_ms, idle_ms
		 FROM daily_rollups`},

		{"hourly_fault_counts", `INSERT OR IGNORE INTO rollups
		     (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
		      severity, occurrences, duration_ms_sum)
		 SELECT event_ts, 'hour', bucket_start_ms, date_utc, 'fault_type', fault_type,
		      severity, occurrences, downtime_ms_sum
		 FROM hourly_fault_counts`},

		{"hourly_step_stats", `INSERT OR IGNORE INTO rollups
		     (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
		      lane, occurrences, duration_ms_sum, duration_ms_max)
		 SELECT event_ts, 'hour', bucket_start_ms, date_utc, 'step',
		      CASE WHEN step IS NULL THEN state ELSE state || '/' || step END,
		      lane, dwell_count, duration_ms_sum, duration_ms_max
		 FROM hourly_step_stats`},
	}

	migrated := 0
	for _, c := range copies {
		var exists int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, c.from,
		).Scan(&exists); err != nil {
			return fmt.Errorf("probe %s: %w", c.from, err)
		}
		if exists == 0 {
			continue
		}
		if _, err := db.Exec(c.sql); err != nil {
			return fmt.Errorf("migrate %s into rollups: %w", c.from, err)
		}
		// Dropped rather than left behind: a table the projector no longer writes would keep
		// its replication cursor and look like a live feed that has simply gone quiet.
		if _, err := db.Exec(`DROP TABLE ` + c.from); err != nil {
			return fmt.Errorf("drop %s: %w", c.from, err)
		}
		migrated++
	}
	if migrated > 0 {
		log.Printf("projector: folded %d rollup table(s) into rollups", migrated)
	}
	return nil
}

func replicaHasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%q)`, table))
	if err != nil {
		return false, fmt.Errorf("probe replica %s.%s: %w", table, column, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid, notnull, pk int
			name, typ        string
			dflt             sql.NullString
		)
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return false, fmt.Errorf("probe replica %s.%s: %w", table, column, err)
		}
		if name == column {
			return true, rows.Err()
		}
	}
	return false, rows.Err()
}

func (w *replicaWriter) Close() error { return w.db.Close() }

// eventTS renders the emit timestamp.
//
// Fixed width matters: the agent's cursor is `ts > ? OR (ts = ? AND rowid > ?)`, which is a
// string comparison. time.RFC3339Nano strips trailing zeros and would break that ordering, so
// the fractional part is forced to nine digits and the Z is appended literally rather than
// via a zone specifier.
func eventTS(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000000000") + "Z"
}

// hourBucket floors a timestamp to its UTC hour. Pre-computing it turns every
// "group by hour" in the admin dashboard from an expression into an indexed column.
func hourBucket(ms int64) int64 {
	const hourMS = int64(time.Hour / time.Millisecond)
	return ms - (ms % hourMS)
}

// Sub-hour bucket keys.
//
// QueryScript, which the dashboard queries through, has no time_bucket() and no window
// functions — so without a column to GROUP BY, every time series is built by pulling raw
// rows and bucketing them in the browser. These are the three widths the dashboard's own
// presets select (1 minute for a 5-minute window, 5 for an hour, 15 for eight hours);
// anything coarser is served from the rollups instead.
func bucketMS(ms, widthMS int64) int64 { return ms - (ms % widthMS) }

func bucket1m(ms int64) int64  { return bucketMS(ms, 60_000) }
func bucket5m(ms int64) int64  { return bucketMS(ms, 5*60_000) }
func bucket15m(ms int64) int64 { return bucketMS(ms, 15*60_000) }

// outcomeFor collapses the eight result values into the three the charts stack.
//
// `result` stays as the audit trail — it distinguishes an aborted cycle from a
// non-recoverable fault, which matters when diagnosing. `outcome` is the serving column,
// and it exists because QueryScript has no CASE: without it the mapping lives in the
// dashboard, in as many copies as there are charts.
func outcomeFor(result string, isProduction bool) string {
	if !isProduction {
		return "non_production"
	}
	switch result {
	case resultCompleted:
		return "success"
	case resultFaultedRecoverable:
		return "recovered"
	default:
		// faulted_non_recoverable and aborted both mean the drink was not made. The old
		// dashboard had no bucket for aborted at all, so folding it in here matches what
		// the charts already showed rather than inventing a fourth series.
		return "failed"
	}
}

// dateUTC is the UTC calendar date. Everything in this pipeline is UTC; there is no shift
// dimension because the controller has no shift concept to derive one from.
func dateUTC(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02")
}

// guard enforces the one invariant that silently destroys data if broken: event_ts must never
// move backwards within a table. Equal timestamps are fine — the agent's cursor breaks ties on
// rowid — so a violation is clamped forward to the watermark rather than rejected, which keeps
// the row rather than dropping it.
func (w *replicaWriter) guard(table string, ms int64) (string, error) {
	ts := eventTS(ms)
	last, err := w.st.Watermark(table)
	if err != nil {
		return "", err
	}
	if last != "" && ts < last {
		logf("event_ts would go backwards in %s (%s < %s); clamping", table, ts, last)
		ts = last
	}
	return ts, nil
}

// FlushInterval writes one interval and all of its children in a single transaction, parent
// first. That ordering is what makes the foreign keys satisfiable — a child cannot be written
// before the cycles row it references exists.
//
// It is also all-or-nothing: kill the projector mid-flush and the interval is either fully
// present or fully absent, never half-written.
func (w *replicaWriter) FlushInterval(ctx context.Context, iv *interval) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	cycleTS, err := w.guard("cycles", iv.endedMS)
	if err != nil {
		return err
	}

	prod := boolToInt(iv.isProduction)

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO cycles (event_ts, started_at_ms, ended_at_ms, duration_ms, order_id,
		                     is_production, result, recipe_id, glass_count,
		                     terminal_state, terminal_step,
		                     fault_count, dominant_fault_type, first_fault_at_ms, last_fault_at_ms,
		                     fsm_event_count, step_event_count, state_transition_count,
		                     unique_state_count, outcome,
		                     bucket_1m_ms, bucket_5m_ms, bucket_15m_ms, date_utc)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(order_id) DO UPDATE SET
		     recipe_id   = COALESCE(cycles.recipe_id,   excluded.recipe_id),
		     glass_count = COALESCE(cycles.glass_count, excluded.glass_count)`,
		cycleTS, iv.startedMS, iv.endedMS, iv.endedMS-iv.startedMS, iv.orderKey,
		prod, iv.result, iv.recipeID, iv.glassCount,
		nullIfEmpty(iv.terminalState), iv.terminalStep,
		iv.faultCount, nullIfEmpty(iv.dominantFaultType), iv.firstFaultMS, iv.lastFaultMS,
		iv.fsmEventCount, iv.stepEventCount, iv.stateTransitionCount, len(iv.uniqueStates),
		outcomeFor(iv.result, iv.isProduction),
		bucket1m(iv.startedMS), bucket5m(iv.startedMS), bucket15m(iv.startedMS),
		dateUTC(iv.startedMS),
	); err != nil {
		return fmt.Errorf("insert cycle %s: %w", iv.orderKey, err)
	}

	// Fill the order metadata into the children that denormalise it.
	//
	// `orders` and the FSM tables have independent read cursors, so the row carrying
	// recipe and quantity can arrive after the cycle it describes was already written and
	// closed. The conflict clause above fills the parent; without this the children keep
	// the null they were written with, and the dashboard sees a cycle with a recipe whose
	// own dwells and faults disagree.
	//
	// Only ever fills a null — a value already recorded is never overwritten, so this stays
	// idempotent across re-runs and cannot corrupt settled rows.
	if iv.recipeID != nil || iv.glassCount != nil {
		for _, child := range []string{
			"step_dwells", "fault_events", "door_events", "actuator_intervals",
		} {
			if _, err := tx.ExecContext(ctx,
				fmt.Sprintf(
					`UPDATE %s SET recipe_id = ? WHERE order_id = ? AND recipe_id IS NULL`,
					child),
				iv.recipeID, iv.orderKey,
			); err != nil {
				return fmt.Errorf("backfill %s.recipe_id for %s: %w", child, iv.orderKey, err)
			}
		}
	}

	maxTS := cycleTS
	track := func(ts string) {
		if ts > maxTS {
			maxTS = ts
		}
	}

	// fsm_events is deliberately not replicated. It was the largest table by an order of
	// magnitude, every row of it derived into step_dwells, state_durations and fault_events
	// before it left the device, and on the live machine it carried no production orders at
	// all. The raw stream stays in the controller's own database for local drill-down.
	//
	// iv.events is still collected: the per-cycle counters on `cycles` are computed from it.

	for _, s := range iv.stateDurations {
		ts := eventTS(s.exitedAtMS)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO state_durations (event_ts, entered_at_ms, exited_at_ms, duration_ms,
			                              order_id, state, entry_reason, exit_reason,
			                              is_downtime, is_production, date_utc)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			ts, s.enteredAtMS, s.exitedAtMS, s.durationMS,
			s.orderKey, s.state, nullIfEmpty(s.entryReason), nullIfEmpty(s.exitReason),
			isDowntimeState(s.state), prod,
			dateUTC(s.enteredAtMS),
		); err != nil {
			return fmt.Errorf("insert state_duration: %w", err)
		}
		track(ts)
	}

	for _, d := range iv.stepDwells {
		ts := eventTS(d.endedAtMS)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO step_dwells (event_ts, started_at_ms, ended_at_ms, duration_ms,
			                          order_id, lane, state, step, step_title, seq_index,
			                          previous_state, previous_step,
			                          event_count, io_event_count, transition_count, fault_count,
			                          sensors_start_bits, sensors_end_bits, door_closed, cip_bypass,
			                          sensors_start_json, sensors_end_json,
			                          actuators_json, source_kind,
			                          fault_type, fault_message,
			                          recipe_id, is_production, cycle_result, cycle_started_at_ms,
			                          bucket_1m_ms, bucket_5m_ms, bucket_15m_ms,
			                          date_utc)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			ts, d.startedAtMS, d.endedAtMS, d.durationMS,
			d.orderKey, d.lane, d.state, d.step, nullIfEmpty(stepTitle(d.state, d.step)), d.seqIndex,
			nullIfEmpty(d.previousState), d.previousStep,
			d.eventCount, d.ioEventCount, d.transitionCount, d.faultCount,
			nullIfEmpty(encodeSensorBits(d.sensorsStart)), nullIfEmpty(encodeSensorBits(d.sensorsEnd)),
			doorClosed(d.sensorsStart), cipBypass(d.sensorsStart),
			nullIfEmpty(d.sensorsStart), nullIfEmpty(d.sensorsEnd),
			nullIfEmpty(d.actuatorsJSON), d.sourceKind,
			nullIfEmpty(d.faultType), nullIfEmpty(d.faultMessage),
			iv.recipeID, prod, iv.result, iv.startedMS,
			bucket1m(d.startedAtMS), bucket5m(d.startedAtMS), bucket15m(d.startedAtMS),
			dateUTC(d.startedAtMS),
		); err != nil {
			return fmt.Errorf("insert step_dwell: %w", err)
		}
		track(ts)
	}

	// step_actuators was per-output totals inside one step run. It is not written any more:
	// actuator_intervals already holds every pulse, and the totals are a GROUP BY over it.

	for _, f := range iv.faults {
		at := f.raisedAtMS
		if f.clearedAtMS != nil {
			at = *f.clearedAtMS
		}
		ts := eventTS(at)
		var stepPtr = f.step
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO fault_events (event_ts, raised_at_ms, cleared_at_ms, downtime_ms,
			                           order_id, fault_key, fault_type, severity,
			                           recovered, recovery_at_ms, state, step, step_title,
			                           message, dwell_seq_index,
			                           recipe_id, is_production, cycle_result, cycle_started_at_ms,
			                           bucket_1m_ms, bucket_5m_ms, bucket_15m_ms,
			                           date_utc)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(fault_key) DO NOTHING`,
			ts, f.raisedAtMS, f.clearedAtMS, f.downtimeMS,
			f.orderKey, f.faultKey, f.faultType, f.severity,
			f.recovered, f.recoveryAtMS, nullIfEmpty(f.state), f.step,
			nullIfEmpty(stepTitle(f.state, &stepPtr)),
			nullIfEmpty(f.message), f.dwellSeqIndex,
			iv.recipeID, prod, iv.result, iv.startedMS,
			bucket1m(f.raisedAtMS), bucket5m(f.raisedAtMS), bucket15m(f.raisedAtMS),
			dateUTC(f.raisedAtMS),
		); err != nil {
			return fmt.Errorf("insert fault_event: %w", err)
		}
		track(ts)
	}

	for _, s := range iv.toggles {
		ts := eventTS(s.eventAtMS)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO sensor_toggles (event_ts, event_at_ms, order_id, src_id,
			                             input_id, input_name, value_from, value_to,
			                             current_state, current_step,
			                             is_production,
			                             bucket_1m_ms, bucket_5m_ms, bucket_15m_ms,
			                             date_utc)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			ts, s.eventAtMS, s.orderKey, s.srcID,
			s.inputID, s.inputName, s.valueFrom, s.valueTo,
			nullIfEmpty(s.currentState), s.currentStep,
			prod,
			bucket1m(s.eventAtMS), bucket5m(s.eventAtMS), bucket15m(s.eventAtMS),
			dateUTC(s.eventAtMS),
		); err != nil {
			return fmt.Errorf("insert sensor_toggle: %w", err)
		}
		track(ts)
	}

	for _, d := range iv.doorEvents {
		ts := eventTS(d.closedAtMS)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO door_events (event_ts, opened_at_ms, closed_at_ms, duration_ms,
			                          order_id, fault_reset_during, fault_reset_count,
			                          first_reset_at_ms, last_reset_at_ms, ms_to_first_reset,
			                          state_at_open, step_at_open, state_at_close, step_at_close,
			                          opened_in_error,
			                          recipe_id, is_production, cycle_result,
			                          date_utc)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			ts, d.openedAtMS, d.closedAtMS, d.durationMS,
			d.orderKey, boolToInt(d.faultResetCount > 0), d.faultResetCount,
			d.firstResetAtMS, d.lastResetAtMS, d.msToFirstReset,
			nullIfEmpty(d.stateAtOpen), d.stepAtOpen,
			nullIfEmpty(d.stateAtClose), d.stepAtClose,
			boolToInt(d.openedInError),
			iv.recipeID, prod, iv.result,
			dateUTC(d.openedAtMS),
		); err != nil {
			return fmt.Errorf("insert door_event: %w", err)
		}
		track(ts)
	}

	for _, c := range iv.configs {
		ts := eventTS(c.changedAtMS)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO config_history (event_ts, changed_at_ms, order_id, config_key,
			                             old_value, new_value, date_utc)
			 VALUES (?,?,?,?,?,?,?)`,
			ts, c.changedAtMS, c.orderKey, c.configKey,
			nullIfEmpty(c.oldValue), nullIfEmpty(c.newValue),
			dateUTC(c.changedAtMS),
		); err != nil {
			return fmt.Errorf("insert config_history: %w", err)
		}
		track(ts)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit interval %s: %w", iv.orderKey, err)
	}

	// Watermarks advance only after a successful commit, so a crash mid-flush replays
	// rather than skips.
	w.st.SetWatermark("cycles", cycleTS)
	for _, tbl := range []string{"state_durations", "step_dwells",
		"fault_events", "sensor_toggles", "config_history",
		"door_events"} {
		w.st.SetWatermark(tbl, maxTS)
	}
	return nil
}

// WriteActuatorInterval writes a closed actuator interval. These close independently of the
// cycle that spawned them, so they are written on their own — safe because the FK parent is
// already present by then, and the denormalized context is read back from it.
func (w *replicaWriter) WriteActuatorInterval(ctx context.Context, a outActuatorInterval, cc cycleContext) error {
	ts, err := w.guard("actuator_intervals", a.endedAtMS)
	if err != nil {
		return err
	}
	if _, err := w.db.ExecContext(ctx,
		`INSERT INTO actuator_intervals (event_ts, started_at_ms, ended_at_ms, duration_ms,
		                                 order_id, src_id, revision, output_id, output_name,
		                                 started_state, started_step, started_send_ok,
		                                 ended_state, ended_step, ended_send_ok,
		                                 fault_type, fault_message,
		                                 fault_raised_at_ms, fault_cleared_at_ms,
		                                 recipe_id, is_production, cycle_result,
		                                 date_utc)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		ts, a.startedAtMS, a.endedAtMS, a.durationMS,
		a.orderKey, a.srcID, a.revision, a.outputID, a.outputName,
		nullIfEmpty(a.startedState), a.startedStep, a.startedSendOK,
		nullIfEmpty(a.endedState), a.endedStep, a.endedSendOK,
		nullIfEmpty(a.faultType), nullIfEmpty(a.faultMessage),
		a.faultRaisedAtMS, a.faultClearedAtMS,
		cc.recipeID, cc.isProduction, nullIfEmpty(cc.result),
		dateUTC(a.startedAtMS),
	); err != nil {
		return fmt.Errorf("insert actuator_interval src=%d: %w", a.srcID, err)
	}
	w.st.SetWatermark("actuator_intervals", ts)
	return nil
}

// CycleContext returns the parent's denormalized attributes, or ok=false when the parent has
// not been written yet — in which case the caller must defer rather than orphan the row.
func (w *replicaWriter) CycleContext(ctx context.Context, orderKey string) (cycleContext, bool, error) {
	var cc cycleContext
	err := w.db.QueryRowContext(ctx,
		`SELECT recipe_id, is_production, result, started_at_ms FROM cycles WHERE order_id = ?`,
		orderKey).Scan(&cc.recipeID, &cc.isProduction, &cc.result, &cc.startedAtMS)
	if err == sql.ErrNoRows {
		return cc, false, nil
	}
	if err != nil {
		return cc, false, err
	}
	return cc, true, nil
}

// RecordGap records a stretch where the projector was not running, so downstream can tell
// missing data from a genuinely idle machine.
// It also carries the projector build and the controller schema it detected. Those used to
// live on projector_runs, whose only other content was a start timestamp and a stopped_at_ms
// that was NULL on every row ever written — the run table never recorded a clean shutdown, so
// it answered the same question as this one, worse.
func (w *replicaWriter) RecordGap(
	ctx context.Context, startedMS, endedMS int64, reason, version, branch string,
) error {
	ts, err := w.guard("gaps", endedMS)
	if err != nil {
		return err
	}
	if _, err := w.db.ExecContext(ctx,
		`INSERT INTO gaps (event_ts, started_at_ms, ended_at_ms, duration_ms, order_id, reason,
		                   version, source_branch)
		 VALUES (?,?,?,?,?,?,?,?)`,
		ts, startedMS, endedMS, endedMS-startedMS, nil, reason,
		nullIfEmpty(version), nullIfEmpty(branch)); err != nil {
		return err
	}
	w.st.SetWatermark("gaps", ts)
	return nil
}

// AttributeActuatorPulses links each pulse to the step dwell it fell inside.
//
// The two arrive on separate read cursors — pulses from actuator_output_intervals, dwells from
// fsm_step_runs or derived from the event stream — so a pulse cannot be attributed at the
// moment it is written. Doing it as a set operation afterwards is both simpler and correct
// regardless of which arrived first.
//
// Only fills a null, so it is idempotent and cannot renumber a pulse that was already placed.
func (w *replicaWriter) AttributeActuatorPulses(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE actuator_intervals
		   SET seq_index = (
		       SELECT d.seq_index FROM step_dwells d
		        WHERE d.order_id = actuator_intervals.order_id
		          AND actuator_intervals.started_at_ms >= d.started_at_ms
		          AND actuator_intervals.started_at_ms <  d.ended_at_ms
		        ORDER BY d.started_at_ms LIMIT 1)
		 WHERE seq_index IS NULL`)
	if err != nil {
		return fmt.Errorf("attribute actuator pulses: %w", err)
	}
	return nil
}

// Prune drops rows older than the retention window. The replica is a buffer, not the archive
// — once the agent's cursor has passed a row it has been delivered, and the permanent copy
// lives in the cloud. Without this the file grows forever, which is exactly the problem the
// source database already has.
func (w *replicaWriter) Prune(ctx context.Context, olderThan time.Time) error {
	cutoff := eventTS(olderThan.UnixMilli())
	tables := []string{"sensor_toggles", "config_history", "state_durations",
		"step_dwells", "fault_events", "actuator_intervals", "cip_runs",
		"door_events", "rollups", "gaps"}
	for _, t := range tables {
		if _, err := w.db.ExecContext(ctx,
			fmt.Sprintf(`DELETE FROM %s WHERE event_ts < ?`, t), cutoff); err != nil {
			return fmt.Errorf("prune %s: %w", t, err)
		}
	}
	// cycles last: it is the FK parent, so its children must be gone first.
	if _, err := w.db.ExecContext(ctx, `DELETE FROM cycles WHERE event_ts < ?`, cutoff); err != nil {
		return fmt.Errorf("prune cycles: %w", err)
	}
	return nil
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
