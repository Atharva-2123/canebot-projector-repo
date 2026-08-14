package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// sourceReader reads the controller's database. It is strictly read-only: the DSN carries
// mode=ro, and every statement here is a SELECT.
//
// Two constraints shape this file.
//
// First, the source runs in SQLite's default rollback-journal mode (store/store.go sets only
// busy_timeout). A reader holds a SHARED lock and a committing writer needs EXCLUSIVE, so a
// long read delays the controller's writes. Every read here is therefore a small bounded
// batch that closes immediately — the caller does its derivation work after the rows are in
// memory, never with a transaction still open.
//
// Second, there is no index on fsm_events at all, so every read is keyed on the `id` primary
// key (which uses the implicit rowid index). A timestamp-ranged read would full-scan.
type sourceReader struct {
	db        *sql.DB
	batchSize int

	// stepRunFaults is set when fsm_step_runs carries fault_type/fault_message. Those
	// columns arrive with a firmware migration, so a controller that has not restarted
	// since the upgrade still has the old shape and must not be queried for them.
	stepRunFaults bool
}

func openSource(path string, batchSize int) (*sourceReader, error) {
	// mode=ro is enforced by SQLite itself: any write attempt fails rather than silently
	// mutating a database we do not own. immutable=0 because the controller is actively
	// writing to it.
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}
	// One connection keeps our lock footprint predictable; we are not trying to parallelise
	// reads against a database someone else is writing.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping source: %w", err)
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &sourceReader{db: db, batchSize: batchSize}, nil
}

func (r *sourceReader) Close() error { return r.db.Close() }

// ---------------------------------------------------------------------------
// Row types — mirrors of the source tables, with timestamps already parsed.
// ---------------------------------------------------------------------------

type srcFSMEvent struct {
	ID             int64
	TS             time.Time
	EventKind      string
	StateFrom      string
	StateTo        string
	CurrentState   string
	StepFrom       *int64
	StepTo         *int64
	ModbusOrderReg *int64
	InputID        string
	InputValue     *int64
	EventType      string
	PayloadJSON    string
	Source         string
	TraceID        string
	OrderKey       string
	SensorsJSON    string
}

type srcFault struct {
	ID        int64
	TS        time.Time
	State     string
	Step      int64
	FaultType string
	Message   string
	OrderKey  string
}

type srcOrder struct {
	ID       int64
	OrderKey string
	Recipe   int64
	Quantity int64
	TS       time.Time
}

type srcActuatorInterval struct {
	ID            int64
	OutputID      string
	OutputName    string
	OrderKey      string
	Started       time.Time
	StartedState  string
	StartedStep   *int64
	StartedSendOK *int64
	Ended         time.Time
	EndedState    string
	EndedStep     *int64
	EndedSendOK   *int64
	DurationMS    *int64
	FaultRaised   *time.Time
	FaultType     string
	FaultMessage  string
	FaultCleared  *time.Time
}

type srcSensorToggle struct {
	ID           int64
	TS           time.Time
	InputID      string
	InputName    string
	ValueFrom    int64
	ValueTo      int64
	CurrentState string
	CurrentStep  *int64
	OrderKey     string
}

// ---------------------------------------------------------------------------
// Batched reads, all keyed on the id primary key.
// ---------------------------------------------------------------------------

func (r *sourceReader) FSMEventsAfter(ctx context.Context, afterID int64) ([]srcFSMEvent, error) {
	const q = `SELECT id, ts_utc, event_kind,
	                  COALESCE(state_from,''), COALESCE(state_to,''), COALESCE(current_state,''),
	                  step_from, step_to, order_id,
	                  COALESCE(input_id,''), input_value,
	                  COALESCE(event_type,''), COALESCE(payload_json,''),
	                  COALESCE(source,''), COALESCE(trace_id,''),
	                  COALESCE(order_key,''), COALESCE(sensors_json,'')
	           FROM fsm_events WHERE id > ? ORDER BY id LIMIT ?`

	rows, err := r.db.QueryContext(ctx, q, afterID, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("read fsm_events: %w", err)
	}
	defer rows.Close()

	var out []srcFSMEvent
	for rows.Next() {
		var e srcFSMEvent
		var ts string
		if err := rows.Scan(&e.ID, &ts, &e.EventKind,
			&e.StateFrom, &e.StateTo, &e.CurrentState,
			&e.StepFrom, &e.StepTo, &e.ModbusOrderReg,
			&e.InputID, &e.InputValue,
			&e.EventType, &e.PayloadJSON,
			&e.Source, &e.TraceID,
			&e.OrderKey, &e.SensorsJSON); err != nil {
			return nil, fmt.Errorf("scan fsm_events: %w", err)
		}
		if e.TS, err = parseSourceTime(ts); err != nil {
			// A single unparseable timestamp must not stall the whole pipeline behind it.
			// Skipping advances the cursor past the row; the raw row is still in the source.
			logf("skipping fsm_events id=%d: %v", e.ID, err)
			continue
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *sourceReader) FaultsAfter(ctx context.Context, afterID int64) ([]srcFault, error) {
	const q = `SELECT id, ts_utc, state, step,
	                  COALESCE(fault_type,''), COALESCE(message,''), COALESCE(order_key,'')
	           FROM faults WHERE id > ? ORDER BY id LIMIT ?`

	rows, err := r.db.QueryContext(ctx, q, afterID, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("read faults: %w", err)
	}
	defer rows.Close()

	var out []srcFault
	for rows.Next() {
		var f srcFault
		var ts string
		if err := rows.Scan(&f.ID, &ts, &f.State, &f.Step, &f.FaultType, &f.Message, &f.OrderKey); err != nil {
			return nil, fmt.Errorf("scan faults: %w", err)
		}
		if f.TS, err = parseSourceTime(ts); err != nil {
			logf("skipping faults id=%d: %v", f.ID, err)
			continue
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (r *sourceReader) OrdersAfter(ctx context.Context, afterID int64) ([]srcOrder, error) {
	const q = `SELECT id, order_id, order_recipe, order_quantity, ts_utc
	           FROM orders WHERE id > ? ORDER BY id LIMIT ?`

	rows, err := r.db.QueryContext(ctx, q, afterID, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("read orders: %w", err)
	}
	defer rows.Close()

	var out []srcOrder
	for rows.Next() {
		var o srcOrder
		var ts string
		if err := rows.Scan(&o.ID, &o.OrderKey, &o.Recipe, &o.Quantity, &ts); err != nil {
			return nil, fmt.Errorf("scan orders: %w", err)
		}
		if o.TS, err = parseSourceTime(ts); err != nil {
			logf("skipping orders id=%d: %v", o.ID, err)
			continue
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (r *sourceReader) SensorTogglesAfter(ctx context.Context, afterID int64) ([]srcSensorToggle, error) {
	const q = `SELECT id, ts_utc, input_id, input_display_name, value_from, value_to,
	                  COALESCE(current_state,''), current_step, COALESCE(order_key,'')
	           FROM sensor_input_toggles WHERE id > ? ORDER BY id LIMIT ?`

	rows, err := r.db.QueryContext(ctx, q, afterID, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("read sensor_input_toggles: %w", err)
	}
	defer rows.Close()

	var out []srcSensorToggle
	for rows.Next() {
		var s srcSensorToggle
		var ts string
		if err := rows.Scan(&s.ID, &ts, &s.InputID, &s.InputName, &s.ValueFrom, &s.ValueTo,
			&s.CurrentState, &s.CurrentStep, &s.OrderKey); err != nil {
			return nil, fmt.Errorf("scan sensor_input_toggles: %w", err)
		}
		if s.TS, err = parseSourceTime(ts); err != nil {
			logf("skipping sensor_input_toggles id=%d: %v", s.ID, err)
			continue
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ClosedActuatorIntervalsAfter returns only intervals that have actually ended.
//
// This table is the one mutable table in the source: a row is inserted when an output turns
// ON and UPDATEd when it turns OFF. Reading it by `id > watermark` would ship a half-open
// row once and never see it complete, so this watermarks on the close time instead.
func (r *sourceReader) ClosedActuatorIntervalsAfter(ctx context.Context, afterEndedMS int64) ([]srcActuatorInterval, error) {
	const q = `SELECT id, output_id, output_name, COALESCE(order_key,''),
	                  started_ts_utc, COALESCE(started_state,''), started_step, started_send_ok,
	                  ended_ts_utc, COALESCE(ended_state,''), ended_step, ended_send_ok,
	                  duration_ms,
	                  fault_raised_ts_utc, COALESCE(fault_type,''), COALESCE(fault_message,''),
	                  fault_cleared_ts_utc
	           FROM actuator_output_intervals
	           WHERE ended_ts_utc IS NOT NULL AND TRIM(ended_ts_utc) <> ''
	           ORDER BY id LIMIT ?`

	// Deliberately over-fetch and filter in Go rather than compare timestamps in SQL: ts_utc
	// is RFC3339Nano with stripped trailing zeros, so a string comparison in SQL would be
	// wrong for exactly the values that differ in fractional-second width.
	rows, err := r.db.QueryContext(ctx, q, r.batchSize*4)
	if err != nil {
		return nil, fmt.Errorf("read actuator_output_intervals: %w", err)
	}
	defer rows.Close()

	var out []srcActuatorInterval
	for rows.Next() {
		var a srcActuatorInterval
		var startedTS, endedTS string
		var faultRaised, faultCleared sql.NullString
		if err := rows.Scan(&a.ID, &a.OutputID, &a.OutputName, &a.OrderKey,
			&startedTS, &a.StartedState, &a.StartedStep, &a.StartedSendOK,
			&endedTS, &a.EndedState, &a.EndedStep, &a.EndedSendOK,
			&a.DurationMS,
			&faultRaised, &a.FaultType, &a.FaultMessage,
			&faultCleared); err != nil {
			return nil, fmt.Errorf("scan actuator_output_intervals: %w", err)
		}
		if a.Started, err = parseSourceTime(startedTS); err != nil {
			logf("skipping actuator interval id=%d: %v", a.ID, err)
			continue
		}
		if a.Ended, err = parseSourceTime(endedTS); err != nil {
			logf("skipping actuator interval id=%d: %v", a.ID, err)
			continue
		}
		if a.Ended.UnixMilli() <= afterEndedMS {
			continue
		}
		a.FaultRaised = parseOptionalTime(faultRaised)
		a.FaultCleared = parseOptionalTime(faultCleared)
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sortActuatorsByEnd(out)
	if len(out) > r.batchSize {
		out = out[:r.batchSize]
	}
	return out, nil
}

// HasTable reports whether a source table exists. Three controller branches are in flight
// with different schemas, so capability is detected rather than assumed — one binary runs
// against main, test/tracking_fsm, and test/maintenance.
func (r *sourceReader) HasTable(ctx context.Context, name string) (bool, error) {
	var found string
	err := r.db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe table %s: %w", name, err)
	}
	return true, nil
}

// HasColumn reports whether a source column exists. Same reasoning as HasTable, one level
// down: fsm_step_runs.fault_type/fault_message arrive with a firmware migration, so a
// controller that has not restarted since the upgrade still has the old shape.
func (r *sourceReader) HasColumn(ctx context.Context, table, column string) (bool, error) {
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%q)`, table))
	if err != nil {
		return false, fmt.Errorf("probe column %s.%s: %w", table, column, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid, notnull, pk int
			name, typ        string
			dflt             sql.NullString
		)
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return false, fmt.Errorf("probe column %s.%s: %w", table, column, err)
		}
		if name == column {
			return true, rows.Err()
		}
	}
	return false, rows.Err()
}

// srcStepRun is one completed step as the controller itself recorded it, available on
// branches carrying fsm_step_runs. Strictly richer than pairing fsm_events ourselves: the
// firmware knows its own step boundaries, and it attributes actuator run time per step.
// FaultType/FaultMessage are set when a fault fired during this step. The firmware stashes
// them via OnFSMEventSent, which runs before the resulting setState(StateError), so they land
// on the dwell the fault interrupted (e.g. AutoCycle/10) rather than the Error dwell that
// follows. On a controller that records step runs but leaves `faults` empty, this is the only
// fault signal available — see tracker.ApplyStepRun.
type srcStepRun struct {
	ID            int64
	Started       time.Time
	Ended         time.Time
	CurrentState  string
	Step          int64
	PreviousState string
	PreviousStep  *int64
	OrderKey      string
	FaultType     string
	FaultMessage  string
	SensorsStart  string
	SensorsEnd    string
	SensorsTrace  string
	ActuatorsJSON string
}

// StepRunActuator mirrors one entry of the controller's actuators_json.
//
// The firmware declares this as `map[string]StepRunActuator` keyed by output_id
// (store/fsm_step_runs.go), so the JSON is an OBJECT — `{"Y0.1": {...}}` — not an array.
// The map key carries the output_id; the Name field inside is the display name.
type StepRunActuator struct {
	Name       string `json:"name"`
	TotalRunMS int64  `json:"total_run_ms"`
	Segments   []struct {
		StartTs           string `json:"start_ts"`
		EndTs             string `json:"end_ts"`
		DurationMS        int64  `json:"duration_ms"`
		RecipeStep        *int   `json:"recipe_step,omitempty"`
		RecipeOriginState string `json:"recipe_origin_state,omitempty"`
	} `json:"segments"`
}

func (r *sourceReader) StepRunsAfter(ctx context.Context, afterID int64) ([]srcStepRun, error) {
	// The fault columns are selected only when they exist, so one binary still runs against a
	// controller that has not yet migrated. Their absence costs fault attribution, not rows.
	faultCols := `'' , ''`
	if r.stepRunFaults {
		faultCols = `COALESCE(fault_type,''), COALESCE(fault_message,'')`
	}
	q := `SELECT id, step_started_ts_utc, step_ended_ts_utc, current_state, step,
	             COALESCE(previous_state,''), previous_step, COALESCE(order_key,''),
	             ` + faultCols + `,
	             COALESCE(sensors_snapshot_start_json,''), COALESCE(sensors_snapshot_end_json,''),
	             COALESCE(sensors_trace_json,''), COALESCE(actuators_json,'')
	      FROM fsm_step_runs WHERE id > ? ORDER BY id LIMIT ?`

	rows, err := r.db.QueryContext(ctx, q, afterID, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("read fsm_step_runs: %w", err)
	}
	defer rows.Close()

	var out []srcStepRun
	for rows.Next() {
		var s srcStepRun
		var startTS, endTS string
		if err := rows.Scan(&s.ID, &startTS, &endTS, &s.CurrentState, &s.Step,
			&s.PreviousState, &s.PreviousStep, &s.OrderKey,
			&s.FaultType, &s.FaultMessage,
			&s.SensorsStart, &s.SensorsEnd, &s.SensorsTrace, &s.ActuatorsJSON); err != nil {
			return nil, fmt.Errorf("scan fsm_step_runs: %w", err)
		}
		if s.Started, err = parseSourceTime(startTS); err != nil {
			logf("skipping fsm_step_runs id=%d: %v", s.ID, err)
			continue
		}
		if s.Ended, err = parseSourceTime(endTS); err != nil {
			logf("skipping fsm_step_runs id=%d: %v", s.ID, err)
			continue
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Config returns the current key/value config map. The source table has no timestamps at
// all, so change detection is a diff against the snapshot in projector_state.db.
func (r *sourceReader) Config(ctx context.Context) (map[string]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT key, COALESCE(value,'') FROM config`)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("scan config: %w", err)
		}
		out[k] = v
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Time handling
// ---------------------------------------------------------------------------

// parseSourceTime parses the controller's timestamps, which are written with
// time.RFC3339Nano. That format strips trailing zeros from the fractional part, so the
// strings vary in width — which is exactly why they are never used as a watermark or sort
// key anywhere in this program.
func parseSourceTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	// The controller itself carries a fallback of this shape (see CompleteActuatorInterval),
	// so match it rather than dropping rows the controller would have accepted.
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp %q", s)
}

func parseOptionalTime(ns sql.NullString) *time.Time {
	if !ns.Valid || strings.TrimSpace(ns.String) == "" {
		return nil
	}
	t, err := parseSourceTime(ns.String)
	if err != nil {
		return nil
	}
	return &t
}
