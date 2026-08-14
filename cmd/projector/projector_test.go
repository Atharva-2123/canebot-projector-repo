package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"canebot-fsm/store"

	_ "modernc.org/sqlite"
)

// base is a fixed instant so tests are deterministic.
var base = time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)

// srcTime renders a timestamp the way the controller does: time.RFC3339Nano, which strips
// trailing zeros. Seeding with the real format is what makes the variable-width handling
// genuinely exercised rather than assumed.
func srcTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// newSourceDB creates a source database using the controller's own store package, so the
// tests run against the real schema rather than a hand-copied approximation. The store handle
// is closed immediately and seeding is done over raw SQL, which keeps full control of the
// timestamps (store.InsertOrder stamps time.Now() internally).
func newSourceDB(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.db")

	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("create source schema: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return path, db
}

// newBaseSourceDB simulates a controller branch WITHOUT fsm_step_runs (main, or
// test/maintenance) by dropping the table. Used to exercise the derived-dwell fallback,
// which is what runs on those branches.
func newBaseSourceDB(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path, db := newSourceDB(t)
	mustExec(t, db, `DROP TABLE IF EXISTS fsm_step_runs`)
	return path, db
}

// seedStepRun writes one completed step exactly as StepRunTracker would on
// test/tracking_fsm, including the per-step actuator rollup.
func seedStepRun(t *testing.T, db *sql.DB, orderKey, state string, step int,
	start, end time.Time, prevState string, prevStep *int, actuatorsJSON string) {
	t.Helper()
	if actuatorsJSON == "" {
		actuatorsJSON = "[]"
	}
	mustExec(t, db,
		`INSERT INTO fsm_step_runs (step_started_ts_utc, step_ended_ts_utc, current_state, step,
		                            previous_state, previous_step, order_key,
		                            sensors_snapshot_start_json, sensors_snapshot_end_json,
		                            sensors_trace_json, actuators_json)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		srcTime(start), srcTime(end), state, step,
		nullable(prevState), prevStep, nullable(orderKey),
		`{"X0.0":true}`, `{"X0.0":false}`, `[]`, actuatorsJSON)
}

// seedStepRunWithFault writes a step run that a fault interrupted, as StepRunTracker does
// when OnFSMEventSent sees a FaultDetected while this session is active.
func seedStepRunWithFault(t *testing.T, db *sql.DB, orderKey, state string, step int,
	start, end time.Time, faultType, faultMessage string) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO fsm_step_runs (step_started_ts_utc, step_ended_ts_utc, current_state, step,
		                            previous_state, previous_step, order_key,
		                            fault_type, fault_message,
		                            sensors_snapshot_start_json, sensors_snapshot_end_json,
		                            sensors_trace_json, actuators_json)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		srcTime(start), srcTime(end), state, step,
		nullable(state), nil, nullable(orderKey),
		nullable(faultType), nullable(faultMessage),
		`{"X0.0":true}`, `{"X0.0":false}`, `[]`, `[]`)
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func seedOrder(t *testing.T, db *sql.DB, orderKey string, recipe int, at time.Time) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO orders (order_id, order_recipe, order_quantity, ts_utc) VALUES (?,?,1,?)`,
		orderKey, recipe, srcTime(at))
}

func seedStep(t *testing.T, db *sql.DB, orderKey, state string, from, to int, at time.Time) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO fsm_events (ts_utc, event_kind, current_state, step_from, step_to, order_key, source)
		 VALUES (?,?,?,?,?,?,?)`,
		srcTime(at), "step_changed", state, from, to, orderKey, "fsm.machine.SetCurrentStep")
}

func seedTransition(t *testing.T, db *sql.DB, orderKey, from, to string, at time.Time) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO fsm_events (ts_utc, event_kind, state_from, state_to, current_state, order_key)
		 VALUES (?,?,?,?,?,?)`,
		srcTime(at), "state_transition", from, to, to, nullable(orderKey))
}

func seedFault(t *testing.T, db *sql.DB, orderKey, faultType string, step int, at time.Time) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO faults (ts_utc, state, step, fault_type, message, order_key) VALUES (?,?,?,?,?,?)`,
		srcTime(at), "Error", step, faultType, "seeded", nullable(orderKey))
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// seedCompletedCycle writes one successful drink: the order, a run of steps, and the step 19
// "cycle complete" marker the firmware emits (fsm/autocycle.go).
func seedCompletedCycle(t *testing.T, db *sql.DB, orderKey string, start time.Time) {
	t.Helper()
	seedOrder(t, db, orderKey, 3, start)
	for i := 0; i < autoCycleCompleteStep; i++ {
		seedStep(t, db, orderKey, "AutoCycle", i, i+1, start.Add(time.Duration(i+1)*time.Second))
	}
}

func runOnce(t *testing.T, sourcePath, dir string) string {
	t.Helper()
	replica := filepath.Join(dir, "canebot_replica.db")
	state := filepath.Join(dir, "projector_state.db")
	if err := run(sourcePath, replica, state, time.Second, 500, 0, true); err != nil {
		t.Fatalf("projector run: %v", err)
	}
	return replica
}

func openRO(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func scalarInt(t *testing.T, db *sql.DB, q string, args ...any) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

func scalarStr(t *testing.T, db *sql.DB, q string, args ...any) string {
	t.Helper()
	var v string
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

// ---------------------------------------------------------------------------
// Cycle emission
// ---------------------------------------------------------------------------

func TestCompletedCycleIsEmittedOnce(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	seedCompletedCycle(t, db, "ORD-aaa111", base)

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if got := scalarInt(t, rep, `SELECT COUNT(*) FROM cycles WHERE order_key='ORD-aaa111'`); got != 1 {
		t.Fatalf("cycles rows = %d, want 1", got)
	}
	if got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key='ORD-aaa111'`); got != resultCompleted {
		t.Errorf("result = %q, want %q", got, resultCompleted)
	}
	if got := scalarInt(t, rep, `SELECT is_production FROM cycles WHERE order_key='ORD-aaa111'`); got != 1 {
		t.Errorf("is_production = %d, want 1", got)
	}
	if got := scalarInt(t, rep, `SELECT recipe_id FROM cycles WHERE order_key='ORD-aaa111'`); got != 3 {
		t.Errorf("recipe_id = %d, want 3", got)
	}
	if got := scalarInt(t, rep, `SELECT duration_ms FROM cycles WHERE order_key='ORD-aaa111'`); got <= 0 {
		t.Errorf("duration_ms = %d, want > 0", got)
	}
}

func TestFaultedCycleIsMarkedFaulted(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-bbb222"

	seedOrder(t, db, orderKey, 2, base)
	seedStep(t, db, orderKey, "AutoCycle", 0, 1, base.Add(time.Second))
	// TransitionToError writes a fault carrying the same order key, then clears the key.
	seedFault(t, db, orderKey, "CrusherMotorFault", 1, base.Add(5*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key=?`, orderKey); got != resultFaultedNonRecoverable {
		t.Errorf("result = %q, want %q", got, resultFaultedNonRecoverable)
	}
	if n := scalarInt(t, rep, `SELECT fault_count FROM cycles WHERE order_key=?`, orderKey); n != 1 {
		t.Errorf("fault_count = %d, want 1", n)
	}
	if sev := scalarStr(t, rep, `SELECT severity FROM fault_events WHERE order_key=?`, orderKey); sev != severityNonRecoverable {
		t.Errorf("severity = %q, want %q", sev, severityNonRecoverable)
	}
}

func TestRecoverableFaultIsClassifiedSeparately(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-rec001"

	seedOrder(t, db, orderKey, 1, base)
	seedStep(t, db, orderKey, "AutoCycle", 0, 1, base.Add(time.Second))
	seedFault(t, db, orderKey, "CupImproperDrop", 1, base.Add(3*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key=?`, orderKey); got != resultFaultedRecoverable {
		t.Errorf("result = %q, want %q", got, resultFaultedRecoverable)
	}
}

// TestInFlightCycleIsMarkedAborted — a cycle still running when the projector stops is
// written rather than lost, but must never be reported as a success. It is force-closed and
// marked `aborted`, so downstream can tell it apart from a drink that actually completed.
func TestInFlightCycleIsMarkedAborted(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	// Started, but never reached step 19 and never faulted.
	seedOrder(t, db, "ORD-ccc333", 1, base)
	seedStep(t, db, "ORD-ccc333", "AutoCycle", 0, 1, base.Add(time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key='ORD-ccc333'`)
	if got != resultAborted {
		t.Errorf("result = %q, want %q — an unfinished cycle must not read as completed", got, resultAborted)
	}
}

// ---------------------------------------------------------------------------
// The invariants that silently destroy data when broken
// ---------------------------------------------------------------------------

// TestSourceIsNeverWritten enforces "we do not touch the controller".
func TestSourceIsNeverWritten(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	seedCompletedCycle(t, db, "ORD-ddd444", base)
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	beforeHash, beforeSize := hashFile(t, sourcePath)
	runOnce(t, sourcePath, t.TempDir())
	afterHash, afterSize := hashFile(t, sourcePath)

	if beforeHash != afterHash || beforeSize != afterSize {
		t.Fatalf("source database was modified: %s (%d bytes) -> %s (%d bytes)",
			beforeHash, beforeSize, afterHash, afterSize)
	}
}

func hashFile(t *testing.T, path string) (string, int64) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), n
}

// TestEventTSNeverGoesBackwards is the highest-value assertion here. The edge agent's cursor
// is `ts > ? OR (ts = ? AND rowid > ?)` and only ever moves forward, so a row written late
// carrying an earlier event_ts falls behind it and is never shipped — silently, no error.
func TestEventTSNeverGoesBackwards(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)

	// A long cycle spanning a shorter one, plus out-of-cycle activity between them: the
	// arrangement most likely to produce out-of-order emission.
	seedCompletedCycle(t, db, "ORD-long01", base)
	seedTransition(t, db, "", "AutoCycle", "HomeIdle", base.Add(25*time.Second))
	seedCompletedCycle(t, db, "ORD-shrt02", base.Add(30*time.Second))
	seedTransition(t, db, "", "AutoCycle", "Maintenance", base.Add(70*time.Second))
	seedCompletedCycle(t, db, "ORD-long03", base.Add(90*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	const wantWidth = len("2026-08-11T10:00:00.000000000Z")
	for _, table := range []string{"cycles", "fsm_events", "step_dwells", "state_durations"} {
		rows, err := rep.Query(fmt.Sprintf(`SELECT event_ts FROM %s ORDER BY id`, table))
		if err != nil {
			t.Fatalf("query %s: %v", table, err)
		}
		var prev string
		var n int
		for rows.Next() {
			var ts string
			if err := rows.Scan(&ts); err != nil {
				rows.Close()
				t.Fatalf("scan %s: %v", table, err)
			}
			if prev != "" && ts < prev {
				rows.Close()
				t.Fatalf("%s: event_ts went backwards in insertion order: %s after %s", table, ts, prev)
			}
			if len(ts) != wantWidth {
				rows.Close()
				t.Fatalf("%s: event_ts %q is not fixed width (%d, want %d)", table, ts, len(ts), wantWidth)
			}
			prev = ts
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate %s: %v", table, err)
		}
		if n == 0 {
			t.Errorf("%s: no rows emitted", table)
		}
	}
}

// TestNoOrphanRows checks the foreign keys hold: every scoped row resolves to a cycles row,
// including everything that happened outside a production order.
func TestNoOrphanRows(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	seedCompletedCycle(t, db, "ORD-eee555", base)
	// Out-of-cycle activity carries no order key in the source, and would be orphaned
	// without synthetic intervals.
	seedTransition(t, db, "", "HomeIdle", "Maintenance", base.Add(40*time.Second))
	seedTransition(t, db, "", "Maintenance", "HomeIdle", base.Add(60*time.Second))
	seedCompletedCycle(t, db, "ORD-fff666", base.Add(80*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	rows, err := rep.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("foreign_key_check reported violations: rows exist with no parent cycle")
	}

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_events WHERE current_state='Maintenance'`); n == 0 {
		t.Error("maintenance events were dropped; out-of-cycle activity must still be captured")
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM cycles WHERE is_production=0`); n == 0 {
		t.Error("no synthetic interval was created for out-of-cycle activity")
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_events WHERE order_key IS NULL OR order_key=''`); n != 0 {
		t.Errorf("%d fsm_events rows have no order_key; every row must be scopeable", n)
	}
}

// TestReRunIsIdempotent checks the cursor genuinely prevents duplicate work.
func TestReRunIsIdempotent(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	seedCompletedCycle(t, db, "ORD-ggg777", base)

	dir := t.TempDir()
	replica := filepath.Join(dir, "canebot_replica.db")
	state := filepath.Join(dir, "projector_state.db")

	for i := 0; i < 3; i++ {
		if err := run(sourcePath, replica, state, time.Second, 500, 0, true); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	rep := openRO(t, replica)
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM cycles WHERE order_key='ORD-ggg777'`); n != 1 {
		t.Errorf("cycles rows = %d after 3 runs, want 1", n)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_events`); n != autoCycleCompleteStep {
		t.Errorf("fsm_events rows = %d after 3 runs, want %d", n, autoCycleCompleteStep)
	}
}

// ---------------------------------------------------------------------------
// Actuator intervals — the source's one mutable table
// ---------------------------------------------------------------------------

// TestHalfOpenActuatorIntervalIsSkipped covers the row that is inserted on ON and UPDATEd on
// OFF. Shipping it while still open would publish a row that later changes.
func TestHalfOpenActuatorIntervalIsSkipped(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-hhh888"
	seedCompletedCycle(t, db, orderKey, base)

	res, err := db.Exec(
		`INSERT INTO actuator_output_intervals
		   (output_id, output_name, order_key, started_ts_utc, started_state, started_step, started_send_ok)
		 VALUES (?,?,?,?,?,?,1)`,
		"Y0.1", "Crusher Motor", orderKey, srcTime(base.Add(2*time.Second)), "AutoCycle", 2)
	if err != nil {
		t.Fatalf("open actuator interval: %v", err)
	}
	id, _ := res.LastInsertId()

	dir := t.TempDir()
	replica := filepath.Join(dir, "canebot_replica.db")
	state := filepath.Join(dir, "projector_state.db")

	if err := run(sourcePath, replica, state, time.Second, 500, 0, true); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if n := scalarInt(t, openRO(t, replica), `SELECT COUNT(*) FROM actuator_intervals`); n != 0 {
		t.Fatalf("half-open interval was emitted (%d rows); it must wait until it closes", n)
	}

	// Close it, as the controller does on the falling edge.
	mustExec(t, db,
		`UPDATE actuator_output_intervals
		    SET ended_ts_utc=?, ended_state=?, ended_step=?, ended_send_ok=1, duration_ms=?
		  WHERE id=?`,
		srcTime(base.Add(6*time.Second)), "AutoCycle", 3, 4000, id)

	if err := run(sourcePath, replica, state, time.Second, 500, 0, true); err != nil {
		t.Fatalf("second run: %v", err)
	}

	rep := openRO(t, replica)
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM actuator_intervals`); n != 1 {
		t.Fatalf("closed interval rows = %d, want 1", n)
	}
	if d := scalarInt(t, rep, `SELECT duration_ms FROM actuator_intervals`); d != 4000 {
		t.Errorf("duration_ms = %d, want 4000 (never reported as zero)", d)
	}
}

// ---------------------------------------------------------------------------
// Lanes and dwells
// ---------------------------------------------------------------------------

// TestParallelLanesAreSeparated checks the three concurrent sub-FSMs do not collapse into one
// timeline. The firmware reports the tilter and crusher sub-FSMs through CurrentState
// (fsm/machine.go), so the dwell key is (lane, state, step).
func TestParallelLanesAreSeparated(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	const orderKey = "ORD-iii999"

	seedOrder(t, db, orderKey, 1, base)
	seq := []struct {
		state string
		step  int
		at    time.Duration
	}{
		{"AutoCycle", 1, 1 * time.Second},
		{"Tilter", 3, 2 * time.Second},
		{"AutoCycle", 2, 3 * time.Second},
		{"Crusher", 1, 4 * time.Second},
		{"Tilter", 5, 5 * time.Second},
		{"AutoCycle", 3, 6 * time.Second},
		{"Crusher", 2, 7 * time.Second},
	}
	for _, e := range seq {
		seedStep(t, db, orderKey, e.state, e.step-1, e.step, base.Add(e.at))
	}
	seedStep(t, db, orderKey, "AutoCycle", 18, autoCycleCompleteStep, base.Add(10*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	for _, lane := range []string{laneMain, laneTilter, laneCrusher} {
		if n := scalarInt(t, rep, `SELECT COUNT(*) FROM step_dwells WHERE order_key=? AND lane=?`, orderKey, lane); n == 0 {
			t.Errorf("lane %q produced no dwells", lane)
		}
	}

	// seq_index must be unique within a lane — that is what lets the frontend order dwells
	// without comparing timestamp strings.
	dupes := scalarInt(t, rep, `
		SELECT COUNT(*) FROM (
			SELECT lane, seq_index FROM step_dwells WHERE order_key=?
			GROUP BY lane, seq_index HAVING COUNT(*) > 1)`, orderKey)
	if dupes != 0 {
		t.Errorf("found %d duplicated (lane, seq_index) pairs", dupes)
	}
}

// TestStepDwellsAreClosedAtNextDwellStart checks durations are contiguous rather than
// measured first-to-last-event within a dwell, which under-counts by the inter-dwell gap.
func TestStepDwellsAreClosedAtNextDwellStart(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	const orderKey = "ORD-jjj000"

	seedOrder(t, db, orderKey, 1, base)
	seedStep(t, db, orderKey, "AutoCycle", 0, 1, base.Add(1*time.Second))
	seedStep(t, db, orderKey, "AutoCycle", 1, 2, base.Add(4*time.Second)) // step 1 lasted 3s
	seedStep(t, db, orderKey, "AutoCycle", 18, autoCycleCompleteStep, base.Add(9*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	got := scalarInt(t, rep,
		`SELECT duration_ms FROM step_dwells WHERE order_key=? AND lane=? AND step=1`, orderKey, laneMain)
	if got != 3000 {
		t.Errorf("step 1 duration = %dms, want 3000 (closed at the next dwell's start)", got)
	}
}

// ---------------------------------------------------------------------------
// Timestamp handling
// ---------------------------------------------------------------------------

func TestEventTSIsFixedWidthAndOrdered(t *testing.T) {
	const want = len("2026-08-11T10:00:00.000000000Z")
	for _, ms := range []int64{
		base.UnixMilli(),
		base.Add(500 * time.Millisecond).UnixMilli(),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
	} {
		got := eventTS(ms)
		if len(got) != want {
			t.Errorf("eventTS(%d) = %q, length %d, want %d", ms, got, len(got), want)
		}
		if got[len(got)-1] != 'Z' {
			t.Errorf("eventTS(%d) = %q, want trailing Z", ms, got)
		}
	}

	// Lexical order must match chronological order — the whole point of the fixed width.
	a := eventTS(base.UnixMilli())
	b := eventTS(base.Add(500 * time.Millisecond).UnixMilli())
	if a >= b {
		t.Errorf("lexical order broken: %q should sort before %q", a, b)
	}
}

func TestParseSourceTimeHandlesVariableWidth(t *testing.T) {
	// All of these occur in the source: RFC3339Nano strips trailing zeros, and the
	// controller carries an RFC3339 fallback of its own.
	for _, in := range []string{
		"2026-08-11T10:00:00Z",
		"2026-08-11T10:00:00.5Z",
		"2026-08-11T10:00:00.123456789Z",
	} {
		if _, err := parseSourceTime(in); err != nil {
			t.Errorf("parseSourceTime(%q) failed: %v", in, err)
		}
	}
	if _, err := parseSourceTime("not-a-time"); err == nil {
		t.Error("parseSourceTime accepted garbage")
	}
	if _, err := parseSourceTime(""); err == nil {
		t.Error("parseSourceTime accepted an empty string")
	}
}

// TestSourceLexicalOrderIsUnsafe documents why the projector never uses ts_utc as a
// watermark: the controller's own format sorts wrongly as a string.
func TestSourceLexicalOrderIsUnsafe(t *testing.T) {
	earlier := srcTime(base)                           // ...T10:00:00Z
	later := srcTime(base.Add(500 * time.Millisecond)) // ...T10:00:00.5Z
	if earlier < later {
		t.Skip("format changed; the hazard this guards against no longer applies")
	}
	// Lexically "10:00:00Z" > "10:00:00.5Z" even though it is chronologically earlier.
	if !(later < earlier) {
		t.Fatalf("expected the documented lexical hazard between %q and %q", earlier, later)
	}
}

// ---------------------------------------------------------------------------
// fsm_step_runs ingestion (branch test/tracking_fsm) and the fallback
// ---------------------------------------------------------------------------

// TestStepRunsAreIngested covers the path taken when the controller carries fsm_step_runs:
// the firmware's own step records are used verbatim rather than derived from fsm_events,
// and they bring previous_state and per-step actuator run time with them.
func TestStepRunsAreIngested(t *testing.T) {
	sourcePath, db := newSourceDB(t) // this branch has fsm_step_runs
	const orderKey = "ORD-sr0001"

	seedOrder(t, db, orderKey, 4, base)
	prev := 0
	seedStepRun(t, db, orderKey, "AutoCycle", 1,
		base.Add(1*time.Second), base.Add(4*time.Second), "AutoCycle", &prev,
		`{"Y0.1":{"name":"Crusher Motor","total_run_ms":2500,
		   "segments":[{"start_ts":"x","end_ts":"y","duration_ms":2500,
		                "recipe_step":11,"recipe_origin_state":"AutoCycle"}]}}`)
	// Close the cycle so the interval flushes.
	seedStep(t, db, orderKey, "AutoCycle", 18, autoCycleCompleteStep, base.Add(9*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM step_dwells WHERE order_key=?`, orderKey); n != 1 {
		t.Fatalf("step_dwells rows = %d, want 1", n)
	}
	if k := scalarStr(t, rep, `SELECT source_kind FROM step_dwells WHERE order_key=?`, orderKey); k != sourceKindStepRuns {
		t.Errorf("source_kind = %q, want %q", k, sourceKindStepRuns)
	}
	if d := scalarInt(t, rep, `SELECT duration_ms FROM step_dwells WHERE order_key=?`, orderKey); d != 3000 {
		t.Errorf("duration_ms = %d, want 3000", d)
	}
	if ps := scalarStr(t, rep, `SELECT previous_state FROM step_dwells WHERE order_key=?`, orderKey); ps != "AutoCycle" {
		t.Errorf("previous_state = %q, want AutoCycle", ps)
	}
	// Sensor snapshots only exist on this path.
	if s := scalarStr(t, rep, `SELECT sensors_start_json FROM step_dwells WHERE order_key=?`, orderKey); s == "" {
		t.Error("sensors_start_json is empty; step-run rows carry boundary snapshots")
	}

	// And the per-step actuator rollup, which cannot be derived from fsm_events at all.
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM step_actuators WHERE order_key=?`, orderKey); n != 1 {
		t.Fatalf("step_actuators rows = %d, want 1", n)
	}
	if ms := scalarInt(t, rep, `SELECT total_run_ms FROM step_actuators WHERE order_key=?`, orderKey); ms != 2500 {
		t.Errorf("total_run_ms = %d, want 2500", ms)
	}
	if rs := scalarInt(t, rep, `SELECT recipe_step FROM step_actuators WHERE order_key=?`, orderKey); rs != 11 {
		t.Errorf("recipe_step = %d, want 11", rs)
	}
}

// TestFallbackDerivesDwellsWithoutStepRuns proves the same binary still works against the
// branches that lack the table (main, test/maintenance).
func TestFallbackDerivesDwellsWithoutStepRuns(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	const orderKey = "ORD-fb0001"

	seedOrder(t, db, orderKey, 1, base)
	seedStep(t, db, orderKey, "AutoCycle", 0, 1, base.Add(1*time.Second))
	seedStep(t, db, orderKey, "AutoCycle", 1, 2, base.Add(4*time.Second))
	seedStep(t, db, orderKey, "AutoCycle", 18, autoCycleCompleteStep, base.Add(9*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM step_dwells WHERE order_key=?`, orderKey); n == 0 {
		t.Fatal("no dwells derived; the fallback must still work without fsm_step_runs")
	}
	if k := scalarStr(t, rep,
		`SELECT source_kind FROM step_dwells WHERE order_key=? LIMIT 1`, orderKey); k != sourceKindDerived {
		t.Errorf("source_kind = %q, want %q", k, sourceKindDerived)
	}
	// step_actuators is unavailable on this path — the source has no per-step attribution.
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM step_actuators`); n != 0 {
		t.Errorf("step_actuators = %d rows, want 0 without fsm_step_runs", n)
	}
}

// ---------------------------------------------------------------------------
// Denormalization
// ---------------------------------------------------------------------------

// TestChildRowsCarryCycleContext checks the denormalized columns are populated, which is what
// lets the admin dashboard filter and group without ever joining.
func TestChildRowsCarryCycleContext(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	const orderKey = "ORD-den001"

	seedOrder(t, db, orderKey, 6, base)
	seedStep(t, db, orderKey, "AutoCycle", 0, 1, base.Add(1*time.Second))
	seedStep(t, db, orderKey, "AutoCycle", 18, autoCycleCompleteStep, base.Add(9*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	for _, table := range []string{"step_dwells", "fsm_events"} {
		q := fmt.Sprintf(`SELECT COUNT(*) FROM %s
		                  WHERE order_key=? AND (hour_bucket_ms IS NULL OR date_utc IS NULL
		                                         OR is_production IS NULL)`, table)
		if n := scalarInt(t, rep, q, orderKey); n != 0 {
			t.Errorf("%s: %d rows missing denormalized columns", table, n)
		}
	}

	if r := scalarInt(t, rep,
		`SELECT recipe_id FROM step_dwells WHERE order_key=? LIMIT 1`, orderKey); r != 6 {
		t.Errorf("step_dwells.recipe_id = %d, want 6 (copied from the parent cycle)", r)
	}
	if cr := scalarStr(t, rep,
		`SELECT cycle_result FROM step_dwells WHERE order_key=? LIMIT 1`, orderKey); cr != resultCompleted {
		t.Errorf("step_dwells.cycle_result = %q, want %q", cr, resultCompleted)
	}
	if d := scalarStr(t, rep, `SELECT date_utc FROM cycles WHERE order_key=?`, orderKey); d != "2026-08-11" {
		t.Errorf("date_utc = %q, want 2026-08-11", d)
	}
	// The bucket must floor to the hour, not merely copy the timestamp.
	if b := scalarInt(t, rep, `SELECT hour_bucket_ms FROM cycles WHERE order_key=?`, orderKey); b%3600000 != 0 {
		t.Errorf("hour_bucket_ms = %d is not floored to an hour", b)
	}
}

// TestStepTitleComesFromFirmware checks labels are resolved via fsm.GetStepDescription rather
// than a copy that can drift from the machine's actual behaviour.
func TestStepTitleComesFromFirmware(t *testing.T) {
	title := stepTitle("AutoCycle", ptrInt64(1))
	if title == "" {
		t.Fatal("no title for AutoCycle step 1; expected firmware metadata")
	}
	// Tilter and Crusher are parallel sub-FSMs, not members of the State enum, so they
	// legitimately have no step metadata.
	if got := stepTitle("Tilter", ptrInt64(3)); got != "" {
		t.Errorf("stepTitle(Tilter) = %q, want empty (no metadata for sub-FSMs)", got)
	}
}

func ptrInt64(v int64) *int64 { return &v }

// TestStepRunRawBlobsArePreserved checks the two firmware blobs survive verbatim.
// sensors_trace_json has no flattened equivalent anywhere, and actuators_json preserves
// per-segment detail that step_actuators (which keeps only the first segment's recipe_step)
// necessarily loses.
func TestStepRunRawBlobsArePreserved(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-raw001"

	const trace = `[{"ts_utc":"2026-08-11T10:00:02Z","input_id":"X1.5","name":"Cane At Tilter","from":false,"to":true}]`
	// Two pulses at DIFFERENT recipe steps — the case step_actuators alone cannot represent.
	const acts = `{"Y0.1":{"name":"Crusher Motor","total_run_ms":9000,"segments":[` +
		`{"duration_ms":5000,"recipe_step":11,"recipe_origin_state":"AutoCycle"},` +
		`{"duration_ms":4000,"recipe_step":12,"recipe_origin_state":"AutoCycle"}]}}`

	seedOrder(t, db, orderKey, 2, base)
	mustExec(t, db,
		`INSERT INTO fsm_step_runs (step_started_ts_utc, step_ended_ts_utc, current_state, step,
		                            previous_state, previous_step, order_key,
		                            sensors_snapshot_start_json, sensors_snapshot_end_json,
		                            sensors_trace_json, actuators_json)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		srcTime(base.Add(time.Second)), srcTime(base.Add(10*time.Second)), "AutoCycle", 11,
		"AutoCycle", 10, orderKey, `{}`, `{}`, trace, acts)
	seedStep(t, db, orderKey, "AutoCycle", 18, autoCycleCompleteStep, base.Add(20*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if got := scalarStr(t, rep, `SELECT sensors_trace_json FROM step_dwells WHERE order_key=?`, orderKey); got != trace {
		t.Errorf("sensors_trace_json not preserved verbatim:\n got %q\nwant %q", got, trace)
	}
	if got := scalarStr(t, rep, `SELECT actuators_json FROM step_dwells WHERE order_key=?`, orderKey); got != acts {
		t.Errorf("actuators_json not preserved verbatim:\n got %q\nwant %q", got, acts)
	}
	// The flattened row still exists, and still reports the total across both pulses.
	if n := scalarInt(t, rep, `SELECT total_run_ms FROM step_actuators WHERE order_key=?`, orderKey); n != 9000 {
		t.Errorf("step_actuators.total_run_ms = %d, want 9000", n)
	}
	if n := scalarInt(t, rep, `SELECT segment_count FROM step_actuators WHERE order_key=?`, orderKey); n != 2 {
		t.Errorf("segment_count = %d, want 2", n)
	}
}

// ---------------------------------------------------------------------------
// Door open→close episodes and fault resets
// ---------------------------------------------------------------------------

// seedToggle writes one input edge as the controller records it (0 = false, 1 = true).
func seedToggle(t *testing.T, db *sql.DB, orderKey, inputID, name string,
	from, to int, state string, step int, at time.Time) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO sensor_input_toggles (ts_utc, input_id, input_display_name,
		                                   value_from, value_to, current_state, current_step, order_key)
		 VALUES (?,?,?,?,?,?,?,?)`,
		srcTime(at), inputID, name, from, to, state, step, nullable(orderKey))
}

// TestDoorEpisodeWithFaultReset covers the whole feature: the door opens (X0.0 true→false),
// a fault is reset while it is open (X0.7 rising), and the door closes.
func TestDoorEpisodeWithFaultReset(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)

	// Machine sits in Error, someone opens the door, resets the fault, closes the door.
	seedTransition(t, db, "", "AutoCycle", "Error", base)
	seedToggle(t, db, "", "X0.0", "Main door switch", 1, 0, "Error", 4, base.Add(5*time.Second))
	seedToggle(t, db, "", "X0.7", "CIP bypass switch", 0, 1, "Error", 4, base.Add(12*time.Second))
	seedToggle(t, db, "", "X0.0", "Main door switch", 0, 1, "Manual", 4, base.Add(20*time.Second))
	// Something afterwards so the interval closes and flushes.
	seedTransition(t, db, "", "Manual", "HomeIdle", base.Add(30*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM door_events`); n != 1 {
		t.Fatalf("door_events rows = %d, want 1", n)
	}
	if d := scalarInt(t, rep, `SELECT duration_ms FROM door_events`); d != 15000 {
		t.Errorf("duration_ms = %d, want 15000 (open 5s → close 20s)", d)
	}
	if r := scalarInt(t, rep, `SELECT fault_reset_during FROM door_events`); r != 1 {
		t.Errorf("fault_reset_during = %d, want 1", r)
	}
	if n := scalarInt(t, rep, `SELECT fault_reset_count FROM door_events`); n != 1 {
		t.Errorf("fault_reset_count = %d, want 1", n)
	}
	if ms := scalarInt(t, rep, `SELECT ms_to_first_reset FROM door_events`); ms != 7000 {
		t.Errorf("ms_to_first_reset = %d, want 7000 (open 5s → reset 12s)", ms)
	}
	if e := scalarInt(t, rep, `SELECT opened_in_error FROM door_events`); e != 1 {
		t.Errorf("opened_in_error = %d, want 1", e)
	}
	if s := scalarStr(t, rep, `SELECT state_at_open FROM door_events`); s != "Error" {
		t.Errorf("state_at_open = %q, want Error", s)
	}
	if s := scalarStr(t, rep, `SELECT state_at_close FROM door_events`); s != "Manual" {
		t.Errorf("state_at_close = %q, want Manual", s)
	}
}

// TestDoorEpisodeWithoutFaultReset is the ordinary case — the door was opened and closed with
// no fault cleared, which must be distinguishable from the case above.
func TestDoorEpisodeWithoutFaultReset(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)

	seedTransition(t, db, "", "AutoCycle", "HomeIdle", base)
	seedToggle(t, db, "", "X0.0", "Main door switch", 1, 0, "HomeIdle", 0, base.Add(2*time.Second))
	seedToggle(t, db, "", "X0.0", "Main door switch", 0, 1, "HomeIdle", 0, base.Add(9*time.Second))
	seedTransition(t, db, "", "HomeIdle", "Maintenance", base.Add(20*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if d := scalarInt(t, rep, `SELECT duration_ms FROM door_events`); d != 7000 {
		t.Errorf("duration_ms = %d, want 7000", d)
	}
	if r := scalarInt(t, rep, `SELECT fault_reset_during FROM door_events`); r != 0 {
		t.Errorf("fault_reset_during = %d, want 0", r)
	}
	if e := scalarInt(t, rep, `SELECT opened_in_error FROM door_events`); e != 0 {
		t.Errorf("opened_in_error = %d, want 0", e)
	}
}

// TestRepeatedResetsDuringOneDoorEpisode — an operator pressing reset several times is a
// signal in itself, so the count is kept rather than collapsed to a boolean.
func TestRepeatedResetsDuringOneDoorEpisode(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)

	seedTransition(t, db, "", "AutoCycle", "Error", base)
	seedToggle(t, db, "", "X0.0", "Main door switch", 1, 0, "Error", 7, base.Add(1*time.Second))
	seedToggle(t, db, "", "X0.7", "CIP bypass switch", 0, 1, "Error", 7, base.Add(3*time.Second))
	seedToggle(t, db, "", "X0.7", "CIP bypass switch", 1, 0, "Error", 7, base.Add(4*time.Second)) // falling: ignored
	seedToggle(t, db, "", "X0.7", "CIP bypass switch", 0, 1, "Error", 7, base.Add(6*time.Second))
	seedToggle(t, db, "", "X0.0", "Main door switch", 0, 1, "Manual", 7, base.Add(10*time.Second))
	seedTransition(t, db, "", "Manual", "HomeIdle", base.Add(20*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT fault_reset_count FROM door_events`); n != 2 {
		t.Errorf("fault_reset_count = %d, want 2 (falling edges must not count)", n)
	}
	if ms := scalarInt(t, rep, `SELECT ms_to_first_reset FROM door_events`); ms != 2000 {
		t.Errorf("ms_to_first_reset = %d, want 2000", ms)
	}
}

// TestResetOutsideDoorEpisodeIsIgnored — X0.7 also fires with the door shut (Modbus-driven
// resets, CIP bypass proper). Those must not invent a door episode.
func TestResetOutsideDoorEpisodeIsIgnored(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)

	seedTransition(t, db, "", "AutoCycle", "Error", base)
	seedToggle(t, db, "", "X0.7", "CIP bypass switch", 0, 1, "Error", 2, base.Add(3*time.Second))
	seedTransition(t, db, "", "Error", "HomeIdle", base.Add(20*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM door_events`); n != 0 {
		t.Errorf("door_events rows = %d, want 0 — no door was opened", n)
	}
}

// TestDoorOpenAtStartupIsNotGuessed — if the projector starts with the door already open we
// never saw it open, so reporting a duration would be fabrication.
func TestDoorOpenAtStartupIsNotGuessed(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)

	seedTransition(t, db, "", "AutoCycle", "Manual", base)
	// Only a closing edge — the opening happened before we were watching.
	seedToggle(t, db, "", "X0.0", "Main door switch", 0, 1, "Manual", 0, base.Add(5*time.Second))
	seedTransition(t, db, "", "Manual", "HomeIdle", base.Add(20*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM door_events`); n != 0 {
		t.Errorf("door_events rows = %d, want 0 — the open edge was never observed", n)
	}
}

// TestIdleProjectorDoesNotFabricateGaps covers the liveness bug: gap detection must not be
// derived from cursor timestamps, because those only move when new rows arrive. A projector
// that runs repeatedly with nothing new to read is healthy, not absent.
func TestIdleProjectorDoesNotFabricateGaps(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	seedCompletedCycle(t, db, "ORD-idle01", base)

	dir := t.TempDir()
	replica := filepath.Join(dir, "canebot_replica.db")
	state := filepath.Join(dir, "projector_state.db")

	// First run consumes everything. The next two have nothing new to read.
	for i := 0; i < 3; i++ {
		if err := run(sourcePath, replica, state, time.Second, 500, 0, true); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	rep := openRO(t, replica)
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM gaps`); n != 0 {
		t.Errorf("gaps rows = %d, want 0 — an idle projector is not an absent one", n)
	}
}

// TestLongRunInOneStateStillEmits is the regression test for the bug real machine data found:
// 23,500 step runs read, zero rows written.
//
// An interval is only written when it closes, and a synthetic interval only closes when the
// state changes. A machine sitting in one state therefore buffered everything in memory and
// shipped nothing. The duration cap force-closes long intervals so data keeps flowing.
func TestLongRunInOneStateStillEmits(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	// Two hours of step runs, all in the same state, no orders and no faults —
	// exactly the shape the Pi's database turned out to have.
	for i := 0; i < 240; i++ {
		start := base.Add(time.Duration(i) * 30 * time.Second)
		seedStepRun(t, db, "", "AutoCycle", i%20,
			start, start.Add(25*time.Second), "AutoCycle", nil, "")
	}

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	cycles := scalarInt(t, rep, `SELECT COUNT(*) FROM cycles`)
	if cycles == 0 {
		t.Fatal("no cycles emitted; a machine that never changes state must still produce rows")
	}
	dwells := scalarInt(t, rep, `SELECT COUNT(*) FROM step_dwells`)
	if dwells != 240 {
		t.Errorf("step_dwells = %d, want 240 — every step run must be written", dwells)
	}
	// Two hours at a 15-minute cap means several intervals, not one giant one.
	if cycles < 2 {
		t.Errorf("cycles = %d; a two-hour run should be split by the interval cap", cycles)
	}
	t.Logf("two hours in one state produced %d intervals and %d dwells", cycles, dwells)
}

// TestActuatorsJSONIsAMap guards the shape the firmware actually writes. It declares
// `map[string]StepRunActuator` keyed by output_id, so the JSON is an object — decoding it as
// an array fails on every row, which is what real data showed.
func TestActuatorsJSONIsAMap(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-map001"

	seedOrder(t, db, orderKey, 1, base)
	seedStepRun(t, db, orderKey, "AutoCycle", 11,
		base.Add(time.Second), base.Add(9*time.Second), "AutoCycle", nil,
		`{"Y0.1":{"name":"Crusher Motor","total_run_ms":4000,"segments":[{"duration_ms":4000}]},
		  "Y0.3":{"name":"Filter Motor","total_run_ms":1500,"segments":[{"duration_ms":1500}]}}`)
	seedStep(t, db, orderKey, "AutoCycle", 18, autoCycleCompleteStep, base.Add(20*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM step_actuators WHERE order_key=?`, orderKey); n != 2 {
		t.Fatalf("step_actuators rows = %d, want 2 (one per output in the map)", n)
	}
	// The map key is the output_id — information that would be lost decoding as an array.
	if id := scalarStr(t, rep,
		`SELECT output_id FROM step_actuators WHERE order_key=? AND output_name='Crusher Motor'`,
		orderKey); id != "Y0.1" {
		t.Errorf("output_id = %q, want Y0.1 (taken from the map key)", id)
	}
	if ms := scalarInt(t, rep,
		`SELECT total_run_ms FROM step_actuators WHERE order_key=? AND output_id='Y0.3'`,
		orderKey); ms != 1500 {
		t.Errorf("total_run_ms = %d, want 1500", ms)
	}
}

// TestCycleCompletesFromStepRunsAlone covers the controller configuration found on the real
// machine: fsm_step_runs is populated but fsm_events, orders and faults are all empty.
//
// Completion was previously detected only from fsm_events, so on that machine every
// production cycle stayed open until the duration cap force-closed it as `aborted` — a
// machine working perfectly reported nothing but failures.
func TestCycleCompletesFromStepRunsAlone(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-sr9001"

	// No orders row, no fsm_events, no faults — only step runs carrying an order_key.
	for i := 1; i < autoCycleCompleteStep; i++ {
		start := base.Add(time.Duration(i) * 2 * time.Second)
		seedStepRun(t, db, orderKey, "AutoCycle", i, start, start.Add(2*time.Second), "AutoCycle", nil, "")
	}
	last := base.Add(time.Duration(autoCycleCompleteStep) * 2 * time.Second)
	seedStepRun(t, db, orderKey, "AutoCycle", autoCycleCompleteStep,
		last, last.Add(time.Second), "AutoCycle", nil, "")

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key=?`, orderKey)
	if got != resultCompleted {
		t.Errorf("result = %q, want %q — step 19 is the firmware's own completion marker", got, resultCompleted)
	}
	if p := scalarInt(t, rep, `SELECT is_production FROM cycles WHERE order_key=?`, orderKey); p != 1 {
		t.Errorf("is_production = %d, want 1 — an order-keyed step run is production work", p)
	}
	if d := scalarInt(t, rep, `SELECT duration_ms FROM cycles WHERE order_key=?`, orderKey); d <= 0 {
		t.Errorf("duration_ms = %d, want > 0", d)
	}
}

// TestFaultFromStepRunsAlone is the companion to TestCycleCompletesFromStepRunsAlone for the
// failure path, and covers the same real-machine configuration: fsm_step_runs populated,
// fsm_events / orders / faults all empty.
//
// Without reading fsm_step_runs.fault_type there is no fault signal at all on that machine —
// fault_events stays empty, cycles.fault_count is 0, and a fault-terminated cycle resolves to
// `aborted` via the duration cap, which is indistinguishable from an operator walking away.
func TestFaultFromStepRunsAlone(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-srf001"

	for i := 1; i < 10; i++ {
		start := base.Add(time.Duration(i) * 2 * time.Second)
		seedStepRun(t, db, orderKey, "AutoCycle", i, start, start.Add(2*time.Second), "AutoCycle", nil, "")
	}
	faultStart := base.Add(10 * 2 * time.Second)
	seedStepRunWithFault(t, db, orderKey, "AutoCycle", 10,
		faultStart, faultStart.Add(3*time.Second),
		"CrusherMotorFault", "Fault detected on X0.4")

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	// CrusherMotorFault is on the non-recoverable list, so the cycle must say so rather than
	// fall through to the duration cap's `aborted`.
	if got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key=?`, orderKey); got != resultFaultedNonRecoverable {
		t.Errorf("result = %q, want %q", got, resultFaultedNonRecoverable)
	}
	if n := scalarInt(t, rep, `SELECT fault_count FROM cycles WHERE order_key=?`, orderKey); n != 1 {
		t.Errorf("cycles.fault_count = %d, want 1", n)
	}
	if got := scalarStr(t, rep, `SELECT dominant_fault_type FROM cycles WHERE order_key=?`, orderKey); got != "CrusherMotorFault" {
		t.Errorf("dominant_fault_type = %q, want CrusherMotorFault", got)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fault_events WHERE order_key=?`, orderKey); n != 1 {
		t.Fatalf("fault_events rows = %d, want 1", n)
	}
	if got := scalarStr(t, rep, `SELECT severity FROM fault_events WHERE order_key=?`, orderKey); got != severityNonRecoverable {
		t.Errorf("severity = %q, want %q", got, severityNonRecoverable)
	}
	// The fault must land on the step it interrupted, which is what makes "which step fails
	// most" answerable — the firmware attaches it before the Error transition for this reason.
	if got := scalarInt(t, rep, `SELECT step FROM fault_events WHERE order_key=?`, orderKey); got != 10 {
		t.Errorf("fault_events.step = %d, want 10", got)
	}
	if got := scalarStr(t, rep, `SELECT message FROM fault_events WHERE order_key=?`, orderKey); got != "Fault detected on X0.4" {
		t.Errorf("message = %q", got)
	}
	if got := scalarStr(t, rep,
		`SELECT fault_type FROM step_dwells WHERE order_key=? AND step=10`, orderKey); got != "CrusherMotorFault" {
		t.Errorf("step_dwells.fault_type = %q, want CrusherMotorFault", got)
	}
	if n := scalarInt(t, rep,
		`SELECT fault_count FROM step_dwells WHERE order_key=? AND step=10`, orderKey); n != 1 {
		t.Errorf("step_dwells.fault_count = %d, want 1", n)
	}
	// Steps that ran clean must stay clean.
	if n := scalarInt(t, rep,
		`SELECT COUNT(*) FROM step_dwells WHERE order_key=? AND fault_type IS NOT NULL`, orderKey); n != 1 {
		t.Errorf("%d dwells carry a fault, want exactly 1", n)
	}
}

// TestRecoverableFaultFromStepRunIsClassified checks the severity split is applied to the
// step-run path too, not just the ledger path — the two must not disagree.
func TestRecoverableFaultFromStepRunIsClassified(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-srf002"

	start := base.Add(2 * time.Second)
	seedStepRun(t, db, orderKey, "AutoCycle", 1, start, start.Add(2*time.Second), "AutoCycle", nil, "")
	faultStart := start.Add(2 * time.Second)
	seedStepRunWithFault(t, db, orderKey, "AutoCycle", 2,
		faultStart, faultStart.Add(2*time.Second),
		"HopperEmptyFault", "hopper empty")

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key=?`, orderKey); got != resultFaultedRecoverable {
		t.Errorf("result = %q, want %q", got, resultFaultedRecoverable)
	}
	if got := scalarStr(t, rep, `SELECT severity FROM fault_events WHERE order_key=?`, orderKey); got != severityRecoverable {
		t.Errorf("severity = %q, want %q", got, severityRecoverable)
	}
}

// TestLedgerFaultIsNotDoubleCountedByStepRun covers a controller that writes both: the
// `faults` ledger AND the same fault denormalised onto the step run. The firmware documents
// `faults` as authoritative (it can record several faults inside one Error dwell, which a
// single pair of columns cannot), so the step run's copy must be recognised as the same event
// rather than inflating fault_count and the Pareto.
func TestLedgerFaultIsNotDoubleCountedByStepRun(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-srf003"

	seedOrder(t, db, orderKey, 3, base)
	dwellStart := base.Add(2 * time.Second)
	dwellEnd := dwellStart.Add(4 * time.Second)
	seedStepRunWithFault(t, db, orderKey, "AutoCycle", 10, dwellStart, dwellEnd,
		"CrusherMotorFault", "Fault detected on X0.4")
	// The ledger's own row for the same fault, raised inside that dwell.
	seedFault(t, db, orderKey, "CrusherMotorFault", 10, dwellStart.Add(time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fault_events WHERE order_key=?`, orderKey); n != 1 {
		t.Errorf("fault_events rows = %d, want 1 — the ledger row and the step run's copy are one fault", n)
	}
	if n := scalarInt(t, rep, `SELECT fault_count FROM cycles WHERE order_key=?`, orderKey); n != 1 {
		t.Errorf("cycles.fault_count = %d, want 1", n)
	}
	// The denormalised columns still ride along on the dwell — they are the join-free view,
	// and suppressing them would lose the step attribution the ledger row does not carry.
	if got := scalarStr(t, rep,
		`SELECT fault_type FROM step_dwells WHERE order_key=? AND step=10`, orderKey); got != "CrusherMotorFault" {
		t.Errorf("step_dwells.fault_type = %q, want CrusherMotorFault", got)
	}
}

// TestOldControllerWithoutFaultColumnsStillProjects guards the mixed-fleet case: a controller
// that carries fsm_step_runs but has not restarted since the firmware migration still has the
// pre-4e88b8d shape. Probing for the columns rather than assuming them is what keeps one
// binary running against both.
func TestOldControllerWithoutFaultColumnsStillProjects(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-srf004"

	// Recreate the old shape: no fault_type / fault_message.
	mustExec(t, db, `DROP TABLE fsm_step_runs`)
	mustExec(t, db, `CREATE TABLE fsm_step_runs (
		id INTEGER PRIMARY KEY AUTOINCREMENT, step_started_ts_utc TEXT NOT NULL,
		step_ended_ts_utc TEXT NOT NULL, current_state TEXT NOT NULL, step INTEGER NOT NULL,
		previous_state TEXT, previous_step INTEGER, order_id INTEGER, order_key TEXT,
		sensors_snapshot_start_json TEXT, sensors_snapshot_end_json TEXT,
		sensors_trace_json TEXT NOT NULL, actuators_json TEXT NOT NULL)`)

	for i := 1; i <= autoCycleCompleteStep; i++ {
		start := base.Add(time.Duration(i) * 2 * time.Second)
		mustExec(t, db,
			`INSERT INTO fsm_step_runs (step_started_ts_utc, step_ended_ts_utc, current_state, step,
			                            previous_state, previous_step, order_id, order_key,
			                            sensors_snapshot_start_json, sensors_snapshot_end_json,
			                            sensors_trace_json, actuators_json)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
			srcTime(start), srcTime(start.Add(2*time.Second)), "AutoCycle", i,
			"AutoCycle", nil, 868, orderKey,
			`{"X0.0":true}`, `{"X0.0":false}`, `[]`, `[]`)
	}

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_key=?`, orderKey); got != resultCompleted {
		t.Errorf("result = %q, want %q — the old schema must still project", got, resultCompleted)
	}
	if n := scalarInt(t, rep,
		`SELECT COUNT(*) FROM step_dwells WHERE order_key=? AND fault_type IS NOT NULL`, orderKey); n != 0 {
		t.Errorf("%d dwells carry a fault_type, want 0 — the source has no such column", n)
	}
}

// TestReplicaGainsFaultColumnsOnUpgrade covers the deployed replica. The schema is all
// CREATE TABLE IF NOT EXISTS, so a database created by an earlier build keeps its original
// shape and every insert naming fault_type would fail. A deployed replica holds up to 14 days
// of not-yet-shipped history, so it has to be migrated rather than rebuilt.
func TestReplicaGainsFaultColumnsOnUpgrade(t *testing.T) {
	dir := t.TempDir()
	replicaPath := filepath.Join(dir, "canebot_replica.db")

	// Build a replica the way the previous version would have, then strip the new columns.
	statePath := filepath.Join(dir, "projector_state.db")
	st, err := openState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := openReplica(replicaPath, st)
	if err != nil {
		t.Fatal(err)
	}
	rep.Close()
	st.Close()

	old, err := sql.Open("sqlite", "file:"+replicaPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"fault_type", "fault_message"} {
		if _, err := old.Exec(`ALTER TABLE step_dwells DROP COLUMN ` + col); err != nil {
			t.Fatalf("strip %s: %v", col, err)
		}
	}
	old.Close()

	// Re-opening must add them back rather than leave the writer broken.
	st2, err := openState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	rep2, err := openReplica(replicaPath, st2)
	if err != nil {
		t.Fatalf("reopen migrated replica: %v", err)
	}
	defer rep2.Close()

	for _, col := range []string{"fault_type", "fault_message"} {
		has, err := replicaHasColumn(rep2.db, "step_dwells", col)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Errorf("step_dwells.%s missing after migration", col)
		}
	}
}

// TestSensorBitsEncoding pins the wire contract for sensor snapshots. The frontend decodes
// this string positionally, so bit order and width are not free to change.
func TestSensorBitsEncoding(t *testing.T) {
	if len(sensorBitOrder) != 32 {
		t.Fatalf("sensorBitOrder = %d inputs, want 32", len(sensorBitOrder))
	}
	// Numeric (byte, bit) order, NOT lexicographic — X0.2 must precede X0.10.
	if sensorBitOrder[0] != "X0.0" {
		t.Errorf("first bit = %q, want X0.0", sensorBitOrder[0])
	}
	idx := map[string]int{}
	for i, tag := range sensorBitOrder {
		idx[tag] = i
	}
	if idx["X0.2"] > idx["X0.10"] {
		t.Errorf("X0.2 (bit %d) sorted after X0.10 (bit %d) — lexicographic order leaked in",
			idx["X0.2"], idx["X0.10"])
	}

	all := map[string]bool{}
	for _, tag := range sensorBitOrder {
		all[tag] = false
	}
	all["X0.0"] = true
	all[sensorBitOrder[31]] = true
	raw, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	bits := encodeSensorBits(string(raw))
	if len(bits) != 32 {
		t.Fatalf("bits = %q (len %d), want width 32", bits, len(bits))
	}
	if bits[0] != '1' || bits[31] != '1' {
		t.Errorf("bits = %q, want first and last set", bits)
	}
	if strings.Count(bits, "1") != 2 {
		t.Errorf("bits = %q, want exactly 2 set", bits)
	}
}

// TestSensorBitsEmptyIsNotAllZeroes: "no snapshot" and "every input reads low" are different
// facts. Reporting the second when we mean the first would be fabrication.
func TestSensorBitsEmptyIsNotAllZeroes(t *testing.T) {
	for _, in := range []string{"", "  ", "{}", "null", "not json"} {
		if got := encodeSensorBits(in); got != "" {
			t.Errorf("encodeSensorBits(%q) = %q, want empty", in, got)
		}
	}
}

// TestFSMEventCarriesSensorBits is the end-to-end check: a seeded snapshot must reach the
// replica as a decodable bit string.
func TestFSMEventCarriesSensorBits(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	const orderKey = "ORD-bits01"

	seedOrder(t, db, orderKey, 2, base)
	mustExec(t, db,
		`INSERT INTO fsm_events (ts_utc, event_kind, current_state, step_from, step_to,
		                         order_key, sensors_json)
		 VALUES (?,?,?,?,?,?,?)`,
		srcTime(base.Add(time.Second)), "input_changed", "AutoCycle", 1, 1, orderKey,
		`{"X0.0":true,"X0.1":false,"X1.5":true}`)
	seedStep(t, db, orderKey, "AutoCycle", 1, autoCycleCompleteStep, base.Add(2*time.Second))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	bits := scalarStr(t, rep,
		`SELECT sensors_bits FROM fsm_events WHERE order_key=? AND sensors_bits IS NOT NULL LIMIT 1`, orderKey)
	if len(bits) != 32 {
		t.Fatalf("sensors_bits = %q (len %d), want width 32", bits, len(bits))
	}
	idx := map[string]int{}
	for i, tag := range sensorBitOrder {
		idx[tag] = i
	}
	if bits[idx["X0.0"]] != '1' {
		t.Errorf("X0.0 should be set: %q", bits)
	}
	if bits[idx["X1.5"]] != '1' {
		t.Errorf("X1.5 should be set: %q", bits)
	}
	if bits[idx["X0.1"]] != '0' {
		t.Errorf("X0.1 should be clear: %q", bits)
	}
	// The raw JSON must still be in the replica — it is what the bits are checked against.
	if raw := scalarStr(t, rep,
		`SELECT sensors_json FROM fsm_events WHERE order_key=? AND sensors_json IS NOT NULL LIMIT 1`,
		orderKey); raw == "" {
		t.Error("sensors_json should remain in the replica for local inspection")
	}
}

// seedMaintenanceCIP drives a full CIP cycle: HomeIdle -> Maintenance, the CIP steps, then
// back out. reachComplete controls whether step 7 ("CIP cycle complete") is reached.
func seedMaintenanceCIP(t *testing.T, db *sql.DB, start time.Time, reachComplete bool) time.Time {
	t.Helper()
	seedTransition(t, db, "", "HomeIdle", "Maintenance", start)
	steps := []int{0, 10, 1, 2, 3, 4, 5, 6}
	if reachComplete {
		steps = append(steps, cipCompleteStep)
	}
	at := start
	for i, s := range steps {
		at = start.Add(time.Duration(i+1) * 30 * time.Second)
		seedStep(t, db, "", "Maintenance", s, s, at)
	}
	end := at.Add(30 * time.Second)
	seedTransition(t, db, "", "Maintenance", "HomeIdle", end)
	return end
}

// TestCIPRunEmittedFromMaintenanceSpan: Maintenance IS the CIP cycle in this firmware, so a
// closed span in that state is one run, and step 7 is the firmware's own completion marker.
func TestCIPRunEmittedFromMaintenanceSpan(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	end := seedMaintenanceCIP(t, db, base.Add(time.Minute), true)
	// Something after the span so the interval closes and the timeline moves past it.
	seedTransition(t, db, "", "HomeIdle", "AutoCycle", end.Add(2*time.Minute))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM cip_runs`); n != 1 {
		t.Fatalf("cip_runs = %d, want 1", n)
	}
	if c := scalarInt(t, rep, `SELECT completed FROM cip_runs`); c != 1 {
		t.Errorf("completed = %d, want 1 — Maintenance step %d was reached", c, cipCompleteStep)
	}
	if d := scalarInt(t, rep, `SELECT duration_ms FROM cip_runs`); d <= 0 {
		t.Errorf("duration_ms = %d, want > 0", d)
	}
	// trigger_source is deliberately NULL: the controller records no auto/manual distinction.
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM cip_runs WHERE trigger_source IS NULL`); n != 1 {
		t.Error("trigger_source should be NULL rather than a guessed value")
	}
}

// TestCIPRunInterruptedIsNotCompleted — a clean that never reached step 7 is a real run that
// did not finish, which is exactly the distinction the current dashboard cannot make.
func TestCIPRunInterruptedIsNotCompleted(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	end := seedMaintenanceCIP(t, db, base.Add(time.Minute), false)
	seedTransition(t, db, "", "HomeIdle", "AutoCycle", end.Add(2*time.Minute))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM cip_runs`); n != 1 {
		t.Fatalf("cip_runs = %d, want 1 — an interrupted clean is still a run", n)
	}
	if c := scalarInt(t, rep, `SELECT completed FROM cip_runs`); c != 0 {
		t.Errorf("completed = %d, want 0", c)
	}
}

// TestHourlyRollupPartitionsThePeriod is the invariant that makes availability computable:
// run + error + maintenance + idle must account for the whole hour, which only holds if state
// spans are clipped to the bucket rather than attributed to the hour they started in.
func TestHourlyRollupPartitionsThePeriod(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	// Enter HomeIdle well before the bucket and leave well after it, so the span strictly
	// contains a full hour and clipping is the only way to get the arithmetic right.
	hourStart := base.Truncate(time.Hour).Add(time.Hour)
	seedTransition(t, db, "", "", "HomeIdle", hourStart.Add(-30*time.Minute))
	seedTransition(t, db, "", "HomeIdle", "AutoCycle", hourStart.Add(90*time.Minute))
	seedStep(t, db, "", "AutoCycle", 1, 2, hourStart.Add(100*time.Minute))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	bucket := hourStart.UnixMilli()
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM hourly_rollups WHERE bucket_start_ms=?`, bucket); n != 1 {
		t.Fatalf("hourly_rollups for the closed bucket = %d, want 1", n)
	}
	total := scalarInt(t, rep,
		`SELECT run_ms + error_ms + maintenance_ms + idle_ms FROM hourly_rollups WHERE bucket_start_ms=?`, bucket)
	if total != 3600000 {
		t.Errorf("run+error+maintenance+idle = %d ms, want 3600000 — the four columns must "+
			"partition the hour or availability computed from them is wrong", total)
	}
	if idle := scalarInt(t, rep, `SELECT idle_ms FROM hourly_rollups WHERE bucket_start_ms=?`, bucket); idle != 3600000 {
		t.Errorf("idle_ms = %d, want the whole hour", idle)
	}
}

// TestRollupsOnlyCoverClosedBuckets — a bucket the timeline has not passed must not be
// emitted, because more rows can still land in it and rollup rows are never revised.
func TestRollupsOnlyCoverClosedBuckets(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	hourStart := base.Truncate(time.Hour).Add(time.Hour)
	seedTransition(t, db, "", "", "HomeIdle", hourStart.Add(-10*time.Minute))
	// Last event lands 20 minutes INTO the bucket, so it is still open.
	seedStep(t, db, "", "HomeIdle", 0, 1, hourStart.Add(20*time.Minute))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep,
		`SELECT COUNT(*) FROM hourly_rollups WHERE bucket_start_ms >= ?`, hourStart.UnixMilli()); n != 0 {
		t.Errorf("%d rollup rows for a bucket the timeline has not passed, want 0", n)
	}
}

// TestHourlyStepStatsAggregate checks the shape the "which step is slow" view reads, including
// that duration_ms_max survives alongside the sum — an average alone hides the outlier.
func TestHourlyStepStatsAggregate(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	hourStart := base.Truncate(time.Hour).Add(time.Hour)
	for i := 0; i < 3; i++ {
		s := hourStart.Add(time.Duration(i*5) * time.Minute)
		seedStepRun(t, db, "", "AutoCycle", 10, s, s.Add(time.Duration(i+1)*time.Second), "AutoCycle", nil, "")
	}
	seedTransition(t, db, "", "HomeIdle", "AutoCycle", hourStart.Add(70*time.Minute))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	bucket := hourStart.UnixMilli()
	if n := scalarInt(t, rep,
		`SELECT dwell_count FROM hourly_step_stats WHERE bucket_start_ms=? AND state='AutoCycle' AND step=10`,
		bucket); n != 3 {
		t.Errorf("dwell_count = %d, want 3", n)
	}
	if max := scalarInt(t, rep,
		`SELECT duration_ms_max FROM hourly_step_stats WHERE bucket_start_ms=? AND state='AutoCycle' AND step=10`,
		bucket); max != 3000 {
		t.Errorf("duration_ms_max = %d, want 3000 (the slowest of the three)", max)
	}
	if title := scalarStr(t, rep,
		`SELECT step_title FROM hourly_step_stats WHERE bucket_start_ms=? AND state='AutoCycle' AND step=10`,
		bucket); title == "" {
		t.Error("step_title should come from the firmware's own metadata")
	}
}

// TestRollupsAreNotDuplicatedOnRerun — the pass is cursored, so running again must not emit a
// second copy of an already-closed bucket.
func TestRollupsAreNotDuplicatedOnRerun(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	dir := t.TempDir()

	hourStart := base.Truncate(time.Hour).Add(time.Hour)
	seedTransition(t, db, "", "", "HomeIdle", hourStart.Add(-30*time.Minute))
	seedTransition(t, db, "", "HomeIdle", "AutoCycle", hourStart.Add(90*time.Minute))

	first := runOnce(t, sourcePath, dir)
	before := scalarInt(t, openRO(t, first), `SELECT COUNT(*) FROM hourly_rollups`)
	if before == 0 {
		t.Fatal("expected at least one rollup row")
	}
	second := runOnce(t, sourcePath, dir)
	after := scalarInt(t, openRO(t, second), `SELECT COUNT(*) FROM hourly_rollups`)
	if after != before {
		t.Errorf("hourly_rollups %d -> %d across runs; the bucket cursor should prevent re-emission",
			before, after)
	}
}

// TestSensorBitsBackfill covers the upgrade path on a live replica: ALTER TABLE ADD COLUMN
// leaves existing rows NULL, and since sensors_json is no longer replicated those rows would
// otherwise reach the cloud with no sensor state at all.
func TestSensorBitsBackfill(t *testing.T) {
	dir := t.TempDir()
	replicaPath := filepath.Join(dir, "canebot_replica.db")
	statePath := filepath.Join(dir, "projector_state.db")

	st, err := openState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := openReplica(replicaPath, st)
	if err != nil {
		t.Fatal(err)
	}

	// fsm_events.order_key is a FK to cycles, so the parent interval has to exist first.
	if _, err := rep.db.Exec(
		`INSERT INTO cycles (event_ts, started_at_ms, ended_at_ms, duration_ms, order_key,
		                     is_production, result, hour_bucket_ms, date_utc)
		 VALUES ('2026-08-13T00:00:00.000000000Z',0,1,1,'IDLE-x',0,'idle',0,'2026-08-13')`); err != nil {
		t.Fatal(err)
	}

	// Rows as an older build left them: snapshot present, bits never computed. One row has an
	// unparseable snapshot, which must not stall the walk.
	seedRow := func(id int64, ts, sensors string) {
		if _, err := rep.db.Exec(
			`INSERT INTO fsm_events (id, event_ts, event_at_ms, order_key, src_id, event_kind,
			                         sensors_json, sensors_bits, is_production, hour_bucket_ms, date_utc)
			 VALUES (?,?,?,?,?,?,?,NULL,0,0,'2026-08-13')`,
			id, ts, id, "IDLE-x", id, "input_changed", sensors); err != nil {
			t.Fatal(err)
		}
	}
	seedRow(1, "2026-08-13T00:00:00.000000001Z", `{"X0.0":true,"X1.5":true}`)
	seedRow(2, "2026-08-13T00:00:00.000000002Z", `{"X0.0":false}`)
	seedRow(3, "2026-08-13T00:00:00.000000003Z", `not json at all`)
	rep.Close()
	st.Close()

	// Reopening runs the backfill.
	st2, err := openState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	rep2, err := openReplica(replicaPath, st2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rep2.Close()

	var bits1, bits2 string
	if err := rep2.db.QueryRow(`SELECT sensors_bits FROM fsm_events WHERE id=1`).Scan(&bits1); err != nil {
		t.Fatal(err)
	}
	if err := rep2.db.QueryRow(`SELECT sensors_bits FROM fsm_events WHERE id=2`).Scan(&bits2); err != nil {
		t.Fatal(err)
	}
	if len(bits1) != 32 || len(bits2) != 32 {
		t.Fatalf("backfilled widths: %q (%d), %q (%d)", bits1, len(bits1), bits2, len(bits2))
	}
	idx := map[string]int{}
	for i, tag := range sensorBitOrder {
		idx[tag] = i
	}
	if bits1[idx["X0.0"]] != '1' || bits1[idx["X1.5"]] != '1' {
		t.Errorf("row 1 bits = %q, want X0.0 and X1.5 set", bits1)
	}
	if strings.Count(bits2, "1") != 0 {
		t.Errorf("row 2 bits = %q, want all clear", bits2)
	}
	// The unparseable row stays NULL — and must not be revisited, which is what the id-based
	// walk guarantees. Reaching this line at all means the loop terminated.
	var bad sql.NullString
	if err := rep2.db.QueryRow(`SELECT sensors_bits FROM fsm_events WHERE id=3`).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad.Valid {
		t.Errorf("unparseable snapshot became %q, want NULL", bad.String)
	}

	// Already-filled rows must not be rewritten on the next start.
	if _, err := rep2.db.Exec(`UPDATE fsm_events SET sensors_bits = 'SENTINEL' WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if err := backfillSensorBits(rep2.db); err != nil {
		t.Fatal(err)
	}
	var after string
	if err := rep2.db.QueryRow(`SELECT sensors_bits FROM fsm_events WHERE id=1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != "SENTINEL" {
		t.Errorf("backfill rewrote an already-filled row: %q", after)
	}
}

// ---------------------------------------------------------------------------
// Late-arriving orders row
// ---------------------------------------------------------------------------

// The firmware writes the orders row when it consumes the order, and writes the FSM rows
// as the cycle runs. Those are separate tables with separate read cursors, so nothing
// guarantees the projector sees the orders row in the same tick as the events — and if a
// table returns zero rows this pass, it contributes no clamp to safeUpTo.
//
// When that happens the cycle is written from the events alone, with recipe_id and
// glass_count null. The orders row then arrives, ApplyOrder reopens the interval, and the
// insert hits ON CONFLICT(order_key) DO NOTHING — so the recipe is dropped on the floor
// and never recoverable, because cycles is immutable after the first write.
//
// This is the shape of the production symptom: recipe_id and glass_count null on every
// replicated row while the cycles themselves look correct.
func TestRecipeArrivesAfterTheCycleWasWritten(t *testing.T) {
	// The base branch, so dwells are derived from fsm_events and the child backfill below
	// has rows to act on. The bug itself is branch-independent: both branches read `orders`
	// through its own cursor.
	sourcePath, db := newBaseSourceDB(t)
	dir := t.TempDir()

	// Tick 1: the cycle runs to completion, but the orders row is not visible yet.
	for i := 0; i < autoCycleCompleteStep; i++ {
		seedStep(t, db, "ORD-late01", "AutoCycle", i, i+1, base.Add(time.Duration(i+1)*time.Second))
	}
	runOnce(t, sourcePath, dir)

	// Tick 2: the orders row lands.
	seedOrder(t, db, "ORD-late01", 4, base)
	replica := runOnce(t, sourcePath, dir)

	rep := openRO(t, replica)

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM cycles WHERE order_key='ORD-late01'`); n != 1 {
		t.Fatalf("cycles rows = %d, want exactly 1 (order_key is unique)", n)
	}

	var recipe, glass sql.NullInt64
	err := rep.QueryRow(
		`SELECT recipe_id, glass_count FROM cycles WHERE order_key='ORD-late01'`,
	).Scan(&recipe, &glass)
	if err != nil {
		t.Fatalf("read cycle: %v", err)
	}

	if !recipe.Valid || recipe.Int64 != 4 {
		t.Errorf("recipe_id = %v, want 4 — the orders row arrived after the cycle was written "+
			"and was discarded by ON CONFLICT DO NOTHING", recipe)
	}
	if !glass.Valid || glass.Int64 != 1 {
		t.Errorf("glass_count = %v, want 1", glass)
	}

	// The children denormalise recipe_id so the dashboard never joins. They were written
	// in the first tick, before the recipe was known, so filling only the parent would
	// leave a cycle whose own dwells disagree with it.
	if r := scalarInt(t, rep,
		`SELECT COALESCE(recipe_id, -1) FROM step_dwells WHERE order_key='ORD-late01' LIMIT 1`); r != 4 {
		t.Errorf("step_dwells.recipe_id = %d, want 4 (backfilled from the late orders row)", r)
	}
}

// The backfill must never overwrite a value already recorded, or a re-derivation could
// replace good data with a null from an interval that happened not to carry it.
func TestBackfillNeverOverwritesAnExistingRecipe(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	dir := t.TempDir()

	seedCompletedCycle(t, db, "ORD-keep01", base) // seeds recipe 3
	runOnce(t, sourcePath, dir)

	// Run again over the same source: every interval is re-derived and re-flushed.
	replica := runOnce(t, sourcePath, dir)
	rep := openRO(t, replica)

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM cycles WHERE order_key='ORD-keep01'`); n != 1 {
		t.Fatalf("cycles rows = %d, want exactly 1", n)
	}
	if r := scalarInt(t, rep,
		`SELECT COALESCE(recipe_id, -1) FROM cycles WHERE order_key='ORD-keep01'`); r != 3 {
		t.Errorf("recipe_id = %d, want 3 — a re-run must not disturb a settled value", r)
	}
}

// ---------------------------------------------------------------------------
// Serving columns: outcome and the sub-hour bucket keys
// ---------------------------------------------------------------------------

// QueryScript has no CASE and no time_bucket(), so a dashboard reading this replica cannot
// collapse `result` into chart series or group rows into time buckets by itself without
// pulling every raw row and doing it in the browser. Both are computed here instead.
func TestOutcomeAndBucketKeysArePopulated(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	seedCompletedCycle(t, db, "ORD-buck01", base)
	replica := runOnce(t, sourcePath, t.TempDir())
	rep := openRO(t, replica)

	if got := scalarStr(t, rep,
		`SELECT outcome FROM cycles WHERE order_key='ORD-buck01'`); got != "success" {
		t.Errorf("outcome = %q, want %q", got, "success")
	}

	var b1, b5, b15, started int64
	err := rep.QueryRow(
		`SELECT bucket_1m_ms, bucket_5m_ms, bucket_15m_ms, started_at_ms
		   FROM cycles WHERE order_key='ORD-buck01'`,
	).Scan(&b1, &b5, &b15, &started)
	if err != nil {
		t.Fatalf("read buckets: %v", err)
	}

	for _, c := range []struct {
		name  string
		got   int64
		width int64
	}{
		{"bucket_1m_ms", b1, 60_000},
		{"bucket_5m_ms", b5, 5 * 60_000},
		{"bucket_15m_ms", b15, 15 * 60_000},
	} {
		want := started - (started % c.width)
		if c.got != want {
			t.Errorf("%s = %d, want %d (floor of started_at_ms)", c.name, c.got, want)
		}
		if c.got%c.width != 0 {
			t.Errorf("%s = %d is not aligned to its %dms width", c.name, c.got, c.width)
		}
	}
}

// A replica written before these columns existed is migrated in place. ALTER TABLE leaves
// every existing row at the DEFAULT, so without a backfill they would all claim to belong to
// the same bucket at the epoch — worse than not having the column, because it looks valid.
func TestMigrationBackfillsBucketsOnExistingRows(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	dir := t.TempDir()
	seedCompletedCycle(t, db, "ORD-mig001", base)
	replica := runOnce(t, sourcePath, dir)

	// Drop the columns back off to simulate a replica from before this change, then let the
	// projector migrate it on its next open.
	rw, err := sql.Open("sqlite", "file:"+replica)
	if err != nil {
		t.Fatalf("open replica rw: %v", err)
	}
	for _, col := range []string{"outcome", "bucket_1m_ms", "bucket_5m_ms", "bucket_15m_ms"} {
		if _, err := rw.Exec(`ALTER TABLE cycles DROP COLUMN ` + col); err != nil {
			t.Fatalf("drop %s: %v", col, err)
		}
	}
	rw.Close()

	replica = runOnce(t, sourcePath, dir)
	rep := openRO(t, replica)

	var b5, started int64
	if err := rep.QueryRow(
		`SELECT bucket_5m_ms, started_at_ms FROM cycles WHERE order_key='ORD-mig001'`,
	).Scan(&b5, &started); err != nil {
		t.Fatalf("read migrated row: %v", err)
	}
	if want := started - (started % (5 * 60_000)); b5 != want {
		t.Errorf("bucket_5m_ms = %d, want %d — the pre-existing row kept the DEFAULT", b5, want)
	}
	if got := scalarStr(t, rep,
		`SELECT outcome FROM cycles WHERE order_key='ORD-mig001'`); got != "success" {
		t.Errorf("migrated outcome = %q, want %q", got, "success")
	}
}

// The full mapping, including the values the integration test above cannot reach without
// constructing every kind of interval. `result` keeps all eight values as the audit trail;
// `outcome` is the three the charts stack, plus the bucket for machine time that is not a
// drink at all.
func TestOutcomeMapping(t *testing.T) {
	cases := []struct {
		result       string
		isProduction bool
		want         string
	}{
		{resultCompleted, true, "success"},
		{resultFaultedRecoverable, true, "recovered"},
		{resultFaultedNonRecoverable, true, "failed"},
		// aborted means the drink was not made. The dashboard has three series and never had
		// a bucket for it, so it joins the failures rather than becoming a fourth.
		{resultAborted, true, "failed"},
		{resultIdle, false, "non_production"},
		{resultMaintenance, false, "non_production"},
		{resultManual, false, "non_production"},
		{resultError, false, "non_production"},
	}
	for _, c := range cases {
		if got := outcomeFor(c.result, c.isProduction); got != c.want {
			t.Errorf("outcomeFor(%q, %v) = %q, want %q", c.result, c.isProduction, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Step sensor state
// ---------------------------------------------------------------------------

// The JSON snapshots are held back from replication at ~820 B/row, and fsm_events — the only
// other table carrying sensor state — is on its way out of the replication set. So the step's
// own snapshot has to travel as bits, and the two inputs with documented meaning have to be
// queryable, because the dashboard's query language has no substring operator.
func TestStepDwellCarriesSensorBitsAndDecodedInputs(t *testing.T) {
	sourcePath, db := newSourceDB(t)

	// seedStepRun writes the snapshots the firmware would: X0.0 true at the step start and
	// false at its end. X0.0 true = door CLOSED, which is the firmware's own convention and
	// the reason the column is named for the meaning rather than the pin.
	const startSnapshot = `{"X0.0":true}`
	seedOrder(t, db, "ORD-sens01", 2, base)
	seedStepRun(t, db, "ORD-sens01", "AutoCycle", 3,
		base.Add(1*time.Second), base.Add(2*time.Second), "", nil, "")

	replica := runOnce(t, sourcePath, t.TempDir())
	rep := openRO(t, replica)

	bits := scalarStr(t, rep,
		`SELECT sensors_start_bits FROM step_dwells WHERE order_key='ORD-sens01' LIMIT 1`)
	if want := encodeSensorBits(startSnapshot); bits != want {
		t.Errorf("sensors_start_bits = %q, want %q", bits, want)
	}
	if len(bits) != len(sensorBitOrder) {
		t.Errorf("sensors_start_bits is %d chars, want the fixed %d — a variable width is what "+
			"makes a misaligned decode undetectable", len(bits), len(sensorBitOrder))
	}

	if got := scalarInt(t, rep,
		`SELECT door_closed FROM step_dwells WHERE order_key='ORD-sens01' LIMIT 1`); got != 1 {
		t.Errorf("door_closed = %d, want 1 (X0.0 true means closed)", got)
	}
	// X0.7 is absent from the snapshot, so its state is unknown rather than low.
	var cip sql.NullInt64
	if err := rep.QueryRow(
		`SELECT cip_bypass FROM step_dwells WHERE order_key='ORD-sens01' LIMIT 1`,
	).Scan(&cip); err != nil {
		t.Fatalf("read cip_bypass: %v", err)
	}
	if cip.Valid {
		t.Errorf("cip_bypass = %d, want NULL — X0.7 is not in the snapshot, so it is unknown",
			cip.Int64)
	}

	// The end snapshot differs from the start, which is the whole point of carrying both.
	endBits := scalarStr(t, rep,
		`SELECT sensors_end_bits FROM step_dwells WHERE order_key='ORD-sens01' LIMIT 1`)
	if endBits == bits {
		t.Errorf("sensors_end_bits equals sensors_start_bits (%q); the seeded run flips X0.0", bits)
	}
}

// No snapshot and every input reading low are different facts. A step run that recorded
// nothing must not claim the door was open.
func TestMissingSnapshotIsNullNotZero(t *testing.T) {
	if v := doorClosed(""); v != nil {
		t.Errorf("doorClosed(\"\") = %v, want nil", *v)
	}
	if v := doorClosed("{}"); v != nil {
		t.Errorf("doorClosed(\"{}\") = %v, want nil", *v)
	}
	// Present but absent from the snapshot is also unknown, not false.
	if v := doorClosed(`{"X0.4":true}`); v != nil {
		t.Errorf("doorClosed with X0.0 absent = %v, want nil", *v)
	}
	if v := doorClosed(`{"X0.0":false}`); v == nil || *v != 0 {
		t.Errorf("doorClosed with X0.0 false = %v, want 0", v)
	}
}
