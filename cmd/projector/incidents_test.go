package main

import (
	"database/sql"
	"testing"
	"time"
)

// seedEventSent writes a non-transition event carrying whatever CurrentState its sender read,
// which is how the controller logs FaultDetected (fsm.machine.SendEvent).
func seedEventSent(t *testing.T, db *sql.DB, orderKey, state, eventType string, at time.Time) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO fsm_events (ts_utc, event_kind, current_state, event_type, order_key, source)
		 VALUES (?,?,?,?,?,?)`,
		srcTime(at), "event_sent", state, eventType, nullable(orderKey), "fsm.machine.SendEvent")
}

// seedDoor opens and closes the main door, pressing the fault reset while open if resetAt is set.
func seedDoor(t *testing.T, db *sql.DB, openAt, closeAt time.Time, resetAt *time.Time) {
	t.Helper()
	seedToggle(t, db, "", "X0.0", "Main door switch", 1, 0, "Error", 18, openAt)
	if resetAt != nil {
		seedToggle(t, db, "", "X0.7", "CIP bypass switch", 0, 1, "Manual", 18, *resetAt)
	}
	seedToggle(t, db, "", "X0.0", "Main door switch", 0, 1, "Manual", 18, closeAt)
}

// seedRaise is one fault as the controller writes it: the transition to Error, then the
// FaultDetected event_sent still reading the state it left, then the faults row.
func seedRaise(t *testing.T, db *sql.DB, orderKey string, at time.Time) {
	t.Helper()
	seedTransition(t, db, orderKey, "AutoCycle", "Error", at.Add(100*time.Microsecond))
	seedEventSent(t, db, orderKey, "AutoCycle", "FaultDetected", at.Add(400*time.Microsecond))
	seedFault(t, db, orderKey, "CupImproperDrop", 18, at.Add(400*time.Microsecond))
}

func sec(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }

// TestStaleStateSampleDoesNotReopenRunning replays fault 147 (18 Sep 20:48:09): the stale
// FaultDetected sample lands after the transition to Error in the same millisecond, and the
// machine then sits faulted for a minute with no further transition.
func TestStaleStateSampleDoesNotReopenRunning(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	seedOrder(t, db, "ORD-stale", 1, base)
	seedTransition(t, db, "ORD-stale", "HomeIdle", "AutoCycle", base)
	seedRaise(t, db, "ORD-stale", sec(10))
	seedTransition(t, db, "", "Error", "Manual", sec(70))
	seedTransition(t, db, "", "Manual", "HomeIdle", sec(80))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if ms := scalarInt(t, rep, `SELECT COALESCE(SUM(duration_ms),0) FROM state_durations
	                             WHERE state = 'AutoCycle' AND entered_at_ms >= ?`,
		sec(10).UnixMilli()); ms != 0 {
		t.Errorf("AutoCycle after the fault = %dms, want 0 — the stale sample reopened it", ms)
	}
	if ms := scalarInt(t, rep, `SELECT COALESCE(SUM(duration_ms),0) FROM state_durations
	                             WHERE state = 'Error'`); ms != 60000 {
		t.Errorf("Error = %dms, want 60000 (fault 10s → Manual 70s)", ms)
	}
}

// TestFaultIncidentFoldsReRaises is the 18 Sep breakdown in miniature: a fault, a door close
// without reset that re-raises it at once, then a proper reset, a clean and a return to idle.
// One incident, two faults, downtime from the first fault to HomeIdle.
func TestFaultIncidentFoldsReRaises(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	seedOrder(t, db, "ORD-inc", 3, base)
	seedTransition(t, db, "ORD-inc", "HomeIdle", "AutoCycle", base)
	seedRaise(t, db, "ORD-inc", sec(10))

	// Door open 20→30s, no reset; closing resumes AutoCycle, which re-faults 50ms later.
	seedDoor(t, db, sec(20), sec(30), nil)
	seedTransition(t, db, "ORD-inc", "Error", "Manual", sec(20))
	seedTransition(t, db, "ORD-inc", "Manual", "AutoCycle", sec(30))
	seedRaise(t, db, "ORD-inc", sec(30).Add(50*time.Millisecond))

	// Door open 40→100s with a reset at 45s, then a clean 100→130s and idle.
	reset := sec(45)
	seedDoor(t, db, sec(40), sec(100), &reset)
	seedTransition(t, db, "", "Error", "Manual", sec(40))
	seedTransition(t, db, "", "Manual", "Maintenance_regular", sec(100))
	seedTransition(t, db, "", "Maintenance_regular", "HomeIdle", sec(130))
	seedTransition(t, db, "", "HomeIdle", "AutoCycle", sec(200)) // flushes the HomeIdle span

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fault_incidents`); n != 1 {
		t.Fatalf("fault_incidents rows = %d, want 1", n)
	}
	for _, c := range []struct {
		col  string
		want int64
	}{
		{"fault_count", 2},
		{"raised_at_ms", sec(10).UnixMilli()},
		{"recovered_at_ms", sec(130).UnixMilli()},
		{"downtime_ms", 120000},
		{"reset_count", 1},
		{"first_reset_at_ms", sec(45).UnixMilli()},
		{"door_open_count", 2},
		{"door_open_ms", 70000},
		{"first_door_open_at_ms", sec(20).UnixMilli()},
		{"maintenance_ms", 30000},
		{"manual_ms", 70000}, // 20→30s, 40→100s
		{"error_ms", 19950},  // 10→20s, 30.05→40s
		{"run_ms", 50},       // the resume that re-faulted
	} {
		if got := scalarInt(t, rep, `SELECT `+c.col+` FROM fault_incidents`); got != c.want {
			t.Errorf("%s = %d, want %d", c.col, got, c.want)
		}
	}
	if v := scalarStr(t, rep, `SELECT recovered_via FROM fault_incidents`); v != "home_idle" {
		t.Errorf("recovered_via = %q, want home_idle", v)
	}
	// State spans partition the timeline: no gap (the toggle-driven interval switch at 45s
	// used to leave one) and no overlap.
	if sum, span := scalarInt(t, rep, `SELECT SUM(duration_ms) FROM state_durations`),
		scalarInt(t, rep, `SELECT MAX(exited_at_ms) - MIN(entered_at_ms) FROM state_durations`); sum != span {
		t.Errorf("state_durations sum %dms over a %dms timeline — gap or overlap", sum, span)
	}
}

// TestFaultIncidentWaitsForRecovery: while the machine is still down nothing is written, and
// once it recovers the incident is written exactly once, however many passes run after.
func TestFaultIncidentWaitsForRecovery(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	seedOrder(t, db, "ORD-wait", 1, base)
	seedTransition(t, db, "ORD-wait", "HomeIdle", "AutoCycle", base)
	seedRaise(t, db, "ORD-wait", sec(10))
	seedTransition(t, db, "", "Error", "Manual", sec(20))

	dir := t.TempDir()
	rep := openRO(t, runOnce(t, sourcePath, dir))
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fault_incidents`); n != 0 {
		t.Fatalf("incident written while still down: %d rows", n)
	}

	seedTransition(t, db, "", "Manual", "HomeIdle", sec(50))
	seedTransition(t, db, "", "HomeIdle", "AutoCycle", sec(90))
	runOnce(t, sourcePath, dir)
	runOnce(t, sourcePath, dir)

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fault_incidents`); n != 1 {
		t.Fatalf("fault_incidents rows = %d, want 1", n)
	}
	if d := scalarInt(t, rep, `SELECT downtime_ms FROM fault_incidents`); d != 40000 {
		t.Errorf("downtime_ms = %d, want 40000 (fault 10s → HomeIdle 50s)", d)
	}
}

// TestFaultIncidentRecoversOnCompletedCycle: with orders queued the machine never idles, so a
// resumed drink reaching step 19 is the recovery.
func TestFaultIncidentRecoversOnCompletedCycle(t *testing.T) {
	sourcePath, db := newBaseSourceDB(t)
	seedOrder(t, db, "ORD-cyc", 1, base)
	seedTransition(t, db, "ORD-cyc", "HomeIdle", "AutoCycle", base)
	seedStep(t, db, "ORD-cyc", "AutoCycle", 17, 18, sec(5))
	seedRaise(t, db, "ORD-cyc", sec(10))
	reset := sec(25)
	seedDoor(t, db, sec(20), sec(30), &reset)
	seedTransition(t, db, "ORD-cyc", "Error", "Manual", sec(20))
	seedTransition(t, db, "ORD-cyc", "Manual", "AutoCycle", sec(30))
	seedStep(t, db, "ORD-cyc", "AutoCycle", 18, 19, sec(40))
	seedOrder(t, db, "ORD-next", 1, sec(45))
	seedStep(t, db, "ORD-next", "AutoCycle", 0, 1, sec(46))

	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fault_incidents`); n != 1 {
		t.Fatalf("fault_incidents rows = %d, want 1", n)
	}
	if v := scalarStr(t, rep, `SELECT recovered_via FROM fault_incidents`); v != "cycle_complete" {
		t.Errorf("recovered_via = %q, want cycle_complete", v)
	}
	if d := scalarInt(t, rep, `SELECT downtime_ms FROM fault_incidents`); d != 30000 {
		t.Errorf("downtime_ms = %d, want 30000 (fault 10s → step 19 at 40s)", d)
	}
}
