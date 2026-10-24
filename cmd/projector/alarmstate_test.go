package main

import (
	"database/sql"
	"testing"
	"time"
)

const (
	doorOpenJSON   = `{"X0.0":false,"X0.7":false}`
	doorClosedJSON = `{"X0.0":true,"X0.7":false}`
)

func ev(id int64, at time.Time, kind string, f func(*srcFSMEvent)) srcFSMEvent {
	e := srcFSMEvent{ID: id, TS: at, EventKind: kind}
	if f != nil {
		f(&e)
	}
	return e
}

func transition(id int64, at time.Time, to string) srcFSMEvent {
	return ev(id, at, "state_transition", func(e *srcFSMEvent) { e.StateTo = to })
}

func faultEvent(id int64, at time.Time, faultType string) srcFSMEvent {
	return ev(id, at, "event_sent", func(e *srcFSMEvent) {
		e.EventType = "FaultDetected"
		e.PayloadJSON = `{"event_payload":{"FaultID":"` + faultType + `","FaultType":"` + faultType +
			`","Message":"` + faultType + ` happened"},"event_type":"FaultDetected"}`
	})
}

func input(id int64, at time.Time, inputID string, v int64, sensors string) srcFSMEvent {
	return ev(id, at, "input_changed", func(e *srcFSMEvent) {
		e.InputID, e.InputValue, e.SensorsJSON = inputID, &v, sensors
	})
}

// TestAlarmLifecycle replays the sequences seen in the field on 22-23 Oct 2026.
func TestAlarmLifecycle(t *testing.T) {
	t0 := time.Date(2026, 10, 23, 21, 58, 5, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	a := &alarmTracker{}

	// AutoCycle -> Error, FaultDetected.
	a.apply(transition(1, at(0), "Error"))
	a.apply(faultEvent(2, at(0), "CaneSinglingWatchdogFault"))
	if !a.Cur.FaultActive || a.Cur.FaultType != "CaneSinglingWatchdogFault" || a.Cur.FaultRaisedMS != at(0).UnixMilli() {
		t.Fatalf("raise: %+v", a.Cur)
	}

	// Door opened: Error -> Manual. NOT a clear.
	a.apply(input(3, at(10), "X0.0", 0, doorOpenJSON))
	a.apply(transition(4, at(10), "Manual"))
	if !a.Cur.FaultActive || !a.Cur.DoorOpen || a.Cur.DoorOpenedMS != at(10).UnixMilli() {
		t.Fatalf("door open must not clear: %+v", a.Cur)
	}

	// X0.7 pressed: the reset. Type, message and raised time stay for the alarm's identity.
	a.apply(input(5, at(20), "X0.7", 1, doorOpenJSON))
	if a.Cur.FaultActive || a.Cur.ClearReason != "x07_reset" || a.Cur.FaultType != "CaneSinglingWatchdogFault" ||
		a.Cur.FaultRaisedMS != at(0).UnixMilli() || a.Cur.FaultClearedMS != at(20).UnixMilli() {
		t.Fatalf("x07 clear: %+v", a.Cur)
	}

	// Further X0.7 bounces and the door closing change nothing about the fault.
	a.apply(input(6, at(21), "X0.7", 1, doorOpenJSON))
	a.apply(input(7, at(30), "X0.0", 1, doorClosedJSON))
	if a.Cur.FaultActive || a.Cur.DoorOpen || a.Cur.DoorOpenedMS != at(10).UnixMilli() {
		t.Fatalf("after close: %+v", a.Cur)
	}

	// X0.7 with no fault active is a pause, not a reset: no clear fields move.
	a.apply(input(8, at(40), "X0.7", 1, doorClosedJSON))
	if a.Cur.FaultClearedMS != at(20).UnixMilli() {
		t.Fatalf("x07 without fault: %+v", a.Cur)
	}

	// MCB still tripped: resume without reset re-raises the SAME fault — one episode.
	a.apply(faultEvent(9, at(50), "MCBFault"))
	a.apply(faultEvent(10, at(60), "MCBFault"))
	if a.Cur.FaultRaisedMS != at(50).UnixMilli() {
		t.Fatalf("same-type re-raise must keep the episode: %+v", a.Cur)
	}
	// A different fault while active is a new episode.
	a.apply(faultEvent(11, at(70), "HopperEmptyFault"))
	if a.Cur.FaultType != "HopperEmptyFault" || a.Cur.FaultRaisedMS != at(70).UnixMilli() {
		t.Fatalf("new type: %+v", a.Cur)
	}

	// Reaching HomeIdle clears (Modbus reset, startup clear, recovered resume).
	a.apply(transition(12, at(80), "HomeIdle"))
	if a.Cur.FaultActive || a.Cur.ClearReason != "home_idle" {
		t.Fatalf("home idle clear: %+v", a.Cur)
	}

	// A replayed event is ignored: re-applying the raise must not re-open the fault.
	a.apply(faultEvent(11, at(70), "HopperEmptyFault"))
	if a.Cur.FaultActive {
		t.Fatalf("replayed event re-raised: %+v", a.Cur)
	}
}

// TestAlarmStateBootstrapAndResume: a fault raised before the projector first ran, with the
// door already opened into Manual, is still open; a later reset lands on the same fault after
// a restart.
func TestAlarmStateBootstrapAndResume(t *testing.T) {
	src, db := newBaseSourceDB(t)
	dir := t.TempDir()
	t0 := time.Date(2026, 10, 23, 21, 58, 5, 0, time.UTC)

	seedTransition(t, db, "", "AutoCycle", "Error", t0)
	mustExec(t, db, `INSERT INTO fsm_events (ts_utc, event_kind, current_state, event_type, payload_json)
	                 VALUES (?,?,?,?,?)`, srcTime(t0), "event_sent", "Error", "FaultDetected",
		`{"event_payload":{"FaultType":"CaneSinglingWatchdogFault","Message":"cane did not reach tilter"}}`)
	seedInput(t, db, "X0.0", 0, doorOpenJSON, t0.Add(time.Minute))
	seedTransition(t, db, "", "Error", "Manual", t0.Add(time.Minute))

	rep := openRO(t, runOnce(t, src, dir))
	assertLatestAlarm(t, rep, 1, "CaneSinglingWatchdogFault", t0.UnixMilli(), 1)

	// The operator resets; the next run resumes from the persisted tracker.
	seedInput(t, db, "X0.7", 1, doorOpenJSON, t0.Add(2*time.Minute))
	runOnce(t, src, dir)
	assertLatestAlarm(t, rep, 0, "CaneSinglingWatchdogFault", t0.UnixMilli(), 1)
	if got := scalarInt(t, rep, `SELECT COUNT(*) FROM alarm_state`); got != 2 {
		t.Fatalf("rows = %d, want 2 (one startup row per run)", got)
	}
}

func seedInput(t *testing.T, db *sql.DB, inputID string, v int64, sensors string, at time.Time) {
	t.Helper()
	mustExec(t, db,
		`INSERT INTO fsm_events (ts_utc, event_kind, current_state, input_id, input_value, sensors_json)
		 VALUES (?,?,?,?,?,?)`, srcTime(at), "input_changed", "Manual", inputID, v, sensors)
}

func assertLatestAlarm(t *testing.T, rep *sql.DB, active int64, faultType string, raisedMS, doorOpen int64) {
	t.Helper()
	var gotActive, gotRaised, gotDoor int64
	var gotType string
	if err := rep.QueryRow(`SELECT fault_active, fault_type, fault_raised_at_ms, door_open
	                          FROM alarm_state ORDER BY id DESC LIMIT 1`).
		Scan(&gotActive, &gotType, &gotRaised, &gotDoor); err != nil {
		t.Fatalf("read alarm_state: %v", err)
	}
	if gotActive != active || gotType != faultType || gotRaised != raisedMS || gotDoor != doorOpen {
		t.Fatalf("alarm_state = active %d type %q raised %d door %d; want %d %q %d %d",
			gotActive, gotType, gotRaised, gotDoor, active, faultType, raisedMS, doorOpen)
	}
}
