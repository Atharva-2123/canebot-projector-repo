package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"canebot-fsm/io"
)

// alarm_state is the machine's live fault and door state, for the platform's alarm rules.
//
// Every other replica table is built from finished intervals, and an interval can stay open for
// 15 minutes. An alarm cannot wait that long, so this one is derived straight from fsm_events as
// each batch is read — before the settle window, which only exists to order rows across source
// tables, and this reads one. A row is written on every change and at least every
// alarmHeartbeat, so the newest row per machine in the cloud is always the current state, and a
// row lost on the way (the agent skips whatever a table holds when it first sees it) is
// replaced within the heartbeat.
//
// event_ts is when the row was WRITTEN, strictly increasing: the agent's cursor only moves
// forward, so a row stamped with the earlier time its change happened could never ship. The
// change times travel in the *_at_ms columns.
//
// The fault lifecycle, checked against fsm/error.go and two days of field logs:
//
//	raised   FaultDetected (event_sent), which carries FaultType and Message.
//	cleared  an X0.7 rising edge while a fault is active — the operator's reset (monitorIO
//	         resets only when a fault is active; otherwise X0.7 is a pause) — or the machine
//	         reaching HomeIdle, which covers a Modbus 870=5 reset (it writes no fsm_events
//	         row), the startup clear, and a resume that recovered.
//	NOT      Error -> Manual. That is the door being opened, and the fault is still there.
//
// A cleared row keeps the fault's type, message and raised time, and a door row keeps the
// opening's time after the door closes: the alarm rules key their instances on them, so the
// clear has to land on the same instance as the raise.
const alarmHeartbeat = 30 * time.Second

type alarmSnapshot struct {
	FaultActive    bool   `json:"fault_active"`
	FaultType      string `json:"fault_type,omitempty"`
	FaultMessage   string `json:"fault_message,omitempty"`
	FaultRaisedMS  int64  `json:"fault_raised_ms,omitempty"`
	FaultClearedMS int64  `json:"fault_cleared_ms,omitempty"`
	ClearReason    string `json:"clear_reason,omitempty"`
	DoorKnown      bool   `json:"door_known"`
	DoorOpen       bool   `json:"door_open"`
	DoorOpenedMS   int64  `json:"door_opened_ms,omitempty"`
	MachineState   string `json:"machine_state,omitempty"`
}

// alarmTracker is persisted whole, so the state and the fsm_events id it is accurate as of
// can never disagree: a restart replays exactly the events after LastID onto exactly that
// state. Replaying an event onto a state that already includes it would re-raise a cleared
// fault.
type alarmTracker struct {
	Cur        alarmSnapshot `json:"cur"`
	Emitted    alarmSnapshot `json:"emitted"`
	LastID     int64         `json:"last_id"`
	LastEmitMS int64         `json:"last_emit_ms"`

	started bool // a row has been written since this process started
}

func (a *alarmTracker) apply(e srcFSMEvent) {
	if e.ID <= a.LastID {
		return // already applied; the batch was re-read after a failed tick or a restart
	}
	a.LastID = e.ID
	ms := e.TS.UnixMilli()
	s := &a.Cur

	if closed := doorClosed(e.SensorsJSON); closed != nil {
		open := *closed == 0
		if open && (!s.DoorKnown || !s.DoorOpen) {
			s.DoorOpenedMS = ms
		}
		s.DoorKnown, s.DoorOpen = true, open
	}

	switch {
	case e.EventKind == "state_transition":
		s.MachineState = e.StateTo
		if s.FaultActive && e.StateTo == stateHomeIdle {
			s.clear(ms, "home_idle")
		}
	case e.EventKind == "event_sent" && e.EventType == "FaultDetected":
		faultType, message := faultDetected(e.PayloadJSON)
		if s.FaultActive && faultType == s.FaultType {
			// The same fault raised again after a resume without a reset (an MCB still
			// tripped). One episode, one alarm — not a resolve and a re-fire on every door close.
			return
		}
		s.FaultActive, s.FaultType, s.FaultMessage, s.FaultRaisedMS = true, faultType, message, ms
		s.FaultClearedMS, s.ClearReason = 0, ""
	case e.EventKind == "input_changed" && e.InputID == string(io.InputCIPBypassSwitch) &&
		e.InputValue != nil && *e.InputValue != 0:
		if s.FaultActive {
			s.clear(ms, "x07_reset")
		}
	}
}

func (s *alarmSnapshot) clear(ms int64, reason string) {
	s.FaultActive, s.FaultClearedMS, s.ClearReason = false, ms, reason
}

// faultDetected reads FaultType and Message out of a FaultDetected event's payload.
func faultDetected(payloadJSON string) (faultType, message string) {
	var p struct {
		EventPayload struct {
			FaultType string
			Message   string
		} `json:"event_payload"`
	}
	_ = json.Unmarshal([]byte(payloadJSON), &p)
	faultType = p.EventPayload.FaultType
	if faultType == "" {
		faultType = "Unknown"
	}
	return faultType, p.EventPayload.Message
}

// step applies a freshly read batch, writes a row if one is due, and persists the tracker.
func (a *alarmTracker) step(ctx context.Context, rep *replicaWriter, st *stateStore, events []srcFSMEvent, nowMS int64) error {
	before := a.LastID
	for _, e := range events {
		a.apply(e)
	}
	wrote := false
	if reason := a.due(nowMS); reason != "" {
		if err := a.emit(ctx, rep, nowMS, reason); err != nil {
			return err
		}
		wrote = true
	}
	if !wrote && a.LastID == before {
		return nil
	}
	return st.SetAlarmTracker(a)
}

func (a *alarmTracker) due(nowMS int64) string {
	switch {
	case !a.started:
		return "startup"
	case a.Cur != a.Emitted:
		return "change"
	case nowMS-a.LastEmitMS >= alarmHeartbeat.Milliseconds():
		return "heartbeat"
	}
	return ""
}

func (a *alarmTracker) emit(ctx context.Context, rep *replicaWriter, nowMS int64, reason string) error {
	ms := max(nowMS, a.LastEmitMS+1)
	ts, err := rep.guard("alarm_state", ms)
	if err != nil {
		return err
	}
	s := a.Cur
	var door any
	if s.DoorKnown {
		door = boolInt(s.DoorOpen)
	}
	if _, err := rep.db.ExecContext(ctx,
		`INSERT INTO alarm_state (event_ts, fault_active, fault_type, fault_message,
		                          fault_raised_at_ms, fault_cleared_at_ms, clear_reason,
		                          door_open, door_opened_at_ms, machine_state, reason, date_utc)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		ts, boolInt(s.FaultActive), nullIfEmpty(s.FaultType), nullIfEmpty(s.FaultMessage),
		nullIfZero(s.FaultRaisedMS), nullIfZero(s.FaultClearedMS), nullIfEmpty(s.ClearReason),
		door, nullIfZero(s.DoorOpenedMS), nullIfEmpty(s.MachineState), reason, dateUTC(ms),
	); err != nil {
		return fmt.Errorf("insert alarm_state: %w", err)
	}
	rep.st.SetWatermark("alarm_state", ts)
	a.Emitted, a.LastEmitMS, a.started = s, ms, true
	return nil
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func nullIfZero(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// loadAlarmTracker returns the persisted tracker, or builds one from the controller's history
// on the first run (or after projector_state.db was lost).
//
// The bootstrap replays every event since the last FaultDetected, so a fault raised before
// this projector started — and possibly still open, with the door already opened into Manual —
// is known. With no FaultDetected left in the controller's retention there is no fault to
// recover, unless the machine sits in Error right now, in which case it is reported with an
// unknown type rather than missed.
func loadAlarmTracker(ctx context.Context, src *sourceReader, st *stateStore) (*alarmTracker, error) {
	if a, ok, err := st.AlarmTracker(); err != nil || ok {
		return a, err
	}
	a := &alarmTracker{}
	latestID, latestState, latestSensors, err := src.LatestFSMEvent(ctx)
	if err != nil {
		return nil, err
	}
	faultID, err := src.LastFaultDetectedID(ctx)
	if err != nil {
		return nil, err
	}
	if faultID > 0 {
		a.LastID = faultID - 1
		for a.LastID < latestID {
			events, err := src.FSMEventsAfter(ctx, a.LastID)
			if err != nil {
				return nil, err
			}
			if len(events) == 0 {
				break
			}
			for _, e := range events {
				if e.ID <= latestID {
					a.apply(e)
				}
			}
			a.LastID = max(a.LastID, events[len(events)-1].ID)
		}
	}
	a.LastID = latestID
	if a.Cur.MachineState == "" {
		a.Cur.MachineState = latestState
	}
	if !a.Cur.FaultActive && a.Cur.FaultRaisedMS == 0 && latestState == "Error" {
		a.Cur.FaultActive, a.Cur.FaultType = true, "Unknown"
		a.Cur.FaultRaisedMS = time.Now().UnixMilli()
	}
	if closed := doorClosed(latestSensors); closed != nil && !a.Cur.DoorKnown {
		a.Cur.DoorKnown, a.Cur.DoorOpen = true, *closed == 0
	}
	logf("alarm_state bootstrap: last FaultDetected id=%d, fsm_events id=%d, state=%+v", faultID, latestID, a.Cur)
	return a, nil
}

// LatestFSMEvent is the newest event's id, state and sensor snapshot.
func (r *sourceReader) LatestFSMEvent(ctx context.Context) (id int64, state, sensors string, err error) {
	err = r.db.QueryRowContext(ctx,
		`SELECT id, COALESCE(current_state,''), COALESCE(sensors_json,'')
		   FROM fsm_events ORDER BY id DESC LIMIT 1`).Scan(&id, &state, &sensors)
	if err == sql.ErrNoRows {
		return 0, "", "", nil
	}
	if err != nil {
		return 0, "", "", fmt.Errorf("latest fsm_event: %w", err)
	}
	return id, state, sensors, nil
}

// LastFaultDetectedID walks fsm_events back from the newest row. It has no index to use, so it
// reads every row since the last fault — at most the controller's retention, once, at the
// first start.
func (r *sourceReader) LastFaultDetectedID(ctx context.Context) (int64, error) {
	var id int64
	err := r.db.QueryRowContext(ctx,
		`SELECT id FROM fsm_events
		  WHERE event_kind = 'event_sent' AND event_type = 'FaultDetected'
		  ORDER BY id DESC LIMIT 1`).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("last FaultDetected: %w", err)
	}
	return id, nil
}

func (s *stateStore) AlarmTracker() (*alarmTracker, bool, error) {
	var doc string
	err := s.db.QueryRow(`SELECT state FROM alarm_tracker WHERE id = 1`).Scan(&doc)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read alarm tracker: %w", err)
	}
	a := &alarmTracker{}
	if err := json.Unmarshal([]byte(doc), a); err != nil {
		return nil, false, fmt.Errorf("decode alarm tracker: %w", err)
	}
	return a, true, nil
}

func (s *stateStore) SetAlarmTracker(a *alarmTracker) error {
	doc, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if _, err := s.db.Exec(
		`INSERT INTO alarm_tracker (id, state) VALUES (1, ?)
		 ON CONFLICT(id) DO UPDATE SET state = excluded.state`, string(doc)); err != nil {
		return fmt.Errorf("write alarm tracker: %w", err)
	}
	return nil
}
