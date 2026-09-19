package main

import (
	"context"
	"database/sql"
	"fmt"
)

// fault_incidents is derived from the replica's finished rows, like cip_runs and rollups, not
// held open in the tracker. An incident can last hours; an in-memory one would be lost to any
// projector restart inside it, while one derived from settled rows is simply found again.
//
// The cursor is the raised_at_ms of the last fault already folded into an emitted incident.
// An incident is only written once its recovery is visible, so every row is final when it
// ships — the same emit-once rule as the rest of the replica.

type incidentFault struct {
	raisedMS  int64
	orderKey  sql.NullString
	faultKey  string
	faultType string
	severity  string
}

func (p *projector) emitFaultIncidents(ctx context.Context) error {
	_, cursor, err := p.st.Cursor("fault_incidents")
	if err != nil {
		return err
	}
	for {
		first, ok, err := p.firstFaultAfter(ctx, cursor)
		if err != nil || !ok {
			return err
		}
		recoveredMS, via, ok, err := p.recoveryAfter(ctx, first.raisedMS)
		if err != nil || !ok {
			// Still down, or recovered but not yet flushed. Try again next tick.
			return err
		}
		lastRaised, err := p.writeIncident(ctx, first, recoveredMS, via)
		if err != nil {
			return err
		}
		cursor = lastRaised
		if err := p.st.SetCursor("fault_incidents", 0, cursor); err != nil {
			return err
		}
	}
}

func (p *projector) firstFaultAfter(ctx context.Context, afterMS int64) (incidentFault, bool, error) {
	var f incidentFault
	err := p.rep.db.QueryRowContext(ctx,
		`SELECT raised_at_ms, order_id, fault_key, fault_type, severity
		   FROM fault_events WHERE raised_at_ms > ?
		  ORDER BY raised_at_ms, id LIMIT 1`, afterMS,
	).Scan(&f.raisedMS, &f.orderKey, &f.faultKey, &f.faultType, &f.severity)
	if err == sql.ErrNoRows {
		return f, false, nil
	}
	if err != nil {
		return f, false, fmt.Errorf("next unresolved fault: %w", err)
	}
	return f, true, nil
}

// recoveryAfter is the first moment after raisedMS the machine was back at normal work.
//
// HomeIdle alone is not enough: with orders queued, a resumed drink runs through step 19
// straight into the next order and the machine never idles. Step 19 alone is not enough
// either: a fault cleared with nothing queued goes to HomeIdle and makes no drink.
// Maintenance on the way back (08:29 on 19 Sep) is part of the incident, not the end of it.
func (p *projector) recoveryAfter(ctx context.Context, raisedMS int64) (int64, string, bool, error) {
	var idle, cycle sql.NullInt64
	if err := p.rep.db.QueryRowContext(ctx,
		`SELECT MIN(entered_at_ms) FROM state_durations
		  WHERE state = ? AND entered_at_ms > ?`, stateHomeIdle, raisedMS,
	).Scan(&idle); err != nil {
		return 0, "", false, fmt.Errorf("incident recovery (idle): %w", err)
	}
	// event_ts is a dwell's end, never before its start, so it bounds the scan through the
	// index; step_dwells is by far the largest table here.
	if err := p.rep.db.QueryRowContext(ctx,
		`SELECT MIN(started_at_ms) FROM step_dwells
		  WHERE event_ts >= ? AND state = 'AutoCycle' AND step = ? AND started_at_ms > ?`,
		eventTS(raisedMS), autoCycleCompleteStep, raisedMS,
	).Scan(&cycle); err != nil {
		return 0, "", false, fmt.Errorf("incident recovery (cycle): %w", err)
	}
	switch {
	case idle.Valid && (!cycle.Valid || idle.Int64 <= cycle.Int64):
		return idle.Int64, "home_idle", true, nil
	case cycle.Valid:
		return cycle.Int64, "cycle_complete", true, nil
	}
	return 0, "", false, nil
}

// writeIncident writes the incident [first.raisedMS, recoveredMS) and returns the raised time
// of its last fault, which becomes the cursor.
func (p *projector) writeIncident(ctx context.Context, first incidentFault, recoveredMS int64, via string) (int64, error) {
	from, to := first.raisedMS, recoveredMS
	db := p.rep.db

	var faultCount, lastRaised int64
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), MAX(raised_at_ms) FROM fault_events
		  WHERE raised_at_ms >= ? AND raised_at_ms < ?`, from, to,
	).Scan(&faultCount, &lastRaised); err != nil {
		return 0, fmt.Errorf("incident faults: %w", err)
	}

	var resetCount int64
	var firstReset sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), MIN(event_at_ms) FROM sensor_toggles
		  WHERE input_id = 'X0.7' AND value_from = 0 AND value_to = 1
		    AND event_at_ms >= ? AND event_at_ms < ?`, from, to,
	).Scan(&resetCount, &firstReset); err != nil {
		return 0, fmt.Errorf("incident resets: %w", err)
	}

	var doorCount, doorMS int64
	var firstDoor sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(duration_ms), 0), MIN(opened_at_ms) FROM door_events
		  WHERE opened_at_ms >= ? AND opened_at_ms < ?`, from, to,
	).Scan(&doorCount, &doorMS, &firstDoor); err != nil {
		return 0, fmt.Errorf("incident doors: %w", err)
	}

	var errMS, manualMS, maintMS, runMS int64
	rows, err := db.QueryContext(ctx,
		`SELECT state, entered_at_ms, exited_at_ms FROM state_durations
		  WHERE exited_at_ms > ? AND entered_at_ms < ?`, from, to)
	if err != nil {
		return 0, fmt.Errorf("incident states: %w", err)
	}
	for rows.Next() {
		var state string
		var entered, exited int64
		if err := rows.Scan(&state, &entered, &exited); err != nil {
			rows.Close()
			return 0, err
		}
		ms := overlapMS(entered, exited, from, to)
		switch {
		case state == "Error":
			errMS += ms
		case state == "Manual":
			manualMS += ms
		case isMaintenanceState(state):
			maintMS += ms
		case state == "AutoCycle":
			runMS += ms
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	ts, err := p.rep.guard("fault_incidents", recoveredMS)
	if err != nil {
		return 0, err
	}
	// DO NOTHING on the first fault's key: a replayed tick finds the incident already written
	// and moves on, rather than failing forever on the unique constraint.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO fault_incidents (event_ts, raised_at_ms, recovered_at_ms, downtime_ms,
		                              recovered_via, order_id, fault_key, fault_type, severity,
		                              fault_count, last_raised_at_ms,
		                              first_reset_at_ms, reset_count,
		                              first_door_open_at_ms, door_open_count, door_open_ms,
		                              error_ms, manual_ms, maintenance_ms, run_ms, date_utc)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(fault_key) DO NOTHING`,
		ts, from, to, to-from, via, first.orderKey, first.faultKey, first.faultType, first.severity,
		faultCount, lastRaised, firstReset, resetCount, firstDoor, doorCount, doorMS,
		errMS, manualMS, maintMS, runMS, dateUTC(from),
	); err != nil {
		return 0, fmt.Errorf("insert fault_incident: %w", err)
	}
	p.rep.st.SetWatermark("fault_incidents", ts)
	return lastRaised, nil
}
