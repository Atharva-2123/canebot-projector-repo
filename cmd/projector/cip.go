package main

import (
	"context"
	"database/sql"
	"fmt"
)

// CIP is independent of synthetic cycles/order IDs: toggles and late step runs can close an
// interval while the machine is still cleaning. Reconstruct the Maintenance episode, then
// split it only on a real completion followed by a new cleaning iteration.
//
// No schema migration or historical rewrite is needed. The existing cip_runs cursor remains
// the cutover boundary. last_src_id is 1 when that boundary is a completion inside an ongoing
// Maintenance episode, and 0 after an exit. Older projectors always wrote 0 here.
type cipEpisode struct {
	started, ended int64
	orderKey       string
	closed         bool
}

type cipStep struct {
	started  int64
	step     sql.NullInt64
	order    string
	fault    sql.NullString
	terminal bool // event-only firmware: the interval retained the completion marker
}

func (p *projector) emitCIPRuns(ctx context.Context, upToMS int64) error {
	phase, last, err := p.st.Cursor("cip_runs")
	if err != nil {
		return err
	}
	p.cipRollupMS = upToMS
	episodes, err := p.cipEpisodes(ctx, last, upToMS)
	if err != nil {
		return err
	}
	completeStep := p.cipCompletionStep
	if completeStep == 0 {
		completeStep = cipCompleteStep
	}
	high, highPhase := last, phase
	for _, episode := range episodes {
		if episode.closed && p.cipEvidenceBuffered(episode) {
			// A state transition can flush before the closing step run, even within one
			// settled batch. Do not classify that clean as interrupted or advance past it.
			p.cipRollupMS = min(p.cipRollupMS, episode.started)
			break
		}
		steps, err := p.cipSteps(ctx, episode.started, episode.ended, completeStep)
		if err != nil {
			return err
		}
		start, order := episode.started, episode.orderKey
		afterComplete := phase == 1 && episode.started == last
		pumpSeen := false
		for _, step := range steps {
			if !step.step.Valid {
				continue
			}
			s := step.step.Int64
			// A trailing/split completion dwell is not a new attempt. An automatic loop
			// starts another iteration at step 0 (or 1 if no step-0 dwell was recorded).
			if afterComplete {
				if s != 0 && s != 1 {
					continue
				}
				start, order = step.started, step.order
				afterComplete, pumpSeen = false, false
			}
			// Do not mistake a stale completion step at entry for a completed clean.
			// Both supported handlers pass through step 1 before their completion marker.
			if s == 1 {
				pumpSeen = true
			}
			if s != completeStep || (!pumpSeen && !step.terminal) || (step.fault.Valid && step.fault.String != "") {
				continue
			}
			if err := p.writeCIPRun(ctx, start, step.started, order, true); err != nil {
				return err
			}
			high, highPhase = step.started, 1
			afterComplete, pumpSeen = true, false
		}
		if episode.closed {
			if !afterComplete {
				if err := p.writeCIPRun(ctx, start, episode.ended, order, false); err != nil {
					return err
				}
			}
			high, highPhase = episode.ended, 0
		} else {
			// A partial interval or a shutdown is not an interrupted cleaning cycle.
			// Wait for the real exit, or the next completion, before finalising its count.
			pending := start
			if afterComplete {
				pending = high
			}
			if pending < p.cipRollupMS {
				p.cipRollupMS = pending
			}
		}
	}
	if high == last && highPhase == phase {
		return nil
	}
	return p.st.SetCursor("cip_runs", highPhase, high)
}

func (p *projector) cipEvidenceBuffered(episode cipEpisode) bool {
	if p.trk == nil {
		return false
	}
	pending := func(iv *interval) bool {
		if iv == nil {
			return false
		}
		for _, d := range iv.stepDwells {
			if isMaintenanceState(d.state) && d.startedAtMS <= episode.ended && d.endedAtMS >= episode.started {
				return true
			}
		}
		for _, s := range iv.stateDurations {
			if isMaintenanceState(s.state) && s.enteredAtMS < episode.ended && s.exitedAtMS > episode.started {
				return true
			}
		}
		return false
	}
	if pending(p.trk.current) {
		return true
	}
	for _, iv := range p.trk.closed {
		if pending(iv) {
			return true
		}
	}
	return false
}

// cipEpisodes reads state time, not interval IDs. A real non-Maintenance state closes an
// episode. A file flush, interval cap, missing sample, or projector restart does not.
func (p *projector) cipEpisodes(ctx context.Context, after, through int64) ([]cipEpisode, error) {
	rows, err := p.rep.db.QueryContext(ctx,
		`SELECT entered_at_ms, exited_at_ms, state, order_id FROM state_durations
		 WHERE exited_at_ms > ? AND entered_at_ms <= ? ORDER BY entered_at_ms, id`, after, through)
	if err != nil {
		return nil, fmt.Errorf("read CIP episodes: %w", err)
	}
	defer rows.Close()
	var episodes []cipEpisode
	var open *cipEpisode
	for rows.Next() {
		var entered, exited int64
		var state, order string
		if err := rows.Scan(&entered, &exited, &state, &order); err != nil {
			return nil, err
		}
		if isMaintenanceState(state) {
			if open == nil {
				open = &cipEpisode{started: max(entered, after), orderKey: order}
			}
			open.ended = max(open.ended, min(exited, through))
		} else if open != nil && entered >= open.started {
			open.ended, open.closed = entered, true
			episodes = append(episodes, *open)
			open = nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if open != nil {
		// The new state's first interval may still be buffered. Its state transition is
		// already known to the tracker, so there is no need to wait for another drink.
		if p.trk != nil && p.trk.openState != nil {
			state := p.trk.openState
			if !isMaintenanceState(state.state) && state.enteredMS >= open.ended && state.enteredMS <= through {
				open.ended, open.closed = state.enteredMS, true
			}
		}
		episodes = append(episodes, *open)
	}
	return episodes, nil
}

func (p *projector) cipSteps(ctx context.Context, start, end, completeStep int64) ([]cipStep, error) {
	rows, err := p.rep.db.QueryContext(ctx,
		`SELECT started_at_ms, step, order_id, fault_type FROM step_dwells
		 WHERE state LIKE 'Maintenance%' AND lane = 'main'
		   AND started_at_ms >= ? AND (started_at_ms < ? OR
		     (started_at_ms = ? AND ended_at_ms = ? AND step = ?))
		 ORDER BY started_at_ms, id`, start, end, end, end, completeStep)
	if err != nil {
		return nil, fmt.Errorf("read CIP steps: %w", err)
	}
	var steps []cipStep
	for rows.Next() {
		var s cipStep
		if err := rows.Scan(&s.started, &s.step, &s.order, &s.fault); err != nil {
			rows.Close()
			return nil, err
		}
		steps = append(steps, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		// Some older controllers expose fsm_step_runs but write no Maintenance rows.
		// Preserve their existing positive completion evidence from the event tracker.
		// Prefer actual dwells whenever present; cycle terminal steps are less precise.
		var at int64
		var order string
		err := p.rep.db.QueryRowContext(ctx,
			`SELECT ended_at_ms, order_id FROM cycles WHERE result = 'maintenance'
			 AND terminal_step = ? AND started_at_ms >= ? AND ended_at_ms <= ?
			 ORDER BY ended_at_ms, id LIMIT 1`, completeStep, start, end).Scan(&at, &order)
		if err != nil && err != sql.ErrNoRows {
			return nil, fmt.Errorf("CIP terminal completion: %w", err)
		}
		if err == nil {
			steps = append(steps, cipStep{started: at, step: sql.NullInt64{Int64: completeStep, Valid: true}, order: order, terminal: true})
		}
	}
	return steps, nil
}

func (p *projector) writeCIPRun(ctx context.Context, start, end int64, order string, completed bool) error {
	if end < start {
		return fmt.Errorf("CIP end %d precedes start %d", end, start)
	}
	var faults int64
	if err := p.rep.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM fault_events WHERE raised_at_ms >= ? AND raised_at_ms < ?`, start, end,
	).Scan(&faults); err != nil {
		return fmt.Errorf("CIP faults: %w", err)
	}
	ts, err := p.rep.guard("cip_runs", end)
	if err != nil {
		return err
	}
	// Replica and state are separate databases. If insertion succeeds but advancing the
	// cursor fails, replay must find this record rather than append it a second time.
	// A completion time is stable even if retention has removed its earliest fragment.
	result, err := p.rep.db.ExecContext(ctx,
		`INSERT INTO cip_runs (event_ts, started_at_ms, ended_at_ms, duration_ms, order_id,
		                       completed, fault_count, date_utc)
		 SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS (
		   SELECT 1 FROM cip_runs WHERE completed = ? AND ended_at_ms = ?
		     AND (? = 1 OR started_at_ms = ?))`,
		ts, start, end, end-start, order, boolToInt(completed), faults, dateUTC(start),
		boolToInt(completed), end, boolToInt(completed), start)
	if err != nil {
		return fmt.Errorf("insert logical CIP run: %w", err)
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n > 0 {
		p.rep.st.SetWatermark("cip_runs", ts)
	}
	return nil
}

// Buffering must not freeze the completion's hour/day at zero. Keep the existing interval
// buffering rules; only delay derived rollups while they could still miss CIP evidence.
func (p *projector) cipRollupThrough(through int64) int64 {
	if p.cipRollupMS > 0 && p.cipRollupMS < through {
		through = p.cipRollupMS
	}
	if p.trk == nil {
		return through
	}
	intervalStart := func(iv *interval) {
		if iv == nil {
			return
		}
		maintenance := iv.result == resultMaintenance
		for state := range iv.uniqueStates {
			maintenance = maintenance || isMaintenanceState(state)
		}
		for _, dwell := range iv.stepDwells {
			maintenance = maintenance || isMaintenanceState(dwell.state)
		}
		if maintenance && iv.startedMS < through {
			through = iv.startedMS
		}
	}
	intervalStart(p.trk.current)
	for _, iv := range p.trk.closed {
		intervalStart(iv)
	}
	return through
}
