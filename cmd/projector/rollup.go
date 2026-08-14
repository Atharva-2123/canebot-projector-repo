package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// cip_runs and the four rollup tables are produced by a pass over the replica's OWN finished
// tables, not by the tracker.
//
// That is deliberate. The tracker is a state machine over machine time and holds work open
// until it can be closed; threading hour-bucket counters through it would mean every counter
// carries the same "is it final yet" question the intervals already answer. By the time a row
// is in `cycles` or `state_durations` it is finished and immutable, so aggregating from there
// is a plain query over settled facts.
//
// Everything here obeys the same emit-once rule as the rest of the pipeline: a bucket is only
// written after the timeline has moved past its end, so a row is never revised. Rollup columns
// are sums and counts, never a stored average, which is what lets hours re-aggregate into days
// and weeks without recomputing from source.

const (
	hourMS = int64(time.Hour / time.Millisecond)
	dayMS  = 24 * hourMS

	// The firmware's own completion marker for the CIP cycle: Maintenance step 7 is labelled
	// "CIP cycle complete - reset maintenance timer and glass count" (fsm/step_metadata.go).
	// Same role as AutoCycle step 19 for a drink.
	cipCompleteStep = 7
)

// emitDerived writes everything that can be derived from already-flushed rows. upToMS is the
// furthest point on the timeline actually processed — never wall-clock now, which would sit
// ahead of the data and finalise buckets we have not filled yet.
// The two axes of the rollups table. Grain is how wide the bucket is; dim_kind is what the
// row is grouped by. A machine row has no dim_key — it IS the whole machine for that bucket.
const (
	grainHour = "hour"
	grainDay  = "day"

	dimMachine   = "machine"
	dimFaultType = "fault_type"
	dimStep      = "step"
	dimRecipe    = "recipe"
	dimSensor    = "sensor"
)

// stepDimKey identifies a step across states, since step 10 of AutoCycle and step 10 of
// Maintenance are different work. The lane rides along in its own column for display.
func stepDimKey(state string, step *int64) string {
	if step == nil {
		return state
	}
	return fmt.Sprintf("%s/%d", state, *step)
}

func (p *projector) emitDerived(ctx context.Context, upToMS int64) error {
	if upToMS <= 0 {
		return nil
	}
	// CIP runs first: hourly_rollups counts them.
	if err := p.emitCIPRuns(ctx, upToMS); err != nil {
		return err
	}
	if err := p.emitHourly(ctx, upToMS); err != nil {
		return err
	}
	return p.emitDaily(ctx, upToMS)
}

// ---------------------------------------------------------------------------
// cip_runs
// ---------------------------------------------------------------------------

// emitCIPRuns turns each closed Maintenance span into one CIP run.
//
// Maintenance IS the CIP cycle in this firmware — getMaintenanceStepMetadata describes the
// Maintenance steps as the CIP sequence — so a span in that state is a run, and reaching step 7
// within it is the firmware saying the run completed rather than being interrupted.
//
// This replaces a dashboard KPI whose value changes when you change the date range, because it
// counts time buckets containing any CIP row rather than actual runs.
func (p *projector) emitCIPRuns(ctx context.Context, upToMS int64) error {
	_, last, err := p.st.Cursor("cip_runs")
	if err != nil {
		return err
	}

	rows, err := p.rep.db.QueryContext(ctx,
		`SELECT entered_at_ms, exited_at_ms, order_id
		   FROM state_durations
		  WHERE state = 'Maintenance' AND exited_at_ms > ? AND exited_at_ms <= ?
		  ORDER BY exited_at_ms`, last, upToMS)
	if err != nil {
		return fmt.Errorf("read maintenance spans: %w", err)
	}
	type span struct {
		started, ended int64
		orderKey       string
	}
	var spans []span
	for rows.Next() {
		var s span
		if err := rows.Scan(&s.started, &s.ended, &s.orderKey); err != nil {
			rows.Close()
			return err
		}
		spans = append(spans, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	high := last
	for _, s := range spans {
		// Two places, because fsm_events is no longer replicated and neither source alone
		// covers every controller:
		//
		//   step_dwells        the step-run branch writes the dwell directly; on the base
		//                      branch the same dwell is derived from fsm_events before they
		//                      are discarded. Maintenance dwells are exempt from idle
		//                      suppression precisely so this keeps working.
		//   cycles             a controller that has fsm_step_runs but does not write step
		//                      runs for Maintenance leaves no dwell at all. The events still
		//                      pass through the tracker, so the interval records step 7 as
		//                      where it ended.
		//
		// Missing the marker downgrades a finished clean to "interrupted", which is why this
		// is worth checking twice rather than trusting one path.
		var completed int64
		if err := p.rep.db.QueryRowContext(ctx,
			`SELECT EXISTS(
			   SELECT 1 FROM step_dwells
			    WHERE state = 'Maintenance' AND step = ?
			      AND started_at_ms >= ? AND started_at_ms < ?
			   UNION ALL
			   SELECT 1 FROM cycles
			    WHERE result = 'maintenance' AND terminal_step = ?
			      AND started_at_ms >= ? AND started_at_ms < ?)`,
			cipCompleteStep, s.started, s.ended,
			cipCompleteStep, s.started, s.ended).Scan(&completed); err != nil {
			return fmt.Errorf("cip completion probe: %w", err)
		}

		var faultCount int64
		if err := p.rep.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM fault_events WHERE raised_at_ms >= ? AND raised_at_ms < ?`,
			s.started, s.ended).Scan(&faultCount); err != nil {
			return fmt.Errorf("cip fault count: %w", err)
		}

		ts, err := p.rep.guard("cip_runs", s.ended)
		if err != nil {
			return err
		}
		// No trigger_source column. It was always NULL — the controller records no distinction
		// between an automatic clean (the maintenance counter reaching MaintenanceCount) and one
		// an operator started. A column that can never hold a value is not a documented gap; it
		// is a gap hidden behind something that looks queryable.
		if _, err := p.rep.db.ExecContext(ctx,
			`INSERT INTO cip_runs (event_ts, started_at_ms, ended_at_ms, duration_ms, order_id,
			                       completed, fault_count, date_utc)
			 VALUES (?,?,?,?,?,?,?,?)`,
			ts, s.started, s.ended, s.ended-s.started, s.orderKey,
			completed, faultCount, dateUTC(s.started),
		); err != nil {
			return fmt.Errorf("insert cip_run: %w", err)
		}
		p.rep.st.SetWatermark("cip_runs", ts)
		high = s.ended
	}

	if high == last {
		return nil
	}
	return p.st.SetCursor("cip_runs", 0, high)
}

// ---------------------------------------------------------------------------
// Hourly
// ---------------------------------------------------------------------------

func (p *projector) emitHourly(ctx context.Context, upToMS int64) error {
	_, last, err := p.st.Cursor("hourly_rollups")
	if err != nil {
		return err
	}

	next := last + hourMS
	if last == 0 {
		earliest, ok, err := p.earliestBucket(ctx)
		if err != nil || !ok {
			return err
		}
		next = earliest
	}

	for b := next; b+hourMS <= upToMS; b += hourMS {
		if err := p.emitHourBucket(ctx, b); err != nil {
			return err
		}
		if err := p.st.SetCursor("hourly_rollups", 0, b); err != nil {
			return err
		}
	}
	return nil
}

// earliestBucket finds the first hour any finished row falls in, so a first run does not have
// to guess where history starts.
func (p *projector) earliestBucket(ctx context.Context) (int64, bool, error) {
	var v sql.NullInt64
	err := p.rep.db.QueryRowContext(ctx,
		`SELECT MIN(b) FROM (
		   SELECT MIN(started_at_ms) b FROM cycles
		   UNION ALL SELECT MIN(entered_at_ms) FROM state_durations
		   UNION ALL SELECT MIN(raised_at_ms)  FROM fault_events
		   UNION ALL SELECT MIN(started_at_ms) FROM step_dwells)`).Scan(&v)
	if err != nil {
		return 0, false, fmt.Errorf("earliest bucket: %w", err)
	}
	if !v.Valid {
		return 0, false, nil
	}
	return hourBucket(v.Int64), true, nil
}

func (p *projector) emitHourBucket(ctx context.Context, bucketStart int64) error {
	bucketEnd := bucketStart + hourMS

	agg, err := p.aggregateCycles(ctx, bucketStart, bucketEnd)
	if err != nil {
		return err
	}
	occupancy, err := p.stateOccupancy(ctx, bucketStart, bucketEnd)
	if err != nil {
		return err
	}

	var faultCount, cipCount int64
	if err := p.rep.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM fault_events WHERE raised_at_ms >= ? AND raised_at_ms < ?`,
		bucketStart, bucketEnd).
		Scan(&faultCount); err != nil {
		return err
	}
	if err := p.rep.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM cip_runs WHERE started_at_ms >= ? AND started_at_ms < ?`,
		bucketStart, bucketEnd).
		Scan(&cipCount); err != nil {
		return err
	}

	ts, err := p.rep.guard("hourly_rollups", bucketEnd)
	if err != nil {
		return err
	}
	// recipe_id is NULL: this is the machine-level row. The time columns have no recipe
	// dimension, so splitting the row per recipe would duplicate them and break the property
	// that run/error/maintenance/idle sum to the period. Per-recipe figures come from `cycles`,
	// which carries recipe_id and is replicated.
	if _, err := p.rep.db.ExecContext(ctx,
		`INSERT INTO rollups (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
		                      glasses, orders_started, orders_completed, orders_faulted,
		                      fault_count, cip_runs, cycle_ms_sum, cycle_count,
		                      run_ms, error_ms, maintenance_ms, idle_ms)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(grain, bucket_start_ms, dim_kind, dim_key) DO NOTHING`,
		ts, grainHour, bucketStart, dateUTC(bucketStart), dimMachine, nil,
		agg.glasses, agg.started, agg.completed, agg.faulted,
		faultCount, cipCount, agg.cycleMS, agg.cycleCount,
		occupancy.run, occupancy.errored, occupancy.maintenance, occupancy.idle,
	); err != nil {
		return fmt.Errorf("insert hourly_rollup: %w", err)
	}
	p.rep.st.SetWatermark("hourly_rollups", ts)

	if err := p.emitFaultTypeRollups(ctx, grainHour, bucketStart, bucketEnd, ts); err != nil {
		return err
	}
	return p.emitStepRollups(ctx, grainHour, bucketStart, bucketEnd, ts)
}

// emitFaultTypeRollups writes one row per fault type over [bucketStart, bucketEnd).
//
// Grouped by fault_type alone even though severity rides along: severity is a pure function of
// the fault type (see tracker.severity), so grouping by both would produce the same rows while
// making it possible for two of them to collide on the (grain, bucket, dim_kind, dim_key)
// unique key — where ON CONFLICT DO NOTHING would silently drop the second one's count.
func (p *projector) emitFaultTypeRollups(ctx context.Context, grain string, bucketStart, bucketEnd int64, ts string) error {
	// Read fully before writing. The replica is capped at one connection, so an INSERT issued
	// while this result set is still open waits for a connection that only closing it frees.
	type row struct {
		faultType, severity   string
		occurrences, downtime int64
	}
	var out []row

	rows, err := p.rep.db.QueryContext(ctx,
		`SELECT fault_type, MIN(severity), COUNT(*), COALESCE(SUM(downtime_ms), 0)
		   FROM fault_events
		  WHERE raised_at_ms >= ? AND raised_at_ms < ?
		  GROUP BY fault_type
		  ORDER BY fault_type`, bucketStart, bucketEnd)
	if err != nil {
		return fmt.Errorf("hourly fault counts: %w", err)
	}
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.faultType, &r.severity, &r.occurrences, &r.downtime); err != nil {
			rows.Close()
			return err
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, r := range out {
		if _, err := p.rep.db.ExecContext(ctx,
			`INSERT INTO rollups (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
			                      severity, occurrences, duration_ms_sum)
			 VALUES (?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(grain, bucket_start_ms, dim_kind, dim_key) DO NOTHING`,
			ts, grain, bucketStart, dateUTC(bucketStart), dimFaultType, r.faultType,
			r.severity, r.occurrences, r.downtime,
		); err != nil {
			return fmt.Errorf("insert %s fault_type rollup: %w", grain, err)
		}
	}
	if len(out) > 0 {
		p.rep.st.SetWatermark("hourly_fault_counts", ts)
	}
	return nil
}

// emitStepRollups writes one row per (state, step) over [bucketStart, bucketEnd).
func (p *projector) emitStepRollups(ctx context.Context, grain string, bucketStart, bucketEnd int64, ts string) error {
	// duration_ms_max is carried alongside the sum because an average hides the outlier that
	// is usually the thing worth looking at.
	// Same single-connection rule as emitHourlyFaultCounts: drain the query before inserting.
	type row struct {
		lane, state    string
		step           sql.NullInt64
		count, sum, ms int64
	}
	var out []row

	rows, err := p.rep.db.QueryContext(ctx,
		`SELECT lane, state, step, COUNT(*), COALESCE(SUM(duration_ms), 0), COALESCE(MAX(duration_ms), 0)
		   FROM step_dwells
		  WHERE started_at_ms >= ? AND started_at_ms < ?
		  GROUP BY lane, state, step
		  ORDER BY lane, state, step`, bucketStart, bucketEnd)
	if err != nil {
		return fmt.Errorf("hourly step stats: %w", err)
	}
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.lane, &r.state, &r.step, &r.count, &r.sum, &r.ms); err != nil {
			rows.Close()
			return err
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, r := range out {
		var stepPtr *int64
		if r.step.Valid {
			v := r.step.Int64
			stepPtr = &v
		}
		// No step_title column here: dim_key carries state/step, and the title is a pure
		// function of the two — the same reason it is derived rather than stored elsewhere.
		if _, err := p.rep.db.ExecContext(ctx,
			`INSERT INTO rollups (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
			                      lane, occurrences, duration_ms_sum, duration_ms_max)
			 VALUES (?,?,?,?,?,?,?,?,?,?)
			 ON CONFLICT(grain, bucket_start_ms, dim_kind, dim_key) DO NOTHING`,
			ts, grain, bucketStart, dateUTC(bucketStart), dimStep, stepDimKey(r.state, stepPtr),
			r.lane, r.count, r.sum, r.ms,
		); err != nil {
			return fmt.Errorf("insert %s step rollup: %w", grain, err)
		}
	}
	if len(out) > 0 {
		p.rep.st.SetWatermark("hourly_step_stats", ts)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Daily
// ---------------------------------------------------------------------------

func (p *projector) emitDaily(ctx context.Context, upToMS int64) error {
	_, last, err := p.st.Cursor("daily_rollups")
	if err != nil {
		return err
	}

	next := last + dayMS
	if last == 0 {
		earliest, ok, err := p.earliestBucket(ctx)
		if err != nil || !ok {
			return err
		}
		next = earliest - (earliest % dayMS)
	}

	for d := next; d+dayMS <= upToMS; d += dayMS {
		if err := p.emitDayBucket(ctx, d); err != nil {
			return err
		}
		if err := p.st.SetCursor("daily_rollups", 0, d); err != nil {
			return err
		}
	}
	return nil
}

func (p *projector) emitDayBucket(ctx context.Context, dayStart int64) error {
	dayEnd := dayStart + dayMS
	date := dateUTC(dayStart)

	agg, err := p.aggregateCycles(ctx, dayStart, dayEnd)
	if err != nil {
		return err
	}
	occupancy, err := p.stateOccupancy(ctx, dayStart, dayEnd)
	if err != nil {
		return err
	}

	var faultCount int64
	if err := p.rep.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM fault_events WHERE date_utc = ?`, date).Scan(&faultCount); err != nil {
		return err
	}

	ts, err := p.rep.guard("daily_rollups", dayEnd)
	if err != nil {
		return err
	}
	if _, err := p.rep.db.ExecContext(ctx,
		`INSERT INTO rollups (event_ts, grain, bucket_start_ms, date_utc, dim_kind, dim_key,
		                      glasses, orders_completed, orders_faulted, fault_count,
		                      cycle_ms_sum, cycle_count,
		                      run_ms, error_ms, maintenance_ms, idle_ms)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(grain, bucket_start_ms, dim_kind, dim_key) DO NOTHING`,
		ts, grainDay, dayStart, date, dimMachine, nil, agg.glasses,
		agg.completed, agg.faulted, faultCount,
		agg.cycleMS, agg.cycleCount,
		occupancy.run, occupancy.errored, occupancy.maintenance, occupancy.idle,
	); err != nil {
		return fmt.Errorf("insert daily_rollup: %w", err)
	}

	// The same two dimensions the hour grain carries, re-aggregated over the day.
	//
	// Not a convenience: a single interactive query is capped at 1,000 rows, so a 90-day range
	// of hourly fault rows (2,160 buckets before the fault-type fan-out) is rejected outright.
	// Without a day grain for these, the long windows in the dashboard's time filter have no
	// readable source at all.
	if err := p.emitFaultTypeRollups(ctx, grainDay, dayStart, dayEnd, ts); err != nil {
		return err
	}
	if err := p.emitStepRollups(ctx, grainDay, dayStart, dayEnd, ts); err != nil {
		return err
	}
	p.rep.st.SetWatermark("daily_rollups", ts)
	return nil
}

// ---------------------------------------------------------------------------
// Shared aggregation
// ---------------------------------------------------------------------------

type cycleAgg struct {
	glasses    int64
	started    int64
	completed  int64
	faulted    int64
	cycleMS    int64
	cycleCount int64
}

// aggregateCycles counts production cycles in one bucket. Synthetic intervals (idle,
// maintenance, manual, error) are excluded from every figure here — they are machine time,
// not orders, and their duration is accounted for by stateOccupancy instead.
// aggregateCycles sums the production cycles that STARTED inside a half-open window.
//
// A range on started_at_ms rather than an equality on a stored bucket key: the key was a
// denormalised copy of exactly this arithmetic, and a range over an indexed column is what the
// planner wants anyway. Attributing a cycle to the bucket it started in also means a drink
// spanning a boundary is counted once, in the bucket a person would look for it.
func (p *projector) aggregateCycles(ctx context.Context, startMS, endMS int64) (cycleAgg, error) {
	var a cycleAgg
	err := p.rep.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COALESCE(SUM(CASE WHEN result = ? THEN COALESCE(glass_count, 0) ELSE 0 END), 0),
		       COUNT(*),
		       COALESCE(SUM(result = ?), 0),
		       COALESCE(SUM(result IN (?, ?)), 0),
		       COALESCE(SUM(duration_ms), 0),
		       COUNT(*)
		  FROM cycles
		 WHERE is_production = 1 AND started_at_ms >= ? AND started_at_ms < ?`),
		resultCompleted, resultCompleted,
		resultFaultedRecoverable, resultFaultedNonRecoverable, startMS, endMS,
	).Scan(&a.glasses, &a.started, &a.completed, &a.faulted, &a.cycleMS, &a.cycleCount)
	if err != nil {
		return a, fmt.Errorf("aggregate cycles: %w", err)
	}
	return a, nil
}

type stateOccupancy struct {
	run         int64
	errored     int64
	maintenance int64
	idle        int64
}

// stateOccupancy sums how long the machine held each state within [bucketStart, bucketEnd),
// clipping spans to the bucket.
//
// The clipping is what makes the four columns sum to the period. Attributing a whole span to
// the hour it started in is cheaper, but a three-hour idle stretch would then put 3 hours into
// one bucket and nothing into the next two — and availability computed from that is wrong in
// both directions.
//
// Manual counts as idle. The four columns are meant to partition the period, and Manual is
// neither production, fault, nor cleaning — folding it in keeps the partition complete rather
// than leaving a silent hole that makes availability look better than it was.
func (p *projector) stateOccupancy(ctx context.Context, bucketStart, bucketEnd int64) (stateOccupancy, error) {
	var o stateOccupancy
	rows, err := p.rep.db.QueryContext(ctx,
		`SELECT state, entered_at_ms, exited_at_ms
		   FROM state_durations
		  WHERE exited_at_ms > ? AND entered_at_ms < ?`, bucketStart, bucketEnd)
	if err != nil {
		return o, fmt.Errorf("state occupancy: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var state string
		var entered, exited int64
		if err := rows.Scan(&state, &entered, &exited); err != nil {
			return o, err
		}
		ms := overlapMS(entered, exited, bucketStart, bucketEnd)
		if ms <= 0 {
			continue
		}
		switch state {
		case "AutoCycle":
			o.run += ms
		case "Error":
			o.errored += ms
		case "Maintenance":
			o.maintenance += ms
		default: // HomeIdle, Manual, and anything the firmware adds later
			o.idle += ms
		}
	}
	return o, rows.Err()
}

// overlapMS is the length of the intersection of [s,e) and [bs,be).
func overlapMS(s, e, bs, be int64) int64 {
	lo, hi := s, e
	if bs > lo {
		lo = bs
	}
	if be < hi {
		hi = be
	}
	if hi <= lo {
		return 0
	}
	return hi - lo
}
