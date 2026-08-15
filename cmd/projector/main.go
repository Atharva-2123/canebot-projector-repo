// Command projector reads the CaneBot controller's SQLite database and projects it into a
// second, dashboard-shaped SQLite database for edge replication.
//
//	config.db ──(read-only)──▶ projector ──▶ canebot_replica.db ──▶ edge agent ──▶ cloud
//
// It never writes to the controller's database, and the controller's own code is untouched.
//
// The work it does is turning a firehose of individual events — "step changed", "sensor
// flipped" — into the facts a dashboard actually wants: one row per drink with its duration
// and outcome, one row per step with its duration, one row per fault. Roughly 400 raw rows
// per drink become about 21.
//
// Usage:
//
//	projector -source /path/to/config.db -replica ./canebot_replica.db -state ./projector_state.db
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	// Pure-Go SQLite, already a direct dependency of the controller. Registers the
	// "sqlite" driver; without it every database/sql call here fails at runtime.
	_ "modernc.org/sqlite"
)

const (
	defaultBatchSize = 500
	version          = "0.1.0"
)

// Fault severity. The firmware is the source of truth for this — fault_catalog.csv carries a
// FATAL/WARNING/UNEFFECTIVE level per code — but the frontend currently hardcodes the split in
// two TypeScript lists. Classifying here means the two can never drift.
const (
	severityRecoverable    = "recoverable"
	severityNonRecoverable = "non_recoverable"
)

// nonRecoverableFaults mirrors the frontend's NON_RECOVERABLE_FAULTS list. Kept explicit
// rather than derived so that a firmware change surfaces as a test failure.
var nonRecoverableFaults = map[string]bool{
	"CaneSinglingMotorFault":   true,
	"CrusherMotorFault":        true,
	"MachineCIPPumpFault":      true,
	"TilterMotorFault":         true,
	"MachineDoorBlowerFault":   true,
	"JuiceTrayLevelSensorMalf": true,
	"CupPresentSensorMalf":     true,
	"GlassFillingFault":        true,
}

func severityFor(faultType string) string {
	if nonRecoverableFaults[faultType] {
		return severityNonRecoverable
	}
	return severityRecoverable
}

var verbose bool

func logf(format string, args ...any) {
	if verbose {
		log.Printf(format, args...)
	}
}

func main() {
	var (
		sourcePath  = flag.String("source", "./config.db", "path to the controller's SQLite database (read-only)")
		replicaPath = flag.String("replica", "./canebot_replica.db", "path to the replica database the edge agent reads")
		statePath   = flag.String("state", "./projector_state.db", "path to the projector's own bookkeeping database")
		interval    = flag.Duration("interval", 5*time.Second, "polling interval")
		batchSize   = flag.Int("batch", defaultBatchSize, "max source rows read per table per tick")
		retention   = flag.Duration("retention", 14*24*time.Hour, "how long to keep rows in the replica")
		once        = flag.Bool("once", false, "run a single pass and exit (useful for tests and backfill)")
		verboseFlag = flag.Bool("v", false, "verbose logging")
	)
	flag.Parse()
	verbose = *verboseFlag

	if err := run(*sourcePath, *replicaPath, *statePath, *interval, *batchSize, *retention, *once); err != nil {
		log.Fatalf("projector: %v", err)
	}
}

func run(sourcePath, replicaPath, statePath string, interval time.Duration,
	batchSize int, retention time.Duration, once bool) error {

	if _, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("source database %s: %w", sourcePath, err)
	}

	st, err := openState(statePath)
	if err != nil {
		return err
	}
	defer st.Close()

	src, err := openSource(sourcePath, batchSize)
	if err != nil {
		return err
	}
	defer src.Close()

	rep, err := openReplica(replicaPath, st)
	if err != nil {
		return err
	}
	defer rep.Close()

	p := &projector{src: src, rep: rep, st: st, trk: newTracker(severityFor)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Detect which controller schema we are attached to rather than assuming. Three
	// branches are in flight — main, test/tracking_fsm, test/maintenance — and only
	// tracking_fsm carries fsm_step_runs. When it is present the firmware's own step
	// records are used; otherwise dwells are derived from fsm_events as before.
	hasStepRuns, err := src.HasTable(ctx, "fsm_step_runs")
	if err != nil {
		return err
	}
	p.trk.useStepRuns = hasStepRuns
	sourceBranch := "base"
	if hasStepRuns {
		sourceBranch = "step_runs"
	}

	// fault_type/fault_message on fsm_step_runs arrive with a firmware migration, so probe
	// for them separately. On a controller that records step runs but leaves `faults` empty
	// they are the only fault signal there is — see tracker.ApplyStepRun.
	if hasStepRuns {
		hasFaultCols, cErr := src.HasColumn(ctx, "fsm_step_runs", "fault_type")
		if cErr != nil {
			return cErr
		}
		src.stepRunFaults = hasFaultCols
		p.trk.stepRunFaults = hasFaultCols
		if hasFaultCols {
			sourceBranch = "step_runs+faults"
		}
	}

	log.Printf("projector: source schema = %s (fsm_step_runs %s)",
		sourceBranch, map[bool]string{true: "present", false: "absent"}[hasStepRuns])

	startedMS := time.Now().UnixMilli()

	// If we were away, say so explicitly rather than leaving a silent hole that downstream
	// would read as "the machine was idle".
	if last, err := st.LastAlive(); err == nil && last > 0 {
		if gap := startedMS - last; gap > int64(2*interval/time.Millisecond) {
			if err := rep.RecordGap(ctx, last, startedMS,
				"projector not running", version, sourceBranch); err != nil {
				logf("record gap: %v", err)
			}
		}
	}

	if once {
		if err := p.tick(ctx); err != nil {
			return err
		}
		// A single pass would otherwise leave the in-progress interval buffered in memory
		// and write nothing at all for a machine that never changed state.
		return p.finalFlush(ctx)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lastPrune := time.Now()

	for {
		select {
		case <-sig:
			log.Printf("projector: shutting down")
			// A final pass so work already visible in the source is not left behind,
			// then flush whatever interval is still open.
			if err := p.tick(ctx); err != nil {
				logf("final tick: %v", err)
			}
			if err := p.finalFlush(ctx); err != nil {
				logf("final flush: %v", err)
			}
			return nil

		case <-ticker.C:
			if err := p.tick(ctx); err != nil {
				// A failing tick must not kill the process: the source keeps producing,
				// and the cursor has not advanced, so the next tick retries the same rows.
				log.Printf("projector: tick failed (will retry): %v", err)
			}
			if retention > 0 && time.Since(lastPrune) > time.Hour {
				if err := rep.Prune(ctx, time.Now().Add(-retention)); err != nil {
					log.Printf("projector: prune failed: %v", err)
				}
				lastPrune = time.Now()
			}
		}
	}
}

// projector wires the three pieces together.
type projector struct {
	src *sourceReader
	rep *replicaWriter
	st  *stateStore
	trk *tracker

	// lastTimelineMS is the furthest point on the machine's timeline we have applied. Derived
	// tables are bounded by it rather than by wall-clock now, so a bucket is only finalised
	// once there is data through its end — otherwise a shutdown would stamp empty rollups on
	// every hour since the last event and report an idle machine where we simply have no data.
	lastTimelineMS int64
}

// Item kinds, ordered by the precedence they take when two rows share a timestamp. An order
// must open its interval before the events that belong to it are applied.
const (
	kindOrder = iota
	kindEvent
	kindStepRun
	kindFault
	kindToggle
)

// timelineItem is one source row, whatever table it came from.
type timelineItem struct {
	at    time.Time
	kind  int
	srcID int64

	order   srcOrder
	event   srcFSMEvent
	fault   srcFault
	toggle  srcSensorToggle
	stepRun srcStepRun
}

// tick is one pass: read a bounded batch from each source table, merge them into a single
// time-ordered stream, feed that through the tracker, and write whatever finished.
//
// The merge is the important part. The tracker is a state machine over machine time, so rows
// must reach it in the order they actually happened. Draining table by table would apply
// every order before any event, which tears the timeline apart — an order would close the
// previous cycle before that cycle's own events had been seen.
//
// Reads and writes are deliberately separated. Every source read closes its transaction
// before any derivation happens, so our locks on the controller's database are held for
// milliseconds rather than for the duration of the work.
func (p *projector) tick(ctx context.Context) error {
	items, safeUpTo, cursors, err := p.collect(ctx)
	if err != nil {
		return err
	}

	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].at.Equal(items[j].at) {
			return items[i].at.Before(items[j].at)
		}
		if items[i].kind != items[j].kind {
			return items[i].kind < items[j].kind
		}
		return items[i].srcID < items[j].srcID
	})

	// Cursors advance only as far as we actually apply, so anything deferred by the
	// safe-window rule is picked up on the next tick rather than skipped.
	applied := map[string]int64{}
	var latest time.Time
	for _, it := range items {
		if !safeUpTo.IsZero() && it.at.After(safeUpTo) {
			break
		}
		switch it.kind {
		case kindOrder:
			p.trk.ApplyOrder(it.order)
			applied["orders"] = it.srcID
		case kindEvent:
			p.trk.ApplyFSMEvent(it.event)
			applied["fsm_events"] = it.srcID
		case kindStepRun:
			var acts map[string]StepRunActuator
			if s := it.stepRun.ActuatorsJSON; s != "" && s != "{}" {
				if err := json.Unmarshal([]byte(s), &acts); err != nil {
					// A malformed blob costs us the actuator detail for one step, not the
					// step itself — and never the whole batch.
					logf("step run %d: actuators_json: %v", it.stepRun.ID, err)
				}
			}
			p.trk.ApplyStepRun(it.stepRun, acts)
			applied["fsm_step_runs"] = it.srcID
		case kindFault:
			p.trk.ApplyFault(it.fault)
			applied["faults"] = it.srcID
		case kindToggle:
			p.trk.ApplySensorToggle(it.toggle)
			applied["sensor_input_toggles"] = it.srcID
		}
		latest = it.at
	}

	// Config has no timestamps in the source at all, so changes are stamped at the latest
	// point on the timeline we have actually reached — never wall-clock now, which would sit
	// ahead of the stream and push event_ts backwards on the next tick.
	if !latest.IsZero() {
		if err := p.drainConfig(ctx, latest); err != nil {
			return err
		}
	}

	for table, id := range applied {
		if id > cursors[table] {
			if err := p.st.SetCursor(table, id, 0); err != nil {
				return err
			}
		}
	}

	if err := p.flushIntervals(ctx); err != nil {
		return err
	}
	// Actuator intervals come last: they attach to a cycle, so the parent should exist.
	if err := p.drainActuators(ctx); err != nil {
		return err
	}
	// Then link each pulse to the dwell it fell inside. Deliberately after both are written:
	// pulses and dwells arrive on separate cursors, so neither can be attributed to the other
	// at the moment it lands.
	if err := p.rep.AttributeActuatorPulses(ctx); err != nil {
		return err
	}
	// Derived tables aggregate rows the steps above have already written, so they run after
	// the flush and are bounded by the timeline rather than wall-clock now — a bucket is only
	// finalised once we have data past its end.
	if !latest.IsZero() {
		p.lastTimelineMS = latest.UnixMilli()
		if err := p.emitDerived(ctx, p.lastTimelineMS); err != nil {
			return err
		}
	}
	// Heartbeat last, so it only advances on a tick that fully succeeded. A failed tick
	// leaves it behind and the stretch is honestly reported as a gap.
	p.st.MarkAlive(time.Now().UnixMilli())
	return nil
}

// collect reads one bounded batch from each event-ish source table.
//
// safeUpTo guards the merge. If a table filled its batch there are more of its rows waiting,
// and applying another table's later rows first would again reorder the timeline. So when any
// table comes back full, the stream is only applied up to the earliest of those tables' last
// timestamps; the rest waits for the next tick.
func (p *projector) collect(ctx context.Context) (items []timelineItem, safeUpTo time.Time, cursors map[string]int64, err error) {
	cursors = map[string]int64{}
	for _, t := range []string{"orders", "fsm_events", "faults", "sensor_input_toggles"} {
		id, _, cErr := p.st.Cursor(t)
		if cErr != nil {
			return nil, time.Time{}, nil, cErr
		}
		cursors[t] = id
	}

	noteFull := func(full bool, last time.Time) {
		if !full {
			return
		}
		if safeUpTo.IsZero() || last.Before(safeUpTo) {
			safeUpTo = last
		}
	}

	orders, err := p.src.OrdersAfter(ctx, cursors["orders"])
	if err != nil {
		return nil, time.Time{}, nil, err
	}
	for _, o := range orders {
		items = append(items, timelineItem{at: o.TS, kind: kindOrder, srcID: o.ID, order: o})
	}
	if n := len(orders); n > 0 {
		noteFull(n >= p.src.batchSize, orders[n-1].TS)
	}

	events, err := p.src.FSMEventsAfter(ctx, cursors["fsm_events"])
	if err != nil {
		return nil, time.Time{}, nil, err
	}
	for _, e := range events {
		items = append(items, timelineItem{at: e.TS, kind: kindEvent, srcID: e.ID, event: e})
	}
	if n := len(events); n > 0 {
		noteFull(n >= p.src.batchSize, events[n-1].TS)
	}

	faults, err := p.src.FaultsAfter(ctx, cursors["faults"])
	if err != nil {
		return nil, time.Time{}, nil, err
	}
	for _, f := range faults {
		items = append(items, timelineItem{at: f.TS, kind: kindFault, srcID: f.ID, fault: f})
	}
	if n := len(faults); n > 0 {
		noteFull(n >= p.src.batchSize, faults[n-1].TS)
	}

	toggles, err := p.src.SensorTogglesAfter(ctx, cursors["sensor_input_toggles"])
	if err != nil {
		return nil, time.Time{}, nil, err
	}
	for _, s := range toggles {
		items = append(items, timelineItem{at: s.TS, kind: kindToggle, srcID: s.ID, toggle: s})
	}
	if n := len(toggles); n > 0 {
		noteFull(n >= p.src.batchSize, toggles[n-1].TS)
	}

	if p.trk.useStepRuns {
		id, _, cErr := p.st.Cursor("fsm_step_runs")
		if cErr != nil {
			return nil, time.Time{}, nil, cErr
		}
		cursors["fsm_step_runs"] = id
		runs, rErr := p.src.StepRunsAfter(ctx, id)
		if rErr != nil {
			return nil, time.Time{}, nil, rErr
		}
		for _, sr := range runs {
			// Placed on the timeline at its END, because that is when the step became a
			// fact — and it keeps emission in the same order the rows are written.
			items = append(items, timelineItem{at: sr.Ended, kind: kindStepRun, srcID: sr.ID, stepRun: sr})
		}
		if n := len(runs); n > 0 {
			noteFull(n >= p.src.batchSize, runs[n-1].Ended)
		}
	}

	return items, safeUpTo, cursors, nil
}

func (p *projector) drainConfig(ctx context.Context, at time.Time) error {
	current, err := p.src.Config(ctx)
	if err != nil {
		return err
	}
	previous, err := p.st.ConfigSnapshot()
	if err != nil {
		return err
	}
	changed := false
	for k, v := range current {
		old, existed := previous[k]
		if existed && old == v {
			continue
		}
		// A first sighting is a baseline, not a change: recording it silently means the
		// next real edit produces exactly one row instead of a spurious pair.
		if existed {
			p.trk.ApplyConfigChange(k, old, v, at)
		}
		changed = true
	}
	if !changed {
		return nil
	}
	return p.st.SetConfigSnapshot(current)
}

// drainActuators ships closed actuator intervals. This table is the one mutable table in the
// source — a row is opened on ON and updated on OFF — so it is watermarked on close time
// rather than on id, and only closed rows are emitted.
func (p *projector) drainActuators(ctx context.Context) error {
	_, lastClosed, err := p.st.Cursor("actuator_output_intervals")
	if err != nil {
		return err
	}
	rows, err := p.src.ClosedActuatorIntervalsAfter(ctx, lastClosed)
	if err != nil || len(rows) == 0 {
		return err
	}

	high := lastClosed
	for _, a := range rows {
		orderKey := a.OrderKey
		if orderKey == "" {
			// Out-of-cycle actuator activity still needs a parent; without one it would be
			// invisible to an order-scoped frontend.
			continue
		}
		cc, ok, err := p.rep.CycleContext(ctx, orderKey)
		if err != nil {
			return err
		}
		if !ok {
			// Parent not written yet — leave the watermark behind it so this row is
			// retried on a later tick rather than lost.
			break
		}

		duration := int64(0)
		if a.DurationMS != nil && *a.DurationMS > 0 {
			duration = *a.DurationMS
		} else {
			duration = a.Ended.Sub(a.Started).Milliseconds()
		}
		if duration <= 0 {
			// Deliberately skipped rather than written as zero: the current dashboard
			// COALESCEs a missing duration to 0 seconds, which reads as real data.
			logf("skipping actuator interval src=%d: non-positive duration", a.ID)
			high = a.Ended.UnixMilli()
			continue
		}

		out := outActuatorInterval{
			startedAtMS:   a.Started.UnixMilli(),
			endedAtMS:     a.Ended.UnixMilli(),
			durationMS:    duration,
			orderKey:      orderKey,
			srcID:         a.ID,
			outputID:      a.OutputID,
			outputName:    a.OutputName,
			startedState:  a.StartedState,
			startedStep:   a.StartedStep,
			startedSendOK: a.StartedSendOK,
			endedState:    a.EndedState,
			endedStep:     a.EndedStep,
			endedSendOK:   a.EndedSendOK,
			faultType:     a.FaultType,
			faultMessage:  a.FaultMessage,
		}
		if a.FaultRaised != nil {
			ms := a.FaultRaised.UnixMilli()
			out.faultRaisedAtMS = &ms
		}
		if a.FaultCleared != nil {
			ms := a.FaultCleared.UnixMilli()
			out.faultClearedAtMS = &ms
		}
		if err := p.rep.WriteActuatorInterval(ctx, out, cc); err != nil {
			return err
		}
		high = a.Ended.UnixMilli()
	}
	if high == lastClosed {
		return nil
	}
	return p.st.SetCursor("actuator_output_intervals", 0, high)
}

// finalFlush closes the in-progress interval and writes it. Used at shutdown and at the end
// of a -once run.
//
// The interval is closed on the machine's timeline, not at wall-clock now — the same rule
// emitDerived already follows, and for a sharper reason.
//
// During a backfill the newest source row can be weeks old. Closing the open interval at "now"
// stamps it with a wall-clock event_ts, which raises the cycles watermark to now; every interval
// written after that is earlier, so the monotonic guard clamps it forward. One -once run over
// historical data is enough to collapse the entire history onto the instant the projector ran,
// and because the watermark is persisted, it never recovers. Every dashboard panel filters on
// event_ts, so the effect is a month of production appearing as a single spike today.
//
// The cost is that a genuinely idle stretch between the last source event and shutdown is not
// counted as idle time. That is the trade already accepted for the rollups, and `gaps` reports
// the uncovered period explicitly rather than letting it read as machine idleness.
func (p *projector) finalFlush(ctx context.Context) error {
	closeAt := p.lastTimelineMS
	if now := time.Now().UnixMilli(); closeAt == 0 || closeAt > now {
		// No timeline yet (nothing has been applied), or a source clock ahead of ours.
		closeAt = now
	}
	p.trk.FlushOpen(closeAt)
	if err := p.flushIntervals(ctx); err != nil {
		return err
	}
	// Derived tables run again here because the interval closed just above is the one holding
	// the newest state spans and dwells. Without this a -once run — which is how backfill and
	// every test invoke the projector — would emit rollups from everything except its own last
	// interval.
	return p.emitDerived(ctx, p.lastTimelineMS)
}

func (p *projector) flushIntervals(ctx context.Context) error {
	for _, iv := range p.trk.PendingIntervals() {
		if err := p.rep.FlushInterval(ctx, iv); err != nil {
			return err
		}
		logf("wrote interval %s (%s) %dms, %d events, %d dwells",
			iv.orderKey, iv.result, iv.endedMS-iv.startedMS, len(iv.events), len(iv.stepDwells))
	}
	return nil
}
