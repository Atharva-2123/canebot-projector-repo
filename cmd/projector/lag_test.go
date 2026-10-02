package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// The firmware does not write its tables at the same moment. orders, fsm_step_runs and faults
// are inserted synchronously; fsm_events sit in a buffer and land in batches roughly every 10s
// (`[store] batch inserted N FSM events`). A projector ticking every 5s therefore sees one
// order's step runs well before the fsm_events that belong to the order before it.
//
// runOnce hides this: it applies everything in one pass, perfectly ordered. That is why the
// 29 Sep chained-order fix passed its test and still let 63 drinks read as failed in the field.
// These tests replay the source the way the projector really sees it — tick by tick, each row
// appearing only once the firmware would have written it.

const fsmEventFlushEvery = 10 * time.Second

// lagRow is one source row and the instant it becomes visible to the projector.
type lagRow struct {
	visibleAt time.Time
	seq       int // insertion order among rows visible at the same instant
	insert    func(db *sql.DB)
}

type lagSource struct {
	t    *testing.T
	db   *sql.DB
	rows []lagRow
}

func (s *lagSource) add(visibleAt time.Time, insert func(db *sql.DB)) {
	s.rows = append(s.rows, lagRow{visibleAt: visibleAt, seq: len(s.rows), insert: insert})
}

// flushedAt is when a buffered fsm_event written at `at` reaches the table.
func flushedAt(at time.Time) time.Time {
	return at.Truncate(fsmEventFlushEvery).Add(fsmEventFlushEvery)
}

func (s *lagSource) order(key string, recipe int, at time.Time) {
	s.add(at, func(db *sql.DB) { seedOrder(s.t, db, key, recipe, at) })
}

func (s *lagSource) stepRun(key string, step int, start, end time.Time, prev *int) {
	s.add(end, func(db *sql.DB) {
		seedStepRun(s.t, db, key, "AutoCycle", step, start, end, "AutoCycle", prev, "")
	})
}

func (s *lagSource) step(key string, from, to int, at time.Time) {
	s.add(flushedAt(at), func(db *sql.DB) { seedStep(s.t, db, key, "AutoCycle", from, to, at) })
}

func (s *lagSource) transition(key, from, to string, at time.Time) {
	s.add(flushedAt(at), func(db *sql.DB) { seedTransition(s.t, db, key, from, to, at) })
}

// chainedDrink seeds one drink the way the firmware logged ORD-d98c0c48d492 on 2 Oct: step 0
// consumed at `start`, steps 1→5→9 within milliseconds, then 10…18 over the drink, and step 19
// ending at the instant the next chained order is consumed. Returns that instant.
func (s *lagSource) chainedDrink(key string, recipe int, start time.Time, drink time.Duration) time.Time {
	s.order(key, recipe, start)
	steps := []int{0, 1, 5, 9, 10, 12, 13, 14, 15, 16, 18, autoCycleCompleteStep}
	at := start
	var prev *int
	for i, step := range steps {
		end := start.Add(drink * time.Duration(i+1) / time.Duration(len(steps)))
		if i < 3 {
			end = at.Add(3 * time.Millisecond) // 0, 1 and 5 pass in milliseconds
		}
		if step == autoCycleCompleteStep {
			end = at.Add(2 * time.Millisecond)
		}
		s.stepRun(key, step, at, end, prev)
		next := step
		if i+1 < len(steps) {
			next = steps[i+1]
			s.step(key, step, next, end)
		}
		p := step
		prev = &p
		at = end
	}
	return at.Add(time.Millisecond)
}

// replay inserts rows as they become visible and ticks the projector every `every`.
func (s *lagSource) replay(p *projector, from, to time.Time, every time.Duration) {
	sort.SliceStable(s.rows, func(i, j int) bool {
		if !s.rows[i].visibleAt.Equal(s.rows[j].visibleAt) {
			return s.rows[i].visibleAt.Before(s.rows[j].visibleAt)
		}
		return s.rows[i].seq < s.rows[j].seq
	})
	next := 0
	for now := from; !now.After(to); now = now.Add(every) {
		for next < len(s.rows) && !s.rows[next].visibleAt.After(now) {
			s.rows[next].insert(s.db)
			next++
		}
		p.now = func() time.Time { return now }
		if err := p.tick(context.Background()); err != nil {
			s.t.Fatalf("tick at %s: %v", now.Format(time.RFC3339Nano), err)
		}
	}
}

// newLiveProjector builds the projector the way runWith does, without its loop, so a test
// can drive tick() itself.
func newLiveProjector(t *testing.T, sourcePath string) (*projector, string) {
	t.Helper()
	dir := t.TempDir()
	replica := filepath.Join(dir, "canebot_replica.db")
	st, err := openState(filepath.Join(dir, "projector_state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	src, err := openSource(sourcePath, 2000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	rep, err := openReplica(replica, st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rep.Close() })

	p := &projector{src: src, rep: rep, st: st, trk: newTracker(severityFor), settle: defaultSettle}
	ctx := context.Background()
	if p.trk.useStepRuns, err = src.HasTable(ctx, "fsm_step_runs"); err != nil {
		t.Fatal(err)
	}
	if p.trk.useStepRuns {
		if src.stepRunFaults, err = src.HasColumn(ctx, "fsm_step_runs", "fault_type"); err != nil {
			t.Fatal(err)
		}
		p.trk.stepRunFaults = src.stepRunFaults
	}
	return p, replica
}

// The field failure, reproduced: four chained drinks (ORD-c9699b97af52 → … → ORD-7110cca39299
// on 2 Oct ran exactly like this). Every one was made; every chained one read `aborted`, 0 ms.
func TestChainedOrdersSurviveLateFSMEvents(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle time.Duration
	}{
		// The production configuration: the settle window removes the reordering.
		{"settled", defaultSettle},
		// The safety net alone: rows still arrive out of order, and none may close a drink
		// it does not belong to.
		{"unsettled", 0},
	} {
		t.Run(tc.name, func(t *testing.T) { chainedOrdersSurviveLateFSMEvents(t, tc.settle) })
	}
}

func chainedOrdersSurviveLateFSMEvents(t *testing.T, settle time.Duration) {
	sourcePath, db := newSourceDB(t)
	src := &lagSource{t: t, db: db}
	keys := []string{"ORD-lag00000000a", "ORD-lag00000000b", "ORD-lag00000000c", "ORD-lag00000000d"}

	start := base.Add(1234 * time.Millisecond) // off the flush grid, like the field
	src.transition("", "HomeIdle", "AutoCycle", start.Add(-2*time.Millisecond))
	for i, key := range keys {
		start = src.chainedDrink(key, i+1, start, 71*time.Second)
	}
	src.transition("", "AutoCycle", "HomeIdle", start.Add(time.Second))

	p, replica := newLiveProjector(t, sourcePath)
	p.settle = settle
	src.replay(p, base, start.Add(2*time.Minute), 5*time.Second)
	rep := openRO(t, replica)

	for _, key := range keys {
		var result, reason string
		var dur, events int64
		if err := rep.QueryRow(`SELECT result, duration_ms, fsm_event_count, close_reason
		                          FROM cycles WHERE order_id=?`, key).
			Scan(&result, &dur, &events, &reason); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if result != resultCompleted {
			t.Errorf("%s result = %q, want %q", key, result, resultCompleted)
		}
		if dur < 65_000 || dur > 75_000 {
			t.Errorf("%s duration_ms = %d, want ~71s", key, dur)
		}
		if events == 0 {
			t.Errorf("%s fsm_event_count = 0, want the drink's events", key)
		}
		if reason != closeStep19 {
			t.Errorf("%s close_reason = %q, want %q", key, reason, closeStep19)
		}
		// One continuous drink: dwell numbering must not restart part-way through.
		if n := scalarInt(t, rep, `SELECT COUNT(DISTINCT seq_index) - COUNT(*) FROM step_dwells
		                            WHERE order_id=? AND lane='main'`, key); n != 0 {
			t.Errorf("%s has %d duplicate seq_index values in step_dwells", key, -n)
		}
		if n := scalarInt(t, rep, `SELECT COUNT(*) FROM step_dwells WHERE order_id=? AND step=19`, key); n != 1 {
			t.Errorf("%s step 19 dwells = %d, want 1", key, n)
		}
	}
}

// A fault the firmware deferred to step 19 ("hopper empty fault: detected during order …
// raised after order complete") belongs to the drink it was detected in. ORD-90af3b139eca
// (2 Oct) reached step 19 at .114 and raised HopperEmptyFault at .117; whether it read
// `completed` or `recovered` used to depend on which row the projector happened to see first.
func TestDeferredStepNineteenFaultBelongsToTheDrink(t *testing.T) {
	for _, settle := range []time.Duration{defaultSettle, 0} {
		t.Run(settle.String(), func(t *testing.T) {
			sourcePath, db := newSourceDB(t)
			src := &lagSource{t: t, db: db}
			const key = "ORD-deferred0001"
			start := base.Add(4321 * time.Millisecond)
			src.transition("", "HomeIdle", "AutoCycle", start.Add(-2*time.Millisecond))
			end := src.chainedDrink(key, 1, start, 70*time.Second)
			faultAt := end.Add(2 * time.Millisecond)
			src.add(faultAt, func(db *sql.DB) { seedFault(t, db, key, "HopperEmptyFault", 19, faultAt) })
			src.transition(key, "AutoCycle", "Error", faultAt.Add(time.Millisecond))

			p, replica := newLiveProjector(t, sourcePath)
			p.settle = settle
			src.replay(p, base, end.Add(2*time.Minute), 5*time.Second)
			rep := openRO(t, replica)

			if got := scalarStr(t, rep, `SELECT result FROM cycles WHERE order_id=?`, key); got != resultFaultedRecoverable {
				t.Errorf("result = %q, want %q", got, resultFaultedRecoverable)
			}
			if n := scalarInt(t, rep, `SELECT fault_count FROM cycles WHERE order_id=?`, key); n != 1 {
				t.Errorf("fault_count = %d, want 1", n)
			}
			if got := scalarStr(t, rep, `SELECT close_reason FROM cycles WHERE order_id=?`, key); got != closeStep19 {
				t.Errorf("close_reason = %q, want %q (the drink was made)", got, closeStep19)
			}
			if d := scalarInt(t, rep, `SELECT duration_ms FROM cycles WHERE order_id=?`, key); d < 60_000 {
				t.Errorf("duration_ms = %d, want the whole drink", d)
			}
		})
	}
}
