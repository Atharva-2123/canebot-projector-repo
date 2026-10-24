package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newCIPFixture(t *testing.T) *projector {
	t.Helper()
	dir := t.TempDir()
	st, err := openState(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	rep, err := openReplica(filepath.Join(dir, "replica.db"), st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rep.Close() })
	return &projector{rep: rep, st: st}
}

func cipState(t *testing.T, p *projector, state string, start, end int64) string {
	t.Helper()
	order := fmt.Sprintf("%s-%d-%d", state, start, end)
	mustExec(t, p.rep.db, `INSERT INTO cycles
	 (event_ts,started_at_ms,ended_at_ms,duration_ms,order_id,is_production,result,date_utc)
	 VALUES (?,?,?,?,?,0,?,?)`, eventTS(end), start, end, end-start, order, resultForState(state), dateUTC(start))
	mustExec(t, p.rep.db, `INSERT INTO state_durations
	 (event_ts,entered_at_ms,exited_at_ms,duration_ms,order_id,state,is_downtime,is_production,date_utc,entry_reason,exit_reason)
	 VALUES (?,?,?,?,?,?,?,0,?,'interval_start','interval_end')`,
		eventTS(end), start, end, end-start, order, state, boolToInt(isMaintenanceState(state)), dateUTC(start))
	return order
}

func cipDwell(t *testing.T, p *projector, order, state string, step int, start, end int64) {
	t.Helper()
	mustExec(t, p.rep.db, `INSERT INTO step_dwells
	 (event_ts,started_at_ms,ended_at_ms,duration_ms,order_id,lane,state,step,seq_index,source_kind,is_production,date_utc)
	 VALUES (?,?,?,?,?,'main',?,?,0,'step_runs',0,?)`,
		eventTS(end), start, end, end-start, order, state, step, dateUTC(start))
}

func emitCIP(t *testing.T, p *projector, through int64) {
	t.Helper()
	if err := p.emitCIPRuns(context.Background(), through); err != nil {
		t.Fatal(err)
	}
}

func TestCIPThirtyOneFragmentsAreSixCompletedCleans(t *testing.T) {
	p := newCIPFixture(t)
	start := base.UnixMilli()
	fragments := []int{4, 1, 2, 6, 4, 6, 2, 6}
	for i, n := range fragments {
		at := start + int64(i)*4*hourMS
		var order string
		for f := 0; f < n; f++ {
			order = cipState(t, p, "Maintenance_regular", at+int64(f)*1000, at+int64(f+1)*1000)
		}
		if i != 1 && i != 2 {
			cipDwell(t, p, order, "Maintenance_regular", 0, at+10, at+100)
			cipDwell(t, p, order, "Maintenance_regular", 1, at+100, at+int64(n)*1000-8)
			cipDwell(t, p, order, "Maintenance_regular", 2, at+int64(n)*1000-8, at+int64(n)*1000-4)
			cipDwell(t, p, order, "Maintenance_regular", 3, at+int64(n)*1000-4, at+int64(n)*1000)
		}
		cipState(t, p, "HomeIdle", at+int64(n)*1000, at+4*hourMS)
	}
	emitCIP(t, p, start+32*hourMS)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM state_durations WHERE state LIKE 'Maintenance%'`); n != 31 {
		t.Fatalf("fixture has %d fragments, want 31", n)
	}
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 8 {
		t.Fatalf("CIP attempts = %d, want 8", n)
	}
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != 6 {
		t.Fatalf("completed cleans = %d, want 6", n)
	}
	if err := p.emitHourly(context.Background(), start+32*hourMS); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, p.rep.db, `SELECT SUM(cip_runs) FROM rollups WHERE grain='hour' AND dim_kind='machine'`); n != 6 {
		t.Fatalf("hourly completed cleans = %d, want 6", n)
	}
	// Re-running, including with a fresh projector object, is idempotent.
	emitCIP(t, &projector{rep: p.rep, st: p.st}, start+33*hourMS)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 8 {
		t.Fatalf("after restart/replay: %d records, want 8", n)
	}
}

func TestCIPAutomaticLoopCountsIterationsNotStateSpans(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_auto_cip_cmd3", s, s+10000)
	for i := int64(0); i < 2; i++ {
		cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 0, s+i*5000, s+i*5000+1000)
		cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 1, s+i*5000+1000, s+i*5000+4000)
		cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 3, s+i*5000+4000, s+i*5000+5000)
	}
	emitCIP(t, p, s+10000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs WHERE completed=1`); n != 2 {
		t.Fatalf("open loop completed iterations = %d, want 2", n)
	}
	// The completion dwell is split by an internal interval/restart. It is still one marker.
	order = cipState(t, p, "Maintenance_auto_cip_cmd3", s+10000, s+11000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 3, s+10000, s+11000)
	p = &projector{rep: p.rep, st: p.st}
	emitCIP(t, p, s+11000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 2 {
		t.Fatalf("split marker created another attempt: %d", n)
	}
	order = cipState(t, p, "Maintenance_auto_cip_cmd3", s+11000, s+15000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 0, s+11000, s+12000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 1, s+12000, s+14000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 3, s+14000, s+15000)
	cipState(t, p, "HomeIdle", s+15000, s+20000)
	emitCIP(t, p, s+20000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs WHERE completed=1`); n != 3 {
		t.Fatalf("loop completed iterations = %d, want 3", n)
	}
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs WHERE completed=0`); n != 0 {
		t.Fatalf("completion tail invented %d interrupted attempts", n)
	}
}

func TestCIPUnfinishedCleanSurvivesRestartAndMissingStateSamples(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_regular", s, s+1000)
	cipDwell(t, p, order, "Maintenance_regular", 0, s, s+500)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+500, s+1000)
	emitCIP(t, p, s+1000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 0 {
		t.Fatalf("an interval flush created %d CIP records", n)
	}
	// Projector absence leaves a gap in state samples, but no real state exit.
	order = cipState(t, p, "Maintenance_regular", s+2000, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 3, s+4000, s+5000)
	cipState(t, p, "HomeIdle", s+5000, s+6000)
	emitCIP(t, &projector{rep: p.rep, st: p.st}, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 1 {
		t.Fatalf("restart split one clean into %d records", n)
	}
	if n := scalarInt(t, p.rep.db, `SELECT started_at_ms FROM cip_runs`); n != s {
		t.Fatalf("start lost across restart: %d, want %d", n, s)
	}
}

func TestCIPCursorFailureDoesNotDuplicateInsertedRun(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_regular", s, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+1000, s+4000)
	cipDwell(t, p, order, "Maintenance_regular", 3, s+4000, s+5000)
	cipState(t, p, "HomeIdle", s+5000, s+6000)
	if err := p.st.SetCursor("cip_runs", 0, 0); err != nil {
		t.Fatal(err)
	}
	mustExec(t, p.st.db, `CREATE TRIGGER fail_cip_cursor BEFORE UPDATE ON source_cursors
	 WHEN NEW.source_table='cip_runs' BEGIN SELECT RAISE(ABORT,'simulated cursor commit failure'); END`)
	if err := p.emitCIPRuns(context.Background(), s+6000); err == nil {
		t.Fatal("expected simulated cursor commit failure")
	}
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 1 {
		t.Fatalf("expected record before cursor failure, got %d", n)
	}
	mustExec(t, p.st.db, `DROP TRIGGER fail_cip_cursor`)
	emitCIP(t, &projector{rep: p.rep, st: p.st}, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 1 {
		t.Fatalf("retry duplicated a committed run: %d", n)
	}
}

func TestCIPCurrentAndLegacyCompletionProfiles(t *testing.T) {
	for _, profile := range []int64{3, 7} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			p := newCIPFixture(t)
			p.cipCompletionStep = profile
			s := base.UnixMilli()
			order := cipState(t, p, "Maintenance_regular", s, s+10000)
			for step := 0; step <= 7; step++ {
				cipDwell(t, p, order, "Maintenance_regular", step, s+int64(step)*1000, s+int64(step+1)*1000)
			}
			cipState(t, p, "HomeIdle", s+10000, s+11000)
			emitCIP(t, p, s+11000)
			if n := scalarInt(t, p.rep.db, `SELECT ended_at_ms FROM cip_runs WHERE completed=1`); n != s+profile*1000 {
				t.Fatalf("completion at %d, want %d", n, s+profile*1000)
			}
		})
	}
	p := newCIPFixture(t)
	p.cipCompletionStep = 7
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_regular", s, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+1000, s+3000)
	cipDwell(t, p, order, "Maintenance_regular", 3, s+3000, s+5000)
	cipState(t, p, "HomeIdle", s+5000, s+6000)
	emitCIP(t, p, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != 0 {
		t.Fatal("legacy profile treated its intermediate step 3 as completion")
	}
}

func TestCIPFaultOnCompletionDwellIsNotSuccess(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_regular", s, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+1000, s+4000)
	cipDwell(t, p, order, "Maintenance_regular", 3, s+4000, s+5000)
	mustExec(t, p.rep.db, `UPDATE step_dwells SET fault_type='MachineCIPPumpFault' WHERE step=3`)
	cipState(t, p, "Error", s+5000, s+6000)
	emitCIP(t, p, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != 0 {
		t.Fatal("faulted completion dwell counted as success")
	}
}

func TestCIPCompletionAtMidnightIsCountedInNextHourAndDay(t *testing.T) {
	p := newCIPFixture(t)
	boundary := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC).UnixMilli()
	s := boundary - 10000
	order := cipState(t, p, "Maintenance_regular", s, boundary+1)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+1000, boundary)
	cipDwell(t, p, order, "Maintenance_regular", 3, boundary, boundary+1)
	cipState(t, p, "HomeIdle", boundary+1, boundary+dayMS)
	emitCIP(t, p, boundary+dayMS)
	ctx := context.Background()
	for _, hour := range []int64{boundary - hourMS, boundary} {
		if err := p.emitHourBucket(ctx, hour); err != nil {
			t.Fatal(err)
		}
	}
	for _, day := range []int64{boundary - dayMS, boundary} {
		if err := p.emitDayBucket(ctx, day); err != nil {
			t.Fatal(err)
		}
	}
	for _, grain := range []string{"hour", "day"} {
		if n := scalarInt(t, p.rep.db, `SELECT cip_runs FROM rollups WHERE grain=? AND dim_kind='machine' AND bucket_start_ms=?`, grain, boundary); n != 1 {
			t.Errorf("%s completion bucket = %d, want 1", grain, n)
		}
		if n := scalarInt(t, p.rep.db, `SELECT SUM(cip_runs) FROM rollups WHERE grain=? AND dim_kind='machine'`, grain); n != 1 {
			t.Errorf("%s sum = %d, want 1", grain, n)
		}
	}
}

func TestCIPCutoverDoesNotRewriteHistoryOrSchema(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	old := cipState(t, p, "Maintenance_regular", s, s+1000)
	if err := p.writeCIPRun(context.Background(), s, s+1000, old, false); err != nil {
		t.Fatal(err)
	}
	if err := p.st.SetCursor("cip_runs", 0, s+1000); err != nil {
		t.Fatal(err)
	}
	before := scalarStr(t, p.rep.db, `SELECT group_concat(sql,';') FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name`)
	cipState(t, p, "HomeIdle", s+1000, s+2000)
	order := cipState(t, p, "Maintenance_regular", s+2000, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+2100, s+4900)
	cipDwell(t, p, order, "Maintenance_regular", 3, s+4900, s+5000)
	cipState(t, p, "HomeIdle", s+5000, s+6000)
	emitCIP(t, p, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 2 {
		t.Fatalf("cutover has %d records, want existing + new", n)
	}
	if n := scalarInt(t, p.rep.db, `SELECT completed FROM cip_runs WHERE id=1`); n != 0 {
		t.Fatal("historical record changed")
	}
	after := scalarStr(t, p.rep.db, `SELECT group_concat(sql,';') FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name`)
	if before != after {
		t.Fatal("replica schema changed")
	}
}

func TestCIPWaitsForBufferedClosingStep(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_regular", s, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+1000, s+4000)
	cipState(t, p, "HomeIdle", s+5000, s+6000)
	step := int64(3)
	p.trk = newTracker(severityFor)
	p.trk.current = newInterval(order, false, s+4000, resultMaintenance)
	p.trk.current.stepDwells = []outStepDwell{{state: "Maintenance_regular", step: &step, startedAtMS: s + 4000, endedAtMS: s + 5000}}
	emitCIP(t, p, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 0 {
		t.Fatal("classified clean before its buffered completion was flushed")
	}
	p.trk.current = nil
	cipDwell(t, p, order, "Maintenance_regular", 3, s+4000, s+5000)
	emitCIP(t, p, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != 1 {
		t.Fatal("completion lost after buffered step was written")
	}
}

func TestCIPBufferedCompletionDelaysHourFinalisation(t *testing.T) {
	p := newCIPFixture(t)
	boundary := base.Truncate(time.Hour).Add(time.Hour).UnixMilli()
	s := boundary - 10000
	order := cipState(t, p, "Maintenance_regular", s, boundary-1)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+1000, boundary-2)
	cipState(t, p, "HomeIdle", boundary-1, boundary+60000)
	step := int64(3)
	p.trk = newTracker(severityFor)
	p.trk.current = newInterval(order, false, boundary-2, resultMaintenance)
	p.trk.current.stepDwells = []outStepDwell{{state: "Maintenance_regular", step: &step, startedAtMS: boundary - 2, endedAtMS: boundary - 1}}
	ctx := context.Background()
	if err := p.emitDerived(ctx, boundary+60000); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM rollups WHERE grain='hour' AND dim_kind='machine'`); n != 0 {
		t.Fatal("hour was frozen before its buffered completion was written")
	}
	p.trk.current = nil
	cipDwell(t, p, order, "Maintenance_regular", 3, boundary-2, boundary-1)
	if err := p.emitDerived(ctx, boundary+60000); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, p.rep.db, `SELECT cip_runs FROM rollups WHERE grain='hour' AND dim_kind='machine' AND bucket_start_ms=?`, boundary-hourMS); n != 1 {
		t.Fatalf("final hour has %d completions, want 1", n)
	}
}

func TestCIPLivePipelineUsesStepRunsDespiteIntervalFragmentation(t *testing.T) {
	path, db := newSourceDB(t)
	s := base.Add(time.Minute)
	end := s.Add(90 * time.Second)
	seedTransition(t, db, "", "HomeIdle", "Maintenance_regular", s)
	seedStepRun(t, db, "", "Maintenance_regular", 0, s, s.Add(10*time.Second), "HomeIdle", nil, "")
	seedStepRun(t, db, "", "Maintenance_regular", 1, s.Add(10*time.Second), end.Add(-8*time.Millisecond), "Maintenance_regular", nil, "")
	seedStepRun(t, db, "", "Maintenance_regular", 2, end.Add(-8*time.Millisecond), end.Add(-4*time.Millisecond), "Maintenance_regular", nil, "")
	seedStepRun(t, db, "", "Maintenance_regular", 3, end.Add(-4*time.Millisecond), end, "Maintenance_regular", nil, "")
	seedTransition(t, db, "", "Maintenance_regular", "HomeIdle", end)
	// Real inputs arriving during cleaning repeatedly change the synthetic interval.
	for _, at := range []time.Duration{time.Second, 11 * time.Second, 30 * time.Second, 89 * time.Second} {
		seedToggle(t, db, "", "X1.0", "fixture input", 0, 1, "HomeIdle", 0, s.Add(at))
	}
	seedTransition(t, db, "", "HomeIdle", "Manual", end.Add(time.Minute))
	p, _ := newLiveProjector(t, path)
	p.now = func() time.Time { return end.Add(3 * time.Minute) }
	if err := p.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.finalFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM state_durations WHERE state LIKE 'Maintenance%'`); n < 2 {
		t.Fatal("fixture did not reproduce fragmented Maintenance intervals")
	}
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 1 {
		t.Fatalf("one clean produced %d CIP records", n)
	}
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != 1 {
		t.Fatal("live step-3 completion was lost")
	}
}

func TestCIPRecordedSiteWindow(t *testing.T) {
	// A read-only export of 31 Maintenance fragments and their original step runs.
	// All replay writes go to the disposable fixture, never to the source of the export.
	var fixture struct {
		States []struct {
			Start int64  `json:"entered_at_ms"`
			End   int64  `json:"exited_at_ms"`
			State string `json:"state"`
			Order string `json:"order_id"`
		} `json:"states"`
		Steps []struct {
			Start int64   `json:"started_at_ms"`
			End   int64   `json:"ended_at_ms"`
			State string  `json:"state"`
			Step  int     `json:"step"`
			Order string  `json:"order_id"`
			Fault *string `json:"fault_type"`
		} `json:"steps"`
		Completed int64 `json:"expected_completed"`
		Attempts  int64 `json:"expected_attempts"`
	}
	b, err := os.ReadFile(filepath.Join("testdata", "cip-fragments-20261014.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	p := newCIPFixture(t)
	parent := func(order, state string, start, end int64) {
		mustExec(t, p.rep.db, `INSERT OR IGNORE INTO cycles
		 (event_ts,started_at_ms,ended_at_ms,duration_ms,order_id,is_production,result,date_utc)
		 VALUES (?,?,?,?,?,0,?,?)`, eventTS(end), start, end, end-start, order, resultForState(state), dateUTC(start))
	}
	var through int64
	for _, state := range fixture.States {
		parent(state.Order, state.State, state.Start, state.End)
		generated := cipState(t, p, state.State, state.Start, state.End)
		mustExec(t, p.rep.db, `UPDATE state_durations SET order_id=? WHERE order_id=?`, state.Order, generated)
		through = max(through, state.End)
	}
	for _, step := range fixture.Steps {
		parent(step.Order, step.State, step.Start, step.End)
		cipDwell(t, p, step.Order, step.State, step.Step, step.Start, step.End)
		if step.Fault != nil {
			mustExec(t, p.rep.db, `UPDATE step_dwells SET fault_type=? WHERE started_at_ms=? AND step=?`, *step.Fault, step.Start, step.Step)
		}
	}
	emitCIP(t, p, through)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != fixture.Attempts {
		t.Fatalf("recorded window: %d attempts, want %d", n, fixture.Attempts)
	}
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != fixture.Completed {
		t.Fatalf("recorded window: %d completions, want %d", n, fixture.Completed)
	}
	// Advance past the final completion's hour. The export keeps only each first exit
	// interval, so its final state record can end before that hour is closed.
	if err := p.emitHourly(context.Background(), through+hourMS); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, p.rep.db, `SELECT SUM(cip_runs) FROM rollups WHERE grain='hour' AND dim_kind='machine'`); n != fixture.Completed {
		t.Fatalf("recorded window: hourly count=%d, want %d", n, fixture.Completed)
	}
}

func TestCIPZeroLengthCompletionAtStateExit(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_regular", s, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 1, s+1000, s+5000)
	cipDwell(t, p, order, "Maintenance_regular", 3, s+5000, s+5000)
	cipState(t, p, "HomeIdle", s+5000, s+6000)
	emitCIP(t, p, s+6000)
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != 1 {
		t.Fatal("millisecond truncation lost the completion at state exit")
	}
}

func TestCIPRestartReopensSavedCompletionInsideLoop(t *testing.T) {
	p := newCIPFixture(t)
	s := base.UnixMilli()
	order := cipState(t, p, "Maintenance_auto_cip_cmd3", s, s+5000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 0, s, s+1000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 1, s+1000, s+4000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 3, s+4000, s+5000)
	emitCIP(t, p, s+5000)
	statePath := scalarStr(t, p.st.db, `SELECT file FROM pragma_database_list WHERE name='main'`)
	replicaPath := scalarStr(t, p.rep.db, `SELECT file FROM pragma_database_list WHERE name='main'`)
	if err := p.rep.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := openState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rep, err := openReplica(replicaPath, st)
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Close()
	p = &projector{rep: rep, st: st}
	emitCIP(t, p, s+5000)
	order = cipState(t, p, "Maintenance_auto_cip_cmd3", s+5000, s+10000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 0, s+5000, s+6000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 1, s+6000, s+9000)
	cipDwell(t, p, order, "Maintenance_auto_cip_cmd3", 3, s+9000, s+10000)
	cipState(t, p, "HomeIdle", s+10000, s+11000)
	emitCIP(t, p, s+11000)
	if n := scalarInt(t, p.rep.db, `SELECT COUNT(*) FROM cip_runs`); n != 2 {
		t.Fatalf("restart: got %d records, want two loop iterations", n)
	}
	if n := scalarInt(t, p.rep.db, `SELECT SUM(completed) FROM cip_runs`); n != 2 {
		t.Fatal("saved completion phase was lost across reopen")
	}
}

func TestCIPInvalidProfileFailsBeforeOpeningDatabases(t *testing.T) {
	if err := runWith(options{cipCompletionStep: 5, sourcePath: "/must/not/open/config.db"}); err == nil {
		t.Fatal("invalid completion profile accepted")
	}
}
