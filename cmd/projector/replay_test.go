package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestReplayFieldSource replays a real controller database the way the projector saw it live:
// rows appear only when the firmware would have written them, and the projector ticks every 5s.
// Opt-in, since it needs an extract of a field config.db:
//
//	REPLAY_SOURCE=/path/source.db [REPLAY_FLUSH_PHASE=7.5s] [REPLAY_TICK_PHASE=2s] \
//	  [REPLAY_SETTLE=30s] [REPLAY_OUT=cycles.tsv] go test -run TestReplayFieldSource -v
//
// Visibility: orders, faults, toggles at their ts; step runs at their end; actuator intervals
// at their close; fsm_events at the firmware's next 10s flush (FSMEventFlushInterval, a ticker
// whose phase is fixed at controller start — read it off the `[store] batch inserted` lines).
func TestReplayFieldSource(t *testing.T) {
	srcPath := os.Getenv("REPLAY_SOURCE")
	if srcPath == "" {
		t.Skip("REPLAY_SOURCE not set")
	}
	flushPhase := envDuration(t, "REPLAY_FLUSH_PHASE", 7500*time.Millisecond)
	tickPhase := envDuration(t, "REPLAY_TICK_PHASE", 0)
	settle := envDuration(t, "REPLAY_SETTLE", defaultSettle)

	real, err := sql.Open("sqlite", "file:"+srcPath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer real.Close()

	// An empty source with the field schema, filled tick by tick.
	livePath := filepath.Join(t.TempDir(), "config.db")
	live, err := sql.Open("sqlite", "file:"+livePath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	live.SetMaxOpenConns(1)
	ddl, err := real.Query(`SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL
	                         AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	var stmts []string
	for ddl.Next() {
		var s string
		ddl.Scan(&s)
		stmts = append(stmts, s)
	}
	ddl.Close()
	for _, s := range stmts {
		mustExec(t, live, s)
	}
	mustExec(t, live, `ATTACH DATABASE ? AS r`, "file:"+srcPath+"?mode=ro")

	flushAt := func(ts time.Time) time.Time {
		// The first flush tick strictly after ts.
		k := ts.Add(-flushPhase).Truncate(10 * time.Second).Add(10 * time.Second)
		return k.Add(flushPhase)
	}
	tables := []struct {
		name, tsCol string
		visible     func(time.Time) time.Time
	}{
		{"orders", "ts_utc", nil},
		{"faults", "ts_utc", nil},
		{"sensor_input_toggles", "ts_utc", nil},
		{"fsm_step_runs", "step_ended_ts_utc", nil},
		{"actuator_output_intervals", "ended_ts_utc", nil},
		{"fsm_events", "ts_utc", flushAt},
	}
	type vis struct {
		id int64
		at time.Time
	}
	pending := map[string][]vis{}
	var first, last time.Time
	for _, tb := range tables {
		rows, err := real.Query(fmt.Sprintf(`SELECT id, %s FROM %s WHERE %s IS NOT NULL`, tb.tsCol, tb.name, tb.tsCol))
		if err != nil {
			t.Fatal(err)
		}
		var vs []vis
		for rows.Next() {
			var id int64
			var raw string
			rows.Scan(&id, &raw)
			ts, err := parseSourceTime(raw)
			if err != nil {
				t.Fatalf("%s %d: %v", tb.name, id, err)
			}
			at := ts
			if tb.visible != nil {
				at = tb.visible(ts)
			}
			vs = append(vs, vis{id, at})
			if first.IsZero() || ts.Before(first) {
				first = ts
			}
			if at.After(last) {
				last = at
			}
		}
		rows.Close()
		sort.Slice(vs, func(i, j int) bool { return vs[i].at.Before(vs[j].at) })
		pending[tb.name] = vs
	}
	mustExec(t, live, `INSERT INTO config SELECT * FROM r.config`)

	p, replica := newLiveProjector(t, livePath)
	p.settle = settle
	start := first.Truncate(5 * time.Second).Add(tickPhase)
	end := last.Add(settle + 10*time.Second)
	for now := start; !now.After(end); now = now.Add(5 * time.Second) {
		for _, tb := range tables {
			vs := pending[tb.name]
			n := 0
			for n < len(vs) && !vs[n].at.After(now) {
				n++
			}
			if n == 0 {
				continue
			}
			ids := make([]string, n)
			for i := range vs[:n] {
				ids[i] = strconv.FormatInt(vs[i].id, 10)
			}
			mustExec(t, live, fmt.Sprintf(`INSERT INTO %s SELECT * FROM r.%s WHERE id IN (%s)`,
				tb.name, tb.name, strings.Join(ids, ",")))
			pending[tb.name] = vs[n:]
		}
		nowCopy := now
		p.now = func() time.Time { return nowCopy }
		if err := p.tick(t.Context()); err != nil {
			t.Fatalf("tick %s: %v", now, err)
		}
	}

	rep := openRO(t, replica)
	rows, err := rep.Query(`SELECT order_id, result, duration_ms, fsm_event_count, close_reason,
	                               started_at_ms
	                          FROM cycles WHERE is_production=1 ORDER BY started_at_ms`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out strings.Builder
	for rows.Next() {
		var key, result, reason string
		var dur, events, started int64
		rows.Scan(&key, &result, &dur, &events, &reason, &started)
		fmt.Fprintf(&out, "%s\t%s\t%s\t%d\t%d\t%s\n", time.UnixMilli(started).UTC().Format("15:04:05.000"),
			key, result, dur, events, reason)
	}
	if path := os.Getenv("REPLAY_OUT"); path != "" {
		os.WriteFile(path, []byte(out.String()), 0o644)
	}
	t.Logf("production cycles:\n%s", out.String())
}

func envDuration(t *testing.T, key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	return d
}
