package main

// Reader progress under the size cap: the 27 Oct 2026 incident. fsm.log sat at its 20GB cap,
// filled with one repeated line. Every tick the cap cut a few KB of unread bytes, the reader
// went back to the open digest window at the file's start, and newer lines were never read —
// for days, with the projector's heartbeat current the whole time.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// collapseUnlessReal emulates the cut, unless FSM_LOG_REAL_COLLAPSE asks for the real
// fallocate (Linux, on a filesystem that supports it; see fsmlog_collapse_linux_test.go).
func collapseUnlessReal(t *testing.T) {
	if os.Getenv("FSM_LOG_REAL_COLLAPSE") == "" {
		emulateCollapse(t)
	}
}

const incidentLine = "[I/O read] sensor: context canceled (logged only, no fault)"

// TestFSMLogFreshLinesReachTheReplicaUnderContinuousCapCuts is the incident: a capped file of
// one old repeated line, newer lines behind it, and the controller still appending. A file kept
// under the cap is no evidence the reader gets anywhere; the newer line has to land.
func TestFSMLogFreshLinesReachTheReplicaUnderContinuousCapCuts(t *testing.T) {
	collapseUnlessReal(t)
	f := newLogFixture(t)
	const capBytes int64 = 6 << 20
	old := logAt(base, incidentLine) + "\n"
	f.appendRaw(strings.Repeat(old, int(capBytes)/len(old)))
	f.appendLines(logAt(base.Add(time.Hour), "fresh sentinel"))
	lt, closeTailer := f.pruningTailer(base.Add(time.Hour), time.Minute)
	defer closeTailer()
	lt.pruneMaxBytes, lt.pruneMaxAge = capBytes, 72*time.Hour
	ro := openRO(t, f.replicaPath)

	for i := 0; i < 32; i++ {
		now := base.Add(time.Hour + time.Duration(i)*5*time.Second)
		f.appendRaw(strings.Repeat(logAt(now, "new traffic "+strings.Repeat("x", 512))+"\n", 16))
		drainAt(t, lt, now, false)
		if got := fileSize(t, f.log); got > capBytes {
			t.Fatalf("tick %d: file %d bytes, over the %d cap", i, got, capBytes)
		}
		if scalarInt(t, ro, `SELECT COUNT(*) FROM fsm_logs WHERE message = 'fresh sentinel'`) == 1 {
			return
		}
	}
	c, _, _ := lt.st.LogCursor(f.log)
	t.Fatalf("fresh line not in fsm_logs after 32 ticks under the cap (saved offset %d, base %d)", c.offset, c.base)
}

// TestFSMLogBacklogSavesItsPositionAsItReads: one repeated line through a backlog many chunks
// long. The saved position must follow the reader, not stay at the window's first line.
func TestFSMLogBacklogSavesItsPositionAsItReads(t *testing.T) {
	f := newLogFixture(t)
	line := logAt(base, incidentLine)
	n := 3 * int(logMaxReadBytes) / (len(line) + 1)
	f.appendRaw(strings.Repeat(line+"\n", n))
	lt, closeTailer := f.openTailer(base, time.Minute)
	defer closeTailer()
	lt.drainBudget = 0 // one chunk per call, as when a tick runs out of time

	drainAt(t, lt, base, false)
	c, _, err := lt.st.LogCursor(f.log)
	if err != nil {
		t.Fatal(err)
	}
	if c.offset < logMaxReadBytes-int64(len(line)+1) {
		t.Fatalf("saved offset %d after a %d byte chunk; the open window pinned it", c.offset, logMaxReadBytes)
	}
	for i := 1; i < 4; i++ {
		drainAt(t, lt, base, false)
	}
	drainAt(t, lt, base.Add(2*time.Minute), true)
	if got := scalarInt(t, openRO(t, f.replicaPath),
		`SELECT SUM(repeat_count) FROM fsm_logs WHERE category <> 'tailer'`); got != int64(n) {
		t.Fatalf("lines accounted for = %d, want %d", got, n)
	}
}

// TestFSMLogIdenticalHeadDoesNotProveAPendingCut: a file that starts with the same bytes before
// and after a cut cannot show from its start whether the cut happened. Only a shorter file can.
func TestFSMLogIdenticalHeadDoesNotProveAPendingCut(t *testing.T) {
	emulateCollapse(t)
	f := newLogFixture(t)
	f.appendRaw(strings.Repeat("x", 64<<10))
	lt, closeTailer := f.openTailer(base, 0)
	defer closeTailer()
	sum, n, err := fileHeadHash(f.log, 4096)
	if err != nil {
		t.Fatal(err)
	}
	pending := logCursor{fileGen: "g", offset: 32 << 10, pendingLen: 8 << 10,
		pendingHeadLen: n, pendingHeadSHA: sum, pendingSize: 64 << 10}

	if c := lt.settlePendingPrune(pending); c.offset != 32<<10 || c.base != 0 {
		t.Fatalf("no cut was made, yet offset=%d base=%d", c.offset, c.base)
	}
	if err := emulatedCollapse(f.log, 8<<10); err != nil {
		t.Fatal(err)
	}
	if c := lt.settlePendingPrune(pending); c.offset != 24<<10 || c.base != 8<<10 {
		t.Fatalf("the cut landed and the file shrank, yet offset=%d base=%d", c.offset, c.base)
	}
}

// TestFSMLogCutsAreRecordedDistinguishably: a journal is only written for a cut after which
// the file would start differently, so the hash check can prove it later.
func TestFSMLogCutsAreRecordedDistinguishably(t *testing.T) {
	f := newLogFixture(t)
	line := logAt(base, "Step changed: 8 -> 12 pad") + "\n" // 46 bytes; cuts of 46×4096 repeat
	f.appendRaw(strings.Repeat(line, 8192))
	size := fileSize(t, f.log)
	head, err := readFileRange(f.log, 0, logHeadBytes)
	if err != nil {
		t.Fatal(err)
	}
	same := int64(len(line)) * 4096 // a whole number of lines and of 4096-byte blocks
	for _, capped := range []bool{true, false} {
		cut, data, err := distinctCut(f.log, size, same, 4096, 8192, capped)
		if err != nil || data == nil {
			t.Fatalf("capped=%t: no cut (%v)", capped, err)
		}
		if string(data) == string(head) {
			t.Fatalf("capped=%t: cut %d leaves the same first bytes", capped, cut)
		}
		if !capped && cut > same {
			t.Fatalf("age cut moved later, to %d, past what is recorded", cut)
		}
	}
}

// TestFSMLogAccountsForEveryLine is the accounting the fix has to keep, over random bursts of
// one repeated line, mixed lines, and (every other seed) a small size cap: every line written is
// counted once in fsm_logs, or lies in a gap a loss row reports. None vanishes silently and none
// is counted twice.
//
// With restarts a gap can also take in lines that already have rows: a restart re-reads from
// its oldest open window, and lines past that were written before it. So there the gaps may
// over-report, never under-report.
func TestFSMLogAccountsForEveryLine(t *testing.T) {
	collapseUnlessReal(t)
	for seed := int64(1); seed <= 12; seed++ {
		for _, restarts := range []bool{false, true} {
			t.Run(fmt.Sprintf("seed%d/restarts=%t", seed, restarts), func(t *testing.T) {
				accountForEveryLine(t, seed, restarts)
			})
		}
	}
}

var lossRange = regexp.MustCompile(`between log positions (\d+) and (\d+)`)

func accountForEveryLine(t *testing.T, seed int64, restarts bool) {
	rng := rand.New(rand.NewSource(seed))
	capped := seed%2 == 0
	f := newLogFixture(t)

	var starts []int64 // where each line written starts, in the file as if never cut
	var end int64
	write := func(lines []string) {
		for _, l := range lines {
			starts = append(starts, end)
			end += int64(len(l)) + 1
		}
		f.appendLines(lines...)
	}
	open := func(now time.Time) (*logTailer, func()) {
		lt, closeTailer := f.pruningTailer(now, time.Minute)
		lt.drainBudget = time.Duration(rng.Intn(2)) * time.Hour // one chunk per call, or all
		if capped {
			lt.pruneMaxBytes = 3 << 20
		}
		return lt, closeTailer
	}

	now := base
	lt, closeTailer := open(now)
	for tick := 0; tick < 40; tick++ {
		now = now.Add(time.Duration(5+rng.Intn(60)) * time.Second)
		var lines []string
		switch rng.Intn(3) {
		case 0:
			for i, n := 0, rng.Intn(80000); i < n; i++ {
				lines = append(lines, logAt(now.Add(time.Duration(i/20000)*time.Second), incidentLine))
			}
		case 1:
			for i, n := 0, rng.Intn(300); i < n; i++ {
				switch rng.Intn(4) {
				case 0:
					lines = append(lines, logAt(now, fmt.Sprintf("❌ [ERROR] write failed %d", rng.Intn(9))))
				case 1:
					lines = append(lines, "  Glass Count: 1")
				case 2:
					lines = append(lines, logAt(now, fmt.Sprintf("Step changed: %d -> %d", rng.Intn(13), rng.Intn(13))))
				default:
					lines = append(lines, logAt(now, "varied "+strings.Repeat("y", rng.Intn(200))))
				}
			}
		default:
			for i := 0; i < 50; i++ {
				lines = append(lines, logAt(now, "[Homing] Cup transfer already at dispense - ensuring door closed"))
			}
		}
		if len(lines) > 0 {
			write(lines)
		}
		drainAt(t, lt, now, false)
		if capped && fileSize(t, f.log) > lt.pruneMaxBytes {
			t.Fatalf("tick %d: file %d bytes, over the %d cap", tick, fileSize(t, f.log), lt.pruneMaxBytes)
		}
		if rng.Intn(5) == 0 && restarts {
			closeTailer()
			lt, closeTailer = open(now)
		}
	}
	write([]string{logAt(now, "end")})
	lt.drainBudget = time.Hour
	drainAt(t, lt, now.Add(2*time.Minute), true)
	closeTailer()

	ro := openRO(t, f.replicaPath)
	counted := scalarInt(t, ro, `SELECT COALESCE(SUM(repeat_count), 0) FROM fsm_logs WHERE category <> 'tailer'`)
	rows, err := ro.Query(`SELECT message FROM fsm_logs WHERE tag = 'fsm_log_gap'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var gaps [][2]int64
	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err != nil {
			t.Fatal(err)
		}
		m := lossRange.FindStringSubmatch(msg)
		if m == nil {
			t.Fatalf("loss row without a range: %q", msg)
		}
		var g [2]int64
		fmt.Sscan(m[1], &g[0])
		fmt.Sscan(m[2], &g[1])
		gaps = append(gaps, g)
	}
	var lost int64
	for _, s := range starts {
		for _, g := range gaps {
			if s >= g[0] && s < g[1] {
				lost++
				break
			}
		}
	}
	if !capped && lost > 0 {
		t.Fatalf("%d lines reported lost with no size cap", lost)
	}
	written := int64(len(starts))
	if counted > written || counted+lost < written || (!restarts && counted+lost != written) {
		t.Fatalf("written %d lines: %d counted in fsm_logs + %d in reported gaps = %d (%d gaps)",
			written, counted, lost, counted+lost, len(gaps))
	}
	if scalarInt(t, ro, `SELECT COUNT(*) FROM fsm_logs WHERE message = 'end'`) != 1 {
		t.Fatal("the last line written is not in fsm_logs")
	}
	t.Logf("lines=%d counted=%d lost=%d gaps=%d", len(starts), counted, lost, len(gaps))
}

// BenchmarkFSMLogBurstCatchUp reads a backlog of the incident's repeated line the way the
// long-running loop does, for the MB/s it catches up at (the burst wrote ~19MB/s).
func BenchmarkFSMLogBurstCatchUp(b *testing.B) {
	line := logAt(base, incidentLine) + "\n"
	benchmarkCatchUp(b, strings.Repeat(line, (256<<20)/len(line)))
}

// BenchmarkFSMLogErrorFloodCatchUp is the slow case: error lines are never digested, so every
// line is a row.
func BenchmarkFSMLogErrorFloodCatchUp(b *testing.B) {
	var sb strings.Builder
	for i := 0; sb.Len() < 32<<20; i++ {
		sb.WriteString(logAt(base.Add(time.Duration(i/1000)*time.Second), fmt.Sprintf("❌ [ERROR] write failed at %d", i)) + "\n")
	}
	benchmarkCatchUp(b, sb.String())
}

func benchmarkCatchUp(b *testing.B, log string) {
	t := &testing.T{}
	f := newLogFixture(t)
	f.appendRaw(log)
	size := fileSize(t, f.log)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lt, closeTailer := f.openTailer(base, time.Minute)
		lt.st.db.Exec(`DELETE FROM log_cursors`)
		if err := lt.Drain(context.Background(), true); err != nil {
			b.Fatal(err)
		}
		closeTailer()
	}
	b.SetBytes(size)
}
