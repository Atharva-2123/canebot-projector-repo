package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// fsm.log tailing
// ---------------------------------------------------------------------------

// logFixture is a source database, an fsm.log beside it, and the projector's two databases.
type logFixture struct {
	t                      *testing.T
	source, dir, log       string
	replicaPath, statePath string
}

func newLogFixture(t *testing.T) *logFixture {
	t.Helper()
	source, _ := newSourceDB(t)
	dir := t.TempDir()
	return &logFixture{
		t: t, source: source, dir: dir,
		log:         filepath.Join(dir, "fsm.log"),
		replicaPath: filepath.Join(dir, "canebot_replica.db"),
		statePath:   filepath.Join(dir, "projector_state.db"),
	}
}

// logAt renders a line the way the controller's default Go logger writes it.
func logAt(at time.Time, msg string) string {
	return at.UTC().Format("2006/01/02 15:04:05") + " " + msg
}

func (f *logFixture) appendRaw(s string) {
	f.t.Helper()
	fh, err := os.OpenFile(f.log, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		f.t.Fatalf("open log: %v", err)
	}
	defer fh.Close()
	if _, err := fh.WriteString(s); err != nil {
		f.t.Fatalf("append log: %v", err)
	}
}

func (f *logFixture) appendLines(lines ...string) {
	f.t.Helper()
	f.appendRaw(strings.Join(lines, "\n") + "\n")
}

// runOnce is one -once projector pass with the tail on.
func (f *logFixture) runOnce(digest time.Duration) *sql.DB {
	f.t.Helper()
	if err := runWith(options{
		sourcePath: f.source, replicaPath: f.replicaPath, statePath: f.statePath,
		interval: time.Second, batchSize: 500, once: true,
		fsmLogPath: f.log, fsmLogDigest: digest,
	}); err != nil {
		f.t.Fatalf("projector run: %v", err)
	}
	return openRO(f.t, f.replicaPath)
}

// openTailer drives the tailer directly, with a controlled clock, the way the long-running
// loop does between ticks.
func (f *logFixture) openTailer(now time.Time, digest time.Duration) (*logTailer, func()) {
	f.t.Helper()
	st, err := openState(f.statePath)
	if err != nil {
		f.t.Fatalf("open state: %v", err)
	}
	rep, err := openReplica(f.replicaPath, st)
	if err != nil {
		st.Close()
		f.t.Fatalf("open replica: %v", err)
	}
	lt := newLogTailer(f.log, rep, st, digest)
	lt.now = func() time.Time { return now }
	return lt, func() { rep.Close(); st.Close() }
}

type logRow struct {
	eventTS, tsSource, level, category, message string
	logger, tag, tag2                           sql.NullString
	loggedAtMS, repeatCount                     int64
	isContinuation                              int64
}

func logRowAt(t *testing.T, db *sql.DB, lineNo int64) logRow {
	t.Helper()
	var r logRow
	if err := db.QueryRow(
		`SELECT event_ts, ts_source, level, category, message, logger, tag, tag2,
		        logged_at_ms, repeat_count, is_continuation
		   FROM fsm_logs WHERE line_no = ? AND ts_source <> 'digest'`, lineNo,
	).Scan(&r.eventTS, &r.tsSource, &r.level, &r.category, &r.message, &r.logger, &r.tag, &r.tag2,
		&r.loggedAtMS, &r.repeatCount, &r.isContinuation); err != nil {
		t.Fatalf("fsm_logs line %d: %v", lineNo, err)
	}
	return r
}

// TestFSMLogParsesTheControllersFormats covers every shape of line the controller writes. A
// line parsed wrong is not an error anywhere — it just lands with the wrong time or no tag, and
// the dashboard filter that depends on it quietly misses it.
func TestFSMLogParsesTheControllersFormats(t *testing.T) {
	f := newLogFixture(t)
	f.appendLines(
		logAt(base, "Step changed: 1 -> 2"),
		// observability loggers put their prefix BEFORE the date
		"[db_hook] "+logAt(base.Add(time.Second), "buffer full, dropping event"),
		// fmt.Printf in main.go: a time, no date
		"[10:00:03] State changed: HomeIdle -> AutoCycle",
		"  Glass Count: 1",
		logAt(base.Add(4*time.Second),
			"📡 [REQUEST #152365] READ from UnitID-1 - Door Status (Addr=4928/Modicon:4928, protocol:927)"),
		logAt(base.Add(5*time.Second),
			"[TILTER-HOLD-GUARD] [Homing] Skipping tilter REV homing: hold pending (remaining=2.5s, step=4)"),
		logAt(base.Add(6*time.Second), "❌ [ERROR] write register failed"),
	)
	rep := f.runOnce(0)

	if r := logRowAt(t, rep, 1); r.tsSource != "line" || r.loggedAtMS != base.UnixMilli() ||
		r.category != "fsm_step" || r.level != "info" || r.message != "Step changed: 1 -> 2" {
		t.Errorf("plain line parsed as %+v", r)
	}
	if r := logRowAt(t, rep, 2); r.logger.String != "db_hook" || r.category != "observability" ||
		r.level != "warn" || r.message != "buffer full, dropping event" ||
		r.loggedAtMS != base.Add(time.Second).UnixMilli() {
		t.Errorf("prefixed logger line parsed as %+v", r)
	}
	if r := logRowAt(t, rep, 3); r.tsSource != "clock" || r.category != "fsm_state" ||
		r.loggedAtMS != base.Add(3*time.Second).UnixMilli() {
		t.Errorf("[HH:MM:SS] line parsed as %+v; want the date taken from the line before", r)
	}
	if r := logRowAt(t, rep, 4); r.isContinuation != 1 || r.tsSource != "inherited" ||
		r.loggedAtMS != base.Add(3*time.Second).UnixMilli() || r.category != "fsm_state" ||
		r.message != "Glass Count: 1" {
		t.Errorf("continuation parsed as %+v; want the previous line's time and category", r)
	}
	if r := logRowAt(t, rep, 5); r.tag.String != "REQUEST" || r.category != "modbus" {
		t.Errorf("modbus line parsed as %+v; want tag REQUEST without the counter", r)
	}
	if r := logRowAt(t, rep, 6); r.tag.String != "TILTER-HOLD-GUARD" || r.tag2.String != "Homing" {
		t.Errorf("two-tag line parsed as %+v", r)
	}
	if r := logRowAt(t, rep, 7); r.level != "error" || r.tag.String != "ERROR" {
		t.Errorf("error line parsed as %+v", r)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE category = 'tailer'`); n != 1 {
		t.Errorf("tailer rows = %d, want exactly the one 'started' row", n)
	}
}

// TestFSMLogIdleLoopLinesAreNotErrors keeps the level filter useful. The idle loop's Modbus
// polling mentions a "Fault Register" and state changes name the Error state, dozens of times a
// minute; flagging those buries every real error.
func TestFSMLogIdleLoopLinesAreNotErrors(t *testing.T) {
	for _, msg := range []string{
		"📡 [REQUEST #1] READ from UnitID-1 - Starting at Fault Register 4883 (Addr=4883/Modicon:4883)",
		"State transition: HomeIdle -> Error",
		"[HomeIdle Step 3] Singling blocked",
		"[HOPPER_EMPTY_MONITOR] heartbeat state=HomeIdle X0.1=false",
	} {
		if got := logLevel(msg); got != "info" {
			t.Errorf("logLevel(%q) = %s, want info", msg, got)
		}
	}
}

// platformTS is event_ts as the platform keeps it: DateTime64(3), so to the millisecond.
func platformTS(ts string) string { return ts[:len("2026-08-11T10:00:00.000")] }

// TestFSMLogEventTSKeepsLineOrderWithinASecond: the controller logs whole seconds, and the
// platform drops the agent's row id, so without the sequence every line of a busy second would
// share one timestamp and come back in any order. The order has to survive the platform's
// millisecond precision, not just the replica's nine digits.
func TestFSMLogEventTSKeepsLineOrderWithinASecond(t *testing.T) {
	f := newLogFixture(t)
	for i := 0; i < 5; i++ {
		f.appendLines(logAt(base, fmt.Sprintf("line %c", 'a'+i)))
	}
	rep := f.runOnce(0)

	rows, err := rep.Query(
		`SELECT event_ts, logged_at_ms FROM fsm_logs WHERE ts_source = 'line' ORDER BY line_no`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	const wantWidth = len("2026-08-11T10:00:00.000000000Z")
	var prev string
	var n int
	for rows.Next() {
		var ts string
		var at int64
		if err := rows.Scan(&ts, &at); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if len(ts) != wantWidth {
			t.Fatalf("event_ts %q is not fixed width", ts)
		}
		if prev != "" && platformTS(ts) <= platformTS(prev) {
			t.Fatalf("event_ts %s does not sort after %s at millisecond precision; "+
				"same-second lines lose their order on the platform", ts, prev)
		}
		if at != base.UnixMilli() {
			t.Errorf("logged_at_ms = %d, want the real second %d", at, base.UnixMilli())
		}
		prev = ts
		n++
	}
	if n != 5 {
		t.Fatalf("rows = %d, want 5", n)
	}
	// The "started" marker takes the second's first millisecond; the first line follows it.
	if first := scalarStr(t, rep,
		`SELECT event_ts FROM fsm_logs WHERE line_no = 1`); first != "2026-08-11T10:00:00.001000000Z" {
		t.Errorf("first line event_ts = %s, want its logged second plus one millisecond step", first)
	}
}

// TestFSMLogDigestsKeepTheirOwnMilliseconds: a window closes with one digest per repeating line,
// all at its end. Sharing one millisecond, they tie on the platform; they must also land after
// every line of their window and before the next window's.
func TestFSMLogDigestsKeepTheirOwnMilliseconds(t *testing.T) {
	f := newLogFixture(t)
	var lines []string
	for s := 0; s < 60; s++ {
		for _, msg := range []string{"Singling blocked %d", "Cup transfer already at dispense %d", "heartbeat %d"} {
			lines = append(lines, logAt(base.Add(time.Duration(s)*time.Second), fmt.Sprintf(msg, s)))
		}
	}
	lines = append(lines, logAt(base.Add(time.Minute), "next window"))
	f.appendLines(lines...)
	rep := f.runOnce(time.Minute)

	rows, err := rep.Query(`SELECT event_ts, ts_source FROM fsm_logs ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var prev string
	digests := 0
	for rows.Next() {
		var ts, src string
		if err := rows.Scan(&ts, &src); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if prev != "" && platformTS(ts) <= platformTS(prev) {
			t.Fatalf("%s row at %s does not sort after %s at millisecond precision", src, ts, prev)
		}
		if src == "digest" {
			digests++
			if !strings.HasPrefix(ts, "2026-08-11T10:00:59.99") {
				t.Errorf("digest event_ts %s is not at the end of its window", ts)
			}
		}
		prev = ts
	}
	if digests != 3 {
		t.Errorf("digest rows = %d, want 3", digests)
	}
}

// TestFSMLogMarkerDoesNotDragBacklogForward: the "tailer started" row is written with a backlog
// behind it. Stamped at now it would clamp every older line up to now through the guard.
func TestFSMLogMarkerDoesNotDragBacklogForward(t *testing.T) {
	f := newLogFixture(t)
	f.appendLines(logAt(base, "old line"), logAt(base.Add(time.Minute), "later line"))
	rep := f.runOnce(0)

	if ts := scalarStr(t, rep, `SELECT event_ts FROM fsm_logs WHERE line_no = 2`); !strings.HasPrefix(ts, "2026-08-11T10:01:00") {
		t.Errorf("backlog line event_ts = %s; it was dragged forward from its logged time", ts)
	}
}

// TestFSMLogPartialLineIsHeldBack: the controller can be mid-write when the tail reads. Emitting
// half a line would split one message into two rows, the second with no timestamp.
func TestFSMLogPartialLineIsHeldBack(t *testing.T) {
	f := newLogFixture(t)
	f.appendRaw(logAt(base, "Step changed: 3 -> 4") + "\n" + logAt(base.Add(time.Second), "Step cha"))
	rep := f.runOnce(0)
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE ts_source = 'line'`); n != 1 {
		t.Fatalf("rows after a partial write = %d, want 1 (the partial line held back)", n)
	}

	f.appendRaw("nged: 4 -> 5\n")
	rep = f.runOnce(0)
	if msg := scalarStr(t, rep, `SELECT message FROM fsm_logs WHERE line_no = 2`); msg != "Step changed: 4 -> 5" {
		t.Errorf("completed line = %q, want it whole", msg)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE is_continuation = 1`); n != 0 {
		t.Errorf("%d continuation rows; the two halves were written separately", n)
	}
}

// TestFSMLogReRunIsIdempotent checks the cursor: re-reading nothing new writes nothing.
func TestFSMLogReRunIsIdempotent(t *testing.T) {
	f := newLogFixture(t)
	f.appendLines(logAt(base, "a"), logAt(base.Add(time.Second), "b"))
	for i := 0; i < 3; i++ {
		f.runOnce(time.Minute)
	}
	rep := openRO(t, f.replicaPath)
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs`); n != 3 {
		t.Errorf("rows after 3 runs = %d, want 3 (marker + 2 lines)", n)
	}
}

// TestFSMLogTruncationStartsANewGeneration: a rotated-by-truncation log shrinks below the saved
// offset. Resuming there would read past the end forever and never see a new line.
func TestFSMLogTruncationStartsANewGeneration(t *testing.T) {
	f := newLogFixture(t)
	f.appendLines(logAt(base, "first file line one"), logAt(base, "first file line two"))
	f.runOnce(0)

	if err := os.Truncate(f.log, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	f.appendLines(logAt(base.Add(time.Hour), "after"))
	rep := f.runOnce(0)

	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE message = 'after'`); n != 1 {
		t.Errorf("line written after truncation: %d rows, want 1", n)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(DISTINCT file_gen) FROM fsm_logs`); n != 2 {
		t.Errorf("generations = %d, want 2", n)
	}
	if n := scalarInt(t, rep,
		`SELECT COUNT(*) FROM fsm_logs WHERE category = 'tailer' AND message LIKE '%truncated%'`); n != 1 {
		t.Errorf("truncation marker rows = %d, want 1", n)
	}
}

// TestFSMLogTruncatedAndRegrownIsDetected: cut and refilled past the old offset between two
// reads, the size check passes. Only the head fingerprint notices, and without it the tail
// resumes mid-line in unrelated content.
func TestFSMLogTruncatedAndRegrownIsDetected(t *testing.T) {
	f := newLogFixture(t)
	f.appendLines(logAt(base, "short"))
	f.runOnce(0)

	if err := os.Truncate(f.log, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	f.appendLines(logAt(base.Add(time.Hour), "a much longer first line in the regrown file"),
		logAt(base.Add(time.Hour), "second"))
	rep := f.runOnce(0)

	if n := scalarInt(t, rep,
		`SELECT COUNT(*) FROM fsm_logs WHERE message = 'a much longer first line in the regrown file'`); n != 1 {
		t.Errorf("regrown file's first line: %d rows, want 1 — the tail resumed mid-file", n)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE ts_source = 'inherited'`); n != 0 {
		t.Errorf("%d rows parsed from the middle of a line", n)
	}
}

// TestFSMLogRotatedRemainderIsRead: lines written between the last read and a copy-then-truncate
// rotation exist only in the copy. Without reading it they are lost.
func TestFSMLogRotatedRemainderIsRead(t *testing.T) {
	f := newLogFixture(t)
	f.appendLines(logAt(base, "read before rotation"))
	f.runOnce(0)

	f.appendLines(logAt(base.Add(time.Second), "written just before rotation"))
	data, err := os.ReadFile(f.log)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if err := os.WriteFile(f.log+".1", data, 0o644); err != nil {
		t.Fatalf("copy log: %v", err)
	}
	if err := os.Truncate(f.log, 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	f.appendLines(logAt(base.Add(2*time.Second), "after rotation"))
	rep := f.runOnce(0)

	for _, msg := range []string{"read before rotation", "written just before rotation", "after rotation"} {
		if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE message = ?`, msg); n != 1 {
			t.Errorf("%q: %d rows, want exactly 1", msg, n)
		}
	}
}

// TestFSMLogFileIsNeverWritten: the log belongs to the controller's supervisor.
func TestFSMLogFileIsNeverWritten(t *testing.T) {
	f := newLogFixture(t)
	f.appendLines(logAt(base, "a"), logAt(base, "b"))
	before, beforeSize := hashFile(t, f.log)
	f.runOnce(time.Minute)
	after, afterSize := hashFile(t, f.log)
	if before != after || beforeSize != afterSize {
		t.Fatalf("fsm.log was modified: %s (%d) -> %s (%d)", before, beforeSize, after, afterSize)
	}
}

// TestFSMLogTableIsNeverCreatedEmpty guards two rollout hazards. With the flag off the table
// must not exist at all, or an omega not yet configured for it installs triggers on it. With it
// on, the table must never be observed empty, or the agent fast-forwards past its first rows.
func TestFSMLogTableIsNeverCreatedEmpty(t *testing.T) {
	sourcePath, db := newSourceDB(t)
	seedCompletedCycle(t, db, "ORD-log001", base)
	rep := openRO(t, runOnce(t, sourcePath, t.TempDir()))
	if n := scalarInt(t, rep,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='fsm_logs'`); n != 0 {
		t.Fatal("fsm_logs exists with -fsm-log unset")
	}

	f := newLogFixture(t)
	f.appendRaw("")
	rep = f.runOnce(time.Minute)
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs`); n != 1 {
		t.Errorf("fsm_logs rows for an empty log = %d, want the marker row alone", n)
	}
}

// TestFSMLogSpamIsDigested is the idle loop: the same lines at 5 Hz. Every line must still be
// accounted for — the digest may compress, never drop.
func TestFSMLogSpamIsDigested(t *testing.T) {
	f := newLogFixture(t)
	const perSecond, seconds = 5, 120
	var lines []string
	for s := 0; s < seconds; s++ {
		for i := 0; i < perSecond; i++ {
			lines = append(lines, logAt(base.Add(time.Duration(s)*time.Second),
				fmt.Sprintf("[TILTER-HOLD-GUARD] [Homing] Skipping tilter REV homing: hold pending (remaining=%d.%ds, step=4)", s%3, i)))
		}
	}
	lines = append(lines, logAt(base.Add(90*time.Second), "❌ [ERROR] write failed"),
		logAt(base.Add(90*time.Second), "❌ [ERROR] write failed"))
	f.appendLines(lines...)
	rep := f.runOnce(time.Minute)

	written := int64(perSecond*seconds + 2)
	if n := scalarInt(t, rep,
		`SELECT SUM(repeat_count) FROM fsm_logs WHERE category <> 'tailer'`); n != written {
		t.Errorf("SUM(repeat_count) = %d, want %d lines accounted for", n, written)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE level = 'info' AND category <> 'tailer'`); n != 4 {
		t.Errorf("info rows = %d, want 4 (first + digest for each of 2 minutes)", n)
	}
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE level = 'error'`); n != 2 {
		t.Errorf("error rows = %d, want 2 — errors are never digested", n)
	}
	var first, last int64
	if err := rep.QueryRow(
		`SELECT first_seen_at_ms, last_seen_at_ms FROM fsm_logs WHERE ts_source = 'digest' ORDER BY id LIMIT 1`,
	).Scan(&first, &last); err != nil {
		t.Fatalf("digest row: %v", err)
	}
	if first != base.UnixMilli() || last != base.Add(59*time.Second).UnixMilli() {
		t.Errorf("first digest spans %d..%d, want the repeats within the first minute", first, last)
	}
}

// TestFSMLogOnceReadsTheWholeBacklog: the long-running loop reads one chunk per tick, but -once
// has no next tick. Stopping after the first chunk silently left most of a large log unread.
func TestFSMLogOnceReadsTheWholeBacklog(t *testing.T) {
	f := newLogFixture(t)
	line := logAt(base, "[HomeIdle Step 5] Cane at crusher top detected - tilter at FWD position, padding "+
		strings.Repeat("x", 120))
	total := int(logMaxReadBytes/int64(len(line)+1)) + 5000
	f.appendRaw(strings.Repeat(line+"\n", total))
	rep := f.runOnce(time.Minute)

	if n := scalarInt(t, rep,
		`SELECT SUM(repeat_count) FROM fsm_logs WHERE category <> 'tailer'`); n != int64(total) {
		t.Errorf("lines accounted for = %d, want all %d across more than one chunk", n, total)
	}
}

// TestFSMLogRestartMidWindowCountsEveryLine: the long-running loop keeps a digest window open
// across ticks. A restart inside it must neither lose the repeats counted so far nor count the
// first occurrence twice.
func TestFSMLogRestartMidWindowCountsEveryLine(t *testing.T) {
	f := newLogFixture(t)
	now := base.Add(30 * time.Second)
	for s := 0; s < 20; s++ {
		f.appendLines(logAt(base.Add(time.Duration(s)*time.Second), "Cup transfer already at dispense"))
	}
	lt, closeTailer := f.openTailer(now, time.Minute)
	if err := lt.Drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	closeTailer() // the restart: the open window's count is gone with the process

	for s := 20; s < 50; s++ {
		f.appendLines(logAt(base.Add(time.Duration(s)*time.Second), "Cup transfer already at dispense"))
	}
	lt, closeTailer = f.openTailer(base.Add(2*time.Minute), time.Minute)
	defer closeTailer()
	if err := lt.Drain(context.Background(), false); err != nil {
		t.Fatalf("drain after restart: %v", err)
	}

	rep := openRO(t, f.replicaPath)
	if n := scalarInt(t, rep,
		`SELECT SUM(repeat_count) FROM fsm_logs WHERE category <> 'tailer'`); n != 50 {
		t.Errorf("lines accounted for after restart = %d, want 50", n)
	}
	if n := scalarInt(t, rep,
		`SELECT COUNT(*) FROM fsm_logs WHERE ts_source = 'line' AND category <> 'tailer'`); n != 1 {
		t.Errorf("first-occurrence rows = %d, want 1 — the restart wrote it again", n)
	}
}

// TestFSMLogDigestWaitsForItsWindow: a window closes on the clock only after it has ended, so
// a tick in the middle of a burst does not split one minute into several digest rows.
func TestFSMLogDigestWaitsForItsWindow(t *testing.T) {
	f := newLogFixture(t)
	for s := 0; s < 10; s++ {
		f.appendLines(logAt(base.Add(time.Duration(s)*time.Second), "Singling blocked"))
	}
	lt, closeTailer := f.openTailer(base.Add(10*time.Second), time.Minute)
	defer closeTailer()
	if err := lt.Drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	rep := openRO(t, f.replicaPath)
	if n := scalarInt(t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE ts_source = 'digest'`); n != 0 {
		t.Fatalf("digest written %d time(s) while its window was still open", n)
	}

	lt.now = func() time.Time { return base.Add(time.Minute + logDigestGrace) }
	if err := lt.Drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n := scalarInt(t, rep, `SELECT repeat_count FROM fsm_logs WHERE ts_source = 'digest'`); n != 9 {
		t.Errorf("digest repeat_count = %d, want 9", n)
	}
}

// ---------------------------------------------------------------------------
// fsm_logs retention
// ---------------------------------------------------------------------------

// newOmegaState writes an omega state db with just the tables omegaDelivered reads, in the
// shape omega-sqlite-replication/src/state.rs creates them.
func newOmegaState(t *testing.T, tsRaw string, rowID int64, buffered, paused bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state-rs-ilyama2.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open omega state: %v", err)
	}
	defer db.Close()
	mustExec(t, db, `CREATE TABLE meta (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)`)
	mustExec(t, db, `CREATE TABLE buffer (buffer_id INTEGER PRIMARY KEY AUTOINCREMENT,
	                  source_table TEXT NOT NULL, topic TEXT NOT NULL, payload BLOB NOT NULL)`)
	mustExec(t, db, `CREATE TABLE lineage_pause (source_table TEXT NOT NULL PRIMARY KEY,
	                  server_watermark INTEGER NOT NULL DEFAULT 0)`)
	if tsRaw != "" {
		mustExec(t, db, `INSERT INTO meta VALUES ('telemetry_ts_raw:fsm_logs', ?)`, tsRaw)
		mustExec(t, db, `INSERT INTO meta VALUES ('telemetry_src_rowid:fsm_logs', ?)`, fmt.Sprint(rowID))
	}
	if buffered {
		mustExec(t, db, `INSERT INTO buffer (source_table, topic, payload) VALUES ('fsm_logs', 't', x'00')`)
	}
	if paused {
		mustExec(t, db, `INSERT INTO lineage_pause VALUES ('fsm_logs', 7)`)
	}
	return path
}

// seedAgedLogs writes one log line per age into fsm_logs and returns the replica writer.
func seedAgedLogs(t *testing.T, now time.Time, ages ...time.Duration) *replicaWriter {
	t.Helper()
	f := newLogFixture(t)
	for _, age := range ages {
		f.appendLines(logAt(now.Add(-age), fmt.Sprintf("aged %s", age)))
	}
	lt, _ := f.openTailer(now, 0)
	t.Cleanup(func() { lt.rep.Close(); lt.st.Close() })
	if err := lt.Drain(context.Background(), true); err != nil {
		t.Fatalf("drain: %v", err)
	}
	return lt.rep
}

func remainingAges(t *testing.T, rep *replicaWriter) []string {
	t.Helper()
	rows, err := rep.db.Query(`SELECT message FROM fsm_logs WHERE category <> 'tailer' ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, strings.TrimPrefix(m, "aged "))
	}
	return out
}

// TestFSMLogPruneDeletesOnlyDeliveredRows is the retention rule: past 48h a row goes only once
// omega has delivered it; past 7 days it goes regardless, so a dead agent cannot fill the disk.
func TestFSMLogPruneDeletesOnlyDeliveredRows(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	rep := seedAgedLogs(t, now, 8*24*time.Hour, 72*time.Hour, 50*time.Hour, 10*time.Hour)

	// Delivered up to and including the 72h row, not the 50h one.
	var tsRaw string
	var rowID int64
	if err := rep.db.QueryRow(`SELECT event_ts, id FROM fsm_logs WHERE message = 'aged 72h0m0s'`).
		Scan(&tsRaw, &rowID); err != nil {
		t.Fatalf("find 72h row: %v", err)
	}
	c, ok, why, err := omegaDelivered(context.Background(),
		newOmegaState(t, tsRaw, rowID, false, false), fsmLogsTable)
	if err != nil || !ok {
		t.Fatalf("omegaDelivered: ok=%v why=%q err=%v", ok, why, err)
	}
	if _, err := rep.PruneFSMLogs(context.Background(), now, 48*time.Hour, 7*24*time.Hour, &c); err != nil {
		t.Fatalf("prune: %v", err)
	}
	got := strings.Join(remainingAges(t, rep), ",")
	if want := "50h0m0s,10h0m0s"; got != want {
		t.Errorf("remaining = %s, want %s (8d gone by age, 72h gone as delivered, 50h kept as undelivered)", got, want)
	}
}

// TestFSMLogPruneWithoutConfirmedDeliveryKeepsRows: a paused table's batches sit in omega's
// local buffer while its cursor moves on anyway, so the cursor alone would delete rows the
// platform never received.
func TestFSMLogPruneWithoutConfirmedDeliveryKeepsRows(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	farFuture := "2099-01-01T00:00:00.000000000Z"
	for _, tc := range []struct {
		name             string
		state            string
		wantWhySubstring string
	}{
		{"never delivered", newOmegaState(t, "", 0, false, false), "not delivered"},
		{"buffered", newOmegaState(t, farFuture, 1<<40, true, false), "buffer"},
		{"paused", newOmegaState(t, farFuture, 1<<40, false, true), "paused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := seedAgedLogs(t, now, 8*24*time.Hour, 72*time.Hour)
			_, ok, why, err := omegaDelivered(context.Background(), tc.state, fsmLogsTable)
			if err != nil || ok || !strings.Contains(why, tc.wantWhySubstring) {
				t.Fatalf("omegaDelivered: ok=%v why=%q err=%v", ok, why, err)
			}
			if _, err := rep.PruneFSMLogs(context.Background(), now, 48*time.Hour, 7*24*time.Hour, nil); err != nil {
				t.Fatalf("prune: %v", err)
			}
			if got := strings.Join(remainingAges(t, rep), ","); got != "72h0m0s" {
				t.Errorf("remaining = %s, want only the 8d row pruned", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Replica prune
// ---------------------------------------------------------------------------

// TestPruneKeepsCyclesThatStillHaveChildren is the FOREIGN KEY (787) failure seen on the
// machine: a cycle past the cutoff with a child that is not. Deleting it failed the whole
// statement, so no cycle was ever pruned.
func TestPruneKeepsCyclesThatStillHaveChildren(t *testing.T) {
	dir := t.TempDir()
	st, err := openState(filepath.Join(dir, "projector_state.db"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	defer st.Close()
	rep, err := openReplica(filepath.Join(dir, "canebot_replica.db"), st)
	if err != nil {
		t.Fatalf("open replica: %v", err)
	}
	defer rep.Close()

	old, recent := base.Add(-72*time.Hour), base
	for _, c := range []struct {
		order string
		at    time.Time
	}{{"ORD-stale-alone", old}, {"ORD-stale-with-child", old}, {"ORD-recent", recent}} {
		mustExec(t, rep.db,
			`INSERT INTO cycles (event_ts, started_at_ms, ended_at_ms, duration_ms, order_id,
			                     is_production, result, date_utc)
			 VALUES (?,?,?,0,?,1,'aborted',?)`,
			eventTS(c.at.UnixMilli()), c.at.UnixMilli(), c.at.UnixMilli(), c.order, dateUTC(c.at.UnixMilli()))
	}
	// Re-flushed under the same order_id hours later: the child is newer than its parent.
	mustExec(t, rep.db,
		`INSERT INTO state_durations (event_ts, entered_at_ms, exited_at_ms, duration_ms, order_id,
		                              state, is_downtime, is_production, date_utc)
		 VALUES (?,?,?,0,'ORD-stale-with-child','AutoCycle',0,1,?)`,
		eventTS(recent.UnixMilli()), recent.UnixMilli(), recent.UnixMilli(), dateUTC(recent.UnixMilli()))

	if err := rep.Prune(context.Background(), base.Add(-48*time.Hour)); err != nil {
		t.Fatalf("prune: %v", err)
	}
	var got []string
	rows, err := rep.db.Query(`SELECT order_id FROM cycles ORDER BY order_id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var o string
		if err := rows.Scan(&o); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, o)
	}
	if strings.Join(got, ",") != "ORD-recent,ORD-stale-with-child" {
		t.Errorf("cycles after prune = %v; want the childless stale cycle gone and the parent kept", got)
	}
}

// ---------------------------------------------------------------------------
// fsm.log pruning
// ---------------------------------------------------------------------------

// emulateCollapse stands in for fallocate(COLLAPSE_RANGE), which only Linux has: the same
// inode, its first n bytes gone and the rest moved down. Not safe against a concurrent writer —
// TestFSMLogPruneRealCollapseWithConcurrentWriter covers that on the machine itself.
func emulateCollapse(t *testing.T) {
	t.Helper()
	prev := collapseFileStart
	collapseFileStart = emulatedCollapse
	t.Cleanup(func() { collapseFileStart = prev })
}

func emulatedCollapse(path string, n int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if n >= int64(len(data)) {
		return fmt.Errorf("collapse of %d reaches the end of a %d byte file", n, len(data))
	}
	if _, err := f.WriteAt(data[n:], 0); err != nil {
		return err
	}
	return f.Truncate(int64(len(data)) - n)
}

// pruningTailer is openTailer with pruning on against an omega state db, small enough limits
// that a test-sized log gets cut, and no wait between checks.
func (f *logFixture) pruningTailer(now time.Time, digest time.Duration, omegaState string) (*logTailer, func()) {
	f.t.Helper()
	lt, closeTailer := f.openTailer(now, digest)
	lt.pruneOmegaState, lt.pruneKeep, lt.pruneStep, lt.pruneEvery = omegaState, 8192, 1<<30, 0
	return lt, closeTailer
}

// deliverThrough moves the fake omega's delivered cursor to fsm_logs row id (0: the newest).
func (f *logFixture) deliverThrough(omegaState string, id int64) {
	f.t.Helper()
	rep := openRO(f.t, f.replicaPath)
	if id == 0 {
		id = scalarInt(f.t, rep, `SELECT MAX(id) FROM fsm_logs`)
	}
	ts := scalarStr(f.t, rep, `SELECT event_ts FROM fsm_logs WHERE id = ?`, id)
	db, err := sql.Open("sqlite", "file:"+omegaState)
	if err != nil {
		f.t.Fatalf("open omega state: %v", err)
	}
	defer db.Close()
	mustExec(f.t, db, `INSERT OR REPLACE INTO meta VALUES ('telemetry_ts_raw:fsm_logs', ?)`, ts)
	mustExec(f.t, db, `INSERT OR REPLACE INTO meta VALUES ('telemetry_src_rowid:fsm_logs', ?)`, fmt.Sprint(id))
}

// appendSeq writes lines "seq <from>".."seq <to-1>", one per second from base.
func (f *logFixture) appendSeq(from, to int) {
	f.t.Helper()
	var lines []string
	for i := from; i < to; i++ {
		lines = append(lines, logAt(base.Add(time.Duration(i)*time.Second), fmt.Sprintf("seq %d", i)))
	}
	f.appendLines(lines...)
}

// assertEverySeqOnce: lines "seq 0".."seq n-1" are each in fsm_logs exactly once.
func (f *logFixture) assertEverySeqOnce(n int) {
	f.t.Helper()
	rep := openRO(f.t, f.replicaPath)
	rows := scalarInt(f.t, rep, `SELECT COUNT(*) FROM fsm_logs WHERE message LIKE 'seq %'`)
	distinct := scalarInt(f.t, rep, `SELECT COUNT(DISTINCT message) FROM fsm_logs WHERE message LIKE 'seq %'`)
	if rows != int64(n) || distinct != int64(n) {
		f.t.Fatalf("seq rows = %d (%d distinct), want each of %d lines exactly once", rows, distinct, n)
	}
	if g := scalarInt(f.t, rep, `SELECT COUNT(DISTINCT file_gen) FROM fsm_logs`); g != 1 {
		f.t.Errorf("generations = %d, want 1 — a prune was mistaken for a truncation", g)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}

// TestFSMLogFileIsPrunedOnlyAfterDelivery is the rule: bytes leave fsm.log once, and only once,
// ilyama2 has the rows that carry them — and lines written after a prune still land, each once.
// Rows are keyed on offset, so a prune that let a new line reuse a pruned offset would drop it
// as a duplicate without a word.
func TestFSMLogFileIsPrunedOnlyAfterDelivery(t *testing.T) {
	emulateCollapse(t)
	f := newLogFixture(t)
	omega := newOmegaState(t, "", 0, false, false)
	f.appendSeq(0, 2000)
	lt, closeTailer := f.pruningTailer(base.Add(time.Hour), 0, omega)
	defer closeTailer()
	ctx := context.Background()

	full := fileSize(t, f.log)
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := fileSize(t, f.log); got != full {
		t.Fatalf("fsm.log went from %d to %d bytes before omega delivered anything", full, got)
	}

	f.deliverThrough(omega, 0)
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := fileSize(t, f.log); got >= full || got < lt.pruneKeep {
		t.Fatalf("fsm.log is %d bytes after delivery (was %d); want it cut down to about %d", got, full, lt.pruneKeep)
	}

	f.appendSeq(2000, 4000)
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	f.deliverThrough(omega, 0)
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	f.appendSeq(4000, 4010)
	if err := lt.Drain(ctx, true); err != nil {
		t.Fatalf("drain: %v", err)
	}
	f.assertEverySeqOnce(4010)
}

// TestFSMLogPruneNeverCutsUndeliveredLines: delivered only part way, the file must still hold
// every line after that point.
func TestFSMLogPruneNeverCutsUndeliveredLines(t *testing.T) {
	emulateCollapse(t)
	f := newLogFixture(t)
	omega := newOmegaState(t, "", 0, false, false)
	f.appendSeq(0, 3000)
	lt, closeTailer := f.pruningTailer(base.Add(time.Hour), 0, omega)
	defer closeTailer()
	if err := lt.Drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}

	f.deliverThrough(omega, scalarInt(t, openRO(t, f.replicaPath), `SELECT id FROM fsm_logs WHERE message = 'seq 1000'`))
	// A later checkpoint that is not delivered yet must not be what the cut uses.
	f.appendSeq(3000, 3100)
	for i := 0; i < 2; i++ {
		if err := lt.Drain(context.Background(), false); err != nil {
			t.Fatalf("drain: %v", err)
		}
	}

	data, err := os.ReadFile(f.log)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	for i := 1001; i < 3100; i++ {
		if !strings.Contains(string(data), fmt.Sprintf(" seq %d\n", i)) {
			t.Fatalf("seq %d is gone from fsm.log, but omega had only delivered through seq 1000", i)
		}
	}
}

// TestFSMLogPruneKeepsRecentHistory: whatever has been delivered, the last pruneKeep bytes stay
// so the file is still worth reading on the machine.
func TestFSMLogPruneKeepsRecentHistory(t *testing.T) {
	emulateCollapse(t)
	f := newLogFixture(t)
	omega := newOmegaState(t, "", 0, false, false)
	f.appendSeq(0, 500)
	lt, closeTailer := f.pruningTailer(base.Add(time.Hour), 0, omega)
	defer closeTailer()
	lt.pruneKeep = 1 << 20
	if err := lt.Drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	f.deliverThrough(omega, 0)
	before := fileSize(t, f.log)
	if err := lt.Drain(context.Background(), false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := fileSize(t, f.log); got != before {
		t.Errorf("a %d byte log was cut to %d with 1MB to keep", before, got)
	}
}

// TestFSMLogPruneWaitsForOpenDigestWindows: a line inside a window still open has no row yet
// beyond its window's first — the digest carrying the rest is written when the window closes.
// Cutting it on the strength of the first row alone would lose the count.
func TestFSMLogPruneWaitsForOpenDigestWindows(t *testing.T) {
	emulateCollapse(t)
	f := newLogFixture(t)
	omega := newOmegaState(t, "", 0, false, false)
	var lines []string
	for s := 0; s < 50; s++ {
		for i := 0; i < 20; i++ {
			lines = append(lines, logAt(base.Add(time.Duration(s)*time.Second), fmt.Sprintf("Singling blocked %d", i)))
		}
	}
	f.appendLines(lines...)
	lt, closeTailer := f.pruningTailer(base.Add(20*time.Second), time.Minute, omega)
	defer closeTailer()
	ctx := context.Background()
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	f.deliverThrough(omega, 0)
	before := fileSize(t, f.log)
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := fileSize(t, f.log); got != before {
		t.Fatalf("fsm.log cut from %d to %d while its only window was open", before, got)
	}

	lt.now = func() time.Time { return base.Add(2 * time.Minute) }
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	f.deliverThrough(omega, 0)
	if err := lt.Drain(ctx, false); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := fileSize(t, f.log); got >= before {
		t.Errorf("fsm.log not cut once the window closed and its digest was delivered")
	}
	if n := scalarInt(t, openRO(t, f.replicaPath),
		`SELECT SUM(repeat_count) FROM fsm_logs WHERE category <> 'tailer'`); n != int64(len(lines)) {
		t.Errorf("lines accounted for = %d, want %d", n, len(lines))
	}
}

// TestFSMLogPruneInterruptedAroundTheCut kills the projector at the two moments a prune can be
// caught half done — cut recorded but not made, and cut made but position not yet moved. The
// restart must resume exactly: no line re-read into a duplicate, none skipped, no new generation.
func TestFSMLogPruneInterruptedAroundTheCut(t *testing.T) {
	for _, tc := range []struct {
		name    string
		collide func(path string, n int64) error
	}{
		{"before the cut", func(string, int64) error { panic("killed before the cut") }},
		{"after the cut", func(path string, n int64) error {
			if err := emulatedCollapse(path, n); err != nil {
				return err
			}
			panic("killed after the cut")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			emulateCollapse(t)
			f := newLogFixture(t)
			omega := newOmegaState(t, "", 0, false, false)
			f.appendSeq(0, 2000)
			lt, closeTailer := f.pruningTailer(base.Add(time.Hour), 0, omega)
			if err := lt.Drain(context.Background(), false); err != nil {
				t.Fatalf("drain: %v", err)
			}
			f.deliverThrough(omega, 0)

			collapseFileStart = tc.collide
			func() {
				defer func() { _ = recover() }()
				_ = lt.Drain(context.Background(), false)
			}()
			closeTailer()
			collapseFileStart = emulatedCollapse

			f.appendSeq(2000, 2500)
			lt, closeTailer = f.pruningTailer(base.Add(time.Hour), 0, omega)
			defer closeTailer()
			if err := lt.Drain(context.Background(), true); err != nil {
				t.Fatalf("drain after restart: %v", err)
			}
			f.deliverThrough(omega, 0)
			f.appendSeq(2500, 2600)
			if err := lt.Drain(context.Background(), true); err != nil {
				t.Fatalf("drain: %v", err)
			}
			f.assertEverySeqOnce(2600)
		})
	}
}

// TestFSMLogPruneNeedsDeliveryConfigured: pruning with nothing to confirm delivery would cut
// lines nobody shipped, so the projector refuses to start that way.
func TestFSMLogPruneNeedsDeliveryConfigured(t *testing.T) {
	f := newLogFixture(t)
	err := runWith(options{
		sourcePath: f.source, replicaPath: f.replicaPath, statePath: f.statePath,
		interval: time.Second, batchSize: 500, once: true,
		fsmLogPath: f.log, fsmLogPruneFile: true,
	})
	if err == nil || !strings.Contains(err.Error(), "-fsm-log-omega-state") {
		t.Fatalf("runWith = %v, want a refusal naming -fsm-log-omega-state", err)
	}
}
