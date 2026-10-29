package main

// fsm_logs — the controller's stdout log, tailed into the replica.
//
//	canebot ──stdout──▶ fsm.log ──(read-only tail)──▶ projector ──▶ fsm_logs ──▶ omega ──▶ cloud
//
// The controller's code is untouched, so the log format is whatever Go's default logger writes:
// `2006/01/02 15:04:05 message`, whole seconds, in the process's local zone. The controller runs
// with no TZ on Android, where Go's local zone is UTC, so the dates are parsed as UTC.
//
// With pruning enabled, the tailer collapses prefixes in place while the controller appends
// to the same inode. Age cuts preserve uncopied bytes; the size cap can sacrifice them to
// protect storage. External rotation is detected by the generation check.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
)

const (
	fsmLogsTable = "fsm_logs"

	// logHeadBytes is how much of the file start is fingerprinted to recognise the same file.
	logHeadBytes = 4096
	// logMaxReadBytes bounds one chunk: what is held in memory and committed at once.
	logMaxReadBytes = 4 << 20
	// logDrainBudget is how long one call keeps reading chunks while behind. One chunk per
	// 5s tick is 0.8MB/s; the 27 Oct 2026 burst wrote ~19MB/s, so a tick must read more.
	logDrainBudget = 2 * time.Second
	// logMaxLineBytes is where a line with no newline stops being "not finished yet" and is
	// emitted as truncated, so one runaway line cannot stall the tail forever.
	logMaxLineBytes = 64 << 10
	// logMaxMessageBytes caps what one row carries. The agent fails a whole flush on a row
	// larger than its payload limit.
	logMaxMessageBytes = 2048
	// logDigestGrace is how long after a digest window ends it is held open for late lines.
	logDigestGrace = 10 * time.Second
	// logRotatedSuffix names the copy a rotation leaves behind, read for lines written after
	// the last tick but before the cut.
	logRotatedSuffix = ".1"

	// logPruneStep is the most removed by normal age trimming. The size cap removes the excess
	// regardless of this step. The cut holds the file's inode lock,
	// so the controller's log writes wait for it: measured on the machine's ext4 at ~5–8ms for
	// 16MB and ~65ms for 256MB. Stay small; the log grows ~12MB an hour.
	logPruneStep = 8 << 20
	// logPruneMin is the least worth cutting. Without it a caught-up tail trims a few tens of KB
	// every check, taking the file's lock twice a minute to reclaim almost nothing.
	logPruneMin = 1 << 20
	// logPruneEvery spaces out checks while there is nothing to cut. After a cut the next tick
	// checks again, so a backlog of expired log drains at a step per tick.
	logPruneEvery = 30 * time.Second
	// logAgeProbeBytes is how far past a candidate cut the age check looks for a stamped line.
	logAgeProbeBytes = 64 << 10
)

// errCollapseUnsupported: the platform or filesystem cannot cut a file's start in place.
var errCollapseUnsupported = errors.New("cutting the start of a file is not supported here")

// collapseFileStart removes the first n bytes of a file in place — same inode, later bytes
// shifted down, file n bytes shorter. A variable so tests can stand in for the syscall.
var collapseFileStart = collapseFileStartOS

// logLine is one fsm_logs row.
type logLine struct {
	fileGen        string
	byteOffset     int64
	lineNo         int64
	loggedAtMS     int64
	tsSource       string
	logger         string
	level          string
	category       string
	tag, tag2      string
	message        string
	fingerprint    string
	repeatCount    int64
	firstSeenMS    *int64
	lastSeenMS     *int64
	isContinuation bool
	truncated      bool

	// eventTS is fixed for digest rows. Every other row is stamped from loggedAtMS at write.
	eventTS string
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

var (
	// `2026/09/15 05:22:17 msg`, or `[db_hook] 2026/09/15 05:22:17 msg` from the observability
	// loggers, which put their prefix before the date. A fraction is accepted so a controller
	// that later logs microseconds keeps parsing.
	logStdLine = regexp.MustCompile(
		`^(?:\[([a-z_]+)\] )?(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(?: (.*))?$`)
	// `[05:49:50] State changed: A -> B` — fmt.Printf in the controller's main loop, time only.
	logClockLine = regexp.MustCompile(`^\[(\d{2}):(\d{2}):(\d{2})\] (.*)$`)
	logTag       = regexp.MustCompile(`^\[([^\[\]]{1,64})\]\s*`)
	// `REQUEST #152365` is one tag, not 152365 of them.
	logTagCounter = regexp.MustCompile(`\s*#\d+$`)
	logNumber     = regexp.MustCompile(`\d+(?:\.\d+)?`)
)

// modbusTags are the HMI server's request/response trace (canebot_server.go).
var modbusTags = map[string]bool{
	"REQUEST": true, "RESPONSE": true, "WRITE": true,
	"CLIENT CONNECTED": true, "CLIENT DISCONNECTED": true,
}

var logLevelRank = map[string]int{"info": 0, "warn": 1, "error": 2}

// parseLogLine turns one physical line into a row. prev is the line before it: a continuation
// line — `  Glass Count: 1`, a stack frame — has no time of its own and borrows prev's.
func parseLogLine(text string, prev *logLine, nowMS int64) logLine {
	ln := logLine{repeatCount: 1}
	body := text

	if m := logStdLine.FindStringSubmatch(text); m != nil {
		if t, err := time.ParseInLocation("2006/01/02 15:04:05", m[2], time.UTC); err == nil {
			ms := t.UnixMilli()
			if m[3] != "" {
				ns, _ := strconv.Atoi((m[3] + "000000000")[:9])
				ms += int64(ns) / int64(time.Millisecond)
			}
			ln.loggedAtMS, ln.tsSource, ln.logger, body = ms, "line", m[1], m[4]
		}
	}
	if ln.tsSource == "" {
		if m := logClockLine.FindStringSubmatch(text); m != nil {
			ref := nowMS
			if prev != nil {
				ref = prev.loggedAtMS
			}
			ln.loggedAtMS, ln.tsSource, body = clockOnDate(m[1], m[2], m[3], ref), "clock", m[4]
		}
	}
	if ln.tsSource == "" {
		ln.isContinuation = true
		if prev != nil {
			ln.loggedAtMS, ln.tsSource = prev.loggedAtMS, "inherited"
		} else {
			ln.loggedAtMS, ln.tsSource = nowMS, "tailer"
		}
	}

	msg := strings.TrimSpace(strings.ToValidUTF8(body, "�"))
	if len(msg) > logMaxMessageBytes {
		cut := logMaxMessageBytes
		for cut > 0 && !utf8RuneStart(msg[cut]) {
			cut--
		}
		msg, ln.truncated = msg[:cut], true
	}
	ln.message = msg
	ln.tag, ln.tag2 = logTags(msg)

	ln.level = logLevel(msg)
	ln.category = logCategory(ln.logger, ln.tag, msg)
	if ln.isContinuation && prev != nil {
		// A stack trace is the panic line's severity, however innocent each frame looks.
		if logLevelRank[prev.level] > logLevelRank[ln.level] {
			ln.level = prev.level
		}
		ln.category = prev.category
	}
	ln.fingerprint = logFingerprint(ln)
	return ln
}

// clockOnDate places an HH:MM:SS on the calendar day of ref, taking whichever of the previous,
// same or next day lands closest — so a line just after midnight follows one just before it.
func clockOnDate(hh, mm, ss string, refMS int64) int64 {
	h, _ := strconv.Atoi(hh)
	m, _ := strconv.Atoi(mm)
	s, _ := strconv.Atoi(ss)
	ref := time.UnixMilli(refMS).UTC()
	t := time.Date(ref.Year(), ref.Month(), ref.Day(), h, m, s, 0, time.UTC)
	switch d := t.Sub(ref); {
	case d > 12*time.Hour:
		t = t.Add(-24 * time.Hour)
	case d < -12*time.Hour:
		t = t.Add(24 * time.Hour)
	}
	return t.UnixMilli()
}

// logTags returns the first two [TAG]s at the head of a message, after any emoji.
func logTags(msg string) (string, string) {
	var tags []string
	s := msg
	for len(tags) < 2 {
		s = strings.TrimLeftFunc(s, isLogDecoration)
		m := logTag.FindStringSubmatch(s)
		if m == nil {
			break
		}
		tags = append(tags, logTagCounter.ReplaceAllString(strings.TrimSpace(m[1]), ""))
		s = s[len(m[0]):]
	}
	for len(tags) < 2 {
		tags = append(tags, "")
	}
	return tags[0], tags[1]
}

// isLogDecoration matches the emoji the controller puts before a tag: `📡 [REQUEST #n]`.
func isLogDecoration(r rune) bool {
	return unicode.IsSpace(r) || unicode.Is(unicode.So, r) || unicode.Is(unicode.Sk, r) ||
		unicode.Is(unicode.Mn, r) || r == '‍' || r == '️'
}

// logLevel is deliberately narrow. The idle loop's lines mention faults, errors and blocked
// states as a matter of course — "READ ... Starting at Fault Register", "HomeIdle -> Error" —
// and a level that fires on those makes the error filter useless.
func logLevel(msg string) string {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(msg, "❌"), strings.HasPrefix(l, "panic"), strings.Contains(l, "fatal"),
		strings.Contains(l, "failed"), strings.Contains(l, "[error]"), strings.Contains(l, "error:"):
		return "error"
	case strings.Contains(msg, "⚠"), strings.Contains(l, "warn"), strings.Contains(l, "dropping"):
		return "warn"
	}
	return "info"
}

func logCategory(logger, tag, msg string) string {
	switch {
	case logger != "":
		return "observability"
	case modbusTags[tag], strings.Contains(msg, "Modicon"):
		return "modbus"
	case strings.HasPrefix(msg, "Step changed"):
		return "fsm_step"
	case strings.Contains(msg, "State transition"), strings.Contains(msg, "State changed"):
		return "fsm_state"
	}
	return "other"
}

// logFingerprint identifies a message up to its numbers, so `remaining=2.5s` and
// `remaining=2.3s` are the same line repeating.
func logFingerprint(ln logLine) string {
	h := sha256.Sum256([]byte(strings.Join([]string{
		ln.level, ln.logger, ln.tag, ln.tag2, logNumber.ReplaceAllString(ln.message, "#"),
	}, "\x1f")))
	return hex.EncodeToString(h[:8])
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }

// ---------------------------------------------------------------------------
// Tailing
// ---------------------------------------------------------------------------

// logTailer follows one log file into fsm_logs.
//
// Its position is (file generation, byte offset). A generation is one lifetime of the file: a
// new one starts when the file is replaced (new inode), cut shorter than the offset, or cut and
// regrown past it — the last caught by fingerprinting the first bytes. Rows are unique on
// (generation, offset), so re-reading any stretch of the file inserts nothing twice.
//
// Repeated lines are digested. The idle loop prints the same handful of lines ~50 times a
// second; within a window, the first occurrence of a line is written as it is and the rest
// become one row carrying repeat_count. Rows are never updated — the agent does not re-ship a
// changed row — so the digest row is written once, when its window closes.
//
// A restart must not lose the count of a window still open. So the saved position is the start
// of the oldest open window's first line, not the end of the last line read: a restart re-reads
// from there, the rows already written are ignored as duplicates, and the counts are rebuilt.
//
// That only holds while the tail is caught up, when a window spans at most a minute of log.
// Reading a backlog, every chunk that leaves more unread writes its windows' counts out as they
// stand (flushDigests), so the saved position is the chunk's end. On 27 Oct 2026 one window
// held over gigabytes of a single repeated line pinned the saved position at the file's start,
// and every size cut sent the reader back there — for days, with newer lines never read.
//
// With pruning on, recorded bytes older than pruneMaxAge are cut off the start.
// The size cap also discards unread bytes when necessary — see pruneFile.
type logTailer struct {
	path   string
	rep    *replicaWriter
	st     *stateStore
	window time.Duration // 0 writes every line
	now    func() time.Time

	// Pruning, off unless pruneOn.
	pruneOn       bool
	pruneMaxAge   time.Duration // lines logged longer ago than this are cut
	pruneMaxBytes int64         // the file is cut back to this whatever its age (0: no cap)
	pruneKeep     int64         // bytes always left in the file
	pruneStep     int64         // most bytes cut at once; the controller's writes wait on it
	pruneMin      int64         // least bytes worth a cut
	pruneEvery    time.Duration // least time between checks that found nothing to cut

	loaded     bool
	haveCursor bool
	cur        logCursor // position after the last line consumed
	prev       *logLine
	open       map[string]*logDigest // by fingerprint
	tableReady bool
	missing    bool
	seqMS      int64
	seq        int64

	lastPrune     time.Time
	pruneDisabled bool

	drainBudget time.Duration

	// The last self-dated line parsed, and its raw bytes, for a run of identical lines.
	parsedOK  bool
	parsedRaw []byte
	parsed    logLine

	// Unread bytes the size cap discarded and no fsm_logs row reports yet: lossBytes in total,
	// between logical positions lossFrom and lossTo.
	lossBytes, lossFrom, lossTo int64
}

// logDigest is one window of one repeating line.
type logDigest struct {
	bucketStartMS int64
	resume        logCursor // position just before the window's first line
	repeats       int64
	firstSeenMS   int64
	last          logLine
}

func newLogTailer(path string, rep *replicaWriter, st *stateStore, window time.Duration) *logTailer {
	return &logTailer{
		path: path, rep: rep, st: st, window: window, now: time.Now,
		pruneStep: logPruneStep, pruneMin: logPruneMin, pruneEvery: logPruneEvery,
		drainBudget: logDrainBudget,
	}
}

// Drain reads whatever has been appended since the last call and writes it. final closes every
// open digest window — for -once, where there is no next tick to close them.
//
// On any error the in-memory position is discarded and reloaded from the last saved one, so
// the next call re-reads rather than skips what failed to land.
func (lt *logTailer) Drain(ctx context.Context, final bool) (err error) {
	defer func() {
		if err != nil {
			lt.loaded = false
		}
	}()

	fi, err := os.Stat(lt.path)
	if errors.Is(err, os.ErrNotExist) {
		if !lt.missing {
			log.Printf("projector: fsm log %s not found; will keep checking", lt.path)
			lt.missing = true
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat %s: %w", lt.path, err)
	}
	lt.missing = false

	if !lt.loaded {
		if err := lt.load(); err != nil {
			return lt.capWithoutCursor(fi, err)
		}
	}
	nowMS := lt.now().UnixMilli()
	dev, inode := fileIdentity(fi)

	reason, err := lt.generationChange(fi.Size(), dev, inode)
	if err != nil {
		return err
	}
	var marker *logLine
	if reason != "" {
		if lt.haveCursor {
			rest, err := lt.readRotatedRemainder(nowMS)
			if err != nil {
				return err
			}
			if err := lt.write(ctx, append(rest, lt.closeDigests(math.MaxInt64)...)); err != nil {
				return err
			}
		}
		lt.cur = logCursor{fileGen: fmt.Sprintf("%d:%d:%d", dev, inode, nowMS), dev: dev, inode: inode}
		lt.haveCursor, lt.prev = true, nil
		// Saved before anything of the new generation is written, so a crash replays into
		// this generation instead of minting another and writing every line twice.
		if err := lt.st.SetLogCursor(lt.path, lt.cur); err != nil {
			return lt.capWithoutCursor(fi, err)
		}
		m := lt.tailerRow(nowMS, reason)
		marker = &m
	}

	size := fi.Size()
	// Enforce the size cap before reading a backlog. Otherwise the 4MB-per-tick
	// reader, or a failed replica write, can prevent a flooded log from shrinking.
	if lt.pruneOn && lt.capCut(size, fileBlockSize(fi)) > 0 {
		if err := lt.pruneFile(ctx); err != nil {
			return err
		}
		fi, err = os.Stat(lt.path)
		if err != nil {
			return err
		}
		size = fi.Size()
	}
	if err := lt.refreshHead(size); err != nil {
		return err
	}
	if err := lt.reportLoss(ctx); err != nil { // one a failed write left behind
		return err
	}
	// A chunk at a time, each committed before the next. The long-running loop keeps reading
	// while behind, for up to drainBudget; a final (-once) call has no next tick, so it reads
	// to the end.
	started := time.Now()
	for {
		var rows []logLine
		start := lt.cur.offset
		end := min(size, start+logMaxReadBytes)
		if end > lt.cur.offset {
			data, err := readFileRange(lt.path, lt.cur.offset, end-lt.cur.offset)
			if err != nil {
				return err
			}
			rows = lt.consume(data, false, nowMS)
		}
		if marker != nil {
			// Stamped at the first line it precedes, not at now. Reading a backlog, now is
			// hours past every line, and the monotonic guard would drag all of them up to it.
			if len(rows) > 0 && rows[0].loggedAtMS < marker.loggedAtMS {
				marker.loggedAtMS = rows[0].loggedAtMS
			}
			rows = append([]logLine{*marker}, rows...)
			marker = nil
		}

		caughtUp := end >= size
		// Windows close on wall-clock only once the backlog is read: while working through
		// old lines, a window's remaining lines may simply be in the next chunk.
		switch {
		case final && caughtUp:
			rows = append(rows, lt.closeDigests(math.MaxInt64)...)
		case caughtUp:
			rows = append(rows, lt.closeDigests(
				nowMS-lt.window.Milliseconds()-logDigestGrace.Milliseconds()+1)...)
		default:
			// More is waiting: write the counts so far, so the saved position is this chunk's
			// end and not the first line of a window that may span the whole backlog.
			rows = append(rows, lt.flushDigests()...)
		}
		if err := lt.write(ctx, rows); err != nil {
			return err
		}
		if err := lt.st.SetLogCursor(lt.path, lt.resumePoint()); err != nil {
			return err
		}
		// A chunk that consumed nothing cannot be helped by reading it again.
		if caughtUp || lt.cur.offset == start || (!final && time.Since(started) >= lt.drainBudget) {
			break
		}
		// The file grows while a backlog is read; keep it under the cap between chunks.
		if err := lt.pruneFile(ctx); err != nil {
			return err
		}
		if fi, err = os.Stat(lt.path); err != nil {
			return err
		}
		size = fi.Size()
	}
	return lt.pruneFile(ctx)
}

// flushDigests writes out every open window's repeats so far and forgets the windows, so
// nothing read is held back and the saved position can be the read position. A window's later
// lines start a new window — its first written as it is, the rest digested — so every line is
// still counted exactly once, across as many rows as it took.
//
// Fragments are stamped at their last line, not at their window's end: a later line of the
// same window would otherwise be dragged up to the window's end by the monotonic guard.
func (lt *logTailer) flushDigests() []logLine {
	var out []logLine
	for _, d := range lt.open {
		if d.repeats == 0 {
			continue
		}
		r := d.last
		first, last := d.firstSeenMS, d.last.loggedAtMS
		r.tsSource, r.repeatCount = "digest", d.repeats
		r.firstSeenMS, r.lastSeenMS = &first, &last
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].byteOffset < out[j].byteOffset })
	lt.open = map[string]*logDigest{}
	return out
}

// noteLoss records unread bytes between logical positions from and to that the size cap
// discarded, for the next loss marker.
func (lt *logTailer) noteLoss(from, to int64) {
	if lt.lossBytes == 0 {
		lt.lossFrom = from
	}
	lt.lossBytes += to - from
	lt.lossTo = to
}

// reportLoss writes one row for the unread bytes the size cap discarded since the last such
// row, so the loss reaches the platform and not only this process's log. It is written at once,
// not held for later: a restart would forget it. It is stamped at the first line that survived —
// stamped at now it would drag a backlog forward, as the started marker would — and keyed on
// the gap's negated end, unique within a generation.
//
// The range can also take in lines that do have rows: a restart re-reads from its oldest open
// window, and lines past that were written before it. It over-reports then, never under.
func (lt *logTailer) reportLoss(ctx context.Context) error {
	if lt.lossBytes == 0 {
		return nil
	}
	now := lt.now()
	ms, ok, err := firstStampedMS(lt.path, 0)
	if err != nil || !ok {
		ms = now.UnixMilli()
	}
	ln := logLine{
		fileGen: lt.cur.fileGen, byteOffset: -lt.lossTo, loggedAtMS: ms,
		tsSource: "tailer", level: "warn", category: "tailer", tag: "fsm_log_gap", repeatCount: 1,
		message: fmt.Sprintf("fsm log size cap discarded %d bytes before they were read, "+
			"between log positions %d and %d (seen %s); not every line there was recorded",
			lt.lossBytes, lt.lossFrom, lt.lossTo, now.UTC().Format(time.RFC3339)),
	}
	ln.fingerprint = logFingerprint(ln)
	if err := lt.write(ctx, []logLine{ln}); err != nil {
		return err
	}
	lt.lossBytes = 0
	return nil
}

func (lt *logTailer) load() error {
	c, ok, err := lt.st.LogCursor(lt.path)
	if err != nil {
		return err
	}
	if ok && c.pendingLen > 0 {
		c = lt.settlePendingPrune(c)
		if err := lt.st.SetLogCursor(lt.path, c); err != nil {
			return err
		}
	}
	lt.cur, lt.haveCursor, lt.loaded = c, ok, true
	lt.open = map[string]*logDigest{}
	lt.prev = nil
	if ok && c.lastLoggedMS > 0 {
		lt.prev = &logLine{loggedAtMS: c.lastLoggedMS, level: "info", category: "other"}
	}
	return nil
}

// settlePendingPrune resolves a prune the last run recorded but may not have finished. If the
// file now starts with the bytes the cut would have left first, it landed and the position moves
// with it; otherwise it never happened and the saved position stands. Either way the ordinary
// generation checks run next.
//
// Those bytes prove the cut only if the file started differently before it. A repeated line can
// make both the same (pruneFile avoids recording such cuts, older runs did not), and then only a
// file shorter than before the cut proves it: the controller only appends. Unproven, the cut is
// taken not to have landed — if it did, up to its length of unread log is skipped.
func (lt *logTailer) settlePendingPrune(c logCursor) logCursor {
	sum, n, err := fileHeadHash(lt.path, c.pendingHeadLen)
	landed := err == nil && n == c.pendingHeadLen && sum == c.pendingHeadSHA
	if landed && (c.headLen != c.pendingHeadLen || c.headSHA == c.pendingHeadSHA) {
		fi, err := os.Stat(lt.path)
		landed = err == nil && c.pendingSize > 0 && fi.Size() < c.pendingSize
		if !landed {
			log.Printf("projector: WARNING: fsm log: cannot tell whether a prune of %d bytes interrupted by a restart landed; "+
				"assuming not, so up to %d unread bytes may be skipped", c.pendingLen, c.pendingLen)
		}
	}
	if landed {
		if c.offset < c.pendingLen {
			c.offset = c.pendingLen + firstCompleteLineOffset(lt.path)
			c.lastLoggedMS = 0
		}
		c.offset = max(0, c.offset-c.pendingLen)
		c.base += c.pendingLen
		c.headLen, c.headSHA = 0, "" // re-fingerprinted from the file as it now is
		log.Printf("projector: fsm log: completing a prune of %d bytes interrupted by a restart", c.pendingLen)
	}
	c.pendingLen, c.pendingHeadLen, c.pendingHeadSHA, c.pendingSize = 0, 0, "", 0
	return c
}

// generationChange reports why the file can no longer be read from the saved offset, or "".
func (lt *logTailer) generationChange(size int64, dev, inode uint64) (string, error) {
	switch {
	case !lt.haveCursor:
		return "started", nil
	case dev != lt.cur.dev || inode != lt.cur.inode:
		return "replaced", nil
	case size < lt.cur.offset:
		return "truncated", nil
	case lt.cur.headLen > 0:
		sum, n, err := fileHeadHash(lt.path, lt.cur.headLen)
		if err != nil {
			return "", err
		}
		if n < lt.cur.headLen || sum != lt.cur.headSHA {
			return "truncated", nil
		}
	}
	return "", nil
}

// readRotatedRemainder reads what the old generation gained after the last read, from the copy
// a rotation left at <path>.1 — if that copy really is the file we were reading.
func (lt *logTailer) readRotatedRemainder(nowMS int64) ([]logLine, error) {
	rotated := lt.path + logRotatedSuffix
	fi, err := os.Stat(rotated)
	if err != nil || lt.cur.headLen == 0 || fi.Size() <= lt.cur.offset {
		return nil, nil
	}
	sum, n, err := fileHeadHash(rotated, lt.cur.headLen)
	if err != nil || n < lt.cur.headLen || sum != lt.cur.headSHA {
		return nil, nil
	}
	data, err := readFileRange(rotated, lt.cur.offset, fi.Size()-lt.cur.offset)
	if err != nil {
		return nil, err
	}
	// The copy is finished, so a last line without a newline is complete.
	return lt.consume(data, true, nowMS), nil
}

// consume turns a chunk that starts at lt.cur.offset into rows, advancing the position past
// every complete line. An unfinished last line is left for the next read unless eof.
func (lt *logTailer) consume(data []byte, eof bool, nowMS int64) []logLine {
	var out []logLine
	pos := 0
	for pos < len(data) {
		var raw []byte
		next, truncated := 0, false
		nl := bytes.IndexByte(data[pos:], '\n')
		switch {
		case nl >= 0 && nl <= logMaxLineBytes:
			raw, next = data[pos:pos+nl], pos+nl+1
		case nl > logMaxLineBytes, len(data)-pos >= logMaxLineBytes:
			raw, next, truncated = data[pos:pos+logMaxLineBytes], pos+logMaxLineBytes, true
		case eof:
			raw, next = data[pos:], len(data)
		default:
			return out
		}

		before := lt.cur
		offset := lt.cur.offset
		lt.cur.offset += int64(next - pos)
		lt.cur.lineNo++
		pos = next

		var ln logLine
		if lt.parsedOK && bytes.Equal(raw, lt.parsedRaw) {
			// The last line again, byte for byte. Dated in itself, it parses the same: a burst
			// is millions of these, and parsing each was most of the time a backlog took.
			ln = lt.parsed
		} else {
			// A writer without O_APPEND leaves NULs where a truncation cut; they are not text.
			text := strings.TrimRight(string(bytes.TrimLeft(raw, "\x00")), "\r")
			if strings.TrimSpace(text) == "" {
				continue
			}
			ln = parseLogLine(text, lt.prev, nowMS)
			if lt.parsedOK = ln.tsSource == "line"; lt.parsedOK {
				lt.parsedRaw, lt.parsed = append(lt.parsedRaw[:0], raw...), ln
			}
		}
		ln.fileGen, ln.byteOffset, ln.lineNo = lt.cur.fileGen, lt.cur.base+offset, lt.cur.lineNo
		ln.truncated = ln.truncated || truncated
		prev := ln
		lt.prev = &prev
		lt.cur.lastLoggedMS = ln.loggedAtMS
		out = append(out, lt.admit(ln, before)...)
	}
	return out
}

// admit applies the digest to one line: returns what should be written now.
func (lt *logTailer) admit(ln logLine, before logCursor) []logLine {
	// Errors and warnings are kept line by line, and so are continuations, which only make
	// sense next to the line they continue.
	if lt.window <= 0 || ln.level != "info" || ln.isContinuation {
		return []logLine{ln}
	}
	winMS := lt.window.Milliseconds()
	start := ln.loggedAtMS - ln.loggedAtMS%winMS
	out := lt.closeDigests(start)

	d := lt.open[ln.fingerprint]
	if d == nil {
		lt.open[ln.fingerprint] = &logDigest{bucketStartMS: start, resume: before}
		return append(out, ln)
	}
	if d.repeats == 0 {
		d.firstSeenMS = ln.loggedAtMS
	}
	d.repeats++
	d.last = ln
	return out
}

// closeDigests closes every window that started before beforeMS and returns its digest row, in
// window then file order so the result does not depend on map iteration.
func (lt *logTailer) closeDigests(beforeMS int64) []logLine {
	var closing []*logDigest
	for fp, d := range lt.open {
		if d.bucketStartMS < beforeMS {
			closing = append(closing, d)
			delete(lt.open, fp)
		}
	}
	sort.Slice(closing, func(i, j int) bool {
		if closing[i].bucketStartMS != closing[j].bucketStartMS {
			return closing[i].bucketStartMS < closing[j].bucketStartMS
		}
		return closing[i].resume.offset < closing[j].resume.offset
	})
	winMS := lt.window.Milliseconds()
	perWindow := map[int64]int64{}
	for _, d := range closing {
		if d.repeats > 0 {
			perWindow[d.bucketStartMS]++
		}
	}
	var out []logLine
	placed := map[int64]int64{}
	for _, d := range closing {
		if d.repeats == 0 {
			continue
		}
		// The digest takes the last repeat's position, which no other row holds, so it is
		// unique and a replay regenerates it identically.
		//
		// Its event_ts is in the window's last milliseconds, one each, ending on the last:
		// after the lines written inside the window — which sit a few ms past their second —
		// and before any line of the next, without two digests sharing a millisecond.
		r := d.last
		first, last := d.firstSeenMS, d.last.loggedAtMS
		r.tsSource = "digest"
		r.repeatCount = d.repeats
		r.firstSeenMS, r.lastSeenMS = &first, &last
		endMS := d.bucketStartMS + winMS - 1
		slot := endMS - (perWindow[d.bucketStartMS] - 1 - placed[d.bucketStartMS])
		placed[d.bucketStartMS]++
		r.eventTS = eventTS(max(slot, d.bucketStartMS))
		out = append(out, r)
	}
	return out
}

// resumePoint is the position to save: the oldest open window's start, else the read position.
func (lt *logTailer) resumePoint() logCursor {
	c := lt.cur
	for _, d := range lt.open {
		if d.resume.offset < c.offset {
			c = d.resume
		}
	}
	c.headLen, c.headSHA = lt.cur.headLen, lt.cur.headSHA
	return c
}

// refreshHead extends the fingerprinted prefix while the file is still shorter than it.
func (lt *logTailer) refreshHead(size int64) error {
	n := min(size, logHeadBytes)
	if n <= lt.cur.headLen {
		return nil
	}
	sum, got, err := fileHeadHash(lt.path, n)
	if err != nil {
		return err
	}
	lt.cur.headLen, lt.cur.headSHA = got, sum
	return nil
}

func (lt *logTailer) tailerRow(nowMS int64, reason string) logLine {
	msg := map[string]string{
		"started":   "fsm log tailer started; reading " + lt.path + " from the beginning",
		"replaced":  lt.path + " was replaced; reading the new file from the beginning",
		"truncated": lt.path + " was truncated; reading it from the beginning",
	}[reason]
	ln := logLine{
		fileGen: lt.cur.fileGen, byteOffset: -1, loggedAtMS: nowMS, tsSource: "tailer",
		level: "info", category: "tailer", tag: "fsm_log_tailer", message: msg, repeatCount: 1,
	}
	ln.fingerprint = logFingerprint(ln)
	return ln
}

// write inserts rows in one transaction, creating fsm_logs alongside the first of them.
func (lt *logTailer) write(ctx context.Context, rows []logLine) error {
	if len(rows) == 0 {
		return nil
	}
	// Committed with synchronous=FULL, not the replica's NORMAL: the saved position goes to
	// projector_state.db (FULL) after this, and must not outlive a power cut these rows did not.
	conn, err := lt.rep.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("fsm_logs connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA synchronous = FULL`); err != nil {
		return fmt.Errorf("fsm_logs synchronous: %w", err)
	}
	defer conn.ExecContext(context.Background(), `PRAGMA synchronous = NORMAL`)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fsm_logs: %w", err)
	}
	defer tx.Rollback()

	if !lt.tableReady {
		if _, err := tx.ExecContext(ctx, fsmLogsSchema); err != nil {
			return fmt.Errorf("create fsm_logs: %w", err)
		}
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT OR IGNORE INTO fsm_logs (event_ts, logged_at_ms, ts_source, file_gen, byte_offset,
		                                 line_no, logger, level, category, tag, tag2, message,
		                                 fingerprint, repeat_count, first_seen_at_ms,
		                                 last_seen_at_ms, is_continuation, truncated,
		                                 bucket_1m_ms, date_utc)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return fmt.Errorf("prepare fsm_logs insert: %w", err)
	}
	defer stmt.Close()

	last, err := lt.rep.st.Watermark(fsmLogsTable)
	if err != nil {
		return err
	}
	for _, r := range rows {
		ts := r.eventTS
		if ts == "" {
			ts = lt.sequencedTS(r.loggedAtMS)
		}
		// Same guard as every other table: the agent's cursor never looks back, so a row
		// stamped before one already written would never ship.
		if ts < last {
			ts = last
		}
		last = ts
		if _, err := stmt.ExecContext(ctx,
			ts, r.loggedAtMS, r.tsSource, r.fileGen, r.byteOffset,
			r.lineNo, nullIfEmpty(r.logger), r.level, r.category,
			nullIfEmpty(r.tag), nullIfEmpty(r.tag2), r.message,
			r.fingerprint, r.repeatCount, r.firstSeenMS,
			r.lastSeenMS, boolToInt(r.isContinuation), boolToInt(r.truncated),
			bucket1m(r.loggedAtMS), dateUTC(r.loggedAtMS),
		); err != nil {
			return fmt.Errorf("insert fsm_logs line %d: %w", r.lineNo, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fsm_logs: %w", err)
	}
	lt.tableReady = true
	lt.rep.st.SetWatermark(fsmLogsTable, last)
	return nil
}

// sequencedTS stamps the n-th row logged at the same instant n milliseconds after it.
//
// Milliseconds, not anything finer: the platform stores event_ts as DateTime64(3), so a finer
// step is truncated away there and every line of a busy second comes back tied. The cost is
// that a line's event_ts can sit up to ~50ms (the idle loop's rate) past the second it was
// logged in — logged_at_ms keeps the real one. Capped below the next second, so a second with
// more lines than that ties at its end rather than overtaking the next second's lines.
func (lt *logTailer) sequencedTS(ms int64) string {
	if ms == lt.seqMS {
		if lt.seq < 999-ms%1000 {
			lt.seq++
		}
	} else {
		lt.seqMS, lt.seq = ms, 0
	}
	return eventTS(ms + lt.seq)
}

// pruneFile cuts expired lines off the start of the log: those logged more than pruneMaxAge
// ago, and, while the file is over pruneMaxBytes, the oldest whatever their age.
//
// Age trimming removes only bytes whose rows are already in fsm_logs — never past the saved position, which
// stops at the oldest open digest window — so a cut can lose nothing that has not been
// recorded. Delivery to the platform is not consulted: the file is the machine's local copy and
// keeps its days either way. The size cap is disk protection: it removes the entire excess,
// even if copying is behind, and warns about unrecorded bytes. pruneKeep bytes are always left.
//
// Age is read off the first stamped line after the candidate cut, and a cut goes only when that
// line is past pruneMaxAge — everything before it is older still. A cut is a whole pruneStep or
// nothing, so up to a step of expired log can wait for the next line to expire.
//
// The cut is in place and never races the controller. It holds fsm.log open O_APPEND, so each
// of its writes lands at the end as the file is at that instant — a truncate would drop a line
// written between the last read and the cut; this cannot. It removes whole filesystem blocks,
// so the file may start partway into an already-recorded line.
//
// Crash-safe in both directions: the cut is recorded before it is made (settlePendingPrune),
// and row keys are base-relative, so the lines that later occupy pruned offsets never collide
// with the rows of the lines that were cut.
func (lt *logTailer) pruneFile(ctx context.Context) error {
	if !lt.pruneOn || lt.pruneDisabled {
		return nil
	}
	now := lt.now()
	fi, err := os.Stat(lt.path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", lt.path, err)
	}
	size := fi.Size()
	block := fileBlockSize(fi)
	rp := lt.resumePoint()
	cut := lt.capCut(size, block)
	capped := cut > 0
	if capped && cut > rp.offset && rp.offset < lt.cur.offset {
		// The cut reaches lines an open window would re-read after a restart. Write the
		// windows out instead, so the saved position is the read position and the cut takes
		// only what was never read. A replica that cannot take them must not stop the cap.
		if rows := lt.flushDigests(); len(rows) > 0 {
			if err := lt.write(ctx, rows); err != nil {
				log.Printf("projector: WARNING: fsm log size cap could not record %d open digest row(s) before cutting; "+
					"their repeat counts are lost: %v", len(rows), err)
			}
		}
		lt.open = map[string]*logDigest{}
		rp = lt.resumePoint()
	}
	recorded := rp.base + rp.offset - lt.cur.base
	if !capped {
		if lt.pruneMaxAge <= 0 || (!lt.lastPrune.IsZero() && now.Sub(lt.lastPrune) < lt.pruneEvery) {
			return nil
		}
		lt.lastPrune = now
		cut = min(recorded, size-lt.pruneKeep, lt.pruneStep)
	}
	cut -= cut % block
	if cut <= 0 || (!capped && cut < lt.pruneMin) || cut >= size {
		return nil
	}
	reason := fmt.Sprintf("over the %d byte cap", lt.pruneMaxBytes)
	if !capped {
		ms, ok, err := firstStampedMS(lt.path, cut)
		if err != nil || !ok || ms >= now.Add(-lt.pruneMaxAge).UnixMilli() {
			return err
		}
		reason = "logged over " + lt.pruneMaxAge.String() + " ago"
	}

	// Record the cut, and what the file will start with once it has, before making it.
	cut, data, err := distinctCut(lt.path, size, cut, block, lt.pruneKeep, capped)
	if err != nil || data == nil {
		return err // shrank under us (the generation check sorts it out next tick), or no age cut is provable
	}
	sum := sha256.Sum256(data)
	pending := lt.resumePoint()
	pending.pendingLen, pending.pendingHeadLen, pending.pendingHeadSHA = cut, int64(len(data)), hex.EncodeToString(sum[:])
	pending.pendingSize = size
	if err := lt.st.SetLogCursor(lt.path, pending); err != nil {
		if !capped {
			return err
		}
		// The cap must free space even if a full disk prevents its journal write.
		// If interrupted, the changed head triggers the normal generation recovery.
		log.Printf("projector: WARNING: fsm log size cap proceeding without a durable cut journal; retained log may be replayed: %v", err)
	}

	started := time.Now()
	if err := collapseFileStart(lt.path, cut); err != nil {
		if errors.Is(err, errCollapseUnsupported) {
			lt.pruneDisabled = true
			log.Printf("projector: fsm log pruning is off: %v", err)
		} else {
			log.Printf("projector: fsm log prune of %d bytes failed: %v", cut, err)
		}
		return lt.st.SetLogCursor(lt.path, lt.resumePoint())
	}

	lost := max(0, cut-recorded)
	if lost > 0 {
		// The reader was behind the cut. Go on from the first whole line that survived it —
		// never back to an older saved position: with the cap cutting again every tick, that
		// kept the reader at the file's start for days (27 Oct 2026).
		from := lt.cur.base + lt.cur.offset
		lt.open = map[string]*logDigest{}
		lt.prev = nil
		lt.cur.lastLoggedMS = 0
		lt.cur.offset = cut + firstCompleteLineOffset(lt.path)
		lost = lt.cur.offset - (from - lt.cur.base)
		lt.noteLoss(from, lt.cur.base+lt.cur.offset)
	}
	lt.cur.offset = max(0, lt.cur.offset-cut)
	lt.cur.base += cut
	for key, d := range lt.open {
		if d.resume.offset < cut {
			delete(lt.open, key) // this digest's raw bytes were sacrificed to the size cap
			continue
		}
		d.resume.offset -= cut
		d.resume.base += cut
	}
	lt.cur.headLen, lt.cur.headSHA = 0, ""
	if err := lt.refreshHead(size - cut); err != nil {
		return err
	}
	log.Printf("projector: fsm log: pruned %d bytes %s off %s in %s",
		cut, reason, lt.path, time.Since(started).Round(time.Millisecond))
	if lost > 0 {
		log.Printf("projector: WARNING: fsm log size cap discarded %d bytes not yet recorded in fsm_logs; preserving disk space took priority", lost)
	}
	// Also retries a report an earlier cut could not write.
	if err := lt.reportLoss(ctx); err != nil {
		log.Printf("projector: fsm log loss report not written yet (will retry): %v", err)
	}
	lt.lastPrune = time.Time{} // more may be due; look again next tick
	return lt.st.SetLogCursor(lt.path, lt.resumePoint())
}

// distinctCut returns a cut near cut after which the file would start with different bytes than
// it does now, and those bytes. Were they the same, a restart could not tell from the file
// whether the cut had landed — likely when one line repeats, as it did through 20GB on
// 27 Oct 2026. It moves the cut a block at a time: earlier or later under the cap, only earlier
// for age (never past what is recorded). data is nil when the file shrank under it, or, for
// age, when no nearby cut is distinct. A capped cut with none goes ahead, and
// settlePendingPrune falls back on the file's size.
func distinctCut(path string, size, cut, block, keep int64, capped bool) (int64, []byte, error) {
	head, err := readFileRange(path, 0, logHeadBytes)
	if err != nil {
		return 0, nil, err
	}
	var first []byte
	for _, step := range []int64{0, 1, -1, 2, -2, 3, -3} {
		c := cut + step*block
		if c <= 0 || size-c < keep || (!capped && step > 0) {
			continue
		}
		n := min(size-c, logHeadBytes)
		data, err := readFileRange(path, c, n)
		if err != nil || int64(len(data)) != n {
			return 0, nil, err
		}
		if !bytes.Equal(data, head[:min(int64(len(head)), n)]) {
			return c, data, nil
		}
		if step == 0 {
			first = data
		}
	}
	if capped {
		return cut, first, nil
	}
	return 0, nil, nil
}

// capCut returns the entire excess, aligned to filesystem blocks. There may be
// less than one block above the effective limit when the minimum tail wins.
func (lt *logTailer) capCut(size, block int64) int64 {
	if lt.pruneMaxBytes <= 0 {
		return 0
	}
	over := size - max(lt.pruneMaxBytes, lt.pruneKeep)
	if over <= 0 {
		return 0
	}
	cut := min(over+block-1, size-lt.pruneKeep)
	return cut - cut%block
}

// A cut may split a physical line. When skipping unread bytes, start after that
// fragment so it cannot acquire "now" and pull old records' event timestamps forward.
func firstCompleteLineOffset(path string) int64 {
	data, err := readFileRange(path, 0, logAgeProbeBytes)
	if err != nil {
		return 0
	}
	if nl := bytes.IndexByte(data, '\n'); nl >= 0 {
		return int64(nl + 1)
	}
	return 0
}

func (lt *logTailer) capWithoutCursor(fi os.FileInfo, cause error) error {
	// Disk protection must still work when a full disk prevents cursor access.
	if cut := lt.capCut(fi.Size(), fileBlockSize(fi)); lt.pruneOn && cut > 0 {
		if err := collapseFileStart(lt.path, cut); err != nil {
			return fmt.Errorf("log cursor: %v; emergency size cap: %w", cause, err)
		}
		log.Printf("projector: WARNING: fsm log emergency size cap removed %d bytes without a durable cursor; retained log may be replayed: %v", cut, cause)
	}
	return cause
}

// fileBlockSize is the filesystem block a cut must be a multiple of.
func fileBlockSize(fi os.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Blksize > 0 {
		return int64(st.Blksize)
	}
	return 4096
}

func fileIdentity(fi os.FileInfo) (dev, inode uint64) {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev), uint64(st.Ino)
	}
	return 0, 0
}

// firstStampedMS is the time of the first dated line starting after offset, if one starts
// within logAgeProbeBytes. Clock-only lines are skipped: their day is a guess.
func firstStampedMS(path string, offset int64) (int64, bool, error) {
	data, err := readFileRange(path, offset, logAgeProbeBytes)
	if err != nil {
		return 0, false, err
	}
	// offset may be partway into a line, so the first full line starts after a newline.
	for nl := bytes.IndexByte(data, '\n'); nl >= 0; nl = bytes.IndexByte(data, '\n') {
		data = data[nl+1:]
		end := bytes.IndexByte(data, '\n')
		if end < 0 {
			break
		}
		if ln := parseLogLine(string(data[:end]), nil, 0); ln.tsSource == "line" {
			return ln.loggedAtMS, true, nil
		}
	}
	return 0, false, nil
}

func readFileRange(path string, offset, length int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	buf := make([]byte, length)
	n, err := io.ReadFull(io.NewSectionReader(f, offset, length), buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return buf[:n], nil
}

// fileHeadHash hashes up to n bytes from the start of a file, and says how many there were.
func fileHeadHash(path string, n int64) (string, int64, error) {
	data, err := readFileRange(path, 0, n)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), int64(len(data)), nil
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

// deliveredCursor is how far omega has shipped a telemetry table, in its own read order:
// every row with (event_ts, rowid) at or before (tsRaw, rowID).
type deliveredCursor struct {
	tsRaw string
	rowID int64
}

// omegaDelivered reads how far the omega owning statePath has delivered table.
//
// ok=false means it vouches for nothing, and why. That is the answer whenever the cursor
// cannot be taken at its word: it moves on the broker's PUBACK, but a table the platform has
// paused is written to a local buffer instead and the cursor moves anyway.
//
// A PUBACK is the broker taking the batch, not the platform storing it — a batch the worker
// rejects is still acked, and the device is never told.
func omegaDelivered(ctx context.Context, statePath, table string) (deliveredCursor, bool, string, error) {
	if _, err := os.Stat(statePath); err != nil {
		return deliveredCursor{}, false, "", fmt.Errorf("omega state %s: %w", statePath, err)
	}
	db, err := sql.Open("sqlite", "file:"+statePath+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return deliveredCursor{}, false, "", fmt.Errorf("open omega state: %w", err)
	}
	defer db.Close()

	var tsRaw, rowID sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT (SELECT value FROM meta WHERE key = ?), (SELECT value FROM meta WHERE key = ?)`,
		"telemetry_ts_raw:"+table, "telemetry_src_rowid:"+table,
	).Scan(&tsRaw, &rowID); err != nil {
		return deliveredCursor{}, false, "", fmt.Errorf("read omega cursor: %w", err)
	}
	if !tsRaw.Valid || !rowID.Valid || tsRaw.String == "" {
		return deliveredCursor{}, false, "omega has not delivered any " + table + " rows yet", nil
	}
	id, err := strconv.ParseInt(rowID.String, 10, 64)
	if err != nil {
		return deliveredCursor{}, false, "", fmt.Errorf("omega cursor rowid %q: %w", rowID.String, err)
	}

	var buffered, paused int64
	if err := db.QueryRowContext(ctx,
		`SELECT (SELECT COUNT(*) FROM buffer WHERE source_table = ?),
		        (SELECT COUNT(*) FROM lineage_pause WHERE source_table = ?)`, table, table,
	).Scan(&buffered, &paused); err != nil {
		return deliveredCursor{}, false, "", fmt.Errorf("read omega buffer: %w", err)
	}
	if paused > 0 {
		return deliveredCursor{}, false, "the platform has paused " + table, nil
	}
	if buffered > 0 {
		return deliveredCursor{}, false,
			fmt.Sprintf("omega is holding %d %s batch(es) in its local buffer", buffered, table), nil
	}
	return deliveredCursor{tsRaw: tsRaw.String, rowID: id}, true, "", nil
}

// PruneFSMLogs deletes fsm_logs rows that are either older than retention AND delivered, or
// older than maxAge whatever their state. delivered == nil means nothing is known to be
// delivered, so only maxAge applies.
//
// Deleted in chunks, so a large first prune does not hold the write lock for the controller-
// facing projector — or the agent reading the replica — for one long transaction.
func (w *replicaWriter) PruneFSMLogs(ctx context.Context, now time.Time,
	retention, maxAge time.Duration, delivered *deliveredCursor) (int64, error) {

	var exists int
	if err := w.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, fsmLogsTable,
	).Scan(&exists); err != nil || exists == 0 {
		return 0, err
	}

	var conds []string
	var args []any
	if maxAge > 0 {
		conds = append(conds, `event_ts < ?`)
		args = append(args, eventTS(now.Add(-maxAge).UnixMilli()))
	}
	if delivered != nil && retention > 0 {
		// The agent reads in (event_ts, rowid) order, so "delivered" is at or before its cursor
		// in that order — not merely id <= rowid.
		conds = append(conds, `(event_ts < ? AND (event_ts < ? OR (event_ts = ? AND id <= ?)))`)
		args = append(args, eventTS(now.Add(-retention).UnixMilli()),
			delivered.tsRaw, delivered.tsRaw, delivered.rowID)
	}
	if len(conds) == 0 {
		return 0, nil
	}
	where := strings.Join(conds, " OR ")

	const chunk = 5000
	var total int64
	for {
		res, err := w.db.ExecContext(ctx,
			`DELETE FROM fsm_logs WHERE id IN (SELECT id FROM fsm_logs WHERE `+where+
				fmt.Sprintf(` LIMIT %d)`, chunk), args...)
		if err != nil {
			return total, fmt.Errorf("prune fsm_logs: %w", err)
		}
		n, _ := res.RowsAffected()
		total += n
		if n < chunk {
			return total, nil
		}
	}
}
