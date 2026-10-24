//go:build linux

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestFSMLogPruneRealCollapseWithConcurrentWriter runs the real fallocate against a writer
// appending the way the controller does — its own O_APPEND descriptor, one write per line — and
// checks every line lands in fsm_logs exactly once while the file is cut under it.
//
// Needs a Linux filesystem that supports COLLAPSE_RANGE, so it is opt-in: set
// FSM_LOG_REAL_COLLAPSE=1 and run it on the machine, in a directory on the same filesystem as
// fsm.log (TMPDIR).
func TestFSMLogPruneRealCollapseWithConcurrentWriter(t *testing.T) {
	if os.Getenv("FSM_LOG_REAL_COLLAPSE") == "" {
		t.Skip("set FSM_LOG_REAL_COLLAPSE=1 to run against the real filesystem")
	}
	f := newLogFixture(t)
	f.appendSeq(0, 1) // the file must exist before the tail starts

	w, err := os.OpenFile(f.log, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer w.Close()
	var written atomic.Int64
	written.Store(1)
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); !stop.Load(); i++ {
			line := logAt(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("seq %d %090d", i, 0)) + "\n"
			if _, err := w.WriteString(line); err != nil {
				t.Errorf("write: %v", err)
				return
			}
			written.Store(i + 1)
			if i%50 == 0 {
				time.Sleep(time.Millisecond) // ~ thousands of lines a second, far past the idle loop
			}
		}
	}()

	lt, closeTailer := f.pruningTailer(base.Add(time.Hour), 0)
	defer closeTailer()
	lt.pruneKeep, lt.pruneStep = 64<<10, 256<<10
	ctx := context.Background()
	cuts := int64(0)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := lt.Drain(ctx, false); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if lt.cur.base > cuts {
			cuts = lt.cur.base
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	if err := lt.Drain(ctx, true); err != nil {
		t.Fatalf("final drain: %v", err)
	}

	n := int(written.Load())
	t.Logf("%d lines written, %d bytes pruned under the writer, file now %d bytes",
		n, lt.cur.base, fileSize(t, f.log))
	if lt.cur.base == 0 {
		t.Fatal("nothing was pruned; the test did not exercise the cut")
	}
	f.assertEverySeqOnce(n)
}

// Exercise the emergency cap with a real O_APPEND writer faster than normal
// ingestion, rather than only the lossless age-cut path above.
func TestFSMLogRealSizeCapWithFloodingWriter(t *testing.T) {
	if os.Getenv("FSM_LOG_REAL_COLLAPSE") == "" {
		t.Skip("set FSM_LOG_REAL_COLLAPSE=1 to run against the real filesystem")
	}
	f := newLogFixture(t)
	f.appendSeq(0, 1)
	before, err := os.Stat(f.log)
	if err != nil {
		t.Fatal(err)
	}
	w, err := os.OpenFile(f.log, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		line := logAt(base, "flood "+strings.Repeat("x", 512)) + "\n"
		burst := strings.Repeat(line, (128<<10)/len(line))
		for !stop.Load() {
			if _, err := w.WriteString(burst); err != nil {
				t.Errorf("append during cap: %v", err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	defer func() { stop.Store(true); wg.Wait(); w.Close() }()
	lt, closeTailer := f.pruningTailer(base, time.Minute)
	defer closeTailer()
	lt.pruneMaxBytes, lt.pruneKeep, lt.pruneStep = 1<<20, 64<<10, 4096
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		drainAt(t, lt, base, false)
		time.Sleep(100 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	f.appendLines(logAt(base.Add(time.Minute), "after real flooding cap"))
	drainAt(t, lt, base.Add(2*time.Minute), true)
	if got := fileSize(t, f.log); got > lt.pruneMaxBytes || got < lt.pruneKeep || lt.cur.base == 0 {
		t.Fatalf("size=%d cap=%d keep=%d removed=%d", got, lt.pruneMaxBytes, lt.pruneKeep, lt.cur.base)
	}
	after, err := os.Stat(f.log)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("size cap replaced the inode under the controller's open writer")
	}
	if n := scalarInt(t, openRO(t, f.replicaPath), `SELECT COUNT(*) FROM fsm_logs WHERE message='after real flooding cap'`); n != 1 {
		t.Fatalf("fresh rows after real cap = %d, want 1", n)
	}
	t.Logf("removed %d bytes; retained %d bytes under a %d byte cap", lt.cur.base, fileSize(t, f.log), lt.pruneMaxBytes)
}
