//go:build linux

package main

import (
	"context"
	"fmt"
	"os"
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
	omega := newOmegaState(t, "", 0, false, false)
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

	lt, closeTailer := f.pruningTailer(base.Add(time.Hour), 0, omega)
	defer closeTailer()
	lt.pruneKeep, lt.pruneStep = 64<<10, 256<<10
	ctx := context.Background()
	cuts := int64(0)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := lt.Drain(ctx, false); err != nil {
			t.Fatalf("drain: %v", err)
		}
		if lt.tableReady {
			f.deliverThrough(omega, 0)
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
