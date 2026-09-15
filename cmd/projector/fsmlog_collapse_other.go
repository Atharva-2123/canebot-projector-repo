//go:build !linux

package main

// collapseFileStartOS: only Linux can cut a file's start in place. The projector runs on the
// machine (Android/Linux); elsewhere pruning reports itself off rather than doing anything else.
func collapseFileStartOS(path string, n int64) error { return errCollapseUnsupported }
