//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// falloc_fl_collapse_range from linux/falloc.h: remove a range and shift the rest down.
const fallocCollapseRange = 0x08

// collapseFileStartOS removes the first n bytes of path with fallocate(COLLAPSE_RANGE). n must
// be a multiple of the filesystem block and less than the file size. The file is opened for
// writing only because fallocate requires it; nothing is written through it.
func collapseFileStartOS(path string, n int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	if err := syscall.Fallocate(int(f.Fd()), fallocCollapseRange, 0, n); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOSYS) {
			return fmt.Errorf("%w: %v", errCollapseUnsupported, err)
		}
		return fmt.Errorf("collapse %d bytes of %s: %w", n, path, err)
	}
	return nil
}
