module canebot-projector

go 1.25.5

// The projector reads two things from the controller's own source tree:
//
//	fsm  GetStepDescription — the authoritative step-title map, so step_title is derived
//	     from firmware metadata rather than a second copy that drifts from it
//	io   the digital-input definitions (X0.0 main door, X0.7 CIP bypass) and their
//	     stable bit order, which sensors_bits encodes against
//
// It is wired by path rather than vendored precisely so there is no second copy. Both
// repos are checked out side by side on the build machine; keep them that way, or the
// build fails loudly instead of silently compiling against a stale copy.
require (
	canebot-fsm v0.0.0
	modernc.org/sqlite v1.46.1
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/sys v0.37.0 // indirect
	modernc.org/libc v1.67.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace canebot-fsm => ../CaneBot_FSM_go
