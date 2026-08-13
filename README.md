# CaneBot projector

Reads the CaneBot controller's SQLite database **read-only** and writes a second,
dashboard-shaped database for edge replication to ilyama.

```
config.db ──ro──▶ projector ──▶ canebot_replica.db ──▶ omega ──▶ ilyama ──▶ dashboards
(the machine's)                 (ours)
```

The controller is never modified, rebuilt, or redeployed. A test asserts its SHA-256 is
unchanged after every run.

## Quick start

```bash
uname -m          # x86_64 -> amd64,  aarch64 -> arm64

./bin/projector-linux-amd64 \
  -source /path/to/config.db \
  -replica ./canebot_replica.db \
  -state ./projector_state.db \
  -once -v
```

Then check the output and confirm the source is untouched:

```bash
sqlite3 -header -column canebot_replica.db \
  "SELECT order_key, result, duration_ms, recipe_id FROM cycles LIMIT 20;"
sqlite3 canebot_replica.db "PRAGMA foreign_key_check;"    # empty = clean
sha256sum /path/to/config.db                              # same before and after
```

Install as a service: see [deploy/DEPLOY.md](deploy/DEPLOY.md).

## What it produces

17 tables. `cycles` is the headline — one row per drink with its duration and outcome, plus
synthetic intervals covering idle, maintenance, manual and error time so the timeline is
continuous. Full column-by-column reference in [db-overview.md](db-overview.md).

## Two files, two purposes

| File | Role |
|---|---|
| `canebot_replica.db` | What omega replicates. Pruned after 14 days — a buffer, not an archive. |
| `projector_state.db` | Our cursors and watermarks. **Never** add this to a sync policy. Back it up alongside the replica. |

## Status

- 25 tests passing; verified end to end against fabricated data
- **Not yet run against a real machine database**
- Five tables have no writer yet: `cip_runs`, `hourly_rollups`, `hourly_fault_counts`,
  `hourly_step_stats`, `daily_rollups`

## Source

Lives in `CaneBot_FSM_go/cmd/projector/`. It imports the firmware's own `fsm` and `io`
packages — step titles come from `fsm.GetStepDescription()` and the door/reset inputs from
`io.InputMainDoorSwitch` / `io.InputCIPBypassSwitch` — so the labels and addresses cannot
drift from the machine. Building it standalone requires either that module or a copy of those
constants.

Build all targets:

```bash
cd CaneBot_FSM_go
for t in linux/amd64 linux/arm64 darwin/arm64 windows/amd64; do
  GOOS=${t%/*} GOARCH=${t#*/} CGO_ENABLED=0 \
    go build -ldflags "-s -w" -o bin/projector-${t%/*}-${t#*/} ./cmd/projector/
done
```
