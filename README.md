# CaneBot projector + omega

Replicates the CaneBot machine's data to ilyama for dashboards.

```
config.db  ──read-only──▶  projector  ──▶  canebot_replica.db  ──▶  omega  ──▶  ilyama
(the machine's own)                        (dashboard-shaped)
```

The controller is never modified, rebuilt or redeployed. A test asserts its SHA-256 is
unchanged after every projector run.

## Layout

```
cmd/projector/              the source — built with `make projector`
  main.go                   orchestration: collect, derive, flush
  reader.go                 read-only access to the controller's config.db
  tracker.go                the derivation: intervals, dwells, faults, synthetic keys
  writer.go                 replica writes and column migrations
  schema.go                 the DDL for both databases
  rollup.go                 hourly and daily aggregates
  sensors.go                sensor snapshot -> sensors_bits
  state.go                  read cursors and watermarks

projector/
  projector                 the binary — built, not committed      (gitignored)
  canebot_replica.db        output — what omega replicates      (runtime, gitignored)
  projector_state.db        read cursors and watermarks         (runtime, gitignored)

omega/
  omega                     the binary
  omega-config.yaml         which tables to replicate and how
  omega.env.example         template — copy to omega.env
  certs/                    ca.crt, device.crt, device.key       (secrets, gitignored)
  state.db                  omega's own cursors                  (runtime, gitignored)

linux/
  install-services.sh       install both as systemd services
  manage-services.sh        start / stop / logs / check

db-overview.md              every table and column, and where each comes from
```

## Build

The projector is built from source on the machine that runs it. The binary is no longer
committed — a rebuild used to add a fresh multi-megabyte blob to git history every time.

It needs **Go >= 1.25.5** and the controller's source tree **as a sibling directory**:

```
canebot-projector-repo/
CaneBot_FSM_go/
```

That is not incidental. The projector imports the controller's own step metadata
(`fsm.GetStepDescription`) and IO pin definitions (`io.InputMainDoorSwitch` = X0.0,
`io.InputCIPBypassSwitch` = X0.7) so that step titles and sensor bit order come from
firmware rather than a second copy that drifts from it. If the sibling is missing, `make`
stops and tells you.

```bash
make projector      # -> projector/projector
make test
make vet
```

If your checkout is laid out differently, either adjust the `replace` directive in
`go.mod`, or create a local `go.work` (gitignored) pointing at the real path.

## Install

```bash
git clone https://github.com/Atharva-2123/canebot-projector-repo.git
cd canebot-projector-repo
make projector

# 1. projector — safe to start immediately, it only reads
sudo ./linux/install-services.sh
sudo systemctl start canebot-projector

# 2. check it is producing
./linux/manage-services.sh check

# 3. omega — needs certs and device details first
cp omega/omega.env.example omega/omega.env   # already filled in for this device
cp /path/to/{ca.crt,device.crt,device.key} omega/certs/
chmod 600 omega/certs/device.key
sudo systemctl start canebot-omega
```

If the controller's database is not at `~/CaneBot_FSM_go/config.db`:

```bash
SOURCE_DB=/actual/path/config.db sudo -E ./linux/install-services.sh
```

## Checking the omega config

Neither connects nor opens a database, so it is safe to run any time:

```bash
./omega/omega --validate --client ./omega/omega-config.yaml
```

It reports unknown modules and missing capability grants. It does **not** validate the
`sqlite-replication` section — `source_db_path` can be missing entirely and it still passes —
so a green result says nothing about the table list or column names.

## Day to day

```bash
./linux/manage-services.sh status
./linux/manage-services.sh check      # row counts, cycle outcomes, integrity
./linux/manage-services.sh logs       # both services, live
./linux/manage-services.sh restart
```

## Try it without installing

The projector opens the controller's database read-only, so this is safe on a live machine.

```bash
cd projector
./projector -source /path/to/config.db -once -v
sqlite3 -header -column canebot_replica.db \
  "SELECT order_key, result, duration_ms FROM cycles LIMIT 20;"
sha256sum /path/to/config.db          # unchanged before and after
```

## What it produces

17 tables. `cycles` is the headline — one row per drink with its duration and outcome, plus
synthetic intervals covering idle, maintenance, manual and error time so the timeline is
continuous and availability is computable. Full reference in [db-overview.md](db-overview.md).

## Controller log (`fsm_logs`)

With `-fsm-log` set, the projector also tails the controller's stdout log into `fsm_logs`, one
row per line. Off by default, and the table is not created until it is on.

```bash
./projector ... \
  -fsm-log /data/local/canebot/logs/fsm.log \
  -fsm-log-omega-state /data/local/canebot-projector-repo/omega/state-rs-ilyama2.db \
  -fsm-log-retention 48h -fsm-log-max-age 168h
```

- **Roll out omega first.** Add `fsm_logs` to `table_strategies` (see `omega/omega-config.yaml`)
  in every omega reading the replica and restart them, then turn the flag on. An omega that
  finds the table unlisted installs `rows` triggers on it.
- **Time.** The controller logs whole seconds in UTC. `logged_at_ms` is that second; `event_ts`
  adds the line's position within it in milliseconds (`.000`, `.001`, `.002`, …) so rows sort in
  log order on `event_ts` alone — including on the platform, which stores it as DateTime64(3).
  Lines without a date take it from the line before (`ts_source`).
- **Repeats.** A line repeating within `-fsm-log-digest` (1m) is written once, then summarised
  by one `ts_source='digest'` row carrying `repeat_count` and `first/last_seen_at_ms`. Errors,
  warnings and continuation lines are always written line by line. `SUM(repeat_count)` is the
  number of lines logged. On the live idle loop, ~133k lines in 90 minutes became ~2k rows.
- **Retention.** A row is deleted when it is older than `-fsm-log-retention` **and** the omega
  at `-fsm-log-omega-state` has delivered it, or when it is older than `-fsm-log-max-age`
  regardless. "Delivered" is the broker's PUBACK, not the platform storing it; while omega holds
  batches in its buffer or the platform has paused the table, nothing counts as delivered.
- **Pruning the log file** (`-fsm-log-prune-file`, needs `-fsm-log-omega-state`). Bytes are cut
  off the start of `fsm.log` once the omega at `-fsm-log-omega-state` has delivered every row that
  carries them; the last `-fsm-log-keep-mb` (8) always stay so the file still shows recent history,
  and a cut waits until at least 1MB is due, so the file sits between 8 and 9MB.
  The cut is `fallocate(COLLAPSE_RANGE)`: same inode, whole filesystem blocks, at most 8MB at a
  time. It cannot race the controller, whose descriptor is `O_APPEND` — every write lands at the
  end as the file then is — so unlike truncating it never loses a line written mid-prune. Each
  cut briefly holds the file's lock, and the controller's log writes wait for it (~5–8ms per 16MB,
  measured on the machine). The file may start partway into a line that was already delivered.
- **Without pruning the log file is never written.** Rotate it however you like — copy-then-
  truncate keeps the lines written just before the cut, because the tail reads `<log>.1` first.

## CIP cycle counting

`cip_runs` contains one record per logical cleaning cycle or interrupted attempt. Internal
interval changes do not create another cleaning cycle. A command-driven CIP loop produces
one completed record per iteration. A projector shutdown does not end the machine's clean.

The current Maintenance handler completes at step **3**. This is the default for
`-cip-completion-step`. Use `-cip-completion-step=7` only with the older seven-step handler.
The old firmware metadata still describes step 7; it does not select the deployed handler.

`completed=1` means a completion marker was recorded. `completed=0` means the machine left
Maintenance before completion. Hourly and daily machine rollups count only completed cycles,
using `ended_at_ms` to select the completion hour/day. Buffered closing steps delay rollup
finalisation, so an hour is not permanently written as zero before its completion arrives.

This update requires no database schema change or cursor reset. It continues from the existing
CIP cursor and writes future records through the normal projector path. Existing CIP records
and rollups remain unchanged. Correcting historical totals is a separate operation; do not
rebuild records or reset any database as part of a binary-only deployment.

The regression fixture in `cmd/projector/testdata/cip-fragments-20261014.json` is a small
read-only export. Local replay converts its 31 Maintenance fragments into six completed cycles
and two interrupted attempts. Tests create all working databases in temporary directories.

## Notes

- `projector_state.db` and `omega/state.db` are **separate on purpose**. Neither belongs in a
  sync policy; omega auto-enrols any table it finds in the database it is pointed at.
- Back both up alongside the replica. Losing them causes duplicate publishes.
- `cip_runs` comes from logical Maintenance cycles and their completion steps. Rollups are
  aggregated from the replica's own finished tables once a bucket has closed.
- **A table created empty and populated later is silently skipped by omega.** Reading zero rows
  marks its snapshot done, and thereafter the cursor is fast-forwarded past everything that
  arrives — no error, no log line. It bit all four rollups. Recovery is to stop omega, clear
  that table's state, and restart:
  ```bash
  sqlite3 omega/state.db "
  DELETE FROM meta WHERE key IN ('telemetry_snapshot_done:<t>','telemetry_src_rowid:<t>','telemetry_ts_raw:<t>');
  UPDATE telemetry_seq SET cursor_ts=NULL, cursor_row_id=NULL WHERE source_table='<t>';"
  ```
  Only ever for a table that has genuinely shipped nothing — doing it to one that has resends
  everything. Symptom: rows in the replica, table absent in the cloud, nothing in the log.
- Nine columns are in the replica but deliberately not replicated — the sensor snapshots and
  actuator blobs on `step_dwells`, and `step_title` wherever it appears (the frontend derives
  it from `(state, step)`). `fsm_events` ships `sensors_bits`, a 32-character encoding of the
  same booleans as `sensors_json`, instead of the JSON. See db-overview.md §9.
- The omega binary is the **Go** omnibus build (36 MB). A slim client build is currently
  impossible because `sqlite-replication` is missing from omega's `modules/modulemap.go`,
  though it is present in `registry.go` — the two have drifted.
- The Rust agent (`Projects/omega-rs`, `main`) is a drop-in alternative: it registers
  `sqlite-replication` directly in `omega-agent/src/main.rs`, publishes the same
  `sync/telemetry/batch` topics, and validates this same config file unchanged. It has to be
  built on the device (`cargo build --release -p omega-agent`) because `rusqlite` is `bundled`
  and `rustls` uses `ring`, which need a full C cross-toolchain from macOS.
- The unit passes `--client` with two dashes. Go's `flag` accepts both forms; the Rust agent
  rejects `-client` outright, so the two-dash form is the one that works either way.
