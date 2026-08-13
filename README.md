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
projector/
  projector                 the binary
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

## Install

```bash
git clone https://github.com/Atharva-2123/canebot-projector-repo.git
cd canebot-projector-repo

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

## Notes

- `projector_state.db` and `omega/state.db` are **separate on purpose**. Neither belongs in a
  sync policy; omega auto-enrols any table it finds in the database it is pointed at.
- Back both up alongside the replica. Losing them causes duplicate publishes.
- All 17 tables are populated. `cip_runs` comes from Maintenance spans; the four rollups are
  aggregated from the replica's own finished tables once a bucket has closed.
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
