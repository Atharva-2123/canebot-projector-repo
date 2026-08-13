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
- Five tables have no writer yet: `cip_runs`, `hourly_rollups`, `hourly_fault_counts`,
  `hourly_step_stats`, `daily_rollups`.
- The omega binary is the omnibus build (36 MB). A slim client build is currently impossible
  because `sqlite-replication` is missing from omega's `modules/modulemap.go`, though it is
  present in `registry.go` — the two have drifted.
