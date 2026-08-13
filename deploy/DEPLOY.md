# Deploying the CaneBot projector (Linux)

## 1. Which binary

```bash
uname -m
# x86_64  -> projector-linux-amd64
# aarch64 -> projector-linux-arm64
```

## 2. Try it first, without installing anything

The projector opens the controller's database **read-only**, so this is safe to run on a live
machine. Better still, run it against a copy.

```bash
./projector-linux-amd64 \
  -source /path/to/config.db \
  -replica ./canebot_replica.db \
  -state ./projector_state.db \
  -once -v
```

Then look at what came out:

```bash
sqlite3 -header -column canebot_replica.db \
  "SELECT order_key, result, duration_ms, recipe_id, fault_count FROM cycles LIMIT 20;"
sqlite3 canebot_replica.db "PRAGMA foreign_key_check;"   # empty output = clean
```

Confirm the source was untouched:

```bash
sha256sum /path/to/config.db   # before and after — must match
```

## 3. Install as a service

```bash
sudo useradd --system --no-create-home canebot
sudo mkdir -p /opt/canebot /var/lib/canebot
sudo cp projector-linux-amd64 /opt/canebot/projector
sudo chown -R canebot:canebot /var/lib/canebot
# the controller's database must be readable by the canebot user
sudo chmod o+r /path/to/config.db

sudo cp canebot-projector.service /etc/systemd/system/
sudo nano /etc/systemd/system/canebot-projector.service   # set the real -source path
sudo systemctl daemon-reload
sudo systemctl enable --now canebot-projector
journalctl -u canebot-projector -f
```

## 4. Watch the first hour

```bash
# Is it keeping up?
watch -n5 'sqlite3 /var/lib/canebot/canebot_replica.db "SELECT COUNT(*) FROM cycles;"'

# Any gaps recorded? (should stay empty while it runs)
sqlite3 /var/lib/canebot/canebot_replica.db "SELECT * FROM gaps;"

# Which firmware schema did it detect?
sqlite3 /var/lib/canebot/canebot_replica.db "SELECT source_branch, note FROM projector_runs;"
```

Also watch the **controller's** own logs. The source runs in rollback-journal mode, so our
reads can briefly delay its writes. Each read is a small batch closing in milliseconds and the
controller has a 5s busy timeout, so it should be invisible — but this is the first time that
has been tested against a live machine.

## Flags

| Flag | Default | Notes |
|---|---|---|
| `-source` | `./config.db` | The controller's database. Read-only. |
| `-replica` | `./canebot_replica.db` | What omega watches. |
| `-state` | `./projector_state.db` | Our bookkeeping. **Never** add to a sync policy. |
| `-interval` | `5s` | Poll interval. |
| `-batch` | `500` | Max source rows per table per tick. Lower it if you see lock contention. |
| `-retention` | `336h` (14d) | How long rows stay in the replica before pruning. |
| `-once` | off | Single pass then exit. Use for the first look and for backfill. |
| `-v` | off | Verbose logging. |

## Important

- `projector_state.db` must stay **out** of the omega sync policy. It lives in a separate file
  precisely so the agent cannot discover and enrol it.
- Back up `projector_state.db` with the replica. Losing it causes duplicate publishes.
- Five tables (`cip_runs`, `hourly_rollups`, `hourly_fault_counts`, `hourly_step_stats`,
  `daily_rollups`) have no writer yet and will stay empty.
