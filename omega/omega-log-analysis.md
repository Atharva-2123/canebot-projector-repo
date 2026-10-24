# CaneBot omega log analysis

Pulled from the Pi on **2026-09-04 ~13:41 UTC** (IST evening).

| Item | Path |
|------|------|
| Live log | `/data/local/canebot/logs/omega.log` (211 696 bytes, 2 129 lines, some NULs/ANSI) |
| Cleaned copy | `~/Desktop/omega-log-cleaned.txt` |
| Supervisor | `/data/local/canebot/logs/supervisor.log` |
| Config | `/data/local/canebot/omega-config.yaml` |
| Binary | Rust `omega-agent 169ee19` (not Go) |
| Replica it reads | `/data/local/canebot-projector-repo/projector/canebot_replica.db` |
| Omega cursors | `/data/local/canebot-projector-repo/omega/state-rs.db` |

Times in **omega.log** are **UTC**. Supervisor timestamps are **IST** (UTC+5:30). Example: supervisor `18:17` IST = `12:47` UTC.

Heartbeat was turned off at **2026-09-04 18:17 IST / 12:47 UTC**: `modules.required: [sqlite-replication]`, `heartbeat: false`. Omega was SIGTERM’d (`rc=143`) and restarted on the new config.

There are **zero** `heartbeat` strings in `omega.log` (this binary does not log heartbeat pulses).

---

## Bottom line

Omega **starts**, talks MQTT, then the broker (or the path to it) sends `Disconnect`. Replication then fails with `transport not connected`. That pattern is the same **before and after** heartbeat was disabled.

It **did not** log a clean “sent batch / publish ok” line. Cursors in `state-rs.db` **did** move (it *tried* to replicate, including today’s drinks). `buffer` and `pending_upload` are **0**, so nothing is queued to retry. Cloud almost certainly does **not** have a reliable copy of those orders.

---

## Counts (full `omega.log` on disk)

This file is a **rolling history** (20 Aug → 4 Sep), not only today.

| What | Count | Meaning |
|------|------:|---------|
| `Invalid incoming packet = Disconnect` | **1543** | Broker closed MQTT; rumqttc |
| `Error: Network timeout` | **420** | Process died / connect timed out |
| `flush_all_tables failed … transport not connected` | **34** | sqlite-replication could not publish |
| `Error: start sqlite-replication` | **22** | Module start failed because MQTT was already dead |
| `omega-agent … started` | **33** | Process (re)starts |
| `Error: Read-only file system (os error 30)` | **5** | Very old; `/system` or similar RO |
| `failed to lookup address information: Try again` | **4** | DNS for `mqtt.ilyama.golain.io` failed |
| `Unsolicited puback packet` | **2** | Broker ACKed a publish omega did not expect — **only hard evidence a packet was sent** |
| Heartbeat log lines | **0** | |

### MQTT disconnects by hour (UTC)

| Hour (UTC) | Disconnects |
|------------|------------:|
| 2026-08-20 11–13 | 118 |
| 2026-08-25 07–09 | 200 |
| 2026-09-03 09–19 | 766 |
| 2026-09-04 08–10 | 128 |
| 2026-09-04 11 | 107 |
| 2026-09-04 12 (includes heartbeat off at 12:47) | **151** |
| 2026-09-04 13 (through 13:38) | 73 |

First timestamped disconnect in this file: **2026-08-25T07:37:13Z** (Aug 20 block has starts/timeouts without always a UTC stamp). Last: **2026-09-04T13:38:20Z**.

---

## Why it failed (root cause)

1. **MQTT session is unstable**  
   `rumqttc::state: Invalid incoming packet = Disconnect` means the client got a MQTT DISCONNECT (or a packet it treats as one) while it thought the session was up. Broker: `ssl://mqtt.ilyama.golain.io:8883`.

2. **Replication cannot run without a live transport**  
   ```
   WARN  omega_sqlite_replication: sqlite replication flush_all_tables failed
         error=transport not connected
         db=/data/local/canebot-projector-repo/projector/canebot_replica.db
   ```
   Then often: `Error: start sqlite-replication` / `Caused by: transport not connected`.

3. **Crash loop (supervisor)** when omega treats that as fatal (`rc=1`), restart in 10s.

4. **Not heartbeat.** Same Disconnect + flush-fail after heartbeat was removed. No new error type appeared.

Secondary / old:

- DNS `Try again` — broker hostname not resolving (4 times).
- `Read-only file system` — 5 times at the top of the log (legacy).
- Pi **clock jumped to 20 Aug** after a reboot today; MQTT timestamps can look “old” until the clock was set back to 4 Sep.

---

## Supervisor: how many times omega died (4 Sep, this log file)

`supervisor.log` was truncated/replaced around **18:12 IST** when the stack was started by hand. It only covers that afternoon, not Aug 20 / Sep 3.

| IST | Event |
|-----|--------|
| 18:12:18 | first omega start |
| 18:12:25 | **exited rc=1** (~7s) |
| 18:12:36 | start |
| 18:13:08 | **rc=1** (~32s) |
| 18:13:18 | start |
| 18:13:52 | **rc=1** (~34s) |
| 18:14:02 | start |
| 18:14:10 | **rc=1** (~8s) |
| 18:14:20 | start |
| 18:14:53 | **rc=1** (~33s) |
| 18:15:03 | start |
| 18:15:10 | **rc=1** (~7s) |
| 18:15:20 | start |
| 18:16:18 | **rc=1** (~58s) |
| 18:16:28 | start |
| 18:17:11 | **rc=1** (~43s) |
| 18:17:21 | start |
| 18:17:43 | **rc=143** — SIGTERM when we restarted omega for heartbeat-off config |
| 18:17:53 | start on **new config** (heartbeat off) |

**Crash-loop deaths: 8 × `rc=1` in ~5 minutes, then 1 × `rc=143` (us).**  
After 18:17:53 IST this supervisor file does **not** show further `omega exited`. The process stayed up and logged Disconnects instead of exiting. Later reboot: omega pid was live again (e.g. 6728) with the same MQTT errors.

---

## After turning heartbeat off

Config change (applied ~12:47 UTC):

```yaml
modules:
  required: [sqlite-replication]   # was [sqlite-replication, heartbeat]
security:
  capabilities:
    sqlite-replication: true
    heartbeat: false               # was true
```

| | Before (~Sep 4 08:00–12:47Z) | After (12:47Z–13:38Z) |
|--|-------------------------------|------------------------|
| Heartbeat in logs | none | none |
| MQTT Disconnect | 347 on Sep 4 before cut | **112** (shorter window) |
| `flush_all_tables` fail | 17 (all-time before cut) | **17** after cut (same failure) |
| New error types | — | **none** |
| Process | crash-loop `rc=1` | **stays running**, MQTT still dies |
| Replication | fails | **still fails** |

Disconnects continued every 1–8 minutes through the `ORD-0caee161b954` drink (12:57–12:58Z) and after.

---

## Did it send data when it “came back”?

**No successful-send log line exists** (no INFO publish/ack/batch). What we can prove:

### Evidence it *tried*

Omega `telemetry_seq` after the afternoon (cursor = last row it considered):

| Table | `next_batch_seq` | `cursor_ts` |
|-------|-----------------:|-------------|
| cycles | 39 | 2026-09-04T13:30:42.126Z |
| step_dwells | 135 | 2026-09-04T13:20:21.816Z |
| state_durations | 36 | 2026-09-04T13:30:42.126Z |
| rollups | 19 | 2026-09-04T13:00:00Z |
| sensor_toggles | 37 | 2026-09-04T13:05:21.921Z |
| door_events | 7 | 2026-09-04T13:03:31.721Z |
| actuator_intervals | 6 | 2026-09-04T13:05:21.928Z |
| gaps | 10 | 2026-09-04T13:36:21.83Z |
| fault_events | 5 | 2026-09-04T11:52:04.403Z |
| cip_runs | 3 | 2026-08-20T13:15:06Z |
| config_history | 2 | 2026-08-20T13:15:08Z |

`buffer = 0`, `pending_upload = 0`.

### Evidence the cloud likely *did not* get a clean copy

Flush failures **on the drinks**:

| UTC | What was happening |
|-----|-------------------|
| 12:13:38, 12:13:59, 12:14:28 | After **ORD-4ab81a39a7d9** completed (~12:13:31Z) |
| 12:56:30 | Just before **ORD-0caee161b954** |
| 12:59:04 | Just after that order ended (~12:58:53Z) |
| 13:04–13:25 | Many more flush fails (replica rebuild / clock / reconnect noise) |

### Tiny evidence *something* hit the broker

Two lines, not today:

```
2026-08-25T08:01:06.297366Z ERROR rumqttc::state: Unsolicited puback packet: 8
2026-08-25T09:45:25.320482Z ERROR rumqttc::state: Unsolicited puback packet: 2
```

That is a PUBACK for a packet id omega did not have in flight — session/QoS mismatch, but it implies **a publish happened** at some point on 25 Aug.

---

## Timeline vs orders (4 Sep UTC)

```
12:12:12  ORD-4ab81a39a7d9 starts
12:13:31  that cycle ends in replica
12:13:38  flush_all_tables failed  ← order not sent
12:42     stack restarted (dwell-fix projector)
12:47     heartbeat disabled; omega SIGTERM + restart
12:57:35  ORD-0caee161b954 starts
12:58:53  that cycle ends
12:59:04  flush_all_tables failed  ← order not sent
13:36–13:38  still Disconnect
```

---

## Representative log lines

**Start (healthy enough to load profile):**
```
omega-agent 169ee19 started profile=CaneBot Vending Machine 1 device=CaneBot Vending Machine 1 transport=mqtt
```

**MQTT death (the 1543× line):**
```
2026-09-04T13:38:20.833403Z ERROR rumqttc::state: Invalid incoming packet = Disconnect
```

**Replication death:**
```
WARN omega_sqlite_replication: sqlite replication flush_all_tables failed
     error=transport not connected
     db=/data/local/canebot-projector-repo/projector/canebot_replica.db
Error: start sqlite-replication
Caused by:
    transport not connected
```

**Process death (no UTC stamp):**
```
Error: Network timeout
```

---

## What to look at next (not done here)

1. Broker / ilyama side: why DISCONNECT (session takeover, packet size, TLS, another client with same `device_id`).
2. Whether `clean_session: false` is replaying a poisoned session (old Go log already warned about this).
3. Do **not** treat omega cursors as “cloud has the row” until flush succeeds without `transport not connected`.

Full verbatim log (NUL/ANSI stripped): `~/Desktop/omega-log-cleaned.txt`.
