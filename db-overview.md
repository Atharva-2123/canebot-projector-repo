# CaneBot replica database — full overview

What the database is, where every table comes from, and what every column means.

- **File:** `canebot_replica.db` (demo copy: `~/Desktop/canebot-projector-demo/demo_replica.db`)
- **Written by:** the projector (`CaneBot_FSM_go/cmd/projector/`)
- **Read by:** the omega edge agent, which replicates each table to ilyama as
  `edge_ts_p<project8>_<table>`
- **17 tables**

---

## 1. The two databases

| | `config.db` — the controller's | `canebot_replica.db` — ours |
|---|---|---|
| Created by | the machine's firmware | the projector |
| Written by | the firmware only | the projector only |
| We | read it, `mode=ro` — never write | write it freely |
| Firmware | writes it constantly | doesn't know it exists |
| Purpose | running the machine | shipping data out |
| Contains | raw events as they happen | finished facts, dashboard-shaped |
| Retention | none — grows forever | pruned after ~14 days |
| If deleted | machine loses config, can't resume after a fault | re-run the projector and it rebuilds |

A test asserts `config.db`'s SHA-256 is unchanged after every projector run, so
"we don't touch the controller" is verified rather than intended.

---

## 2. What exists in the controller, and what we do with it

The controller has 7–10 tables depending on which branch it runs.

| Source table | Branch | What we do with it |
|---|---|---|
| `fsm_events` | all | Mirrored to `fsm_events`, and derived into `cycles`, `state_durations`, `step_dwells` |
| `faults` | all | Derived into `fault_events`, and sets each cycle's `result` |
| `orders` | all | Folded into `cycles` — no standalone table |
| `actuator_output_intervals` | all | Mirrored to `actuator_intervals` (closed rows only) |
| `sensor_input_toggles` | all | Mirrored to `sensor_toggles`, and derived into `door_events` |
| `config` | all | Diffed into `config_history` |
| `fsm_step_runs` | `test/tracking_fsm` | Becomes `step_dwells` + `step_actuators` when present. Its `fault_type`/`fault_message` also drive `fault_events` and each cycle's `result` when `faults` is empty |
| `fsm_resume_state` | all | **Not replicated** — operational state, single overwritten row |
| `Audit` | tracking_fsm, maintenance | **Not replicated** — only mirrors `faults` and `orders`, already covered |
| `order_checkpoint` | `test/maintenance` | **Not replicated** — single overwritten power-loss resume row |

---

## 3. The 17 replica tables, classified

| Table | Kind | Built from | Populated? |
|---|---|---|---|
| `cycles` | **derived** | `orders` + `fsm_events` + `faults` | yes |
| `step_dwells` | **derived** | `fsm_step_runs`, or `fsm_events` as fallback | yes |
| `step_actuators` | **derived** | `fsm_step_runs.actuators_json` | yes |
| `state_durations` | **derived** | `fsm_events` | yes |
| `fault_events` | **derived** | `faults`, or `fsm_step_runs.fault_type` when that ledger is empty | yes |
| `door_events` | **derived** | `sensor_input_toggles` (X0.0 + X0.7) | yes |
| `cip_runs` | **derived** | Maintenance state spans | yes |
| `fsm_events` | **raw mirror** | `fsm_events` | yes |
| `sensor_toggles` | **raw mirror** | `sensor_input_toggles` | yes |
| `config_history` | **raw-ish** | `config` (diffed — source has no history) | yes |
| `actuator_intervals` | **raw mirror** | `actuator_output_intervals` | yes |
| `hourly_rollups` | **rollup** | aggregates of the above | yes |
| `hourly_fault_counts` | **rollup** | aggregates of `fault_events` | yes |
| `hourly_step_stats` | **rollup** | aggregates of `step_dwells` | yes |
| `daily_rollups` | **rollup** | aggregates of the above | yes |
| `projector_runs` | **health** | the projector itself | yes |
| `gaps` | **health** | the projector itself | yes |

**Every one of the 17 is new.** None exists in the controller. Four are close mirrors of a
source table; the rest are computed.

---

## 4. Columns we added everywhere

These appear on most tables and exist in **no** source table.

| Column | Why it exists |
|---|---|
| `event_ts` | Fixed-width UTC text. **Always when the fact became true** (a cycle's end, a step's end), never its start. The sync cursor only moves forward, so a row written late with an earlier timestamp would never ship. |
| `*_at_ms` | The same instants as integer milliseconds. The source stores `RFC3339Nano` text, which strips trailing zeros — so its string order is *not* time order. Integers remove that hazard. |
| `order_key` | Non-null everywhere. The source leaves it null outside a drink; we assign synthetic `IDLE-`/`MAINT-`/`MANUAL-`/`ERR-` keys so every row is scopeable. |
| `hour_bucket_ms` | Timestamp floored to the UTC hour, so "group by hour" is an indexed column. |
| `date_utc` | `YYYY-MM-DD`, UTC. No shift dimension — the controller has no shift concept. |
| `is_production` | 1 = a real drink, 0 = idle/maintenance/manual/error. |
| `recipe_id`, `cycle_result` | Copied from the parent cycle so queries never join. |
| `src_id` | The originating row's `id` in the controller's database, for tracing back. |

Copying parent attributes down is safe because rows are immutable and a cycle and all its
children are written in one transaction — the copies cannot drift.

---

## 5. Raw mirrors — column by column

### `fsm_events` — the raw event stream

Nearly 1:1 with the controller's `fsm_events`.

| Column | Source | Meaning |
|---|---|---|
| `id` | added | Row number in our table |
| `event_ts` | added | Fixed-width UTC timestamp |
| `event_at_ms` | from `ts_utc` | When the event happened, as integer ms |
| `order_key` | **1:1** `order_key` | Which interval it belongs to (never null here) |
| `src_id` | from `id` | The controller's row id |
| `event_kind` | **1:1** | `state_transition`, `step_changed`, `input_changed`, `event_sent`, `event_processed` |
| `state_from` | **1:1** | State before the transition |
| `state_to` | **1:1** | State after |
| `current_state` | **1:1** | State at the time. May be `Tilter`/`Crusher` for the parallel sub-machines |
| `step_from` | **1:1** | Step before |
| `step_to` | **1:1** | Step after |
| `modbus_order_reg` | **renamed** from `order_id` | Raw Modbus trigger register value. **Unrelated to `order_key`** — renamed so nobody joins on it by mistake |
| `input_id` | **1:1** | Which sensor changed |
| `input_value` | **1:1** | What it changed to |
| `event_type` | **1:1** | Free-text event label |
| `source` | **1:1** | Which firmware function emitted it |
| `trace_id` | **1:1** | Correlation id, usually empty |
| `payload_json` | **1:1** | Event-specific extra detail |
| `sensors_json` | **1:1** | Snapshot of all 32 digital inputs at that instant. **Local only — not replicated**, see `sensors_bits` |
| `sensors_bits` | **computed** | The same 32 booleans as one `0`/`1` character each. This is the column that ships |
| `is_production`, `hour_bucket_ms`, `date_utc` | added | See §4 |

> **Why `sensors_bits` exists.** The firmware's snapshot is a JSON object keyed by input tag —
> `{"X0.0":true,…}` — about 410 bytes, and roughly double that on the wire once escaped as a
> string inside the replication payload. That is ~205 bytes per bit, and across ~700k events it
> was the single largest thing in the pipeline. `sensors_bits` carries the identical
> information in 32 characters with nothing to escape.
>
> **Bit order is the contract.** Bit *i* is the *i*-th input of `io.AllDigitalInputs()` sorted
> numerically by `(byte, bit)` — `X0.0, X0.1, … X0.15, X1.0, …` — deliberately **not** the
> lexicographic order of the tag strings, which would put `X0.10` before `X0.2`. A consumer
> reproduces it by applying the same numeric sort to its own tag list; the fixed 32-character
> width is what makes a mismatched list detectable instead of silently misaligned. An empty
> string means *no snapshot was recorded*, which is not the same as every input reading low.
>
> The raw `sensors_json` stays in the replica for local inspection — it is what the bits are
> checked against — it simply is not shipped.
>
> On upgrade the column is added by `ALTER TABLE`, which leaves existing rows NULL. Since
> `sensors_json` is no longer replicated, those rows would otherwise reach the cloud carrying no
> sensor state at all, so the projector backfills them once at startup from the JSON already in
> the replica. Rows the agent has not yet shipped pick the bits up automatically; rows already
> sent stay as they went. Measured at ~77k rows/sec, so a full replica is a matter of seconds.

### `sensor_toggles` — one row per sensor flip

1:1 with `sensor_input_toggles`.

| Column | Source | Meaning |
|---|---|---|
| `event_at_ms` | from `ts_utc` | When it flipped |
| `src_id` | from `id` | The controller's row id |
| `input_id` | **1:1** | Input address, e.g. `X1.5` |
| `input_name` | **renamed** from `input_display_name` | Human name, e.g. "Cane At Tilter Sensor" |
| `value_from` | **1:1** | 0 or 1 before |
| `value_to` | **1:1** | 0 or 1 after |
| `current_state` | **1:1** | State at the time |
| `current_step` | **1:1** | Step at the time |
| `order_key` | **1:1** | Which interval (never null here) |

### `actuator_intervals` — each actuator ON→OFF interval

1:1 with `actuator_output_intervals`, but **only rows that have closed**. The source inserts a
row when an output turns on and updates it when it turns off; shipping it early would publish
something that later changes.

| Column | Source | Meaning |
|---|---|---|
| `started_at_ms` | from `started_ts_utc` | When the output switched on |
| `ended_at_ms` | from `ended_ts_utc` | When it switched off |
| `duration_ms` | **1:1** | How long it ran. Never reported as 0 when unknown — the row is skipped instead |
| `src_id` | from `id` | The controller's row id |
| `revision` | added | Bumped when the machine retroactively backfills fault details onto an already-shipped row |
| `output_id` | **1:1** | Coil address, e.g. `Y0.1` |
| `output_name` | **1:1** | Human name, e.g. "Crusher Motor" |
| `started_state` / `started_step` | **1:1** | Where the machine was when it switched on |
| `ended_state` / `ended_step` | **1:1** | Where it was when it switched off. Differing values mean the pulse spanned a step |
| `started_send_ok` / `ended_send_ok` | **1:1** | Whether the command reached the hardware. 0 is a comms failure, not a mechanical one |
| `fault_type` / `fault_message` | **1:1** | A fault associated with this interval |
| `fault_raised_at_ms` / `fault_cleared_at_ms` | from `*_ts_utc` | When that fault was raised and cleared |

### `config_history` — one row per settings change

The source `config` table is a bare key/value store with **no timestamps and no history**, so
this is produced by diffing against a snapshot the projector keeps.

| Column | Source | Meaning |
|---|---|---|
| `changed_at_ms` | **computed** | When the change was *detected* — not necessarily when it was made |
| `config_key` | **1:1** `key` | Setting name, e.g. `CrusherFWDTime` |
| `old_value` | **computed** | Previous value (from our snapshot) |
| `new_value` | **1:1** `value` | Current value |

---

## 6. Derived tables — column by column

### `cycles` — one row per drink, and per gap between drinks

Derived from `orders` (start), `fsm_events` (end and counts) and `faults` (outcome). The
controller has **no** table that says when a drink ended or whether it worked.

| Column | Meaning |
|---|---|
| `started_at_ms` | When the interval began. For a drink, when the order was consumed at AutoCycle step 0 |
| `ended_at_ms` | When it ended — step 19 reached, a fault raised, or the machine left that state |
| `duration_ms` | How long the whole thing took |
| `order_key` | Unique. The business key every other table joins on |
| `is_production` | 1 = a real drink; 0 = a synthetic interval covering non-production time |
| `result` | `completed`, `faulted_recoverable`, `faulted_non_recoverable`, `aborted`, or for synthetic intervals `idle` / `maintenance` / `manual` / `error` |
| `recipe_id` | Which flavour, 1–6 |
| `glass_count` | Glasses in the order. Always 1 in current firmware |
| `terminal_state` | The state the interval ended in |
| `terminal_step` | The step it ended on. 19 means it completed normally |
| `fault_count` | How many faults occurred |
| `dominant_fault_type` | The most frequent fault type |
| `first_fault_at_ms` | When the first fault hit — how far in things went wrong |
| `last_fault_at_ms` | When the last one hit |
| `fsm_event_count` | Raw events in this interval, pre-counted |
| `step_event_count` | How many step changes |
| `state_transition_count` | How many state changes |
| `unique_state_count` | Distinct states visited. High on a drink suggests it bounced around |

> **Why `result` matters.** The current dashboard has three incompatible definitions of
> success, and in the main one "success" is the `ELSE` branch — the *absence* of a fault row.
> A cycle still running counts as a success. Here it is decided from firmware evidence:
> reaching AutoCycle step 19, which the firmware itself labels "cycle complete".

### `step_dwells` — how long each step took

From `fsm_step_runs` when the firmware provides it, otherwise paired from `fsm_events`.

| Column | Meaning |
|---|---|
| `started_at_ms` | When the machine entered this step |
| `ended_at_ms` | When it moved on — the moment the *next* dwell began, so no time is unattributed |
| `duration_ms` | Time on this step. The number to average when hunting slow steps |
| `lane` | `main`, `tilter` or `crusher`. The tilter and crusher are parallel sub-machines, not steps within the main sequence |
| `state` | Which state this step belongs to |
| `step` | Step number within that state |
| `step_title` | Human label, e.g. "Check cup transfer at dispense position". Read from the firmware's own metadata so it cannot drift |
| `seq_index` | Running order within the lane (0, 1, 2…). Lets the UI sort without comparing timestamps |
| `previous_state` | Which state it came from *(firmware-sourced rows only)* |
| `previous_step` | Which step it came from — reveals loops and retries |
| `event_count` | Raw events during this step. Unusually high can mean thrashing |
| `io_event_count` | How many were sensor changes |
| `transition_count` | How many were state transitions |
| `fault_count` | Faults raised during this step — which step is failing |
| `sensors_start_json` | All digital inputs at step entry *(firmware-sourced only)* |
| `sensors_end_json` | All digital inputs at step exit. Diffing the two shows what physically changed |
| `sensors_trace_json` | **Every** input edge during the step, verbatim from the firmware |
| `actuators_json` | Full per-output actuator breakdown, verbatim. `step_actuators` flattens this but keeps only the first segment's `recipe_step`, so the raw blob is what preserves multi-pulse detail |
| `source_kind` | `step_runs` if the firmware reported the step, `derived` if we paired it. Keeps a mixed-firmware fleet interpretable |
| `fault_type`, `fault_message` | The fault that interrupted this step *(firmware-sourced rows only)*. The firmware attaches it before the transition to Error, so it lands on the step that actually failed rather than the Error dwell that follows — which is what makes "which step fails most" answerable without a join |
| `cycle_started_at_ms` | The parent cycle's start, so "how far into the drink" needs no join |

### `step_actuators` — motor run time within one step

Unpacked from `fsm_step_runs.actuators_json`. **Cannot be derived from the raw event stream.**

| Column | Meaning |
|---|---|
| `step_started_at_ms` / `step_ended_at_ms` | Bounds of the parent step |
| `lane`, `state`, `step`, `step_title`, `seq_index` | Identify the parent step |
| `output_name` | Which actuator, e.g. "Crusher Motor" |
| `total_run_ms` | Total ON time for this output during this step. Drives wear, energy and bottleneck analysis |
| `segment_count` | How many separate ON pulses made up that total |
| `recipe_step` | The step in effect when the pulse started. Survives a step boundary splitting one pulse |
| `recipe_origin_state` | Which state that step number belongs to — distinguishes AutoCycle step 10 from Tilter step 10 |

### `state_durations` — how long the machine held each state

Paired from consecutive `fsm_events`. Consecutive spells in the same state are merged.

| Column | Meaning |
|---|---|
| `entered_at_ms` | When the machine entered the state |
| `exited_at_ms` | When it left |
| `duration_ms` | Time held |
| `state` | `HomeIdle`, `AutoCycle`, `Maintenance`, `Manual` or `Error` |
| `entry_reason` | What kind of event caused entry |
| `exit_reason` | What ended it |
| `is_downtime` | 1 for Error and Maintenance. Sum where this is 1 against the total to get **availability** |

### `fault_events` — one row per fault

From `faults`, emitted exactly once with a stable id.

> **Two sources, one row.** The controller in the field records `fsm_step_runs` but writes no
> `faults` rows at all, so this table would be empty on it and every fault-terminated cycle
> would resolve to `aborted` via the duration cap — indistinguishable from an operator walking
> away. `fsm_step_runs.fault_type`/`fault_message` are therefore projected into `fault_events`
> too, stamped at the dwell's end (the step ended *because* of the fault). `faults` stays
> authoritative — it can record several faults inside one `Error` dwell, which a single pair of
> columns cannot — so when both carry the same fault type within the same dwell, the ledger
> wins and the step run's copy is dropped rather than double-counted. Step-run-sourced rows are
> recognisable by a `fault_key` of the form `<type>|sr<step_run_id>|<order_key>`.

| Column | Meaning |
|---|---|
| `raised_at_ms` | When the fault occurred |
| `cleared_at_ms` | When it was resolved. Null while unresolved |
| `downtime_ms` | Time lost to it. Average for MTTR — and unrecovered faults still count, unlike the current dashboard |
| `fault_key` | Stable unique id. Its existence removes the need for de-duplication downstream |
| `fault_type` | The code, e.g. `CrusherMotorFault`. Group by this for a Pareto |
| `severity` | `recoverable` or `non_recoverable` — whether the machine can carry on. Classified on the device |
| `recovered` | 1 if the machine returned to normal afterwards |
| `recovery_at_ms` | When it did |
| `state` | State it faulted in |
| `step` | Step it was on — which stage fails most |
| `step_title` | That step's human label |
| `message` | Free-text detail from the firmware |
| `dwell_seq_index` | Which dwell it landed in, pre-attributed |
| `cycle_started_at_ms` | Parent cycle's start |

### `door_events` — one row per door open→close episode

Derived from two inputs in `sensor_input_toggles`:

- **X0.0** the main door switch. The firmware treats `true` as **closed**, so the door *opens*
  on the true→false edge (`fsm/interlocks.go`)
- **X0.7** labelled "CIP bypass switch", but its **rising edge is the fault reset**
  (`fsm/error.go`). A rise while the door is open means someone cleared a fault

| Column | Meaning |
|---|---|
| `opened_at_ms` | When the door opened |
| `closed_at_ms` | When it closed |
| `duration_ms` | How long it stood open |
| `fault_reset_during` | 1 if X0.7 rose while it was open |
| `fault_reset_count` | More than one means repeated attempts — usually the first didn't take |
| `first_reset_at_ms` / `last_reset_at_ms` | When resets happened |
| `ms_to_first_reset` | Open → first reset. How long the operator took to react |
| `state_at_open` / `step_at_open` | What the machine was doing when it opened |
| `state_at_close` / `step_at_close` | What it was doing when it closed |
| `opened_in_error` | 1 if the machine was already faulted when the door opened |

> A door already open when the projector starts is ignored — we never saw it open, so
> reporting a duration would be fabrication. Resets with the door shut (Modbus resets, genuine
> CIP bypass) do not invent an episode.

### `cip_runs` — one row per clean-in-place run

One row per closed **Maintenance** span. Maintenance *is* the CIP cycle in this firmware —
`getMaintenanceStepMetadata` describes its steps as the CIP sequence — so a span in that state
is a run.

| Column | Meaning |
|---|---|
| `started_at_ms` / `ended_at_ms` | When the clean began and finished |
| `duration_ms` | How long it took |
| `completed` | 1 if Maintenance step 7 was reached — the firmware labels it "CIP cycle complete" — 0 if the clean was interrupted |
| `trigger_source` | **Always NULL.** The controller records no auto/manual distinction, and inventing one would be worse than an honest gap |
| `fault_count` | Faults during the clean |

> Replaces a KPI whose value currently **changes when you change the date range** — it counts
> time buckets containing any CIP row, not actual runs.
>
> The completion marker is looked for in both `step_dwells` and `fsm_events`, for the same
> reason AutoCycle step 19 is detected on both paths: a controller carrying `fsm_step_runs`
> skips the dwell derivation from `fsm_events` entirely, so depending on the branch the step
> appears in only one of the two.

---

## 7. Rollups

Every column is additive — sums and counts, never a stored average — so hours re-aggregate
correctly into days, weeks or months.

Rollups are computed by a pass over the replica's **own finished tables**, not by the tracker.
By the time a row is in `cycles` or `state_durations` it is settled, so aggregating from there
is a query over facts rather than another "is it final yet" question. A bucket is written only
once the timeline has moved past its end, so a rollup row is never revised — and the pass is
bounded by the timeline, never wall-clock now, so a shutdown cannot stamp empty rollups on
every hour since the last event.

Two conventions worth knowing:

- **`recipe_id` is always NULL.** These are machine-level rows. The time columns have no recipe
  dimension, so splitting per recipe would duplicate them and break the partition property
  below. Per-recipe figures come from `cycles`, which carries `recipe_id` and is replicated.
- **`idle_ms` includes Manual.** `run`/`error`/`maintenance`/`idle` are meant to partition the
  period, and Manual is neither production, fault, nor cleaning. Folding it into idle keeps the
  partition complete rather than leaving a hole that makes availability look better than it was.

State spans are **clipped to the bucket**, not attributed to the hour they started in — a
three-hour idle stretch would otherwise put three hours into one bucket and nothing into the
next two, and availability computed from that is wrong in both directions.

### `hourly_rollups` · `daily_rollups`

| Column | Meaning |
|---|---|
| `bucket_start_ms` | Start of the hour *(hourly only; daily keys on `date_utc`)* |
| `glasses` | Drinks produced |
| `orders_started` | Drinks begun *(hourly only)* |
| `orders_completed` | Drinks that finished successfully |
| `orders_faulted` | Drinks that failed |
| `fault_count` | Total faults |
| `cip_runs` | Cleans performed *(hourly only)* |
| `cycle_ms_sum` | Total cycle time. Divide by `cycle_count` for the average |
| `cycle_count` | Number of cycles |
| `run_ms` | Time actually producing |
| `error_ms` | Time in Error |
| `maintenance_ms` | Time in Maintenance |
| `idle_ms` | Time idle. These four sum to the period, which is what makes availability computable |

### `hourly_fault_counts`

| Column | Meaning |
|---|---|
| `fault_type` / `severity` | Which fault, and how serious |
| `occurrences` | How many times this hour. Order by this for a Pareto |
| `downtime_ms_sum` | Total time lost to that fault type |

### `hourly_step_stats`

| Column | Meaning |
|---|---|
| `lane` / `state` / `step` / `step_title` | Which step is summarised |
| `dwell_count` | How many times it ran |
| `duration_ms_sum` | Total time in it. Divide by the count for the average |
| `duration_ms_max` | Worst single occurrence — catches the outlier an average hides |

---

## 8. Health tables

Without these, "the machine was idle" and "we recorded nothing" look identical — which
silently corrupts any availability figure.

### `projector_runs`

| Column | Meaning |
|---|---|
| `started_at_ms` | When the projector started |
| `stopped_at_ms` | When it stopped cleanly, if it did |
| `version` | Projector version, so old data can be read against the code that produced it |
| `source_branch` | Which firmware schema it detected — `step_runs` or `base` |
| `note` | Why the row was written |

### `gaps`

| Column | Meaning |
|---|---|
| `started_at_ms` / `ended_at_ms` | The span with no data |
| `duration_ms` | How long we were blind |
| `order_key` | Interval active at the time (nullable — a gap can predate any interval) |
| `reason` | e.g. "projector not running" |

---

## 9. Status

**Populated:** `cycles`, `step_dwells`, `step_actuators`, `state_durations`, `fault_events`,
`door_events`, `actuator_intervals`, `fsm_events`, `sensor_toggles`, `config_history`,
`projector_runs`, `gaps`.

**All 17 tables are populated.** `cip_runs` and the four rollups were schema-only for a while
— created empty, with nothing inserting into them — and now have writers (`cmd/projector/rollup.go`).

**Not everything in the replica is replicated.** Nine columns are deliberately held back
because nothing reads them and they dominated the wire cost; the replica keeps them all:

| Table | Not shipped | Why |
|---|---|---|
| `step_dwells` | `sensors_start_json`, `sensors_end_json` | ~820 B/row, and identical to each other on 99.7% of rows |
| `step_dwells` | `sensors_trace_json`, `actuators_json` | empty on nearly every row; `step_actuators` carries the flattened form |
| `step_dwells`, `step_actuators`, `fault_events`, `hourly_step_stats` | `step_title` | derived client-side from `(state, step)` |
| `fsm_events` | `sensors_json` | superseded by `sensors_bits` |

**Verification:** 24 tests passing, `PRAGMA foreign_key_check` clean, and the source
database's hash is unchanged after every run.
