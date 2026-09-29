package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"canebot-fsm/io"
)

// The tracker turns the controller's raw event stream into finished, dashboard-shaped rows.
//
// The central idea is "accumulate, then emit on close". You cannot know how long a step took
// until the next step begins, so nothing is written when a thing starts — it is held open,
// and written once its end is known. That is also what makes every emitted row immutable,
// which is what the append-only replication path requires.
//
// Intervals form a CONTINUOUS PARTITION of machine time. Production orders are one kind of
// interval; the stretches between them (idle, maintenance, manual, error) get synthetic keys
// so that every event has a parent to point at. Without that, out-of-cycle activity would
// carry a null order_key and be invisible to an order-scoped frontend — and any availability
// figure computed from what remained would be wrong.

// FSM cycle-completion marker. Reaching AutoCycle step 19 is literally labelled
// "Cycle complete" in the firmware (fsm/autocycle.go), where the glass count is incremented.
const autoCycleCompleteStep = 19

// maxIntervalMS caps how long an interval may stay open before being force-closed. See
// ensureInterval for why this is load-bearing rather than a tidiness measure.
var maxIntervalMS int64 = int64((15 * time.Minute) / time.Millisecond)

// Lanes. The firmware runs two parallel sub-FSMs alongside the main one and reports them
// through CurrentState (fsm/machine.go sets CurrentState "Tilter" / "Crusher" for their step
// changes). Dwells are keyed per lane so concurrent activity does not collapse together.
const (
	laneMain    = "main"
	laneTilter  = "tilter"
	laneCrusher = "crusher"
)

// How a step_dwells row was produced — recorded on the row so a mixed-branch fleet stays
// interpretable downstream.
const (
	sourceKindStepRuns = "step_runs"
	sourceKindDerived  = "derived"
)

func laneForState(state string) string {
	switch state {
	case "Tilter":
		return laneTilter
	case "Crusher":
		return laneCrusher
	default:
		return laneMain
	}
}

// Interval results.
const (
	resultCompleted             = "completed"
	resultFaultedRecoverable    = "faulted_recoverable"
	resultFaultedNonRecoverable = "faulted_non_recoverable"
	resultAborted               = "aborted"
	resultIdle                  = "idle"
	resultMaintenance           = "maintenance"
	resultManual                = "manual"
	resultError                 = "error"
)

// isMaintenanceState reports whether a firmware state name is a Maintenance (CIP) state.
//
// The firmware does NOT write a bare "Maintenance". It writes the variant that started the
// clean, and the suffix is the trigger: observed on the machine as "Maintenance_regular" and
// "Maintenance_auto_cip_cmd3". Every bare-literal comparison therefore never fired, and the
// failures were silent rather than loud:
//
//	cip_runs        stayed empty forever — emitCIPRuns selects Maintenance spans
//	maintenance_ms  billed to idle — the occupancy switch fell through to default
//	is_downtime     0 through every clean, inflating availability
//	cycles.result   never held "maintenance", which broke the CIP completion probe
//
// Match on the prefix so a variant the firmware adds later is picked up rather than silently
// dropped into idle. The suffix itself is the distinction the cip_runs comment calls missing:
// "_regular" is operator/counter-triggered, "_auto_cip_cmd3" is the auto CIP command.
func isMaintenanceState(state string) bool {
	return strings.HasPrefix(state, "Maintenance")
}

// resultForState maps a non-production stretch to its interval kind.
func resultForState(state string) string {
	if isMaintenanceState(state) {
		return resultMaintenance
	}
	switch state {
	case "Manual":
		return resultManual
	case "Error":
		return resultError
	default:
		return resultIdle
	}
}

// synthPrefix gives synthetic intervals a key shaped like the real ones, so the frontend's
// single scoping rule holds for production and non-production alike.
func synthPrefix(result string) string {
	switch result {
	case resultMaintenance:
		return "MAINT"
	case resultManual:
		return "MANUAL"
	case resultError:
		return "ERR"
	default:
		return "IDLE"
	}
}

// ---------------------------------------------------------------------------
// Open (in-flight) work
// ---------------------------------------------------------------------------

type openStateSpan struct {
	state       string
	enteredMS   int64
	entryReason string
}

type openDwell struct {
	lane       string
	state      string
	step       *int64
	startedMS  int64
	seqIndex   int64
	events     int
	ioEvents   int
	transition int
	faults     int
}

// ledgerFault is one fault as the authoritative `faults` table reported it. Only what dedup
// against a step run's denormalised copy needs.
type ledgerFault struct {
	faultType string
	atMS      int64
}

type openFault struct {
	faultKey  string
	faultType string
	severity  string
	raisedMS  int64
	orderKey  string
	state     string
	step      int64
	message   string
	dwellSeq  *int64
}

// interval is one span of machine time and everything that happened inside it.
// Children are buffered here and flushed together with the parent when it closes, which is
// what keeps the foreign key satisfiable: parent first, then children, in one transaction.
type interval struct {
	orderKey     string
	isProduction bool
	startedMS    int64
	endedMS      int64
	result       string

	recipeID   *int64
	glassCount *int64

	terminalState string
	terminalStep  *int64

	faultCount        int
	dominantFaultType string
	faultTypeCounts   map[string]int
	firstFaultMS      *int64
	lastFaultMS       *int64

	fsmEventCount        int
	stepEventCount       int
	stateTransitionCount int
	uniqueStates         map[string]struct{}

	events         []outFSMEvent
	stateDurations []outStateDuration
	stepDwells     []outStepDwell
	stepActuators  []outStepActuator
	faults         []outFaultEvent
	toggles        []outSensorToggle
	configs        []outConfigChange
	doorEvents     []outDoorEvent
}

func newInterval(orderKey string, isProduction bool, startedMS int64, result string) *interval {
	return &interval{
		orderKey:        orderKey,
		isProduction:    isProduction,
		startedMS:       startedMS,
		result:          result,
		faultTypeCounts: map[string]int{},
		uniqueStates:    map[string]struct{}{},
	}
}

// tracker holds all in-flight state. One instance, driven by the main loop.
type tracker struct {
	severity func(string) string

	// useStepRuns is set when the controller carries fsm_step_runs (branch
	// test/tracking_fsm). The firmware then reports its own step boundaries, so the dwell
	// derivation below is skipped entirely in favour of ingesting those rows — they are
	// authoritative and carry sensor snapshots and per-step actuator run time we cannot
	// reconstruct. On branches without the table we fall back to deriving from fsm_events.
	useStepRuns bool

	// stepRunFaults is set when fsm_step_runs carries fault_type/fault_message. It gates
	// fault synthesis from step runs; see applyStepRunFault.
	stepRunFaults bool

	// ledgerFaults are faults seen from the `faults` table, which the firmware documents as
	// the authoritative ledger. Kept so a step run denormalising the same fault is not
	// counted twice. Pruned to the interval cap in recordLedgerFault.
	ledgerFaults []ledgerFault

	current    *interval
	openState  *openStateSpan
	openDwells map[string]*openDwell // by lane
	openFaults map[string]*openFault // by fault key
	// openDoorEpisode survives across intervals: a door opened while idle may not close
	// until a later one, and the episode belongs to whichever interval it closes in.
	openDoorEpisode *openDoor
	dwellSeq        map[string]int64 // next seq_index per lane, reset per interval

	// Completed intervals waiting to be written.
	closed []*interval

	// Rows whose parent interval has already been flushed (late-closing actuator
	// intervals, mostly). Written directly, since the FK parent already exists.
	lateActuators []outActuatorInterval

	// closedOrders remembers the last few production orders after they close, so a step run
	// arriving for one of them late is written to that order instead of closing whatever
	// interval is open now. See applyLateStepRun.
	closedOrders    map[string]*closedOrder
	closedOrderKeys []string // oldest first, bounded by maxClosedOrders

	synthCounter int64
}

// closedOrder is what a late child row needs from an order whose interval already closed:
// the context the writer denormalises onto children, and where its dwell numbering got to.
type closedOrder struct {
	startedMS, endedMS int64
	result             string
	recipeID           *int64
	glassCount         *int64
	dwellSeq           map[string]int64
}

// A late row trails its order by milliseconds (the step-19 run ends as the next order starts),
// so only the most recent orders are ever needed.
const maxClosedOrders = 8

func newTracker(severity func(string) string) *tracker {
	return &tracker{
		severity:     severity,
		openDwells:   map[string]*openDwell{},
		openFaults:   map[string]*openFault{},
		dwellSeq:     map[string]int64{},
		closedOrders: map[string]*closedOrder{},
	}
}

// ---------------------------------------------------------------------------
// Interval lifecycle
// ---------------------------------------------------------------------------

func (t *tracker) nextSynthKey(result string, atMS int64) string {
	t.synthCounter++
	return fmt.Sprintf("%s-%d%04d", synthPrefix(result), atMS, t.synthCounter%10000)
}

// ensureInterval guarantees there is an open interval covering `atMS` whose order key
// matches `orderKey`. An empty orderKey means out-of-cycle activity, which gets a synthetic
// interval keyed by the machine state it is sitting in.
func (t *tracker) ensureInterval(orderKey, state string, atMS int64) {
	// Force-close anything open too long, whatever kind it is.
	//
	// Without this a machine that sits in one state emits NOTHING: an interval is only
	// written when it closes, and a synthetic interval only closes when the state changes.
	// A machine running AutoCycle for hours would buffer every row in memory and ship none
	// of it. The cap bounds both memory and latency, at the cost of splitting a long stretch
	// into several rows — the right trade, since they re-aggregate by summing.
	if t.current != nil && maxIntervalMS > 0 && atMS-t.current.startedMS >= maxIntervalMS {
		forced := t.current.result
		if t.current.isProduction {
			// A production cycle open this long never completed normally.
			forced = resultAborted
		}
		t.closeInterval(atMS, forced)
	}

	if orderKey != "" {
		if t.current != nil && t.current.orderKey == orderKey {
			return
		}
		// AutoCycle often starts a few milliseconds before the orders row is applied, so
		// the first events land on a synthetic IDLE interval. Promote that interval to
		// the production key instead of closing it — closing would reset dwellSeq and
		// leave the early AutoCycle steps on a different cycle (or drop them).
		if t.current != nil && !t.current.isProduction && t.current.isAutoCyclePrelude() {
			t.current.adoptProduction(orderKey)
			return
		}
		t.closeInterval(atMS, "")
		t.current = newInterval(orderKey, true, atMS, resultAborted)
		t.resetPerInterval()
		return
	}

	// Events with a blank order_key still belong to the drink if one is open. Stealing
	// the interval here was splitting ORD-* into an IDLE fragment plus a later production
	// fragment, and UNIQUE(order_id) then kept only the later fragment's steps.
	if t.current != nil && t.current.isProduction {
		return
	}

	// The first AutoCycle row starts a fresh interval rather than joining the idle one. That
	// interval is the prelude the order adopts above; joining would hand the order the whole
	// idle stretch as its start, and an order begun 14.5 min into idle then hit the cap
	// mid-drink and was closed `aborted` (ORD-61d9a47bfa45, 28 Sep).
	want := resultForState(state)
	if t.current != nil && !t.current.isProduction && t.current.result == want &&
		(state != "AutoCycle" || t.current.isAutoCyclePrelude()) {
		return
	}
	t.closeInterval(atMS, "")
	t.current = newInterval(t.nextSynthKey(want, atMS), false, atMS, want)
	if state == "AutoCycle" {
		// Marked here so a step run (which records no uniqueStates) cannot split it again.
		t.current.uniqueStates["AutoCycle"] = struct{}{}
	}
	t.resetPerInterval()
}

// isAutoCyclePrelude reports whether this synthetic interval is the few milliseconds of
// AutoCycle that happen before the orders row is visible. HomeIdle / Manual / Error
// synthetics must not be adopted — those are a different stretch of machine time.
func (iv *interval) isAutoCyclePrelude() bool {
	if iv == nil || iv.isProduction || iv.result != resultIdle {
		return false
	}
	if _, ok := iv.uniqueStates["AutoCycle"]; ok {
		return true
	}
	for _, d := range iv.stepDwells {
		if d.state == "AutoCycle" {
			return true
		}
	}
	return false
}

func (iv *interval) adoptProduction(orderKey string) {
	iv.orderKey = orderKey
	iv.isProduction = true
	for i := range iv.events {
		iv.events[i].orderKey = orderKey
	}
	for i := range iv.stateDurations {
		iv.stateDurations[i].orderKey = orderKey
	}
	for i := range iv.stepDwells {
		iv.stepDwells[i].orderKey = orderKey
	}
	for i := range iv.stepActuators {
		iv.stepActuators[i].orderKey = orderKey
	}
	for i := range iv.faults {
		iv.faults[i].orderKey = orderKey
	}
	for i := range iv.toggles {
		iv.toggles[i].orderKey = orderKey
	}
	for i := range iv.configs {
		iv.configs[i].orderKey = orderKey
	}
	for i := range iv.doorEvents {
		iv.doorEvents[i].orderKey = orderKey
	}
}

func (t *tracker) resetPerInterval() {
	t.dwellSeq = map[string]int64{}
}

// closeInterval finalises the open interval and moves it to the pending-write queue.
// An empty synthetic interval with no children is dropped. A production interval, or any
// interval that already has step dwells, is kept even when close lands on the same
// millisecond as open — sub-millisecond AutoCycle steps are still real steps.
func (t *tracker) closeInterval(atMS int64, forcedResult string) {
	if t.current == nil {
		return
	}
	// Close anything still open inside it, so no dwell or state span is lost.
	t.closeOpenDwells(atMS)
	carry := t.openState
	t.closeOpenStateSpan(atMS, "interval_end")
	// The machine is still in that state; only the interval changed. Without this the span
	// stays closed until the next FSM event, and when the switch came from a toggle, a fault
	// or an order, everything between went unrecorded (state_durations on 19 Sep has no row
	// for 11:06:59–11:07:11, with the machine in Manual throughout).
	// Only when it actually closed: a row arriving late can close the interval before the span
	// began, and replacing the span then would start it earlier and overlap its predecessor.
	if carry != nil && t.openState == nil {
		t.openState = &openStateSpan{state: carry.state, enteredMS: atMS, entryReason: "interval_start"}
	}

	iv := t.current
	if atMS < iv.startedMS {
		atMS = iv.startedMS
	}
	iv.endedMS = atMS
	if forcedResult != "" {
		iv.result = forcedResult
	}
	if iv.endedMS == iv.startedMS && !iv.isProduction && len(iv.stepDwells) == 0 &&
		len(iv.events) == 0 && len(iv.faults) == 0 {
		t.current = nil
		return
	}
	iv.dominantFaultType = dominantKey(iv.faultTypeCounts)
	t.closed = append(t.closed, iv)
	t.current = nil
	if iv.isProduction {
		t.rememberClosedOrder(iv)
	}
}

// rememberClosedOrder records a production interval as it closes. The first close wins, as it
// does in the replica (cycles upserts on order_id and keeps the first row); a later fragment of
// the same order only moves the dwell numbering on.
func (t *tracker) rememberClosedOrder(iv *interval) {
	if c, ok := t.closedOrders[iv.orderKey]; ok {
		c.dwellSeq = t.dwellSeq
		return
	}
	t.closedOrders[iv.orderKey] = &closedOrder{
		startedMS:  iv.startedMS,
		endedMS:    iv.endedMS,
		result:     iv.result,
		recipeID:   iv.recipeID,
		glassCount: iv.glassCount,
		dwellSeq:   t.dwellSeq,
	}
	t.closedOrderKeys = append(t.closedOrderKeys, iv.orderKey)
	if len(t.closedOrderKeys) > maxClosedOrders {
		delete(t.closedOrders, t.closedOrderKeys[0])
		t.closedOrderKeys = t.closedOrderKeys[1:]
	}
}

// ---------------------------------------------------------------------------
// Event ingestion
// ---------------------------------------------------------------------------

// ApplyOrder opens a production interval. The firmware consumes the order at AutoCycle
// step 0, generates ORD-<hex12>, and writes the orders row (fsm/autocycle.go).
func (t *tracker) ApplyOrder(o srcOrder) {
	atMS := o.TS.UnixMilli()
	t.ensureInterval(o.OrderKey, "AutoCycle", atMS)
	if t.current != nil && t.current.orderKey == o.OrderKey {
		recipe, qty := o.Recipe, o.Quantity
		t.current.recipeID = &recipe
		t.current.glassCount = &qty
	}
}

func (t *tracker) ApplyFSMEvent(e srcFSMEvent) {
	atMS := e.TS.UnixMilli()
	t.ensureInterval(e.OrderKey, e.CurrentState, atMS)
	iv := t.current
	if iv == nil {
		return
	}

	iv.fsmEventCount++
	if e.CurrentState != "" {
		iv.uniqueStates[e.CurrentState] = struct{}{}
		iv.terminalState = e.CurrentState
	}
	if e.StepTo != nil {
		iv.terminalStep = e.StepTo
	}

	switch e.EventKind {
	case "state_transition":
		iv.stateTransitionCount++
	case "step_changed":
		iv.stepEventCount++
	}

	t.trackStateSpan(e, atMS)
	if !t.useStepRuns {
		t.trackDwell(e, atMS)
	}

	iv.events = append(iv.events, outFSMEvent{
		eventAtMS:      atMS,
		orderKey:       iv.orderKey,
		srcID:          e.ID,
		eventKind:      e.EventKind,
		stateFrom:      e.StateFrom,
		stateTo:        e.StateTo,
		currentState:   e.CurrentState,
		stepFrom:       e.StepFrom,
		stepTo:         e.StepTo,
		modbusOrderReg: e.ModbusOrderReg,
		inputID:        e.InputID,
		inputValue:     e.InputValue,
		eventType:      e.EventType,
		source:         e.Source,
		traceID:        e.TraceID,
		payloadJSON:    e.PayloadJSON,
		sensorsJSON:    e.SensorsJSON,
	})

	// Reaching step 19 in AutoCycle is the firmware's own "cycle complete" marker. It is
	// the only positive evidence of success available — everything else would be inferring
	// success from the absence of a fault, which is how the current dashboard gets it wrong.
	if iv.isProduction && e.CurrentState == "AutoCycle" &&
		e.StepTo != nil && *e.StepTo == autoCycleCompleteStep {
		t.closeInterval(atMS, resultCompleted)
	}
}

// trackStateSpan pairs consecutive states into closed spans, merging repeats. The dashboard's
// current LEAD-based version emits one segment per event, so a state held for an hour becomes
// hundreds of rows; merging here is what makes availability cheap to compute later.
func (t *tracker) trackStateSpan(e srcFSMEvent, atMS int64) {
	state := e.CurrentState
	if state == "" || state == "Tilter" || state == "Crusher" {
		// Sub-FSM rows describe a parallel lane, not main-FSM occupancy.
		return
	}
	if t.openState == nil {
		t.openState = &openStateSpan{state: state, enteredMS: atMS, entryReason: e.EventKind}
		return
	}
	if t.openState.state == state {
		return
	}
	if isStaleStateSample(t.openState, e, atMS) {
		return
	}
	t.closeStateSpanAt(atMS, e.EventKind)
	t.openState = &openStateSpan{state: state, enteredMS: atMS, entryReason: e.EventKind}
}

// staleSampleWindowMS bounds how long after a state_transition a non-transition event may
// still be carrying the state the machine just left.
const staleSampleWindowMS = 1000

// isStaleStateSample reports whether e is a non-transition event whose CurrentState was
// sampled before the transition that opened the current span.
//
// Every fault writes two rows in the same instant: `state_transition AutoCycle→Error`, and
// `event_sent FaultDetected` stamped with the CurrentState its sender read — AutoCycle, since
// the transition had not happened yet. Which of the two carries the later timestamp is a race
// (fault 149 on 19 Sep: event_sent first; fault 147 the night before: second). When it came
// second it replaced the Error span with AutoCycle, and with no further transition for 40
// minutes, state_durations reported a faulted machine as running and is_downtime read 0.
//
// Only a state_transition moves the machine to another state. Any other event reporting a
// different state within a second of one is describing the state before it.
func isStaleStateSample(open *openStateSpan, e srcFSMEvent, atMS int64) bool {
	return e.EventKind != "state_transition" &&
		open.entryReason == "state_transition" &&
		atMS-open.enteredMS < staleSampleWindowMS
}

func (t *tracker) closeStateSpanAt(atMS int64, reason string) {
	if t.openState == nil || t.current == nil {
		return
	}
	if atMS <= t.openState.enteredMS {
		return
	}
	t.current.stateDurations = append(t.current.stateDurations, outStateDuration{
		enteredAtMS: t.openState.enteredMS,
		exitedAtMS:  atMS,
		durationMS:  atMS - t.openState.enteredMS,
		orderKey:    t.current.orderKey,
		state:       t.openState.state,
		entryReason: t.openState.entryReason,
		exitReason:  reason,
	})
	t.openState = nil
}

func (t *tracker) closeOpenStateSpan(atMS int64, reason string) {
	t.closeStateSpanAt(atMS, reason)
}

// trackDwell segments (lane, state, step) occupancy. Each dwell is closed at the moment the
// next one starts, so the gap between a dwell's last event and the next dwell's first event
// is attributed rather than lost — the JS implementation this replaces measures first-to-last
// event within a dwell and silently under-counts.
func (t *tracker) trackDwell(e srcFSMEvent, atMS int64) {
	if t.current == nil {
		return
	}
	lane := laneForState(e.CurrentState)
	step := e.StepTo
	if step == nil {
		step = e.StepFrom
	}

	open := t.openDwells[lane]
	if open != nil {
		open.events++
		if e.EventKind == "input_changed" {
			open.ioEvents++
		}
		if e.EventKind == "state_transition" {
			open.transition++
		}
	}

	if open != nil && sameDwell(open, e.CurrentState, step) {
		return
	}
	t.closeDwell(lane, atMS)

	seq := t.dwellSeq[lane]
	t.dwellSeq[lane] = seq + 1
	nd := &openDwell{
		lane:      lane,
		state:     e.CurrentState,
		step:      step,
		startedMS: atMS,
		seqIndex:  seq,
		events:    1,
	}
	if e.EventKind == "input_changed" {
		nd.ioEvents = 1
	}
	if e.EventKind == "state_transition" {
		nd.transition = 1
	}
	t.openDwells[lane] = nd
}

func sameDwell(d *openDwell, state string, step *int64) bool {
	if d.state != state {
		return false
	}
	switch {
	case d.step == nil && step == nil:
		return true
	case d.step == nil || step == nil:
		return false
	default:
		return *d.step == *step
	}
}

// emitsDwell reports whether a step dwell is worth keeping.
//
// One state drowns this table. On the live machine 1,845,864 of 1,845,948 step_dwells rows
// are HomeIdle on a non-production interval — 99.995% of the table describing a machine
// standing still. Everything else together is 84 rows.
//
// So the filter is on that state, not on is_production. The distinction matters: nine of
// those rows are AutoCycle dwells on intervals with no order row, and they are real machine
// work — the shape the Pi's database actually had. A rule keyed on is_production would drop
// them along with the idle churn.
//
// HomeIdle inside a production cycle is kept: the machine pausing mid-drink is a fact worth
// having, and there are three such rows, not two million.
const stateHomeIdle = "HomeIdle"

func emitsDwell(state string, isProduction bool) bool {
	return isProduction || state != stateHomeIdle
}

func (t *tracker) closeDwell(lane string, atMS int64) {
	d := t.openDwells[lane]
	if d == nil || t.current == nil {
		return
	}
	delete(t.openDwells, lane)
	if atMS < d.startedMS {
		atMS = d.startedMS
	}
	if !emitsDwell(d.state, t.current.isProduction) {
		// Dropped here rather than at flush so a long idle stretch does not buffer tens of
		// thousands of rows in memory first. This is an idle-state filter, not a duration
		// filter: a 3ms AutoCycle step on an order is kept.
		return
	}
	t.current.stepDwells = append(t.current.stepDwells, outStepDwell{
		startedAtMS:     d.startedMS,
		endedAtMS:       atMS,
		durationMS:      atMS - d.startedMS,
		orderKey:        t.current.orderKey,
		lane:            d.lane,
		state:           d.state,
		step:            d.step,
		seqIndex:        d.seqIndex,
		eventCount:      d.events,
		ioEventCount:    d.ioEvents,
		transitionCount: d.transition,
		faultCount:      d.faults,
		sourceKind:      sourceKindDerived,
	})
}

func (t *tracker) closeOpenDwells(atMS int64) {
	lanes := make([]string, 0, len(t.openDwells))
	for lane := range t.openDwells {
		lanes = append(lanes, lane)
	}
	sort.Strings(lanes)
	for _, lane := range lanes {
		t.closeDwell(lane, atMS)
	}
}

// ApplyFault records a fault and, for a production interval, ends the cycle.
// TransitionToError clears the order key in the firmware (fsm/machine.go), so a fault is
// terminal for the order it belongs to.
func (t *tracker) ApplyFault(f srcFault) {
	atMS := f.TS.UnixMilli()
	t.ensureInterval(f.OrderKey, f.State, atMS)
	iv := t.current
	if iv == nil {
		return
	}

	severity := t.severity(f.FaultType)
	key := fmt.Sprintf("%s|%d|%s", f.FaultType, f.ID, iv.orderKey)
	t.recordLedgerFault(f.FaultType, atMS)

	iv.faultCount++
	iv.faultTypeCounts[f.FaultType]++
	if iv.firstFaultMS == nil {
		ms := atMS
		iv.firstFaultMS = &ms
	}
	ms := atMS
	iv.lastFaultMS = &ms

	if d := t.openDwells[laneMain]; d != nil {
		d.faults++
	}
	var dwellSeq *int64
	if d := t.openDwells[laneMain]; d != nil {
		s := d.seqIndex
		dwellSeq = &s
	}

	iv.faults = append(iv.faults, outFaultEvent{
		raisedAtMS:    atMS,
		orderKey:      iv.orderKey,
		faultKey:      key,
		faultType:     f.FaultType,
		severity:      severity,
		state:         f.State,
		step:          f.Step,
		message:       f.Message,
		dwellSeqIndex: dwellSeq,
	})

	if iv.isProduction {
		result := resultFaultedRecoverable
		if severity == severityNonRecoverable {
			result = resultFaultedNonRecoverable
		}
		t.closeInterval(atMS, result)
	}
}

// recordLedgerFault notes a fault from the authoritative `faults` table so a step run
// carrying the same fault denormalised is not counted a second time. The list is pruned to
// the interval cap — dedup only ever looks back as far as one dwell.
func (t *tracker) recordLedgerFault(faultType string, atMS int64) {
	t.ledgerFaults = append(t.ledgerFaults, ledgerFault{faultType: faultType, atMS: atMS})
	cutoff := atMS - maxIntervalMS
	keep := t.ledgerFaults[:0]
	for _, lf := range t.ledgerFaults {
		if lf.atMS >= cutoff {
			keep = append(keep, lf)
		}
	}
	t.ledgerFaults = keep
}

// ledgerCovers reports whether the `faults` table already recorded this fault type inside the
// dwell it is attributed to. The slack absorbs the millisecond of drift between the firmware
// stamping the fault row and the step run's own end timestamp.
func (t *tracker) ledgerCovers(faultType string, startMS, endMS int64) bool {
	const slackMS = 2000
	for _, lf := range t.ledgerFaults {
		if lf.faultType == faultType && lf.atMS >= startMS-slackMS && lf.atMS <= endMS+slackMS {
			return true
		}
	}
	return false
}

// applyStepRunFault turns a step run's denormalised fault into the same rows ApplyFault would
// have produced, and reports whether it closed the interval.
//
// This exists because the ledger can be empty. The controller in the field records step runs
// but writes no `faults` rows at all, so without this every fault-terminated cycle is
// invisible: fault_events has nothing in it, cycles.fault_count is 0, and the cycle resolves
// to `aborted` by the duration cap rather than `faulted_*`. The firmware documents `faults` as
// authoritative — it handles several faults inside one Error dwell, which a single pair of
// columns cannot — so when both sources carry the fault the ledger wins and this is skipped.
//
// The fault is stamped at the dwell's END: the firmware sends FaultDetected, which flushes
// this session and transitions to Error, so the step ended because of the fault. That is the
// closest instant the row actually pins down.
func (t *tracker) applyStepRunFault(iv *interval, sr srcStepRun, startMS, endMS int64, seq int64) bool {
	if iv == nil || sr.FaultType == "" || !t.stepRunFaults {
		return false
	}
	if t.ledgerCovers(sr.FaultType, startMS, endMS) {
		return false
	}

	severity := t.severity(sr.FaultType)
	// Keyed on the step run's own source id, so it is stable across re-runs and can never
	// collide with a ledger-sourced key (those carry the faults row id).
	key := fmt.Sprintf("%s|sr%d|%s", sr.FaultType, sr.ID, iv.orderKey)

	iv.faultCount++
	iv.faultTypeCounts[sr.FaultType]++
	if iv.firstFaultMS == nil {
		ms := endMS
		iv.firstFaultMS = &ms
	}
	ms := endMS
	iv.lastFaultMS = &ms

	// The dwell this fault belongs to is the step run being ingested, which is appended
	// closed rather than held open — so its seq index is attributed directly.
	seqIdx := seq
	iv.faults = append(iv.faults, outFaultEvent{
		raisedAtMS:    endMS,
		orderKey:      iv.orderKey,
		faultKey:      key,
		faultType:     sr.FaultType,
		severity:      severity,
		state:         sr.CurrentState,
		step:          sr.Step,
		message:       sr.FaultMessage,
		dwellSeqIndex: &seqIdx,
	})

	if !iv.isProduction || iv != t.current {
		// A detached late fragment records the fault but never closes the open interval.
		return false
	}
	result := resultFaultedRecoverable
	if severity == severityNonRecoverable {
		result = resultFaultedNonRecoverable
	}
	t.closeInterval(endMS, result)
	return true
}

// ApplyStepRun ingests one completed step as the controller recorded it. Used instead of
// trackDwell on branches carrying fsm_step_runs — the row is already closed and already
// carries what we would otherwise have to derive, plus per-step actuator attribution.
//
// Zero-length dwells are kept. UnixMilli truncates, so a 3ms AutoCycle step often has
// startMS == endMS; dropping those hid steps 1 and 5 of a live order. Inverted times
// (end before start) are clamped rather than skipped.
func (t *tracker) ApplyStepRun(sr srcStepRun, actuators map[string]StepRunActuator) {
	startMS := sr.Started.UnixMilli()
	endMS := sr.Ended.UnixMilli()
	if endMS < startMS {
		endMS = startMS
	}
	if t.isLateForClosedOrder(sr.OrderKey) {
		t.applyLateStepRun(sr, actuators, startMS, endMS)
		return
	}
	t.ensureInterval(sr.OrderKey, sr.CurrentState, startMS)
	iv := t.current
	if iv == nil {
		return
	}

	lane := laneForState(sr.CurrentState)
	seq := t.dwellSeq[lane]
	t.dwellSeq[lane] = seq + 1

	if !emitsDwell(sr.CurrentState, iv.isProduction) {
		// The sequence counter is still advanced above, so seq_index stays a faithful
		// running position within the lane rather than silently renumbering.
		return
	}
	appendStepRun(iv, sr, actuators, startMS, endMS, lane, seq)

	// A fault on this row ended the cycle, so it is resolved before completion is considered:
	// the two cannot both be true, and a fault is the stronger evidence.
	if t.applyStepRunFault(iv, sr, startMS, endMS, seq) {
		return
	}

	// Reaching AutoCycle step 19 closes the cycle — the firmware labels it "cycle complete"
	// and increments the glass count there (fsm/autocycle.go).
	//
	// This has to be detected here as well as in ApplyFSMEvent: on a controller that records
	// step runs but not fsm_events, this is the ONLY place completion is visible. Without it
	// every production cycle stays open until the duration cap force-closes it as `aborted`,
	// and a machine that is working perfectly reports nothing but failures.
	if iv.isProduction && sr.CurrentState == "AutoCycle" && sr.Step == autoCycleCompleteStep {
		t.closeInterval(endMS, resultCompleted)
	}
}

// isLateForClosedOrder reports whether a row keyed to orderKey belongs to an order that has
// already closed and is not the one open now.
//
// Step runs sit on the timeline at their END. When the firmware chains orders ("[Step 19]
// Next order received"), the previous order's step-19 run ends at the instant the next order
// starts — after that order's row has opened its interval. Routed through ensureInterval, the
// late row closed the new order at birth (`aborted`, 0 events, 0 ms) and UNIQUE(order_id) kept
// that empty row over the real drink. Eleven drinks on 28 Sep read as failed this way.
func (t *tracker) isLateForClosedOrder(orderKey string) bool {
	if orderKey == "" {
		return false
	}
	if _, ok := t.closedOrders[orderKey]; !ok {
		return false
	}
	return t.current == nil || t.current.orderKey != orderKey
}

// applyLateStepRun writes a late step run to its own order without touching the open interval,
// the open dwells or the state span. It goes out as a detached fragment carrying the closed
// order's context: the fragment's cycles row is a no-op against the one already written (the
// upsert keeps the first), and its dwell lands under the right order_id with the right
// cycle_result and recipe.
func (t *tracker) applyLateStepRun(sr srcStepRun, actuators map[string]StepRunActuator, startMS, endMS int64) {
	c := t.closedOrders[sr.OrderKey]
	lane := laneForState(sr.CurrentState)
	seq := c.dwellSeq[lane]
	c.dwellSeq[lane] = seq + 1

	frag := newInterval(sr.OrderKey, true, c.startedMS, c.result)
	frag.endedMS = c.endedMS
	frag.recipeID = c.recipeID
	frag.glassCount = c.glassCount
	appendStepRun(frag, sr, actuators, startMS, endMS, lane, seq)
	t.applyStepRunFault(frag, sr, startMS, endMS, seq)
	t.closed = append(t.closed, frag)
}

// appendStepRun adds one step run's dwell and per-output actuator totals to iv.
func appendStepRun(iv *interval, sr srcStepRun, actuators map[string]StepRunActuator,
	startMS, endMS int64, lane string, seq int64) {
	step := sr.Step
	iv.stepDwells = append(iv.stepDwells, outStepDwell{
		startedAtMS:   startMS,
		endedAtMS:     endMS,
		durationMS:    endMS - startMS,
		orderKey:      iv.orderKey,
		lane:          lane,
		state:         sr.CurrentState,
		step:          &step,
		seqIndex:      seq,
		previousState: sr.PreviousState,
		previousStep:  sr.PreviousStep,
		sensorsStart:  sr.SensorsStart,
		sensorsEnd:    sr.SensorsEnd,
		sensorsTrace:  sr.SensorsTrace,
		actuatorsJSON: sr.ActuatorsJSON,
		sourceKind:    sourceKindStepRuns,
		faultType:     sr.FaultType,
		faultMessage:  sr.FaultMessage,
	})
	if sr.FaultType != "" {
		// The dwell just appended is the one the fault interrupted.
		iv.stepDwells[len(iv.stepDwells)-1].faultCount = 1
	}

	outputIDs := make([]string, 0, len(actuators))
	for id := range actuators {
		outputIDs = append(outputIDs, id)
	}
	sort.Strings(outputIDs) // deterministic emission order

	for _, outputID := range outputIDs {
		a := actuators[outputID]
		if a.TotalRunMS <= 0 {
			continue
		}
		var recipeStep *int64
		var originState string
		if len(a.Segments) > 0 {
			if rs := a.Segments[0].RecipeStep; rs != nil {
				v := int64(*rs)
				recipeStep = &v
			}
			originState = a.Segments[0].RecipeOriginState
		}
		iv.stepActuators = append(iv.stepActuators, outStepActuator{
			stepStartedAtMS:   startMS,
			stepEndedAtMS:     endMS,
			orderKey:          iv.orderKey,
			lane:              lane,
			state:             sr.CurrentState,
			step:              &step,
			seqIndex:          seq,
			outputID:          outputID,
			outputName:        a.Name,
			totalRunMS:        a.TotalRunMS,
			segmentCount:      len(a.Segments),
			recipeStep:        recipeStep,
			recipeOriginState: originState,
		})
	}
}

// Door tracking.
//
// Two inputs, both already present in the toggle stream:
//
//	X0.0  the main door switch. The firmware treats true as CLOSED (fsm/interlocks.go),
//	      so the door OPENS on the true→false edge and closes on false→true.
//	X0.7  labelled "CIP bypass switch" in the IO map, but the firmware uses its RISING
//	      edge as the fault reset (fsm/error.go). A rise while the door is open is
//	      therefore someone clearing a fault during that episode.
//
// An episode is emitted only when the door closes, so like everything else here the row is
// immutable once written. A door already open when the projector starts is ignored — we
// never saw it open, so we cannot honestly report how long it has been.
type openDoor struct {
	openedMS      int64
	stateAtOpen   string
	stepAtOpen    *int64
	openedInError bool
	resetCount    int
	firstResetMS  *int64
	lastResetMS   *int64
}

func (t *tracker) applyDoorSignals(s srcSensorToggle, atMS int64) {
	switch io.Input(s.InputID) {
	case io.InputMainDoorSwitch:
		opened := s.ValueFrom == 1 && s.ValueTo == 0
		closed := s.ValueFrom == 0 && s.ValueTo == 1
		switch {
		case opened:
			step := s.CurrentStep
			t.openDoorEpisode = &openDoor{
				openedMS:      atMS,
				stateAtOpen:   s.CurrentState,
				stepAtOpen:    step,
				openedInError: s.CurrentState == "Error",
			}
		case closed:
			t.closeDoorEpisode(s, atMS)
		}

	case io.InputCIPBypassSwitch:
		// Rising edge only — that is what the firmware acts on.
		if s.ValueFrom == 0 && s.ValueTo == 1 && t.openDoorEpisode != nil {
			d := t.openDoorEpisode
			d.resetCount++
			ms := atMS
			if d.firstResetMS == nil {
				d.firstResetMS = &ms
			}
			d.lastResetMS = &ms
		}
	}
}

func (t *tracker) closeDoorEpisode(s srcSensorToggle, atMS int64) {
	d := t.openDoorEpisode
	if d == nil || t.current == nil || atMS <= d.openedMS {
		t.openDoorEpisode = nil
		return
	}
	t.openDoorEpisode = nil

	var msToFirst *int64
	if d.firstResetMS != nil {
		v := *d.firstResetMS - d.openedMS
		msToFirst = &v
	}

	t.current.doorEvents = append(t.current.doorEvents, outDoorEvent{
		openedAtMS:      d.openedMS,
		closedAtMS:      atMS,
		durationMS:      atMS - d.openedMS,
		orderKey:        t.current.orderKey,
		faultResetCount: d.resetCount,
		firstResetAtMS:  d.firstResetMS,
		lastResetAtMS:   d.lastResetMS,
		msToFirstReset:  msToFirst,
		stateAtOpen:     d.stateAtOpen,
		stepAtOpen:      d.stepAtOpen,
		stateAtClose:    s.CurrentState,
		stepAtClose:     s.CurrentStep,
		openedInError:   d.openedInError,
	})
}

func (t *tracker) ApplySensorToggle(s srcSensorToggle) {
	atMS := s.TS.UnixMilli()
	t.ensureInterval(s.OrderKey, s.CurrentState, atMS)
	if t.current == nil {
		return
	}
	t.applyDoorSignals(s, atMS)
	t.current.toggles = append(t.current.toggles, outSensorToggle{
		eventAtMS:    atMS,
		orderKey:     t.current.orderKey,
		srcID:        s.ID,
		inputID:      s.InputID,
		inputName:    s.InputName,
		valueFrom:    s.ValueFrom,
		valueTo:      s.ValueTo,
		currentState: s.CurrentState,
		currentStep:  s.CurrentStep,
	})
}

func (t *tracker) ApplyConfigChange(key, oldVal, newVal string, at time.Time) {
	atMS := at.UnixMilli()
	state := ""
	if t.current != nil && !t.current.isProduction {
		state = t.current.result
	}
	t.ensureInterval("", state, atMS)
	if t.current == nil {
		return
	}
	t.current.configs = append(t.current.configs, outConfigChange{
		changedAtMS: atMS,
		orderKey:    t.current.orderKey,
		configKey:   key,
		oldValue:    oldVal,
		newValue:    newVal,
	})
}

// FlushOpen force-closes the interval still in progress. Called when the process is shutting
// down or a -once run is ending, so buffered work is written rather than discarded. The row
// is marked as force-closed via its result so downstream can tell it apart from a natural end.
// FlushOpen closes the in-progress interval at atMS.
//
// atMS is floored to just past the interval's start. The caller closes on the machine's
// timeline rather than wall-clock now (see finalFlush), and a tick whose only applied row is the
// one that opened this interval leaves the two equal — which closeInterval treats as
// zero-length and drops. Dropping it loses the row entirely, and with it the late-arriving
// `orders` fill that the ON CONFLICT in FlushInterval exists to apply.
func (t *tracker) FlushOpen(atMS int64) {
	if t.current == nil {
		return
	}
	if atMS <= t.current.startedMS {
		atMS = t.current.startedMS + 1
	}
	forced := t.current.result
	if t.current.isProduction {
		forced = resultAborted
	}
	t.closeInterval(atMS, forced)
}

// PendingIntervals hands over everything closed since the last call.
func (t *tracker) PendingIntervals() []*interval {
	out := t.closed
	t.closed = nil
	sort.SliceStable(out, func(i, j int) bool { return out[i].endedMS < out[j].endedMS })
	return out
}

func dominantKey(counts map[string]int) string {
	best, bestN := "", 0
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic tie-break
	for _, k := range keys {
		if counts[k] > bestN {
			best, bestN = k, counts[k]
		}
	}
	return best
}

func sortActuatorsByEnd(in []srcActuatorInterval) {
	sort.SliceStable(in, func(i, j int) bool { return in[i].Ended.Before(in[j].Ended) })
}
