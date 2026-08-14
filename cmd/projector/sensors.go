package main

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"canebot-fsm/io"
)

// Sensor snapshots are 32 booleans. The firmware serialises them as a JSON object keyed by
// input tag — {"X0.0":true,"X0.1":false,…} — which is ~410 bytes, and roughly double that on
// the wire once it is escaped as a string inside the replication payload. That is ~205 bytes
// per bit.
//
// sensorBits renders the same information as one character per input, so a snapshot is 32
// bytes and contains no quote characters to escape. The column it feeds is the only sensor
// state replicated to the cloud; the raw JSON stays in the replica for local inspection.
//
// ORDER IS THE CONTRACT. Bit i is sensorBitOrder[i]. The order is every input in
// io.AllDigitalInputs() sorted numerically by (byte, bit) — X0.0, X0.1, … X0.15, X1.0, …
// deliberately NOT the lexicographic order of the tag strings, which would sort X0.10 before
// X0.2. A consumer reproduces it by applying the same numeric sort to its own tag list; the
// fixed length is what makes a mismatched list detectable rather than silently misaligned.
var sensorBitOrder = buildSensorBitOrder()

func buildSensorBitOrder() []string {
	all := io.AllDigitalInputs()
	tags := make([]string, 0, len(all))
	for _, in := range all {
		tags = append(tags, string(in))
	}
	sort.Slice(tags, func(i, j int) bool {
		bi, si := parseInputTag(tags[i])
		bj, sj := parseInputTag(tags[j])
		if bi != bj {
			return bi < bj
		}
		return si < sj
	})
	return tags
}

// parseInputTag splits "X1.5" into (1, 5). Anything unparseable sorts last but keeps a stable
// position, so an unexpected tag shifts nothing before it.
func parseInputTag(tag string) (int, int) {
	s := strings.TrimPrefix(tag, "X")
	dot := strings.IndexByte(s, '.')
	if dot < 0 {
		return 1 << 30, 0
	}
	b, err := strconv.Atoi(s[:dot])
	if err != nil {
		return 1 << 30, 0
	}
	bit, err := strconv.Atoi(s[dot+1:])
	if err != nil {
		return b, 1 << 30
	}
	return b, bit
}

// encodeSensorBits turns the firmware's snapshot object into the fixed-width bit string.
//
// An empty or unparseable snapshot yields "" rather than a string of zeroes: a row with no
// snapshot and a row where every input reads low are different facts, and reporting the second
// when we mean the first would be fabrication.
func encodeSensorBits(sensorsJSON string) string {
	s := strings.TrimSpace(sensorsJSON)
	if s == "" || s == "{}" || s == "null" {
		return ""
	}
	var snapshot map[string]bool
	if err := json.Unmarshal([]byte(s), &snapshot); err != nil {
		logf("sensors_json: %v", err)
		return ""
	}
	if len(snapshot) == 0 {
		return ""
	}

	var b strings.Builder
	b.Grow(len(sensorBitOrder))
	for _, tag := range sensorBitOrder {
		if snapshot[tag] {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	return b.String()
}

// sensorValue reads one input out of a snapshot, as 1, 0, or absent.
//
// The bit string is 32 characters with no separators, and the dashboard queries this replica
// through a JSON DSL with no substring operator — so "every step where the door was open" is
// unaskable against sensors_bits, however cheap the decode is in the client. Lifting the two
// inputs whose meaning is documented into their own columns makes that a WHERE clause.
//
// Only these two are lifted. Thirty-two boolean columns to answer two questions would be the
// wrong trade, and the rest stay in the bit string where a client that wants them can decode
// it with the order in this file.
//
// Returns nil when there is no snapshot at all, which is not the same as the input reading
// low — a step run that recorded nothing must not claim the door was open.
func sensorValue(sensorsJSON string, tag io.Input) *int64 {
	s := strings.TrimSpace(sensorsJSON)
	if s == "" || s == "{}" || s == "null" {
		return nil
	}
	var snapshot map[string]bool
	if err := json.Unmarshal([]byte(s), &snapshot); err != nil {
		return nil
	}
	v, ok := snapshot[string(tag)]
	if !ok {
		return nil
	}
	var out int64
	if v {
		out = 1
	}
	return &out
}

// doorClosed reports the main door switch, which the firmware wires true = CLOSED. The column
// is named for what the value means rather than for the pin, so a reader cannot invert it by
// accident.
func doorClosed(sensorsJSON string) *int64 {
	return sensorValue(sensorsJSON, io.InputMainDoorSwitch)
}

// cipBypass reports the CIP bypass switch. Its RISING EDGE is the fault reset, so a 1 here is
// the switch position at that instant, not evidence that a reset happened — door_events
// carries the edge count for that.
func cipBypass(sensorsJSON string) *int64 {
	return sensorValue(sensorsJSON, io.InputCIPBypassSwitch)
}
