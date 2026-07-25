package identity

import (
	"encoding/base32"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/jothishkamal/mnemosyne/sdk/hlc"
)

// TracestateKey is the W3C tracestate key Mnemosyne claims.
const TracestateKey = "mnemo"

// tracestateVersion prefixes every value so the encoding can evolve.
const tracestateVersion = "1"

// emitterEnc encodes emitter IDs using lowercase base32 without padding, which
// stays inside the W3C tracestate value character set.
var emitterEnc = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// EncodeTracestate renders the HLC and event ID as a W3C tracestate value.
//
// Layout: <version>-<wallMS b36>-<logical b36>-<emitter b32>-<sequence b36>.
// Worst case is 1 + 1 + 13 + 1 + 7 + 1 + 13 + 1 + 13 = 51 characters, well
// inside the 256-character per-entry limit.
func EncodeTracestate(ts hlc.Timestamp, id EventID) string {
	var b strings.Builder
	b.WriteString(tracestateVersion)
	b.WriteByte('-')
	b.WriteString(strconv.FormatUint(ts.WallMS, 36))
	b.WriteByte('-')
	b.WriteString(strconv.FormatUint(uint64(ts.Logical), 36))
	b.WriteByte('-')
	b.WriteString(emitterEnc.EncodeToString(id.Emitter[:]))
	b.WriteByte('-')
	b.WriteString(strconv.FormatUint(id.Sequence, 36))
	return b.String()
}

var errBadTracestate = errors.New("mnemosyne: malformed tracestate value")

// DecodeTracestate parses a value produced by EncodeTracestate.
func DecodeTracestate(v string) (hlc.Timestamp, EventID, error) {
	parts := strings.Split(v, "-")
	if len(parts) != 5 || parts[0] != tracestateVersion {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}

	wall, err := strconv.ParseUint(parts[1], 36, 64)
	if err != nil {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}
	logical, err := strconv.ParseUint(parts[2], 36, 32)
	if err != nil {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}
	raw, err := emitterEnc.DecodeString(parts[3])
	if err != nil || len(raw) != 8 {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}
	seq, err := strconv.ParseUint(parts[4], 36, 64)
	if err != nil {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}

	var em EmitterID
	copy(em[:], raw)
	return hlc.Timestamp{WallMS: wall, Logical: uint32(logical)},
		EventID{Emitter: em, Sequence: seq}, nil
}

// FormatTraceparent renders a W3C traceparent header value.
func FormatTraceparent(t TraceID, s SpanID, sampled bool) string {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return "00-" + hex.EncodeToString(t[:]) + "-" + hex.EncodeToString(s[:]) + "-" + flags
}

// ParseTraceparent parses a W3C traceparent header value. It rejects the
// all-zero trace and span IDs, which the specification defines as invalid.
func ParseTraceparent(v string) (TraceID, SpanID, bool) {
	var t TraceID
	var s SpanID

	parts := strings.Split(v, "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return t, s, false
	}
	if parts[0] != "00" {
		return t, s, false
	}
	tb, err := hex.DecodeString(parts[1])
	if err != nil {
		return t, s, false
	}
	sb, err := hex.DecodeString(parts[2])
	if err != nil {
		return t, s, false
	}
	copy(t[:], tb)
	copy(s[:], sb)
	if t == (TraceID{}) || s == (SpanID{}) {
		return TraceID{}, SpanID{}, false
	}
	return t, s, true
}
