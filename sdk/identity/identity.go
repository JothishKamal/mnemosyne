// Package identity owns all Mnemosyne identifier generation. No instrumentation
// adapter ever mints an identifier; centralising this is what keeps future
// language SDKs from diverging.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
)

type (
	// EmitterID identifies whoever produced an event. It deliberately does not
	// commit to pod, process, container, or VM semantics.
	EmitterID [8]byte
	// TraceID is W3C-compatible: 16 bytes.
	TraceID [16]byte
	// SpanID is W3C-compatible: 8 bytes. Assigned only to events that cross a
	// process boundary.
	SpanID [8]byte
)

func (e EmitterID) String() string { return hex.EncodeToString(e[:]) }
func (t TraceID) String() string   { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string    { return hex.EncodeToString(s[:]) }

// EventID is the primary identity of every recorded event. It is monotonic,
// compact, and sortable by construction.
type EventID struct {
	Emitter  EmitterID
	Sequence uint64
}

// IsZero reports whether the EventID is unset. Sequence numbers start at 1 so
// that the zero value is unambiguous.
func (e EventID) IsZero() bool { return e.Sequence == 0 }

// Generator mints identifiers for one process. It is safe for concurrent use.
type Generator struct {
	// seq is the atomic.Uint64 type rather than a bare uint64 field: on 32-bit
	// platforms the type carries the 8-byte alignment guarantee that raw
	// atomic.AddUint64 on a struct field does not.
	seq     atomic.Uint64
	emitter EmitterID
}

// NewGenerator returns a Generator with a randomly-assigned emitter ID.
func NewGenerator() (*Generator, error) {
	g := &Generator{}
	if _, err := rand.Read(g.emitter[:]); err != nil {
		return nil, err
	}
	return g, nil
}

// Emitter returns this generator's emitter ID.
func (g *Generator) Emitter() EmitterID { return g.emitter }

// NextEvent returns the next event ID. Sequence numbers start at 1.
func (g *Generator) NextEvent() EventID {
	return EventID{Emitter: g.emitter, Sequence: g.seq.Add(1)}
}

// NewTrace returns a fresh random trace ID.
func (g *Generator) NewTrace() (TraceID, error) {
	var t TraceID
	_, err := rand.Read(t[:])
	return t, err
}

// NewSpan returns a fresh random span ID.
func (g *Generator) NewSpan() (SpanID, error) {
	var s SpanID
	_, err := rand.Read(s[:])
	return s, err
}
