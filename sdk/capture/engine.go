// Package capture buffers payloads in bounded per-trace rings so that a trace
// which later turns out to be interesting can have its payloads flushed
// retroactively.
package capture

import (
	"crypto/sha256"
	"sync"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
)

// Level is how much of an operation is recorded.
type Level int

const (
	LevelOff Level = iota
	LevelMetadata
	LevelFull
)

// Kind distinguishes the two payload slots on an event.
type Kind int

const (
	KindRequest Kind = iota
	KindResponse
)

// Policy configures the engine.
//
// Spec 1 resolves Default only. Route, SQL, cache-key, and topic matchers are
// part of the config grammar but are added with the code that consumes them.
type Policy struct {
	Default          Level
	MaxBytesPerTrace int
	MaxTraces        int
	MaxPayloadBytes  int
	Redact           Redact
}

// Blob is a buffered payload.
type Blob struct {
	TraceID identity.TraceID
	EventID identity.EventID
	Kind    Kind
	SHA256  [32]byte
	Body    []byte
}

type ring struct {
	blobs []Blob
	bytes int
}

// Engine holds per-trace payload rings.
type Engine struct {
	mu      sync.Mutex
	policy  Policy
	rings   map[identity.TraceID]*ring
	order   []identity.TraceID // LRU, oldest first
	dropped uint64
}

// NewEngine returns an Engine. Zero bounds fall back to conservative defaults.
func NewEngine(p Policy) *Engine {
	if p.MaxBytesPerTrace <= 0 {
		p.MaxBytesPerTrace = 1 << 20 // 1 MiB
	}
	if p.MaxTraces <= 0 {
		p.MaxTraces = 1000
	}
	if p.MaxPayloadBytes <= 0 {
		p.MaxPayloadBytes = 64 << 10 // 64 KiB
	}
	return &Engine{policy: p, rings: map[identity.TraceID]*ring{}}
}

// Record computes a PayloadRef for body and, at LevelFull, buffers the redacted
// bytes. The returned ref always carries sha256 and size, whether or not the
// body is retained.
func (e *Engine) Record(tr identity.TraceID, ev identity.EventID, kind Kind, body []byte) *v1.PayloadRef {
	sum := sha256.Sum256(body)
	ref := &v1.PayloadRef{Sha256: sum[:], Size: uint64(len(body))}

	if e.policy.Default != LevelFull {
		return ref
	}

	stored, wasRedacted := e.policy.Redact.Body(body)
	if len(stored) > e.policy.MaxPayloadBytes {
		stored = stored[:e.policy.MaxPayloadBytes]
		ref.Truncated = true
	}
	ref.Redacted = wasRedacted
	ref.Stored = true

	e.mu.Lock()
	defer e.mu.Unlock()
	e.push(tr, Blob{TraceID: tr, EventID: ev, Kind: kind, SHA256: sum, Body: stored})
	return ref
}

// push appends a blob and enforces both bounds. Caller holds e.mu.
func (e *Engine) push(tr identity.TraceID, b Blob) {
	r, ok := e.rings[tr]
	if !ok {
		r = &ring{}
		e.rings[tr] = r
		e.order = append(e.order, tr)
	}
	r.blobs = append(r.blobs, b)
	r.bytes += len(b.Body)

	// Byte bound: drop oldest blobs within this trace.
	for r.bytes > e.policy.MaxBytesPerTrace && len(r.blobs) > 0 {
		r.bytes -= len(r.blobs[0].Body)
		r.blobs = r.blobs[1:]
		e.dropped++
	}

	// Trace bound: evict least-recently-written traces.
	for len(e.order) > e.policy.MaxTraces {
		oldest := e.order[0]
		e.order = e.order[1:]
		if victim, ok := e.rings[oldest]; ok {
			e.dropped += uint64(len(victim.blobs))
			delete(e.rings, oldest)
		}
	}
}

// Flush returns and removes a trace's buffered payloads.
func (e *Engine) Flush(tr identity.TraceID) []Blob {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rings[tr]
	if !ok {
		return nil
	}
	delete(e.rings, tr)
	e.removeFromOrder(tr)
	return r.blobs
}

// Discard drops a trace's payloads without flushing, returning how many were
// dropped.
func (e *Engine) Discard(tr identity.TraceID) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rings[tr]
	if !ok {
		return 0
	}
	n := uint64(len(r.blobs))
	delete(e.rings, tr)
	e.removeFromOrder(tr)
	e.dropped += n
	return n
}

func (e *Engine) removeFromOrder(tr identity.TraceID) {
	for i, id := range e.order {
		if id == tr {
			e.order = append(e.order[:i], e.order[i+1:]...)
			return
		}
	}
}

// DroppedCount returns the number of payloads dropped by eviction or discard.
func (e *Engine) DroppedCount() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// BufferedBytes returns the total bytes currently held. Test observability.
func (e *Engine) BufferedBytes() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := 0
	for _, r := range e.rings {
		total += r.bytes
	}
	return total
}

// BufferedTraces returns the number of traces currently held.
func (e *Engine) BufferedTraces() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.rings)
}
