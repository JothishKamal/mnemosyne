// Package memory provides in-memory EventStore and BlobStore implementations.
// They back the deterministic test harness and let the walking skeleton reach a
// rendered replay before any persistence code exists.
package memory

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"sort"
	"sync"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store"
	"google.golang.org/protobuf/proto"
)

// Events is an in-memory EventStore.
type Events struct {
	mu     sync.RWMutex
	events []*v1.Event
	gaps   []*v1.Gap
}

// NewEvents returns an empty in-memory EventStore.
func NewEvents() *Events { return &Events{} }

func (s *Events) AppendEvents(_ context.Context, in []*v1.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range in {
		s.events = append(s.events, proto.Clone(e).(*v1.Event))
	}
	return nil
}

func (s *Events) AppendGaps(_ context.Context, in []*v1.Gap) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range in {
		s.gaps = append(s.gaps, proto.Clone(g).(*v1.Gap))
	}
	return nil
}

func (s *Events) TraceEvents(_ context.Context, traceID []byte) ([]*v1.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*v1.Event
	for _, e := range s.events {
		if bytes.Equal(e.TraceId, traceID) {
			out = append(out, proto.Clone(e).(*v1.Event))
		}
	}
	sortByHLC(out)
	return out, nil
}

func (s *Events) Window(_ context.Context, q store.WindowQuery) ([]*v1.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	svc := map[string]bool{}
	for _, n := range q.Services {
		svc[n] = true
	}

	var out []*v1.Event
	for _, e := range s.events {
		w := e.GetHlc().GetWallMs()
		if w < q.FromWallMS || w > q.ToWallMS {
			continue
		}
		if len(svc) > 0 && !svc[e.GetService().GetName()] {
			continue
		}
		out = append(out, proto.Clone(e).(*v1.Event))
	}
	sortByHLC(out)
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (s *Events) Gaps(_ context.Context, from, to uint64) ([]*v1.Gap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*v1.Gap
	for _, g := range s.gaps {
		if g.GetTo().GetWallMs() < from || g.GetFrom().GetWallMs() > to {
			continue
		}
		out = append(out, proto.Clone(g).(*v1.Gap))
	}
	return out, nil
}

func (s *Events) Traces(ctx context.Context, q store.TraceQuery) ([]store.TraceSummary, error) {
	events, err := s.Window(ctx, store.WindowQuery{FromWallMS: q.FromWallMS, ToWallMS: q.ToWallMS})
	if err != nil {
		return nil, err
	}
	return store.SummarizeTraces(events, q), nil
}

func (s *Events) Close() error { return nil }

// hlcLess reports whether a sorts before b under (wall_ms, logical,
// emitter_id). Wall clock is never consulted.
func hlcLess(a, b *v1.Event) bool {
	ah, bh := a.GetHlc(), b.GetHlc()
	if ah.GetWallMs() != bh.GetWallMs() {
		return ah.GetWallMs() < bh.GetWallMs()
	}
	if ah.GetLogical() != bh.GetLogical() {
		return ah.GetLogical() < bh.GetLogical()
	}
	return bytes.Compare(a.GetId().GetEmitterId(), b.GetId().GetEmitterId()) < 0
}

// sortByHLC orders events by (wall_ms, logical, emitter_id). Wall clock is
// never consulted.
func sortByHLC(e []*v1.Event) {
	sort.SliceStable(e, func(i, j int) bool { return hlcLess(e[i], e[j]) })
}

// Blobs is an in-memory content-addressed BlobStore.
type Blobs struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewBlobs returns an empty in-memory BlobStore.
func NewBlobs() *Blobs { return &Blobs{data: map[string][]byte{}} }

// IDFor returns the opaque blob ID for a content hash. The current
// implementation is the hex hash; callers must not depend on that.
func (b *Blobs) IDFor(sha [32]byte) string { return hex.EncodeToString(sha[:]) }

func (b *Blobs) Put(_ context.Context, sha [32]byte, body []byte) (string, error) {
	id := b.IDFor(sha)
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.data[id]; !exists {
		b.data[id] = append([]byte(nil), body...)
	}
	return id, nil
}

var errNoBlob = errors.New("mnemosyne: blob not found")

func (b *Blobs) Get(_ context.Context, id string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	body, ok := b.data[id]
	if !ok {
		return nil, errNoBlob
	}
	return append([]byte(nil), body...), nil
}

func (b *Blobs) Has(_ context.Context, shas [][32]byte) ([][32]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var missing [][32]byte
	for _, s := range shas {
		if _, ok := b.data[b.IDFor(s)]; !ok {
			missing = append(missing, s)
		}
	}
	return missing, nil
}

func (b *Blobs) Close() error { return nil }
