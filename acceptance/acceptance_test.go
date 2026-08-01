// Package acceptance holds the Spec 1 definition of done. Every subtest maps to
// one row of the assertion table in the frozen design document.
package acceptance_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jothishkamal/mnemosyne/harness"
	"github.com/jothishkamal/mnemosyne/replay"
	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store"
	"github.com/jothishkamal/mnemosyne/store/blobfs"
	"github.com/jothishkamal/mnemosyne/store/sqlite"
)

// storeSink writes every recorded event straight into an EventStore, so the
// acceptance suite exercises the durable storage path rather than depending
// on exporter timing. It also buffers events locally (safe for concurrent
// use) so tests can inspect the recording from a different goroutine than the
// one driving the request.
type storeSink struct {
	t  *testing.T
	es store.EventStore

	mu     sync.Mutex
	events []*v1.Event
}

func (s *storeSink) Emit(e *v1.Event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	if err := s.es.AppendEvents(context.Background(), []*v1.Event{e}); err != nil {
		s.t.Errorf("AppendEvents: %v", err)
	}
}

func (s *storeSink) all() []*v1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*v1.Event(nil), s.events...)
}

// runScenario drives the harness's AcceptanceScenario to completion against
// sink, following the exact sequencing harness/harness_test.go requires: run
// the request in the background, wait until the harness signals it has
// actually reached the hang, then Close (which releases the hang), then wait
// for the request goroutine to finish. A naive synchronous h.Run(...) races
// Close's teardown against the call chain and records nothing for the hung
// leg.
func runScenario(t *testing.T, h *harness.Harness) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.Run("checkout-api", "/checkout"); err == nil {
			resp.Body.Close()
		}
	}()
	if err := h.WaitForHang(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	h.Close()
	<-done
}

// recording runs the acceptance scenario into durable storage and returns the
// stores plus every recorded event.
func recording(t *testing.T) (store.EventStore, store.BlobStore, []*v1.Event) {
	t.Helper()
	dir := t.TempDir()

	es, err := sqlite.Open(filepath.Join(dir, "events.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { es.Close() })

	bs, err := blobfs.Open(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatalf("blobfs.Open: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	// A sink that writes straight into the store, so the test exercises the
	// storage layer without depending on exporter timing.
	sink := &storeSink{t: t, es: es}
	h := harness.AcceptanceScenario(t, sink)
	runScenario(t, h)

	return es, bs, sink.all()
}

func TestRemoteCallEdgesSpanAllThreeProcesses(t *testing.T) {
	_, _, events := recording(t)

	services := map[string]bool{}
	var remote int
	for _, e := range events {
		services[e.GetService().GetName()] = true
		for _, l := range e.Links {
			if l.Type == v1.EdgeType_REMOTE_CALL {
				remote++
			}
		}
	}
	for _, want := range []string{"checkout-api", "inventory-svc", "pricing-svc"} {
		if !services[want] {
			t.Fatalf("no events from %s", want)
		}
	}
	if remote == 0 {
		t.Fatal("no REMOTE_CALL edges: propagation over traceparent/tracestate failed")
	}
}

func TestRetryAttemptsAreSiblingsUnderOneScope(t *testing.T) {
	_, _, events := recording(t)

	ends := map[uint64]*v1.Event{}
	for _, e := range events {
		if e.Phase == v1.Phase_END {
			ends[e.Id.Sequence] = e
		}
	}

	parents := map[uint64]bool{}
	var retryLinks int
	for _, e := range ends {
		for _, l := range e.Links {
			if l.Type != v1.EdgeType_RETRY_OF {
				continue
			}
			retryLinks++
			// A retry and the attempt it replaces must share a parent: neither
			// contains the other.
			parents[e.GetParent().GetSequence()] = true
			if prev := ends[l.Event.GetSequence()]; prev != nil {
				parents[prev.GetParent().GetSequence()] = true
			}
		}
	}
	if retryLinks == 0 {
		t.Fatal("no RETRY_OF edges recorded")
	}
	if len(parents) != 1 {
		t.Fatalf("retry attempts have %d distinct parents, want 1, they were nested, not siblings", len(parents))
	}
}

func TestHungCallAppearsAsUnmatchedBegin(t *testing.T) {
	es, bs, _ := recording(t)

	m, err := replay.New(es, bs).Timeline(context.Background(), replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1, Resolution: 100,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.InFlight) == 0 {
		t.Fatal("the hung operation was not reported as in-flight")
	}
}

func TestOrderHoldsUnderClockSkew(t *testing.T) {
	es, bs, _ := recording(t)

	m, err := replay.New(es, bs).Timeline(context.Background(), replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1, Resolution: 100,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	var found bool
	for _, s := range m.SkewWarnings {
		if s.Service == "pricing-svc" {
			found = true
		}
	}
	if !found {
		t.Fatal("pricing-svc runs 400ms behind but produced no skew warning")
	}

	// HLC order must still be monotonic across the whole recording despite the
	// skew.
	events, err := es.Window(context.Background(), store.WindowQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1,
	})
	if err != nil {
		t.Fatalf("Window: %v", err)
	}
	for i := 1; i < len(events); i++ {
		prev, cur := events[i-1].GetHlc(), events[i].GetHlc()
		if cur.WallMs < prev.WallMs {
			t.Fatalf("event %d has HLC %v, out of order after %v", i, cur, prev)
		}
	}
}

func TestGapsAreRecordedNotSilent(t *testing.T) {
	es, bs, _ := recording(t)
	ctx := context.Background()

	// Simulate the collector being unavailable during the incident.
	if err := es.AppendGaps(ctx, []*v1.Gap{{
		EmitterId: []byte("emitter0"),
		From:      &v1.HLC{WallMs: 1}, To: &v1.HLC{WallMs: 1 << 40},
		DroppedCount: 12, Reason: v1.GapReason_UNREACHABLE,
	}}); err != nil {
		t.Fatalf("AppendGaps: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1, Resolution: 100,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.Gaps) == 0 {
		t.Fatal("dropped events produced no gap marker: the timeline would show silence")
	}
	if m.Gaps[0].DroppedCount != 12 {
		t.Fatalf("gap reports %d drops, want 12", m.Gaps[0].DroppedCount)
	}
}

func TestChainReadsAsTheIncidentNarrative(t *testing.T) {
	es, bs, events := recording(t)
	ctx := context.Background()

	// Start from the hung operation and walk back to the root.
	var hung *v1.EventID
	ends := map[uint64]bool{}
	for _, e := range events {
		if e.Phase == v1.Phase_END {
			ends[e.Id.Sequence] = true
		}
	}
	for _, e := range events {
		if e.Phase == v1.Phase_BEGIN && !ends[e.Id.Sequence] {
			hung = e.Id
			break
		}
	}
	if hung == nil {
		t.Fatal("no unmatched BEGIN to start the chain from")
	}

	chain, err := replay.New(es, bs).Chain(ctx, events[0].TraceId, hung, 200)
	if err != nil {
		t.Fatalf("Chain: %v", err)
	}
	if len(chain.Nodes) < 2 {
		t.Fatalf("chain has %d nodes; the causal path back to the root is missing", len(chain.Nodes))
	}
}

func TestRecordingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")

	es, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sink := &storeSink{t: t, es: es}
	h := harness.AcceptanceScenario(t, sink)
	runScenario(t, h)

	events := sink.all()
	traceID := events[0].TraceId
	if err := es.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.TraceEvents(context.Background(), traceID)
	if err != nil {
		t.Fatalf("TraceEvents: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("recording did not survive a restart")
	}
}
