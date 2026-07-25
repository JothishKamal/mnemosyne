package sdk_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
)

// capturingSink records every emitted event for assertion.
type capturingSink struct {
	mu     sync.Mutex
	events []*v1.Event
}

func (s *capturingSink) Emit(e *v1.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *capturingSink) ends() []*v1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*v1.Event
	for _, e := range s.events {
		if e.Phase == v1.Phase_END {
			out = append(out, e)
		}
	}
	return out
}

func newTestRecorder(t *testing.T) (*sdk.Recorder, *capturingSink) {
	t.Helper()
	sink := &capturingSink{}
	ms := int64(1_700_000_000_000)
	rec, err := sdk.New(sdk.Config{
		Service: "test-svc",
		Version: "0.0.1",
		Sink:    sink,
		Enabled: true,
		Now: func() time.Time {
			ms++
			return time.UnixMilli(ms)
		},
	})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	return rec, sink
}

// TestSequentialOperationsAreSiblings is the regression test for the
// linked-list bug: recording an operation must not change what the enclosing
// scope sees.
func TestSequentialOperationsAreSiblings(t *testing.T) {
	rec, sink := newTestRecorder(t)
	ctx := context.Background()

	root := rec.Begin(ctx, sdk.HTTPServer{Method: "POST", Route: "/checkout"})
	inner := root.Context()

	a := rec.Begin(inner, sdk.Local{Name: "a"})
	a.End()
	b := rec.Begin(inner, sdk.Local{Name: "b"})
	b.End()
	root.End()

	byID := map[uint64]*v1.Event{}
	for _, e := range sink.ends() {
		byID[e.Id.Sequence] = e
	}
	ea, eb := byID[a.ID().Sequence], byID[b.ID().Sequence]
	if ea == nil || eb == nil {
		t.Fatal("missing END events for a and b")
	}
	if ea.Parent.Sequence != root.ID().Sequence {
		t.Fatalf("a.parent = %d, want root %d", ea.Parent.Sequence, root.ID().Sequence)
	}
	if eb.Parent.Sequence != root.ID().Sequence {
		t.Fatalf("b.parent = %d, want root %d (b was made a child of a — the linked-list bug)",
			eb.Parent.Sequence, root.ID().Sequence)
	}
}

func TestNestedOperationsAreNested(t *testing.T) {
	rec, sink := newTestRecorder(t)
	outer := rec.Begin(context.Background(), sdk.Local{Name: "outer"})
	inner := rec.Begin(outer.Context(), sdk.Local{Name: "inner"})
	inner.End()
	outer.End()

	for _, e := range sink.ends() {
		if e.Id.Sequence == inner.ID().Sequence && e.Parent.Sequence != outer.ID().Sequence {
			t.Fatalf("inner.parent = %d, want %d", e.Parent.Sequence, outer.ID().Sequence)
		}
	}
}

func TestEndIsIdempotent(t *testing.T) {
	rec, sink := newTestRecorder(t)
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.End()
	h.End()
	if got := len(sink.ends()); got != 1 {
		t.Fatalf("got %d END events, want 1", got)
	}
}

func TestBeginEmitsExactlyOneBeginPhase(t *testing.T) {
	rec, sink := newTestRecorder(t)
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	sink.mu.Lock()
	n := len(sink.events)
	phase := sink.events[0].Phase
	sink.mu.Unlock()
	if n != 1 || phase != v1.Phase_BEGIN {
		t.Fatalf("after Begin: %d events, first phase %v; want 1 BEGIN", n, phase)
	}
	h.End()
}

func TestFailSetsErrorStatus(t *testing.T) {
	rec, sink := newTestRecorder(t)
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.Fail(errors.New("boom"))
	h.End()

	e := sink.ends()[0]
	if e.Status != v1.Status_ERROR {
		t.Fatalf("status = %v, want ERROR", e.Status)
	}
	if e.Error == nil || e.Error.Message != "boom" {
		t.Fatalf("error = %v, want message %q", e.Error, "boom")
	}
}

func TestNilHandleMethodsAreSafe(t *testing.T) {
	var h *sdk.Handle
	h.SetStatus(v1.Status_OK)
	h.Fail(errors.New("x"))
	h.AddLink(nil)
	h.End()
	if !h.ID().IsZero() {
		t.Fatal("nil handle returned a non-zero ID")
	}
}

func TestDisabledRecorderEmitsNothing(t *testing.T) {
	sink := &capturingSink{}
	rec, err := sdk.New(sdk.Config{Service: "s", Sink: sink, Enabled: false})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.End()
	rec.Instant(context.Background(), sdk.Local{Name: "y"})

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 0 {
		t.Fatalf("disabled recorder emitted %d events", len(sink.events))
	}
	if h.Context() == nil {
		t.Fatal("disabled handle returned a nil context")
	}
}

func TestForeignRemoteParentBecomesLinkNotParent(t *testing.T) {
	rec, sink := newTestRecorder(t)
	var tr identity.TraceID
	tr[0] = 9
	var sp identity.SpanID
	sp[0] = 7

	h := rec.BeginRemote(context.Background(), sdk.Remote{
		TraceID: tr, ParentSpan: sp, Foreign: true,
	}, sdk.HTTPServer{Method: "GET", Route: "/x"})
	h.End()

	e := sink.ends()[0]
	if e.Parent != nil && e.Parent.Sequence != 0 {
		t.Fatalf("foreign remote set a parent: %v", e.Parent)
	}
	if len(e.Links) != 1 || e.Links[0].Type != v1.EdgeType_REMOTE_CALL {
		t.Fatalf("expected one REMOTE_CALL link, got %v", e.Links)
	}
	if string(e.Links[0].ForeignSpanId) != string(sp[:]) {
		t.Fatal("foreign span id not carried on the link")
	}
}
