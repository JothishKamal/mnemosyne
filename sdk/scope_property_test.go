package sdk_test

import (
	"context"
	"testing"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
	"pgregory.net/rapid"
)

// TestScopeInvariantUnderRandomizedNesting builds arbitrary nesting trees and
// asserts that every emitted event's parent is exactly its lexically enclosing
// scope — the invariant the whole causal model rests on.
func TestScopeInvariantUnderRandomizedNesting(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		rec, sink := newTestRecorderRapid(rt)

		// expected maps an event ID sequence to the sequence of its intended
		// parent (0 meaning "trace root, no parent").
		expected := map[uint64]uint64{}

		var build func(ctx context.Context, parent identity.EventID, depth int)
		build = func(ctx context.Context, parent identity.EventID, depth int) {
			n := rapid.IntRange(0, 3).Draw(rt, "children")
			for i := 0; i < n; i++ {
				h := rec.Begin(ctx, sdk.Local{Name: "op"})
				expected[h.ID().Sequence] = parent.Sequence
				if depth < 3 && rapid.Bool().Draw(rt, "recurse") {
					build(h.Context(), h.ID(), depth+1)
				}
				h.End()
			}
		}
		build(context.Background(), identity.EventID{}, 0)

		for _, e := range sink.ends() {
			want, ok := expected[e.Id.Sequence]
			if !ok {
				rt.Fatalf("emitted an event with unknown id %d", e.Id.Sequence)
			}
			var got uint64
			if e.Parent != nil {
				got = e.Parent.Sequence
			}
			if got != want {
				rt.Fatalf("event %d: parent = %d, want %d", e.Id.Sequence, got, want)
			}
		}

		// Every BEGIN must have a matching END in this test, which always
		// closes what it opens.
		var begins, ends int
		sink.mu.Lock()
		for _, e := range sink.events {
			switch e.Phase {
			case v1.Phase_BEGIN:
				begins++
			case v1.Phase_END:
				ends++
			}
		}
		sink.mu.Unlock()
		if begins != ends {
			rt.Fatalf("%d BEGIN events but %d END events", begins, ends)
		}
	})
}

func newTestRecorderRapid(rt *rapid.T) (*sdk.Recorder, *capturingSink) {
	rt.Helper()
	sink := &capturingSink{}
	rec, err := sdk.New(sdk.Config{Service: "prop", Sink: sink, Enabled: true})
	if err != nil {
		rt.Fatalf("sdk.New: %v", err)
	}
	return rec, sink
}
