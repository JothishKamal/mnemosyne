package sdk_test

import (
	"context"
	"testing"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
)

// panickingSink is a Sink whose Emit always panics, standing in for a broken
// exporter, a bad type assertion in a future adapter, or any other bug in the
// recording path. Recording is always what gets sacrificed: this must never
// propagate out of the SDK.
type panickingSink struct{}

func (panickingSink) Emit(*v1.Event) {
	panic("boom: recording exploded")
}

func newPanicRecorder(t *testing.T) *sdk.Recorder {
	t.Helper()
	rec, err := sdk.New(sdk.Config{Service: "panic-svc", Sink: panickingSink{}, Enabled: true})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	return rec
}

// TestBeginContainsSinkPanic verifies that a panicking Sink during Begin
// cannot break the API contract: the caller must still get back a usable
// Handle with a non-nil Context, and End on that Handle must not panic
// either.
func TestBeginContainsSinkPanic(t *testing.T) {
	rec := newPanicRecorder(t)

	h := rec.Begin(context.Background(), sdk.Local{Name: "op"})
	if h == nil {
		t.Fatal("Begin returned a nil Handle")
	}
	if h.Context() == nil {
		t.Fatal("Handle.Context() is nil after a recording panic")
	}

	// End must not panic even though the operation is inert.
	h.End()

	if rec.PanicCount() == 0 {
		t.Fatal("PanicCount() is zero after a Sink panic in Begin")
	}
}

// TestInstantContainsSinkPanic verifies the same containment for Instant,
// which has no Handle to fall back on and so must simply not panic.
func TestInstantContainsSinkPanic(t *testing.T) {
	rec := newPanicRecorder(t)

	rec.Instant(context.Background(), sdk.Local{Name: "instant-op"})

	if rec.PanicCount() == 0 {
		t.Fatal("PanicCount() is zero after a Sink panic in Instant")
	}
}

// TestNormalRecorderNeverContains proves containment does not fire
// spuriously: a Recorder wired to a well-behaved Sink must leave PanicCount
// at zero across a normal Begin/End and Instant.
func TestNormalRecorderNeverContains(t *testing.T) {
	sk := &capturingSink{}
	rec, err := sdk.New(sdk.Config{Service: "healthy-svc", Sink: sk, Enabled: true})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	h := rec.Begin(context.Background(), sdk.Local{Name: "op"})
	h.End()
	rec.Instant(context.Background(), sdk.Local{Name: "instant-op"})

	if got := rec.PanicCount(); got != 0 {
		t.Fatalf("PanicCount() = %d on a healthy recorder, want 0", got)
	}
}
