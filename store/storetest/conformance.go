// Package storetest holds the conformance suite every store backend must pass.
package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store"
)

func ev(trace byte, seq uint64, wallMS uint64, logical uint32, svc string, status v1.Status) *v1.Event {
	tr := make([]byte, 16)
	tr[0] = trace
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       tr,
		Id:            &v1.EventID{EmitterId: []byte("emitter0"), Sequence: seq},
		Service:       &v1.Service{Name: svc},
		Hlc:           &v1.HLC{WallMs: wallMS, Logical: logical},
		WallUnixNanos: int64(wallMS) * 1e6,
		Phase:         v1.Phase_END,
		OperationType: v1.OperationType_HTTP_SERVER,
		Operation:     &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/r"}},
		Status:        status,
	}
}

// RunConformance exercises the contract both store interfaces must satisfy.
func RunConformance(t *testing.T, newEvent func(t *testing.T) store.EventStore, newBlob func(t *testing.T) store.BlobStore) {
	t.Run("AppendAndReadTrace", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		want := []*v1.Event{ev(1, 1, 1000, 0, "a", v1.Status_OK), ev(1, 2, 1000, 1, "a", v1.Status_OK)}
		if err := es.AppendEvents(ctx, want); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}
		tr := make([]byte, 16)
		tr[0] = 1
		got, err := es.TraceEvents(ctx, tr)
		if err != nil {
			t.Fatalf("TraceEvents: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d events, want 2", len(got))
		}
	})

	t.Run("WindowOrdersByHLCNotWallClock", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		// Two events whose HLC order is the reverse of their wall-clock order,
		// which is what a skewed emitter produces.
		early := ev(1, 1, 5000, 0, "a", v1.Status_OK)
		early.WallUnixNanos = 9_000_000_000
		late := ev(1, 2, 6000, 0, "a", v1.Status_OK)
		late.WallUnixNanos = 1_000_000_000

		if err := es.AppendEvents(ctx, []*v1.Event{late, early}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}
		got, err := es.Window(ctx, store.WindowQuery{FromWallMS: 0, ToWallMS: 10000, Limit: 10})
		if err != nil {
			t.Fatalf("Window: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d events, want 2", len(got))
		}
		if got[0].Hlc.WallMs != 5000 {
			t.Fatalf("first event has hlc %v; window ordered by wall clock, not HLC", got[0].Hlc)
		}
	})

	t.Run("TracesReportsErrorFlag", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		if err := es.AppendEvents(ctx, []*v1.Event{
			ev(1, 1, 1000, 0, "a", v1.Status_OK),
			ev(1, 2, 1001, 0, "a", v1.Status_ERROR),
			ev(2, 3, 1002, 0, "b", v1.Status_OK),
		}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}
		got, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 10})
		if err != nil {
			t.Fatalf("Traces: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d traces, want 2", len(got))
		}
		var errored int
		for _, s := range got {
			if s.HasError {
				errored++
			}
		}
		if errored != 1 {
			t.Fatalf("%d traces flagged with errors, want 1", errored)
		}
	})

	t.Run("GapsRoundTrip", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		if err := es.AppendGaps(ctx, []*v1.Gap{{
			EmitterId:    []byte("emitter0"),
			From:         &v1.HLC{WallMs: 1000},
			To:           &v1.HLC{WallMs: 2000},
			DroppedCount: 42,
			Reason:       v1.GapReason_BACKPRESSURE,
		}}); err != nil {
			t.Fatalf("AppendGaps: %v", err)
		}
		got, err := es.Gaps(ctx, 0, 9999)
		if err != nil {
			t.Fatalf("Gaps: %v", err)
		}
		if len(got) != 1 || got[0].DroppedCount != 42 {
			t.Fatalf("gaps round-trip failed: %v", got)
		}
	})

	t.Run("BlobDeduplicatesIdenticalContent", func(t *testing.T) {
		bs := newBlob(t)
		defer bs.Close()
		ctx := context.Background()

		body := []byte("identical payload")
		sum := sha256.Sum256(body)

		id1, err := bs.Put(ctx, sum, body)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		id2, err := bs.Put(ctx, sum, body)
		if err != nil {
			t.Fatalf("Put (second): %v", err)
		}
		if id1 != id2 {
			t.Fatalf("identical content produced different blob IDs: %q and %q", id1, id2)
		}
		got, err := bs.Get(ctx, id1)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("Get returned %q, want %q", got, body)
		}
	})

	t.Run("HasReportsOnlyMissing", func(t *testing.T) {
		bs := newBlob(t)
		defer bs.Close()
		ctx := context.Background()

		present := sha256.Sum256([]byte("present"))
		absent := sha256.Sum256([]byte("absent"))
		if _, err := bs.Put(ctx, present, []byte("present")); err != nil {
			t.Fatalf("Put: %v", err)
		}

		missing, err := bs.Has(ctx, [][32]byte{present, absent})
		if err != nil {
			t.Fatalf("Has: %v", err)
		}
		if len(missing) != 1 || missing[0] != absent {
			t.Fatalf("Has returned %v, want exactly the absent hash", missing)
		}
	})

	t.Run("GetUnknownBlobReturnsError", func(t *testing.T) {
		bs := newBlob(t)
		defer bs.Close()
		if _, err := bs.Get(context.Background(), "definitely-not-a-blob"); err == nil {
			t.Fatal("Get of an unknown blob ID succeeded")
		}
	})
}
