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

	t.Run("IDForMatchesPutResult", func(t *testing.T) {
		bs := newBlob(t)
		defer bs.Close()
		ctx := context.Background()

		body := []byte("some content")
		sum := sha256.Sum256(body)

		id, err := bs.Put(ctx, sum, body)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if got := bs.IDFor(sum); got != id {
			t.Fatalf("IDFor(sha) = %q, want %q (the ID Put returned for the same content)", got, id)
		}
	})

	t.Run("MutatingReturnedEventDoesNotCorruptStore", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		tr := make([]byte, 16)
		tr[0] = 1
		if err := es.AppendEvents(ctx, []*v1.Event{ev(1, 1, 1000, 0, "a", v1.Status_OK)}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		got, err := es.TraceEvents(ctx, tr)
		if err != nil {
			t.Fatalf("TraceEvents: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d events, want 1", len(got))
		}
		got[0].Status = v1.Status_ERROR
		got[0].Service.Name = "mutated"

		again, err := es.TraceEvents(ctx, tr)
		if err != nil {
			t.Fatalf("TraceEvents (second): %v", err)
		}
		if len(again) != 1 {
			t.Fatalf("got %d events on reread, want 1", len(again))
		}
		if again[0].Status != v1.Status_OK {
			t.Fatalf("mutation of returned event leaked into store: Status = %v, want OK", again[0].Status)
		}
		if again[0].GetService().GetName() != "a" {
			t.Fatalf("mutation of returned event leaked into store: Service.Name = %q, want %q", again[0].GetService().GetName(), "a")
		}
	})

	t.Run("HLCTieBreakOrdering", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		tr := make([]byte, 16)
		tr[0] = 9

		mk := func(seq uint64, wallMS uint64, logical uint32, emitter string) *v1.Event {
			e := ev(9, seq, wallMS, logical, "a", v1.Status_OK)
			e.Id.EmitterId = []byte(emitter)
			return e
		}

		// Same wall_ms, different logical.
		logicalHigh := mk(1, 1000, 5, "emitter0")
		logicalLow := mk(2, 1000, 1, "emitter0")
		// Same wall_ms and logical, different emitter_id.
		emitterB := mk(3, 2000, 0, "emitterB")
		emitterA := mk(4, 2000, 0, "emitterA")

		// Append in an order that is not the expected output order, so a
		// backend that merely preserves insertion order would fail.
		if err := es.AppendEvents(ctx, []*v1.Event{emitterB, logicalHigh, emitterA, logicalLow}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		got, err := es.TraceEvents(ctx, tr)
		if err != nil {
			t.Fatalf("TraceEvents: %v", err)
		}
		if len(got) != 4 {
			t.Fatalf("got %d events, want 4", len(got))
		}
		want := []uint64{2, 1, 4, 3} // logicalLow, logicalHigh, emitterA, emitterB
		for i, w := range want {
			if got[i].GetId().GetSequence() != w {
				t.Fatalf("event %d: got sequence %d, want %d (full order: %v)", i, got[i].GetId().GetSequence(), w, seqsOf(got))
			}
		}
	})

	t.Run("WindowBoundariesAreInclusive", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		below := ev(1, 1, 999, 0, "a", v1.Status_OK)
		atFrom := ev(1, 2, 1000, 0, "a", v1.Status_OK)
		atTo := ev(1, 3, 2000, 0, "a", v1.Status_OK)
		above := ev(1, 4, 2001, 0, "a", v1.Status_OK)

		if err := es.AppendEvents(ctx, []*v1.Event{below, atFrom, atTo, above}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		got, err := es.Window(ctx, store.WindowQuery{FromWallMS: 1000, ToWallMS: 2000, Limit: 10})
		if err != nil {
			t.Fatalf("Window: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d events, want exactly the 2 boundary events: %v", len(got), seqsOf(got))
		}
		seen := map[uint64]bool{}
		for _, e := range got {
			seen[e.GetId().GetSequence()] = true
		}
		if !seen[2] || !seen[3] {
			t.Fatalf("boundary events missing from result: got sequences %v, want 2 and 3", seqsOf(got))
		}
	})

	t.Run("WindowFiltersByService", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		api := ev(1, 1, 1000, 0, "api", v1.Status_OK)
		apiGateway := ev(1, 2, 1000, 1, "api-gateway", v1.Status_OK)
		worker := ev(1, 3, 1000, 2, "worker", v1.Status_OK)

		if err := es.AppendEvents(ctx, []*v1.Event{api, apiGateway, worker}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		got, err := es.Window(ctx, store.WindowQuery{FromWallMS: 0, ToWallMS: 9999, Services: []string{"api"}, Limit: 10})
		if err != nil {
			t.Fatalf("Window: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d events, want 1 (exact service match, not substring): %v", len(got), seqsOf(got))
		}
		if got[0].GetService().GetName() != "api" {
			t.Fatalf("got service %q, want %q", got[0].GetService().GetName(), "api")
		}
	})

	t.Run("WindowLimitTruncates", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		var events []*v1.Event
		for i := uint64(0); i < 5; i++ {
			events = append(events, ev(1, i+1, 1000+i, 0, "a", v1.Status_OK))
		}
		if err := es.AppendEvents(ctx, events); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		got, err := es.Window(ctx, store.WindowQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 3})
		if err != nil {
			t.Fatalf("Window: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d events, want 3 (limit truncation)", len(got))
		}
		want := []uint64{1, 2, 3}
		for i, w := range want {
			if got[i].GetId().GetSequence() != w {
				t.Fatalf("event %d: got sequence %d, want %d (should be the first 3 by HLC order): %v", i, got[i].GetId().GetSequence(), w, seqsOf(got))
			}
		}
	})

	t.Run("TracesFiltersByServiceAndErrors", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		if err := es.AppendEvents(ctx, []*v1.Event{
			ev(1, 1, 1000, 0, "a", v1.Status_OK),    // trace1: service a, ok
			ev(2, 2, 1001, 0, "b", v1.Status_ERROR), // trace2: service b, error
			ev(3, 3, 1002, 0, "a", v1.Status_ERROR), // trace3: service a, error
			ev(4, 4, 1003, 0, "b", v1.Status_OK),    // trace4: service b, ok
		}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		byService, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Service: "a", Limit: 10})
		if err != nil {
			t.Fatalf("Traces (service filter): %v", err)
		}
		if len(byService) != 2 {
			t.Fatalf("service filter: got %d traces, want 2", len(byService))
		}
		for _, s := range byService {
			if s.RootService != "a" {
				t.Fatalf("service filter: got trace with RootService %q, want only %q", s.RootService, "a")
			}
		}

		byError, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, OnlyErrors: true, Limit: 10})
		if err != nil {
			t.Fatalf("Traces (errors filter): %v", err)
		}
		if len(byError) != 2 {
			t.Fatalf("errors filter: got %d traces, want 2", len(byError))
		}
		for _, s := range byError {
			if !s.HasError {
				t.Fatalf("errors filter: got trace without HasError set")
			}
		}

		limited, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 1})
		if err != nil {
			t.Fatalf("Traces (limit): %v", err)
		}
		if len(limited) != 1 {
			t.Fatalf("limit: got %d traces, want 1", len(limited))
		}
	})

	t.Run("TraceSummaryFieldsArePopulated", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		if err := es.AppendEvents(ctx, []*v1.Event{
			ev(5, 1, 1000, 0, "a", v1.Status_OK),
			ev(5, 2, 1500, 0, "a", v1.Status_OK),
			ev(5, 3, 2000, 0, "a", v1.Status_OK),
		}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		got, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 10})
		if err != nil {
			t.Fatalf("Traces: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d traces, want 1", len(got))
		}
		sum := got[0]
		tr := make([]byte, 16)
		tr[0] = 5
		if !bytes.Equal(sum.TraceID, tr) {
			t.Fatalf("TraceID = %x, want %x", sum.TraceID, tr)
		}
		if sum.StartWallMS != 1000 {
			t.Fatalf("StartWallMS = %d, want 1000", sum.StartWallMS)
		}
		if sum.EndWallMS != 2000 {
			t.Fatalf("EndWallMS = %d, want 2000", sum.EndWallMS)
		}
		if sum.EventCount != 3 {
			t.Fatalf("EventCount = %d, want 3", sum.EventCount)
		}
	})

	t.Run("TraceSummaryRootIsEarliestEvent", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		root := ev(6, 1, 1000, 0, "root-service", v1.Status_OK)
		root.Operation = &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/root"}}
		notRoot := ev(6, 2, 2000, 0, "later-service", v1.Status_OK)
		notRoot.Operation = &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/later"}}

		// Append with the later event first, so an implementation that takes
		// root fields from whichever event it encounters first would fail.
		if err := es.AppendEvents(ctx, []*v1.Event{notRoot, root}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		got, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 10})
		if err != nil {
			t.Fatalf("Traces: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d traces, want 1", len(got))
		}
		sum := got[0]
		if sum.RootService != "root-service" {
			t.Fatalf("RootService = %q, want %q (earliest event by HLC)", sum.RootService, "root-service")
		}
		if sum.RootRoute != "/root" {
			t.Fatalf("RootRoute = %q, want %q (earliest event by HLC)", sum.RootRoute, "/root")
		}
	})

	t.Run("TracesOrderingIsDeterministicOnTies", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		traceHigh := make([]byte, 16)
		traceHigh[0] = 0xFF
		traceLow := make([]byte, 16)
		traceLow[0] = 0x01

		eHigh := ev(0, 1, 1000, 0, "a", v1.Status_OK)
		eHigh.TraceId = traceHigh
		eLow := ev(0, 2, 1000, 0, "a", v1.Status_OK)
		eLow.TraceId = traceLow

		if err := es.AppendEvents(ctx, []*v1.Event{eHigh, eLow}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}

		for i := 0; i < 3; i++ {
			got, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 10})
			if err != nil {
				t.Fatalf("Traces (call %d): %v", i, err)
			}
			if len(got) != 2 {
				t.Fatalf("call %d: got %d traces, want 2", i, len(got))
			}
			if !bytes.Equal(got[0].TraceID, traceLow) || !bytes.Equal(got[1].TraceID, traceHigh) {
				t.Fatalf("call %d: order = %x, %x; want TraceID-ascending tie-break: %x, %x",
					i, got[0].TraceID, got[1].TraceID, traceLow, traceHigh)
			}
		}
	})

	t.Run("GapsOverlapSemantics", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		inside := &v1.Gap{EmitterId: []byte("e1"), From: &v1.HLC{WallMs: 1200}, To: &v1.HLC{WallMs: 1300}, DroppedCount: 1}
		overlapLow := &v1.Gap{EmitterId: []byte("e2"), From: &v1.HLC{WallMs: 900}, To: &v1.HLC{WallMs: 1100}, DroppedCount: 2}
		overlapHigh := &v1.Gap{EmitterId: []byte("e3"), From: &v1.HLC{WallMs: 1900}, To: &v1.HLC{WallMs: 2100}, DroppedCount: 3}
		outside := &v1.Gap{EmitterId: []byte("e4"), From: &v1.HLC{WallMs: 3000}, To: &v1.HLC{WallMs: 4000}, DroppedCount: 4}

		if err := es.AppendGaps(ctx, []*v1.Gap{inside, overlapLow, overlapHigh, outside}); err != nil {
			t.Fatalf("AppendGaps: %v", err)
		}

		got, err := es.Gaps(ctx, 1000, 2000)
		if err != nil {
			t.Fatalf("Gaps: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d gaps, want 3 (inside + both partial overlaps, excluding the disjoint one)", len(got))
		}
		dropped := map[uint64]bool{}
		for _, g := range got {
			dropped[g.DroppedCount] = true
		}
		if !dropped[1] || !dropped[2] || !dropped[3] {
			t.Fatalf("got gaps with dropped counts %v, want 1, 2, and 3 present", dropped)
		}
		if dropped[4] {
			t.Fatalf("gap entirely outside the window was incorrectly included")
		}
	})

	t.Run("EmptyStoreQueriesReturnNoResults", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		tr := make([]byte, 16)
		events, err := es.TraceEvents(ctx, tr)
		if err != nil {
			t.Fatalf("TraceEvents: %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("TraceEvents on empty store returned %d events", len(events))
		}

		win, err := es.Window(ctx, store.WindowQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 10})
		if err != nil {
			t.Fatalf("Window: %v", err)
		}
		if len(win) != 0 {
			t.Fatalf("Window on empty store returned %d events", len(win))
		}

		traces, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 10})
		if err != nil {
			t.Fatalf("Traces: %v", err)
		}
		if len(traces) != 0 {
			t.Fatalf("Traces on empty store returned %d traces", len(traces))
		}

		gaps, err := es.Gaps(ctx, 0, 9999)
		if err != nil {
			t.Fatalf("Gaps: %v", err)
		}
		if len(gaps) != 0 {
			t.Fatalf("Gaps on empty store returned %d gaps", len(gaps))
		}
	})
}

// seqsOf returns the EventID.Sequence of each event, for diagnostic messages.
func seqsOf(events []*v1.Event) []uint64 {
	out := make([]uint64, len(events))
	for i, e := range events {
		out[i] = e.GetId().GetSequence()
	}
	return out
}
