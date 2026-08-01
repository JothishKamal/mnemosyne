package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store/blobfs"
	"github.com/jothishkamal/mnemosyne/store/sqlite"
)

// TestSurvivesReopen proves durability, which the in-memory backends cannot:
// data written before Close must still be present, and still correctly
// HLC-ordered, after reopening the store from the same path.
func TestSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")
	blobDir := filepath.Join(dir, "blobs")

	tr := make([]byte, 16)
	tr[0] = 7

	mk := func(seq uint64, wallMS uint64, logical uint32, emitter string) *v1.Event {
		return &v1.Event{
			SchemaVersion: 1,
			TraceId:       tr,
			Id:            &v1.EventID{EmitterId: []byte(emitter), Sequence: seq},
			Service:       &v1.Service{Name: "svc"},
			Hlc:           &v1.HLC{WallMs: wallMS, Logical: logical},
			WallUnixNanos: int64(wallMS) * 1e6,
			Phase:         v1.Phase_END,
			OperationType: v1.OperationType_HTTP_SERVER,
			Operation:     &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/r"}},
			Status:        v1.Status_OK,
		}
	}

	// Deliberately out of HLC order on write, mirroring HLCTieBreakOrdering
	// in the conformance suite, so a backend that merely preserves insertion
	// order (or loses ordering across a reopen) would fail this check too.
	late := mk(2, 2000, 0, "emitterZ")
	early := mk(1, 1000, 5, "emitterA")

	es, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	if err := es.AppendEvents(ctx, []*v1.Event{late, early}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
	if err := es.AppendGaps(ctx, []*v1.Gap{{
		EmitterId:    []byte("emitterA"),
		From:         &v1.HLC{WallMs: 500},
		To:           &v1.HLC{WallMs: 900},
		DroppedCount: 3,
	}}); err != nil {
		t.Fatalf("AppendGaps: %v", err)
	}

	bs, err := blobfs.Open(blobDir)
	if err != nil {
		t.Fatalf("blobfs.Open: %v", err)
	}
	body := []byte("durable payload")
	var sum [32]byte
	copy(sum[:], []byte("0123456789abcdef0123456789abcde"))
	blobID, err := bs.Put(ctx, sum, body)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := es.Close(); err != nil {
		t.Fatalf("Close (events): %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close (blobs): %v", err)
	}

	// Reopen from the same paths, a fresh process would do exactly this.
	es2, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open (reopen): %v", err)
	}
	defer es2.Close()

	got, err := es2.TraceEvents(ctx, tr)
	if err != nil {
		t.Fatalf("TraceEvents (after reopen): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events after reopen, want 2", len(got))
	}
	if got[0].GetId().GetSequence() != 1 || got[1].GetId().GetSequence() != 2 {
		t.Fatalf("events not HLC-ordered after reopen: sequences %d, %d (want 1, 2)",
			got[0].GetId().GetSequence(), got[1].GetId().GetSequence())
	}

	gaps, err := es2.Gaps(ctx, 0, 9999)
	if err != nil {
		t.Fatalf("Gaps (after reopen): %v", err)
	}
	if len(gaps) != 1 || gaps[0].DroppedCount != 3 {
		t.Fatalf("gap not durable across reopen: %v", gaps)
	}

	bs2, err := blobfs.Open(blobDir)
	if err != nil {
		t.Fatalf("blobfs.Open (reopen): %v", err)
	}
	defer bs2.Close()
	gotBody, err := bs2.Get(ctx, blobID)
	if err != nil {
		t.Fatalf("Get (after reopen): %v", err)
	}
	if string(gotBody) != string(body) {
		t.Fatalf("blob not durable across reopen: got %q, want %q", gotBody, body)
	}
}
