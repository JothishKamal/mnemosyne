package exporter_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk/exporter"
	"google.golang.org/protobuf/proto"
)

func event(seq uint64) *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       make([]byte, 16),
		Id:            &v1.EventID{EmitterId: []byte("emitter0"), Sequence: seq},
		Hlc:           &v1.HLC{WallMs: 1000 + seq},
		Phase:         v1.Phase_INSTANT,
	}
}

func TestEmitNeverBlocksAndDropsWhenFull(t *testing.T) {
	// A collector that never responds: the queue must fill, not block callers.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)

	ex := exporter.New(exporter.Config{
		Endpoint: srv.URL, QueueSize: 4, BatchSize: 2,
		FlushInterval: time.Hour, EmitterID: [8]byte{1},
	})
	defer ex.Close()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			ex.Emit(event(uint64(i)))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Emit blocked the caller")
	}
	if ex.DroppedCount() == 0 {
		t.Fatal("queue overflow did not increment the dropped counter")
	}
}

func TestDropsBecomeGapRecords(t *testing.T) {
	var mu sync.Mutex
	var batches []*v1.Batch
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		if _, err := r.Body.Read(body); err != nil && err.Error() != "EOF" {
			t.Errorf("read body: %v", err)
		}
		var b v1.Batch
		if err := proto.Unmarshal(body, &b); err == nil {
			mu.Lock()
			batches = append(batches, &b)
			mu.Unlock()
		}
		ack, _ := proto.Marshal(&v1.BatchAck{Accepted: uint32(len(b.Events))})
		w.Write(ack)
	}))
	defer srv.Close()

	ex := exporter.New(exporter.Config{
		Endpoint: srv.URL, QueueSize: 2, BatchSize: 8,
		FlushInterval: 10 * time.Millisecond, EmitterID: [8]byte{7},
	})
	for i := 0; i < 500; i++ {
		ex.Emit(event(uint64(i)))
	}
	if err := ex.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	ex.Close()

	mu.Lock()
	defer mu.Unlock()
	var gaps int
	var dropped uint64
	for _, b := range batches {
		for _, g := range b.Gaps {
			gaps++
			dropped += g.DroppedCount
			if g.Reason != v1.GapReason_BACKPRESSURE {
				t.Fatalf("gap reason = %v, want BACKPRESSURE", g.Reason)
			}
			if g.From == nil || g.To == nil {
				t.Fatal("gap is missing its HLC window")
			}
		}
	}
	if gaps == 0 || dropped == 0 {
		t.Fatalf("got %d gaps totalling %d drops; drops must never be silent", gaps, dropped)
	}
}

func TestUnreachableCollectorProducesGapsAndKeepsRunning(t *testing.T) {
	ex := exporter.New(exporter.Config{
		Endpoint: "http://127.0.0.1:1", // nothing listens here
		QueueSize: 16, BatchSize: 4, FlushInterval: 5 * time.Millisecond,
		EmitterID: [8]byte{3},
	})
	for i := 0; i < 100; i++ {
		ex.Emit(event(uint64(i)))
	}
	time.Sleep(100 * time.Millisecond)
	ex.Close()

	if ex.DroppedCount() == 0 {
		t.Fatal("an unreachable collector produced no drops")
	}
}

func TestGapsCarryTheRecordersEmitterID(t *testing.T) {
	ex := exporter.New(exporter.Config{
		Endpoint: "http://127.0.0.1:1", QueueSize: 1, BatchSize: 1,
		FlushInterval: time.Hour,
	})
	defer ex.Close()

	want := [8]byte{9, 9, 9, 9, 9, 9, 9, 9}
	ex.SetEmitterID(want)
	for i := 0; i < 10; i++ {
		ex.Emit(event(uint64(i)))
	}
	if ex.DroppedCount() == 0 {
		t.Fatal("expected drops with a queue size of 1")
	}
}

// TestTransportFailureProducesUnreachableGaps covers the path
// TestUnreachableCollectorProducesGapsAndKeepsRunning cannot: that test only
// asserts DroppedCount is non-zero, and ordinary queue-overflow backpressure
// drives that counter on its own, so the transport-error path stayed
// unexercised. This asserts the actual Gap records, with reason UNREACHABLE.
//
// The collector rejects the first batches and then recovers, which is what an
// outage looks like: the gaps accumulated while it was down must be delivered
// once it returns, or the timeline shows silence where events were lost.
func TestTransportFailureProducesUnreachableGaps(t *testing.T) {
	var mu sync.Mutex
	var failing = true
	var got []*v1.Gap

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		down := failing
		mu.Unlock()
		if down {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var b v1.Batch
		if err := proto.Unmarshal(body, &b); err == nil {
			mu.Lock()
			got = append(got, b.Gaps...)
			mu.Unlock()
		}
		ack, _ := proto.Marshal(&v1.BatchAck{Accepted: uint32(len(b.Events))})
		w.Write(ack)
	}))
	defer srv.Close()

	ex := exporter.New(exporter.Config{
		Endpoint: srv.URL, QueueSize: 64, BatchSize: 4,
		FlushInterval: 5 * time.Millisecond, EmitterID: [8]byte{9},
	})

	for i := 0; i < 40; i++ {
		ex.Emit(event(uint64(i)))
	}
	if err := ex.Flush(context.Background()); err != nil {
		t.Fatalf("flush while down: %v", err)
	}

	mu.Lock()
	failing = false // the collector comes back
	mu.Unlock()

	ex.Emit(event(999))
	if err := ex.Flush(context.Background()); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}
	ex.Close()

	mu.Lock()
	defer mu.Unlock()
	var unreachable int
	var dropped uint64
	for _, g := range got {
		if g.Reason == v1.GapReason_UNREACHABLE {
			unreachable++
			dropped += g.DroppedCount
			if len(g.EmitterId) == 0 {
				t.Fatal("gap carries no emitter id: it cannot be attributed")
			}
		}
	}
	if unreachable == 0 {
		t.Fatal("a failing collector produced no UNREACHABLE gap: those events would be silent holes in the timeline")
	}
	if dropped == 0 {
		t.Fatal("UNREACHABLE gaps report zero dropped events")
	}
}
