package capture_test

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/jothishkamal/mnemosyne/sdk/capture"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
	"pgregory.net/rapid"
)

func trace(b byte) identity.TraceID {
	var t identity.TraceID
	t[0] = b
	return t
}

func event(seq uint64) identity.EventID {
	return identity.EventID{Sequence: seq}
}

func TestPayloadRefIsPopulatedEvenWhenNotStored(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelMetadata, MaxBytesPerTrace: 1024, MaxTraces: 8,
	})
	body := []byte("hello world")
	ref := e.Record(trace(1), event(1), capture.KindRequest, body)

	if ref.Stored {
		t.Fatal("metadata level stored a payload")
	}
	if ref.Size != uint64(len(body)) {
		t.Fatalf("size = %d, want %d", ref.Size, len(body))
	}
	sum := sha256.Sum256(body)
	if !bytes.Equal(ref.Sha256, sum[:]) {
		t.Fatal("sha256 not recorded for an unstored payload")
	}
	if got := e.Flush(trace(1)); len(got) != 0 {
		t.Fatalf("metadata level buffered %d payloads", len(got))
	}
}

func TestFullLevelBuffersAndFlushesOnce(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelFull, MaxBytesPerTrace: 1024, MaxTraces: 8,
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte("body-a"))
	e.Record(trace(1), event(2), capture.KindResponse, []byte("body-b"))

	got := e.Flush(trace(1))
	if len(got) != 2 {
		t.Fatalf("flushed %d payloads, want 2", len(got))
	}
	if again := e.Flush(trace(1)); len(again) != 0 {
		t.Fatalf("second flush returned %d payloads, want 0", len(again))
	}
}

func TestRedactionHappensBeforeTheRing(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default:          capture.LevelFull,
		MaxBytesPerTrace: 1024,
		MaxTraces:        8,
		Redact:           capture.Redact{JSONKeys: []string{"password"}},
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte(`{"password":"hunter2"}`))

	for _, b := range e.Flush(trace(1)) {
		if bytes.Contains(b.Body, []byte("hunter2")) {
			t.Fatalf("secret reached the ring buffer: %s", b.Body)
		}
	}
}

func TestRingEvictsOldestTraceBeyondMaxTraces(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelFull, MaxBytesPerTrace: 1024, MaxTraces: 2,
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte("a"))
	e.Record(trace(2), event(2), capture.KindRequest, []byte("b"))
	e.Record(trace(3), event(3), capture.KindRequest, []byte("c")) // evicts trace 1

	if got := e.Flush(trace(1)); len(got) != 0 {
		t.Fatalf("trace 1 survived eviction with %d payloads", len(got))
	}
	if got := e.Flush(trace(3)); len(got) != 1 {
		t.Fatalf("trace 3 has %d payloads, want 1", len(got))
	}
	if e.DroppedCount() == 0 {
		t.Fatal("eviction did not increment the dropped counter")
	}
}

func TestRingEvictsWithinTraceBeyondMaxBytes(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelFull, MaxBytesPerTrace: 10, MaxTraces: 4,
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte("aaaaaa")) // 6
	e.Record(trace(1), event(2), capture.KindRequest, []byte("bbbbbb")) // 6, total 12 > 10

	got := e.Flush(trace(1))
	total := 0
	for _, b := range got {
		total += len(b.Body)
	}
	if total > 10 {
		t.Fatalf("ring holds %d bytes, exceeds MaxBytesPerTrace 10", total)
	}
	if e.DroppedCount() == 0 {
		t.Fatal("byte-bound eviction did not increment the dropped counter")
	}
}

func TestRingMemoryIsAlwaysBounded(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		maxBytes := rapid.IntRange(16, 512).Draw(rt, "maxBytes")
		maxTraces := rapid.IntRange(1, 8).Draw(rt, "maxTraces")
		e := capture.NewEngine(capture.Policy{
			Default: capture.LevelFull, MaxBytesPerTrace: maxBytes, MaxTraces: maxTraces,
		})

		writes := rapid.IntRange(1, 300).Draw(rt, "writes")
		for i := 0; i < writes; i++ {
			tr := trace(byte(rapid.IntRange(0, 20).Draw(rt, "trace")))
			size := rapid.IntRange(0, 128).Draw(rt, "size")
			e.Record(tr, event(uint64(i)), capture.KindRequest, bytes.Repeat([]byte("x"), size))

			if got := e.BufferedBytes(); got > maxBytes*maxTraces {
				rt.Fatalf("buffered %d bytes, exceeds bound %d", got, maxBytes*maxTraces)
			}
			if got := e.BufferedTraces(); got > maxTraces {
				rt.Fatalf("buffered %d traces, exceeds bound %d", got, maxTraces)
			}
		}
	})
}
