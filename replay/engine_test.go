package replay_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jothishkamal/mnemosyne/replay"
	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store/memory"
)

func synth(seq uint64, wallMS uint64, svc string, phase v1.Phase, status v1.Status) *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       make([]byte, 16),
		Id:            &v1.EventID{EmitterId: []byte("emitter0"), Sequence: seq},
		Service:       &v1.Service{Name: svc, EmitterId: []byte("emitter0")},
		Hlc:           &v1.HLC{WallMs: wallMS},
		WallUnixNanos: int64(wallMS) * 1e6,
		Phase:         phase,
		OperationType: v1.OperationType_HTTP_SERVER,
		Operation:     &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/r"}},
		Status:        status,
	}
}

func TestBucketCountIsBoundedByResolution(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	const n = 100_000
	batch := make([]*v1.Event, 0, n)
	for i := 0; i < n; i++ {
		batch = append(batch, synth(uint64(i), 1000+uint64(i), "svc", v1.Phase_END, v1.Status_OK))
	}
	if err := es.AppendEvents(ctx, batch); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	eng := replay.New(es, bs)
	m, err := eng.Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 1000, ToWallMS: 1000 + n, Resolution: 200,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.Lanes) == 0 {
		t.Fatal("no lanes produced")
	}
	total := 0
	for _, lane := range m.Lanes {
		if len(lane.Buckets) > 200 {
			t.Fatalf("lane %s has %d buckets, exceeds resolution 200", lane.Service, len(lane.Buckets))
		}
		for _, b := range lane.Buckets {
			total += b.Count
		}
	}
	if total != n {
		t.Fatalf("buckets account for %d events, want %d — events were dropped at a window edge", total, n)
	}
}

func TestUnmatchedBeginBecomesInFlight(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	if err := es.AppendEvents(ctx, []*v1.Event{
		synth(1, 1000, "svc", v1.Phase_BEGIN, v1.Status_OK),
		synth(1, 1000, "svc", v1.Phase_END, v1.Status_OK),
		synth(2, 1001, "svc", v1.Phase_BEGIN, v1.Status_OK), // never ends
	}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: 9999, Resolution: 50,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.InFlight) != 1 || m.InFlight[0].EventID.Sequence != 2 {
		t.Fatalf("in-flight = %v, want exactly event 2", m.InFlight)
	}
}

func TestSkewWarningWhenChildWallClockPrecedesParent(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	parent := synth(1, 1000, "parent-svc", v1.Phase_END, v1.Status_OK)
	parent.WallUnixNanos = 5_000_000_000

	child := synth(2, 1001, "skewed-svc", v1.Phase_END, v1.Status_OK)
	child.Service.EmitterId = []byte("emitter1")
	child.Id.EmitterId = []byte("emitter1")
	child.Parent = &v1.EventID{EmitterId: []byte("emitter0"), Sequence: 1}
	child.WallUnixNanos = 1_000_000_000 // four seconds before its parent

	if err := es.AppendEvents(ctx, []*v1.Event{parent, child}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: 9999, Resolution: 50,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.SkewWarnings) == 0 {
		t.Fatal("no skew warning for a child whose wall clock precedes its parent")
	}
	if m.SkewWarnings[0].Service != "skewed-svc" {
		t.Fatalf("skew attributed to %q, want skewed-svc", m.SkewWarnings[0].Service)
	}
}

func TestGapsSurfaceInTheModel(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	if err := es.AppendGaps(ctx, []*v1.Gap{{
		EmitterId: []byte("emitter0"),
		From:      &v1.HLC{WallMs: 1000}, To: &v1.HLC{WallMs: 2000},
		DroppedCount: 17, Reason: v1.GapReason_BACKPRESSURE,
	}}); err != nil {
		t.Fatalf("AppendGaps: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: 9999, Resolution: 50,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.Gaps) != 1 || m.Gaps[0].DroppedCount != 17 {
		t.Fatalf("gaps = %v, want one marker with 17 drops", m.Gaps)
	}
}

func TestChainIsBoundedAndMarksTruncation(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	// A deep parent chain: 1 <- 2 <- 3 <- ... <- 50
	var batch []*v1.Event
	for i := uint64(1); i <= 50; i++ {
		e := synth(i, 1000+i, "svc", v1.Phase_END, v1.Status_OK)
		if i > 1 {
			e.Parent = &v1.EventID{EmitterId: []byte("emitter0"), Sequence: i - 1}
		}
		batch = append(batch, e)
	}
	if err := es.AppendEvents(ctx, batch); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	chain, err := replay.New(es, bs).Chain(ctx, batch[0].TraceId,
		&v1.EventID{EmitterId: []byte("emitter0"), Sequence: 50}, 10)
	if err != nil {
		t.Fatalf("Chain: %v", err)
	}
	if len(chain.Nodes) > 10 {
		t.Fatalf("chain returned %d nodes, exceeds maxNodes 10", len(chain.Nodes))
	}
	if !chain.Truncated {
		t.Fatal("chain hit its bound but did not report Truncated")
	}
	if got := fmt.Sprint(chain.Nodes[0].Event.Id.Sequence); got != "50" {
		t.Fatalf("chain starts at event %s, want the selected event 50", got)
	}
}

// TestEventsAtWindowEdgesAreCounted pins the boundary the bucketing clamp
// exists for. TestBucketCountIsBoundedByResolution never places an event at
// ToWallMS — its events stop one millisecond short — so it cannot detect a
// missing or wrong clamp. An off-by-one here silently drops the first or last
// event of an incident, which is often the interesting one.
func TestEventsAtWindowEdgesAreCounted(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	const from, to = 1000, 2000
	if err := es.AppendEvents(ctx, []*v1.Event{
		synth(1, from, "svc", v1.Phase_END, v1.Status_OK),        // exactly the lower edge
		synth(2, (from+to)/2, "svc", v1.Phase_END, v1.Status_OK), // middle
		synth(3, to, "svc", v1.Phase_END, v1.Status_OK),          // exactly the upper edge
	}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	const resolution = 10
	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: from, ToWallMS: to, Resolution: resolution,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}

	total := 0
	for _, lane := range m.Lanes {
		if len(lane.Buckets) != resolution {
			t.Fatalf("lane %s has %d buckets, want exactly %d", lane.Service, len(lane.Buckets), resolution)
		}
		for _, b := range lane.Buckets {
			if b.Index < 0 || b.Index >= resolution {
				t.Fatalf("bucket index %d is out of range [0,%d)", b.Index, resolution)
			}
			total += b.Count
		}
	}
	if total != 3 {
		t.Fatalf("buckets account for %d events, want 3 — an event fell off a window edge", total)
	}
}
