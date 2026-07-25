package replay

import (
	"context"
	"fmt"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store"
)

// TimelineQuery selects and shapes a timeline.
type TimelineQuery struct {
	FromWallMS uint64
	ToWallMS   uint64
	Services   []string
	Resolution int
}

// Engine builds Timeline Models from a recording.
type Engine struct {
	events store.EventStore
	blobs  store.BlobStore
}

// New returns an Engine.
func New(es store.EventStore, bs store.BlobStore) *Engine {
	return &Engine{events: es, blobs: bs}
}

// DefaultResolution is used when a query does not specify one.
const DefaultResolution = 240

// Timeline builds the model for a window.
func (e *Engine) Timeline(ctx context.Context, q TimelineQuery) (*TimelineModel, error) {
	if q.Resolution <= 0 {
		q.Resolution = DefaultResolution
	}
	if q.ToWallMS <= q.FromWallMS {
		return nil, fmt.Errorf("replay: empty window %d..%d", q.FromWallMS, q.ToWallMS)
	}

	events, err := e.events.Window(ctx, store.WindowQuery{
		FromWallMS: q.FromWallMS, ToWallMS: q.ToWallMS, Services: q.Services,
	})
	if err != nil {
		return nil, err
	}

	m := &TimelineModel{
		FromWallMS: q.FromWallMS, ToWallMS: q.ToWallMS, Resolution: q.Resolution,
	}

	span := q.ToWallMS - q.FromWallMS
	laneIdx := map[string]int{}
	begins := map[string]*v1.Event{}
	ended := map[string]bool{}
	byID := map[string]*v1.Event{}

	for _, ev := range events {
		key := eventKey(ev.GetId())
		switch ev.Phase {
		case v1.Phase_BEGIN:
			begins[key] = ev
		case v1.Phase_END:
			ended[key] = true
		}
		if ev.Phase != v1.Phase_BEGIN {
			byID[key] = ev
		}

		// Only END and INSTANT events contribute to lane aggregates, so an
		// operation is counted once rather than twice.
		if ev.Phase == v1.Phase_BEGIN {
			continue
		}

		name := laneName(ev)
		idx, ok := laneIdx[name]
		if !ok {
			idx = len(m.Lanes)
			laneIdx[name] = idx
			m.Lanes = append(m.Lanes, Lane{
				Service:  ev.GetService().GetName(),
				Resource: laneResource(ev),
				Buckets:  make([]Bucket, q.Resolution),
			})
			for i := range m.Lanes[idx].Buckets {
				m.Lanes[idx].Buckets[i].Index = i
			}
		}

		// Bucket index: clamp the final millisecond into the last bucket so no
		// event falls off the right edge of the window.
		offset := ev.GetHlc().GetWallMs() - q.FromWallMS
		b := int(offset * uint64(q.Resolution) / span)
		if b >= q.Resolution {
			b = q.Resolution - 1
		}

		bucket := &m.Lanes[idx].Buckets[b]
		bucket.Count++
		if ev.Status == v1.Status_ERROR || ev.Status == v1.Status_TIMEOUT {
			bucket.ErrorCount++
		}
		if ev.DurationNanos > bucket.MaxDurationNanos {
			bucket.MaxDurationNanos = ev.DurationNanos
		}
	}

	for key, ev := range begins {
		if ended[key] {
			continue
		}
		m.InFlight = append(m.InFlight, InFlightOp{
			EventID:     ev.GetId(),
			Service:     ev.GetService().GetName(),
			Description: describe(ev),
			StartWallMS: ev.GetHlc().GetWallMs(),
		})
	}

	m.SkewWarnings = detectSkew(events, byID)

	gaps, err := e.events.Gaps(ctx, q.FromWallMS, q.ToWallMS)
	if err != nil {
		return nil, err
	}
	for _, g := range gaps {
		m.Gaps = append(m.Gaps, GapMarker{
			EmitterID:    g.EmitterId,
			FromWallMS:   g.GetFrom().GetWallMs(),
			ToWallMS:     g.GetTo().GetWallMs(),
			DroppedCount: g.DroppedCount,
			Reason:       g.Reason.String(),
		})
	}
	return m, nil
}

// detectSkew reports emitters whose wall clock disagrees with causal order: a
// child cannot truly have started before the parent that caused it.
func detectSkew(events []*v1.Event, byID map[string]*v1.Event) []SkewWarning {
	worst := map[string]SkewWarning{}
	for _, ev := range events {
		parent := byID[eventKey(ev.GetParent())]
		if parent == nil || ev.GetParent() == nil {
			continue
		}
		delta := (parent.WallUnixNanos - ev.WallUnixNanos) / 1e6
		if delta <= 0 {
			continue
		}
		svc := ev.GetService().GetName()
		if cur, ok := worst[svc]; !ok || delta > cur.ObservedSkewMS {
			worst[svc] = SkewWarning{
				Service:        svc,
				Emitter:        ev.GetId().GetEmitterId(),
				ObservedSkewMS: delta,
			}
		}
	}
	out := make([]SkewWarning, 0, len(worst))
	for _, w := range worst {
		out = append(out, w)
	}
	return out
}

func eventKey(id *v1.EventID) string {
	if id == nil {
		return ""
	}
	return fmt.Sprintf("%x/%d", id.EmitterId, id.Sequence)
}

func laneName(e *v1.Event) string {
	return e.GetService().GetName() + "\x00" + laneResource(e)
}

func laneResource(e *v1.Event) string {
	switch e.OperationType {
	case v1.OperationType_HTTP_SERVER:
		return "http.server"
	case v1.OperationType_HTTP_CLIENT:
		return "http.client"
	default:
		return "local"
	}
}

func describe(e *v1.Event) string {
	if s := e.GetHttpServer(); s != nil {
		return s.Method + " " + s.Route
	}
	if c := e.GetHttpClient(); c != nil {
		return c.Method + " " + c.Host + c.Path
	}
	return e.OperationType.String()
}
