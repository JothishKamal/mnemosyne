package store

import (
	"bytes"
	"sort"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
)

// SummarizeTraces groups events into trace summaries. Both backends use this
// so their Traces results cannot diverge.
//
// events must already be sorted by (hlc.wall_ms, hlc.logical, id.emitter_id)
// — RootService/RootRoute are taken from the first event seen per trace,
// which is only the trace's earliest event if the input is HLC-sorted.
func SummarizeTraces(events []*v1.Event, q TraceQuery) []TraceSummary {
	byTrace := map[string]*TraceSummary{}
	for _, e := range events {
		if q.Service != "" && e.GetService().GetName() != q.Service {
			continue
		}
		w := e.GetHlc().GetWallMs()
		key := string(e.TraceId)
		sum, ok := byTrace[key]
		if !ok {
			sum = &TraceSummary{
				TraceID:     append([]byte(nil), e.TraceId...),
				RootService: e.GetService().GetName(),
				RootRoute:   e.GetHttpServer().GetRoute(),
				StartWallMS: w,
				EndWallMS:   w,
			}
			byTrace[key] = sum
		}
		sum.EventCount++
		if w < sum.StartWallMS {
			sum.StartWallMS = w
		}
		if w > sum.EndWallMS {
			sum.EndWallMS = w
		}
		if e.Status == v1.Status_ERROR || e.Status == v1.Status_TIMEOUT {
			sum.HasError = true
		}
	}

	out := make([]TraceSummary, 0, len(byTrace))
	for _, sum := range byTrace {
		if q.OnlyErrors && !sum.HasError {
			continue
		}
		out = append(out, *sum)
	}
	// Ordered by StartWallMS, then by TraceID bytes as a deterministic
	// tie-break (see EventStore.Traces).
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartWallMS != out[j].StartWallMS {
			return out[i].StartWallMS < out[j].StartWallMS
		}
		return bytes.Compare(out[i].TraceID, out[j].TraceID) < 0
	})
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out
}
