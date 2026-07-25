package replay

import (
	"context"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
)

// Chain returns the recorded causal chain containing eventID: the ancestor path
// via PARENT edges, followed by outgoing link edges. Traversal is bounded by
// maxNodes; every edge returned was observed, never inferred.
func (e *Engine) Chain(ctx context.Context, traceID []byte, eventID *v1.EventID, maxNodes int) (*Chain, error) {
	if maxNodes <= 0 {
		maxNodes = 200
	}
	events, err := e.events.TraceEvents(ctx, traceID)
	if err != nil {
		return nil, err
	}

	// Index by ID, preferring END events since they carry outcome and duration.
	byID := map[string]*v1.Event{}
	for _, ev := range events {
		k := eventKey(ev.GetId())
		if cur, ok := byID[k]; !ok || cur.Phase == v1.Phase_BEGIN {
			byID[k] = ev
		}
	}

	chain := &Chain{}
	seen := map[string]bool{}

	type step struct {
		id    *v1.EventID
		edge  string
		depth int
	}
	queue := []step{{id: eventID, edge: "", depth: 0}}

	for len(queue) > 0 {
		if len(chain.Nodes) >= maxNodes {
			chain.Truncated = true
			break
		}
		cur := queue[0]
		queue = queue[1:]

		k := eventKey(cur.id)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true

		ev := byID[k]
		if ev == nil {
			continue
		}
		chain.Nodes = append(chain.Nodes, ChainNode{
			Event: ev, EdgeFromParent: cur.edge, Depth: cur.depth,
		})

		if p := ev.GetParent(); p != nil {
			queue = append(queue, step{id: p, edge: v1.EdgeType_PARENT.String(), depth: cur.depth + 1})
		}
		for _, l := range ev.Links {
			if l.Event == nil {
				continue // foreign link: correlation only, nothing local to walk
			}
			queue = append(queue, step{id: l.Event, edge: l.Type.String(), depth: cur.depth + 1})
		}
	}
	return chain, nil
}
