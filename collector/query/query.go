// Package query serves the read side of the collector.
package query

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jothishkamal/mnemosyne/replay"
	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Handler returns the query routes.
func Handler(eng *replay.Engine, es store.EventStore, bs store.BlobStore) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/timeline", func(w http.ResponseWriter, r *http.Request) {
		q := replay.TimelineQuery{
			FromWallMS: uintParam(r, "from"),
			ToWallMS:   uintParam(r, "to"),
			Resolution: intParam(r, "resolution"),
			Services:   r.URL.Query()["service"],
		}
		m, err := eng.Timeline(r.Context(), q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := toTimelineResponse(m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, resp)
	})

	mux.HandleFunc("GET /api/chain", func(w http.ResponseWriter, r *http.Request) {
		traceID, err := hex.DecodeString(r.URL.Query().Get("trace"))
		if err != nil {
			http.Error(w, "bad trace id", http.StatusBadRequest)
			return
		}
		emitter, err := hex.DecodeString(r.URL.Query().Get("emitter"))
		if err != nil {
			http.Error(w, "bad emitter id", http.StatusBadRequest)
			return
		}
		id := &v1.EventID{EmitterId: emitter, Sequence: uintParam(r, "seq")}
		chain, err := eng.Chain(r.Context(), traceID, id, intParam(r, "max"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp, err := toChainResponse(chain)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, resp)
	})

	mux.HandleFunc("GET /api/traces", func(w http.ResponseWriter, r *http.Request) {
		traces, err := es.Traces(r.Context(), store.TraceQuery{
			FromWallMS: uintParam(r, "from"),
			ToWallMS:   uintParam(r, "to"),
			Service:    r.URL.Query().Get("service"),
			OnlyErrors: r.URL.Query().Get("errors") == "1",
			Limit:      intParam(r, "limit"),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, traces)
	})

	mux.HandleFunc("GET /api/blob/{blobID}", func(w http.ResponseWriter, r *http.Request) {
		body, err := bs.Get(r.Context(), r.PathValue("blobID"))
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(body)
	})

	return mux
}

// The Timeline Model and Chain carry embedded protobuf messages
// (*v1.Event, *v1.EventID). encoding/json against the generated
// protoc-gen-go structs directly would emit snake_case field names and,
// for the operation oneof, a Go-cased wrapper object, a different
// naming convention than the rest of the payload, which uses explicit
// camelCase tags (see replay/model.go). protojson emits camelCase and
// flattens oneofs to their field name, so every protobuf message is
// marshaled with protojson first and embedded as json.RawMessage: the
// surrounding encoding/json encoder then emits that fragment unchanged,
// and the whole response speaks one convention.

// timelineResponse mirrors replay.TimelineModel, with InFlight's embedded
// *v1.EventID pre-marshaled via protojson.
type timelineResponse struct {
	FromWallMS   uint64               `json:"fromWallMs"`
	ToWallMS     uint64               `json:"toWallMs"`
	Resolution   int                  `json:"resolution"`
	Lanes        []replay.Lane        `json:"lanes"`
	InFlight     []inFlightOpResponse `json:"inFlight"`
	Gaps         []replay.GapMarker   `json:"gaps"`
	SkewWarnings []replay.SkewWarning `json:"skewWarnings"`
}

// inFlightOpResponse mirrors replay.InFlightOp, with EventID pre-marshaled
// via protojson.
type inFlightOpResponse struct {
	EventID     json.RawMessage `json:"eventId"`
	Service     string          `json:"service"`
	Description string          `json:"description"`
	StartWallMS uint64          `json:"startWallMs"`
}

func toTimelineResponse(m *replay.TimelineModel) (*timelineResponse, error) {
	resp := &timelineResponse{
		FromWallMS:   m.FromWallMS,
		ToWallMS:     m.ToWallMS,
		Resolution:   m.Resolution,
		Lanes:        m.Lanes,
		Gaps:         m.Gaps,
		SkewWarnings: m.SkewWarnings,
	}
	for _, f := range m.InFlight {
		raw, err := marshalProto(f.EventID)
		if err != nil {
			return nil, err
		}
		resp.InFlight = append(resp.InFlight, inFlightOpResponse{
			EventID:     raw,
			Service:     f.Service,
			Description: f.Description,
			StartWallMS: f.StartWallMS,
		})
	}
	return resp, nil
}

// chainResponse mirrors replay.Chain, with each node's embedded *v1.Event
// pre-marshaled via protojson.
type chainResponse struct {
	Nodes     []chainNodeResponse `json:"nodes"`
	Truncated bool                `json:"truncated"`
}

// chainNodeResponse mirrors replay.ChainNode, with Event pre-marshaled via
// protojson.
type chainNodeResponse struct {
	Event          json.RawMessage `json:"event"`
	EdgeFromParent string          `json:"edgeFromParent"`
	Depth          int             `json:"depth"`
}

func toChainResponse(c *replay.Chain) (*chainResponse, error) {
	resp := &chainResponse{Truncated: c.Truncated}
	for _, n := range c.Nodes {
		raw, err := marshalProto(n.Event)
		if err != nil {
			return nil, err
		}
		resp.Nodes = append(resp.Nodes, chainNodeResponse{
			Event:          raw,
			EdgeFromParent: n.EdgeFromParent,
			Depth:          n.Depth,
		})
	}
	return resp, nil
}

// marshalProto renders a protobuf message the way every client of this API
// should see it: idiomatic camelCase field names, with oneofs flattened to
// their field name rather than wrapped in a Go-cased type name.
func marshalProto(msg proto.Message) (json.RawMessage, error) {
	b, err := protojson.MarshalOptions{}.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func uintParam(r *http.Request, name string) uint64 {
	v, _ := strconv.ParseUint(r.URL.Query().Get(name), 10, 64)
	return v
}

func intParam(r *http.Request, name string) int {
	v, _ := strconv.Atoi(r.URL.Query().Get(name))
	return v
}
