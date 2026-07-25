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
		writeJSON(w, m)
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
		writeJSON(w, chain)
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
