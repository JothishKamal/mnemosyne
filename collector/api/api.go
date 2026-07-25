// Package api assembles the collector's HTTP surface.
package api

import (
	"net/http"

	"github.com/jothishkamal/mnemosyne/collector/ingest"
	"github.com/jothishkamal/mnemosyne/collector/query"
	"github.com/jothishkamal/mnemosyne/replay"
	"github.com/jothishkamal/mnemosyne/store"
	"github.com/jothishkamal/mnemosyne/ui"
)

// New returns the collector handler, mounting both the ingest routes and the
// query routes (/api/timeline, /api/chain, /api/traces, /api/blob/{blobID}).
func New(es store.EventStore, bs store.BlobStore) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/", ingest.Handler(es, bs))
	mux.Handle("/api/", query.Handler(replay.New(es, bs), es, bs))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.Handle("/", ui.Handler())
	return mux
}
