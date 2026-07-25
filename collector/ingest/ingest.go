// Package ingest accepts recorded batches and blobs from SDKs.
package ingest

import (
	"crypto/sha256"
	"io"
	"net/http"
	"strings"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store"
	"google.golang.org/protobuf/proto"
)

// MaxBatchBytes bounds a single ingest request.
const MaxBatchBytes = 8 << 20

// Handler returns the ingest routes.
func Handler(es store.EventStore, bs store.BlobStore) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/batch", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxBatchBytes))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		var b v1.Batch
		if err := proto.Unmarshal(body, &b); err != nil {
			http.Error(w, "malformed batch", http.StatusBadRequest)
			return
		}
		if len(b.Events) > 0 {
			if err := es.AppendEvents(r.Context(), b.Events); err != nil {
				http.Error(w, "append events", http.StatusInternalServerError)
				return
			}
		}
		if len(b.Gaps) > 0 {
			if err := es.AppendGaps(r.Context(), b.Gaps); err != nil {
				http.Error(w, "append gaps", http.StatusInternalServerError)
				return
			}
		}
		ack, _ := proto.Marshal(&v1.BatchAck{Accepted: uint32(len(b.Events))})
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(ack)
	})

	mux.HandleFunc("POST /v1/blobs/{blobID}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxBatchBytes))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(body)
		// The URL carries the expected blob ID; content that does not match is
		// rejected rather than silently stored under the wrong identity.
		if got := bs.IDFor(sum); got != strings.TrimSpace(r.PathValue("blobID")) {
			http.Error(w, "content does not match blob id", http.StatusBadRequest)
			return
		}
		if _, err := bs.Put(r.Context(), sum, body); err != nil {
			http.Error(w, "store blob", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	return mux
}
