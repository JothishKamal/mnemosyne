// Command mnemo is the Mnemosyne collector.
package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"github.com/jothishkamal/mnemosyne/collector/api"
	"github.com/jothishkamal/mnemosyne/store"
	"github.com/jothishkamal/mnemosyne/store/blobfs"
	"github.com/jothishkamal/mnemosyne/store/memory"
	"github.com/jothishkamal/mnemosyne/store/sqlite"
)

func main() {
	addr := flag.String("addr", ":4318", "listen address")
	backend := flag.String("store", "sqlite", "storage backend: sqlite or memory")
	data := flag.String("data", "./mnemo-data", "data directory (sqlite backend only)")
	flag.Parse()

	var (
		es  store.EventStore
		bs  store.BlobStore
		err error
	)
	switch *backend {
	case "memory":
		es, bs = memory.NewEvents(), memory.NewBlobs()
	case "sqlite":
		es, err = sqlite.Open(filepath.Join(*data, "events.db"))
		if err != nil {
			log.Fatalf("open event store: %v", err)
		}
		bs, err = blobfs.Open(filepath.Join(*data, "blobs"))
		if err != nil {
			log.Fatalf("open blob store: %v", err)
		}
	default:
		log.Fatalf("unknown -store %q: want sqlite or memory", *backend)
	}

	log.Printf("mnemo collector listening on %s (store=%s)", *addr, *backend)
	if err := http.ListenAndServe(*addr, api.New(es, bs)); err != nil {
		log.Fatal(err)
	}
}
