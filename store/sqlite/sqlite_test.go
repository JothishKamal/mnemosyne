package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/jothishkamal/mnemosyne/store"
	"github.com/jothishkamal/mnemosyne/store/blobfs"
	"github.com/jothishkamal/mnemosyne/store/sqlite"
	"github.com/jothishkamal/mnemosyne/store/storetest"
)

func TestDurableConformance(t *testing.T) {
	storetest.RunConformance(t,
		func(t *testing.T) store.EventStore {
			s, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
			if err != nil {
				t.Fatalf("sqlite.Open: %v", err)
			}
			return s
		},
		func(t *testing.T) store.BlobStore {
			s, err := blobfs.Open(t.TempDir())
			if err != nil {
				t.Fatalf("blobfs.Open: %v", err)
			}
			return s
		},
	)
}
