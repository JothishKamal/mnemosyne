package memory_test

import (
	"testing"

	"github.com/jothishkamal/mnemosyne/store"
	"github.com/jothishkamal/mnemosyne/store/memory"
	"github.com/jothishkamal/mnemosyne/store/storetest"
)

func TestMemoryConformance(t *testing.T) {
	storetest.RunConformance(t,
		func(t *testing.T) store.EventStore { return memory.NewEvents() },
		func(t *testing.T) store.BlobStore { return memory.NewBlobs() },
	)
}
