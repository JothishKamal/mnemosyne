package identity_test

import (
	"testing"

	"github.com/jothishkamal/mnemosyne/sdk/identity"
)

func TestNextEventIsMonotonic(t *testing.T) {
	g, err := identity.NewGenerator()
	if err != nil {
		t.Fatalf("NewGenerator: %v", err)
	}
	prev := g.NextEvent()
	for i := 0; i < 1000; i++ {
		got := g.NextEvent()
		if got.Sequence != prev.Sequence+1 {
			t.Fatalf("iteration %d: got sequence %d after %d", i, got.Sequence, prev.Sequence)
		}
		if got.Emitter != prev.Emitter {
			t.Fatal("emitter changed within one generator")
		}
		prev = got
	}
}

func TestSequenceStartsAtOneSoZeroMeansUnset(t *testing.T) {
	g, _ := identity.NewGenerator()
	if first := g.NextEvent(); first.Sequence != 1 {
		t.Fatalf("first sequence = %d, want 1", first.Sequence)
	}
	var zero identity.EventID
	if !zero.IsZero() {
		t.Fatal("zero EventID must report IsZero")
	}
	if g.NextEvent().IsZero() {
		t.Fatal("a generated EventID must never report IsZero")
	}
}

func TestGeneratorsHaveDistinctEmitters(t *testing.T) {
	seen := map[identity.EmitterID]bool{}
	for i := 0; i < 100; i++ {
		g, err := identity.NewGenerator()
		if err != nil {
			t.Fatalf("NewGenerator: %v", err)
		}
		if seen[g.Emitter()] {
			t.Fatalf("duplicate emitter id after %d generators", i)
		}
		seen[g.Emitter()] = true
	}
}

func TestConcurrentNextEventProducesNoDuplicates(t *testing.T) {
	g, _ := identity.NewGenerator()
	const workers, each = 8, 500
	out := make(chan uint64, workers*each)
	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < each; i++ {
				out <- g.NextEvent().Sequence
			}
			done <- struct{}{}
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}
	close(out)
	seen := map[uint64]bool{}
	for s := range out {
		if seen[s] {
			t.Fatalf("duplicate sequence %d", s)
		}
		seen[s] = true
	}
	if len(seen) != workers*each {
		t.Fatalf("got %d unique sequences, want %d", len(seen), workers*each)
	}
}
