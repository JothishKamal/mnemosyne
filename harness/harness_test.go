package harness_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jothishkamal/mnemosyne/harness"
	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
)

type sink struct {
	mu     sync.Mutex
	events []*v1.Event
}

func (s *sink) Emit(e *v1.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *sink) all() []*v1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*v1.Event(nil), s.events...)
}

func TestAcceptanceScenarioProducesTheExpectedGraph(t *testing.T) {
	sk := &sink{}
	h := harness.AcceptanceScenario(t, sk)

	// The final attempt hangs, so run the request in the background and let
	// Close release it.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.Run("checkout-api", "/checkout"); err == nil {
			resp.Body.Close()
		}
	}()

	// Wait until the call chain has actually reached the hang before closing:
	// Close begins by tearing down listeners, and doing that before the chain
	// has connected all the way through would race the request itself.
	if err := h.WaitForHang(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	// The two failing attempts complete quickly; the third blocks.
	h.Close()
	<-done

	events := sk.all()

	traces := map[string]bool{}
	var remote, retry int
	begins, ends := map[uint64]bool{}, map[uint64]bool{}
	services := map[string]bool{}

	for _, e := range events {
		traces[string(e.TraceId)] = true
		services[e.GetService().GetName()] = true
		switch e.Phase {
		case v1.Phase_BEGIN:
			begins[e.Id.Sequence] = true
		case v1.Phase_END:
			ends[e.Id.Sequence] = true
		}
		for _, l := range e.Links {
			switch l.Type {
			case v1.EdgeType_REMOTE_CALL:
				remote++
			case v1.EdgeType_RETRY_OF:
				retry++
			}
		}
	}

	if len(traces) != 1 {
		t.Fatalf("events span %d traces, want 1", len(traces))
	}
	for _, want := range []string{"checkout-api", "inventory-svc", "pricing-svc"} {
		if !services[want] {
			t.Fatalf("no events recorded for %s", want)
		}
	}
	if remote == 0 {
		t.Fatal("no REMOTE_CALL edges recorded")
	}
	// Two retries link back to a previous attempt; the first attempt links to
	// nothing. Both BEGIN and END carry the links, so each is counted twice.
	if retry == 0 {
		t.Fatal("no RETRY_OF edges recorded")
	}

	var unmatched int
	for seq := range begins {
		if !ends[seq] {
			unmatched++
		}
	}
	if unmatched == 0 {
		t.Fatal("no unmatched BEGIN: the hung operation was not recorded as in-flight")
	}
}

// scenarioShape is the structural fingerprint of one AcceptanceScenario run:
// counts that must be identical across runs (modulo the random IDs each run
// mints fresh) if the harness is truly deterministic.
type scenarioShape struct {
	events    int
	remote    int
	retry     int
	unmatched int
}

// runAcceptanceScenario runs the acceptance scenario to completion once and
// returns its structural fingerprint.
func runAcceptanceScenario(t *testing.T) scenarioShape {
	t.Helper()
	sk := &sink{}
	h := harness.AcceptanceScenario(t, sk)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.Run("checkout-api", "/checkout"); err == nil {
			resp.Body.Close()
		}
	}()

	if err := h.WaitForHang(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	h.Close()
	<-done

	events := sk.all()

	// Keyed by (emitter, sequence): sequence numbers restart at 1 in every
	// service's own Generator, so a bare sequence number is not unique across
	// the whole scenario.
	type eventKey struct {
		emitter  string
		sequence uint64
	}
	begins, ends := map[eventKey]bool{}, map[eventKey]bool{}
	var remote, retry int
	for _, e := range events {
		k := eventKey{string(e.Id.EmitterId), e.Id.Sequence}
		switch e.Phase {
		case v1.Phase_BEGIN:
			begins[k] = true
		case v1.Phase_END:
			ends[k] = true
		}
		for _, l := range e.Links {
			switch l.Type {
			case v1.EdgeType_REMOTE_CALL:
				remote++
			case v1.EdgeType_RETRY_OF:
				retry++
			}
		}
	}

	var unmatched int
	for k := range begins {
		if !ends[k] {
			unmatched++
		}
	}

	return scenarioShape{events: len(events), remote: remote, retry: retry, unmatched: unmatched}
}

// TestScenarioIsDeterministic runs the acceptance scenario five times in one
// process and asserts the resulting event graph is structurally identical
// every time (modulo the random trace/emitter/event IDs each run mints
// fresh). Determinism here comes entirely from the virtual clock plus
// in-process httptest servers, no real sleep is used anywhere in the
// scenario or in this test.
func TestScenarioIsDeterministic(t *testing.T) {
	var want scenarioShape
	for i := 0; i < 5; i++ {
		got := runAcceptanceScenario(t)
		t.Logf("run %d: %+v", i, got)
		if i == 0 {
			want = got
			continue
		}
		if got != want {
			t.Fatalf("run %d shape = %+v, want %+v (run 0)", i, got, want)
		}
	}
}

// TestRetryAttemptsAreSiblingsNotAChain is the assertion the edge-count checks
// cannot make. Counting RETRY_OF edges proves attempts were linked; it says
// nothing about whether they were linked as siblings under one scope or nested
// into a chain. Nesting would assert that attempt N happened *inside* attempt
// N-1, which is false, they are successive tries of the same call, and it is
// exactly the degeneration the SDK's scope invariant exists to prevent.
func TestRetryAttemptsAreSiblingsNotAChain(t *testing.T) {
	sk := &sink{}
	h := harness.AcceptanceScenario(t, sk)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.Run("checkout-api", "/checkout"); err == nil {
			resp.Body.Close()
		}
	}()
	if err := h.WaitForHang(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	h.Close()
	<-done

	// Collect every event that carries a RETRY_OF link, plus the attempt it
	// links back to. All of them must share one parent.
	ends := map[uint64]*v1.Event{}
	for _, e := range sk.all() {
		if e.Phase == v1.Phase_END {
			ends[e.Id.Sequence] = e
		}
	}

	parents := map[uint64]bool{}
	var attempts int
	for _, e := range ends {
		var isRetry bool
		for _, l := range e.Links {
			if l.Type != v1.EdgeType_RETRY_OF {
				continue
			}
			isRetry = true
			// The attempt being retried must share this attempt's parent.
			if prev := ends[l.Event.GetSequence()]; prev != nil {
				parents[prev.GetParent().GetSequence()] = true
			}
		}
		if isRetry {
			attempts++
			parents[e.GetParent().GetSequence()] = true
		}
	}

	if attempts == 0 {
		t.Fatal("no retry attempts recorded; the scenario did not produce a retry storm")
	}
	if len(parents) != 1 {
		t.Fatalf("retry attempts span %d distinct parents, want exactly 1: they were nested into a chain rather than recorded as siblings", len(parents))
	}
}
