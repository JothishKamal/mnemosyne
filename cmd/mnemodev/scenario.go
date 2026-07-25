package main

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jothishkamal/mnemosyne/harness"
	"github.com/jothishkamal/mnemosyne/sdk/exporter"
)

// runScenario records the acceptance incident into a live collector so the UI
// can be checked by eye. It borrows testing.T because the harness is built for
// tests; that is deliberate — the demo and the test must exercise identical
// wiring, or the demo stops proving anything.
func runScenario(w io.Writer, endpoint string) error {
	ex := exporter.New(exporter.Config{
		Endpoint: endpoint, QueueSize: 4096, BatchSize: 64,
		FlushInterval: 100 * time.Millisecond,
	})
	defer ex.Close()

	t := &testing.T{}
	h := harness.AcceptanceScenario(t, ex)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.Run("checkout-api", "/checkout"); err == nil {
			resp.Body.Close()
		}
	}()

	// pricing-svc's third attempt hangs inside the request above until
	// Harness.Close releases it; Close also tears the local httptest servers
	// down. Wait until the harness signals the scenario has actually reached
	// the hang before that happens — otherwise Close can win the race and
	// close the listener before the goroutine above has dialed it.
	if err := h.WaitForHang(10 * time.Second); err != nil {
		return err
	}
	h.Close()
	<-done

	if err := ex.Flush(context.Background()); err != nil {
		return err
	}
	fmt.Fprintf(w, "recorded acceptance scenario to %s (%d events dropped)\n", endpoint, ex.DroppedCount())
	return nil
}
