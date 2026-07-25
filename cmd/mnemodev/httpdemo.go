package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
	mnemohttp "github.com/jothishkamal/mnemosyne/sdk/instrument/nethttp"
)

// printingSink emits events straight to a writer as they are recorded, so the
// demo shows the propagated trace forming in real time.
type printingSink struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *printingSink) Emit(e *v1.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = PrintEvent(s.w, e)
}

// runHTTPDemo wires two services with mnemohttp exactly as
// TestTracePropagatesAcrossTwoServices does, issues one request through them,
// and prints every recorded event as JSON so the propagated trace can be
// inspected by eye.
func runHTTPDemo(w io.Writer) error {
	sk := &printingSink{w: w}

	svcB, err := sdk.New(sdk.Config{Service: "svc-b", Sink: sk, Enabled: true})
	if err != nil {
		return err
	}
	serverB := httptest.NewServer(mnemohttp.Middleware(svcB)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "ok")
		})))
	defer serverB.Close()

	svcA, err := sdk.New(sdk.Config{Service: "svc-a", Sink: sk, Enabled: true})
	if err != nil {
		return err
	}
	clientA := &http.Client{Transport: mnemohttp.Transport(svcA, nil)}
	serverA := httptest.NewServer(mnemohttp.Middleware(svcA)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req, _ := http.NewRequestWithContext(r.Context(), "GET", serverB.URL, nil)
			resp, err := clientA.Do(req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
		})))
	defer serverA.Close()

	resp, err := http.Get(serverA.URL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}
