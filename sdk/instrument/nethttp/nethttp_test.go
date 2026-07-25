package mnemohttp_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
	mnemohttp "github.com/jothishkamal/mnemosyne/sdk/instrument/nethttp"
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

func (s *sink) snapshot() []*v1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*v1.Event(nil), s.events...)
}

func newRec(t *testing.T, name string) (*sdk.Recorder, *sink) {
	t.Helper()
	sk := &sink{}
	rec, err := sdk.New(sdk.Config{Service: name, Sink: sk, Enabled: true})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	return rec, sk
}

func TestTracePropagatesAcrossTwoServices(t *testing.T) {
	svcB, sinkB := newRec(t, "svc-b")
	serverB := httptest.NewServer(mnemohttp.Middleware(svcB)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "ok")
		})))
	defer serverB.Close()

	svcA, sinkA := newRec(t, "svc-a")
	clientA := &http.Client{Transport: mnemohttp.Transport(svcA, nil)}
	serverA := httptest.NewServer(mnemohttp.Middleware(svcA)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req, _ := http.NewRequestWithContext(r.Context(), "GET", serverB.URL, nil)
			resp, err := clientA.Do(req)
			if err != nil {
				t.Errorf("call to B: %v", err)
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
		})))
	defer serverA.Close()

	resp, err := http.Get(serverA.URL)
	if err != nil {
		t.Fatalf("call to A: %v", err)
	}
	resp.Body.Close()

	all := append(sinkA.snapshot(), sinkB.snapshot()...)
	traces := map[string]int{}
	var remoteLinks int
	for _, e := range all {
		traces[string(e.TraceId)]++
		for _, l := range e.Links {
			if l.Type == v1.EdgeType_REMOTE_CALL {
				remoteLinks++
			}
		}
	}
	if len(traces) != 1 {
		t.Fatalf("events span %d traces, want 1", len(traces))
	}
	if remoteLinks == 0 {
		t.Fatal("no REMOTE_CALL edge recorded")
	}

	// B's server event must be parented by nothing locally but linked remotely.
	for _, e := range sinkB.snapshot() {
		if e.OperationType == v1.OperationType_HTTP_SERVER && e.Phase == v1.Phase_END {
			if len(e.Links) == 0 {
				t.Fatal("B's server event carries no remote link")
			}
		}
	}
}

func TestForeignTraceparentIsCorrelationOnly(t *testing.T) {
	rec, sk := newRec(t, "svc")
	srv := httptest.NewServer(mnemohttp.Middleware(rec)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})))
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL, nil)
	// A traceparent with no mnemo tracestate entry: another vendor's context.
	req.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()

	for _, e := range sk.snapshot() {
		if e.OperationType != v1.OperationType_HTTP_SERVER {
			continue
		}
		if e.Parent != nil && e.Parent.Sequence != 0 {
			t.Fatalf("foreign context produced a parent edge: %v", e.Parent)
		}
		if len(e.Links) != 1 || len(e.Links[0].ForeignSpanId) != 8 {
			t.Fatalf("expected one link carrying a foreign span id, got %v", e.Links)
		}
	}
}

func TestTransportPreservesOtherVendorsTracestate(t *testing.T) {
	rec, _ := newRec(t, "svc")
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("tracestate")
	}))
	defer srv.Close()

	client := &http.Client{Transport: mnemohttp.Transport(rec, nil)}
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("tracestate", "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()

	if !strings.Contains(got, "congo=") || !strings.Contains(got, "rojo=") {
		t.Fatalf("other vendors' tracestate entries were dropped: %q", got)
	}
	if !strings.Contains(got, "mnemo=") {
		t.Fatalf("mnemo entry missing from tracestate: %q", got)
	}
}

func TestServerSetsCaptureHeaderOnError(t *testing.T) {
	rec, _ := newRec(t, "svc")
	srv := httptest.NewServer(mnemohttp.Middleware(rec)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get(mnemohttp.CaptureHeader) != "1" {
		t.Fatalf("%s = %q, want \"1\" on a 500 response",
			mnemohttp.CaptureHeader, resp.Header.Get(mnemohttp.CaptureHeader))
	}
}

func TestMiddlewareDoesNotBreakTheHandlerOnRecordingFailure(t *testing.T) {
	rec, _ := newRec(t, "svc")
	srv := httptest.NewServer(mnemohttp.Middleware(rec)(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "still works")
		})))
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("traceparent", "total garbage")
	req.Header.Set("tracestate", "mnemo=also-garbage")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "still works" {
		t.Fatalf("handler output = %q; malformed headers broke the request", body)
	}
}
