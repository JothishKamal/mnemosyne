package harness

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
	mnemohttp "github.com/jothishkamal/mnemosyne/sdk/instrument/nethttp"
)

// Service is one instrumented process in a scenario.
type Service struct {
	Name   string
	Rec    *sdk.Recorder
	URL    string
	Client *http.Client
	clock  *Clock
	server *httptest.Server
}

// ServiceSpec declares a service in a scenario.
type ServiceSpec struct {
	Name      string
	Handler   func(*Ctx)
	ClockSkew time.Duration
}

// Options configures a Harness.
type Options struct {
	Services []ServiceSpec
	Sink     sdk.Sink
	Start    time.Time
}

// Harness owns a set of in-process instrumented services.
type Harness struct {
	t        *testing.T
	services map[string]*Service
	clock    *Clock
	release  chan struct{}
	once     sync.Once
}

// Ctx is what a scenario handler receives.
type Ctx struct {
	h   *Harness
	svc *Service
	w   http.ResponseWriter
	r   *http.Request
}

// Request returns the inbound request.
func (c *Ctx) Request() *http.Request { return c.r }

// Call issues an instrumented request to another service in the scenario.
func (c *Ctx) Call(service, path string) (*http.Response, error) {
	target := c.h.Service(service)
	if target == nil {
		return nil, fmt.Errorf("harness: unknown service %q", service)
	}
	req, err := http.NewRequestWithContext(c.r.Context(), http.MethodGet, target.URL+path, nil)
	if err != nil {
		return nil, err
	}
	return c.svc.Client.Do(req)
}

// CallWithRetries issues up to attempts requests, linking each attempt to the
// previous one with a RETRY_OF edge. All attempts are siblings under the
// current scope.
func (c *Ctx) CallWithRetries(service, path string, attempts int) error {
	target := c.h.Service(service)
	if target == nil {
		return fmt.Errorf("harness: unknown service %q", service)
	}

	var prev *sdk.Handle
	var lastErr error
	for i := 0; i < attempts; i++ {
		h := c.svc.Rec.Begin(c.r.Context(), sdk.HTTPClient{
			Method: http.MethodGet, Host: target.Name, Path: path,
		})
		if prev != nil {
			h.AddLink(sdk.RetryOf(h.Scope().TraceID, prev.ID()))
		}

		req, _ := http.NewRequestWithContext(h.Context(), http.MethodGet, target.URL+path, nil)
		resp, err := target.Client.Transport.RoundTrip(req)
		switch {
		case err != nil:
			h.Fail(err)
			lastErr = err
		case resp.StatusCode >= 500:
			h.SetStatus(v1.Status_ERROR)
			lastErr = fmt.Errorf("harness: %s returned %d", service, resp.StatusCode)
			resp.Body.Close()
		default:
			resp.Body.Close()
			lastErr = nil
		}
		h.End()

		prev = h
		if lastErr == nil {
			return nil
		}
		c.h.clock.Advance(200 * time.Millisecond)
	}
	return lastErr
}

// Hang opens an operation that never completes, producing an unmatched BEGIN.
// It is released when the harness closes, so it cannot leak a goroutine.
func (c *Ctx) Hang() {
	// Opened and never ended on purpose: an unmatched BEGIN is exactly what a
	// deadlocked or hung operation looks like in a real recording.
	_ = c.svc.Rec.Begin(c.r.Context(), sdk.Local{Name: "hung-operation"})
	<-c.h.release
}

// Fail responds with an error status.
func (c *Ctx) Fail(code int) { c.w.WriteHeader(code) }

// New builds and starts a scenario.
func New(t *testing.T, opts Options) *Harness {
	t.Helper()
	if opts.Start.IsZero() {
		opts.Start = time.Date(2026, 7, 23, 10, 34, 12, 0, time.UTC)
	}
	h := &Harness{
		t:        t,
		services: map[string]*Service{},
		clock:    NewClock(opts.Start),
		release:  make(chan struct{}),
	}

	for _, spec := range opts.Services {
		svcClock := NewClock(opts.Start)
		svcClock.Skew(spec.ClockSkew)

		rec, err := sdk.New(sdk.Config{
			Service: spec.Name, Version: "test", Sink: opts.Sink, Enabled: true,
			Now: svcClock.Now,
		})
		if err != nil {
			t.Fatalf("harness: sdk.New(%s): %v", spec.Name, err)
		}

		svc := &Service{Name: spec.Name, Rec: rec, clock: svcClock}
		svc.Client = &http.Client{Transport: mnemohttp.Transport(rec, nil)}

		handler := spec.Handler
		svc.server = httptest.NewServer(mnemohttp.Middleware(rec)(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				h.clock.Advance(time.Millisecond)
				svcClock.Advance(time.Millisecond)
				if handler != nil {
					handler(&Ctx{h: h, svc: svc, w: w, r: r})
				}
			})))
		svc.URL = svc.server.URL
		h.services[spec.Name] = svc
	}
	return h
}

// Service returns a service by name.
func (h *Harness) Service(name string) *Service { return h.services[name] }

// Run issues an uninstrumented request to the first service, standing in for an
// external client.
func (h *Harness) Run(service, path string) (*http.Response, error) {
	svc := h.Service(service)
	if svc == nil {
		return nil, fmt.Errorf("harness: unknown service %q", service)
	}
	return http.Get(svc.URL + path)
}

// Close releases hung operations and shuts every service down.
func (h *Harness) Close() {
	h.once.Do(func() {
		close(h.release)
		for _, s := range h.services {
			s.server.Close()
		}
	})
}
