package mnemohttp

import (
	"net/http"
	"strings"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
)

// CaptureHeader carries the backward trigger flag on responses.
const CaptureHeader = "Mnemosyne-Capture"

// Option configures the middleware.
type Option func(*options)

type options struct {
	route func(*http.Request) string
}

// WithRoute supplies a route template for a request. Without it the raw path is
// used, which is high-cardinality for parameterised routes.
func WithRoute(fn func(*http.Request) string) Option {
	return func(o *options) { o.route = fn }
}

// statusWriter captures the response status code and any capture flag.
type statusWriter struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.written {
		w.status, w.written = code, true
		if code >= 500 {
			// Must be set before delegating: once the real WriteHeader runs,
			// headers are flushed and any later mutation is silently dropped.
			w.Header().Set(CaptureHeader, "1")
		}
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.written {
		w.status, w.written = http.StatusOK, true
	}
	return w.ResponseWriter.Write(b)
}

// Middleware records inbound requests and continues any propagated trace.
func Middleware(rec *sdk.Recorder, opts ...Option) func(http.Handler) http.Handler {
	var o options
	for _, fn := range opts {
		fn(&o)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := r.URL.Path
			if o.route != nil {
				route = o.route(r)
			}

			h := beginServer(rec, r, route)
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			defer func() {
				op := sdk.HTTPServer{
					Method: r.Method, Route: route, Path: r.URL.Path,
					Status: uint32(sw.status),
				}
				h.SetOperation(op)
				if sw.status >= 500 {
					h.SetStatus(v1.Status_ERROR)
				}
				h.End()
			}()

			next.ServeHTTP(sw, r.WithContext(h.Context()))
		})
	}
}

// beginServer resolves inbound trace context and opens the server operation.
// Malformed headers are ignored rather than propagated as errors: recording
// must never affect the request.
func beginServer(rec *sdk.Recorder, r *http.Request, route string) *sdk.Handle {
	op := sdk.HTTPServer{Method: r.Method, Route: route, Path: r.URL.Path}

	tr, sp, ok := identity.ParseTraceparent(r.Header.Get("traceparent"))
	if !ok {
		return rec.Begin(r.Context(), op)
	}

	rem := sdk.Remote{TraceID: tr, ParentSpan: sp, Foreign: true}
	if ts, id, err := identity.DecodeTracestate(mnemoEntry(r.Header.Get("tracestate"))); err == nil {
		rem.HLC, rem.HasHLC = ts, true
		rem.ParentEvent, rem.Foreign = id, false
	}
	return rec.BeginRemote(r.Context(), rem, op)
}

// mnemoEntry extracts the mnemo value from a tracestate list.
func mnemoEntry(tracestate string) string {
	for _, entry := range strings.Split(tracestate, ",") {
		entry = strings.TrimSpace(entry)
		if v, ok := strings.CutPrefix(entry, identity.TracestateKey+"="); ok {
			return v
		}
	}
	return ""
}
