package mnemohttp

import (
	"net/http"
	"strings"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk"
	"github.com/jothishkamal/mnemosyne/sdk/hlc"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
)

type transport struct {
	rec  *sdk.Recorder
	base http.RoundTripper
}

// Transport returns a RoundTripper that records outbound requests and injects
// propagation headers. A nil base uses http.DefaultTransport.
func Transport(rec *sdk.Recorder, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &transport{rec: rec, base: base}
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	h := t.rec.Begin(r.Context(), sdk.HTTPClient{
		Method: r.Method, Host: r.URL.Host, Path: r.URL.Path,
	})
	defer h.End()

	// Clone before mutating: RoundTrippers must not modify the caller's request.
	out := r.Clone(h.Context())
	inject(out, h)

	resp, err := t.base.RoundTrip(out)
	if err != nil {
		h.Fail(err)
		return resp, err
	}

	h.SetOperation(sdk.HTTPClient{
		Method: r.Method, Host: r.URL.Host, Path: r.URL.Path,
		Status: uint32(resp.StatusCode),
	})
	if resp.StatusCode >= 500 {
		h.SetStatus(v1.Status_ERROR)
	}
	// Backward trigger propagation: a downstream trigger flushes us too.
	if resp.Header.Get(CaptureHeader) == "1" {
		t.rec.FlushTrace(h.Scope().TraceID)
	}
	return resp, nil
}

// inject writes traceparent and the mnemo tracestate entry, preserving any
// other vendors' entries.
func inject(r *http.Request, h *sdk.Handle) {
	scope := h.Scope()
	span := h.SpanID()
	r.Header.Set("traceparent", identity.FormatTraceparent(scope.TraceID, span, scope.Sampled))

	entry := identity.TracestateKey + "=" + identity.EncodeTracestate(currentHLC(h), h.ID())
	r.Header.Set("tracestate", prependEntry(entry, r.Header.Get("tracestate")))
}

// prependEntry puts the mnemo entry first, dropping any prior mnemo entry while
// keeping every other vendor's entry in order.
func prependEntry(entry, existing string) string {
	out := []string{entry}
	for _, e := range strings.Split(existing, ",") {
		e = strings.TrimSpace(e)
		if e == "" || strings.HasPrefix(e, identity.TracestateKey+"=") {
			continue
		}
		out = append(out, e)
	}
	return strings.Join(out, ",")
}

// currentHLC returns the HLC to propagate. The handle's BEGIN timestamp is the
// causally-correct value: it is what the receiver must observe as happening
// before its own work.
func currentHLC(h *sdk.Handle) hlc.Timestamp { return h.HLC() }
