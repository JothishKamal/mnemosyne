// Package sdk is the Mnemosyne core recorder. It has no knowledge of any
// specific instrumented library; adapters live under sdk/instrument and depend
// on this package, never the reverse.
package sdk

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk/capture"
	"github.com/jothishkamal/mnemosyne/sdk/hlc"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
	"github.com/jothishkamal/mnemosyne/sdk/mnemoctx"
)

// SchemaVersion is the single schema version this SDK emits. An emitter never
// mixes versions within its lifetime.
const SchemaVersion = 1

// Sink receives constructed events. The exporter implements it in Task 7.
type Sink interface {
	Emit(*v1.Event)
}

// Config configures a Recorder.
type Config struct {
	Service string
	Version string
	Sink    Sink
	Enabled bool
	// Now supplies physical time. Nil uses time.Now.
	Now func() time.Time
	// Capture buffers full payloads for retroactive flush. Nil means
	// metadata-only recording: PayloadRefs are still computed, but no bytes
	// are ever buffered.
	Capture *capture.Engine
}

// Recorder constructs and emits events. It is safe for concurrent use.
type Recorder struct {
	svc     *v1.Service
	gen     *identity.Generator
	clk     *hlc.Clock
	sink    Sink
	now     func() time.Time
	enabled bool
	cap     *capture.Engine

	// panics counts containments: recording panics that were recovered rather
	// than allowed to break the wrapped application. Must be an atomic.Uint64
	// value (not atomic.AddUint64 on a plain field) because this project
	// targets 32-bit platforms, where a struct-field 64-bit atomic panics if
	// it isn't 8-byte aligned.
	panics atomic.Uint64
}

// PanicCount returns the number of recording panics this Recorder has
// recovered. A non-zero count means recording work panicked and was
// contained — recording is broken and should be investigated, but the
// application it instruments kept running. It is never expected to grow in a
// healthy deployment.
func (r *Recorder) PanicCount() uint64 {
	return r.panics.Load()
}

// contain recovers a panic from recording work and counts it so the failure
// is never silent. It must only guard recording work — Handle's pure
// accessors (Context, ID, Scope, HLC) are deliberately left unguarded because
// a panic there would be an application bug, not a recording bug, and this
// package must not hide those.
//
// Usage is always `defer r.contain()` at the top of a guarded section, never
// an inline recover(): a single, consistent shape keeps every containment
// point auditable.
func (r *Recorder) contain() {
	if p := recover(); p != nil {
		r.panics.Add(1)
	}
}

// Remote describes trace context received from another process.
type Remote struct {
	TraceID     identity.TraceID
	ParentSpan  identity.SpanID
	ParentEvent identity.EventID
	HLC         hlc.Timestamp
	HasHLC      bool
	// Foreign marks context minted by a non-Mnemosyne participant. Foreign
	// context supplies correlation identity only and never a parent edge.
	Foreign bool
}

// New returns a Recorder.
func New(cfg Config) (*Recorder, error) {
	if cfg.Sink == nil {
		return nil, errors.New("mnemosyne: Config.Sink is required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	gen, err := identity.NewGenerator()
	if err != nil {
		return nil, err
	}
	em := gen.Emitter()
	r := &Recorder{
		svc:     &v1.Service{Name: cfg.Service, Version: cfg.Version, EmitterId: em[:]},
		gen:     gen,
		clk:     hlc.New(cfg.Now),
		sink:    cfg.Sink,
		now:     cfg.Now,
		enabled: cfg.Enabled,
	}
	r.cap = cfg.Capture
	return r, nil
}

// Begin opens an operation. The returned Handle carries the child context; the
// caller's ctx is deliberately left untouched so that sibling operations stay
// siblings. Callers use h.Context() for work nested inside the operation.
//
//	h := rec.Begin(ctx, sdk.HTTPClient{Method: "GET", Host: "b"})
//	defer h.End()
//	resp, err := client.Do(req.WithContext(h.Context()))
func (r *Recorder) Begin(ctx context.Context, op Operation) *Handle {
	return r.begin(ctx, op, nil)
}

// BeginRemote opens an operation continuing a trace received from another
// process.
func (r *Recorder) BeginRemote(ctx context.Context, rem Remote, op Operation) *Handle {
	return r.begin(ctx, op, &rem)
}

// FlushTrace returns the buffered payloads for a trace, for the exporter to
// upload. Called when a trigger fires.
func (r *Recorder) FlushTrace(tr identity.TraceID) (blobs []capture.Blob) {
	if r.cap == nil {
		return nil
	}
	defer r.contain()
	blobs = r.cap.Flush(tr)
	return blobs
}

// Instant records an operation with no meaningful duration.
func (r *Recorder) Instant(ctx context.Context, op Operation) {
	if !r.enabled {
		return
	}
	defer r.contain()
	scope, _ := r.resolveScope(ctx, nil)
	e := r.newEvent(scope, r.gen.NextEvent(), v1.Phase_INSTANT, op)
	e.Status = v1.Status_OK // an instant operation has already succeeded
	r.sink.Emit(e)
}

// begin performs the recording work behind Begin/BeginRemote. It must always
// return a usable Handle: callers pass h.Context() straight into things like
// http.NewRequestWithContext, so a nil Handle here would turn a recording bug
// into a different, worse application crash.
//
// h starts as an inert fallback carrying the caller's own context. It is only
// promoted to child once the child context has been computed and the BEGIN
// event has been fully constructed and emitted without panicking — so if
// recording work panics anywhere along the way (event construction, a bad
// Operation implementation, the Sink itself), contain() recovers it, counts
// it, and the caller still gets back a working, if inert, Handle.
func (r *Recorder) begin(ctx context.Context, op Operation, rem *Remote) (h *Handle) {
	if !r.enabled {
		return &Handle{ctx: ctx}
	}
	h = &Handle{ctx: ctx}
	defer r.contain()

	scope, links := r.resolveScope(ctx, rem)
	id := r.gen.NextEvent()

	e := r.newEvent(scope, id, v1.Phase_BEGIN, op)
	e.Links = links

	beginHLC := hlc.Timestamp{WallMS: e.Hlc.WallMs, Logical: e.Hlc.Logical}

	// The child scope is attached to the Handle, not written back into the
	// caller's context. This is what keeps siblings siblings. Update the
	// fallback's context now, before the Sink is invoked below, so that even
	// if Emit panics the caller still gets a context usable for nested work.
	child := mnemoctx.With(ctx, mnemoctx.Scope{
		TraceID: scope.TraceID,
		Current: id,
		Sampled: scope.Sampled,
	})
	h.ctx = child

	r.sink.Emit(e)

	h = &Handle{
		rec:     r,
		ctx:     child,
		scope:   scope,
		id:      id,
		op:      op,
		links:   links,
		started: r.now(),
		status:  v1.Status_OK,
		hlc:     beginHLC,
	}
	return h
}

// resolveScope determines the trace and parent for a new operation, and returns
// any links the operation must carry.
func (r *Recorder) resolveScope(ctx context.Context, rem *Remote) (mnemoctx.Scope, []*v1.Link) {
	if rem != nil {
		scope := mnemoctx.Scope{TraceID: rem.TraceID, Sampled: true}
		if rem.HasHLC {
			r.clk.Observe(rem.HLC)
		}
		link := &v1.Link{Type: v1.EdgeType_REMOTE_CALL, TraceId: rem.TraceID[:]}
		if rem.Foreign {
			// Foreign identity: correlation only. No parent edge.
			sp := rem.ParentSpan
			link.ForeignSpanId = sp[:]
		} else {
			em := rem.ParentEvent.Emitter
			link.Event = &v1.EventID{EmitterId: em[:], Sequence: rem.ParentEvent.Sequence}
		}
		return scope, []*v1.Link{link}
	}

	if scope, ok := mnemoctx.From(ctx); ok {
		return scope, nil
	}

	tr, err := r.gen.NewTrace()
	if err != nil {
		// Identity generation failing means crypto/rand failed; recording is
		// always what gets sacrificed, so continue with a zero trace rather
		// than failing the application's request.
		tr = identity.TraceID{}
	}
	return mnemoctx.Scope{TraceID: tr, Sampled: true}, nil
}

// newEvent builds an event with everything known at construction time. Status
// is deliberately left at STATUS_UNSPECIFIED: a BEGIN has no outcome yet, and
// an operation that never ends must not claim one. End and Instant set it.
func (r *Recorder) newEvent(scope mnemoctx.Scope, id identity.EventID, phase v1.Phase, op Operation) *v1.Event {
	em := id.Emitter
	e := &v1.Event{
		SchemaVersion: SchemaVersion,
		TraceId:       append([]byte(nil), scope.TraceID[:]...),
		Id:            &v1.EventID{EmitterId: em[:], Sequence: id.Sequence},
		Service:       r.svc,
		Hlc:           toProtoHLC(r.clk.Now()),
		WallUnixNanos: r.now().UnixNano(),
		Phase:         phase,
		Completeness:  v1.Completeness_METADATA_ONLY,
	}
	if !scope.Current.IsZero() {
		pe := scope.Current.Emitter
		e.Parent = &v1.EventID{EmitterId: pe[:], Sequence: scope.Current.Sequence}
	}
	op.applyTo(e)
	return e
}

func toProtoHLC(t hlc.Timestamp) *v1.HLC {
	return &v1.HLC{WallMs: t.WallMS, Logical: t.Logical}
}

// Handle is an open operation. All methods are safe on a nil receiver.
//
// A Handle is NOT safe for concurrent use: confine each Handle to the goroutine
// that opened it. Its mutable state (status, error, links, span, ended) is
// unsynchronised by design — the Recorder is shared and locked, a Handle is not.
// Instrumentation adapters that fan out must open a Handle per goroutine.
type Handle struct {
	rec     *Recorder
	ctx     context.Context
	scope   mnemoctx.Scope
	id      identity.EventID
	op      Operation
	links   []*v1.Link
	started time.Time
	status  v1.Status
	err     error
	span    identity.SpanID
	hasSpan bool
	ended   bool
	hlc     hlc.Timestamp

	reqRef, respRef *v1.PayloadRef
}

// Context returns the context for work nested inside this operation.
func (h *Handle) Context() context.Context {
	if h == nil {
		return context.Background()
	}
	return h.ctx
}

// ID returns this operation's event ID.
func (h *Handle) ID() identity.EventID {
	if h == nil {
		return identity.EventID{}
	}
	return h.id
}

// SpanID returns the W3C span ID for this operation, allocating one on first
// use. Span IDs exist only for events that cross a process boundary.
func (h *Handle) SpanID() (id identity.SpanID) {
	if h == nil || h.rec == nil {
		return identity.SpanID{}
	}
	if h.hasSpan {
		return h.span
	}
	defer h.rec.contain()
	sp, err := h.rec.gen.NewSpan()
	if err != nil {
		return identity.SpanID{}
	}
	h.span, h.hasSpan = sp, true
	return h.span
}

// Scope returns the scope this operation was opened in.
func (h *Handle) Scope() mnemoctx.Scope {
	if h == nil {
		return mnemoctx.Scope{}
	}
	return h.scope
}

// HLC returns the hybrid logical clock timestamp assigned to this operation's
// BEGIN event. This is the value propagated to downstream processes.
func (h *Handle) HLC() hlc.Timestamp {
	if h == nil {
		return hlc.Timestamp{}
	}
	return h.hlc
}

// SetStatus sets the operation outcome.
func (h *Handle) SetStatus(s v1.Status) {
	if h == nil {
		return
	}
	h.status = s
}

// Fail marks the operation as failed.
func (h *Handle) Fail(err error) {
	if h == nil || err == nil {
		return
	}
	h.err = err
	if h.status == v1.Status_OK {
		h.status = v1.Status_ERROR
	}
}

// AddLink attaches a non-parent causal edge.
func (h *Handle) AddLink(l *v1.Link) {
	if h == nil || l == nil {
		return
	}
	h.links = append(h.links, l)
}

// SetOperation replaces the operation detail, for fields only known at End
// (an HTTP status code, for example).
func (h *Handle) SetOperation(op Operation) {
	if h == nil || op == nil {
		return
	}
	h.op = op
}

// RecordRequest records the request payload for this operation.
func (h *Handle) RecordRequest(body []byte) {
	if h == nil || h.rec == nil || h.rec.cap == nil {
		return
	}
	defer h.rec.contain()
	h.reqRef = h.rec.cap.Record(h.scope.TraceID, h.id, capture.KindRequest, body)
}

// RecordResponse records the response payload for this operation.
func (h *Handle) RecordResponse(body []byte) {
	if h == nil || h.rec == nil || h.rec.cap == nil {
		return
	}
	defer h.rec.contain()
	h.respRef = h.rec.cap.Record(h.scope.TraceID, h.id, capture.KindResponse, body)
}

// End closes the operation and emits its END event. It is idempotent.
func (h *Handle) End() {
	if h == nil || h.ended || h.rec == nil {
		return
	}
	h.ended = true
	defer h.rec.contain()

	e := h.rec.newEvent(h.scope, h.id, v1.Phase_END, h.op)
	e.DurationNanos = h.rec.now().Sub(h.started).Nanoseconds()
	e.Status = h.status
	e.Links = h.links
	if h.hasSpan {
		e.SpanId = h.span[:]
	}
	if h.err != nil {
		e.Error = &v1.ErrorInfo{Type: errorType(h.err), Message: h.err.Error()}
	}

	e.Request, e.Response = h.reqRef, h.respRef
	switch {
	case h.reqRef.GetStored() || h.respRef.GetStored():
		e.Completeness = v1.Completeness_FULL
		if h.reqRef.GetTruncated() || h.respRef.GetTruncated() {
			e.Completeness = v1.Completeness_TRUNCATED
		}
	default:
		e.Completeness = v1.Completeness_METADATA_ONLY
	}

	h.rec.sink.Emit(e)
}

func errorType(err error) string {
	var t interface{ Timeout() bool }
	if errors.As(err, &t) && t.Timeout() {
		return "timeout"
	}
	return "error"
}
