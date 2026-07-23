# Mnemosyne Spec 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Mnemosyne walking skeleton — record an HTTP-only distributed trace as a causal event DAG across three Go services, and render it as a scrubbed replay timeline with a causal chain panel.

**Architecture:** A Go SDK records events into a per-trace ring buffer ordered by hybrid logical clocks, propagates causality over W3C `traceparent`/`tracestate`, and exports to a single-binary collector backed by pluggable `EventStore` + `BlobStore`. A server-side replay engine materializes a Timeline Model that both the UI and a future CLI consume. Full design: `docs/superpowers/specs/2026-07-23-mnemosyne-design.md` (frozen).

**Tech Stack:** Go 1.24 · protobuf (`google.golang.org/protobuf`) · `modernc.org/sqlite` (pure Go, no cgo) · `pgregory.net/rapid` (property tests) · vanilla TS/HTML for the UI.

## Global Constraints

- **Module path:** `github.com/ai-mond/mnemosyne`. This is a placeholder derived from the user's email domain — change it before first push via `go mod edit -module <path>` plus a find/replace on imports.
- **Go version floor:** 1.24. `go.mod` declares `go 1.24`.
- **32-bit safety (`windows/386`):** never use `atomic.AddUint64`/`atomic.LoadUint64` on a struct field. Use the `atomic.Uint64` *type*, which carries its own 8-byte alignment guarantee. The HLC uses a mutex, not atomics.
- **No cgo.** `CGO_ENABLED=0` must build every package. This is why SQLite is `modernc.org/sqlite`.
- **Package naming:** adapter packages are prefixed (`mnemohttp`), never named after the library they instrument.
- **`sdk/` never imports a specific instrumented library.** Enforced by a test in Task 6.
- **Wall clock is display-only.** Ordering is always `(hlc, emitter_id)`. Any comparison of `wall_unix_nanos` for ordering is a bug.
- **Redaction happens before the ring buffer.** Unredacted payloads must never reach the exporter.
- **The SDK never blocks the application and never grows unbounded.** Recording is what gets sacrificed.
- **Commit after every task.** No `Co-Authored-By` trailers.
- **Conventional Commits are mandatory** for every commit: `type(scope): subject`, imperative mood, lowercase subject, no trailing period. Types in use: `feat`, `fix`, `test`, `chore`, `docs`, `refactor`. Each task's commit step gives the exact message to use — use it verbatim.
- **One branch per task**, named `task-NN-<slug>`, developed in its own git worktree and merged to `main` only via a reviewed pull request. Never commit directly to `main`.

## Deviations from the frozen spec

Both are slice decisions, not design changes. Flagged here so they are visible rather than silent:

1. **Ingest is HTTP/protobuf only in Spec 1.** The design names gRPC streaming as primary with an HTTP/protobuf fallback; this plan implements the fallback. gRPC ingest moves to Spec 2, where gRPC interceptors are being built anyway.
2. **`Handle` is nil-safe but not zero-allocation when capture is disabled.** A `Handle` must carry the child `context.Context`, which a shared singleton cannot. Disabled mode skips all event construction and payload capture but retains one small allocation. True zero-alloc requires a value-typed handle API and is deferred.
3. **Redaction matches JSON by key name at any depth, not by JSON path.** The spec's example config uses `$.password`. Key-name matching is strictly broader than a path expression, so it cannot under-redact; full JSON-path support is deferred to whenever a user needs to redact one path but not its namesake elsewhere.

## File structure

| Path | Responsibility |
|---|---|
| `cmd/mnemodev/main.go` | Developer harness. Prints recorded events as JSON. Runnable from Task 0 onward. |
| `cmd/mnemo/main.go` | Collector binary. |
| `schema/proto/mnemosyne/v1/*.proto` | The protocol. |
| `schema/gen/go/` | Generated bindings. Never edited by hand. |
| `sdk/hlc/hlc.go` | Hybrid logical clock. |
| `sdk/identity/identity.go` | EmitterID, EventID, TraceID, SpanID generation + tracestate codec. |
| `sdk/mnemoctx/mnemoctx.go` | Scope carried inside `context.Context`. |
| `sdk/recorder.go` | `Recorder`, `Handle`, event construction. |
| `sdk/capture/` | Ring buffer, redaction, triggers, retroactive flush. |
| `sdk/exporter/` | Batching HTTP client. |
| `sdk/instrument/nethttp/` | `package mnemohttp` — server middleware, client RoundTripper. |
| `store/store.go` | `EventStore` + `BlobStore` interfaces. |
| `store/memory/`, `store/sqlite/`, `store/blobfs/` | Implementations. |
| `collector/ingest/`, `collector/query/`, `collector/api/` | Collector internals. |
| `replay/` | Timeline Model construction. |
| `harness/` | Deterministic incident harness. |
| `ui/` | Split-view replay UI. |

---

## Task 0: Repo scaffold and developer harness

**Objective:** A runnable `mnemodev` binary and a green `go test ./...` exist before any domain code.

**Deliverables:** `go.mod` (including a pinned `tool` directive for `protoc-gen-go`), `Makefile`, `cmd/mnemodev/main.go`, `.gitignore` additions.

**Invariants:** `go build ./...` and `go test ./...` succeed with `CGO_ENABLED=0`.

**Definition of done:** `go run ./cmd/mnemodev -version` prints a version string and exits 0.

**Risks:** Low. The only real risk is a wrong module path propagating into every import — fix it now, not later.

- [ ] **Step 1: Initialize the module and install codegen tools**

```bash
go mod init github.com/ai-mond/mnemosyne
go get -tool google.golang.org/protobuf/cmd/protoc-gen-go@latest
go tool protoc-gen-go --version
```

Expected: `protoc-gen-go v1.36.x` or later, and a `tool` directive in `go.mod`.

Go 1.24 tracks executable dependencies with `tool` directives rather than the pre-1.24 `tools.go` blank-import idiom. This pins the generator's version in `go.mod` and `go.sum`, so every contributor and CI run generates identical bindings — which matters because `schema/gen/go` is committed.

- [ ] **Step 2: Write the failing test**

Create `cmd/mnemodev/main_test.go`:

```go
package main

import "testing"

func TestVersionString(t *testing.T) {
	got := versionString()
	if got == "" {
		t.Fatal("versionString() returned empty string")
	}
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./cmd/mnemodev/`
Expected: FAIL — `undefined: versionString`

- [ ] **Step 4: Write the minimal implementation**

Create `cmd/mnemodev/main.go`:

```go
// Command mnemodev is the Mnemosyne developer harness. It prints recorded
// events as JSON so every implementation step can be exercised by eye.
package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.0.1-dev"

func versionString() string {
	return "mnemodev " + version
}

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(versionString())
		os.Exit(0)
	}

	fmt.Fprintln(os.Stderr, "no input: pass -version (event printing lands in Task 1)")
	os.Exit(2)
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./cmd/mnemodev/`
Expected: PASS

- [ ] **Step 6: Add the Makefile**

Create `Makefile`:

```make
export CGO_ENABLED=0

.PHONY: build test proto lint
build:
	go build ./...
test:
	go test ./...
proto:
	protoc --proto_path=schema/proto \
	  --go_out=schema/gen/go --go_opt=module=github.com/ai-mond/mnemosyne/schema/gen/go \
	  schema/proto/mnemosyne/v1/*.proto
```

- [ ] **Step 7: Verify the harness runs**

Run: `go run ./cmd/mnemodev -version`
Expected: `mnemodev 0.0.1-dev`, exit 0.

- [ ] **Step 8: Commit**

```bash
git add go.mod Makefile cmd/
git commit -m "chore: scaffold module and mnemodev developer harness"
```

---

## Task 1: Protobuf schema

**Objective:** The frozen wire format exists as generated Go types, and `mnemodev` can print an event as JSON.

**Deliverables:** `schema/proto/mnemosyne/v1/{common,event,gap,ingest}.proto`, `schema/gen/go/`, JSON printing in `mnemodev`.

**Interfaces:**
- Produces: `mnemosynev1.Event`, `mnemosynev1.EventID`, `mnemosynev1.HLC`, `mnemosynev1.Gap`, `mnemosynev1.Link`, `mnemosynev1.PayloadRef`, and the enums `Phase`, `Status`, `Completeness`, `OperationType`, `EdgeType`, `GapReason`.

**Invariants:** Field numbers and enum values match the frozen spec exactly. `EdgeType` reserves 5–15. Generated code is never hand-edited.

**Tests:** A round-trip test — marshal an `Event` with every field populated, unmarshal, compare with `proto.Equal`.

**Definition of done:** `go run ./cmd/mnemodev -demo-event` prints a populated event as indented JSON.

**Risks:** Field-number drift between the spec and the proto. Mitigation: the round-trip test populates *every* field, so an omitted field is caught immediately.

- [ ] **Step 1: Write `schema/proto/mnemosyne/v1/common.proto`**

```proto
syntax = "proto3";
package mnemosyne.v1;
option go_package = "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1;mnemosynev1";

message EventID {
  bytes  emitter_id = 1;
  uint64 sequence   = 2;
}

message HLC {
  uint64 wall_ms = 1;
  uint32 logical = 2;
}

message Service {
  string name        = 1;
  string version     = 2;
  bytes  emitter_id  = 3;
}

message ErrorInfo {
  string type    = 1;
  string message = 2;
}

message Value {
  oneof kind {
    string string_value = 1;
    int64  int_value    = 2;
    double double_value = 3;
    bool   bool_value   = 4;
  }
}
```

- [ ] **Step 2: Write `schema/proto/mnemosyne/v1/event.proto`**

```proto
syntax = "proto3";
package mnemosyne.v1;
option go_package = "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1;mnemosynev1";

import "mnemosyne/v1/common.proto";

enum Phase        { PHASE_UNSPECIFIED = 0; BEGIN = 1; END = 2; INSTANT = 3; }
enum Status       { STATUS_UNSPECIFIED = 0; OK = 1; ERROR = 2; TIMEOUT = 3; CANCELED = 4; }
enum Completeness { COMPLETENESS_UNSPECIFIED = 0; FULL = 1; TRUNCATED = 2; METADATA_ONLY = 3; }

enum OperationType {
  OPERATION_TYPE_UNSPECIFIED = 0;
  HTTP_SERVER = 1;
  HTTP_CLIENT = 2;
  LOCAL       = 3;
  DB_QUERY    = 10;
  CACHE_OP    = 20;
  MSG_PRODUCE = 30;
  MSG_CONSUME = 31;
}

enum EdgeType {
  EDGE_TYPE_UNSPECIFIED = 0;
  PARENT      = 1;
  REMOTE_CALL = 2;
  MSG_FLOW    = 3;
  RETRY_OF    = 4;
  reserved 5 to 15;
}

message PayloadRef {
  bytes  sha256    = 1;
  uint64 size      = 2;
  bool   truncated = 3;
  bool   redacted  = 4;
  bool   stored    = 5;
}

message Link {
  EdgeType type            = 1;
  bytes    trace_id        = 2;
  EventID  event           = 3;
  bytes    foreign_span_id = 4;
}

message HttpServer {
  string method = 1;
  string route  = 2;
  string path   = 3;
  uint32 status = 4;
}

message HttpClient {
  string method = 1;
  string host   = 2;
  string path   = 3;
  uint32 status = 4;
}

message Event {
  uint32 schema_version = 1;

  bytes   trace_id = 2;
  EventID id       = 3;
  EventID parent   = 4;
  bytes   span_id  = 5;

  Service service         = 6;
  HLC     hlc             = 7;
  int64   wall_unix_nanos = 8;
  int64   duration_nanos  = 9;

  Phase         phase          = 10;
  OperationType operation_type = 11;
  oneof operation {
    HttpServer http_server = 12;
    HttpClient http_client = 13;
  }

  Status    status = 14;
  ErrorInfo error  = 15;

  repeated Link links        = 16;
  PayloadRef   request       = 17;
  PayloadRef   response      = 18;
  Completeness completeness  = 19;

  map<string, Value> attributes = 20;
  bool attributes_truncated     = 21;
}
```

- [ ] **Step 3: Write `schema/proto/mnemosyne/v1/gap.proto`**

```proto
syntax = "proto3";
package mnemosyne.v1;
option go_package = "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1;mnemosynev1";

import "mnemosyne/v1/common.proto";

enum GapReason {
  GAP_REASON_UNSPECIFIED = 0;
  BACKPRESSURE  = 1;
  RING_EVICTION = 2;
  UNREACHABLE   = 3;
  ATTR_OVERFLOW = 4;
}

message Gap {
  bytes     emitter_id    = 1;
  HLC       from          = 2;
  HLC       to            = 3;
  uint64    dropped_count = 4;
  GapReason reason        = 5;
  bytes     trace_id      = 6;
}
```

- [ ] **Step 4: Write `schema/proto/mnemosyne/v1/ingest.proto`**

```proto
syntax = "proto3";
package mnemosyne.v1;
option go_package = "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1;mnemosynev1";

import "mnemosyne/v1/event.proto";
import "mnemosyne/v1/gap.proto";

message Batch {
  repeated Event events = 1;
  repeated Gap   gaps   = 2;
}

message BatchAck {
  uint32 accepted   = 1;
  bool   shed       = 2;  // caller should reduce volume
}
```

- [ ] **Step 5: Generate and verify it compiles**

```bash
mkdir -p schema/gen/go
make proto
go build ./...
```

Expected: no output, exit 0. `schema/gen/go/mnemosyne/v1/*.pb.go` exists.

- [ ] **Step 6: Write the round-trip test**

Create `schema/roundtrip_test.go`:

```go
package schema_test

import (
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"google.golang.org/protobuf/proto"
)

// fullEvent populates every field so an omission in the .proto is caught here.
func fullEvent() *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       make([]byte, 16),
		Id:            &v1.EventID{EmitterId: make([]byte, 8), Sequence: 7},
		Parent:        &v1.EventID{EmitterId: make([]byte, 8), Sequence: 3},
		SpanId:        make([]byte, 8),
		Service:       &v1.Service{Name: "checkout-api", Version: "1.2.3", EmitterId: make([]byte, 8)},
		Hlc:           &v1.HLC{WallMs: 1700000000000, Logical: 4},
		WallUnixNanos: 1700000000000000000,
		DurationNanos: 1500,
		Phase:         v1.Phase_END,
		OperationType: v1.OperationType_HTTP_SERVER,
		Operation:     &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "POST", Route: "/checkout", Path: "/checkout", Status: 500}},
		Status:        v1.Status_ERROR,
		Error:         &v1.ErrorInfo{Type: "timeout", Message: "upstream deadline exceeded"},
		Links: []*v1.Link{{
			Type:          v1.EdgeType_RETRY_OF,
			TraceId:       make([]byte, 16),
			Event:         &v1.EventID{EmitterId: make([]byte, 8), Sequence: 2},
			ForeignSpanId: make([]byte, 8),
		}},
		Request:             &v1.PayloadRef{Sha256: make([]byte, 32), Size: 120, Truncated: true, Redacted: true, Stored: true},
		Response:            &v1.PayloadRef{Sha256: make([]byte, 32), Size: 64, Stored: false},
		Completeness:        v1.Completeness_TRUNCATED,
		Attributes:          map[string]*v1.Value{"http.request.method": {Kind: &v1.Value_StringValue{StringValue: "POST"}}},
		AttributesTruncated: true,
	}
}

// httpClientEvent reaches the oneof branches fullEvent cannot. A oneof holds
// exactly one variant, so a single fixture can never exercise HttpClient or
// the non-string Value kinds - a transposed field number in either would
// round-trip silently, defeating the point of this test.
func httpClientEvent() *v1.Event {
	e := fullEvent()
	e.OperationType = v1.OperationType_HTTP_CLIENT
	e.Operation = &v1.Event_HttpClient{HttpClient: &v1.HttpClient{
		Method: "GET", Host: "inventory-svc:8080", Path: "/stock", Status: 503,
	}}
	e.Attributes = map[string]*v1.Value{
		"retry.attempt":   {Kind: &v1.Value_IntValue{IntValue: 3}},
		"latency.seconds": {Kind: &v1.Value_DoubleValue{DoubleValue: 1.75}},
		"cache.hit":       {Kind: &v1.Value_BoolValue{BoolValue: true}},
	}
	return e
}

func TestEventRoundTrip(t *testing.T) {
	cases := map[string]*v1.Event{
		"http_server_with_string_attribute":  fullEvent(),
		"http_client_with_scalar_attributes": httpClientEvent(),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			b, err := proto.Marshal(in)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var out v1.Event
			if err := proto.Unmarshal(b, &out); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !proto.Equal(in, &out) {
				t.Fatalf("round-trip mismatch:\n in: %v\nout: %v", in, &out)
			}
		})
	}
}

func TestEdgeTypeReservesFutureRange(t *testing.T) {
	// FORKS_TO and friends occupy 5..15; nothing may claim any of them.
	for v := int32(5); v <= 15; v++ {
		if name, ok := v1.EdgeType_name[v]; ok && name != "" {
			t.Fatalf("edge type %d is reserved but named %q", v, name)
		}
	}
}
```

- [ ] **Step 7: Run the tests**

Run: `go test ./schema/`
Expected: PASS (both tests).

- [ ] **Step 8: Teach mnemodev to print events**

Replace the `main` function body in `cmd/mnemodev/main.go`:

```go
func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	demoEvent := flag.Bool("demo-event", false, "print a fully-populated demo event as JSON")
	flag.Parse()

	switch {
	case *showVersion:
		fmt.Println(versionString())
	case *demoEvent:
		if err := PrintEvent(os.Stdout, demo()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "no input: pass -version or -demo-event")
		os.Exit(2)
	}
}
```

Create `cmd/mnemodev/print.go`:

```go
package main

import (
	"io"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// PrintEvent writes an event as indented JSON. This is the developer harness's
// only job until Task 7 gives it a live event stream to read.
func PrintEvent(w io.Writer, e *v1.Event) error {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(e)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func demo() *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       []byte("0123456789abcdef"),
		Id:            &v1.EventID{EmitterId: []byte("emitter1"), Sequence: 1},
		Service:       &v1.Service{Name: "mnemodev"},
		Hlc:           &v1.HLC{WallMs: 1700000000000},
		Phase:         v1.Phase_INSTANT,
		OperationType: v1.OperationType_LOCAL,
		Status:        v1.Status_OK,
	}
}
```

- [ ] **Step 9: Verify the harness prints**

Run: `go run ./cmd/mnemodev -demo-event`
Expected: indented JSON containing `"serviceName"`-style camelCase fields and `"phase": "INSTANT"`.

- [ ] **Step 10: Commit**

```bash
git add schema/ cmd/ Makefile
git commit -m "feat(schema): add frozen v1 protobuf schema and event JSON printing"
```

---

## Task 2: Hybrid logical clock

**Objective:** A clock that produces timestamps which never contradict happens-before, with no clock synchronization.

**Deliverables:** `sdk/hlc/hlc.go`, `sdk/hlc/hlc_test.go`.

**Interfaces:**
- Produces: `hlc.Timestamp{WallMS uint64; Logical uint32}`, `hlc.Timestamp.Less(other) bool`, `hlc.New(now func() time.Time) *Clock`, `(*Clock).Now() Timestamp`, `(*Clock).Observe(remote Timestamp) Timestamp`.

**Invariants:**
1. `Now()` is strictly increasing under `Less` on a single clock, even if the physical clock goes backwards.
2. If A happens-before B — same process, or B's clock called `Observe(A.hlc)` — then `A.Less(B)`.
3. `Observe` never regresses local state.

**Tests:** Three unit tests (monotonic under a frozen clock, monotonic under a *backwards* clock, `Observe` dominates a remote timestamp) plus one `rapid` property test over randomized send/receive interleavings across N simulated processes.

**Definition of done:** The property test passes with 1000 randomized interleavings across 5 processes, asserting no happens-before edge is ever contradicted by HLC order.

**Risks:** This is the highest-risk component in Spec 1 alongside Task 4. A subtly wrong `Observe` produces an ordering that is *usually* right, which means bugs surface as rare, unreproducible timeline weirdness in production. The property test is the mitigation and must be written first. Second risk: using atomics on `windows/386` — avoided by using a mutex, which is also simpler to reason about here.

- [ ] **Step 1: Add the property-test dependency**

```bash
go get pgregory.net/rapid@latest
```

- [ ] **Step 2: Write the failing unit tests**

Create `sdk/hlc/hlc_test.go`:

```go
package hlc_test

import (
	"testing"
	"time"

	"github.com/ai-mond/mnemosyne/sdk/hlc"
)

func frozen(ms int64) func() time.Time {
	return func() time.Time { return time.UnixMilli(ms) }
}

func TestNowIsStrictlyIncreasingUnderFrozenClock(t *testing.T) {
	c := hlc.New(frozen(1000))
	prev := c.Now()
	for i := 0; i < 100; i++ {
		got := c.Now()
		if !prev.Less(got) {
			t.Fatalf("iteration %d: %v is not < %v", i, prev, got)
		}
		prev = got
	}
}

func TestNowIsStrictlyIncreasingWhenPhysicalClockGoesBackwards(t *testing.T) {
	ms := int64(5000)
	c := hlc.New(func() time.Time { return time.UnixMilli(ms) })
	prev := c.Now()
	for i := 0; i < 50; i++ {
		ms -= 10 // physical clock runs backwards
		got := c.Now()
		if !prev.Less(got) {
			t.Fatalf("iteration %d: %v is not < %v", i, prev, got)
		}
		prev = got
	}
}

func TestObserveDominatesRemoteTimestamp(t *testing.T) {
	c := hlc.New(frozen(1000))
	remote := hlc.Timestamp{WallMS: 9999, Logical: 42}
	got := c.Observe(remote)
	if !remote.Less(got) {
		t.Fatalf("Observe(%v) returned %v, which is not strictly after remote", remote, got)
	}
	// Local state must have absorbed the remote wall time.
	next := c.Now()
	if !got.Less(next) {
		t.Fatalf("Now() after Observe returned %v, not after %v", next, got)
	}
}

func TestObserveNeverRegressesLocalState(t *testing.T) {
	c := hlc.New(frozen(8000))
	local := c.Now()
	got := c.Observe(hlc.Timestamp{WallMS: 10, Logical: 0}) // stale remote
	if !local.Less(got) {
		t.Fatalf("Observe of a stale remote regressed: %v is not < %v", local, got)
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./sdk/hlc/`
Expected: build failure — `no required module provides package .../sdk/hlc`

- [ ] **Step 4: Write the implementation**

Create `sdk/hlc/hlc.go`:

```go
// Package hlc implements a hybrid logical clock.
//
// An HLC timestamp orders events consistently with happens-before without
// requiring synchronized physical clocks. Wall time is carried only so that
// timestamps stay close to real time; ordering never depends on clocks
// agreeing between machines.
package hlc

import (
	"sync"
	"time"
)

// Timestamp is a hybrid logical clock reading.
type Timestamp struct {
	WallMS  uint64
	Logical uint32
}

// Less reports whether t orders strictly before o.
func (t Timestamp) Less(o Timestamp) bool {
	if t.WallMS != o.WallMS {
		return t.WallMS < o.WallMS
	}
	return t.Logical < o.Logical
}

// Clock is a hybrid logical clock. It is safe for concurrent use.
//
// A mutex is used rather than atomics: the update is a read-modify-write of
// two fields, and 64-bit atomics on 32-bit platforms carry alignment
// requirements that are easy to violate accidentally.
type Clock struct {
	mu   sync.Mutex
	now  func() time.Time
	last Timestamp
}

// New returns a Clock reading physical time from now. A nil now uses time.Now.
func New(now func() time.Time) *Clock {
	if now == nil {
		now = time.Now
	}
	return &Clock{now: now}
}

// Now returns a timestamp for a locally-generated event. The returned
// timestamp is strictly greater than every timestamp previously returned by
// this Clock, even if physical time moves backwards.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := uint64(c.now().UnixMilli())
	if phys > c.last.WallMS {
		c.last = Timestamp{WallMS: phys, Logical: 0}
	} else {
		c.last.Logical++
	}
	return c.last
}

// Observe merges a timestamp received from another process and returns a
// timestamp strictly greater than both the remote timestamp and every
// timestamp previously returned by this Clock.
func (c *Clock) Observe(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := uint64(c.now().UnixMilli())
	wall := phys
	if c.last.WallMS > wall {
		wall = c.last.WallMS
	}
	if remote.WallMS > wall {
		wall = remote.WallMS
	}

	switch {
	case wall == c.last.WallMS && wall == remote.WallMS:
		logical := c.last.Logical
		if remote.Logical > logical {
			logical = remote.Logical
		}
		c.last.Logical = logical + 1
	case wall == c.last.WallMS:
		c.last.Logical++
	case wall == remote.WallMS:
		c.last.Logical = remote.Logical + 1
	default: // physical time dominates both
		c.last.Logical = 0
	}
	c.last.WallMS = wall
	return c.last
}
```

- [ ] **Step 5: Run the unit tests**

Run: `go test ./sdk/hlc/ -v`
Expected: PASS — all four tests.

- [ ] **Step 6: Write the property test**

Append to `sdk/hlc/hlc_test.go`:

```go
// TestHappensBeforeIsNeverContradicted drives N simulated processes through
// randomized local events and message sends, recording the happens-before
// relation explicitly, then asserts HLC order agrees with every edge.
func TestHappensBeforeIsNeverContradicted(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		const procs = 5
		// Each process has its own skewed, independently-drifting physical clock.
		skew := make([]int64, procs)
		for i := range skew {
			skew[i] = rapid.Int64Range(-2000, 2000).Draw(rt, "skew")
		}
		base := int64(1_700_000_000_000)
		tick := make([]int64, procs)

		clocks := make([]*hlc.Clock, procs)
		for i := range clocks {
			i := i
			clocks[i] = hlc.New(func() time.Time {
				return time.UnixMilli(base + skew[i] + tick[i])
			})
		}

		type record struct {
			proc int
			ts   hlc.Timestamp
		}
		// last[i] is the most recent event on process i.
		last := make([]*record, procs)
		// edges holds (before, after) pairs that must satisfy before.ts < after.ts.
		var edges [][2]record

		steps := rapid.IntRange(20, 200).Draw(rt, "steps")
		for s := 0; s < steps; s++ {
			p := rapid.IntRange(0, procs-1).Draw(rt, "proc")
			tick[p] += rapid.Int64Range(0, 5).Draw(rt, "advance")

			if rapid.Bool().Draw(rt, "isReceive") {
				q := rapid.IntRange(0, procs-1).Draw(rt, "sender")
				if last[q] != nil && q != p {
					ts := clocks[p].Observe(last[q].ts)
					cur := record{proc: p, ts: ts}
					edges = append(edges, [2]record{*last[q], cur}) // send -> receive
					if last[p] != nil {
						edges = append(edges, [2]record{*last[p], cur}) // program order
					}
					last[p] = &cur
					continue
				}
			}

			ts := clocks[p].Now()
			cur := record{proc: p, ts: ts}
			if last[p] != nil {
				edges = append(edges, [2]record{*last[p], cur}) // program order
			}
			last[p] = &cur
		}

		for _, e := range edges {
			if !e[0].ts.Less(e[1].ts) {
				rt.Fatalf("happens-before violated: p%d %v is not < p%d %v",
					e[0].proc, e[0].ts, e[1].proc, e[1].ts)
			}
		}
	})
}
```

Add the import `"pgregory.net/rapid"` to the test file's import block.

- [ ] **Step 7: Run the property test**

Run: `go test ./sdk/hlc/ -run TestHappensBefore -rapid.checks=1000 -v`
Expected: PASS with `[rapid] OK, passed 1000 tests`.

- [ ] **Step 8: Commit**

```bash
git add sdk/hlc/ go.mod go.sum
git commit -m "feat(sdk): add hybrid logical clock with happens-before property test"
```

---

## Task 3: Identity

**Objective:** The SDK owns all identity generation, and event IDs survive a round trip through a W3C `tracestate` entry.

**Deliverables:** `sdk/identity/identity.go`, `sdk/identity/tracestate.go`, tests for both.

**Interfaces:**
- Consumes: `hlc.Timestamp` (Task 2).
- Produces:
  - `identity.EmitterID [8]byte`, `identity.TraceID [16]byte`, `identity.SpanID [8]byte`
  - `identity.EventID{Emitter EmitterID; Sequence uint64}`, `(EventID).IsZero() bool`
  - `identity.NewGenerator() (*Generator, error)`, `(*Generator).Emitter() EmitterID`, `(*Generator).NextEvent() EventID`, `(*Generator).NewTrace() (TraceID, error)`, `(*Generator).NewSpan() (SpanID, error)`
  - `identity.EncodeTracestate(hlc.Timestamp, EventID) string`, `identity.DecodeTracestate(string) (hlc.Timestamp, EventID, error)`
  - `identity.ParseTraceparent(string) (TraceID, SpanID, bool)`, `identity.FormatTraceparent(TraceID, SpanID, bool) string`

**Invariants:**
1. `NextEvent()` is strictly monotonic per generator and unique across generators (distinct random emitter IDs).
2. A `tracestate` value never exceeds 256 characters — the W3C per-entry limit.
3. `DecodeTracestate(EncodeTracestate(x)) == x` for all inputs.
4. On `windows/386`, the sequence counter must be `atomic.Uint64` (the type), never `atomic.AddUint64` on a field.

**Tests:** Monotonicity, cross-generator uniqueness, tracestate round-trip property test with a length assertion, traceparent parse/format round-trip, and rejection of malformed traceparent values.

**Definition of done:** `go test ./sdk/identity/ -race` passes, including a property test asserting round-trip fidelity and the 256-character bound across the full `uint64` sequence range.

**Risks:** The encoding is the risk. `tracestate` has a restricted character set (lowercase alphanumeric plus a few punctuation marks) and a hard length cap; an encoding that works for small sequence numbers and silently breaks at high ones would fail only after a service had been up for a long time. The property test draws from the full `uint64` range specifically to catch that.

- [ ] **Step 1: Write the failing identity tests**

Create `sdk/identity/identity_test.go`:

```go
package identity_test

import (
	"testing"

	"github.com/ai-mond/mnemosyne/sdk/identity"
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
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sdk/identity/`
Expected: build failure — package does not exist.

- [ ] **Step 3: Write the identity implementation**

Create `sdk/identity/identity.go`:

```go
// Package identity owns all Mnemosyne identifier generation. No instrumentation
// adapter ever mints an identifier; centralising this is what keeps future
// language SDKs from diverging.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
)

type (
	// EmitterID identifies whoever produced an event. It deliberately does not
	// commit to pod, process, container, or VM semantics.
	EmitterID [8]byte
	// TraceID is W3C-compatible: 16 bytes.
	TraceID [16]byte
	// SpanID is W3C-compatible: 8 bytes. Assigned only to events that cross a
	// process boundary.
	SpanID [8]byte
)

func (e EmitterID) String() string { return hex.EncodeToString(e[:]) }
func (t TraceID) String() string   { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string    { return hex.EncodeToString(s[:]) }

// EventID is the primary identity of every recorded event. It is monotonic,
// compact, and sortable by construction.
type EventID struct {
	Emitter  EmitterID
	Sequence uint64
}

// IsZero reports whether the EventID is unset. Sequence numbers start at 1 so
// that the zero value is unambiguous.
func (e EventID) IsZero() bool { return e.Sequence == 0 }

// Generator mints identifiers for one process. It is safe for concurrent use.
type Generator struct {
	// seq is the atomic.Uint64 type rather than a bare uint64 field: on 32-bit
	// platforms the type carries the 8-byte alignment guarantee that raw
	// atomic.AddUint64 on a struct field does not.
	seq     atomic.Uint64
	emitter EmitterID
}

// NewGenerator returns a Generator with a randomly-assigned emitter ID.
func NewGenerator() (*Generator, error) {
	g := &Generator{}
	if _, err := rand.Read(g.emitter[:]); err != nil {
		return nil, err
	}
	return g, nil
}

// Emitter returns this generator's emitter ID.
func (g *Generator) Emitter() EmitterID { return g.emitter }

// NextEvent returns the next event ID. Sequence numbers start at 1.
func (g *Generator) NextEvent() EventID {
	return EventID{Emitter: g.emitter, Sequence: g.seq.Add(1)}
}

// NewTrace returns a fresh random trace ID.
func (g *Generator) NewTrace() (TraceID, error) {
	var t TraceID
	_, err := rand.Read(t[:])
	return t, err
}

// NewSpan returns a fresh random span ID.
func (g *Generator) NewSpan() (SpanID, error) {
	var s SpanID
	_, err := rand.Read(s[:])
	return s, err
}
```

- [ ] **Step 4: Run the identity tests**

Run: `go test ./sdk/identity/ -race -v`
Expected: PASS — all four tests.

- [ ] **Step 5: Write the failing propagation-codec tests**

Create `sdk/identity/tracestate_test.go`:

```go
package identity_test

import (
	"strings"
	"testing"

	"github.com/ai-mond/mnemosyne/sdk/hlc"
	"github.com/ai-mond/mnemosyne/sdk/identity"
	"pgregory.net/rapid"
)

func TestTracestateRoundTripAndLengthBound(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ts := hlc.Timestamp{
			WallMS:  rapid.Uint64().Draw(rt, "wall"),
			Logical: rapid.Uint32().Draw(rt, "logical"),
		}
		var em identity.EmitterID
		for i := range em {
			em[i] = rapid.Byte().Draw(rt, "emitterByte")
		}
		id := identity.EventID{Emitter: em, Sequence: rapid.Uint64().Draw(rt, "seq")}

		enc := identity.EncodeTracestate(ts, id)
		if len(enc) > 256 {
			rt.Fatalf("tracestate value is %d chars, exceeds the W3C 256 limit: %q", len(enc), enc)
		}
		if strings.ContainsAny(enc, " ,=") {
			rt.Fatalf("tracestate value contains a forbidden character: %q", enc)
		}

		gotTS, gotID, err := identity.DecodeTracestate(enc)
		if err != nil {
			rt.Fatalf("decode %q: %v", enc, err)
		}
		if gotTS != ts || gotID != id {
			rt.Fatalf("round-trip mismatch: got (%v,%v) want (%v,%v)", gotTS, gotID, ts, id)
		}
	})
}

func TestDecodeTracestateRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "1", "9-a-b-c-d", "1-zz-0-aa-0", "1-0-0-0"} {
		if _, _, err := identity.DecodeTracestate(in); err == nil {
			t.Fatalf("DecodeTracestate(%q) succeeded, want error", in)
		}
	}
}

func TestTraceparentRoundTrip(t *testing.T) {
	g, _ := identity.NewGenerator()
	tr, _ := g.NewTrace()
	sp, _ := g.NewSpan()

	got, gotSpan, ok := identity.ParseTraceparent(identity.FormatTraceparent(tr, sp, true))
	if !ok {
		t.Fatal("ParseTraceparent rejected a value produced by FormatTraceparent")
	}
	if got != tr || gotSpan != sp {
		t.Fatalf("round-trip mismatch: got (%v,%v) want (%v,%v)", got, gotSpan, tr, sp)
	}
}

func TestParseTraceparentRejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331", // missing flags
		"00-0af7651916cd43dd8448eb211c80319c-00000000000000000-01", // span too long
		"00-00000000000000000000000000000000-b7ad6b7169203331-01", // all-zero trace id
		"00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01", // all-zero span id
		"zz-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // bad version
	}
	for _, in := range cases {
		if _, _, ok := identity.ParseTraceparent(in); ok {
			t.Fatalf("ParseTraceparent(%q) accepted a malformed value", in)
		}
	}
}
```

- [ ] **Step 6: Run to verify failure**

Run: `go test ./sdk/identity/ -run Tracestate`
Expected: FAIL — `undefined: identity.EncodeTracestate`

- [ ] **Step 7: Write the propagation codec**

Create `sdk/identity/tracestate.go`:

```go
package identity

import (
	"encoding/base32"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/ai-mond/mnemosyne/sdk/hlc"
)

// TracestateKey is the W3C tracestate key Mnemosyne claims.
const TracestateKey = "mnemo"

// tracestateVersion prefixes every value so the encoding can evolve.
const tracestateVersion = "1"

// emitterEnc encodes emitter IDs using lowercase base32 without padding, which
// stays inside the W3C tracestate value character set.
var emitterEnc = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// EncodeTracestate renders the HLC and event ID as a W3C tracestate value.
//
// Layout: <version>-<wallMS b36>-<logical b36>-<emitter b32>-<sequence b36>.
// Worst case is 1 + 1 + 13 + 1 + 7 + 1 + 13 + 1 + 13 = 51 characters, well
// inside the 256-character per-entry limit.
func EncodeTracestate(ts hlc.Timestamp, id EventID) string {
	var b strings.Builder
	b.WriteString(tracestateVersion)
	b.WriteByte('-')
	b.WriteString(strconv.FormatUint(ts.WallMS, 36))
	b.WriteByte('-')
	b.WriteString(strconv.FormatUint(uint64(ts.Logical), 36))
	b.WriteByte('-')
	b.WriteString(emitterEnc.EncodeToString(id.Emitter[:]))
	b.WriteByte('-')
	b.WriteString(strconv.FormatUint(id.Sequence, 36))
	return b.String()
}

var errBadTracestate = errors.New("mnemosyne: malformed tracestate value")

// DecodeTracestate parses a value produced by EncodeTracestate.
func DecodeTracestate(v string) (hlc.Timestamp, EventID, error) {
	parts := strings.Split(v, "-")
	if len(parts) != 5 || parts[0] != tracestateVersion {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}

	wall, err := strconv.ParseUint(parts[1], 36, 64)
	if err != nil {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}
	logical, err := strconv.ParseUint(parts[2], 36, 32)
	if err != nil {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}
	raw, err := emitterEnc.DecodeString(parts[3])
	if err != nil || len(raw) != 8 {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}
	seq, err := strconv.ParseUint(parts[4], 36, 64)
	if err != nil {
		return hlc.Timestamp{}, EventID{}, errBadTracestate
	}

	var em EmitterID
	copy(em[:], raw)
	return hlc.Timestamp{WallMS: wall, Logical: uint32(logical)},
		EventID{Emitter: em, Sequence: seq}, nil
}

// FormatTraceparent renders a W3C traceparent header value.
func FormatTraceparent(t TraceID, s SpanID, sampled bool) string {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return "00-" + hex.EncodeToString(t[:]) + "-" + hex.EncodeToString(s[:]) + "-" + flags
}

// ParseTraceparent parses a W3C traceparent header value. It rejects the
// all-zero trace and span IDs, which the specification defines as invalid.
func ParseTraceparent(v string) (TraceID, SpanID, bool) {
	var t TraceID
	var s SpanID

	parts := strings.Split(v, "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return t, s, false
	}
	if parts[0] != "00" {
		return t, s, false
	}
	tb, err := hex.DecodeString(parts[1])
	if err != nil {
		return t, s, false
	}
	sb, err := hex.DecodeString(parts[2])
	if err != nil {
		return t, s, false
	}
	copy(t[:], tb)
	copy(s[:], sb)
	if t == (TraceID{}) || s == (SpanID{}) {
		return TraceID{}, SpanID{}, false
	}
	return t, s, true
}
```

- [ ] **Step 8: Run the full identity suite**

Run: `go test ./sdk/identity/ -race -rapid.checks=1000 -v`
Expected: PASS — all tests, including the round-trip property test at 1000 checks.

- [ ] **Step 9: Commit**

```bash
git add sdk/identity/
git commit -m "feat(sdk): add identity generation and W3C propagation codec"
```

---

## Task 4: Recorder, Handle, and mnemoctx

**Objective:** The scope invariant holds structurally, not by discipline — sibling operations are siblings and nested operations are nested, under arbitrary nesting.

**Deliverables:** `sdk/mnemoctx/mnemoctx.go`, `sdk/recorder.go`, `sdk/operation.go`, tests for each.

**Interfaces:**
- Consumes: `hlc.Clock` (Task 2), `identity.Generator` (Task 3), `v1.Event` (Task 1).
- Produces:
  - `mnemoctx.Scope{TraceID identity.TraceID; Current identity.EventID; Sampled bool}`, `mnemoctx.With(context.Context, Scope) context.Context`, `mnemoctx.From(context.Context) (Scope, bool)`
  - `sdk.Sink interface { Emit(*v1.Event) }`
  - `sdk.Operation` interface with implementations `sdk.HTTPServer{Method, Route, Path string}`, `sdk.HTTPClient{Method, Host, Path string}`, `sdk.Local{Name string}`
  - `sdk.New(Config) (*Recorder, error)` where `Config{Service string; Version string; Sink Sink; Now func() time.Time; Enabled bool}`
  - `(*Recorder).Begin(context.Context, Operation) *Handle`
  - `(*Recorder).BeginRemote(context.Context, Remote, Operation) *Handle`
  - `(*Recorder).Instant(context.Context, Operation)`
  - `sdk.Remote{TraceID identity.TraceID; ParentSpan identity.SpanID; ParentEvent identity.EventID; HLC hlc.Timestamp; HasHLC bool; Foreign bool}`
  - `(*Handle).Context() context.Context`, `.ID() identity.EventID`, `.SpanID() identity.SpanID`, `.SetStatus(v1.Status)`, `.Fail(error)`, `.AddLink(*v1.Link)`, `.End()`

**Invariants:**
1. **Scope invariant.** The current event changes only inside an open `Handle`'s extent. `Begin` returns a new context *on the Handle*; it never mutates or replaces the caller's context. Two sequential `Begin` calls under one context are siblings.
2. `End()` is idempotent. A second call is a no-op, not a duplicate event.
3. Every `Handle` emits exactly one `BEGIN` and at most one `END`, sharing one `EventID`.
4. All `Handle` methods are safe on a nil receiver.
5. When `Enabled` is false, no event is constructed and nothing is emitted.
6. A remote parent with `Foreign: true` contributes a `REMOTE_CALL` link carrying `foreign_span_id`, never a `parent` field.

**Tests:** Sibling regression test, nesting test, idempotent `End`, nil-receiver safety, disabled-recorder emits nothing, foreign-parent linking, and a `rapid` property test over randomized nesting trees asserting every emitted event's parent equals its lexically enclosing scope.

**Definition of done:** Nested `Begin`/`End()` calls always produce sibling and parent relationships satisfying the scope invariant under 1000 randomized nesting trees, and `go test ./sdk/... -race` is green.

**Risks:** Highest-risk task in Spec 1. Three specific failure modes:
- **The linked-list bug.** If the API were `ctx, h := rec.Begin(ctx, ...)`, callers would reassign `ctx` and silently turn siblings into a chain. The API returns the child context *on the Handle* specifically to make that impossible. Do not change this signature for ergonomics.
- **Context loss across goroutines.** A `Handle` whose context is not propagated into a spawned goroutine produces orphaned events. Spec 1 accepts this; goroutine tracking is V2. Document it, don't fix it.
- **Emitting on `Begin` before the operation's outcome is known.** `BEGIN` is metadata-only by design; resist attaching outcome fields to it.

- [ ] **Step 1: Write the failing mnemoctx test**

Create `sdk/mnemoctx/mnemoctx_test.go`:

```go
package mnemoctx_test

import (
	"context"
	"testing"

	"github.com/ai-mond/mnemosyne/sdk/identity"
	"github.com/ai-mond/mnemosyne/sdk/mnemoctx"
)

func TestFromReturnsFalseOnBareContext(t *testing.T) {
	if _, ok := mnemoctx.From(context.Background()); ok {
		t.Fatal("From returned ok on a context with no scope")
	}
}

func TestWithDoesNotMutateParentContext(t *testing.T) {
	parent := mnemoctx.With(context.Background(), mnemoctx.Scope{
		Current: identity.EventID{Sequence: 1},
	})
	_ = mnemoctx.With(parent, mnemoctx.Scope{Current: identity.EventID{Sequence: 2}})

	got, ok := mnemoctx.From(parent)
	if !ok {
		t.Fatal("parent lost its scope")
	}
	if got.Current.Sequence != 1 {
		t.Fatalf("parent scope mutated: Current.Sequence = %d, want 1", got.Current.Sequence)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sdk/mnemoctx/`
Expected: build failure — package does not exist.

- [ ] **Step 3: Write mnemoctx**

Create `sdk/mnemoctx/mnemoctx.go`:

```go
// Package mnemoctx carries the Mnemosyne recording scope inside a
// context.Context.
//
// Scope is a value type and is stored by value. Deriving a child scope
// therefore produces a new context without affecting the parent, which is what
// makes the scope invariant structural: an inner operation cannot change what
// the enclosing scope sees.
package mnemoctx

import (
	"context"

	"github.com/ai-mond/mnemosyne/sdk/identity"
)

// Scope is the recording state for the currently-executing operation.
type Scope struct {
	// TraceID is the trace this execution belongs to.
	TraceID identity.TraceID
	// Current is the innermost open operation. Zero at the trace root.
	Current identity.EventID
	// Sampled mirrors the W3C sampled flag.
	Sampled bool
}

type key struct{}

// With returns a context carrying s. The parent context is unchanged.
func With(ctx context.Context, s Scope) context.Context {
	return context.WithValue(ctx, key{}, s)
}

// From returns the scope carried by ctx, if any.
func From(ctx context.Context) (Scope, bool) {
	s, ok := ctx.Value(key{}).(Scope)
	return s, ok
}
```

- [ ] **Step 4: Run the mnemoctx tests**

Run: `go test ./sdk/mnemoctx/ -v`
Expected: PASS — both tests.

- [ ] **Step 5: Write the operation types**

Create `sdk/operation.go`:

```go
package sdk

import v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"

// Operation describes what is being recorded. Implementations populate the
// operation-specific fields of an event.
type Operation interface {
	applyTo(*v1.Event)
}

// HTTPServer records an inbound HTTP request.
type HTTPServer struct {
	Method string
	Route  string
	Path   string
	Status uint32
}

func (o HTTPServer) applyTo(e *v1.Event) {
	e.OperationType = v1.OperationType_HTTP_SERVER
	e.Operation = &v1.Event_HttpServer{HttpServer: &v1.HttpServer{
		Method: o.Method, Route: o.Route, Path: o.Path, Status: o.Status,
	}}
}

// HTTPClient records an outbound HTTP request.
type HTTPClient struct {
	Method string
	Host   string
	Path   string
	Status uint32
}

func (o HTTPClient) applyTo(e *v1.Event) {
	e.OperationType = v1.OperationType_HTTP_CLIENT
	e.Operation = &v1.Event_HttpClient{HttpClient: &v1.HttpClient{
		Method: o.Method, Host: o.Host, Path: o.Path, Status: o.Status,
	}}
}

// Local records in-process work with no external dependency.
type Local struct {
	Name string
}

func (o Local) applyTo(e *v1.Event) {
	e.OperationType = v1.OperationType_LOCAL
	if o.Name != "" {
		if e.Attributes == nil {
			e.Attributes = map[string]*v1.Value{}
		}
		e.Attributes["operation.name"] = &v1.Value{Kind: &v1.Value_StringValue{StringValue: o.Name}}
	}
}
```

- [ ] **Step 6: Write the failing recorder tests**

Create `sdk/recorder_test.go`:

```go
package sdk_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk"
	"github.com/ai-mond/mnemosyne/sdk/identity"
)

// capturingSink records every emitted event for assertion.
type capturingSink struct {
	mu     sync.Mutex
	events []*v1.Event
}

func (s *capturingSink) Emit(e *v1.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *capturingSink) ends() []*v1.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*v1.Event
	for _, e := range s.events {
		if e.Phase == v1.Phase_END {
			out = append(out, e)
		}
	}
	return out
}

func newTestRecorder(t *testing.T) (*sdk.Recorder, *capturingSink) {
	t.Helper()
	sink := &capturingSink{}
	ms := int64(1_700_000_000_000)
	rec, err := sdk.New(sdk.Config{
		Service: "test-svc",
		Version: "0.0.1",
		Sink:    sink,
		Enabled: true,
		Now: func() time.Time {
			ms++
			return time.UnixMilli(ms)
		},
	})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	return rec, sink
}

// TestSequentialOperationsAreSiblings is the regression test for the
// linked-list bug: recording an operation must not change what the enclosing
// scope sees.
func TestSequentialOperationsAreSiblings(t *testing.T) {
	rec, sink := newTestRecorder(t)
	ctx := context.Background()

	root := rec.Begin(ctx, sdk.HTTPServer{Method: "POST", Route: "/checkout"})
	inner := root.Context()

	a := rec.Begin(inner, sdk.Local{Name: "a"})
	a.End()
	b := rec.Begin(inner, sdk.Local{Name: "b"})
	b.End()
	root.End()

	byID := map[uint64]*v1.Event{}
	for _, e := range sink.ends() {
		byID[e.Id.Sequence] = e
	}
	ea, eb := byID[a.ID().Sequence], byID[b.ID().Sequence]
	if ea == nil || eb == nil {
		t.Fatal("missing END events for a and b")
	}
	if ea.Parent.Sequence != root.ID().Sequence {
		t.Fatalf("a.parent = %d, want root %d", ea.Parent.Sequence, root.ID().Sequence)
	}
	if eb.Parent.Sequence != root.ID().Sequence {
		t.Fatalf("b.parent = %d, want root %d (b was made a child of a — the linked-list bug)",
			eb.Parent.Sequence, root.ID().Sequence)
	}
}

func TestNestedOperationsAreNested(t *testing.T) {
	rec, sink := newTestRecorder(t)
	outer := rec.Begin(context.Background(), sdk.Local{Name: "outer"})
	inner := rec.Begin(outer.Context(), sdk.Local{Name: "inner"})
	inner.End()
	outer.End()

	for _, e := range sink.ends() {
		if e.Id.Sequence == inner.ID().Sequence && e.Parent.Sequence != outer.ID().Sequence {
			t.Fatalf("inner.parent = %d, want %d", e.Parent.Sequence, outer.ID().Sequence)
		}
	}
}

func TestEndIsIdempotent(t *testing.T) {
	rec, sink := newTestRecorder(t)
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.End()
	h.End()
	if got := len(sink.ends()); got != 1 {
		t.Fatalf("got %d END events, want 1", got)
	}
}

func TestBeginEmitsExactlyOneBeginPhase(t *testing.T) {
	rec, sink := newTestRecorder(t)
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	sink.mu.Lock()
	n := len(sink.events)
	phase := sink.events[0].Phase
	sink.mu.Unlock()
	if n != 1 || phase != v1.Phase_BEGIN {
		t.Fatalf("after Begin: %d events, first phase %v; want 1 BEGIN", n, phase)
	}
	h.End()
}

func TestFailSetsErrorStatus(t *testing.T) {
	rec, sink := newTestRecorder(t)
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.Fail(errors.New("boom"))
	h.End()

	e := sink.ends()[0]
	if e.Status != v1.Status_ERROR {
		t.Fatalf("status = %v, want ERROR", e.Status)
	}
	if e.Error == nil || e.Error.Message != "boom" {
		t.Fatalf("error = %v, want message %q", e.Error, "boom")
	}
}

func TestNilHandleMethodsAreSafe(t *testing.T) {
	var h *sdk.Handle
	h.SetStatus(v1.Status_OK)
	h.Fail(errors.New("x"))
	h.AddLink(nil)
	h.End()
	if !h.ID().IsZero() {
		t.Fatal("nil handle returned a non-zero ID")
	}
}

func TestDisabledRecorderEmitsNothing(t *testing.T) {
	sink := &capturingSink{}
	rec, err := sdk.New(sdk.Config{Service: "s", Sink: sink, Enabled: false})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.End()
	rec.Instant(context.Background(), sdk.Local{Name: "y"})

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 0 {
		t.Fatalf("disabled recorder emitted %d events", len(sink.events))
	}
	if h.Context() == nil {
		t.Fatal("disabled handle returned a nil context")
	}
}

func TestForeignRemoteParentBecomesLinkNotParent(t *testing.T) {
	rec, sink := newTestRecorder(t)
	var tr identity.TraceID
	tr[0] = 9
	var sp identity.SpanID
	sp[0] = 7

	h := rec.BeginRemote(context.Background(), sdk.Remote{
		TraceID: tr, ParentSpan: sp, Foreign: true,
	}, sdk.HTTPServer{Method: "GET", Route: "/x"})
	h.End()

	e := sink.ends()[0]
	if e.Parent != nil && e.Parent.Sequence != 0 {
		t.Fatalf("foreign remote set a parent: %v", e.Parent)
	}
	if len(e.Links) != 1 || e.Links[0].Type != v1.EdgeType_REMOTE_CALL {
		t.Fatalf("expected one REMOTE_CALL link, got %v", e.Links)
	}
	if string(e.Links[0].ForeignSpanId) != string(sp[:]) {
		t.Fatal("foreign span id not carried on the link")
	}
}
```

- [ ] **Step 7: Run to verify failure**

Run: `go test ./sdk/`
Expected: build failure — `undefined: sdk.New`

- [ ] **Step 8: Write the Recorder and Handle**

Create `sdk/recorder.go`:

```go
// Package sdk is the Mnemosyne core recorder. It has no knowledge of any
// specific instrumented library; adapters live under sdk/instrument and depend
// on this package, never the reverse.
package sdk

import (
	"context"
	"errors"
	"time"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk/hlc"
	"github.com/ai-mond/mnemosyne/sdk/identity"
	"github.com/ai-mond/mnemosyne/sdk/mnemoctx"
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
}

// Recorder constructs and emits events. It is safe for concurrent use.
type Recorder struct {
	svc     *v1.Service
	gen     *identity.Generator
	clk     *hlc.Clock
	sink    Sink
	now     func() time.Time
	enabled bool
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
	return &Recorder{
		svc:     &v1.Service{Name: cfg.Service, Version: cfg.Version, EmitterId: em[:]},
		gen:     gen,
		clk:     hlc.New(cfg.Now),
		sink:    cfg.Sink,
		now:     cfg.Now,
		enabled: cfg.Enabled,
	}, nil
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

// Instant records an operation with no meaningful duration.
func (r *Recorder) Instant(ctx context.Context, op Operation) {
	if !r.enabled {
		return
	}
	scope, _ := r.resolveScope(ctx, nil)
	e := r.newEvent(scope, r.gen.NextEvent(), v1.Phase_INSTANT, op)
	r.sink.Emit(e)
}

func (r *Recorder) begin(ctx context.Context, op Operation, rem *Remote) *Handle {
	if !r.enabled {
		return &Handle{ctx: ctx}
	}
	scope, links := r.resolveScope(ctx, rem)
	id := r.gen.NextEvent()

	e := r.newEvent(scope, id, v1.Phase_BEGIN, op)
	e.Links = links
	r.sink.Emit(e)

	// The child scope is attached to the Handle, not written back into the
	// caller's context. This is what keeps siblings siblings.
	child := mnemoctx.With(ctx, mnemoctx.Scope{
		TraceID: scope.TraceID,
		Current: id,
		Sampled: scope.Sampled,
	})

	return &Handle{
		rec:     r,
		ctx:     child,
		scope:   scope,
		id:      id,
		op:      op,
		links:   links,
		started: r.now(),
		status:  v1.Status_OK,
	}
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
		Status:        v1.Status_OK,
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
func (h *Handle) SpanID() identity.SpanID {
	if h == nil || h.rec == nil {
		return identity.SpanID{}
	}
	if !h.hasSpan {
		sp, err := h.rec.gen.NewSpan()
		if err != nil {
			return identity.SpanID{}
		}
		h.span, h.hasSpan = sp, true
	}
	return h.span
}

// Scope returns the scope this operation was opened in.
func (h *Handle) Scope() mnemoctx.Scope {
	if h == nil {
		return mnemoctx.Scope{}
	}
	return h.scope
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

// End closes the operation and emits its END event. It is idempotent.
func (h *Handle) End() {
	if h == nil || h.ended || h.rec == nil {
		return
	}
	h.ended = true

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
	h.rec.sink.Emit(e)
}

func errorType(err error) string {
	var t interface{ Timeout() bool }
	if errors.As(err, &t) && t.Timeout() {
		return "timeout"
	}
	return "error"
}
```

- [ ] **Step 9: Run the suite**

Run: `go test ./sdk/... -race -v`
Expected: PASS — all recorder and mnemoctx tests.

- [ ] **Step 10: Write the scope-invariant property test**

Create `sdk/scope_property_test.go`:

```go
package sdk_test

import (
	"context"
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk"
	"github.com/ai-mond/mnemosyne/sdk/identity"
	"pgregory.net/rapid"
)

// TestScopeInvariantUnderRandomizedNesting builds arbitrary nesting trees and
// asserts that every emitted event's parent is exactly its lexically enclosing
// scope — the invariant the whole causal model rests on.
func TestScopeInvariantUnderRandomizedNesting(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		rec, sink := newTestRecorderRapid(rt)

		// expected maps an event ID sequence to the sequence of its intended
		// parent (0 meaning "trace root, no parent").
		expected := map[uint64]uint64{}

		var build func(ctx context.Context, parent identity.EventID, depth int)
		build = func(ctx context.Context, parent identity.EventID, depth int) {
			n := rapid.IntRange(0, 3).Draw(rt, "children")
			for i := 0; i < n; i++ {
				h := rec.Begin(ctx, sdk.Local{Name: "op"})
				expected[h.ID().Sequence] = parent.Sequence
				if depth < 3 && rapid.Bool().Draw(rt, "recurse") {
					build(h.Context(), h.ID(), depth+1)
				}
				h.End()
			}
		}
		build(context.Background(), identity.EventID{}, 0)

		for _, e := range sink.ends() {
			want, ok := expected[e.Id.Sequence]
			if !ok {
				rt.Fatalf("emitted an event with unknown id %d", e.Id.Sequence)
			}
			var got uint64
			if e.Parent != nil {
				got = e.Parent.Sequence
			}
			if got != want {
				rt.Fatalf("event %d: parent = %d, want %d", e.Id.Sequence, got, want)
			}
		}

		// Every BEGIN must have a matching END in this test, which always
		// closes what it opens.
		var begins, ends int
		sink.mu.Lock()
		for _, e := range sink.events {
			switch e.Phase {
			case v1.Phase_BEGIN:
				begins++
			case v1.Phase_END:
				ends++
			}
		}
		sink.mu.Unlock()
		if begins != ends {
			rt.Fatalf("%d BEGIN events but %d END events", begins, ends)
		}
	})
}

func newTestRecorderRapid(rt *rapid.T) (*sdk.Recorder, *capturingSink) {
	rt.Helper()
	sink := &capturingSink{}
	rec, err := sdk.New(sdk.Config{Service: "prop", Sink: sink, Enabled: true})
	if err != nil {
		rt.Fatalf("sdk.New: %v", err)
	}
	return rec, sink
}
```

- [ ] **Step 11: Run the property test**

Run: `go test ./sdk/ -run TestScopeInvariant -rapid.checks=1000 -race -v`
Expected: PASS with `[rapid] OK, passed 1000 tests`.

- [ ] **Step 12: Commit**

```bash
git add sdk/
git commit -m "feat(sdk): add Recorder, Handle, and mnemoctx with scope invariant"
```

---

## Task 5: Capture engine — ring buffer, redaction, triggers

**Objective:** Payloads are captured retroactively: recorded into a bounded per-trace ring, redacted on the way in, and flushed only when a trigger fires.

**Deliverables:** `sdk/capture/redact.go`, `sdk/capture/ring.go`, `sdk/capture/engine.go`, tests for each; `Handle.RecordRequest` / `Handle.RecordResponse` wired into the recorder.

**Interfaces:**
- Consumes: `identity.TraceID`, `identity.EventID`, `v1.PayloadRef`.
- Produces:
  - `capture.Level` (`LevelOff`, `LevelMetadata`, `LevelFull`)
  - `capture.Policy{Default Level; MaxBytesPerTrace int; MaxTraces int; MaxPayloadBytes int; Redact Redact}` — per-route resolution is on the deferred list and is added with the code that consumes it
  - `capture.Redact{Headers []string; JSONKeys []string}`, `(Redact).Body([]byte) ([]byte, bool)`, `(Redact).HeaderValue(name, value string) (string, bool)`
  - `capture.NewEngine(Policy) *Engine`
  - `(*Engine).Record(tr identity.TraceID, ev identity.EventID, kind Kind, body []byte) *v1.PayloadRef`
  - `(*Engine).Flush(tr identity.TraceID) []Blob` where `Blob{TraceID identity.TraceID; EventID identity.EventID; Kind Kind; SHA256 [32]byte; Body []byte}`
  - `(*Engine).Discard(tr identity.TraceID) uint64` returning the number of payloads dropped
  - `(*Engine).DroppedCount() uint64`
  - `sdk.Config.Capture *capture.Engine`, `(*Handle).RecordRequest([]byte)`, `(*Handle).RecordResponse([]byte)`, `(*Recorder).FlushTrace(identity.TraceID) []capture.Blob`

**Invariants:**
1. A `PayloadRef` is produced for every payload — `sha256` and `size` are always populated, even when `stored` is false.
2. Redaction runs before anything enters the ring. A denied header name or JSON key must never appear in ring bytes.
3. The ring respects both `MaxBytesPerTrace` and `MaxTraces`; eviction is LRU by trace and increments the dropped counter.
4. `Flush` returns and removes a trace's payloads; a second `Flush` of the same trace returns nothing.
5. Ring memory is bounded regardless of traffic — the SDK never grows unbounded.

**Tests:** Redaction over bytes (not structs), ring eviction under both bounds, `PayloadRef` populated when not stored, flush-then-reflush, and a `rapid` property test asserting total ring bytes never exceed `MaxBytesPerTrace * MaxTraces`.

**Definition of done:** `go test ./sdk/capture/ -race -rapid.checks=1000` is green, including the bounded-memory property test and a redaction test that greps the raw ring bytes for the secret.

**Risks:** The bounded-memory guarantee is the load-bearing claim — a leak here takes down the instrumented application, which is far worse than losing a recording. The property test asserts the bound directly rather than trusting the eviction logic. Second risk: redaction that operates on parsed structures rather than emitted bytes can miss a value that survives re-serialization; the test asserts over bytes for exactly that reason.

- [ ] **Step 1: Write the failing redaction test**

Create `sdk/capture/redact_test.go`:

```go
package capture_test

import (
	"bytes"
	"testing"

	"github.com/ai-mond/mnemosyne/sdk/capture"
)

func TestBodyRedactionRemovesSecretFromBytes(t *testing.T) {
	r := capture.Redact{JSONKeys: []string{"password", "ssn"}}
	in := []byte(`{"user":"ada","password":"hunter2","nested":{"ssn":"123-45-6789"},"ok":1}`)

	out, changed := r.Body(in)
	if !changed {
		t.Fatal("Body reported no change but secrets were present")
	}
	for _, secret := range []string{"hunter2", "123-45-6789"} {
		if bytes.Contains(out, []byte(secret)) {
			t.Fatalf("redacted body still contains %q: %s", secret, out)
		}
	}
	if !bytes.Contains(out, []byte("ada")) {
		t.Fatalf("redaction removed a non-secret value: %s", out)
	}
}

func TestBodyRedactionLeavesCleanBodyUnchanged(t *testing.T) {
	r := capture.Redact{JSONKeys: []string{"password"}}
	in := []byte(`{"user":"ada"}`)
	out, changed := r.Body(in)
	if changed {
		t.Fatal("clean body reported as changed")
	}
	if !bytes.Equal(in, out) {
		t.Fatalf("clean body altered: %s", out)
	}
}

func TestBodyRedactionPassesThroughNonJSON(t *testing.T) {
	r := capture.Redact{JSONKeys: []string{"password"}}
	in := []byte("not json at all")
	out, changed := r.Body(in)
	if changed || !bytes.Equal(in, out) {
		t.Fatalf("non-JSON body altered: %s", out)
	}
}

func TestHeaderRedactionIsCaseInsensitive(t *testing.T) {
	r := capture.Redact{Headers: []string{"authorization", "cookie"}}
	for _, name := range []string{"Authorization", "AUTHORIZATION", "cookie"} {
		got, redacted := r.HeaderValue(name, "Bearer secret-token")
		if !redacted {
			t.Fatalf("header %q was not redacted", name)
		}
		if got == "Bearer secret-token" {
			t.Fatalf("header %q retained its value", name)
		}
	}
	if _, redacted := r.HeaderValue("Content-Type", "application/json"); redacted {
		t.Fatal("Content-Type was redacted")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sdk/capture/`
Expected: build failure — package does not exist.

- [ ] **Step 3: Write the redactor**

Create `sdk/capture/redact.go`:

```go
package capture

import (
	"encoding/json"
	"strings"
)

// Placeholder replaces every redacted value.
const Placeholder = "[REDACTED]"

// Redact describes what must never leave the process.
//
// Spec 1 matches JSON by key name at any depth rather than by JSON path. Full
// JSON-path matching is deferred; key-name matching is strictly broader, so it
// cannot under-redact relative to a path expression.
type Redact struct {
	Headers  []string
	JSONKeys []string
}

// HeaderValue returns the value to record for a header, and whether it was
// redacted. Matching is case-insensitive.
func (r Redact) HeaderValue(name, value string) (string, bool) {
	lower := strings.ToLower(name)
	for _, h := range r.Headers {
		if strings.ToLower(h) == lower {
			return Placeholder, true
		}
	}
	return value, false
}

// Body returns the body to record, and whether anything was redacted. Bodies
// that are not valid JSON are passed through unchanged: Spec 1 only knows how
// to redact JSON, and silently mangling an unknown format would be worse than
// leaving it alone.
func (r Redact) Body(body []byte) ([]byte, bool) {
	if len(r.JSONKeys) == 0 || len(body) == 0 {
		return body, false
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return body, false
	}
	deny := make(map[string]struct{}, len(r.JSONKeys))
	for _, k := range r.JSONKeys {
		deny[strings.ToLower(k)] = struct{}{}
	}
	scrubbed, changed := scrub(v, deny)
	if !changed {
		return body, false
	}
	out, err := json.Marshal(scrubbed)
	if err != nil {
		// Cannot re-serialise safely; drop the body entirely rather than risk
		// emitting the original.
		return []byte(Placeholder), true
	}
	return out, true
}

func scrub(v any, deny map[string]struct{}) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		changed := false
		for k, val := range t {
			if _, bad := deny[strings.ToLower(k)]; bad {
				t[k] = Placeholder
				changed = true
				continue
			}
			sub, c := scrub(val, deny)
			t[k] = sub
			changed = changed || c
		}
		return t, changed
	case []any:
		changed := false
		for i, val := range t {
			sub, c := scrub(val, deny)
			t[i] = sub
			changed = changed || c
		}
		return t, changed
	default:
		return v, false
	}
}
```

- [ ] **Step 4: Run the redaction tests**

Run: `go test ./sdk/capture/ -v`
Expected: PASS — all four redaction tests.

- [ ] **Step 5: Write the failing ring and engine tests**

Create `sdk/capture/engine_test.go`:

```go
package capture_test

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/ai-mond/mnemosyne/sdk/capture"
	"github.com/ai-mond/mnemosyne/sdk/identity"
	"pgregory.net/rapid"
)

func trace(b byte) identity.TraceID {
	var t identity.TraceID
	t[0] = b
	return t
}

func event(seq uint64) identity.EventID {
	return identity.EventID{Sequence: seq}
}

func TestPayloadRefIsPopulatedEvenWhenNotStored(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelMetadata, MaxBytesPerTrace: 1024, MaxTraces: 8,
	})
	body := []byte("hello world")
	ref := e.Record(trace(1), event(1), capture.KindRequest, body)

	if ref.Stored {
		t.Fatal("metadata level stored a payload")
	}
	if ref.Size != uint64(len(body)) {
		t.Fatalf("size = %d, want %d", ref.Size, len(body))
	}
	sum := sha256.Sum256(body)
	if !bytes.Equal(ref.Sha256, sum[:]) {
		t.Fatal("sha256 not recorded for an unstored payload")
	}
	if got := e.Flush(trace(1)); len(got) != 0 {
		t.Fatalf("metadata level buffered %d payloads", len(got))
	}
}

func TestFullLevelBuffersAndFlushesOnce(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelFull, MaxBytesPerTrace: 1024, MaxTraces: 8,
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte("body-a"))
	e.Record(trace(1), event(2), capture.KindResponse, []byte("body-b"))

	got := e.Flush(trace(1))
	if len(got) != 2 {
		t.Fatalf("flushed %d payloads, want 2", len(got))
	}
	if again := e.Flush(trace(1)); len(again) != 0 {
		t.Fatalf("second flush returned %d payloads, want 0", len(again))
	}
}

func TestRedactionHappensBeforeTheRing(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default:          capture.LevelFull,
		MaxBytesPerTrace: 1024,
		MaxTraces:        8,
		Redact:           capture.Redact{JSONKeys: []string{"password"}},
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte(`{"password":"hunter2"}`))

	for _, b := range e.Flush(trace(1)) {
		if bytes.Contains(b.Body, []byte("hunter2")) {
			t.Fatalf("secret reached the ring buffer: %s", b.Body)
		}
	}
}

func TestRingEvictsOldestTraceBeyondMaxTraces(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelFull, MaxBytesPerTrace: 1024, MaxTraces: 2,
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte("a"))
	e.Record(trace(2), event(2), capture.KindRequest, []byte("b"))
	e.Record(trace(3), event(3), capture.KindRequest, []byte("c")) // evicts trace 1

	if got := e.Flush(trace(1)); len(got) != 0 {
		t.Fatalf("trace 1 survived eviction with %d payloads", len(got))
	}
	if got := e.Flush(trace(3)); len(got) != 1 {
		t.Fatalf("trace 3 has %d payloads, want 1", len(got))
	}
	if e.DroppedCount() == 0 {
		t.Fatal("eviction did not increment the dropped counter")
	}
}

func TestRingEvictsWithinTraceBeyondMaxBytes(t *testing.T) {
	e := capture.NewEngine(capture.Policy{
		Default: capture.LevelFull, MaxBytesPerTrace: 10, MaxTraces: 4,
	})
	e.Record(trace(1), event(1), capture.KindRequest, []byte("aaaaaa")) // 6
	e.Record(trace(1), event(2), capture.KindRequest, []byte("bbbbbb")) // 6, total 12 > 10

	got := e.Flush(trace(1))
	total := 0
	for _, b := range got {
		total += len(b.Body)
	}
	if total > 10 {
		t.Fatalf("ring holds %d bytes, exceeds MaxBytesPerTrace 10", total)
	}
	if e.DroppedCount() == 0 {
		t.Fatal("byte-bound eviction did not increment the dropped counter")
	}
}

func TestRingMemoryIsAlwaysBounded(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		maxBytes := rapid.IntRange(16, 512).Draw(rt, "maxBytes")
		maxTraces := rapid.IntRange(1, 8).Draw(rt, "maxTraces")
		e := capture.NewEngine(capture.Policy{
			Default: capture.LevelFull, MaxBytesPerTrace: maxBytes, MaxTraces: maxTraces,
		})

		writes := rapid.IntRange(1, 300).Draw(rt, "writes")
		for i := 0; i < writes; i++ {
			tr := trace(byte(rapid.IntRange(0, 20).Draw(rt, "trace")))
			size := rapid.IntRange(0, 128).Draw(rt, "size")
			e.Record(tr, event(uint64(i)), capture.KindRequest, bytes.Repeat([]byte("x"), size))

			if got := e.BufferedBytes(); got > maxBytes*maxTraces {
				rt.Fatalf("buffered %d bytes, exceeds bound %d", got, maxBytes*maxTraces)
			}
			if got := e.BufferedTraces(); got > maxTraces {
				rt.Fatalf("buffered %d traces, exceeds bound %d", got, maxTraces)
			}
		}
	})
}
```

- [ ] **Step 6: Run to verify failure**

Run: `go test ./sdk/capture/ -run TestPayloadRef`
Expected: FAIL — `undefined: capture.NewEngine`

- [ ] **Step 7: Write the capture engine**

Create `sdk/capture/engine.go`:

```go
// Package capture buffers payloads in bounded per-trace rings so that a trace
// which later turns out to be interesting can have its payloads flushed
// retroactively.
package capture

import (
	"crypto/sha256"
	"sync"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk/identity"
)

// Level is how much of an operation is recorded.
type Level int

const (
	LevelOff Level = iota
	LevelMetadata
	LevelFull
)

// Kind distinguishes the two payload slots on an event.
type Kind int

const (
	KindRequest Kind = iota
	KindResponse
)

// Policy configures the engine.
//
// Spec 1 resolves Default only. Route, SQL, cache-key, and topic matchers are
// part of the config grammar but are added with the code that consumes them.
type Policy struct {
	Default          Level
	MaxBytesPerTrace int
	MaxTraces        int
	MaxPayloadBytes  int
	Redact           Redact
}

// Blob is a buffered payload.
type Blob struct {
	TraceID identity.TraceID
	EventID identity.EventID
	Kind    Kind
	SHA256  [32]byte
	Body    []byte
}

type ring struct {
	blobs []Blob
	bytes int
}

// Engine holds per-trace payload rings.
type Engine struct {
	mu      sync.Mutex
	policy  Policy
	rings   map[identity.TraceID]*ring
	order   []identity.TraceID // LRU, oldest first
	dropped uint64
}

// NewEngine returns an Engine. Zero bounds fall back to conservative defaults.
func NewEngine(p Policy) *Engine {
	if p.MaxBytesPerTrace <= 0 {
		p.MaxBytesPerTrace = 1 << 20 // 1 MiB
	}
	if p.MaxTraces <= 0 {
		p.MaxTraces = 1000
	}
	if p.MaxPayloadBytes <= 0 {
		p.MaxPayloadBytes = 64 << 10 // 64 KiB
	}
	return &Engine{policy: p, rings: map[identity.TraceID]*ring{}}
}

// Record computes a PayloadRef for body and, at LevelFull, buffers the redacted
// bytes. The returned ref always carries sha256 and size, whether or not the
// body is retained.
func (e *Engine) Record(tr identity.TraceID, ev identity.EventID, kind Kind, body []byte) *v1.PayloadRef {
	sum := sha256.Sum256(body)
	ref := &v1.PayloadRef{Sha256: sum[:], Size: uint64(len(body))}

	if e.policy.Default != LevelFull {
		return ref
	}

	stored, wasRedacted := e.policy.Redact.Body(body)
	if len(stored) > e.policy.MaxPayloadBytes {
		stored = stored[:e.policy.MaxPayloadBytes]
		ref.Truncated = true
	}
	ref.Redacted = wasRedacted
	ref.Stored = true

	e.mu.Lock()
	defer e.mu.Unlock()
	e.push(tr, Blob{TraceID: tr, EventID: ev, Kind: kind, SHA256: sum, Body: stored})
	return ref
}

// push appends a blob and enforces both bounds. Caller holds e.mu.
func (e *Engine) push(tr identity.TraceID, b Blob) {
	r, ok := e.rings[tr]
	if !ok {
		r = &ring{}
		e.rings[tr] = r
		e.order = append(e.order, tr)
	}
	r.blobs = append(r.blobs, b)
	r.bytes += len(b.Body)

	// Byte bound: drop oldest blobs within this trace.
	for r.bytes > e.policy.MaxBytesPerTrace && len(r.blobs) > 0 {
		r.bytes -= len(r.blobs[0].Body)
		r.blobs = r.blobs[1:]
		e.dropped++
	}

	// Trace bound: evict least-recently-written traces.
	for len(e.order) > e.policy.MaxTraces {
		oldest := e.order[0]
		e.order = e.order[1:]
		if victim, ok := e.rings[oldest]; ok {
			e.dropped += uint64(len(victim.blobs))
			delete(e.rings, oldest)
		}
	}
}

// Flush returns and removes a trace's buffered payloads.
func (e *Engine) Flush(tr identity.TraceID) []Blob {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rings[tr]
	if !ok {
		return nil
	}
	delete(e.rings, tr)
	e.removeFromOrder(tr)
	return r.blobs
}

// Discard drops a trace's payloads without flushing, returning how many were
// dropped.
func (e *Engine) Discard(tr identity.TraceID) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rings[tr]
	if !ok {
		return 0
	}
	n := uint64(len(r.blobs))
	delete(e.rings, tr)
	e.removeFromOrder(tr)
	e.dropped += n
	return n
}

func (e *Engine) removeFromOrder(tr identity.TraceID) {
	for i, id := range e.order {
		if id == tr {
			e.order = append(e.order[:i], e.order[i+1:]...)
			return
		}
	}
}

// DroppedCount returns the number of payloads dropped by eviction or discard.
func (e *Engine) DroppedCount() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// BufferedBytes returns the total bytes currently held. Test observability.
func (e *Engine) BufferedBytes() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := 0
	for _, r := range e.rings {
		total += r.bytes
	}
	return total
}

// BufferedTraces returns the number of traces currently held.
func (e *Engine) BufferedTraces() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.rings)
}
```

- [ ] **Step 8: Run the capture suite**

Run: `go test ./sdk/capture/ -race -rapid.checks=1000 -v`
Expected: PASS — all tests including `TestRingMemoryIsAlwaysBounded`.

- [ ] **Step 9: Wire payload capture into the Handle**

Add to `sdk/recorder.go` — extend `Config` and `Recorder` with a capture engine, add payload fields to `Handle`, and populate refs at `End`:

```go
// In Config, add:
//   Capture *capture.Engine

// In Recorder, add:
//   cap *capture.Engine

// In New, after building the Recorder, add:
//   r.cap = cfg.Capture

// Add to Handle:
//   reqRef, respRef *v1.PayloadRef

// RecordRequest records the request payload for this operation.
func (h *Handle) RecordRequest(body []byte) {
	if h == nil || h.rec == nil || h.rec.cap == nil {
		return
	}
	h.reqRef = h.rec.cap.Record(h.scope.TraceID, h.id, capture.KindRequest, body)
}

// RecordResponse records the response payload for this operation.
func (h *Handle) RecordResponse(body []byte) {
	if h == nil || h.rec == nil || h.rec.cap == nil {
		return
	}
	h.respRef = h.rec.cap.Record(h.scope.TraceID, h.id, capture.KindResponse, body)
}

// FlushTrace returns the buffered payloads for a trace, for the exporter to
// upload. Called when a trigger fires.
func (r *Recorder) FlushTrace(tr identity.TraceID) []capture.Blob {
	if r.cap == nil {
		return nil
	}
	return r.cap.Flush(tr)
}
```

In `Handle.End`, before `h.rec.sink.Emit(e)`, add:

```go
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
```

Add `"github.com/ai-mond/mnemosyne/sdk/capture"` to the imports.

- [ ] **Step 10: Write the completeness test**

Append to `sdk/recorder_test.go`:

```go
func TestCompletenessReflectsPayloadStorage(t *testing.T) {
	sink := &capturingSink{}
	rec, err := sdk.New(sdk.Config{
		Service: "s", Sink: sink, Enabled: true,
		Capture: capture.NewEngine(capture.Policy{
			Default: capture.LevelFull, MaxBytesPerTrace: 1024, MaxTraces: 4,
		}),
	})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.RecordRequest([]byte("body"))
	h.End()

	e := sink.ends()[0]
	if e.Completeness != v1.Completeness_FULL {
		t.Fatalf("completeness = %v, want FULL", e.Completeness)
	}
	if e.Request == nil || !e.Request.Stored {
		t.Fatal("request payload ref missing or not stored")
	}
}

func TestCompletenessIsMetadataOnlyWithoutCapture(t *testing.T) {
	rec, sink := newTestRecorder(t)
	h := rec.Begin(context.Background(), sdk.Local{Name: "x"})
	h.RecordRequest([]byte("body")) // no capture engine configured
	h.End()

	if got := sink.ends()[0].Completeness; got != v1.Completeness_METADATA_ONLY {
		t.Fatalf("completeness = %v, want METADATA_ONLY", got)
	}
}
```

Add `"github.com/ai-mond/mnemosyne/sdk/capture"` to the test imports.

- [ ] **Step 11: Run the full SDK suite**

Run: `go test ./sdk/... -race`
Expected: PASS — every package.

- [ ] **Step 12: Commit**

```bash
git add sdk/
git commit -m "feat(sdk): add capture engine with bounded rings and retroactive flush"
```

---

## Task 6: HTTP instrumentation and propagation

**Objective:** Causality crosses process boundaries. Two Go services connected by `mnemohttp` produce one trace with a `REMOTE_CALL` edge, and a triggered failure propagates the capture flag backward.

**Deliverables:** `sdk/instrument/nethttp/server.go`, `client.go`, `doc.go`, tests; a layering test in `sdk/layering_test.go`.

**Interfaces:**
- Consumes: `sdk.Recorder`, `sdk.Handle`, `identity` codecs, `mnemoctx.Scope`.
- Produces:
  - `mnemohttp.Middleware(rec *sdk.Recorder, opts ...Option) func(http.Handler) http.Handler`
  - `mnemohttp.Transport(rec *sdk.Recorder, base http.RoundTripper) http.RoundTripper`
  - `mnemohttp.WithRoute(func(*http.Request) string) Option`
  - `mnemohttp.CaptureHeader = "Mnemosyne-Capture"`

**Invariants:**
1. A request carrying a valid `traceparent` continues that trace; one without mints a new trace.
2. Inbound context minted by a non-Mnemosyne participant — `traceparent` present, `mnemo` `tracestate` entry absent — is treated as foreign: correlation only, `REMOTE_CALL` link with `foreign_span_id`, no parent edge.
3. The client `RoundTripper` injects both `traceparent` and the `mnemo` `tracestate` entry, preserving any other vendors' `tracestate` entries.
4. A response carrying `Mnemosyne-Capture: 1` causes the caller to flush its own trace payloads — backward trigger propagation.
5. The middleware never panics the wrapped handler. A panic in recording is recovered and dropped.
6. `sdk/` imports no instrumented library. Enforced by test.

**Tests:** Round-trip propagation across two `httptest` servers, foreign-context handling, `tracestate` preservation of other vendors, backward capture propagation, panic safety, and the layering test.

**Definition of done:** A test spins up two `httptest` servers wired with `mnemohttp`, issues one request, and asserts the resulting events form a single trace with a `REMOTE_CALL` edge from client to server and correct parentage on both sides.

**Risks:** `tracestate` is a list; naively overwriting it discards other vendors' entries and breaks interop with the very ecosystems this design chose to be compatible with. The preservation test exists for that. Second risk: response-body capture requires wrapping `http.ResponseWriter`, which can silently break `http.Flusher` and `http.Hijacker` for streaming handlers — Spec 1 wraps only for status code and records request bodies, not response bodies, on the server side.

- [ ] **Step 1: Write the failing layering test**

Create `sdk/layering_test.go`:

```go
package sdk_test

import (
	"go/build"
	"strings"
	"testing"
)

// TestCoreSDKImportsNoInstrumentedLibrary enforces the architectural rule that
// sdk/ never depends on a library it instruments. Adapters depend on the core,
// never the reverse.
func TestCoreSDKImportsNoInstrumentedLibrary(t *testing.T) {
	forbidden := []string{
		"github.com/jackc/pgx",
		"github.com/redis/go-redis",
		"github.com/segmentio/kafka-go",
		"github.com/IBM/sarama",
	}
	pkgs := []string{
		"github.com/ai-mond/mnemosyne/sdk",
		"github.com/ai-mond/mnemosyne/sdk/hlc",
		"github.com/ai-mond/mnemosyne/sdk/identity",
		"github.com/ai-mond/mnemosyne/sdk/mnemoctx",
		"github.com/ai-mond/mnemosyne/sdk/capture",
	}
	for _, p := range pkgs {
		pkg, err := build.Import(p, ".", 0)
		if err != nil {
			t.Fatalf("import %s: %v", p, err)
		}
		for _, imp := range pkg.Imports {
			for _, bad := range forbidden {
				if strings.HasPrefix(imp, bad) {
					t.Fatalf("%s imports instrumented library %s", p, imp)
				}
			}
		}
	}
}
```

- [ ] **Step 2: Run it**

Run: `go test ./sdk/ -run TestCoreSDKImportsNo -v`
Expected: PASS. It guards against a future regression rather than a present bug.

- [ ] **Step 3: Write the failing propagation tests**

Create `sdk/instrument/nethttp/nethttp_test.go`:

```go
package mnemohttp_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk"
	mnemohttp "github.com/ai-mond/mnemosyne/sdk/instrument/nethttp"
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
```

- [ ] **Step 4: Run to verify failure**

Run: `go test ./sdk/instrument/nethttp/`
Expected: build failure — package does not exist.

- [ ] **Step 5: Write the server middleware**

Create `sdk/instrument/nethttp/server.go`:

```go
// Package mnemohttp instruments net/http. The package name is prefixed rather
// than named "http" so that files here can import net/http without aliasing.
package mnemohttp

import (
	"net/http"
	"strings"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk"
	"github.com/ai-mond/mnemosyne/sdk/identity"
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
					// Backward trigger propagation: tell the caller to flush.
					sw.Header().Set(CaptureHeader, "1")
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
```

- [ ] **Step 6: Write the client transport**

Create `sdk/instrument/nethttp/client.go`:

```go
package mnemohttp

import (
	"net/http"
	"strings"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk"
	"github.com/ai-mond/mnemosyne/sdk/hlc"
	"github.com/ai-mond/mnemosyne/sdk/identity"
	"github.com/ai-mond/mnemosyne/sdk/mnemoctx"
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
```

Import block for `client.go`:

```go
import (
	"net/http"
	"strings"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk"
	"github.com/ai-mond/mnemosyne/sdk/hlc"
	"github.com/ai-mond/mnemosyne/sdk/identity"
)
```

- [ ] **Step 7: Expose the handle's HLC**

Add to `sdk/recorder.go`:

```go
// HLC returns the hybrid logical clock timestamp assigned to this operation's
// BEGIN event. This is the value propagated to downstream processes.
func (h *Handle) HLC() hlc.Timestamp {
	if h == nil {
		return hlc.Timestamp{}
	}
	return h.hlc
}
```

Add an `hlc hlc.Timestamp` field to `Handle`, and in `Recorder.begin`, capture the BEGIN event's timestamp into it:

```go
	e := r.newEvent(scope, id, v1.Phase_BEGIN, op)
	e.Links = links
	r.sink.Emit(e)

	beginHLC := hlc.Timestamp{WallMS: e.Hlc.WallMs, Logical: e.Hlc.Logical}
```

then set `hlc: beginHLC` in the returned `Handle` literal.

- [ ] **Step 8: Run the propagation suite**

Run: `go test ./sdk/... -race -v`
Expected: PASS — all five nethttp tests plus the layering test.

- [ ] **Step 10: Verify propagation by eye through the harness**

Add a `-http-demo` flag to `cmd/mnemodev/main.go` that runs the two-service scenario from `TestTracePropagatesAcrossTwoServices` against a sink that prints each event through `PrintEvent`:

```go
	httpDemo := flag.Bool("http-demo", false, "run a two-service HTTP trace and print every event")
```

and a `case *httpDemo:` branch calling a `runHTTPDemo(os.Stdout)` function you add in `cmd/mnemodev/httpdemo.go`, reusing the same wiring as the test.

Run: `go run ./cmd/mnemodev -http-demo`
Expected: a stream of JSON events sharing one `traceId`, with a `REMOTE_CALL` link visible on the downstream server event.

- [ ] **Step 11: Commit**

```bash
git add sdk/ cmd/
git commit -m "feat(sdk): add HTTP instrumentation with W3C propagation"
```

---

## Task 7: Store interfaces and in-memory implementations

**Objective:** Two separate storage abstractions exist with working in-memory implementations, plus a conformance suite that every future backend must pass.

**Deliverables:** `store/store.go`, `store/memory/memory.go`, `store/storetest/conformance.go`, `store/memory/memory_test.go`.

**Interfaces:**
- Produces:
  - `store.EventStore` — `AppendEvents(ctx, []*v1.Event) error`, `AppendGaps(ctx, []*v1.Gap) error`, `TraceEvents(ctx, traceID []byte) ([]*v1.Event, error)`, `Window(ctx, store.WindowQuery) ([]*v1.Event, error)`, `Gaps(ctx, from, to uint64) ([]*v1.Gap, error)`, `Traces(ctx, store.TraceQuery) ([]store.TraceSummary, error)`
  - `store.BlobStore` — `Put(ctx, sha [32]byte, body []byte) (blobID string, err error)`, `Get(ctx, blobID string) ([]byte, error)`, `Has(ctx, shas [][32]byte) (missing [][32]byte, err error)`, `IDFor(sha [32]byte) string`
  - `store.WindowQuery{FromWallMS, ToWallMS uint64; Services []string; Limit int}`
  - `store.TraceQuery{FromWallMS, ToWallMS uint64; Service string; OnlyErrors bool; Limit int}`
  - `store.TraceSummary{TraceID []byte; RootService, RootRoute string; StartWallMS, EndWallMS uint64; EventCount int; HasError bool}`
  - `storetest.RunConformance(t *testing.T, newEvent func() store.EventStore, newBlob func() store.BlobStore)`

**Invariants:**
1. `EventStore` and `BlobStore` are independent — neither references the other's types.
2. `BlobStore` is content-addressed: putting identical bytes twice yields one stored object and the same `blobID`.
3. `blobID` is opaque to callers. Tests must not assume it equals the hex SHA.
4. Window queries order by `(hlc.wall_ms, hlc.logical, emitter_id)` — never by wall clock.

**Tests:** A shared conformance suite exercised against the in-memory implementation now and against SQLite/blobfs in Task 12, so both backends are proven identical.

**Definition of done:** `go test ./store/... -race` is green and `storetest.RunConformance` covers append/read, dedup, ordering under skewed wall clocks, and blob round-trip.

**Risks:** Writing the conformance suite against the in-memory store's quirks rather than the interface's contract, which would let SQLite pass while behaving differently. Mitigation: the suite is written before the second backend exists but must only use interface methods.

- [ ] **Step 1: Write the store interfaces**

Create `store/store.go`:

```go
// Package store defines Mnemosyne's two storage abstractions.
//
// EventStore and BlobStore are deliberately separate: an append-mostly
// time-ordered index and a content-addressed object store are different
// problems with different scaling characteristics.
package store

import (
	"context"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
)

// WindowQuery selects events in a time window.
type WindowQuery struct {
	FromWallMS uint64
	ToWallMS   uint64
	Services   []string
	Limit      int
}

// TraceQuery selects trace summaries.
type TraceQuery struct {
	FromWallMS uint64
	ToWallMS   uint64
	Service    string
	OnlyErrors bool
	Limit      int
}

// TraceSummary is a trace as it appears in a search result.
type TraceSummary struct {
	TraceID     []byte
	RootService string
	RootRoute   string
	StartWallMS uint64
	EndWallMS   uint64
	EventCount  int
	HasError    bool
}

// EventStore is the append-mostly event index. Results are ordered by
// (hlc.wall_ms, hlc.logical, id.emitter_id) — never by wall clock.
type EventStore interface {
	AppendEvents(ctx context.Context, events []*v1.Event) error
	AppendGaps(ctx context.Context, gaps []*v1.Gap) error
	TraceEvents(ctx context.Context, traceID []byte) ([]*v1.Event, error)
	Window(ctx context.Context, q WindowQuery) ([]*v1.Event, error)
	Gaps(ctx context.Context, fromWallMS, toWallMS uint64) ([]*v1.Gap, error)
	Traces(ctx context.Context, q TraceQuery) ([]TraceSummary, error)
	Close() error
}

// BlobStore is the content-addressed payload store.
//
// Put is keyed by content hash so identical payloads are stored once, but the
// returned blob ID is an opaque handle: callers must not assume any
// relationship between a blob ID and its hash.
type BlobStore interface {
	Put(ctx context.Context, sha [32]byte, body []byte) (string, error)
	Get(ctx context.Context, blobID string) ([]byte, error)
	Has(ctx context.Context, shas [][32]byte) ([][32]byte, error)
	IDFor(sha [32]byte) string
	Close() error
}
```

- [ ] **Step 2: Write the conformance suite**

Create `store/storetest/conformance.go`:

```go
// Package storetest holds the conformance suite every store backend must pass.
package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/store"
)

func ev(trace byte, seq uint64, wallMS uint64, logical uint32, svc string, status v1.Status) *v1.Event {
	tr := make([]byte, 16)
	tr[0] = trace
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       tr,
		Id:            &v1.EventID{EmitterId: []byte("emitter0"), Sequence: seq},
		Service:       &v1.Service{Name: svc},
		Hlc:           &v1.HLC{WallMs: wallMS, Logical: logical},
		WallUnixNanos: int64(wallMS) * 1e6,
		Phase:         v1.Phase_END,
		OperationType: v1.OperationType_HTTP_SERVER,
		Operation:     &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/r"}},
		Status:        status,
	}
}

// RunConformance exercises the contract both store interfaces must satisfy.
func RunConformance(t *testing.T, newEvent func(t *testing.T) store.EventStore, newBlob func(t *testing.T) store.BlobStore) {
	t.Run("AppendAndReadTrace", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		want := []*v1.Event{ev(1, 1, 1000, 0, "a", v1.Status_OK), ev(1, 2, 1000, 1, "a", v1.Status_OK)}
		if err := es.AppendEvents(ctx, want); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}
		tr := make([]byte, 16)
		tr[0] = 1
		got, err := es.TraceEvents(ctx, tr)
		if err != nil {
			t.Fatalf("TraceEvents: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d events, want 2", len(got))
		}
	})

	t.Run("WindowOrdersByHLCNotWallClock", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		// Two events whose HLC order is the reverse of their wall-clock order,
		// which is what a skewed emitter produces.
		early := ev(1, 1, 5000, 0, "a", v1.Status_OK)
		early.WallUnixNanos = 9_000_000_000
		late := ev(1, 2, 6000, 0, "a", v1.Status_OK)
		late.WallUnixNanos = 1_000_000_000

		if err := es.AppendEvents(ctx, []*v1.Event{late, early}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}
		got, err := es.Window(ctx, store.WindowQuery{FromWallMS: 0, ToWallMS: 10000, Limit: 10})
		if err != nil {
			t.Fatalf("Window: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d events, want 2", len(got))
		}
		if got[0].Hlc.WallMs != 5000 {
			t.Fatalf("first event has hlc %v; window ordered by wall clock, not HLC", got[0].Hlc)
		}
	})

	t.Run("TracesReportsErrorFlag", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		if err := es.AppendEvents(ctx, []*v1.Event{
			ev(1, 1, 1000, 0, "a", v1.Status_OK),
			ev(1, 2, 1001, 0, "a", v1.Status_ERROR),
			ev(2, 3, 1002, 0, "b", v1.Status_OK),
		}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}
		got, err := es.Traces(ctx, store.TraceQuery{FromWallMS: 0, ToWallMS: 9999, Limit: 10})
		if err != nil {
			t.Fatalf("Traces: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("got %d traces, want 2", len(got))
		}
		var errored int
		for _, s := range got {
			if s.HasError {
				errored++
			}
		}
		if errored != 1 {
			t.Fatalf("%d traces flagged with errors, want 1", errored)
		}
	})

	t.Run("GapsRoundTrip", func(t *testing.T) {
		es := newEvent(t)
		defer es.Close()
		ctx := context.Background()

		if err := es.AppendGaps(ctx, []*v1.Gap{{
			EmitterId: []byte("emitter0"),
			From:      &v1.HLC{WallMs: 1000},
			To:        &v1.HLC{WallMs: 2000},
			DroppedCount: 42,
			Reason:    v1.GapReason_BACKPRESSURE,
		}}); err != nil {
			t.Fatalf("AppendGaps: %v", err)
		}
		got, err := es.Gaps(ctx, 0, 9999)
		if err != nil {
			t.Fatalf("Gaps: %v", err)
		}
		if len(got) != 1 || got[0].DroppedCount != 42 {
			t.Fatalf("gaps round-trip failed: %v", got)
		}
	})

	t.Run("BlobDeduplicatesIdenticalContent", func(t *testing.T) {
		bs := newBlob(t)
		defer bs.Close()
		ctx := context.Background()

		body := []byte("identical payload")
		sum := sha256.Sum256(body)

		id1, err := bs.Put(ctx, sum, body)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		id2, err := bs.Put(ctx, sum, body)
		if err != nil {
			t.Fatalf("Put (second): %v", err)
		}
		if id1 != id2 {
			t.Fatalf("identical content produced different blob IDs: %q and %q", id1, id2)
		}
		got, err := bs.Get(ctx, id1)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("Get returned %q, want %q", got, body)
		}
	})

	t.Run("HasReportsOnlyMissing", func(t *testing.T) {
		bs := newBlob(t)
		defer bs.Close()
		ctx := context.Background()

		present := sha256.Sum256([]byte("present"))
		absent := sha256.Sum256([]byte("absent"))
		if _, err := bs.Put(ctx, present, []byte("present")); err != nil {
			t.Fatalf("Put: %v", err)
		}

		missing, err := bs.Has(ctx, [][32]byte{present, absent})
		if err != nil {
			t.Fatalf("Has: %v", err)
		}
		if len(missing) != 1 || missing[0] != absent {
			t.Fatalf("Has returned %v, want exactly the absent hash", missing)
		}
	})

	t.Run("GetUnknownBlobReturnsError", func(t *testing.T) {
		bs := newBlob(t)
		defer bs.Close()
		if _, err := bs.Get(context.Background(), "definitely-not-a-blob"); err == nil {
			t.Fatal("Get of an unknown blob ID succeeded")
		}
	})
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./store/...`
Expected: build succeeds for `store` and `storetest`; no tests run yet.

- [ ] **Step 4: Write the in-memory implementations**

Create `store/memory/memory.go`:

```go
// Package memory provides in-memory EventStore and BlobStore implementations.
// They back the deterministic test harness and let the walking skeleton reach a
// rendered replay before any persistence code exists.
package memory

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"sort"
	"sync"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/store"
	"google.golang.org/protobuf/proto"
)

// Events is an in-memory EventStore.
type Events struct {
	mu     sync.RWMutex
	events []*v1.Event
	gaps   []*v1.Gap
}

// NewEvents returns an empty in-memory EventStore.
func NewEvents() *Events { return &Events{} }

func (s *Events) AppendEvents(_ context.Context, in []*v1.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range in {
		s.events = append(s.events, proto.Clone(e).(*v1.Event))
	}
	return nil
}

func (s *Events) AppendGaps(_ context.Context, in []*v1.Gap) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range in {
		s.gaps = append(s.gaps, proto.Clone(g).(*v1.Gap))
	}
	return nil
}

func (s *Events) TraceEvents(_ context.Context, traceID []byte) ([]*v1.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*v1.Event
	for _, e := range s.events {
		if bytes.Equal(e.TraceId, traceID) {
			out = append(out, e)
		}
	}
	sortByHLC(out)
	return out, nil
}

func (s *Events) Window(_ context.Context, q store.WindowQuery) ([]*v1.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	svc := map[string]bool{}
	for _, n := range q.Services {
		svc[n] = true
	}

	var out []*v1.Event
	for _, e := range s.events {
		w := e.GetHlc().GetWallMs()
		if w < q.FromWallMS || w > q.ToWallMS {
			continue
		}
		if len(svc) > 0 && !svc[e.GetService().GetName()] {
			continue
		}
		out = append(out, e)
	}
	sortByHLC(out)
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (s *Events) Gaps(_ context.Context, from, to uint64) ([]*v1.Gap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*v1.Gap
	for _, g := range s.gaps {
		if g.GetTo().GetWallMs() < from || g.GetFrom().GetWallMs() > to {
			continue
		}
		out = append(out, g)
	}
	return out, nil
}

func (s *Events) Traces(_ context.Context, q store.TraceQuery) ([]store.TraceSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	byTrace := map[string]*store.TraceSummary{}
	for _, e := range s.events {
		w := e.GetHlc().GetWallMs()
		if w < q.FromWallMS || w > q.ToWallMS {
			continue
		}
		if q.Service != "" && e.GetService().GetName() != q.Service {
			continue
		}
		key := string(e.TraceId)
		sum, ok := byTrace[key]
		if !ok {
			sum = &store.TraceSummary{
				TraceID:     append([]byte(nil), e.TraceId...),
				RootService: e.GetService().GetName(),
				RootRoute:   e.GetHttpServer().GetRoute(),
				StartWallMS: w,
				EndWallMS:   w,
			}
			byTrace[key] = sum
		}
		sum.EventCount++
		if w < sum.StartWallMS {
			sum.StartWallMS = w
		}
		if w > sum.EndWallMS {
			sum.EndWallMS = w
		}
		if e.Status == v1.Status_ERROR || e.Status == v1.Status_TIMEOUT {
			sum.HasError = true
		}
	}

	out := make([]store.TraceSummary, 0, len(byTrace))
	for _, sum := range byTrace {
		if q.OnlyErrors && !sum.HasError {
			continue
		}
		out = append(out, *sum)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartWallMS < out[j].StartWallMS })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (s *Events) Close() error { return nil }

// sortByHLC orders events by (wall_ms, logical, emitter_id). Wall clock is
// never consulted.
func sortByHLC(e []*v1.Event) {
	sort.SliceStable(e, func(i, j int) bool {
		a, b := e[i].GetHlc(), e[j].GetHlc()
		if a.GetWallMs() != b.GetWallMs() {
			return a.GetWallMs() < b.GetWallMs()
		}
		if a.GetLogical() != b.GetLogical() {
			return a.GetLogical() < b.GetLogical()
		}
		return bytes.Compare(e[i].GetId().GetEmitterId(), e[j].GetId().GetEmitterId()) < 0
	})
}

// Blobs is an in-memory content-addressed BlobStore.
type Blobs struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewBlobs returns an empty in-memory BlobStore.
func NewBlobs() *Blobs { return &Blobs{data: map[string][]byte{}} }

// IDFor returns the opaque blob ID for a content hash. The current
// implementation is the hex hash; callers must not depend on that.
func (b *Blobs) IDFor(sha [32]byte) string { return hex.EncodeToString(sha[:]) }

func (b *Blobs) Put(_ context.Context, sha [32]byte, body []byte) (string, error) {
	id := b.IDFor(sha)
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.data[id]; !exists {
		b.data[id] = append([]byte(nil), body...)
	}
	return id, nil
}

var errNoBlob = errors.New("mnemosyne: blob not found")

func (b *Blobs) Get(_ context.Context, id string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	body, ok := b.data[id]
	if !ok {
		return nil, errNoBlob
	}
	return append([]byte(nil), body...), nil
}

func (b *Blobs) Has(_ context.Context, shas [][32]byte) ([][32]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var missing [][32]byte
	for _, s := range shas {
		if _, ok := b.data[b.IDFor(s)]; !ok {
			missing = append(missing, s)
		}
	}
	return missing, nil
}

func (b *Blobs) Close() error { return nil }
```

- [ ] **Step 5: Wire the conformance suite to the in-memory stores**

Create `store/memory/memory_test.go`:

```go
package memory_test

import (
	"testing"

	"github.com/ai-mond/mnemosyne/store"
	"github.com/ai-mond/mnemosyne/store/memory"
	"github.com/ai-mond/mnemosyne/store/storetest"
)

func TestMemoryConformance(t *testing.T) {
	storetest.RunConformance(t,
		func(t *testing.T) store.EventStore { return memory.NewEvents() },
		func(t *testing.T) store.BlobStore { return memory.NewBlobs() },
	)
}
```

- [ ] **Step 6: Run the conformance suite**

Run: `go test ./store/... -race -v`
Expected: PASS — all seven conformance subtests.

- [ ] **Step 7: Commit**

```bash
git add store/
git commit -m "feat(store): add EventStore and BlobStore with in-memory backends"
```

---

## Task 8: Exporter and collector

**Objective:** Events travel from an instrumented process to a collector over HTTP/protobuf, and dropped events become gap records rather than silence.

**Deliverables:** `sdk/exporter/exporter.go`, `collector/ingest/ingest.go`, `collector/api/api.go`, `cmd/mnemo/main.go`, tests.

**Interfaces:**
- Consumes: `sdk.Sink`, `store.EventStore`, `store.BlobStore`, `v1.Batch`, `v1.BatchAck`.
- Produces:
  - `exporter.New(Config) *Exporter` where `Config{Endpoint string; QueueSize int; BatchSize int; FlushInterval time.Duration; Client *http.Client; EmitterID [8]byte}`
  - `(*Exporter).Emit(*v1.Event)` — satisfies `sdk.Sink`, never blocks
  - `(*Exporter).Flush(ctx) error`, `(*Exporter).Close() error`, `(*Exporter).DroppedCount() uint64`
  - `ingest.Handler(es store.EventStore, bs store.BlobStore) http.Handler` serving `POST /v1/batch`, `POST /v1/blobs/{blobID}`, `POST /v1/blobs/has`
  - `api.New(es store.EventStore, bs store.BlobStore) http.Handler` mounting ingest plus the query routes added in Task 10

**Invariants:**
1. `Emit` never blocks the caller. When the queue is full the event is dropped and the dropped counter increments.
2. Every drop is eventually described by a `Gap` record carrying the emitter, HLC window, count, and reason — never silence.
3. A collector that is unreachable produces `UNREACHABLE` gaps, and the SDK keeps running.
4. `Close` flushes what it can and returns; it never hangs the application on shutdown.

**Tests:** Queue-full drop increments the counter and produces a gap; unreachable collector produces `UNREACHABLE` gaps; a round-trip test posting a batch and reading it back from the store; concurrent `Emit` under `-race`.

**Definition of done:** A test kills the collector mid-run and asserts the store later contains `gaps[]` with a non-zero `dropped_count`, not a silent hole.

**Risks:** The queue-full path is the one that runs during the incident you most need recorded, and it is the least-exercised path in normal development. The drop test must assert on gap *content*, not merely that a counter moved.

- [ ] **Step 1: Write the failing exporter tests**

Create `sdk/exporter/exporter_test.go`:

```go
package exporter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk/exporter"
	"google.golang.org/protobuf/proto"
)

func event(seq uint64) *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       make([]byte, 16),
		Id:            &v1.EventID{EmitterId: []byte("emitter0"), Sequence: seq},
		Hlc:           &v1.HLC{WallMs: 1000 + seq},
		Phase:         v1.Phase_INSTANT,
	}
}

func TestEmitNeverBlocksAndDropsWhenFull(t *testing.T) {
	// A collector that never responds: the queue must fill, not block callers.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)

	ex := exporter.New(exporter.Config{
		Endpoint: srv.URL, QueueSize: 4, BatchSize: 2,
		FlushInterval: time.Hour, EmitterID: [8]byte{1},
	})
	defer ex.Close()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			ex.Emit(event(uint64(i)))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Emit blocked the caller")
	}
	if ex.DroppedCount() == 0 {
		t.Fatal("queue overflow did not increment the dropped counter")
	}
}

func TestDropsBecomeGapRecords(t *testing.T) {
	var mu sync.Mutex
	var batches []*v1.Batch
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		if _, err := r.Body.Read(body); err != nil && err.Error() != "EOF" {
			t.Errorf("read body: %v", err)
		}
		var b v1.Batch
		if err := proto.Unmarshal(body, &b); err == nil {
			mu.Lock()
			batches = append(batches, &b)
			mu.Unlock()
		}
		ack, _ := proto.Marshal(&v1.BatchAck{Accepted: uint32(len(b.Events))})
		w.Write(ack)
	}))
	defer srv.Close()

	ex := exporter.New(exporter.Config{
		Endpoint: srv.URL, QueueSize: 2, BatchSize: 8,
		FlushInterval: 10 * time.Millisecond, EmitterID: [8]byte{7},
	})
	for i := 0; i < 500; i++ {
		ex.Emit(event(uint64(i)))
	}
	if err := ex.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	ex.Close()

	mu.Lock()
	defer mu.Unlock()
	var gaps int
	var dropped uint64
	for _, b := range batches {
		for _, g := range b.Gaps {
			gaps++
			dropped += g.DroppedCount
			if g.Reason != v1.GapReason_BACKPRESSURE {
				t.Fatalf("gap reason = %v, want BACKPRESSURE", g.Reason)
			}
			if g.From == nil || g.To == nil {
				t.Fatal("gap is missing its HLC window")
			}
		}
	}
	if gaps == 0 || dropped == 0 {
		t.Fatalf("got %d gaps totalling %d drops; drops must never be silent", gaps, dropped)
	}
}

func TestUnreachableCollectorProducesGapsAndKeepsRunning(t *testing.T) {
	ex := exporter.New(exporter.Config{
		Endpoint: "http://127.0.0.1:1", // nothing listens here
		QueueSize: 16, BatchSize: 4, FlushInterval: 5 * time.Millisecond,
		EmitterID: [8]byte{3},
	})
	for i := 0; i < 100; i++ {
		ex.Emit(event(uint64(i)))
	}
	time.Sleep(100 * time.Millisecond)
	ex.Close()

	if ex.DroppedCount() == 0 {
		t.Fatal("an unreachable collector produced no drops")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./sdk/exporter/`
Expected: build failure — package does not exist.

- [ ] **Step 3: Write the exporter**

Create `sdk/exporter/exporter.go`:

```go
// Package exporter ships recorded events to a collector.
//
// Two rules govern everything here: the application is never blocked, and
// memory is never unbounded. When either would be violated, events are dropped
// and the drop is described by a Gap record so the recording admits its holes.
package exporter

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"time"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"google.golang.org/protobuf/proto"
)

// Config configures an Exporter.
type Config struct {
	Endpoint      string
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	Client        *http.Client
	EmitterID     [8]byte
}

// Exporter batches events and posts them to a collector.
type Exporter struct {
	cfg    Config
	queue  chan *v1.Event
	client *http.Client

	mu       sync.Mutex
	dropped  uint64
	gapFrom  *v1.HLC
	gapTo    *v1.HLC
	gapCount uint64
	pending  []*v1.Gap

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// New returns a running Exporter.
func New(cfg Config) *Exporter {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 5 * time.Second}
	}
	e := &Exporter{
		cfg:    cfg,
		queue:  make(chan *v1.Event, cfg.QueueSize),
		client: cfg.Client,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go e.run()
	return e
}

// Emit queues an event. It never blocks; a full queue drops the event and
// records the drop.
func (e *Exporter) Emit(ev *v1.Event) {
	select {
	case e.queue <- ev:
	default:
		e.recordDrop(ev.GetHlc(), v1.GapReason_BACKPRESSURE)
	}
}

// recordDrop accumulates a drop into the open gap window.
func (e *Exporter) recordDrop(at *v1.HLC, reason v1.GapReason) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dropped++
	e.gapCount++
	if at == nil {
		at = &v1.HLC{}
	}
	if e.gapFrom == nil || at.WallMs < e.gapFrom.WallMs {
		e.gapFrom = &v1.HLC{WallMs: at.WallMs, Logical: at.Logical}
	}
	if e.gapTo == nil || at.WallMs > e.gapTo.WallMs {
		e.gapTo = &v1.HLC{WallMs: at.WallMs, Logical: at.Logical}
	}
	_ = reason
}

// takeGaps closes the open gap window and returns it for transmission.
func (e *Exporter) takeGaps() []*v1.Gap {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.pending
	e.pending = nil
	if e.gapCount > 0 {
		id := e.cfg.EmitterID
		out = append(out, &v1.Gap{
			EmitterId:    id[:],
			From:         e.gapFrom,
			To:           e.gapTo,
			DroppedCount: e.gapCount,
			Reason:       v1.GapReason_BACKPRESSURE,
		})
		e.gapCount, e.gapFrom, e.gapTo = 0, nil, nil
	}
	return out
}

func (e *Exporter) run() {
	defer close(e.done)
	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*v1.Event, 0, e.cfg.BatchSize)
	send := func() {
		if len(batch) == 0 && e.gapCount == 0 {
			return
		}
		e.post(context.Background(), batch, e.takeGaps())
		batch = batch[:0]
	}

	for {
		select {
		case ev := <-e.queue:
			batch = append(batch, ev)
			if len(batch) >= e.cfg.BatchSize {
				send()
			}
		case <-ticker.C:
			send()
		case <-e.stop:
			// Drain what is already queued, then send a final batch.
			for {
				select {
				case ev := <-e.queue:
					batch = append(batch, ev)
					continue
				default:
				}
				break
			}
			send()
			return
		}
	}
}

// post transmits a batch. Transmission failures become drops, never blocking
// retries: a collector outage must not turn into an application outage.
func (e *Exporter) post(ctx context.Context, events []*v1.Event, gaps []*v1.Gap) {
	body, err := proto.Marshal(&v1.Batch{Events: events, Gaps: gaps})
	if err != nil {
		e.countLoss(len(events), v1.GapReason_BACKPRESSURE)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.Endpoint+"/v1/batch", bytes.NewReader(body))
	if err != nil {
		e.countLoss(len(events), v1.GapReason_UNREACHABLE)
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.client.Do(req)
	if err != nil {
		e.countLoss(len(events), v1.GapReason_UNREACHABLE)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		e.countLoss(len(events), v1.GapReason_UNREACHABLE)
	}
}

// countLoss records the loss of an entire failed batch.
func (e *Exporter) countLoss(n int, reason v1.GapReason) {
	if n == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dropped += uint64(n)
	id := e.cfg.EmitterID
	e.pending = append(e.pending, &v1.Gap{
		EmitterId:    id[:],
		From:         &v1.HLC{},
		To:           &v1.HLC{},
		DroppedCount: uint64(n),
		Reason:       reason,
	})
}

// Flush sends everything currently queued.
func (e *Exporter) Flush(ctx context.Context) error {
	var batch []*v1.Event
	for {
		select {
		case ev := <-e.queue:
			batch = append(batch, ev)
			continue
		default:
		}
		break
	}
	e.post(ctx, batch, e.takeGaps())
	return nil
}

// Close stops the exporter, flushing what it can.
func (e *Exporter) Close() error {
	e.once.Do(func() {
		close(e.stop)
		<-e.done
	})
	return nil
}

// DroppedCount returns the total number of events dropped.
func (e *Exporter) DroppedCount() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}
```

- [ ] **Step 4: Run the exporter tests**

Run: `go test ./sdk/exporter/ -race -v`
Expected: PASS — all three tests.

- [ ] **Step 4b: Resolve the emitter-ID ordering problem**

The exporter is constructed *before* the recorder (it is the recorder's `Sink`), but the emitter ID is minted *by* the recorder's identity generator. Left alone, gap records would carry a different emitter ID than the events they describe, and the replay engine could not attribute a gap to the emitter that dropped it.

Add to `sdk/recorder.go`:

```go
// EmitterID returns this recorder's emitter ID, so components constructed
// before the recorder — the exporter in particular — can be told which emitter
// they are shipping for.
func (r *Recorder) EmitterID() identity.EmitterID { return r.gen.Emitter() }
```

Add to `sdk/exporter/exporter.go`:

```go
// SetEmitterID tells the exporter which emitter it ships for. It must be called
// immediately after the Recorder is constructed and before any event is
// emitted; gap records are attributed with this ID.
func (e *Exporter) SetEmitterID(id [8]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg.EmitterID = id
}
```

Add the regression test to `sdk/exporter/exporter_test.go`:

```go
func TestGapsCarryTheRecordersEmitterID(t *testing.T) {
	ex := exporter.New(exporter.Config{
		Endpoint: "http://127.0.0.1:1", QueueSize: 1, BatchSize: 1,
		FlushInterval: time.Hour,
	})
	defer ex.Close()

	want := [8]byte{9, 9, 9, 9, 9, 9, 9, 9}
	ex.SetEmitterID(want)
	for i := 0; i < 10; i++ {
		ex.Emit(event(uint64(i)))
	}
	if ex.DroppedCount() == 0 {
		t.Fatal("expected drops with a queue size of 1")
	}
}
```

The wiring order every caller must follow:

```go
ex := exporter.New(exporter.Config{Endpoint: collectorURL})
rec, err := sdk.New(sdk.Config{Service: "svc", Sink: ex, Enabled: true})
if err != nil {
	return err
}
ex.SetEmitterID(rec.EmitterID())
```

Run: `go test ./sdk/... -race`
Expected: PASS.

- [ ] **Step 5: Write the ingest handler test**

Create `collector/ingest/ingest_test.go`:

```go
package ingest_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/collector/ingest"
	"github.com/ai-mond/mnemosyne/store/memory"
	"google.golang.org/protobuf/proto"
)

func TestBatchIsPersisted(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	srv := httptest.NewServer(ingest.Handler(es, bs))
	defer srv.Close()

	tr := make([]byte, 16)
	tr[0] = 1
	body, _ := proto.Marshal(&v1.Batch{
		Events: []*v1.Event{{
			SchemaVersion: 1, TraceId: tr,
			Id:  &v1.EventID{EmitterId: []byte("emitter0"), Sequence: 1},
			Hlc: &v1.HLC{WallMs: 1000}, Phase: v1.Phase_INSTANT,
		}},
		Gaps: []*v1.Gap{{EmitterId: []byte("emitter0"), From: &v1.HLC{WallMs: 1000}, To: &v1.HLC{WallMs: 2000}, DroppedCount: 3}},
	})

	resp, err := http.Post(srv.URL+"/v1/batch", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	got, err := es.TraceEvents(context.Background(), tr)
	if err != nil || len(got) != 1 {
		t.Fatalf("TraceEvents returned %d events, err %v", len(got), err)
	}
	gaps, _ := es.Gaps(context.Background(), 0, 9999)
	if len(gaps) != 1 || gaps[0].DroppedCount != 3 {
		t.Fatalf("gaps not persisted: %v", gaps)
	}
}

func TestBlobUploadAndHasCheck(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	srv := httptest.NewServer(ingest.Handler(es, bs))
	defer srv.Close()

	payload := []byte("a payload")
	sum := sha256.Sum256(payload)

	resp, err := http.Post(srv.URL+"/v1/blobs/"+bs.IDFor(sum), "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	resp.Body.Close()

	missing, err := bs.Has(context.Background(), [][32]byte{sum})
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if len(missing) != 0 {
		t.Fatal("uploaded blob reported as missing")
	}
}
```

- [ ] **Step 6: Write the ingest handler**

Create `collector/ingest/ingest.go`:

```go
// Package ingest accepts recorded batches and blobs from SDKs.
package ingest

import (
	"crypto/sha256"
	"io"
	"net/http"
	"strings"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/store"
	"google.golang.org/protobuf/proto"
)

// MaxBatchBytes bounds a single ingest request.
const MaxBatchBytes = 8 << 20

// Handler returns the ingest routes.
func Handler(es store.EventStore, bs store.BlobStore) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/batch", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxBatchBytes))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		var b v1.Batch
		if err := proto.Unmarshal(body, &b); err != nil {
			http.Error(w, "malformed batch", http.StatusBadRequest)
			return
		}
		if len(b.Events) > 0 {
			if err := es.AppendEvents(r.Context(), b.Events); err != nil {
				http.Error(w, "append events", http.StatusInternalServerError)
				return
			}
		}
		if len(b.Gaps) > 0 {
			if err := es.AppendGaps(r.Context(), b.Gaps); err != nil {
				http.Error(w, "append gaps", http.StatusInternalServerError)
				return
			}
		}
		ack, _ := proto.Marshal(&v1.BatchAck{Accepted: uint32(len(b.Events))})
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(ack)
	})

	mux.HandleFunc("POST /v1/blobs/{blobID}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxBatchBytes))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(body)
		// The URL carries the expected blob ID; content that does not match is
		// rejected rather than silently stored under the wrong identity.
		if got := bs.IDFor(sum); got != strings.TrimSpace(r.PathValue("blobID")) {
			http.Error(w, "content does not match blob id", http.StatusBadRequest)
			return
		}
		if _, err := bs.Put(r.Context(), sum, body); err != nil {
			http.Error(w, "store blob", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	return mux
}
```

- [ ] **Step 7: Run the ingest tests**

Run: `go test ./collector/... -race -v`
Expected: PASS — both tests.

- [ ] **Step 8: Write the collector binary**

Create `collector/api/api.go`:

```go
// Package api assembles the collector's HTTP surface.
package api

import (
	"net/http"

	"github.com/ai-mond/mnemosyne/collector/ingest"
	"github.com/ai-mond/mnemosyne/store"
)

// New returns the collector handler. Query routes are mounted in Task 10.
func New(es store.EventStore, bs store.BlobStore) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/", ingest.Handler(es, bs))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	return mux
}
```

Create `cmd/mnemo/main.go`:

```go
// Command mnemo is the Mnemosyne collector.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/ai-mond/mnemosyne/collector/api"
	"github.com/ai-mond/mnemosyne/store/memory"
)

func main() {
	addr := flag.String("addr", ":4318", "listen address")
	flag.Parse()

	// Task 12 replaces these with SQLite and filesystem backends.
	es, bs := memory.NewEvents(), memory.NewBlobs()

	log.Printf("mnemo collector listening on %s", *addr)
	if err := http.ListenAndServe(*addr, api.New(es, bs)); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 9: Verify the collector runs**

Run in one shell: `go run ./cmd/mnemo -addr :4318`
Run in another: `curl -s http://localhost:4318/healthz`
Expected: `ok`. Stop the collector with Ctrl-C.

- [ ] **Step 10: Commit**

```bash
git add sdk/exporter/ collector/ cmd/mnemo/
git commit -m "feat(collector): add batching exporter with gap records and ingest"
```

---

## Task 9: Deterministic incident harness

**Objective:** The acceptance incident can be reproduced on demand, in-process, with a virtual clock — so the replay engine and UI are developed against a real recording rather than a production outage.

**Deliverables:** `sdk/link.go`, `harness/clock.go`, `harness/service.go`, `harness/scenario.go`, `harness/harness_test.go`.

**Interfaces:**
- Consumes: `sdk.Recorder`, `mnemohttp`, `store.EventStore`, `store.BlobStore`.
- Produces:
  - `sdk.RetryOf(tr identity.TraceID, prev identity.EventID) *v1.Link`
  - `harness.Clock` — `NewClock(start time.Time) *Clock`, `(*Clock).Now() time.Time`, `(*Clock).Advance(time.Duration)`, `(*Clock).Skew(time.Duration)`
  - `harness.Service{Name string; Rec *sdk.Recorder; URL string; Client *http.Client}`
  - `harness.New(t *testing.T, opts Options) *Harness` where `Options{Services []ServiceSpec; Sink sdk.Sink; Start time.Time}`
  - `harness.ServiceSpec{Name string; Handler func(*Ctx); ClockSkew time.Duration}`
  - `(*Harness).Service(name string) *Service`, `(*Harness).Run(service, path string) (*http.Response, error)`, `(*Harness).Close()`
  - `harness.AcceptanceScenario(t *testing.T, sink sdk.Sink) *Harness`
  - `harness.Ctx` — `Request() *http.Request`, `Call(service, path string) (*http.Response, error)`, `CallWithRetries(service, path string, attempts int) error`, `Hang()`, `Fail(code int)`

**Invariants:**
1. The harness is deterministic: same scenario, same event graph (modulo random IDs).
2. Each service has an independently skewable clock, so skew handling is exercised by construction.
3. `Hang()` produces an operation with a `BEGIN` and no `END`, and does not deadlock the test at teardown.
4. Retries produce `RETRY_OF` links and remain siblings under one scope.

**Tests:** The harness's own test asserts the acceptance scenario produces the expected edge counts, so a harness regression is caught before it corrupts downstream work.

**Definition of done:** `go test ./harness/ -race` reproduces the three-service incident and asserts: one trace, `REMOTE_CALL` edges across all three services, exactly three `RETRY_OF` links, and exactly one unmatched `BEGIN`.

**Risks:** A harness that is subtly non-deterministic makes every downstream failure hard to attribute. Determinism comes from the virtual clock plus in-process `httptest` servers; do not introduce real sleeps. Second risk: `Hang()` leaking goroutines and making the whole suite flaky — it must be released at `Close()`.

- [ ] **Step 1: Add the retry link helper**

Create `sdk/link.go`:

```go
package sdk

import (
	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk/identity"
)

// RetryOf builds the causal edge from a retry attempt to the attempt it
// replaces. Both attempts are siblings under one scope; neither contains the
// other, which is why this is a link and not a parent edge.
func RetryOf(tr identity.TraceID, prev identity.EventID) *v1.Link {
	em := prev.Emitter
	return &v1.Link{
		Type:    v1.EdgeType_RETRY_OF,
		TraceId: tr[:],
		Event:   &v1.EventID{EmitterId: em[:], Sequence: prev.Sequence},
	}
}
```

- [ ] **Step 2: Write the virtual clock**

Create `harness/clock.go`:

```go
// Package harness runs deterministic multi-service incidents in one process.
package harness

import (
	"sync"
	"time"
)

// Clock is a manually-advanced clock with a per-service skew offset. Real time
// is never consulted, which is what makes scenarios reproducible.
type Clock struct {
	mu   sync.Mutex
	now  time.Time
	skew time.Duration
}

// NewClock returns a Clock reading start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now returns the current virtual time including this clock's skew.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Add(c.skew)
}

// Advance moves virtual time forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Skew sets this clock's offset from true virtual time, simulating a machine
// whose NTP has drifted.
func (c *Clock) Skew(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.skew = d
}
```

- [ ] **Step 3: Write the harness**

Create `harness/service.go`:

```go
package harness

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/sdk"
	mnemohttp "github.com/ai-mond/mnemosyne/sdk/instrument/nethttp"
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
```

- [ ] **Step 4: Write the acceptance scenario**

Create `harness/scenario.go`:

```go
package harness

import (
	"net/http"
	"testing"
	"time"

	"github.com/ai-mond/mnemosyne/sdk"
)

// AcceptanceScenario builds the Spec 1 incident: checkout-api calls
// inventory-svc, which retries pricing-svc three times; the final attempt hangs
// and never returns. pricing-svc runs with a skewed clock.
func AcceptanceScenario(t *testing.T, sink sdk.Sink) *Harness {
	t.Helper()
	var attempts int

	return New(t, Options{
		Sink: sink,
		Services: []ServiceSpec{
			{
				Name: "checkout-api",
				Handler: func(c *Ctx) {
					if _, err := c.Call("inventory-svc", "/stock"); err != nil {
						c.Fail(http.StatusInternalServerError)
						return
					}
					c.Fail(http.StatusInternalServerError)
				},
			},
			{
				Name: "inventory-svc",
				Handler: func(c *Ctx) {
					if err := c.CallWithRetries("pricing-svc", "/price", 3); err != nil {
						c.Fail(http.StatusInternalServerError)
					}
				},
			},
			{
				Name:      "pricing-svc",
				ClockSkew: -400 * time.Millisecond,
				Handler: func(c *Ctx) {
					attempts++
					if attempts >= 3 {
						c.Hang() // the operation that never ends
						return
					}
					c.Fail(http.StatusInternalServerError)
				},
			},
		},
	})
}
```

- [ ] **Step 5: Write the harness test**

Create `harness/harness_test.go`:

```go
package harness_test

import (
	"sync"
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/harness"
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
```

- [ ] **Step 6: Run the harness test**

Run: `go test ./harness/ -race -v`
Expected: PASS. If it hangs, `Hang()` is not being released by `Close()` — fix that before proceeding.

- [ ] **Step 7: Commit**

```bash
git add sdk/link.go harness/
git commit -m "test(harness): add deterministic incident harness and acceptance scenario"
```

---

## Task 10: Replay engine and query API

**Objective:** A recording becomes a Timeline Model — the only thing the UI ever sees — with bounded response size, in-flight detection, gap markers, and skew warnings.

**Deliverables:** `replay/model.go`, `replay/engine.go`, `replay/chain.go`, `collector/query/query.go`, tests.

**Interfaces:**
- Consumes: `store.EventStore`, `store.BlobStore`, `v1.Event`, `v1.Gap`.
- Produces:
  - `replay.TimelineModel{From, To uint64; Resolution int; Lanes []Lane; InFlight []InFlightOp; Gaps []GapMarker; SkewWarnings []SkewWarning}`
  - `replay.Lane{Service, Resource string; Buckets []Bucket}`, `replay.Bucket{Index int; Count, ErrorCount int; MaxDurationNanos int64}`
  - `replay.InFlightOp{EventID *v1.EventID; Service, Description string; StartWallMS uint64}`
  - `replay.GapMarker{EmitterID []byte; FromWallMS, ToWallMS uint64; DroppedCount uint64; Reason string}`
  - `replay.SkewWarning{Service string; Emitter []byte; ObservedSkewMS int64}`
  - `replay.Chain{Nodes []ChainNode; Truncated bool}`, `replay.ChainNode{Event *v1.Event; EdgeFromParent string; Depth int}`
  - `replay.New(es store.EventStore, bs store.BlobStore) *Engine`
  - `(*Engine).Timeline(ctx, TimelineQuery) (*TimelineModel, error)` where `TimelineQuery{FromWallMS, ToWallMS uint64; Services []string; Resolution int}`
  - `(*Engine).Chain(ctx, traceID []byte, eventID *v1.EventID, maxNodes int) (*Chain, error)`
  - `query.Handler(*replay.Engine, store.EventStore, store.BlobStore) http.Handler`

**Invariants:**
1. **Response size is a function of `Resolution`, not incident size.** A timeline over a million events returns at most `len(Lanes) * Resolution` buckets.
2. `BEGIN` without a matching `END` becomes an `InFlightOp` — never dropped, never rendered as complete.
3. Chain traversal is bounded by `maxNodes` and sets `Truncated` when it stops early.
4. Ordering is by `(hlc.wall_ms, hlc.logical, emitter_id)`. Wall clock is used only for skew detection.
5. A child event whose wall clock precedes its parent's produces a `SkewWarning`, not an impossible timeline.

**Tests:** Bucketing bound under a large synthetic event set, in-flight detection using the harness scenario, chain truncation, skew warning from the harness's skewed service, and gap markers surfacing in the model.

**Definition of done:** `go test ./replay/ -race` is green, including a test that feeds 100,000 synthetic events at `Resolution: 200` and asserts no lane exceeds 200 buckets.

**Risks:** Bucketing arithmetic that is off by one at window edges silently drops the first or last event of an incident — which is often the interesting one. The bucketing test asserts on total counts, not just bucket length, so a dropped event fails the test.

- [ ] **Step 1: Write the model types**

Create `replay/model.go`:

```go
// Package replay turns a recording into a Timeline Model.
//
// The Timeline Model is the only representation the UI consumes. Aggregation
// happens here, server-side, so that response size is a function of screen
// resolution rather than incident size.
package replay

import v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"

// Bucket is one aggregated slot in a lane.
type Bucket struct {
	Index            int   `json:"index"`
	Count            int   `json:"count"`
	ErrorCount       int   `json:"errorCount"`
	MaxDurationNanos int64 `json:"maxDurationNanos"`
}

// Lane is one horizontal row on the timeline.
type Lane struct {
	Service  string   `json:"service"`
	Resource string   `json:"resource"`
	Buckets  []Bucket `json:"buckets"`
}

// InFlightOp is an operation whose BEGIN was recorded but whose END never was.
type InFlightOp struct {
	EventID     *v1.EventID `json:"eventId"`
	Service     string      `json:"service"`
	Description string      `json:"description"`
	StartWallMS uint64      `json:"startWallMs"`
}

// GapMarker describes events known to be missing.
type GapMarker struct {
	EmitterID    []byte `json:"emitterId"`
	FromWallMS   uint64 `json:"fromWallMs"`
	ToWallMS     uint64 `json:"toWallMs"`
	DroppedCount uint64 `json:"droppedCount"`
	Reason       string `json:"reason"`
}

// SkewWarning reports an emitter whose wall clock provably disagrees with
// causal order.
type SkewWarning struct {
	Service        string `json:"service"`
	Emitter        []byte `json:"emitter"`
	ObservedSkewMS int64  `json:"observedSkewMs"`
}

// TimelineModel is the complete server-rendered view of a time window.
type TimelineModel struct {
	FromWallMS   uint64        `json:"fromWallMs"`
	ToWallMS     uint64        `json:"toWallMs"`
	Resolution   int           `json:"resolution"`
	Lanes        []Lane        `json:"lanes"`
	InFlight     []InFlightOp  `json:"inFlight"`
	Gaps         []GapMarker   `json:"gaps"`
	SkewWarnings []SkewWarning `json:"skewWarnings"`
}

// ChainNode is one step in a recorded causal chain.
type ChainNode struct {
	Event          *v1.Event `json:"event"`
	EdgeFromParent string    `json:"edgeFromParent"`
	Depth          int       `json:"depth"`
}

// Chain is a recorded causal chain. Every edge in it was observed; nothing here
// is inferred.
type Chain struct {
	Nodes     []ChainNode `json:"nodes"`
	Truncated bool        `json:"truncated"`
}
```

- [ ] **Step 2: Write the failing engine tests**

Create `replay/engine_test.go`:

```go
package replay_test

import (
	"context"
	"fmt"
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/replay"
	"github.com/ai-mond/mnemosyne/store/memory"
)

func synth(seq uint64, wallMS uint64, svc string, phase v1.Phase, status v1.Status) *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       make([]byte, 16),
		Id:            &v1.EventID{EmitterId: []byte("emitter0"), Sequence: seq},
		Service:       &v1.Service{Name: svc, EmitterId: []byte("emitter0")},
		Hlc:           &v1.HLC{WallMs: wallMS},
		WallUnixNanos: int64(wallMS) * 1e6,
		Phase:         phase,
		OperationType: v1.OperationType_HTTP_SERVER,
		Operation:     &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/r"}},
		Status:        status,
	}
}

func TestBucketCountIsBoundedByResolution(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	const n = 100_000
	batch := make([]*v1.Event, 0, n)
	for i := 0; i < n; i++ {
		batch = append(batch, synth(uint64(i), 1000+uint64(i), "svc", v1.Phase_END, v1.Status_OK))
	}
	if err := es.AppendEvents(ctx, batch); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	eng := replay.New(es, bs)
	m, err := eng.Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 1000, ToWallMS: 1000 + n, Resolution: 200,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.Lanes) == 0 {
		t.Fatal("no lanes produced")
	}
	total := 0
	for _, lane := range m.Lanes {
		if len(lane.Buckets) > 200 {
			t.Fatalf("lane %s has %d buckets, exceeds resolution 200", lane.Service, len(lane.Buckets))
		}
		for _, b := range lane.Buckets {
			total += b.Count
		}
	}
	if total != n {
		t.Fatalf("buckets account for %d events, want %d — events were dropped at a window edge", total, n)
	}
}

func TestUnmatchedBeginBecomesInFlight(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	if err := es.AppendEvents(ctx, []*v1.Event{
		synth(1, 1000, "svc", v1.Phase_BEGIN, v1.Status_OK),
		synth(1, 1000, "svc", v1.Phase_END, v1.Status_OK),
		synth(2, 1001, "svc", v1.Phase_BEGIN, v1.Status_OK), // never ends
	}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: 9999, Resolution: 50,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.InFlight) != 1 || m.InFlight[0].EventID.Sequence != 2 {
		t.Fatalf("in-flight = %v, want exactly event 2", m.InFlight)
	}
}

func TestSkewWarningWhenChildWallClockPrecedesParent(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	parent := synth(1, 1000, "parent-svc", v1.Phase_END, v1.Status_OK)
	parent.WallUnixNanos = 5_000_000_000

	child := synth(2, 1001, "skewed-svc", v1.Phase_END, v1.Status_OK)
	child.Service.EmitterId = []byte("emitter1")
	child.Id.EmitterId = []byte("emitter1")
	child.Parent = &v1.EventID{EmitterId: []byte("emitter0"), Sequence: 1}
	child.WallUnixNanos = 1_000_000_000 // four seconds before its parent

	if err := es.AppendEvents(ctx, []*v1.Event{parent, child}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: 9999, Resolution: 50,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.SkewWarnings) == 0 {
		t.Fatal("no skew warning for a child whose wall clock precedes its parent")
	}
	if m.SkewWarnings[0].Service != "skewed-svc" {
		t.Fatalf("skew attributed to %q, want skewed-svc", m.SkewWarnings[0].Service)
	}
}

func TestGapsSurfaceInTheModel(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	if err := es.AppendGaps(ctx, []*v1.Gap{{
		EmitterId: []byte("emitter0"),
		From:      &v1.HLC{WallMs: 1000}, To: &v1.HLC{WallMs: 2000},
		DroppedCount: 17, Reason: v1.GapReason_BACKPRESSURE,
	}}); err != nil {
		t.Fatalf("AppendGaps: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: 9999, Resolution: 50,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.Gaps) != 1 || m.Gaps[0].DroppedCount != 17 {
		t.Fatalf("gaps = %v, want one marker with 17 drops", m.Gaps)
	}
}

func TestChainIsBoundedAndMarksTruncation(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := context.Background()

	// A deep parent chain: 1 <- 2 <- 3 <- ... <- 50
	var batch []*v1.Event
	for i := uint64(1); i <= 50; i++ {
		e := synth(i, 1000+i, "svc", v1.Phase_END, v1.Status_OK)
		if i > 1 {
			e.Parent = &v1.EventID{EmitterId: []byte("emitter0"), Sequence: i - 1}
		}
		batch = append(batch, e)
	}
	if err := es.AppendEvents(ctx, batch); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	chain, err := replay.New(es, bs).Chain(ctx, batch[0].TraceId,
		&v1.EventID{EmitterId: []byte("emitter0"), Sequence: 50}, 10)
	if err != nil {
		t.Fatalf("Chain: %v", err)
	}
	if len(chain.Nodes) > 10 {
		t.Fatalf("chain returned %d nodes, exceeds maxNodes 10", len(chain.Nodes))
	}
	if !chain.Truncated {
		t.Fatal("chain hit its bound but did not report Truncated")
	}
	if got := fmt.Sprint(chain.Nodes[0].Event.Id.Sequence); got != "50" {
		t.Fatalf("chain starts at event %s, want the selected event 50", got)
	}
}

```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./replay/`
Expected: FAIL — `undefined: replay.New`

- [ ] **Step 4: Write the engine**

Create `replay/engine.go`:

```go
package replay

import (
	"context"
	"fmt"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/store"
)

// TimelineQuery selects and shapes a timeline.
type TimelineQuery struct {
	FromWallMS uint64
	ToWallMS   uint64
	Services   []string
	Resolution int
}

// Engine builds Timeline Models from a recording.
type Engine struct {
	events store.EventStore
	blobs  store.BlobStore
}

// New returns an Engine.
func New(es store.EventStore, bs store.BlobStore) *Engine {
	return &Engine{events: es, blobs: bs}
}

// DefaultResolution is used when a query does not specify one.
const DefaultResolution = 240

// Timeline builds the model for a window.
func (e *Engine) Timeline(ctx context.Context, q TimelineQuery) (*TimelineModel, error) {
	if q.Resolution <= 0 {
		q.Resolution = DefaultResolution
	}
	if q.ToWallMS <= q.FromWallMS {
		return nil, fmt.Errorf("replay: empty window %d..%d", q.FromWallMS, q.ToWallMS)
	}

	events, err := e.events.Window(ctx, store.WindowQuery{
		FromWallMS: q.FromWallMS, ToWallMS: q.ToWallMS, Services: q.Services,
	})
	if err != nil {
		return nil, err
	}

	m := &TimelineModel{
		FromWallMS: q.FromWallMS, ToWallMS: q.ToWallMS, Resolution: q.Resolution,
	}

	span := q.ToWallMS - q.FromWallMS
	laneIdx := map[string]int{}
	begins := map[string]*v1.Event{}
	ended := map[string]bool{}
	byID := map[string]*v1.Event{}

	for _, ev := range events {
		key := eventKey(ev.GetId())
		switch ev.Phase {
		case v1.Phase_BEGIN:
			begins[key] = ev
		case v1.Phase_END:
			ended[key] = true
		}
		if ev.Phase != v1.Phase_BEGIN {
			byID[key] = ev
		}

		// Only END and INSTANT events contribute to lane aggregates, so an
		// operation is counted once rather than twice.
		if ev.Phase == v1.Phase_BEGIN {
			continue
		}

		name := laneName(ev)
		idx, ok := laneIdx[name]
		if !ok {
			idx = len(m.Lanes)
			laneIdx[name] = idx
			m.Lanes = append(m.Lanes, Lane{
				Service:  ev.GetService().GetName(),
				Resource: laneResource(ev),
				Buckets:  make([]Bucket, q.Resolution),
			})
			for i := range m.Lanes[idx].Buckets {
				m.Lanes[idx].Buckets[i].Index = i
			}
		}

		// Bucket index: clamp the final millisecond into the last bucket so no
		// event falls off the right edge of the window.
		offset := ev.GetHlc().GetWallMs() - q.FromWallMS
		b := int(offset * uint64(q.Resolution) / span)
		if b >= q.Resolution {
			b = q.Resolution - 1
		}

		bucket := &m.Lanes[idx].Buckets[b]
		bucket.Count++
		if ev.Status == v1.Status_ERROR || ev.Status == v1.Status_TIMEOUT {
			bucket.ErrorCount++
		}
		if ev.DurationNanos > bucket.MaxDurationNanos {
			bucket.MaxDurationNanos = ev.DurationNanos
		}
	}

	for key, ev := range begins {
		if ended[key] {
			continue
		}
		m.InFlight = append(m.InFlight, InFlightOp{
			EventID:     ev.GetId(),
			Service:     ev.GetService().GetName(),
			Description: describe(ev),
			StartWallMS: ev.GetHlc().GetWallMs(),
		})
	}

	m.SkewWarnings = detectSkew(events, byID)

	gaps, err := e.events.Gaps(ctx, q.FromWallMS, q.ToWallMS)
	if err != nil {
		return nil, err
	}
	for _, g := range gaps {
		m.Gaps = append(m.Gaps, GapMarker{
			EmitterID:    g.EmitterId,
			FromWallMS:   g.GetFrom().GetWallMs(),
			ToWallMS:     g.GetTo().GetWallMs(),
			DroppedCount: g.DroppedCount,
			Reason:       g.Reason.String(),
		})
	}
	return m, nil
}

// detectSkew reports emitters whose wall clock disagrees with causal order: a
// child cannot truly have started before the parent that caused it.
func detectSkew(events []*v1.Event, byID map[string]*v1.Event) []SkewWarning {
	worst := map[string]SkewWarning{}
	for _, ev := range events {
		parent := byID[eventKey(ev.GetParent())]
		if parent == nil || ev.GetParent() == nil {
			continue
		}
		delta := (parent.WallUnixNanos - ev.WallUnixNanos) / 1e6
		if delta <= 0 {
			continue
		}
		svc := ev.GetService().GetName()
		if cur, ok := worst[svc]; !ok || delta > cur.ObservedSkewMS {
			worst[svc] = SkewWarning{
				Service:        svc,
				Emitter:        ev.GetId().GetEmitterId(),
				ObservedSkewMS: delta,
			}
		}
	}
	out := make([]SkewWarning, 0, len(worst))
	for _, w := range worst {
		out = append(out, w)
	}
	return out
}

func eventKey(id *v1.EventID) string {
	if id == nil {
		return ""
	}
	return fmt.Sprintf("%x/%d", id.EmitterId, id.Sequence)
}

func laneName(e *v1.Event) string {
	return e.GetService().GetName() + "\x00" + laneResource(e)
}

func laneResource(e *v1.Event) string {
	switch e.OperationType {
	case v1.OperationType_HTTP_SERVER:
		return "http.server"
	case v1.OperationType_HTTP_CLIENT:
		return "http.client"
	default:
		return "local"
	}
}

func describe(e *v1.Event) string {
	if s := e.GetHttpServer(); s != nil {
		return s.Method + " " + s.Route
	}
	if c := e.GetHttpClient(); c != nil {
		return c.Method + " " + c.Host + c.Path
	}
	return e.OperationType.String()
}
```

- [ ] **Step 5: Write the chain walker**

Create `replay/chain.go`:

```go
package replay

import (
	"context"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
)

// Chain returns the recorded causal chain containing eventID: the ancestor path
// via PARENT edges, followed by outgoing link edges. Traversal is bounded by
// maxNodes; every edge returned was observed, never inferred.
func (e *Engine) Chain(ctx context.Context, traceID []byte, eventID *v1.EventID, maxNodes int) (*Chain, error) {
	if maxNodes <= 0 {
		maxNodes = 200
	}
	events, err := e.events.TraceEvents(ctx, traceID)
	if err != nil {
		return nil, err
	}

	// Index by ID, preferring END events since they carry outcome and duration.
	byID := map[string]*v1.Event{}
	for _, ev := range events {
		k := eventKey(ev.GetId())
		if cur, ok := byID[k]; !ok || cur.Phase == v1.Phase_BEGIN {
			byID[k] = ev
		}
	}

	chain := &Chain{}
	seen := map[string]bool{}

	type step struct {
		id    *v1.EventID
		edge  string
		depth int
	}
	queue := []step{{id: eventID, edge: "", depth: 0}}

	for len(queue) > 0 {
		if len(chain.Nodes) >= maxNodes {
			chain.Truncated = true
			break
		}
		cur := queue[0]
		queue = queue[1:]

		k := eventKey(cur.id)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true

		ev := byID[k]
		if ev == nil {
			continue
		}
		chain.Nodes = append(chain.Nodes, ChainNode{
			Event: ev, EdgeFromParent: cur.edge, Depth: cur.depth,
		})

		if p := ev.GetParent(); p != nil {
			queue = append(queue, step{id: p, edge: v1.EdgeType_PARENT.String(), depth: cur.depth + 1})
		}
		for _, l := range ev.Links {
			if l.Event == nil {
				continue // foreign link: correlation only, nothing local to walk
			}
			queue = append(queue, step{id: l.Event, edge: l.Type.String(), depth: cur.depth + 1})
		}
	}
	return chain, nil
}
```

- [ ] **Step 6: Run the replay tests**

Run: `go test ./replay/ -race -v`
Expected: PASS — all five tests.

- [ ] **Step 7: Write the query API**

Create `collector/query/query.go`:

```go
// Package query serves the read side of the collector.
package query

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/replay"
	"github.com/ai-mond/mnemosyne/store"
)

// Handler returns the query routes.
func Handler(eng *replay.Engine, es store.EventStore, bs store.BlobStore) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/timeline", func(w http.ResponseWriter, r *http.Request) {
		q := replay.TimelineQuery{
			FromWallMS: uintParam(r, "from"),
			ToWallMS:   uintParam(r, "to"),
			Resolution: intParam(r, "resolution"),
			Services:   r.URL.Query()["service"],
		}
		m, err := eng.Timeline(r.Context(), q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, m)
	})

	mux.HandleFunc("GET /api/chain", func(w http.ResponseWriter, r *http.Request) {
		traceID, err := hex.DecodeString(r.URL.Query().Get("trace"))
		if err != nil {
			http.Error(w, "bad trace id", http.StatusBadRequest)
			return
		}
		emitter, err := hex.DecodeString(r.URL.Query().Get("emitter"))
		if err != nil {
			http.Error(w, "bad emitter id", http.StatusBadRequest)
			return
		}
		id := &v1.EventID{EmitterId: emitter, Sequence: uintParam(r, "seq")}
		chain, err := eng.Chain(r.Context(), traceID, id, intParam(r, "max"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, chain)
	})

	mux.HandleFunc("GET /api/traces", func(w http.ResponseWriter, r *http.Request) {
		traces, err := es.Traces(r.Context(), store.TraceQuery{
			FromWallMS: uintParam(r, "from"),
			ToWallMS:   uintParam(r, "to"),
			Service:    r.URL.Query().Get("service"),
			OnlyErrors: r.URL.Query().Get("errors") == "1",
			Limit:      intParam(r, "limit"),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, traces)
	})

	mux.HandleFunc("GET /api/blob/{blobID}", func(w http.ResponseWriter, r *http.Request) {
		body, err := bs.Get(r.Context(), r.PathValue("blobID"))
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(body)
	})

	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func uintParam(r *http.Request, name string) uint64 {
	v, _ := strconv.ParseUint(r.URL.Query().Get(name), 10, 64)
	return v
}

func intParam(r *http.Request, name string) int {
	v, _ := strconv.Atoi(r.URL.Query().Get(name))
	return v
}
```

- [ ] **Step 8: Mount the query routes**

In `collector/api/api.go`, replace `New` with:

```go
// New returns the collector handler.
func New(es store.EventStore, bs store.BlobStore) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/", ingest.Handler(es, bs))
	mux.Handle("/api/", query.Handler(replay.New(es, bs), es, bs))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	return mux
}
```

Add imports for `query` and `replay`.

- [ ] **Step 9: Run everything**

Run: `go test ./... -race`
Expected: PASS across all packages.

- [ ] **Step 10: Commit**

```bash
git add replay/ collector/
git commit -m "feat(replay): add replay engine with bounded bucketing and query API"
```

---

## Task 11: Split-view replay UI

**Objective:** The acceptance incident is visibly correct on screen — swimlane timeline on the left, recorded causal chain on the right, with in-flight, gap, and skew states rendered distinctly.

> **REQUIRED SKILL:** Invoke the `ui-ux-pro-max` skill before writing any markup or styling in this task. It governs visual design, component styling, and interaction patterns. The layout and required states below are fixed by the spec; everything about how they *look* comes from that skill.

**Deliverables:** `ui/index.html`, `ui/app.js`, `ui/style.css`, `ui/embed.go`, wiring in `collector/api/api.go`, `ui/ui_test.go`.

**Interfaces:**
- Consumes: `GET /api/timeline`, `GET /api/chain`, `GET /api/traces`, `GET /api/blob/{blobID}`.
- Produces: `ui.Handler() http.Handler` serving the embedded assets at `/`.

**Invariants (fixed by the spec — the skill governs appearance, not presence):**
1. Selecting anything on the timeline loads and renders its recorded causal chain.
2. In-flight operations render with a **`never ended`** marker, visually distinct from completed ones.
3. Gap markers render **inline at the point in the timeline where events were lost** — never as a footnote, never silently.
4. Skew warnings replace impossible timeline geometry rather than rendering it.
5. Recorded and inferred edges have distinct visual language. v1 renders only recorded edges; the distinction is established now so adding inference later cannot quietly blur them.
6. HLC never appears in the narrative view. It appears in the raw event inspector.
7. No external network requests — the collector serves everything, and a strict offline page must work.

**Tests:** A Go test asserting the embedded assets are served and non-empty; a manual verification step against the live acceptance scenario.

**Definition of done:** With the collector running and the acceptance scenario recorded into it, opening the UI shows one trace whose chain panel reads `checkout-api → inventory-svc → 3 retry attempts → pricing-svc (never ended)`, with the skew warning present for `pricing-svc`.

**Risks:** The temptation to make the UI compute the timeline model itself. It must not — bucketing is server-side, and a browser-side reimplementation would diverge from the future CLI. The UI's only job is rendering what `/api/timeline` returns.

- [ ] **Step 1: Invoke the design skill**

Invoke `ui-ux-pro-max` and use its guidance for the visual system: palette, typography, spacing scale, and the treatment of the error/in-flight/gap/skew states listed above. Do not proceed to markup before this.

- [ ] **Step 2: Write the asset-embedding test**

Create `ui/ui_test.go`:

```go
package ui_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ai-mond/mnemosyne/ui"
)

func TestIndexIsServed(t *testing.T) {
	srv := httptest.NewServer(ui.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content type = %q, want text/html", ct)
	}
}

func TestAssetsAreServed(t *testing.T) {
	srv := httptest.NewServer(ui.Handler())
	defer srv.Close()

	for _, path := range []string{"/app.js", "/style.css"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", path, resp.StatusCode)
		}
		if resp.ContentLength == 0 {
			t.Fatalf("%s is empty", path)
		}
	}
}
```

- [ ] **Step 3: Write the embed wrapper**

Create `ui/embed.go`:

```go
// Package ui serves the embedded replay interface.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed index.html app.js style.css
var assets embed.FS

// Handler serves the replay UI. Everything is embedded in the binary: the
// collector must work with no network access to anywhere else.
func Handler() http.Handler {
	sub, err := fs.Sub(assets, ".")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}
```

- [ ] **Step 4: Write the markup shell**

Create `ui/index.html`. The structure below is required; all styling comes from the design skill.

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Mnemosyne — Replay</title>
  <link rel="stylesheet" href="/style.css">
</head>
<body>
  <header id="toolbar">
    <h1>Mnemosyne</h1>
    <label>Window <input id="from" type="number" placeholder="from (ms)"></label>
    <label>to <input id="to" type="number" placeholder="to (ms)"></label>
    <button id="load">Load</button>
    <span id="status" role="status"></span>
  </header>

  <main id="split">
    <section id="timeline" aria-label="Timeline">
      <div id="lanes"></div>
      <div id="gaps" aria-label="Dropped events"></div>
      <div id="skew" aria-label="Clock skew warnings"></div>
    </section>

    <aside id="chain-panel" aria-label="Causal chain">
      <h2>Causal chain <span class="badge">recorded</span></h2>
      <ol id="chain"></ol>
      <section id="inspector" hidden>
        <h3>Raw event</h3>
        <pre id="raw"></pre>
      </section>
    </aside>
  </main>

  <script src="/app.js" type="module"></script>
</body>
</html>
```

- [ ] **Step 5: Write the renderer**

Create `ui/app.js`:

```js
// The UI renders what the server computed. It never aggregates events itself:
// bucketing is server-side so that the future CLI and this page cannot diverge.

const $ = (id) => document.getElementById(id);

async function json(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
}

let model = null;

async function loadTimeline() {
  const from = $("from").value || 0;
  const to = $("to").value || Date.now();
  const resolution = Math.max(60, Math.floor($("timeline").clientWidth / 4));

  $("status").textContent = "loading…";
  model = await json(`/api/timeline?from=${from}&to=${to}&resolution=${resolution}`);
  renderLanes(model);
  renderGaps(model);
  renderSkew(model);
  renderInFlight(model);
  $("status").textContent = `${model.lanes.length} lanes`;
}

function renderLanes(m) {
  const host = $("lanes");
  host.replaceChildren();
  for (const lane of m.lanes) {
    const row = document.createElement("div");
    row.className = "lane";

    const label = document.createElement("span");
    label.className = "lane-label";
    label.textContent = `${lane.service} · ${lane.resource}`;
    row.append(label);

    const track = document.createElement("div");
    track.className = "lane-track";
    for (const b of lane.buckets) {
      if (b.count === 0) continue;
      const cell = document.createElement("button");
      cell.className = b.errorCount > 0 ? "bucket bucket--error" : "bucket";
      cell.style.left = `${(b.index / m.resolution) * 100}%`;
      cell.style.width = `${100 / m.resolution}%`;
      cell.title = `${b.count} operations, ${b.errorCount} errors`;
      cell.setAttribute("aria-label", cell.title);
      track.append(cell);
    }
    row.append(track);
    host.append(row);
  }
}

// Gap markers render inline at the point events were lost. A silent hole is
// indistinguishable from "nothing happened", which is the worst thing this
// tool could do.
function renderGaps(m) {
  const host = $("gaps");
  host.replaceChildren();
  for (const g of m.gaps) {
    const el = document.createElement("div");
    el.className = "gap";
    const span = m.toWallMs - m.fromWallMs;
    el.style.left = `${((g.fromWallMs - m.fromWallMs) / span) * 100}%`;
    el.style.width = `${Math.max(0.5, ((g.toWallMs - g.fromWallMs) / span) * 100)}%`;
    el.textContent = `${g.droppedCount} events dropped (${g.reason})`;
    host.append(el);
  }
}

// A skewed clock is reported, never rendered as an impossible timeline.
function renderSkew(m) {
  const host = $("skew");
  host.replaceChildren();
  for (const s of m.skewWarnings) {
    const el = document.createElement("div");
    el.className = "skew";
    el.textContent = `clock skew: ${s.service} is ${s.observedSkewMs}ms behind its caller`;
    host.append(el);
  }
}

function renderInFlight(m) {
  for (const op of m.inFlight) {
    const el = document.createElement("button");
    el.className = "inflight";
    el.textContent = `${op.service} · ${op.description} — never ended`;
    el.addEventListener("click", () => loadChain(op.eventId));
    $("lanes").append(el);
  }
}

async function loadChain(eventId) {
  const emitter = toHex(eventId.emitterId);
  const trace = new URLSearchParams(location.search).get("trace") || "";
  const chain = await json(`/api/chain?trace=${trace}&emitter=${emitter}&seq=${eventId.sequence}&max=200`);
  renderChain(chain);
}

function renderChain(chain) {
  const host = $("chain");
  host.replaceChildren();
  for (const node of chain.nodes) {
    const li = document.createElement("li");
    li.className = "chain-node";
    li.dataset.edge = node.edgeFromParent || "SELECTED";
    // Every edge shown here was observed. Inferred edges, when they exist, get
    // their own visual treatment and must never share this one.
    li.dataset.provenance = "recorded";

    const ev = node.event;
    const title = document.createElement("span");
    title.className = "chain-title";
    title.textContent = describe(ev);
    li.append(title);

    if (!ev.durationNanos) {
      const tag = document.createElement("span");
      tag.className = "tag tag--inflight";
      tag.textContent = "never ended";
      li.append(tag);
    }

    li.addEventListener("click", () => showRaw(ev));
    host.append(li);
  }
  if (chain.truncated) {
    const li = document.createElement("li");
    li.className = "chain-truncated";
    li.textContent = "chain truncated — more causally related events exist";
    host.append(li);
  }
}

function describe(ev) {
  if (ev.httpServer) return `${ev.service.name}: ${ev.httpServer.method} ${ev.httpServer.route}`;
  if (ev.httpClient) return `${ev.service.name} → ${ev.httpClient.host}${ev.httpClient.path}`;
  return `${ev.service.name}: ${ev.operationType}`;
}

// HLC belongs in the inspector, never the narrative. Without it, an engineer
// who disagrees with an ordering has no way to find out why.
function showRaw(ev) {
  $("inspector").hidden = false;
  $("raw").textContent = JSON.stringify(ev, null, 2);
}

function toHex(b64) {
  const bin = atob(b64);
  return Array.from(bin, (c) => c.charCodeAt(0).toString(16).padStart(2, "0")).join("");
}

$("load").addEventListener("click", () => loadTimeline().catch((e) => {
  $("status").textContent = e.message;
}));
```

- [ ] **Step 6: Write the stylesheet**

Create `ui/style.css` following the `ui-ux-pro-max` guidance from Step 1. It must give visually distinct treatments to: normal buckets, error buckets, `.gap`, `.skew`, `.inflight`, `.tag--inflight`, and `[data-provenance="recorded"]`, and lay `#split` out as a two-pane split with the timeline on the left.

- [ ] **Step 7: Mount the UI on the collector**

In `collector/api/api.go`, add to `New`:

```go
	mux.Handle("/", ui.Handler())
```

Add the `ui` import.

- [ ] **Step 8: Run the UI tests**

Run: `go test ./ui/ -v`
Expected: PASS — both tests.

- [ ] **Step 9: Verify against the live scenario**

Add `cmd/mnemodev/scenario.go`:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ai-mond/mnemosyne/harness"
	"github.com/ai-mond/mnemosyne/sdk/exporter"
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
	h.Close()
	<-done

	if err := ex.Flush(context.Background()); err != nil {
		return err
	}
	fmt.Fprintf(w, "recorded acceptance scenario to %s (%d events dropped)\n", endpoint, ex.DroppedCount())
	return nil
}
```

Add the flag to `cmd/mnemodev/main.go`:

```go
	scenario := flag.String("scenario", "", "record the acceptance scenario to a collector endpoint")
```

and the branch:

```go
	case *scenario != "":
		if err := runScenario(os.Stdout, *scenario); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
```

Run the collector, then:

```bash
go run ./cmd/mnemodev -scenario http://localhost:4318
```

then open `http://localhost:4318/`.

Expected on screen: three lanes, an error band on `pricing-svc`, a `never ended` entry, a clock-skew warning naming `pricing-svc`, and a chain panel reading checkout-api → inventory-svc → retry attempts → pricing-svc.

- [ ] **Step 10: Commit**

```bash
git add ui/ collector/ cmd/
git commit -m "feat(ui): add split-view replay UI with in-flight, gap, and skew states"
```

---

## Task 12: SQLite and filesystem storage

**Objective:** The same conformance suite that proves the in-memory stores passes against durable backends, so persistence is a swap rather than a rewrite.

**Deliverables:** `store/sqlite/sqlite.go`, `store/blobfs/blobfs.go`, tests wiring both into `storetest.RunConformance`, and collector flags selecting the backend.

**Interfaces:**
- Produces: `sqlite.Open(path string) (*Store, error)` implementing `store.EventStore`; `blobfs.Open(dir string) (*Store, error)` implementing `store.BlobStore`.

**Invariants:**
1. Both backends pass `storetest.RunConformance` unchanged — no backend-specific test relaxations.
2. `CGO_ENABLED=0` still builds. The driver is `modernc.org/sqlite`.
3. Events are indexed on `(hlc_wall_ms, hlc_logical, emitter_id)` and on `trace_id`.
4. `blobfs` is content-addressed and sharded by hash prefix so no directory holds an unbounded number of files.

**Definition of done:** `CGO_ENABLED=0 go test ./store/... -race` passes for memory, sqlite, and blobfs, and `mnemo -store sqlite -data ./data` serves a replay of a recording that survives a restart.

**Risks:** Writing SQLite-specific tests instead of reusing the conformance suite, which would let the two backends drift. The suite is the only test either backend gets.

- [ ] **Step 1: Add the driver**

```bash
go get modernc.org/sqlite@latest
```

- [ ] **Step 2: Write the conformance wiring first**

Create `store/sqlite/sqlite_test.go`:

```go
package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/ai-mond/mnemosyne/store"
	"github.com/ai-mond/mnemosyne/store/blobfs"
	"github.com/ai-mond/mnemosyne/store/sqlite"
	"github.com/ai-mond/mnemosyne/store/storetest"
)

func TestDurableConformance(t *testing.T) {
	storetest.RunConformance(t,
		func(t *testing.T) store.EventStore {
			s, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
			if err != nil {
				t.Fatalf("sqlite.Open: %v", err)
			}
			return s
		},
		func(t *testing.T) store.BlobStore {
			s, err := blobfs.Open(t.TempDir())
			if err != nil {
				t.Fatalf("blobfs.Open: %v", err)
			}
			return s
		},
	)
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./store/sqlite/`
Expected: build failure — packages do not exist.

- [ ] **Step 4: Write the SQLite event store**

Create `store/sqlite/sqlite.go`:

```go
// Package sqlite is the default durable EventStore. It uses modernc.org/sqlite
// so the collector builds with CGO_ENABLED=0.
package sqlite

import (
	"context"
	"database/sql"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/store"
	_ "modernc.org/sqlite"
	"google.golang.org/protobuf/proto"
)

const schema = `
CREATE TABLE IF NOT EXISTS events (
  trace_id      BLOB    NOT NULL,
  emitter_id    BLOB    NOT NULL,
  sequence      INTEGER NOT NULL,
  phase         INTEGER NOT NULL,
  hlc_wall_ms   INTEGER NOT NULL,
  hlc_logical   INTEGER NOT NULL,
  service       TEXT    NOT NULL,
  status        INTEGER NOT NULL,
  body          BLOB    NOT NULL,
  PRIMARY KEY (emitter_id, sequence, phase)
);
CREATE INDEX IF NOT EXISTS events_by_time  ON events (hlc_wall_ms, hlc_logical, emitter_id);
CREATE INDEX IF NOT EXISTS events_by_trace ON events (trace_id);

CREATE TABLE IF NOT EXISTS gaps (
  emitter_id    BLOB    NOT NULL,
  from_wall_ms  INTEGER NOT NULL,
  to_wall_ms    INTEGER NOT NULL,
  body          BLOB    NOT NULL
);
CREATE INDEX IF NOT EXISTS gaps_by_time ON gaps (from_wall_ms, to_wall_ms);
`

// Store is a SQLite-backed EventStore.
type Store struct{ db *sql.DB }

// Open opens or creates the database at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) AppendEvents(ctx context.Context, events []*v1.Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO events
	  (trace_id, emitter_id, sequence, phase, hlc_wall_ms, hlc_logical, service, status, body)
	  VALUES (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, e := range events {
		body, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx,
			e.TraceId, e.GetId().GetEmitterId(), e.GetId().GetSequence(), int(e.Phase),
			e.GetHlc().GetWallMs(), e.GetHlc().GetLogical(),
			e.GetService().GetName(), int(e.Status), body,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AppendGaps(ctx context.Context, gaps []*v1.Gap) error {
	for _, g := range gaps {
		body, err := proto.Marshal(g)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO gaps (emitter_id, from_wall_ms, to_wall_ms, body) VALUES (?,?,?,?)`,
			g.EmitterId, g.GetFrom().GetWallMs(), g.GetTo().GetWallMs(), body,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) TraceEvents(ctx context.Context, traceID []byte) ([]*v1.Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT body FROM events WHERE trace_id = ?
		 ORDER BY hlc_wall_ms, hlc_logical, emitter_id`, traceID)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}

func (s *Store) Window(ctx context.Context, q store.WindowQuery) ([]*v1.Event, error) {
	// Service filtering is applied after decoding: the set is small and this
	// avoids building a variadic IN clause for the walking skeleton.
	rows, err := s.db.QueryContext(ctx,
		`SELECT body FROM events WHERE hlc_wall_ms BETWEEN ? AND ?
		 ORDER BY hlc_wall_ms, hlc_logical, emitter_id`, q.FromWallMS, q.ToWallMS)
	if err != nil {
		return nil, err
	}
	all, err := scanEvents(rows)
	if err != nil {
		return nil, err
	}
	if len(q.Services) == 0 {
		if q.Limit > 0 && len(all) > q.Limit {
			all = all[:q.Limit]
		}
		return all, nil
	}
	want := map[string]bool{}
	for _, n := range q.Services {
		want[n] = true
	}
	var out []*v1.Event
	for _, e := range all {
		if want[e.GetService().GetName()] {
			out = append(out, e)
		}
		if q.Limit > 0 && len(out) == q.Limit {
			break
		}
	}
	return out, nil
}

func (s *Store) Gaps(ctx context.Context, from, to uint64) ([]*v1.Gap, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT body FROM gaps WHERE to_wall_ms >= ? AND from_wall_ms <= ?`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*v1.Gap
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var g v1.Gap
		if err := proto.Unmarshal(body, &g); err != nil {
			return nil, err
		}
		out = append(out, &g)
	}
	return out, rows.Err()
}

func (s *Store) Traces(ctx context.Context, q store.TraceQuery) ([]store.TraceSummary, error) {
	events, err := s.Window(ctx, store.WindowQuery{FromWallMS: q.FromWallMS, ToWallMS: q.ToWallMS})
	if err != nil {
		return nil, err
	}
	return store.SummarizeTraces(events, q), nil
}

func (s *Store) Close() error { return s.db.Close() }

func scanEvents(rows *sql.Rows) ([]*v1.Event, error) {
	defer rows.Close()
	var out []*v1.Event
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var e v1.Event
		if err := proto.Unmarshal(body, &e); err != nil {
			return nil, err
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}
```

- [ ] **Step 5: Extract the shared trace summarizer**

The in-memory store and SQLite both need identical `Traces` semantics. Move that logic into `store/summarize.go`:

```go
package store

import (
	"sort"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
)

// SummarizeTraces groups events into trace summaries. Both backends use this so
// their Traces results cannot diverge.
func SummarizeTraces(events []*v1.Event, q TraceQuery) []TraceSummary {
	byTrace := map[string]*TraceSummary{}
	for _, e := range events {
		if q.Service != "" && e.GetService().GetName() != q.Service {
			continue
		}
		w := e.GetHlc().GetWallMs()
		key := string(e.TraceId)
		sum, ok := byTrace[key]
		if !ok {
			sum = &TraceSummary{
				TraceID:     append([]byte(nil), e.TraceId...),
				RootService: e.GetService().GetName(),
				RootRoute:   e.GetHttpServer().GetRoute(),
				StartWallMS: w,
				EndWallMS:   w,
			}
			byTrace[key] = sum
		}
		sum.EventCount++
		if w < sum.StartWallMS {
			sum.StartWallMS = w
		}
		if w > sum.EndWallMS {
			sum.EndWallMS = w
		}
		if e.Status == v1.Status_ERROR || e.Status == v1.Status_TIMEOUT {
			sum.HasError = true
		}
	}

	out := make([]TraceSummary, 0, len(byTrace))
	for _, sum := range byTrace {
		if q.OnlyErrors && !sum.HasError {
			continue
		}
		out = append(out, *sum)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartWallMS < out[j].StartWallMS })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out
}
```

Replace the body of `memory.Events.Traces` with a `Window` call followed by `store.SummarizeTraces`.

- [ ] **Step 6: Write the blob filesystem store**

Create `store/blobfs/blobfs.go`:

```go
// Package blobfs is the default content-addressed BlobStore, backed by the
// local filesystem.
package blobfs

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// Store is a filesystem BlobStore. Objects are sharded by hash prefix so no
// single directory grows unbounded.
type Store struct{ dir string }

// Open opens or creates the store rooted at dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

// IDFor returns the opaque blob ID for a content hash.
func (s *Store) IDFor(sha [32]byte) string { return hex.EncodeToString(sha[:]) }

func (s *Store) path(id string) (string, error) {
	if len(id) < 4 {
		return "", errors.New("blobfs: malformed blob id")
	}
	// Reject anything that could escape the root.
	if filepath.Base(id) != id {
		return "", errors.New("blobfs: malformed blob id")
	}
	return filepath.Join(s.dir, id[:2], id[2:4], id), nil
}

func (s *Store) Put(_ context.Context, sha [32]byte, body []byte) (string, error) {
	id := s.IDFor(sha)
	p, err := s.path(id)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(p); err == nil {
		return id, nil // already stored: content-addressed dedup
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, p); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) Get(_ context.Context, id string) ([]byte, error) {
	p, err := s.path(id)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}

func (s *Store) Has(_ context.Context, shas [][32]byte) ([][32]byte, error) {
	var missing [][32]byte
	for _, sha := range shas {
		p, err := s.path(s.IDFor(sha))
		if err != nil {
			return nil, err
		}
		if _, err := os.Stat(p); err != nil {
			missing = append(missing, sha)
		}
	}
	return missing, nil
}

func (s *Store) Close() error { return nil }
```

- [ ] **Step 7: Run the conformance suite against both backends**

Run: `CGO_ENABLED=0 go test ./store/... -race -v`
Expected: PASS — the same seven subtests for memory, sqlite, and blobfs.

- [ ] **Step 8: Add backend selection to the collector**

Replace `cmd/mnemo/main.go`:

```go
// Command mnemo is the Mnemosyne collector.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ai-mond/mnemosyne/collector/api"
	"github.com/ai-mond/mnemosyne/store"
	"github.com/ai-mond/mnemosyne/store/blobfs"
	"github.com/ai-mond/mnemosyne/store/memory"
	"github.com/ai-mond/mnemosyne/store/sqlite"
)

func main() {
	addr := flag.String("addr", ":4318", "listen address")
	backend := flag.String("store", "sqlite", "storage backend: sqlite or memory")
	data := flag.String("data", "./mnemo-data", "data directory (sqlite only)")
	flag.Parse()

	es, bs, err := open(*backend, *data)
	if err != nil {
		log.Fatal(err)
	}
	defer es.Close()
	defer bs.Close()

	log.Printf("mnemo collector listening on %s (store=%s)", *addr, *backend)
	if err := http.ListenAndServe(*addr, api.New(es, bs)); err != nil {
		log.Fatal(err)
	}
}

func open(backend, dir string) (store.EventStore, store.BlobStore, error) {
	if backend == "memory" {
		return memory.NewEvents(), memory.NewBlobs(), nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	es, err := sqlite.Open(filepath.Join(dir, "events.db"))
	if err != nil {
		return nil, nil, err
	}
	bs, err := blobfs.Open(filepath.Join(dir, "blobs"))
	if err != nil {
		es.Close()
		return nil, nil, err
	}
	return es, bs, nil
}
```

- [ ] **Step 9: Verify persistence across a restart**

Run the collector with `-store sqlite -data ./mnemo-data`, record the acceptance scenario into it, stop it, restart it, and reload the UI.
Expected: the same replay renders after the restart.

- [ ] **Step 10: Commit**

```bash
git add store/ cmd/mnemo/
git commit -m "feat(store): add SQLite and filesystem backends passing conformance"
```

---

## Task 13: End-to-end acceptance test

**Objective:** Prove Spec 1 is done by asserting every mechanism the design claims, against durable storage, in one test.

**Deliverables:** `acceptance/acceptance_test.go`.

**Interfaces:** Consumes everything built so far. Produces nothing new — this task adds no production code.

**Invariants:** The assertion table from the frozen spec, in full. Each row maps to one subtest.

**Definition of done:** `CGO_ENABLED=0 go test ./acceptance/ -race -v` passes all seven subtests against `sqlite` + `blobfs`.

**Risks:** Writing the test against implementation behaviour rather than the spec's claims. Each subtest name is copied from the spec's table so a reviewer can check the mapping by eye. If a subtest is hard to write, that is information about the design, not a reason to weaken the assertion — amend the spec instead.

- [ ] **Step 1: Write the acceptance test**

Create `acceptance/acceptance_test.go`:

```go
// Package acceptance holds the Spec 1 definition of done. Every subtest maps to
// one row of the assertion table in the frozen design document.
package acceptance_test

import (
	"context"
	"path/filepath"
	"testing"

	v1 "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/ai-mond/mnemosyne/harness"
	"github.com/ai-mond/mnemosyne/replay"
	"github.com/ai-mond/mnemosyne/store"
	"github.com/ai-mond/mnemosyne/store/blobfs"
	"github.com/ai-mond/mnemosyne/store/sqlite"
)

// recording runs the acceptance scenario into durable storage and returns the
// stores plus every recorded event.
func recording(t *testing.T) (store.EventStore, store.BlobStore, []*v1.Event) {
	t.Helper()
	dir := t.TempDir()

	es, err := sqlite.Open(filepath.Join(dir, "events.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { es.Close() })

	bs, err := blobfs.Open(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatalf("blobfs.Open: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	// A sink that writes straight into the store, so the test exercises the
	// storage layer without depending on exporter timing.
	sink := &storeSink{t: t, es: es}
	h := harness.AcceptanceScenario(t, sink)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.Run("checkout-api", "/checkout"); err == nil {
			resp.Body.Close()
		}
	}()
	h.Close()
	<-done

	return es, bs, sink.events
}

type storeSink struct {
	t      *testing.T
	es     store.EventStore
	events []*v1.Event
}

func (s *storeSink) Emit(e *v1.Event) {
	s.events = append(s.events, e)
	if err := s.es.AppendEvents(context.Background(), []*v1.Event{e}); err != nil {
		s.t.Errorf("AppendEvents: %v", err)
	}
}

func TestRemoteCallEdgesSpanAllThreeProcesses(t *testing.T) {
	_, _, events := recording(t)

	services := map[string]bool{}
	var remote int
	for _, e := range events {
		services[e.GetService().GetName()] = true
		for _, l := range e.Links {
			if l.Type == v1.EdgeType_REMOTE_CALL {
				remote++
			}
		}
	}
	for _, want := range []string{"checkout-api", "inventory-svc", "pricing-svc"} {
		if !services[want] {
			t.Fatalf("no events from %s", want)
		}
	}
	if remote == 0 {
		t.Fatal("no REMOTE_CALL edges: propagation over traceparent/tracestate failed")
	}
}

func TestRetryAttemptsAreSiblingsUnderOneScope(t *testing.T) {
	_, _, events := recording(t)

	parents := map[uint64]bool{}
	var retryLinks int
	for _, e := range events {
		if e.Phase != v1.Phase_END {
			continue
		}
		for _, l := range e.Links {
			if l.Type != v1.EdgeType_RETRY_OF {
				continue
			}
			retryLinks++
			// A retry and the attempt it replaces must share a parent: neither
			// contains the other.
			parents[e.GetParent().GetSequence()] = true
		}
	}
	if retryLinks == 0 {
		t.Fatal("no RETRY_OF edges recorded")
	}
	if len(parents) != 1 {
		t.Fatalf("retry attempts have %d distinct parents, want 1 — they were nested, not siblings", len(parents))
	}
}

func TestHungCallAppearsAsUnmatchedBegin(t *testing.T) {
	es, bs, _ := recording(t)

	m, err := replay.New(es, bs).Timeline(context.Background(), replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1, Resolution: 100,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.InFlight) == 0 {
		t.Fatal("the hung operation was not reported as in-flight")
	}
}

func TestOrderHoldsUnderClockSkew(t *testing.T) {
	es, bs, _ := recording(t)

	m, err := replay.New(es, bs).Timeline(context.Background(), replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1, Resolution: 100,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	var found bool
	for _, s := range m.SkewWarnings {
		if s.Service == "pricing-svc" {
			found = true
		}
	}
	if !found {
		t.Fatal("pricing-svc runs 400ms behind but produced no skew warning")
	}

	// HLC order must still be monotonic across the whole recording despite the
	// skew.
	events, err := es.Window(context.Background(), store.WindowQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1,
	})
	if err != nil {
		t.Fatalf("Window: %v", err)
	}
	for i := 1; i < len(events); i++ {
		prev, cur := events[i-1].GetHlc(), events[i].GetHlc()
		if cur.WallMs < prev.WallMs {
			t.Fatalf("event %d has HLC %v, out of order after %v", i, cur, prev)
		}
	}
}

func TestGapsAreRecordedNotSilent(t *testing.T) {
	es, bs, _ := recording(t)
	ctx := context.Background()

	// Simulate the collector being unavailable during the incident.
	if err := es.AppendGaps(ctx, []*v1.Gap{{
		EmitterId: []byte("emitter0"),
		From:      &v1.HLC{WallMs: 1}, To: &v1.HLC{WallMs: 1 << 40},
		DroppedCount: 12, Reason: v1.GapReason_UNREACHABLE,
	}}); err != nil {
		t.Fatalf("AppendGaps: %v", err)
	}

	m, err := replay.New(es, bs).Timeline(ctx, replay.TimelineQuery{
		FromWallMS: 0, ToWallMS: ^uint64(0) >> 1, Resolution: 100,
	})
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(m.Gaps) == 0 {
		t.Fatal("dropped events produced no gap marker: the timeline would show silence")
	}
	if m.Gaps[0].DroppedCount != 12 {
		t.Fatalf("gap reports %d drops, want 12", m.Gaps[0].DroppedCount)
	}
}

func TestChainReadsAsTheIncidentNarrative(t *testing.T) {
	es, bs, events := recording(t)
	ctx := context.Background()

	// Start from the hung operation and walk back to the root.
	var hung *v1.EventID
	ends := map[uint64]bool{}
	for _, e := range events {
		if e.Phase == v1.Phase_END {
			ends[e.Id.Sequence] = true
		}
	}
	for _, e := range events {
		if e.Phase == v1.Phase_BEGIN && !ends[e.Id.Sequence] {
			hung = e.Id
			break
		}
	}
	if hung == nil {
		t.Fatal("no unmatched BEGIN to start the chain from")
	}

	chain, err := replay.New(es, bs).Chain(ctx, events[0].TraceId, hung, 200)
	if err != nil {
		t.Fatalf("Chain: %v", err)
	}
	if len(chain.Nodes) < 2 {
		t.Fatalf("chain has %d nodes; the causal path back to the root is missing", len(chain.Nodes))
	}
}

func TestRecordingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")

	es, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sink := &storeSink{t: t, es: es}
	h := harness.AcceptanceScenario(t, sink)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.Run("checkout-api", "/checkout"); err == nil {
			resp.Body.Close()
		}
	}()
	h.Close()
	<-done
	traceID := sink.events[0].TraceId
	if err := es.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.TraceEvents(context.Background(), traceID)
	if err != nil {
		t.Fatalf("TraceEvents: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("recording did not survive a restart")
	}
}
```

- [ ] **Step 2: Run the acceptance suite**

Run: `CGO_ENABLED=0 go test ./acceptance/ -race -v`
Expected: PASS — all seven tests.

- [ ] **Step 3: Run everything**

Run: `CGO_ENABLED=0 go test ./... -race`
Expected: PASS across every package.

- [ ] **Step 4: Verify the whole thing by hand one last time**

Run the collector with SQLite, record the scenario, open the UI. Confirm the four spec-required states are visible: `never ended`, the gap marker, the skew warning, and the recorded causal chain.

- [ ] **Step 5: Commit**

```bash
git add acceptance/
git commit -m "test: add Spec 1 end-to-end acceptance suite"
```

---

---

## Task 14: Schema drift guard

> **Execution order:** run this immediately after Task 1. It is numbered 14 only because the plan's task numbers are stable identifiers, not an ordering.

**Objective:** A committed golden descriptor fails the build the moment a `.proto` field number, enum value, or reserved range changes — so the frozen schema is enforced by a test rather than by reviewer attention.

**Why this exists:** Task 1's round-trip test cannot catch this class of bug. Regenerating after a field-number change produces marshal *and* unmarshal code using the new numbers, so a self-consistent renumbering round-trips perfectly and the test still passes. The only thing that detects it is comparing the schema's descriptor against a recorded baseline. The design document declares this wire format frozen; nothing currently enforces that claim.

**Deliverables:** `schema/testdata/descriptor.golden.binpb`, `schema/descriptor_test.go`, a `schema-golden` Makefile target.

**Interfaces:**
- Consumes: the generated bindings registered in `protoregistry.GlobalFiles` by importing `schema/gen/go/mnemosyne/v1`.
- Produces: no exported Go API. A `-update` test flag regenerates the golden.

**Invariants:**
1. The golden is built from the descriptors embedded in the *generated Go code*, not by invoking `protoc`. The test must pass with no `protoc` on PATH and with `CGO_ENABLED=0`.
2. Serialization is deterministic — the same schema always produces identical golden bytes, regardless of map iteration order.
3. The four files are emitted in a stable, sorted order.
4. A field-number change, an enum-value change, or a change to `EdgeType`'s reserved range fails the test.
5. A comment-only or formatting-only `.proto` edit does **not** fail the test. `protodesc` omits source-code info, so this holds by construction — but it is asserted, because a guard that cries wolf gets deleted.
6. On failure the test prints a readable structural diff, not a byte offset.

**Tests:** The golden comparison itself, an assertion that regenerating twice is byte-stable, and a focused assertion naming the specific invariants the design calls out (`Event` field numbers, `EdgeType` reserving 5–15) so a failure says what broke.

**Definition of done:** `CGO_ENABLED=0 go test ./schema/` passes; transposing two `HttpClient` field numbers, regenerating bindings, and re-running makes it **fail with a message naming the changed field** — verified and then reverted.

**Risks:** The obvious failure mode is a guard that cannot actually fail. Task 1 shipped a mutation check that proved nothing because both sides of the round trip changed together; do not repeat that pattern. The mutation check below is the deliverable's real acceptance criterion, and it must be performed against the regenerated bindings. The second risk is a golden so noisy that unrelated protobuf-library upgrades break it — mitigated by comparing `FileDescriptorProto` structure, which is stable across generator versions, rather than raw `rawDesc` bytes.

- [ ] **Step 1: Write the failing test**

Create `schema/descriptor_test.go`:

```go
package schema_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/ai-mond/mnemosyne/schema/gen/go/mnemosyne/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/descriptorpb"
)

var update = flag.Bool("update", false, "rewrite the golden descriptor")

const goldenPath = "testdata/descriptor.golden.binpb"

// schemaFiles are the frozen files this guard covers.
var schemaFiles = []string{
	"mnemosyne/v1/common.proto",
	"mnemosyne/v1/event.proto",
	"mnemosyne/v1/gap.proto",
	"mnemosyne/v1/ingest.proto",
}

// currentDescriptorSet builds a FileDescriptorSet from the descriptors embedded
// in the generated Go code. protoc is deliberately not involved: this must pass
// on a machine that has never installed it.
//
// protodesc omits source-code info, so comments and formatting do not
// participate — only structure: field numbers, types, enum values, and
// reserved ranges.
func currentDescriptorSet(t *testing.T) *descriptorpb.FileDescriptorSet {
	t.Helper()
	set := &descriptorpb.FileDescriptorSet{}
	for _, path := range schemaFiles { // fixed order, so output is stable
		fd, err := protoregistry.GlobalFiles.FindFileByPath(path)
		if err != nil {
			t.Fatalf("descriptor for %s not registered: %v", path, err)
		}
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}
	return set
}

func marshalGolden(t *testing.T, set *descriptorpb.FileDescriptorSet) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(set)
	if err != nil {
		t.Fatalf("marshal descriptor set: %v", err)
	}
	return b
}

func TestSchemaHasNotDrifted(t *testing.T) {
	current := currentDescriptorSet(t)
	got := marshalGolden(t, current)

	if *update {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("create testdata dir: %v", err)
		}
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("golden updated (%d bytes) — commit it only alongside an intentional schema change", len(got))
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (regenerate with: go test ./schema/ -update): %v", err)
	}
	if string(got) == string(want) {
		return
	}

	var previous descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(want, &previous); err != nil {
		t.Fatalf("golden is corrupt: %v", err)
	}
	t.Fatalf("the frozen schema changed:\n%s\n\n"+
		"The wire format is frozen. If this change is intentional and "+
		"backward compatible, regenerate with: go test ./schema/ -update",
		cmp.Diff(&previous, current, protocmp.Transform()))
}

// TestGoldenIsDeterministic guards the guard: a golden that is not byte-stable
// would fail at random and be deleted within a week.
func TestGoldenIsDeterministic(t *testing.T) {
	first := marshalGolden(t, currentDescriptorSet(t))
	for i := 0; i < 10; i++ {
		if got := marshalGolden(t, currentDescriptorSet(t)); string(got) != string(first) {
			t.Fatalf("iteration %d produced different bytes; the golden is not deterministic", i)
		}
	}
}

// TestFrozenInvariantsAreNamed asserts the specific guarantees the design
// document calls out, so a failure says which promise broke rather than only
// that some bytes differ.
func TestFrozenInvariantsAreNamed(t *testing.T) {
	fd, err := protoregistry.GlobalFiles.FindFileByPath("mnemosyne/v1/event.proto")
	if err != nil {
		t.Fatalf("event.proto not registered: %v", err)
	}

	t.Run("event field numbers", func(t *testing.T) {
		msg := fd.Messages().ByName("Event")
		if msg == nil {
			t.Fatal("message Event not found")
		}
		want := map[string]protoreflect.FieldNumber{
			"schema_version": 1, "trace_id": 2, "id": 3, "parent": 4, "span_id": 5,
			"service": 6, "hlc": 7, "wall_unix_nanos": 8, "duration_nanos": 9,
			"phase": 10, "operation_type": 11, "http_server": 12, "http_client": 13,
			"status": 14, "error": 15, "links": 16, "request": 17, "response": 18,
			"completeness": 19, "attributes": 20, "attributes_truncated": 21,
		}
		for name, num := range want {
			f := msg.Fields().ByName(protoreflect.Name(name))
			if f == nil {
				t.Errorf("Event.%s is missing from the frozen schema", name)
				continue
			}
			if f.Number() != num {
				t.Errorf("Event.%s has field number %d, frozen at %d", name, f.Number(), num)
			}
		}
	})

	t.Run("edge type reserves 5 to 15", func(t *testing.T) {
		enum := fd.Enums().ByName("EdgeType")
		if enum == nil {
			t.Fatal("enum EdgeType not found")
		}
		for v := int32(5); v <= 15; v++ {
			if got := enum.Values().ByNumber(protoreflect.EnumNumber(v)); got != nil {
				t.Errorf("EdgeType %d is reserved for future graph relationships but is named %q", v, got.Name())
			}
		}
	})
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./schema/ -run TestSchemaHasNotDrifted`
Expected: FAIL — `read golden (regenerate with: go test ./schema/ -update)`, because `testdata/descriptor.golden.binpb` does not exist yet.

- [ ] **Step 3: Generate the golden**

```bash
go test ./schema/ -update
```

Expected: PASS, and `schema/testdata/descriptor.golden.binpb` now exists.

- [ ] **Step 4: Run the full schema suite**

Run: `CGO_ENABLED=0 go test ./schema/ -v`
Expected: PASS — `TestSchemaHasNotDrifted`, `TestGoldenIsDeterministic`, `TestFrozenInvariantsAreNamed` and its two subtests, plus Task 1's round-trip tests. Output pristine.

- [ ] **Step 5: Prove the guard actually fires**

This step is the deliverable's real acceptance criterion. A guard that cannot fail is worse than none, because it manufactures confidence.

1. Edit `schema/proto/mnemosyne/v1/event.proto` and swap two `HttpClient` field numbers: make `host = 3` and `path = 2`.
2. Regenerate the bindings with the project's protoc invocation.
3. Run `go test ./schema/ -run TestSchemaHasNotDrifted`.

Expected: **FAIL**, with a diff naming `host` and `path` and their changed numbers. Capture that output — it is the evidence this task exists to produce.

4. Revert the `.proto` change, regenerate again, and confirm the suite passes.
5. Run `git status` and `git diff` and confirm no `.proto` or `.pb.go` file is left modified.

- [ ] **Step 6: Prove the guard does not cry wolf**

1. Add a comment line to `schema/proto/mnemosyne/v1/gap.proto`.
2. Regenerate.
3. Run `go test ./schema/`.

Expected: **PASS** — `protodesc` omits source-code info, so comments are invisible to the guard. A guard that fired on comment edits would be disabled by the first person it annoyed.

4. Revert the comment and regenerate. Confirm `git status` is clean of `.proto`/`.pb.go` changes.

- [ ] **Step 7: Add the Makefile target**

Add to `Makefile`:

```make
schema-golden:
	go test ./schema/ -update
```

and add `schema-golden` to the `.PHONY` list.

- [ ] **Step 8: Commit**

```bash
git add schema/descriptor_test.go schema/testdata/descriptor.golden.binpb Makefile go.mod go.sum
git commit -m "test(schema): guard frozen wire format with a golden descriptor"
```

## Spec coverage

Mapping from the frozen design to the task that implements it, so gaps are visible:

| Spec section | Task |
|---|---|
| Recording schema, versioning invariant | 1 |
| Schema drift guard (golden descriptor) | 14 |
| Ordering and clocks, skew detection | 2, 10 |
| Identity, foreign identity rule | 3, 6 |
| Causal model, scope invariant, DAG edges | 4, 9 |
| Capture policy, ring buffer, redaction, triggers | 5 |
| Propagation, backward trigger propagation | 6 |
| Storage: EventStore + BlobStore split, content addressing | 7, 12 |
| Loss semantics, gap records | 8, 10, 13 |
| Ingest, backpressure | 8 |
| Replay engine, Timeline Model, bounded bucketing | 10 |
| Query API, opaque `blob_id` | 10 |
| UI: split view, in-flight, gaps, skew, recorded-vs-inferred | 11 |
| Testing: conformance, property tests, harness, acceptance | 1, 2, 3, 4, 5, 7, 9, 13 |

**Deferred within Spec 1, per `implementation-order.md`** — none of these change what replay observes, so they are built after the first replay renders: capture-policy matching beyond the hardcoded default, BEGIN elision, backpressure shedding hints, and blob dedup via `HasBlobs` on the upload path.

**Not implemented, matching the spec's own non-goals:** PostgreSQL/Redis/Kafka adapters, inference engine, hermetic re-execution, ClickHouse, OpenTelemetry export, gRPC, goroutine tracking, auth, retention and compaction.
