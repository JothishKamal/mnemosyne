# Mnemosyne — Memory for Distributed Systems

**Design spec · 2026-07-23 · v1 (walking skeleton) · Status: ACCEPTED — design frozen**

From this point: implementation bugs are fixed immediately; invariants discovered during coding amend this spec; anything else becomes a future spec.

---

## Philosophy

> **Mnemosyne records execution, not interpretation.**
>
> Every event and every causal edge in the recording format corresponds to something directly observed during execution. Higher-level explanations, inferred dependencies, and root-cause analyses are layered on top of this immutable execution graph and never modify it.

Two corollaries that decide most arguments downstream:

- **Events are first-class. Spans are compatibility.** The internal model is an event graph. OpenTelemetry spans are an export format, never a source of truth.
- **Parent edges express scope containment. Cross-process causality uses link edges.** This is the core modelling decision and the reason Mnemosyne is not another tracing tool.

---

## What it does

Records every instrumented operation across a distributed system — requests, database queries, cache operations, message publishes and consumes — with enough fidelity and causal structure to reconstruct exactly what happened during a production incident, and step through it.

v1 **reconstructs**; nothing re-executes. The recording format is designed from day one so that hermetic single-service re-execution can be layered on later without re-instrumenting anything.

---

## Scope

### Roadmap

| Version | Contents |
|---|---|
| **V1** | Go SDK, HTTP middleware, PostgreSQL wrapper, Redis wrapper, Kafka wrapper, event collector, replay UI |
| V2 | gRPC interceptors, OpenTelemetry bridge, goroutine tracking, context propagation improvements, hermetic re-execution, collector-initiated retro-fetch |
| V3 | Node SDK |
| V4 | Python SDK |
| V5 | JVM java agent |

### V1 is sliced into three specs

**This document specifies Spec 1 only — the walking skeleton.** One thin vertical slice, end to end:

recording schema + causal model + Go core recorder + **HTTP instrumentation only** + collector with embedded store + replay UI rendering one real cross-service causal chain.

- **Spec 2** thickens instrumentation: PostgreSQL, Redis, Kafka adapters.
- **Spec 3** thickens the UI and query surface.

Rationale for the vertical slice: the schema gets validated against real end-to-end use *before* three more integrations are built on top of it.

### Non-goals for Spec 1

PostgreSQL / Redis / Kafka adapters · inference engine · hermetic re-execution · ClickHouse store · OpenTelemetry export · gRPC · goroutine tracking · authentication and multi-tenancy · retention and compaction.

---

## Architecture

### Package layout

```
mnemosyne/
├── schema/
│   ├── proto/mnemosyne/v1/    event, hlc, capture, trace, ingest
│   ├── semconv/               semantic-convention registry for attributes
│   ├── testdata/              golden recordings (language-neutral conformance suite)
│   └── gen/go/                generated Go bindings
├── sdk/
│   ├── recorder.go            Recorder, Handle
│   ├── identity.go            ID generation (SDK owns identity)
│   ├── hlc.go                 hybrid logical clock
│   ├── builder.go             EventBuilder
│   ├── capture.go             CaptureEngine: policy resolution + trigger evaluation
│   ├── buffer.go              per-trace payload ring buffer
│   ├── exporter.go            batching client
│   ├── mnemoctx/              Mnemosyne context (rides inside context.Context)
│   └── instrument/
│       └── nethttp/           package mnemohttp — server.go, client.go
├── store/
│   ├── store.go               EventStore + BlobStore interfaces
│   ├── sqlite/                default EventStore
│   └── blobfs/                default BlobStore (content-addressed)
├── replay/                    replay engine → Timeline Model
├── collector/
│   ├── ingest/
│   ├── query/
│   └── api/
├── cmd/mnemo/
└── ui/
```

### Dependency graph

```text
              schema
                 ▲
        ┌────────┼────────┬─────────┐
        │        │        │         │
       sdk     store   replay   collector
        ▲                 ▲         ▲
        │                 └────┬────┘
 ┌──────┼──────┬──────┐        │
 │      │      │      │       cmd/mnemo ── ui
HTTP   PG    Redis  Kafka
      └──── Spec 2 ─────┘
```

The arrow direction is a hard rule: **`sdk/` never imports a specific library.** Integrations are thin adapters over the `Recorder` interface. This is what keeps the core small and makes V3–V5 tractable.

### Naming conventions

- Package prefix `mnemo*` (`mnemohttp`, `mnemopgx`, `mnemogrpc`). Adapter directories are named after the library (`instrument/nethttp`, `instrument/grpc`) but package names are prefixed to avoid shadowing the library being instrumented — the same reason OpenTelemetry ships `otelhttp` rather than `http`.
- CLI binary: `mnemo`.
- Schema namespace: `mnemosyne.v1`.

### Deployment shape

The SDK talks to the collector **directly** — batched, locally buffered, dropping under backpressure. No sidecar agent. The collector is a single binary containing ingest, query, API, and the served UI; `store/` and `replay/` are top-level packages so `cmd/mnemo` can consume them without running a collector at all (enabling a future `mnemo replay` CLI against a local recording).

### Data flow for one recorded request

```
inbound HTTP ──▶ mnemohttp server middleware
                 ├─ parse traceparent + tracestate (or mint a new trace)
                 ├─ tick HLC; allocate EventID
                 └─ push scope into mnemoctx; emit BEGIN
                          │
   handler work ──────────┤ each instrumented op: BeginOperation → Handle → End()
                          │ parent = enclosing scope (siblings stay siblings)
                          ▼
outbound HTTP ──▶ mnemohttp RoundTripper: inject traceparent + tracestate

     metadata events ────────────────▶ exporter queue ──▶ collector
     payloads ──▶ per-trace ring buffer (bounded, redacted on the way in)
                          │
              trigger fires (error / latency / explicit flag)
                          └─▶ retroactively flush that trace's payloads
                          └─▶ set capture flag on the response (backward propagation)

collector ──▶ EventStore (sqlite index) + BlobStore (content-addressed blobs)
replay/   ──▶ Timeline Model ──▶ query API ──▶ ui
```

---

## Recording schema

The schema **is** the protocol. Everything speaks it; it is versioned and language-neutral.

### Core types

```proto
syntax = "proto3";
package mnemosyne.v1;

message EventID {
  bytes  emitter_id = 1;  // 8 bytes, random at process start
  uint64 sequence   = 2;  // monotonic within emitter
}

message HLC {
  uint64 wall_ms = 1;
  uint32 logical = 2;
}

enum Phase        { PHASE_UNSPECIFIED=0; BEGIN=1; END=2; INSTANT=3; }
enum Status       { STATUS_UNSPECIFIED=0; OK=1; ERROR=2; TIMEOUT=3; CANCELED=4; }
enum Completeness { COMPLETENESS_UNSPECIFIED=0; FULL=1; TRUNCATED=2; METADATA_ONLY=3; }

enum OperationType {
  OPERATION_TYPE_UNSPECIFIED = 0;
  HTTP_SERVER = 1;
  HTTP_CLIENT = 2;
  LOCAL       = 3;
  DB_QUERY    = 10;  // Spec 2
  CACHE_OP    = 20;  // Spec 2
  MSG_PRODUCE = 30;  // Spec 2
  MSG_CONSUME = 31;  // Spec 2
}

enum EdgeType {
  EDGE_TYPE_UNSPECIFIED = 0;
  PARENT      = 1;
  REMOTE_CALL = 2;
  MSG_FLOW    = 3;
  RETRY_OF    = 4;
  reserved 5 to 15;   // future graph relationships, e.g. FORKS_TO / JOINS_FROM
}

message PayloadRef {
  bytes  sha256    = 1;
  uint64 size      = 2;
  bool   truncated = 3;
  bool   redacted  = 4;
  bool   stored    = 5;  // false → hash and size recorded, body not retained
}

message Link {
  EdgeType type            = 1;
  bytes    trace_id        = 2;
  EventID  event           = 3;
  bytes    foreign_span_id = 4;  // set when linking to a non-Mnemosyne participant
}

message Event {
  uint32  schema_version   = 1;

  bytes   trace_id         = 2;  // 16 bytes, W3C-compatible
  EventID id               = 3;
  EventID parent           = 4;  // scope containment; unset at trace root
  bytes   span_id          = 5;  // 8 bytes; assigned only when crossing a process boundary

  Service service          = 6;
  HLC     hlc              = 7;
  int64   wall_unix_nanos  = 8;  // display only, never used for ordering
  int64   duration_nanos   = 9;  // set on END

  Phase         phase          = 10;
  OperationType operation_type = 11;
  oneof operation {
    HttpServer http_server = 12;
    HttpClient http_client = 13;
  }

  Status    status = 14;
  ErrorInfo error  = 15;

  repeated Link links      = 16;
  PayloadRef   request     = 17;
  PayloadRef   response    = 18;
  Completeness completeness = 19;

  map<string, Value> attributes = 20;
  bool attributes_truncated     = 21;
}

enum GapReason {
  GAP_REASON_UNSPECIFIED = 0;
  BACKPRESSURE  = 1;  // exporter queue full
  RING_EVICTION = 2;  // ring buffer bound exceeded before flush
  UNREACHABLE   = 3;  // collector unavailable
  ATTR_OVERFLOW = 4;  // reserved; attribute truncation is flagged on the event itself
}

message Gap {
  bytes     emitter_id    = 1;
  HLC       from          = 2;
  HLC       to            = 3;
  uint64    dropped_count = 4;
  GapReason reason        = 5;
  bytes     trace_id      = 6;  // set when the drop is attributable to one trace
}
```

`Gap` is a first-class record in the ingest stream, not an `Event` — it describes the *absence* of events and so cannot carry an `EventID`, a parent, or a phase. The `EventStore` indexes gaps by `(emitter_id, hlc window)` so the replay engine can attribute them to a time range even when no trace is known.

### Versioning invariant

> **Replay engines may consume older schema versions. An SDK never emits multiple schema versions concurrently.**

A single process emits exactly one `schema_version` for its entire lifetime. Version heterogeneity exists across a fleet during rollout, never within one emitter — which means the replay engine's compatibility surface is per-event, and an emitter's stream is never internally mixed.

### Identity

The SDK owns identity generation. No adapter ever mints an ID.

- `trace_id` — 16 bytes, W3C-compatible. Imported from an inbound `traceparent` when present.
- `span_id` — 8 bytes, W3C-compatible. **Assigned only to events crossing a process boundary**, which is all W3C needs and makes the future OTel export a clean 1:1 mapping.
- `EventID = (emitter_id, sequence)` — monotonic, compact, sortable, allocation-free. "Emitter" deliberately avoids committing to pod / process / container / VM semantics; it means *whoever produced this event*.

**Foreign identity rule.** Inbound `trace_id` / `span_id` from a non-Mnemosyne service are imported as *foreign correlation identity* — they stitch us to the external trace. They never define our causal graph. A foreign span becomes a leaf we attach to, never a parent that dictates structure.

### Payload refs

`sha256` and `size` are recorded **even when the body is not stored** (`stored = false`). Two consequences:

1. The UI can state "payload identical to request #1842" without either payload being retained.
2. `BlobStore` is **content-addressed by sha256**, so identical payloads are stored once at rest, and the SDK can ask the collector `HasBlobs([]sha256) → missing[]` and upload only what's new. Deduplication is a storage property, not a UI trick.

### Attributes

`map<string, Value>` exists so the core schema can stay frozen while ecosystem-specific data (`grpc.method`, `sql.database`, `tenant`, `region`, `build_sha`) evolves. Three constraints keep it from becoming the dumping ground that kills tracing systems:

1. Hard caps on key count and total size, enforced in the SDK. Excess is dropped and `attributes_truncated` is set.
2. Known keys live in `schema/semconv/`, reusing OpenTelemetry's names where they exist.
3. **Attributes never participate in causality or ordering.** They are payload, not protocol. Anything the replay engine must reason about earns a real field.

### Phase — and why it matters

`BEGIN` is emitted eagerly and is metadata-only. `END` references the same `EventID` and carries outcome, duration, and payload refs. `INSTANT` is a single record for operations with no meaningful duration.

Without this, an operation that never ends is never recorded — and during an incident, the single most important event (the deadlocked query, the hung RPC, the request killed by OOM) would be invisible, with the timeline going silent exactly where the answer is. **An unmatched `BEGIN` renders as "in flight, never completed."**

Cost is 2× event volume for durational operations, mitigated by eliding `BEGIN` from the outgoing batch when `END` lands in the same flush window. Fast operations collapse back to one record; only operations that outlive a flush pay double — which are precisely the ones worth paying for.

**BEGIN elision is an exporter optimization and must be observationally equivalent to transmitting both records.** The schema, the store, and the replay engine all behave as though `BEGIN` and `END` always exist. An exporter is permitted to compress them; it is not permitted to change what replay observes. This keeps replay semantics independent of batching strategy, so the flush-window policy can be retuned freely without touching the protocol.

### Completeness

Exists solely for V2 hermetic re-execution. It lets a future replay engine **refuse** to re-execute a trace it cannot faithfully feed — "cannot execute: payload incomplete" — instead of silently producing a wrong answer.

---

## Causal model

Causality is a **DAG**, not a tree.

| Edge | Meaning | Why it cannot be a parent edge |
|---|---|---|
| `PARENT` | scope containment within one process | — |
| `REMOTE_CALL` | client event → server event in another process | — |
| `MSG_FLOW` | produce → consume | The consume event's parent is the consumer's poll loop. The producer may be in a different trace, hours earlier. |
| `RETRY_OF` | attempt N → attempt N−1 | Both attempts are siblings under one scope. Neither contains the other. |

`RETRY_OF` is what makes a retry storm a **traversal**, not an inference — you walk the edges.

### Scope invariant

> The context's current event changes only inside an open `Handle`'s extent. Recording an event never changes it as a side effect.

`BeginOperation` pushes a scope; `Handle.End()` pops it. Operations recorded within a scope are siblings sharing a parent.

Violating this degenerates the graph from a tree into a linked list: a database query and a cache read under one handler become parent-and-child rather than siblings, which asserts *"the query caused the cache read"* when the truth is *"they ran concurrently under the same request."* During an incident that distinction is the whole answer.

### Recorded edges only in v1

Inference — "the retry storm caused the database overload" — is a statistical claim over concurrent load, not an observation. Rendering inferred and recorded edges as the same arrow would make the tool lie to an operator mid-incident. v1 ships facts only. The inference engine gets its own spec, developed against real recorded data.

---

## Ordering and clocks

Hybrid logical clocks. On a local event: `wall = max(physical_now, last_wall)`, incrementing `logical` on ties. On receiving a remote HLC: `wall = max(physical_now, last_wall, remote_wall)`.

Guarantee: **if A happened-before B, then `hlc(A) < hlc(B)`** — with no clock synchronization required. Total order is `(hlc, emitter_id)`.

Wall clock is display-only. This is not cosmetic: "10:34:14" across five machines with 200ms of NTP skew is otherwise a lie.

**Skew detection falls out for free.** When a child event's local wall clock reads earlier than its parent's, that machine's clock is provably skewed. The UI shows a skew warning rather than rendering a physically impossible timeline.

**HLC is never shown in the narrative view.** The UI says "occurred after inventory request (+3 ms logical)", never `123:7`. It remains visible in the raw event inspector — otherwise, the first time the tool orders two events in a way an engineer disagrees with, there is no way to find out why.

---

## Propagation

W3C-compatible, so Mnemosyne interoperates with existing tracing and V2's OTel bridge is an export adapter rather than a rewrite.

- `traceparent` — standard. Carries `trace_id` and `span_id`.
- `tracestate` — one entry, `mnemo=<version>-<hlc_wall>-<hlc_logical>-<emitter_id>-<sequence>`, base36/base32 encoded. Must stay well under the 256-character per-entry limit, which is why `EventID` is compact by construction.
- `mnemosyne-capture: 1` — **response** header, for backward trigger propagation (below).

---

## Capture policy, triggers, loss

### Policy

Resolves to `off | metadata | full` per operation, matched by operation type and pattern. Default is `metadata`. Declarative config, loaded at startup; hot reload is V2.

The pattern grammar is defined now for all operation types the roadmap will need — route, SQL shape, cache key prefix, topic — so Spec 2 adapters plug in without a config-format change. **Spec 1 implements route matching only**; the other matchers parse and validate but have no operations to match against yet.

```yaml
capture:
  default: metadata
  rules:
    - match: { operation_type: HTTP_SERVER, route: "/checkout" }
      level: full
  triggers:
    - on: error
    - on: latency
      exceeds: 500ms
    - on: flag
  ring:
    max_bytes_per_trace: 1MB
    max_traces: 1000
  redact:
    - match: { operation_type: HTTP_SERVER }
      headers: [authorization, cookie, set-cookie]
      body_json_paths: ["$.password", "$.ssn"]
```

### Ring buffer

Per-trace, bounded by both `max_bytes_per_trace` and `max_traces`, LRU eviction. **Redaction is applied on the way in.** This is a security property, not an optimization: unredacted payloads never leave the process.

### Triggers

Evaluated at each `End()` and at trace root completion: `error` (any non-OK status), `latency` (operation or root exceeds threshold), `flag` (explicit `mnemoctx.MarkForCapture(ctx)`).

Firing flushes that trace's entire ring contents **retroactively** — you get the payloads from before anyone knew the trace was interesting. That is the entire point of the design.

### Backward trigger propagation — and its known limit

Triggers are per-process. If service A's request looks fine but service D four hops down times out, A's ring buffer never flushes — yet A's payload is often exactly what explains why D was called that way.

v1 propagates the trigger **backward along the response path**: a triggered response sets `mnemosyne-capture: 1`, and each caller up the chain flushes on seeing it. This covers synchronous call chains, which is the large majority of the graph.

**It does not work across Kafka**, where there is no response path. Those traces require collector-initiated retro-fetch over a control channel to the SDK. Deferred to V2 and documented as a v1 limitation rather than papered over.

**Backward trigger propagation is best-effort. A trace may contain a mix of flushed and metadata-only payloads when the triggering event crosses an asynchronous boundary.** The replay engine must expect partial payload coverage within a single trace and render it as such — per-event `completeness`, not a trace-level assumption.

### Backpressure

The collector acks each batch with a shed hint; the SDK drops lowest-value events first. One absolute invariant: **the SDK never blocks the application thread and never grows unbounded.** Recording is always what gets sacrificed.

### Loss semantics

Every drop — backpressure, ring eviction, collector unreachable — emits a **gap record**: emitter, HLC window, count, reason. Traces carry `gaps[]`; the replay engine renders "N events dropped here" inline on the timeline.

An incomplete recording that admits it beats a complete-looking recording that lies. A silent hole is indistinguishable from "nothing happened," which is the single most dangerous failure mode this tool can have.

---

## Storage

Two interfaces, deliberately separate because they are fundamentally different problems:

```go
type EventStore interface { /* append, query by trace / time window / service / status */ }
type BlobStore  interface { /* content-addressed get / put / has by sha256 */ }
```

Defaults: SQLite for the event index, local filesystem for blobs. Swapping in ClickHouse and S3 touches neither the SDK nor the UI. This keeps the getting-started path to a single `docker run` while leaving the scale path open.

---

## Replay engine

Server-side Go package. Reads `EventStore` + `BlobStore`, produces the **Timeline Model** — the only thing the UI ever sees. Both the UI and a future `mnemo replay` CLI consume it, so there is no logic to duplicate and it is unit-testable with no browser involved.

Timeline Model: `Lanes[]` (bucketed aggregates per service/resource) · `Chain` (recorded causal chain for a selected event) · `InFlight[]` (unmatched BEGINs) · `Gaps[]` · `SkewWarnings[]`.

Four algorithms:

1. **Join** `BEGIN`/`END` by `EventID`; unmatched `BEGIN` → in-flight.
2. **Order** by `(hlc, emitter_id)`.
3. **Chain walk** — bounded BFS with explicit truncation markers. Never unbounded; incidents are large.
4. **Lane bucketing** — server-side aggregation to a target resolution.

Bucketing is the reason the engine is server-side. An incident with 1,200 Redis timeouts must arrive as one aggregated band with a count, not 1,200 objects. **Response size is a function of screen resolution, not incident size** — which is what makes a genuinely bad outage viewable at all.

### Query API

| Endpoint | Returns |
|---|---|
| `GET /api/timeline?from&to&services[]&resolution` | Timeline Model lanes |
| `GET /api/chain?event_id` | Recorded causal chain |
| `GET /api/traces?from&to&service&status` | Trace search |
| `GET /api/event/{id}` | Raw event, attributes, HLC (debug pane) |
| `GET /api/blob/{blob_id}` | Payload |

`blob_id` is an **opaque logical handle** minted by the collector and surfaced through the query API. `sha256` remains content metadata — it drives deduplication, the `HasBlobs` upload check, and the "payload identical to request #1842" claim — but it is not the public address. This decouples the API from storage layout, so encryption, compression, sharding, or a hash-algorithm migration never breaks a client. In v1 the collector implements `blob_id` as the sha256 hex string; the contract is that clients must not rely on that.

---

## UI

**Split view, linked.** Swimlane timeline answering *when and how much*; narrative causal chain panel answering *what caused what*, driven by the current selection.

The two halves fail at exactly what the other is good at, and a real incident needs both: you must see 1,200 Redis timeouts in aggregate *and* follow one request through to the deadlock.

Required in v1 regardless of layout refinement:

- **`never ended`** marker on unmatched `BEGIN` events.
- **Dropped-events warning** rendered inline where the gap occurred.
- **Clock-skew warning** in place of an impossible timeline.
- **Recorded-vs-inferred distinction** in the visual language, established now even though v1 renders only recorded edges.
- HLC absent from the narrative view, present in the raw event inspector.

**Frontend implementation constraint:** UI work uses the `ui-ux-pro-max` skill for visual design, component styling, and interaction patterns.

---

## Testing

**1. Schema conformance suite.** Golden recordings in `schema/testdata/`, language-neutral. Every future SDK (Node, Python, JVM) must reproduce them. This is what stops V3–V5 from drifting.

**2. Property tests on the core.** HLC never violates happens-before under randomized interleavings · redaction never emits a denied key, asserted over output *bytes* rather than structs · ring buffer eviction respects both bounds.

**3. Deterministic incident harness.** N in-process fake services, virtual clock, injectable faults, real SDK, real collector, in-memory store, assertions on the resulting Timeline Model. The UI is developed against this rather than against a production outage, and the fixtures double as demo data.

**4. End-to-end acceptance test — the definition of done for Spec 1.**

HTTP-only, because PostgreSQL / Redis / Kafka are Spec 2: `checkout-api → inventory-svc → pricing-svc`, where `pricing-svc` hangs, `inventory-svc` retries 3×, and the final attempt never returns.

| Assertion | Mechanism proven |
|---|---|
| `REMOTE_CALL` edges span all three processes | Propagation over `traceparent` / `tracestate` |
| Three `RETRY_OF` edges, correctly siblings under one scope | DAG model and the scope invariant |
| The hung call appears as an unmatched `BEGIN` | `phase`, in-flight rendering |
| Payloads present for the whole trace though the trigger fired last | Ring buffer, retroactive flush |
| `checkout-api`'s payloads flushed too | Backward trigger propagation |
| Killing the collector mid-run yields `gaps[]`, not silence | Loss semantics |
| Order holds with one process's clock skewed −400 ms | HLC ordering, skew warning |

---

## Deferred decisions

Recorded here so they are decided deliberately later rather than by accident:

- **Retention and compaction.** No policy in v1; recordings grow unbounded on disk. Needed before any real production deployment.
- **Authentication on ingest and query.** v1 assumes a trusted network.
- **Hot reload of capture policy.** V2.
- **Collector-initiated retro-fetch** for asynchronous (Kafka) traces. V2.
- **`FORKS_TO` edge.** Enum range reserved. Deferred because fan-out is already derivable from "children of E overlapping in time"; an explicit edge only earns its place once V2 goroutine tracking lets us *observe* the spawn. Recording it earlier would mean recording an inference, which the philosophy forbids.
