# Implementation Order — Spec 1

Dependency order only. Architecture lives in `2026-07-23-mnemosyne-design.md`.

The ordering rule: **get to a rendered replay before building anything durable.** Persistence is step 10 for a reason — two weeks of storage work before the first timeline renders is how the schema goes unvalidated.

| # | Step | Unblocks | Done when |
|---|---|---|---|
| 1 | Generate protobufs from `schema/proto` | everything | `schema/gen/go` builds; golden fixtures round-trip |
| 2 | HLC | ordering | property test: happens-before never violated under randomized interleavings |
| 3 | Identity (`emitter_id`, sequence, trace/span IDs) | events | IDs are unique, monotonic, allocation-free on the hot path |
| 4 | `Recorder` + `Handle` + `mnemoctx` | all instrumentation | scope invariant holds: siblings stay siblings; `Handle` is nil-safe and zero-alloc when capture is off |
| 5 | `mnemohttp` server + client | cross-process traces | `traceparent`/`tracestate` round-trip across two processes |
| 6 | In-memory collector + in-memory stores | end-to-end flow | events from 3 processes land in one queryable place |
| 7 | Deterministic incident harness | everything after | the acceptance scenario runs reproducibly with a virtual clock and injected faults |
| 8 | Replay engine → first `TimelineModel` | UI | BEGIN/END join, HLC ordering, chain walk, in-flight detection all assert green |
| 9 | Render first replay (split view) | validation of the whole model | the acceptance scenario is visibly correct on screen |
| 10 | Replace in-memory with SQLite `EventStore` + content-addressed `BlobStore` | production shape | same assertions pass against durable storage |

Deferred within Spec 1 until after step 9, because none of them change what replay observes: capture policy matching beyond a hardcoded default, BEGIN elision, backpressure shedding, blob dedup via `HasBlobs`.

Steps 2–4 are the only ones where a mistake is expensive to undo — they define the invariants every later step assumes. Everything from 6 onward is replaceable.
