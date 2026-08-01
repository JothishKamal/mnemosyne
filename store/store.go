// Package store defines Mnemosyne's two storage abstractions.
//
// EventStore and BlobStore are deliberately separate: an append-mostly
// time-ordered index and a content-addressed object store are different
// problems with different scaling characteristics.
package store

import (
	"context"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
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
//
// RootService and RootRoute are the service and route of the trace's
// earliest event by (hlc.wall_ms, hlc.logical, id.emitter_id), not
// necessarily the first event a backend happens to encounter while scanning.
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
// (hlc.wall_ms, hlc.logical, id.emitter_id), never by wall clock.
type EventStore interface {
	AppendEvents(ctx context.Context, events []*v1.Event) error
	AppendGaps(ctx context.Context, gaps []*v1.Gap) error
	TraceEvents(ctx context.Context, traceID []byte) ([]*v1.Event, error)
	Window(ctx context.Context, q WindowQuery) ([]*v1.Event, error)
	Gaps(ctx context.Context, fromWallMS, toWallMS uint64) ([]*v1.Gap, error)
	// Traces returns trace summaries ordered by StartWallMS, then by
	// TraceID bytes as a deterministic tie-break.
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
	// Has reports which of shas are missing from the store, its return
	// value is the absent subset, not the present one.
	Has(ctx context.Context, shas [][32]byte) ([][32]byte, error)
	IDFor(sha [32]byte) string
	Close() error
}
