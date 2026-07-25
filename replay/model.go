// Package replay turns a recording into a Timeline Model.
//
// The Timeline Model is the only representation the UI consumes. Aggregation
// happens here, server-side, so that response size is a function of screen
// resolution rather than incident size.
package replay

import v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"

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
