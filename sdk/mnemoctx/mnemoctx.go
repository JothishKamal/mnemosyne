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

	"github.com/jothishkamal/mnemosyne/sdk/identity"
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
