package mnemoctx_test

import (
	"context"
	"testing"

	"github.com/jothishkamal/mnemosyne/sdk/identity"
	"github.com/jothishkamal/mnemosyne/sdk/mnemoctx"
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
