package sdk

import (
	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
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
