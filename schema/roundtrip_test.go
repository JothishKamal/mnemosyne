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

func TestEventRoundTrip(t *testing.T) {
	in := fullEvent()
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
}

func TestEdgeTypeReservesFutureRange(t *testing.T) {
	// FORKS_TO and friends occupy 5..15; nothing may claim them.
	if v1.EdgeType_name[5] != "" {
		t.Fatalf("edge type 5 is reserved but named %q", v1.EdgeType_name[5])
	}
}
