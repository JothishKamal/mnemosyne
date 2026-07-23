package schema_test

import (
	"testing"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
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

// httpClientEvent populates every field including the HttpClient variant and non-string Value types.
func httpClientEvent() *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       make([]byte, 16),
		Id:            &v1.EventID{EmitterId: make([]byte, 8), Sequence: 7},
		Parent:        &v1.EventID{EmitterId: make([]byte, 8), Sequence: 3},
		SpanId:        make([]byte, 8),
		Service:       &v1.Service{Name: "api-gateway", Version: "2.0.0", EmitterId: make([]byte, 8)},
		Hlc:           &v1.HLC{WallMs: 1700000000000, Logical: 4},
		WallUnixNanos: 1700000000000000000,
		DurationNanos: 1500,
		Phase:         v1.Phase_END,
		OperationType: v1.OperationType_HTTP_CLIENT,
		Operation:     &v1.Event_HttpClient{HttpClient: &v1.HttpClient{Method: "GET", Host: "example.com", Path: "/api/v1/users", Status: 200}},
		Status:        v1.Status_OK,
		Error:         nil,
		Links: []*v1.Link{{
			Type:          v1.EdgeType_REMOTE_CALL,
			TraceId:       make([]byte, 16),
			Event:         &v1.EventID{EmitterId: make([]byte, 8), Sequence: 5},
			ForeignSpanId: make([]byte, 8),
		}},
		Request:             &v1.PayloadRef{Sha256: make([]byte, 32), Size: 256, Truncated: false, Redacted: false, Stored: true},
		Response:            &v1.PayloadRef{Sha256: make([]byte, 32), Size: 512, Truncated: true, Redacted: false, Stored: true},
		Completeness:        v1.Completeness_FULL,
		Attributes: map[string]*v1.Value{
			"http.response.time": {Kind: &v1.Value_IntValue{IntValue: 42}},
			"cache.size":         {Kind: &v1.Value_DoubleValue{DoubleValue: 3.14}},
			"is_cached":          {Kind: &v1.Value_BoolValue{BoolValue: true}},
		},
		AttributesTruncated: false,
	}
}

func TestEventRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		event *v1.Event
	}{
		{"fullEvent-http-server", fullEvent()},
		{"httpClientEvent", httpClientEvent()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.event
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
	// EdgeType values 5 through 15 are reserved for future use.
	for i := 5; i <= 15; i++ {
		if v1.EdgeType_name[int32(i)] != "" {
			t.Fatalf("edge type %d is reserved but named %q", i, v1.EdgeType_name[int32(i)])
		}
	}
}
