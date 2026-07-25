package query_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jothishkamal/mnemosyne/collector/query"
	"github.com/jothishkamal/mnemosyne/replay"
	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store/memory"
)

// TestChainSpeaksOneJSONConvention pins the wire-format contract of
// /api/chain: every embedded *v1.Event must be rendered by protojson
// (camelCase field names, oneof flattened to its field name) rather than by
// encoding/json against the generated protoc-gen-go struct directly
// (snake_case field names, plus a Go-cased "Operation" oneof wrapper). A
// payload mixing the two conventions is exactly the bug this endpoint must
// not regress to.
func TestChainSpeaksOneJSONConvention(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	ctx := t.Context()

	traceID := make([]byte, 16)
	for i := range traceID {
		traceID[i] = byte(i + 1)
	}
	emitter := []byte("emitter0")

	server := &v1.Event{
		SchemaVersion: 1,
		TraceId:       traceID,
		Id:            &v1.EventID{EmitterId: emitter, Sequence: 1},
		Service:       &v1.Service{Name: "svc-a", EmitterId: emitter},
		Hlc:           &v1.HLC{WallMs: 1000},
		WallUnixNanos: 1_000_000_000,
		Phase:         v1.Phase_END,
		OperationType: v1.OperationType_HTTP_SERVER,
		Operation:     &v1.Event_HttpServer{HttpServer: &v1.HttpServer{Method: "GET", Route: "/r"}},
		Status:        v1.Status_OK,
	}
	client := &v1.Event{
		SchemaVersion: 1,
		TraceId:       traceID,
		Id:            &v1.EventID{EmitterId: emitter, Sequence: 2},
		Parent:        &v1.EventID{EmitterId: emitter, Sequence: 1},
		Service:       &v1.Service{Name: "svc-a", EmitterId: emitter},
		Hlc:           &v1.HLC{WallMs: 1001},
		WallUnixNanos: 1_001_000_000,
		Phase:         v1.Phase_END,
		OperationType: v1.OperationType_HTTP_CLIENT,
		Operation:     &v1.Event_HttpClient{HttpClient: &v1.HttpClient{Method: "GET", Host: "downstream", Path: "/d"}},
		Status:        v1.Status_OK,
	}

	if err := es.AppendEvents(ctx, []*v1.Event{server, client}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	eng := replay.New(es, bs)
	handler := query.Handler(eng, es, bs)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	url := srv.URL + "/api/chain?trace=" + hexEncode(traceID) +
		"&emitter=" + hexEncode(emitter) + "&seq=2&max=200"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /api/chain: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	got := string(body)

	for _, want := range []string{`"operationType"`, `"httpServer"`, `"httpClient"`} {
		if !strings.Contains(got, want) {
			t.Errorf("response missing %s\nbody: %s", want, got)
		}
	}

	// The point of this test: the mixed-convention shapes must never come
	// back. If any of these appear, encoding/json is touching a protobuf
	// message directly again.
	for _, unwanted := range []string{`"operation_type"`, `"trace_id"`, `"Operation"`} {
		if strings.Contains(got, unwanted) {
			t.Errorf("response contains %s, want protojson-only camelCase\nbody: %s", unwanted, got)
		}
	}
}

func hexEncode(b []byte) string {
	const hexDigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hexDigits[c>>4]
		out[i*2+1] = hexDigits[c&0xf]
	}
	return string(out)
}
