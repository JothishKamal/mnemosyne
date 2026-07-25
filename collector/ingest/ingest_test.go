package ingest_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jothishkamal/mnemosyne/collector/ingest"
	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"github.com/jothishkamal/mnemosyne/store/memory"
	"google.golang.org/protobuf/proto"
)

func TestBatchIsPersisted(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	srv := httptest.NewServer(ingest.Handler(es, bs))
	defer srv.Close()

	tr := make([]byte, 16)
	tr[0] = 1
	body, _ := proto.Marshal(&v1.Batch{
		Events: []*v1.Event{{
			SchemaVersion: 1, TraceId: tr,
			Id:  &v1.EventID{EmitterId: []byte("emitter0"), Sequence: 1},
			Hlc: &v1.HLC{WallMs: 1000}, Phase: v1.Phase_INSTANT,
		}},
		Gaps: []*v1.Gap{{EmitterId: []byte("emitter0"), From: &v1.HLC{WallMs: 1000}, To: &v1.HLC{WallMs: 2000}, DroppedCount: 3}},
	})

	resp, err := http.Post(srv.URL+"/v1/batch", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	got, err := es.TraceEvents(context.Background(), tr)
	if err != nil || len(got) != 1 {
		t.Fatalf("TraceEvents returned %d events, err %v", len(got), err)
	}
	gaps, _ := es.Gaps(context.Background(), 0, 9999)
	if len(gaps) != 1 || gaps[0].DroppedCount != 3 {
		t.Fatalf("gaps not persisted: %v", gaps)
	}
}

func TestBlobUploadAndHasCheck(t *testing.T) {
	es, bs := memory.NewEvents(), memory.NewBlobs()
	srv := httptest.NewServer(ingest.Handler(es, bs))
	defer srv.Close()

	payload := []byte("a payload")
	sum := sha256.Sum256(payload)

	resp, err := http.Post(srv.URL+"/v1/blobs/"+bs.IDFor(sum), "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	resp.Body.Close()

	missing, err := bs.Has(context.Background(), [][32]byte{sum})
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if len(missing) != 0 {
		t.Fatal("uploaded blob reported as missing")
	}
}
