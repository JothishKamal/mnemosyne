package schema_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/descriptorpb"
)

var update = flag.Bool("update", false, "rewrite the golden descriptor")

const goldenPath = "testdata/descriptor.golden.binpb"

// schemaFiles are the frozen files this guard covers.
var schemaFiles = []string{
	"mnemosyne/v1/common.proto",
	"mnemosyne/v1/event.proto",
	"mnemosyne/v1/gap.proto",
	"mnemosyne/v1/ingest.proto",
}

// currentDescriptorSet builds a FileDescriptorSet from the descriptors embedded
// in the generated Go code. protoc is deliberately not involved: this must pass
// on a machine that has never installed it.
//
// protodesc omits source-code info, so comments and formatting do not
// participate — only structure: field numbers, types, enum values, and
// reserved ranges.
func currentDescriptorSet(t *testing.T) *descriptorpb.FileDescriptorSet {
	t.Helper()
	set := &descriptorpb.FileDescriptorSet{}
	for _, path := range schemaFiles { // fixed order, so output is stable
		fd, err := protoregistry.GlobalFiles.FindFileByPath(path)
		if err != nil {
			t.Fatalf("descriptor for %s not registered: %v", path, err)
		}
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}
	return set
}

func marshalGolden(t *testing.T, set *descriptorpb.FileDescriptorSet) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(set)
	if err != nil {
		t.Fatalf("marshal descriptor set: %v", err)
	}
	return b
}

func TestSchemaHasNotDrifted(t *testing.T) {
	current := currentDescriptorSet(t)
	got := marshalGolden(t, current)

	if *update {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("create testdata dir: %v", err)
		}
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("golden updated (%d bytes) — commit it only alongside an intentional schema change", len(got))
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (regenerate with: go test ./schema/ -update): %v", err)
	}
	if string(got) == string(want) {
		return
	}

	var previous descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(want, &previous); err != nil {
		t.Fatalf("golden is corrupt: %v", err)
	}
	t.Fatalf("the frozen schema changed:\n%s\n\n"+
		"The wire format is frozen. If this change is intentional and "+
		"backward compatible, regenerate with: go test ./schema/ -update",
		cmp.Diff(&previous, current, protocmp.Transform()))
}

// TestGoldenIsDeterministic guards the guard: a golden that is not byte-stable
// would fail at random and be deleted within a week.
func TestGoldenIsDeterministic(t *testing.T) {
	first := marshalGolden(t, currentDescriptorSet(t))
	for i := 0; i < 10; i++ {
		if got := marshalGolden(t, currentDescriptorSet(t)); string(got) != string(first) {
			t.Fatalf("iteration %d produced different bytes; the golden is not deterministic", i)
		}
	}
}

// TestFrozenInvariantsAreNamed asserts the specific guarantees the design
// document calls out, so a failure says which promise broke rather than only
// that some bytes differ.
func TestFrozenInvariantsAreNamed(t *testing.T) {
	fd, err := protoregistry.GlobalFiles.FindFileByPath("mnemosyne/v1/event.proto")
	if err != nil {
		t.Fatalf("event.proto not registered: %v", err)
	}

	t.Run("event field numbers", func(t *testing.T) {
		msg := fd.Messages().ByName("Event")
		if msg == nil {
			t.Fatal("message Event not found")
		}
		want := map[string]protoreflect.FieldNumber{
			"schema_version": 1, "trace_id": 2, "id": 3, "parent": 4, "span_id": 5,
			"service": 6, "hlc": 7, "wall_unix_nanos": 8, "duration_nanos": 9,
			"phase": 10, "operation_type": 11, "http_server": 12, "http_client": 13,
			"status": 14, "error": 15, "links": 16, "request": 17, "response": 18,
			"completeness": 19, "attributes": 20, "attributes_truncated": 21,
		}
		for name, num := range want {
			f := msg.Fields().ByName(protoreflect.Name(name))
			if f == nil {
				t.Errorf("Event.%s is missing from the frozen schema", name)
				continue
			}
			if f.Number() != num {
				t.Errorf("Event.%s has field number %d, frozen at %d", name, f.Number(), num)
			}
		}
	})

	t.Run("edge type reserves 5 to 15", func(t *testing.T) {
		enum := fd.Enums().ByName("EdgeType")
		if enum == nil {
			t.Fatal("enum EdgeType not found")
		}
		for v := int32(5); v <= 15; v++ {
			if got := enum.Values().ByNumber(protoreflect.EnumNumber(v)); got != nil {
				t.Errorf("EdgeType %d is reserved for future graph relationships but is named %q", v, got.Name())
			}
		}
	})
}
