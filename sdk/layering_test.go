package sdk_test

import (
	"go/build"
	"strings"
	"testing"
)

// TestCoreSDKImportsNoInstrumentedLibrary enforces the architectural rule that
// sdk/ never depends on a library it instruments. Adapters depend on the core,
// never the reverse.
func TestCoreSDKImportsNoInstrumentedLibrary(t *testing.T) {
	forbidden := []string{
		"github.com/jackc/pgx",
		"github.com/redis/go-redis",
		"github.com/segmentio/kafka-go",
		"github.com/IBM/sarama",
	}
	pkgs := []string{
		"github.com/jothishkamal/mnemosyne/sdk",
		"github.com/jothishkamal/mnemosyne/sdk/hlc",
		"github.com/jothishkamal/mnemosyne/sdk/identity",
		"github.com/jothishkamal/mnemosyne/sdk/mnemoctx",
		"github.com/jothishkamal/mnemosyne/sdk/capture",
	}
	for _, p := range pkgs {
		pkg, err := build.Import(p, ".", 0)
		if err != nil {
			t.Fatalf("import %s: %v", p, err)
		}
		for _, imp := range pkg.Imports {
			for _, bad := range forbidden {
				if strings.HasPrefix(imp, bad) {
					t.Fatalf("%s imports instrumented library %s", p, imp)
				}
			}
		}
	}
}
