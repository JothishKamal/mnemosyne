export CGO_ENABLED=0

.PHONY: build test proto lint schema-golden
build:
	go build ./...
test:
	go test ./...
proto:
	protoc --proto_path=schema/proto \
	  --go_out=schema/gen/go --go_opt=module=github.com/jothishkamal/mnemosyne/schema/gen/go \
	  schema/proto/mnemosyne/v1/*.proto
schema-golden:
	go test ./schema/ -update
