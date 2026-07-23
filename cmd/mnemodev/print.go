package main

import (
	"io"

	v1 "github.com/jothishkamal/mnemosyne/schema/gen/go/mnemosyne/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// PrintEvent writes an event as indented JSON. This is the developer harness's
// only job until Task 7 gives it a live event stream to read.
func PrintEvent(w io.Writer, e *v1.Event) error {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(e)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func demo() *v1.Event {
	return &v1.Event{
		SchemaVersion: 1,
		TraceId:       []byte("0123456789abcdef"),
		Id:            &v1.EventID{EmitterId: []byte("emitter1"), Sequence: 1},
		Service:       &v1.Service{Name: "mnemodev"},
		Hlc:           &v1.HLC{WallMs: 1700000000000},
		Phase:         v1.Phase_INSTANT,
		OperationType: v1.OperationType_LOCAL,
		Status:        v1.Status_OK,
	}
}
