package identity_test

import (
	"strings"
	"testing"

	"github.com/jothishkamal/mnemosyne/sdk/hlc"
	"github.com/jothishkamal/mnemosyne/sdk/identity"
	"pgregory.net/rapid"
)

func TestTracestateRoundTripAndLengthBound(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ts := hlc.Timestamp{
			WallMS:  rapid.Uint64().Draw(rt, "wall"),
			Logical: rapid.Uint32().Draw(rt, "logical"),
		}
		var em identity.EmitterID
		for i := range em {
			em[i] = rapid.Byte().Draw(rt, "emitterByte")
		}
		id := identity.EventID{Emitter: em, Sequence: rapid.Uint64().Draw(rt, "seq")}

		enc := identity.EncodeTracestate(ts, id)
		if len(enc) > 256 {
			rt.Fatalf("tracestate value is %d chars, exceeds the W3C 256 limit: %q", len(enc), enc)
		}
		if strings.ContainsAny(enc, " ,=") {
			rt.Fatalf("tracestate value contains a forbidden character: %q", enc)
		}

		gotTS, gotID, err := identity.DecodeTracestate(enc)
		if err != nil {
			rt.Fatalf("decode %q: %v", enc, err)
		}
		if gotTS != ts || gotID != id {
			rt.Fatalf("round-trip mismatch: got (%v,%v) want (%v,%v)", gotTS, gotID, ts, id)
		}
	})
}

func TestDecodeTracestateRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "1", "9-a-b-c-d", "1-zz-0-aa-0", "1-0-0-0"} {
		if _, _, err := identity.DecodeTracestate(in); err == nil {
			t.Fatalf("DecodeTracestate(%q) succeeded, want error", in)
		}
	}
}

func TestTraceparentRoundTrip(t *testing.T) {
	g, _ := identity.NewGenerator()
	tr, _ := g.NewTrace()
	sp, _ := g.NewSpan()

	got, gotSpan, ok := identity.ParseTraceparent(identity.FormatTraceparent(tr, sp, true))
	if !ok {
		t.Fatal("ParseTraceparent rejected a value produced by FormatTraceparent")
	}
	if got != tr || gotSpan != sp {
		t.Fatalf("round-trip mismatch: got (%v,%v) want (%v,%v)", got, gotSpan, tr, sp)
	}
}

func TestParseTraceparentRejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",     // missing flags
		"00-0af7651916cd43dd8448eb211c80319c-00000000000000000-01", // span too long
		"00-00000000000000000000000000000000-b7ad6b7169203331-01",  // all-zero trace id
		"00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",  // all-zero span id
		"zz-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",  // bad version
	}
	for _, in := range cases {
		if _, _, ok := identity.ParseTraceparent(in); ok {
			t.Fatalf("ParseTraceparent(%q) accepted a malformed value", in)
		}
	}
}
