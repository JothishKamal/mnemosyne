package capture_test

import (
	"bytes"
	"testing"

	"github.com/jothishkamal/mnemosyne/sdk/capture"
)

func TestBodyRedactionRemovesSecretFromBytes(t *testing.T) {
	r := capture.Redact{JSONKeys: []string{"password", "ssn"}}
	in := []byte(`{"user":"ada","password":"hunter2","nested":{"ssn":"123-45-6789"},"ok":1}`)

	out, changed := r.Body(in)
	if !changed {
		t.Fatal("Body reported no change but secrets were present")
	}
	for _, secret := range []string{"hunter2", "123-45-6789"} {
		if bytes.Contains(out, []byte(secret)) {
			t.Fatalf("redacted body still contains %q: %s", secret, out)
		}
	}
	if !bytes.Contains(out, []byte("ada")) {
		t.Fatalf("redaction removed a non-secret value: %s", out)
	}
}

func TestBodyRedactionLeavesCleanBodyUnchanged(t *testing.T) {
	r := capture.Redact{JSONKeys: []string{"password"}}
	in := []byte(`{"user":"ada"}`)
	out, changed := r.Body(in)
	if changed {
		t.Fatal("clean body reported as changed")
	}
	if !bytes.Equal(in, out) {
		t.Fatalf("clean body altered: %s", out)
	}
}

func TestBodyRedactionPassesThroughNonJSON(t *testing.T) {
	r := capture.Redact{JSONKeys: []string{"password"}}
	in := []byte("not json at all")
	out, changed := r.Body(in)
	if changed || !bytes.Equal(in, out) {
		t.Fatalf("non-JSON body altered: %s", out)
	}
}

func TestHeaderRedactionIsCaseInsensitive(t *testing.T) {
	r := capture.Redact{Headers: []string{"authorization", "cookie"}}
	for _, name := range []string{"Authorization", "AUTHORIZATION", "cookie"} {
		got, redacted := r.HeaderValue(name, "Bearer secret-token")
		if !redacted {
			t.Fatalf("header %q was not redacted", name)
		}
		if got == "Bearer secret-token" {
			t.Fatalf("header %q retained its value", name)
		}
	}
	if _, redacted := r.HeaderValue("Content-Type", "application/json"); redacted {
		t.Fatal("Content-Type was redacted")
	}
}
