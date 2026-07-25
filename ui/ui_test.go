package ui_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jothishkamal/mnemosyne/ui"
)

func TestIndexIsServed(t *testing.T) {
	srv := httptest.NewServer(ui.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content type = %q, want text/html", ct)
	}
}

func TestAssetsAreServed(t *testing.T) {
	srv := httptest.NewServer(ui.Handler())
	defer srv.Close()

	for _, path := range []string{"/app.js", "/style.css"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", path, resp.StatusCode)
		}
		if resp.ContentLength == 0 {
			t.Fatalf("%s is empty", path)
		}
	}
}
