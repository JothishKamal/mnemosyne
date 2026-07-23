package main

import "testing"

func TestVersionString(t *testing.T) {
	got := versionString()
	if got == "" {
		t.Fatal("versionString() returned empty string")
	}
}
