package blobfs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jothishkamal/mnemosyne/store/blobfs"
)

// TestRejectsPathTraversal proves a malicious blob ID cannot escape the
// store's root, whether via ../ segments or an embedded path separator.
func TestRejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	bs, err := blobfs.Open(dir)
	if err != nil {
		t.Fatalf("blobfs.Open: %v", err)
	}
	defer bs.Close()
	ctx := context.Background()

	malicious := []string{
		"../../etc/passwd",
		"../../../../etc/passwd",
		"a/../../../etc/passwd",
		"sub/dir/file",
		"..",
		".",
		"a\\..\\..\\evil",
	}

	for _, id := range malicious {
		t.Run(id, func(t *testing.T) {
			if _, err := bs.Get(ctx, id); err == nil {
				t.Fatalf("Get(%q) succeeded, want rejection", id)
			}
		})
	}

	// Confirm nothing escaped the root: no file was created outside dir.
	outside := filepath.Join(filepath.Dir(dir), "passwd")
	if _, err := os.Stat(outside); err == nil {
		t.Fatalf("a file was created outside the store root at %s", outside)
	}
}

// TestSurvivesReopen proves blobfs durability: content written before Close
// must still be retrievable after reopening the store rooted at the same
// directory.
func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	bs, err := blobfs.Open(dir)
	if err != nil {
		t.Fatalf("blobfs.Open: %v", err)
	}
	body := []byte("payload that must survive a reopen")
	var sum [32]byte
	copy(sum[:], []byte("abcdefghijklmnopqrstuvwxyz012345"))
	id, err := bs.Put(ctx, sum, body)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs2, err := blobfs.Open(dir)
	if err != nil {
		t.Fatalf("blobfs.Open (reopen): %v", err)
	}
	defer bs2.Close()
	got, err := bs2.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get (after reopen): %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("blob not durable across reopen: got %q, want %q", got, body)
	}
}
