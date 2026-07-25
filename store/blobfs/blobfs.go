// Package blobfs is the default content-addressed BlobStore, backed by the
// local filesystem.
package blobfs

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// Store is a filesystem BlobStore. Objects are sharded by hash prefix so no
// single directory grows unbounded.
type Store struct{ dir string }

// Open opens or creates the store rooted at dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

// IDFor returns the opaque blob ID for a content hash.
func (s *Store) IDFor(sha [32]byte) string { return hex.EncodeToString(sha[:]) }

func (s *Store) path(id string) (string, error) {
	if len(id) < 4 {
		return "", errors.New("blobfs: malformed blob id")
	}
	// Reject anything that could escape the root: a valid ID is a bare
	// filename with no path separators or traversal segments. filepath.Base
	// alone is not enough on its own (e.g. it would still accept a name
	// containing embedded separators after cleaning), so also reject any ID
	// that contains a slash or backslash outright.
	for _, r := range id {
		if r == '/' || r == '\\' {
			return "", errors.New("blobfs: malformed blob id")
		}
	}
	if filepath.Base(id) != id || id == "." || id == ".." {
		return "", errors.New("blobfs: malformed blob id")
	}
	return filepath.Join(s.dir, id[:2], id[2:4], id), nil
}

func (s *Store) Put(_ context.Context, sha [32]byte, body []byte) (string, error) {
	id := s.IDFor(sha)
	p, err := s.path(id)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(p); err == nil {
		return id, nil // already stored: content-addressed dedup
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, p); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) Get(_ context.Context, id string) ([]byte, error) {
	p, err := s.path(id)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}

func (s *Store) Has(_ context.Context, shas [][32]byte) ([][32]byte, error) {
	var missing [][32]byte
	for _, sha := range shas {
		p, err := s.path(s.IDFor(sha))
		if err != nil {
			return nil, err
		}
		if _, err := os.Stat(p); err != nil {
			missing = append(missing, sha)
		}
	}
	return missing, nil
}

func (s *Store) Close() error { return nil }
