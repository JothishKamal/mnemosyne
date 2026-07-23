// Package hlc implements a hybrid logical clock.
//
// An HLC timestamp orders events consistently with happens-before without
// requiring synchronized physical clocks. Wall time is carried only so that
// timestamps stay close to real time; ordering never depends on clocks
// agreeing between machines.
package hlc

import (
	"sync"
	"time"
)

// Timestamp is a hybrid logical clock reading.
type Timestamp struct {
	WallMS  uint64
	Logical uint32
}

// Less reports whether t orders strictly before o.
func (t Timestamp) Less(o Timestamp) bool {
	if t.WallMS != o.WallMS {
		return t.WallMS < o.WallMS
	}
	return t.Logical < o.Logical
}

// Clock is a hybrid logical clock. It is safe for concurrent use.
//
// A mutex is used rather than atomics: the update is a read-modify-write of
// two fields, and 64-bit atomics on 32-bit platforms carry alignment
// requirements that are easy to violate accidentally.
type Clock struct {
	mu   sync.Mutex
	now  func() time.Time
	last Timestamp
}

// New returns a Clock reading physical time from now. A nil now uses time.Now.
func New(now func() time.Time) *Clock {
	if now == nil {
		now = time.Now
	}
	return &Clock{now: now}
}

// Now returns a timestamp for a locally-generated event. The returned
// timestamp is strictly greater than every timestamp previously returned by
// this Clock, even if physical time moves backwards.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := uint64(c.now().UnixMilli())
	if phys > c.last.WallMS {
		c.last = Timestamp{WallMS: phys, Logical: 0}
	} else {
		c.last.Logical++
	}
	return c.last
}

// Observe merges a timestamp received from another process and returns a
// timestamp strictly greater than both the remote timestamp and every
// timestamp previously returned by this Clock.
func (c *Clock) Observe(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	phys := uint64(c.now().UnixMilli())
	wall := phys
	if c.last.WallMS > wall {
		wall = c.last.WallMS
	}
	if remote.WallMS > wall {
		wall = remote.WallMS
	}

	switch {
	case wall == c.last.WallMS && wall == remote.WallMS:
		logical := c.last.Logical
		if remote.Logical > logical {
			logical = remote.Logical
		}
		c.last.Logical = logical + 1
	case wall == c.last.WallMS:
		c.last.Logical++
	case wall == remote.WallMS:
		c.last.Logical = remote.Logical + 1
	default: // physical time dominates both
		c.last.Logical = 0
	}
	c.last.WallMS = wall
	return c.last
}
