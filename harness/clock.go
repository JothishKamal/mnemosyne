// Package harness runs deterministic multi-service incidents in one process.
package harness

import (
	"sync"
	"time"
)

// Clock is a manually-advanced clock with a per-service skew offset. Real time
// is never consulted, which is what makes scenarios reproducible.
type Clock struct {
	mu   sync.Mutex
	now  time.Time
	skew time.Duration
}

// NewClock returns a Clock reading start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now returns the current virtual time including this clock's skew.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Add(c.skew)
}

// Advance moves virtual time forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Skew sets this clock's offset from true virtual time, simulating a machine
// whose NTP has drifted.
func (c *Clock) Skew(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.skew = d
}
