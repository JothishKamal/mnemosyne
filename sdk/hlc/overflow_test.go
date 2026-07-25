package hlc

import (
	"math"
	"testing"
	"time"
)

func TestBumpCarriesIntoWallOnOverflow(t *testing.T) {
	if got := bump(100, math.MaxUint32); got != (Timestamp{WallMS: 101, Logical: 0}) {
		t.Fatalf("bump(100, MaxUint32) = %v, want {101, 0}", got)
	}
	if got := bump(100, 5); got != (Timestamp{WallMS: 100, Logical: 6}) {
		t.Fatalf("bump(100, 5) = %v, want {100, 6}", got)
	}
}

func TestNowStaysMonotonicAtLogicalCeiling(t *testing.T) {
	c := New(func() time.Time { return time.UnixMilli(100) }) // frozen: ties every call
	c.last = Timestamp{WallMS: 100, Logical: math.MaxUint32}
	prev := c.last
	got := c.Now()
	if !prev.Less(got) {
		t.Fatalf("Now at ceiling returned %v, not strictly after %v", got, prev)
	}
	if got != (Timestamp{WallMS: 101, Logical: 0}) {
		t.Fatalf("Now at ceiling = %v, want {101, 0}", got)
	}
}

func TestObserveStaysMonotonicAtLogicalCeiling(t *testing.T) {
	c := New(func() time.Time { return time.UnixMilli(100) })
	c.last = Timestamp{WallMS: 100, Logical: math.MaxUint32}
	prev := c.last
	got := c.Observe(Timestamp{WallMS: 100, Logical: 5}) // remote ties wall; local logical dominates
	if !prev.Less(got) {
		t.Fatalf("Observe at ceiling returned %v, not strictly after %v", got, prev)
	}
}
