package hlc_test

import (
	"testing"
	"time"

	"github.com/jothishkamal/mnemosyne/sdk/hlc"
	"pgregory.net/rapid"
)

func frozen(ms int64) func() time.Time {
	return func() time.Time { return time.UnixMilli(ms) }
}

func TestNowIsStrictlyIncreasingUnderFrozenClock(t *testing.T) {
	c := hlc.New(frozen(1000))
	prev := c.Now()
	for i := 0; i < 100; i++ {
		got := c.Now()
		if !prev.Less(got) {
			t.Fatalf("iteration %d: %v is not < %v", i, prev, got)
		}
		prev = got
	}
}

func TestNowIsStrictlyIncreasingWhenPhysicalClockGoesBackwards(t *testing.T) {
	ms := int64(5000)
	c := hlc.New(func() time.Time { return time.UnixMilli(ms) })
	prev := c.Now()
	for i := 0; i < 50; i++ {
		ms -= 10 // physical clock runs backwards
		got := c.Now()
		if !prev.Less(got) {
			t.Fatalf("iteration %d: %v is not < %v", i, prev, got)
		}
		prev = got
	}
}

func TestObserveDominatesRemoteTimestamp(t *testing.T) {
	c := hlc.New(frozen(1000))
	remote := hlc.Timestamp{WallMS: 9999, Logical: 42}
	got := c.Observe(remote)
	if !remote.Less(got) {
		t.Fatalf("Observe(%v) returned %v, which is not strictly after remote", remote, got)
	}
	// Local state must have absorbed the remote wall time.
	next := c.Now()
	if !got.Less(next) {
		t.Fatalf("Now() after Observe returned %v, not after %v", next, got)
	}
}

func TestObserveNeverRegressesLocalState(t *testing.T) {
	c := hlc.New(frozen(8000))
	local := c.Now()
	got := c.Observe(hlc.Timestamp{WallMS: 10, Logical: 0}) // stale remote
	if !local.Less(got) {
		t.Fatalf("Observe of a stale remote regressed: %v is not < %v", local, got)
	}
}

// TestHappensBeforeIsNeverContradicted drives N simulated processes through
// randomized local events and message sends, recording the happens-before
// relation explicitly, then asserts HLC order agrees with every edge.
func TestHappensBeforeIsNeverContradicted(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		const procs = 5
		// Each process has its own skewed, independently-drifting physical clock.
		skew := make([]int64, procs)
		for i := range skew {
			skew[i] = rapid.Int64Range(-2000, 2000).Draw(rt, "skew")
		}
		base := int64(1_700_000_000_000)
		tick := make([]int64, procs)

		clocks := make([]*hlc.Clock, procs)
		for i := range clocks {
			i := i
			clocks[i] = hlc.New(func() time.Time {
				return time.UnixMilli(base + skew[i] + tick[i])
			})
		}

		type record struct {
			proc int
			ts   hlc.Timestamp
		}
		// last[i] is the most recent event on process i.
		last := make([]*record, procs)
		// edges holds (before, after) pairs that must satisfy before.ts < after.ts.
		var edges [][2]record

		steps := rapid.IntRange(20, 200).Draw(rt, "steps")
		for s := 0; s < steps; s++ {
			p := rapid.IntRange(0, procs-1).Draw(rt, "proc")
			tick[p] += rapid.Int64Range(0, 5).Draw(rt, "advance")

			if rapid.Bool().Draw(rt, "isReceive") {
				q := rapid.IntRange(0, procs-1).Draw(rt, "sender")
				if last[q] != nil && q != p {
					ts := clocks[p].Observe(last[q].ts)
					cur := record{proc: p, ts: ts}
					edges = append(edges, [2]record{*last[q], cur}) // send -> receive
					if last[p] != nil {
						edges = append(edges, [2]record{*last[p], cur}) // program order
					}
					last[p] = &cur
					continue
				}
			}

			ts := clocks[p].Now()
			cur := record{proc: p, ts: ts}
			if last[p] != nil {
				edges = append(edges, [2]record{*last[p], cur}) // program order
			}
			last[p] = &cur
		}

		for _, e := range edges {
			if !e[0].ts.Less(e[1].ts) {
				rt.Fatalf("happens-before violated: p%d %v is not < p%d %v",
					e[0].proc, e[0].ts, e[1].proc, e[1].ts)
			}
		}
	})
}
