package raft

import (
	"testing"
	"time"
)

// isolateAndRejoin cuts a follower off long enough to time out several
// times, reconnects it, and returns the leader's term before and after.
func isolateAndRejoin(t *testing.T, c *cluster) (leader int, before, after uint64) {
	t.Helper()
	leader = c.leader()
	before = c.nodes[leader].Status().Term
	follower := (leader + 1) % len(c.nodes)

	c.disconnect(follower)
	time.Sleep(time.Second)
	c.connect(follower)
	time.Sleep(500 * time.Millisecond)
	return leader, before, c.nodes[c.leader()].Status().Term
}

func TestRejoiningFollowerDoesNotDisruptLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader, before, after := isolateAndRejoin(t, c)
	if got := c.leader(); got != leader {
		t.Fatalf("leadership moved from %d to %d", leader, got)
	}
	if after != before {
		t.Fatalf("term moved from %d to %d", before, after)
	}
}

// Without Pre-Vote the isolated follower raises its term on every timeout,
// and its higher term forces the leader to step down when it returns.
func TestWithoutPreVoteRejoiningFollowerForcesElection(t *testing.T) {
	c := newClusterWith(t, 3, Config{DisablePreVote: true})
	_, before, after := isolateAndRejoin(t, c)
	if after <= before {
		t.Fatalf("term stayed at %d; expected the rejoining follower to force a new term", before)
	}
}
