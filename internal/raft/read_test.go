package raft

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func read(n *Node, timeout time.Duration) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	v, err := n.Read(ctx, nil)
	if err != nil {
		return nil, err
	}
	return v.([]string), nil
}

func TestReadSeesEveryCommittedWriteWithoutGrowingTheLog(t *testing.T) {
	c := newCluster(t, 3)
	want := commands("cmd", 5)
	for _, cmd := range want {
		c.propose(cmd)
	}
	leader := c.nodes[c.leader()]
	before := leader.Status().LastIndex

	for range 10 {
		got, err := read(leader, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("read %q, want %q", got, want)
		}
	}
	if after := leader.Status().LastIndex; after != before {
		t.Fatalf("reads added %d log entries", after-before)
	}
}

func TestReadOnSingleNode(t *testing.T) {
	c := newCluster(t, 1)
	c.propose("x")
	got, err := read(c.nodes[0], time.Second)
	if err != nil || !slices.Equal(got, []string{"x"}) {
		t.Fatalf("got %q, %v", got, err)
	}
}

// A leader cut off from the majority may already have been replaced, so it
// must not answer reads from its possibly stale state.
func TestIsolatedLeaderCannotServeReads(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	c.disconnect((leader + 1) % 3)
	c.disconnect((leader + 2) % 3)

	if _, err := read(c.nodes[leader], 300*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the read to wait for a majority", err)
	}
}

func TestFollowerRedirectsReads(t *testing.T) {
	c := newCluster(t, 3)
	follower := (c.leader() + 1) % 3
	var notLeader *NotLeaderError
	if _, err := read(c.nodes[follower], time.Second); !errors.As(err, &notLeader) {
		t.Fatalf("got %v, want NotLeaderError", err)
	}
}
