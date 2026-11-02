package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestElectsOneStableLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	term := c.nodes[leader].Status().Term

	// With a healthy leader sending heartbeats, nobody starts an election.
	time.Sleep(time.Second)
	if got := c.leader(); got != leader {
		t.Fatalf("leader changed from %d to %d", leader, got)
	}
	if got := c.nodes[leader].Status().Term; got != term {
		t.Fatalf("term changed from %d to %d", term, got)
	}
}

func TestReelectsAfterLeaderFailure(t *testing.T) {
	c := newCluster(t, 3)
	old := c.leader()
	oldTerm := c.nodes[old].Status().Term

	c.disconnect(old)
	leader := c.leader()
	if leader == old {
		t.Fatal("disconnected node is still the only leader")
	}
	if term := c.nodes[leader].Status().Term; term <= oldTerm {
		t.Fatalf("new leader has term %d, want more than %d", term, oldTerm)
	}

	// When it comes back, the old leader learns the newer term and steps down.
	c.connect(old)
	eventually(t, 5*time.Second, "old leader to step down", func() bool {
		return c.nodes[old].Status().Role == Follower
	})
}

func TestReplicatesCommandsInOrder(t *testing.T) {
	c := newCluster(t, 3)
	want := commands("cmd", 20)
	for _, cmd := range want {
		c.propose(cmd)
	}
	c.waitApplied(want)
}

func TestProposeReturnsApplyResult(t *testing.T) {
	c := newCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := c.nodes[c.leader()].Propose(ctx, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("got %v, want 1 (the first applied command)", got)
	}
}

func TestFollowerRejectsProposals(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	follower := (leader + 1) % 3
	eventually(t, time.Second, "follower to learn the leader", func() bool {
		return c.nodes[follower].Status().LeaderID != ""
	})

	_, err := c.nodes[follower].Propose(context.Background(), []byte("x"))
	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("got %v, want NotLeaderError", err)
	}
	if notLeader.Leader.Id != c.members[leader].Id {
		t.Fatalf("redirected to %s, want %s", notLeader.Leader.Id, c.members[leader].Id)
	}
}

func TestNoCommitWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	c.disconnect((leader + 1) % 3)
	c.disconnect((leader + 2) % 3)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.nodes[leader].Propose(ctx, []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the proposal to wait for a majority", err)
	}
	if got := c.fsms[leader].values(); len(got) != 0 {
		t.Fatalf("leader applied %q without a majority", got)
	}
}

func TestDisconnectedFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	follower := (c.leader() + 1) % 3
	c.disconnect(follower)

	want := commands("cmd", 30)
	for _, cmd := range want {
		c.propose(cmd)
	}
	c.connect(follower)
	c.waitApplied(want)
}

// An isolated leader keeps appending entries that can never commit. After
// the rest of the cluster moves on, those entries must be replaced.
func TestUncommittedEntriesAreOverwritten(t *testing.T) {
	c := newCluster(t, 5)
	c.propose("base")

	old := c.leader()
	c.disconnect(old)
	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		c.nodes[old].Propose(ctx, []byte("lost"))
		cancel()
	}

	want := []string{"base"}
	for _, cmd := range commands("new", 10) {
		c.propose(cmd)
		want = append(want, cmd)
	}
	c.connect(old)
	c.waitApplied(want)
}
