package raft

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func memberCount(n *Node) int { return len(n.Status().Members) }

func (c *cluster) addMember(i int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		err := c.nodes[c.leader()].AddMember(ctx, c.members[i])
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			c.t.Fatalf("adding %s: %v", c.members[i].Id, err)
		}
		time.Sleep(20 * time.Millisecond) // e.g. the leader's no-op is not committed yet
	}
}

func (c *cluster) removeMember(i int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		err := c.nodes[c.leader()].RemoveMember(ctx, c.members[i].Id)
		if err == nil || errors.Is(err, ErrUnknownMember) {
			return
		}
		if ctx.Err() != nil {
			c.t.Fatalf("removing %s: %v", c.members[i].Id, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestAddMember(t *testing.T) {
	c := newCluster(t, 3)
	want := commands("cmd", 10)
	for _, cmd := range want {
		c.propose(cmd)
	}

	added := c.addNode()
	if memberCount(c.nodes[added]) != 0 {
		t.Fatal("a node that has not been added yet already has members")
	}
	c.addMember(added)
	c.waitApplied(want, added)
	for _, n := range c.nodes {
		eventually(t, 5*time.Second, "everyone to see 4 members", func() bool { return memberCount(n) == 4 })
	}

	c.propose("after")
	c.waitApplied(append(want, "after"))
}

func TestAddedMemberCatchesUpFromSnapshot(t *testing.T) {
	c := newClusterWith(t, 3, smallSnapshots)
	want := commands("cmd", 40)
	for _, cmd := range want {
		c.propose(cmd)
	}
	eventually(t, 5*time.Second, "leader to compact", func() bool {
		return c.nodes[c.leader()].Status().SnapshotIndex > 0
	})

	added := c.addNode()
	c.addMember(added)
	c.waitApplied(want, added)
	// The snapshot carried the configuration, so the new node knows the
	// cluster even though the configuration entries were compacted away.
	if memberCount(c.nodes[added]) != 4 {
		t.Fatalf("new node sees %d members, want 4", memberCount(c.nodes[added]))
	}
}

func TestAddingTheSameMemberTwiceIsANoOp(t *testing.T) {
	c := newCluster(t, 3)
	added := c.addNode()
	c.addMember(added)
	before := c.nodes[c.leader()].Status().LastIndex
	c.addMember(added)
	if after := c.nodes[c.leader()].Status().LastIndex; after != before {
		t.Fatalf("second add appended %d entries", after-before)
	}
}

// The removed node keeps running and, never told it was removed, keeps
// trying to start elections. The cluster must not be disrupted by it.
func TestRemoveFollower(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	removed := (leader + 1) % 3
	c.removeMember(removed)
	if memberCount(c.nodes[leader]) != 2 {
		t.Fatalf("leader sees %d members, want 2", memberCount(c.nodes[leader]))
	}

	term := c.nodes[leader].Status().Term
	time.Sleep(time.Second)
	if got := c.leader(); got != leader {
		t.Fatalf("leadership moved from %d to %d", leader, got)
	}
	if got := c.nodes[leader].Status().Term; got != term {
		t.Fatalf("term moved from %d to %d", term, got)
	}

	want := commands("cmd", 5)
	for _, cmd := range want {
		c.propose(cmd)
	}
	remaining := slices.DeleteFunc([]int{0, 1, 2}, func(i int) bool { return i == removed })
	c.waitApplied(want, remaining...)
}

func TestRemoveLeader(t *testing.T) {
	c := newCluster(t, 3)
	old := c.leader()
	c.removeMember(old)

	eventually(t, 5*time.Second, "removed leader to step down", func() bool {
		return c.nodes[old].Status().Role == Follower
	})
	leader := c.leader()
	if leader == old {
		t.Fatal("removed leader is leading again")
	}
	if memberCount(c.nodes[leader]) != 2 {
		t.Fatalf("new leader sees %d members, want 2", memberCount(c.nodes[leader]))
	}
	c.propose("after")
	c.waitApplied([]string{"after"}, leader)
}

func TestOneMembershipChangeAtATime(t *testing.T) {
	c := newCluster(t, 3)
	c.propose("x") // make sure the leader's no-op is committed
	leader := c.leader()
	c.disconnect((leader + 1) % 3)
	c.disconnect((leader + 2) % 3)

	// Without a majority the first change cannot commit...
	first := c.newMember()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.nodes[leader].AddMember(ctx, c.members[first]); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first change: got %v, want it to wait for a majority", err)
	}
	// ...so a second one must be refused.
	second := c.newMember()
	if err := c.nodes[leader].AddMember(context.Background(), c.members[second]); !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("second change: got %v, want ErrConfigChangeInProgress", err)
	}
}
