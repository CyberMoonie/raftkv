package raft

import (
	"testing"
	"time"
)

var smallSnapshots = Config{SnapshotThreshold: 10}

func TestSnapshotCompactsLog(t *testing.T) {
	c := newClusterWith(t, 3, smallSnapshots)
	want := commands("cmd", 50)
	for _, cmd := range want {
		c.propose(cmd)
	}
	c.waitApplied(want)

	for _, n := range c.nodes {
		eventually(t, 5*time.Second, "log compaction", func() bool {
			st := n.Status()
			return st.SnapshotIndex > 0 && st.LastIndex-st.SnapshotIndex < 2*smallSnapshots.SnapshotThreshold
		})
	}
}

// A follower that missed more entries than the leader still has must be
// brought up to date with InstallSnapshot.
func TestLaggingFollowerReceivesSnapshot(t *testing.T) {
	c := newClusterWith(t, 3, smallSnapshots)
	follower := (c.leader() + 1) % 3
	c.disconnect(follower)

	want := commands("cmd", 50)
	for _, cmd := range want {
		c.propose(cmd)
	}
	leader := c.leader()
	eventually(t, 5*time.Second, "leader to compact", func() bool {
		return c.nodes[leader].Status().SnapshotIndex > 0
	})

	c.connect(follower)
	c.waitApplied(want)
	if c.nodes[follower].Status().SnapshotIndex == 0 {
		t.Fatal("follower caught up without installing a snapshot")
	}

	// It keeps working normally after the snapshot.
	c.propose("after")
	c.waitApplied(append(want, "after"))
}

func TestRestartLoadsSnapshot(t *testing.T) {
	c := newClusterWith(t, 3, smallSnapshots)
	want := commands("cmd", 35)
	for _, cmd := range want {
		c.propose(cmd)
	}
	c.waitApplied(want)

	for i := range c.nodes {
		c.restart(i)
		if c.nodes[i].Status().SnapshotIndex == 0 {
			t.Fatalf("node %d restarted without its snapshot", i)
		}
	}
	c.waitApplied(want)
}

func TestRequestSnapshot(t *testing.T) {
	c := newCluster(t, 1)
	c.propose("x")
	c.waitApplied([]string{"x"})
	c.nodes[0].RequestSnapshot()
	eventually(t, 5*time.Second, "requested snapshot", func() bool {
		return c.nodes[0].Status().SnapshotIndex == c.nodes[0].Status().LastApplied
	})
}
