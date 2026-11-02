package raft

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
)

var (
	// ErrConfigChangeInProgress means the previous membership change has not
	// committed yet, or the leader has not committed an entry of its term.
	// Retrying shortly is safe.
	ErrConfigChangeInProgress = errors.New("raft: a membership change is already in progress")
	ErrUnknownMember          = errors.New("raft: no member with this ID")
)

// AddMember adds a node to the cluster and returns once the change is
// committed. The new node is started without Bootstrap; the leader then
// sends it the log, or a snapshot. Adding a member that is already present
// with the same addresses succeeds without a new change.
func (n *Node) AddMember(ctx context.Context, m *raftpb.Member) error {
	if m.GetId() == "" || m.GetRaftAddr() == "" {
		return errors.New("raft: a member needs an ID and a Raft address")
	}
	return n.changeConfig(ctx, func(members []*raftpb.Member) ([]*raftpb.Member, error) {
		for _, old := range members {
			if old.Id == m.Id {
				if proto.Equal(old, m) {
					return nil, nil
				}
				return nil, fmt.Errorf("raft: member %s already exists with other addresses", m.Id)
			}
		}
		return append(slices.Clone(members), m), nil
	})
}

// RemoveMember removes a node from the cluster and returns once the change
// is committed. The leader may remove itself: it keeps leading until the
// change commits, then steps down.
func (n *Node) RemoveMember(ctx context.Context, id string) error {
	return n.changeConfig(ctx, func(members []*raftpb.Member) ([]*raftpb.Member, error) {
		i := slices.IndexFunc(members, func(m *raftpb.Member) bool { return m.Id == id })
		if i < 0 {
			return nil, ErrUnknownMember
		}
		if len(members) == 1 {
			return nil, errors.New("raft: cannot remove the last member")
		}
		return slices.Delete(slices.Clone(members), i, i+1), nil
	})
}

// changeConfig appends a configuration that differs from the current one by
// a single server. Any majority of the old configuration then overlaps any
// majority of the new one, so the two can never elect separate leaders, and
// the new configuration can take effect as soon as it is appended.
//
// That argument holds only for one change at a time, so a change waits until
// the previous one is committed. It also waits until the leader has committed
// an entry of its own term: otherwise a change left uncommitted by an earlier
// leader could be overwritten after this one was built on it.
func (n *Node) changeConfig(ctx context.Context, change func([]*raftpb.Member) ([]*raftpb.Member, error)) error {
	n.mu.Lock()
	if err := n.checkLeader(); err != nil {
		n.mu.Unlock()
		return err
	}
	if n.configIndex > n.commitIndex || n.commitIndex < n.leaderStart {
		n.mu.Unlock()
		return ErrConfigChangeInProgress
	}
	members, err := change(n.members)
	if members == nil || err != nil {
		n.mu.Unlock()
		return err
	}
	data, err := proto.Marshal(&raftpb.Configuration{Members: members})
	if err != nil {
		n.mu.Unlock()
		return err
	}
	n.logger.Info("changing membership", "members", memberIDs(members))
	_, err = n.propose(ctx, raftpb.EntryType_ENTRY_TYPE_CONFIG, data)
	return err
}

// configAt returns the configuration in effect at log index i, and the index
// it was written at: the latest configuration entry up to i, or the
// snapshot's configuration if the log holds none.
func (n *Node) configAt(i uint64) ([]*raftpb.Member, uint64) {
	for ; i > n.log.firstIndex(); i-- {
		e := n.log.entry(i)
		if e.Type != raftpb.EntryType_ENTRY_TYPE_CONFIG {
			continue
		}
		var cfg raftpb.Configuration
		if err := proto.Unmarshal(e.Data, &cfg); err != nil {
			panic(fmt.Sprintf("raft %s: corrupt configuration entry %d: %v", n.id, e.Index, err))
		}
		return cfg.Members, e.Index
	}
	return n.snapshot.GetConfig().GetMembers(), n.log.firstIndex()
}

// refreshConfig switches to the latest configuration in the log.
func (n *Node) refreshConfig() {
	n.members, n.configIndex = n.configAt(n.log.lastIndex())
}

// syncReplicators makes the leader replicate to exactly the current members.
func (n *Node) syncReplicators() {
	if n.role != Leader {
		return
	}
	for id, r := range n.replicators {
		if n.member(id) == nil {
			close(r.stop)
			delete(n.replicators, id)
			delete(n.nextIndex, id)
			delete(n.matchIndex, id)
		}
	}
	for _, m := range n.members {
		if _, ok := n.replicators[m.Id]; !ok && m.Id != n.id {
			n.nextIndex[m.Id] = n.log.lastIndex() + 1
			n.startReplicator(m)
		}
	}
}

// stepDownIfRemoved ends the leadership of a leader that removed itself,
// once that change is committed. Until then it still had to lead, to get the
// change committed.
func (n *Node) stepDownIfRemoved() {
	if n.role == Leader && n.configIndex <= n.commitIndex && n.member(n.id) == nil {
		n.logger.Info("removed from the cluster, stepping down", "term", n.term)
		n.becomeFollower(n.term)
	}
}

func memberIDs(members []*raftpb.Member) []string {
	ids := make([]string, len(members))
	for i, m := range members {
		ids[i] = m.Id
	}
	return ids
}
