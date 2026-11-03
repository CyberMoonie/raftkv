package raft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/CyberMoonie/raftkv/internal/kv"
	"github.com/CyberMoonie/raftkv/internal/raft/raftpb"
	"github.com/CyberMoonie/raftkv/internal/resp"
)

// kvInput is one client operation: a SET (put) or a GET.
type kvInput struct {
	put   bool
	key   string
	value string
}

// kvModel is a map of registers, checked one key at a time: a history is
// linearizable exactly when the history of every key is.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			key := op.Input.(kvInput).key
			byKey[key] = append(byKey[key], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		in := input.(kvInput)
		if in.put {
			return true, in.value
		}
		return output.(string) == state.(string), state
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		if in.put {
			return fmt.Sprintf("set(%s, %s)", in.key, in.value)
		}
		return fmt.Sprintf("get(%s) -> %q", in.key, output)
	},
}

// kvClient sends commands to whichever node it believes leads, following
// redirects and retrying, like a real Redis client would.
type kvClient struct {
	c      *cluster
	leader int
}

// do runs one command until it succeeds or the deadline passes. A write is
// only retried when the error proves it was not applied (no leader there,
// or its entry was overwritten); after a timeout it may still commit later,
// and sending it again could apply it twice. ok is false if the outcome is
// unknown.
func (cl *kvClient) do(deadline time.Time, args ...string) (resp.Value, bool) {
	bargs := make([][]byte, len(args))
	for i, a := range args {
		bargs[i] = []byte(a)
	}
	for time.Now().Before(deadline) {
		data, _ := kv.Encode(bargs, time.Now().UnixMilli())
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		node := cl.c.node(cl.leader)
		var v any
		var err error
		if kv.IsReadOnly(bargs) {
			v, err = node.Read(ctx, data)
		} else {
			v, err = node.Propose(ctx, data)
		}
		cancel()
		if err == nil {
			return v.(resp.Value), true
		}
		var notLeader *NotLeaderError
		if !kv.IsReadOnly(bargs) && !errors.As(err, &notLeader) && !errors.Is(err, ErrLeadershipLost) {
			cl.leader = (cl.leader + 1) % len(cl.c.members)
			return resp.Value{}, false
		}
		if errors.As(err, &notLeader) && notLeader.Leader != nil {
			cl.leader = slices.IndexFunc(cl.c.members, func(m *raftpb.Member) bool { return m.Id == notLeader.Leader.Id })
		} else {
			cl.leader = (cl.leader + 1) % len(cl.c.members)
		}
	}
	return resp.Value{}, false
}

// TestLinearizable runs concurrent clients against a 5-node cluster of real
// key-value stores while a chaos goroutine crashes, restarts, disconnects and
// reconnects nodes, then checks with Porcupine that every result could have
// come from a single copy of the data applying operations one at a time.
func TestLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	const (
		clients = 5
		chaos   = 4 * time.Second
		calm    = time.Second
	)
	c := newClusterOf(t, 5, Config{SnapshotThreshold: 25}, func() StateMachine { return kv.New() })
	c.leader()

	seed := rand.Uint64()
	t.Logf("chaos seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	start := time.Now()
	stop := make(chan struct{})
	var chaosDone sync.WaitGroup
	chaosDone.Add(1)
	go func() {
		defer chaosDone.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(100+rng.IntN(200)) * time.Millisecond):
			}
			i := rng.IntN(len(c.nodes))
			switch rng.IntN(5) {
			case 0, 1:
				c.setDown(i, !c.isDown(i))
			case 2:
				c.restart(i)
			case 3:
				// Cut off the leader: clients still talking to it are the
				// ones that could see stale data.
				for j := range c.nodes {
					if c.node(j).Status().Role == Leader && !c.isDown(j) {
						c.disconnect(j)
						break
					}
				}
			case 4:
				for j := range c.nodes {
					c.connect(j)
				}
			}
		}
	}()

	var (
		mu       sync.Mutex
		history  []porcupine.Operation
		writes   atomic.Int64
		failures atomic.Int64
	)
	end := start.Add(chaos + calm)
	var wg sync.WaitGroup
	for id := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := &kvClient{c: c, leader: id % len(c.nodes)}
			for seq := 0; time.Now().Before(end); seq++ {
				in := kvInput{put: rand.IntN(2) == 0, key: fmt.Sprintf("k%d", rand.IntN(3))}
				call := time.Since(start).Nanoseconds()
				var v resp.Value
				var ok bool
				if in.put {
					in.value = fmt.Sprintf("c%d-%d", id, seq)
					v, ok = cl.do(time.Now().Add(2*time.Second), "SET", in.key, in.value)
				} else {
					v, ok = cl.do(time.Now().Add(2*time.Second), "GET", in.key)
				}
				op := porcupine.Operation{ClientId: id, Input: in, Call: call, Return: time.Since(start).Nanoseconds()}
				switch {
				case ok && in.put:
					writes.Add(1)
				case ok:
					op.Output = string(v.Bulk)
				case in.put:
					// The write may or may not have happened, at any time
					// from now on.
					op.Return = math.MaxInt64
					failures.Add(1)
				default:
					// A read that never answered tells us nothing.
					failures.Add(1)
					continue
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
			}
		}()
	}

	time.Sleep(chaos)
	close(stop)
	chaosDone.Wait()
	for i := range c.nodes {
		c.connect(i)
	}
	wg.Wait()

	t.Logf("%d operations (%d acknowledged writes), %d timed out", len(history), writes.Load(), failures.Load())
	if writes.Load() < 50 {
		t.Fatalf("only %d writes succeeded; the cluster made too little progress to test anything", writes.Load())
	}
	result, info := porcupine.CheckOperationsVerbose(kvModel, history, 30*time.Second)
	if result != porcupine.Ok {
		path := filepath.Join(os.TempDir(), fmt.Sprintf("raftkv-linearizability-%d.html", seed))
		if err := porcupine.VisualizePath(kvModel, info, path); err != nil {
			t.Logf("writing visualization: %v", err)
		}
		t.Fatalf("history is not linearizable (%v); see %s", result, path)
	}
}
