package lab

import (
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/CyberMoonie/raftkv/internal/resp"
)

func TestSplitArgs(t *testing.T) {
	got, err := splitArgs(`SET  greeting "hello world" EX 10`)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"SET", "greeting", "hello world", "EX", "10"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if _, err := splitArgs(`SET k "open`); err == nil {
		t.Fatal("accepted unbalanced quotes")
	}
}

func TestFormatReplyLikeRedisCLI(t *testing.T) {
	v := resp.Array(resp.Bulk([]byte("a")), resp.Integer(2), resp.Null())
	want := []string{`1) "a"`, "2) (integer) 2", "3) (nil)"}
	if got := formatReply(v, ""); !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := formatReply(resp.Array(), ""); got[0] != "(empty array)" {
		t.Fatalf("got %q", got)
	}
}

// newTestLab starts a lab at fast speed without Redis ports.
func newTestLab(t *testing.T) *Lab {
	t.Helper()
	l, err := New(Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	if err := l.Do(Action{Type: "reset", Speed: "fast", PreVote: true}); err != nil {
		t.Fatal(err)
	}
	return l
}

func waitLeader(t *testing.T, l *Lab) NodeState {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range l.State().Nodes {
			if n.Role == "leader" {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return NodeState{}
}

func TestCommandFollowsRedirectsAndCrashesElectNewLeader(t *testing.T) {
	l := newTestLab(t)
	leader := waitLeader(t, l)
	follower := "n1"
	if leader.ID == "n1" {
		follower = "n2"
	}
	// The follower redirects only once a heartbeat told it who leads.
	deadline := time.Now().Add(5 * time.Second)
	for !slices.ContainsFunc(l.State().Nodes, func(n NodeState) bool { return n.ID == follower && n.Leader == leader.ID }) {
		if time.Now().After(deadline) {
			t.Fatal("follower never learned the leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	out := l.Command(follower, "SET user:1 Neo", true)
	if len(out) != 2 || out[0] != "-> Redirected to "+leader.ID || out[1] != "OK" {
		t.Fatalf("SET via follower: %q", out)
	}
	if out := l.Command(follower, "GET user:1", false); !strings.HasPrefix(out[0], "(error) MOVED") {
		t.Fatalf("without -c a follower must answer MOVED, got %q", out)
	}

	if err := l.Do(Action{Type: "crash", Node: leader.ID}); err != nil {
		t.Fatal(err)
	}
	next := waitLeader(t, l)
	if next.ID == leader.ID || next.Term <= leader.Term {
		t.Fatalf("after the crash: leader %s term %d", next.ID, next.Term)
	}
	if out := l.Command(next.ID, "GET user:1", true); len(out) != 1 || out[0] != `"Neo"` {
		t.Fatalf("GET after failover: %q", out)
	}
	if out := l.Command(leader.ID, "GET user:1", true); !strings.HasPrefix(out[0], "Could not connect") {
		t.Fatalf("crashed node answered: %q", out)
	}
}

func TestStateShowsDataAndPartitions(t *testing.T) {
	l := newTestLab(t)
	leader := waitLeader(t, l)
	l.Command(leader.ID, "SET k v", true)
	if err := l.Do(Action{Type: "isolate", Node: "n3"}); err != nil {
		t.Fatal(err)
	}

	st := l.State()
	for _, n := range st.Nodes {
		if !n.Member || !n.Up {
			t.Fatalf("%s: member %v up %v", n.ID, n.Member, n.Up)
		}
		if n.ID == "n3" && n.Group == 0 {
			t.Fatal("n3 is not shown as isolated")
		}
	}
	if !slices.ContainsFunc(st.Nodes, func(n NodeState) bool {
		return n.ID == leader.ID && len(n.Data) == 1 && n.Data[0].Key == "k" && len(n.Log) > 0
	}) {
		t.Fatal("leader's data or log missing from the state")
	}
}
