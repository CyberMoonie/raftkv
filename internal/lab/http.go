package lab

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"time"

	"github.com/CyberMoonie/raftkv/internal/raft"
)

//go:embed web
var web embed.FS

// Handler serves the dashboard and its API:
//
//	GET  /api/events   server-sent events: state, msg (packets), feed, reset
//	POST /api/command  {"node", "line"} → {"lines"}
//	POST /api/action   Action → {"error"}
func (l *Lab) Handler() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(web, "web")
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /api/events", l.serveEvents)
	mux.HandleFunc("POST /api/command", l.serveCommand)
	mux.HandleFunc("POST /api/action", l.serveAction)
	return mux
}

func (l *Lab) serveEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	rc := http.NewResponseController(w)
	events, unsubscribe := l.hub.subscribe()
	defer unsubscribe()

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		var frame []byte
		select {
		case <-r.Context().Done():
			return
		case frame = <-events:
		case <-ticker.C:
			frame = encodeEvent("state", l.State())
		}
		if _, err := w.Write(frame); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

func (l *Lab) serveCommand(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Node, Line string
		Follow     bool
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]any{"lines": l.Command(req.Node, req.Line, req.Follow)})
}

func (l *Lab) serveAction(w http.ResponseWriter, r *http.Request) {
	var a Action
	if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result := map[string]string{}
	if err := l.Do(a); err != nil {
		result["error"] = err.Error()
	}
	writeJSON(w, result)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// State is everything the dashboard draws, refreshed several times a second.
type State struct {
	Time        int64       `json:"time"` // Unix milliseconds
	Speed       string      `json:"speed"`
	PreVote     bool        `json:"preVote"`
	HeartbeatMs int64       `json:"heartbeatMs"`
	ElectionMs  int64       `json:"electionMs"`
	LatencyMs   int64       `json:"latencyMs"`
	DropPct     int         `json:"dropPct"`
	Threshold   uint64      `json:"snapshotThreshold"`
	Nodes       []NodeState `json:"nodes"`
}

type NodeState struct {
	ID       string `json:"id"`
	Up       bool   `json:"up"`
	Member   bool   `json:"member"`
	Group    int    `json:"group"`
	Redis    string `json:"redis,omitempty"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	VotedFor string `json:"votedFor"`
	Leader   string `json:"leader"`
	Commit   uint64 `json:"commit"`
	Applied  uint64 `json:"applied"`
	Last     uint64 `json:"last"`
	Snapshot uint64 `json:"snapshot"`
	// Election timer: time left before this node starts an election, and
	// the full random timeout it was drawn from.
	TimerLeftMs  int64      `json:"timerLeftMs"`
	TimerTotalMs int64      `json:"timerTotalMs"`
	Progress     []Progress `json:"progress,omitempty"`
	Log          []Entry    `json:"log"`
	Data         []Item     `json:"data"`
}

type Progress struct {
	ID    string `json:"id"`
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
}

type Entry struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"t"`
	Type  string `json:"type"` // command, noop, config
	Text  string `json:"text"`
}

type Item struct {
	Key   string `json:"k"`
	Value string `json:"v"`
	TTLMs int64  `json:"ttl"` // -1 without expiry
}

func (l *Lab) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	sp := speeds[l.speed]
	latency, drop := l.net.conditions()
	now := time.Now()
	st := State{
		Time:        now.UnixMilli(),
		Speed:       l.speed,
		PreVote:     l.preVote,
		HeartbeatMs: sp.Heartbeat.Milliseconds(),
		ElectionMs:  sp.Election.Milliseconds(),
		LatencyMs:   latency.Milliseconds(),
		DropPct:     int(drop*100 + 0.5),
		Threshold:   snapshotThreshold,
	}

	// The configuration is what the leader (or else any node) believes.
	members := map[string]bool{}
	if leader := l.leader(); leader != nil {
		for _, m := range leader.node.Status().Members {
			members[m.Id] = true
		}
	}

	for _, n := range l.nodes {
		ns := NodeState{ID: n.id(), Up: n.up, Group: l.net.group(n.id()), Role: "offline"}
		if l.opts.RedisPort > 0 {
			ns.Redis = n.member.ClientAddr
		}
		if n.up {
			s := n.node.Status()
			ns.Role = s.Role.String()
			ns.Term, ns.VotedFor, ns.Leader = s.Term, s.VotedFor, s.LeaderID
			ns.Commit, ns.Applied, ns.Last, ns.Snapshot = s.CommitIndex, s.LastApplied, s.LastIndex, s.SnapshotIndex
			if s.Role != raft.Leader {
				ns.TimerLeftMs = max(s.ElectionDeadline.Sub(now).Milliseconds(), 0)
				ns.TimerTotalMs = s.ElectionTimeout.Milliseconds()
			}
			for _, p := range s.Progress {
				ns.Progress = append(ns.Progress, Progress{ID: p.ID, Match: p.Match, Next: p.Next})
			}
			for _, e := range n.node.LogTail(logTail) {
				ns.Log = append(ns.Log, Entry{Index: e.Index, Term: e.Term, Type: entryType(e.Type.String()), Text: describe(e)})
			}
			for _, it := range n.store.items() {
				ttl := int64(-1)
				if it.ExpireAtMs != 0 {
					ttl = max(it.ExpireAtMs-now.UnixMilli(), 0)
				}
				ns.Data = append(ns.Data, Item{Key: it.Key, Value: string(it.Value), TTLMs: ttl})
			}
			if len(members) == 0 {
				for _, m := range s.Members {
					members[m.Id] = true
				}
			}
		}
		st.Nodes = append(st.Nodes, ns)
	}
	for i := range st.Nodes {
		st.Nodes[i].Member = members[st.Nodes[i].ID]
	}
	return st
}

func entryType(protoName string) string {
	switch protoName {
	case "ENTRY_TYPE_NOOP":
		return "noop"
	case "ENTRY_TYPE_CONFIG":
		return "config"
	}
	return "command"
}
