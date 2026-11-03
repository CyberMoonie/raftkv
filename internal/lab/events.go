package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// hub fans events out to every connected dashboard as server-sent events
// and keeps the recent feed for dashboards that connect later.
type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
	feed [][]byte // recent "feed" events, oldest first
}

const feedBacklog = 200

func newHub() *hub { return &hub{subs: make(map[chan []byte]struct{})} }

func encodeEvent(event string, data any) []byte {
	payload, err := json.Marshal(data)
	if err != nil {
		payload, _ = json.Marshal(map[string]string{"error": err.Error()})
	}
	return fmt.Appendf(nil, "event: %s\ndata: %s\n\n", event, payload)
}

func (h *hub) publish(event string, data any) {
	frame := encodeEvent(event, data)
	h.mu.Lock()
	defer h.mu.Unlock()
	if event == "feed" {
		h.feed = append(h.feed, frame)
		if len(h.feed) > feedBacklog {
			h.feed = h.feed[len(h.feed)-feedBacklog:]
		}
	}
	for ch := range h.subs {
		select {
		case ch <- frame:
		default: // a slow dashboard misses packets rather than slowing the lab
		}
	}
}

// subscribe returns a channel of events, starting with the feed backlog.
func (h *hub) subscribe() (chan []byte, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan []byte, 1024)
	for _, frame := range h.feed {
		ch <- frame
	}
	h.subs[ch] = struct{}{}
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs, ch)
	}
}

// feedEvent is one line of the dashboard's event feed.
type feedEvent struct {
	Time int64  `json:"t"` // Unix milliseconds
	Cat  string `json:"cat"`
	Node string `json:"node,omitempty"`
	Text string `json:"text"`
}

func (h *hub) feedf(cat, node, format string, args ...any) {
	h.publish("feed", feedEvent{Time: time.Now().UnixMilli(), Cat: cat, Node: node, Text: fmt.Sprintf(format, args...)})
}

// feedHandler is a slog.Handler that turns the nodes' log lines into feed
// events explained in plain words.
type feedHandler struct {
	hub   *hub
	attrs []slog.Attr
}

func (h feedHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo
}

func (h feedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return feedHandler{hub: h.hub, attrs: append(h.attrs[:len(h.attrs):len(h.attrs)], attrs...)}
}

func (h feedHandler) WithGroup(string) slog.Handler { return h }

func (h feedHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]slog.Value{}
	for _, a := range h.attrs {
		attrs[a.Key] = a.Value
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		return true
	})
	node := attrs["node"].String()
	cat, text := explain(r.Message, node, attrs)
	if r.Level >= slog.LevelWarn {
		cat = "error"
	}
	h.hub.publish("feed", feedEvent{Time: r.Time.UnixMilli(), Cat: cat, Node: node, Text: text})
	return nil
}

// explain rewrites a Raft log message for someone learning Raft.
func explain(msg, node string, a map[string]slog.Value) (cat, text string) {
	switch msg {
	case "starting pre-vote":
		return "election", fmt.Sprintf("%s heard no heartbeat in time. It asks the others for PRE-VOTES: \"would you elect me in term %s?\" Its own term does not change yet.", node, a["term"])
	case "starting election":
		return "election", fmt.Sprintf("%s becomes a CANDIDATE: it moves to term %s, votes for itself and asks everyone for their vote.", node, a["term"])
	case "became leader":
		return "leader", fmt.Sprintf("%s won a majority of votes: it is the LEADER of term %s. It appends a no-op entry and starts sending heartbeats.", node, a["term"])
	case "became follower":
		return "election", fmt.Sprintf("%s steps down to FOLLOWER (term %s).", node, a["term"])
	case "took snapshot":
		return "snapshot", fmt.Sprintf("%s compacted its log: everything up to entry #%s is now one snapshot of %s bytes, and those entries were deleted.", node, a["index"], a["bytes"])
	case "installed snapshot from leader":
		return "snapshot", fmt.Sprintf("%s was so far behind that the leader had already deleted the entries it needed, so it received the leader's SNAPSHOT (up to #%s) instead.", node, a["index"])
	case "changing membership":
		return "membership", fmt.Sprintf("Leader %s proposes a new membership: [%s]. It becomes the cluster's configuration as soon as it is in the log.", node, formatList(a["members"]))
	case "removed from the cluster, stepping down":
		return "membership", fmt.Sprintf("%s removed itself from the cluster. The change is committed, so it stops leading and the others will elect a new leader.", node)
	}
	return "system", node + ": " + msg
}

func formatList(v slog.Value) string {
	if ids, ok := v.Any().([]string); ok {
		return strings.Join(ids, ", ")
	}
	return v.String()
}
