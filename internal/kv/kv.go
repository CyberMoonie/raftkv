// Package kv is the replicated state machine: an in-memory key-value store
// that executes Redis commands. Raft hands it committed log entries, every
// node applies the same entries in the same order, and so every node ends up
// with the same data.
package kv

import (
	"fmt"
	"maps"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/CyberMoonie/raftkv/internal/kv/kvpb"
	"github.com/CyberMoonie/raftkv/internal/resp"
)

// Store is not safe for concurrent use: the Raft applier goroutine is its
// only caller.
type Store struct {
	items map[string]item
	// now is the logical clock in Unix milliseconds, taken from the commands
	// themselves (see Execute).
	now int64
	// frozen is set while a query runs: expired keys are hidden but not
	// deleted, so a read never changes one replica's state.
	frozen bool
}

type item struct {
	value    []byte
	expireAt int64 // Unix milliseconds, 0 means no expiry
}

func New() *Store {
	return &Store{items: make(map[string]item)}
}

type command struct {
	// arity counts the command name too. Positive means exactly that many
	// arguments, negative means at least -arity (the Redis convention).
	arity int
	// readOnly commands can be served by Query, outside the log.
	readOnly bool
	run      func(s *Store, args [][]byte) resp.Value
}

var commands = map[string]command{
	"GET":     {arity: 2, readOnly: true, run: (*Store).get},
	"SET":     {arity: -3, run: (*Store).set},
	"DEL":     {arity: -2, run: (*Store).del},
	"EXISTS":  {arity: -2, readOnly: true, run: (*Store).exists},
	"INCR":    {arity: 2, run: func(s *Store, args [][]byte) resp.Value { return s.incrBy(args[1], 1) }},
	"DECR":    {arity: 2, run: func(s *Store, args [][]byte) resp.Value { return s.incrBy(args[1], -1) }},
	"INCRBY":  {arity: 3, run: (*Store).incrByCmd},
	"EXPIRE":  {arity: 3, run: (*Store).expire},
	"TTL":     {arity: 2, readOnly: true, run: (*Store).ttl},
	"PERSIST": {arity: 2, run: (*Store).persist},
	"KEYS":    {arity: 2, readOnly: true, run: (*Store).keys},
	"DBSIZE":  {arity: 1, readOnly: true, run: (*Store).dbsize},
}

// IsReadOnly reports whether a valid command only reads data.
func IsReadOnly(args [][]byte) bool {
	return len(args) > 0 && commands[strings.ToUpper(string(args[0]))].readOnly
}

// Validate checks a command before it is proposed to Raft, so that unknown
// commands and wrong argument counts never reach the log.
func Validate(args [][]byte) error {
	if len(args) == 0 {
		return fmt.Errorf("ERR empty command")
	}
	name := strings.ToUpper(string(args[0]))
	c, ok := commands[name]
	if !ok {
		return fmt.Errorf("ERR unknown command '%s'", args[0])
	}
	if (c.arity > 0 && len(args) != c.arity) || (c.arity < 0 && len(args) < -c.arity) {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(name))
	}
	return nil
}

// Encode turns a command into log entry data. nowMs is the leader's clock at
// proposal time; storing it in the entry is what makes expiry deterministic.
func Encode(args [][]byte, nowMs int64) ([]byte, error) {
	return proto.Marshal(&kvpb.Command{Args: args, NowMs: nowMs})
}

// Apply executes one committed log entry and returns its reply. It is the
// method Raft calls on its state machine.
func (s *Store) Apply(_ uint64, data []byte) any {
	var cmd kvpb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return resp.Errorf("ERR corrupt log entry: %v", err)
	}
	return s.Execute(cmd.Args, cmd.NowMs)
}

// Execute runs one command at time nowMs. Replicas must not read their own
// clocks, or a key could expire on one node and still exist on another, so
// all time comes from the commands. The clock never moves backwards, even if
// a new leader's clock is behind the old leader's.
func (s *Store) Execute(args [][]byte, nowMs int64) resp.Value {
	if err := Validate(args); err != nil {
		return resp.Error(err.Error())
	}
	s.now = max(s.now, nowMs)
	return commands[strings.ToUpper(string(args[0]))].run(s, args)
}

// Query answers a read-only command encoded with Encode without changing the
// store. It is the method Raft calls for reads that skip the log (ReadIndex).
func (s *Store) Query(data []byte) any {
	var cmd kvpb.Command
	if err := proto.Unmarshal(data, &cmd); err != nil {
		return resp.Errorf("ERR corrupt query: %v", err)
	}
	return s.read(cmd.Args, cmd.NowMs)
}

// read runs a read-only command as if at time nowMs. Neither the clock nor
// expired keys are touched: this replica must stay identical to the others,
// which never see the query.
func (s *Store) read(args [][]byte, nowMs int64) resp.Value {
	if err := Validate(args); err != nil {
		return resp.Error(err.Error())
	}
	if !IsReadOnly(args) {
		return resp.Errorf("ERR '%s' is not a read-only command", args[0])
	}
	clock := s.now
	s.now, s.frozen = max(s.now, nowMs), true
	defer func() { s.now, s.frozen = clock, false }()
	return commands[strings.ToUpper(string(args[0]))].run(s, args)
}

// lookup returns a live item, deleting it first if it has expired (except
// during a query).
func (s *Store) lookup(key string) (item, bool) {
	it, ok := s.items[key]
	if ok && s.expired(it) {
		if !s.frozen {
			delete(s.items, key)
		}
		return item{}, false
	}
	return it, ok
}

func (s *Store) expired(it item) bool {
	return it.expireAt != 0 && it.expireAt <= s.now
}

// expireAt converts a relative TTL into an absolute time on the logical
// clock. ok is false for TTLs that would overflow.
func (s *Store) expireAt(ttl, unit int64) (int64, bool) {
	if ttl > (math.MaxInt64-s.now)/unit {
		return 0, false
	}
	return s.now + ttl*unit, true
}

func (s *Store) get(args [][]byte) resp.Value {
	it, ok := s.lookup(string(args[1]))
	if !ok {
		return resp.Null()
	}
	return resp.Bulk(it.value)
}

// set implements SET key value [EX seconds | PX milliseconds].
func (s *Store) set(args [][]byte) resp.Value {
	it := item{value: args[2]}
	for i := 3; i < len(args); i += 2 {
		unit := map[string]int64{"EX": 1000, "PX": 1}[strings.ToUpper(string(args[i]))]
		if unit == 0 || i+1 == len(args) || it.expireAt != 0 {
			return resp.Error("ERR syntax error")
		}
		ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil || ttl <= 0 {
			return resp.Error("ERR invalid expire time in 'set' command")
		}
		var ok bool
		if it.expireAt, ok = s.expireAt(ttl, unit); !ok {
			return resp.Error("ERR invalid expire time in 'set' command")
		}
	}
	s.items[string(args[1])] = it
	return resp.OK()
}

func (s *Store) del(args [][]byte) resp.Value {
	var n int64
	for _, key := range args[1:] {
		if _, ok := s.lookup(string(key)); ok {
			delete(s.items, string(key))
			n++
		}
	}
	return resp.Integer(n)
}

func (s *Store) exists(args [][]byte) resp.Value {
	var n int64
	for _, key := range args[1:] {
		if _, ok := s.lookup(string(key)); ok {
			n++
		}
	}
	return resp.Integer(n)
}

func (s *Store) incrByCmd(args [][]byte) resp.Value {
	delta, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	return s.incrBy(args[1], delta)
}

// incrBy adds delta to the integer stored at key, treating a missing key as
// 0. Like Redis, it keeps the key's TTL.
func (s *Store) incrBy(key []byte, delta int64) resp.Value {
	it, _ := s.lookup(string(key))
	var n int64
	if it.value != nil {
		var err error
		if n, err = strconv.ParseInt(string(it.value), 10, 64); err != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
	}
	if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
		return resp.Error("ERR increment or decrement would overflow")
	}
	n += delta
	it.value = strconv.AppendInt(nil, n, 10)
	s.items[string(key)] = it
	return resp.Integer(n)
}

// expire sets a TTL in seconds. A TTL of zero or less deletes the key, as in
// Redis.
func (s *Store) expire(args [][]byte) resp.Value {
	ttl, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	key := string(args[1])
	it, ok := s.lookup(key)
	if !ok {
		return resp.Integer(0)
	}
	if ttl <= 0 {
		delete(s.items, key)
		return resp.Integer(1)
	}
	if it.expireAt, ok = s.expireAt(ttl, 1000); !ok {
		return resp.Error("ERR invalid expire time in 'expire' command")
	}
	s.items[key] = it
	return resp.Integer(1)
}

// ttl returns the seconds left, -1 for a key without a TTL and -2 for a
// missing key.
func (s *Store) ttl(args [][]byte) resp.Value {
	it, ok := s.lookup(string(args[1]))
	switch {
	case !ok:
		return resp.Integer(-2)
	case it.expireAt == 0:
		return resp.Integer(-1)
	}
	return resp.Integer((it.expireAt - s.now + 500) / 1000)
}

func (s *Store) persist(args [][]byte) resp.Value {
	key := string(args[1])
	it, ok := s.lookup(key)
	if !ok || it.expireAt == 0 {
		return resp.Integer(0)
	}
	it.expireAt = 0
	s.items[key] = it
	return resp.Integer(1)
}

// keys returns the live keys matching a glob pattern such as "user:*",
// sorted so that every replica returns the same order.
func (s *Store) keys(args [][]byte) resp.Value {
	pattern := string(args[1])
	if _, err := path.Match(pattern, ""); err != nil {
		return resp.Error("ERR invalid pattern")
	}
	var names []string
	for key := range s.items {
		if _, ok := s.lookup(key); ok {
			if match, _ := path.Match(pattern, key); match {
				names = append(names, key)
			}
		}
	}
	slices.Sort(names)
	out := make([]resp.Value, len(names))
	for i, name := range names {
		out[i] = resp.Bulk([]byte(name))
	}
	return resp.Array(out...)
}

func (s *Store) dbsize(_ [][]byte) resp.Value {
	var n int64
	for key := range s.items {
		if _, ok := s.lookup(key); ok {
			n++
		}
	}
	return resp.Integer(n)
}

// Snapshot serializes the whole store for log compaction. Keys are sorted
// and expired items left out, so replicas with the same state produce the
// same bytes.
func (s *Store) Snapshot() ([]byte, error) {
	snap := &kvpb.Snapshot{NowMs: s.now}
	for _, key := range slices.Sorted(maps.Keys(s.items)) {
		it := s.items[key]
		if !s.expired(it) {
			snap.Items = append(snap.Items, &kvpb.Item{Key: key, Value: it.value, ExpireAtMs: it.expireAt})
		}
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(snap)
}

// Restore replaces the store with the contents of a snapshot.
func (s *Store) Restore(data []byte) error {
	var snap kvpb.Snapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return err
	}
	s.items = make(map[string]item, len(snap.Items))
	for _, it := range snap.Items {
		s.items[it.Key] = item{value: it.Value, expireAt: it.ExpireAtMs}
	}
	s.now = snap.NowMs
	return nil
}
