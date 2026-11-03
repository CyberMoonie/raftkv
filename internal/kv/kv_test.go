package kv

import (
	"strings"
	"testing"

	"github.com/CyberMoonie/raftkv/internal/resp"
)

func toArgs(cmd string) [][]byte {
	fields := strings.Fields(cmd)
	args := make([][]byte, len(fields))
	for i, f := range fields {
		args[i] = []byte(f)
	}
	return args
}

// run executes a space-separated command at time now (Unix ms).
func run(s *Store, now int64, cmd string) resp.Value {
	return s.Execute(toArgs(cmd), now)
}

func wantInt(t *testing.T, v resp.Value, want int64) {
	t.Helper()
	if v.Kind != resp.KindInteger || v.Int != want {
		t.Fatalf("got %+v, want integer %d", v, want)
	}
}

func wantBulk(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.Kind != resp.KindBulk || v.Null || string(v.Bulk) != want {
		t.Fatalf("got %+v, want bulk %q", v, want)
	}
}

func wantNull(t *testing.T, v resp.Value) {
	t.Helper()
	if !v.Null {
		t.Fatalf("got %+v, want null", v)
	}
}

func wantError(t *testing.T, v resp.Value, prefix string) {
	t.Helper()
	if v.Kind != resp.KindError || !strings.HasPrefix(v.Str, prefix) {
		t.Fatalf("got %+v, want error starting with %q", v, prefix)
	}
}

func TestSetGetDel(t *testing.T) {
	s := New()
	wantNull(t, run(s, 0, "GET k"))
	if v := run(s, 0, "SET k hello"); v.Str != "OK" {
		t.Fatalf("SET: %+v", v)
	}
	wantBulk(t, run(s, 0, "GET k"), "hello")
	wantInt(t, run(s, 0, "EXISTS k k missing"), 2)
	wantInt(t, run(s, 0, "DEL k missing"), 1)
	wantNull(t, run(s, 0, "GET k"))
}

func TestCommandNamesAreCaseInsensitive(t *testing.T) {
	s := New()
	run(s, 0, "set k v")
	wantBulk(t, run(s, 0, "gEt k"), "v")
}

func TestIncr(t *testing.T) {
	s := New()
	wantInt(t, run(s, 0, "INCR n"), 1)
	wantInt(t, run(s, 0, "INCRBY n 10"), 11)
	wantInt(t, run(s, 0, "DECR n"), 10)
	run(s, 0, "SET word abc")
	wantError(t, run(s, 0, "INCR word"), "ERR value is not an integer")
	run(s, 0, "SET big 9223372036854775807")
	wantError(t, run(s, 0, "INCR big"), "ERR increment or decrement would overflow")
}

func TestExpiryUsesCommandTime(t *testing.T) {
	s := New()
	run(s, 1_000, "SET k v EX 10")
	wantInt(t, run(s, 1_000, "TTL k"), 10)
	wantBulk(t, run(s, 10_999, "GET k"), "v")
	wantNull(t, run(s, 11_000, "GET k"))
	wantInt(t, run(s, 11_000, "TTL k"), -2)
}

func TestClockNeverMovesBackwards(t *testing.T) {
	s := New()
	run(s, 1_000, "SET k v PX 100")
	run(s, 2_000, "DBSIZE")
	// A command stamped with an older time (a new leader with a slower
	// clock) must not bring the key back.
	wantNull(t, run(s, 1_050, "GET k"))
}

func TestExpireAndPersist(t *testing.T) {
	s := New()
	wantInt(t, run(s, 0, "EXPIRE missing 10"), 0)
	run(s, 0, "SET k v")
	wantInt(t, run(s, 0, "TTL k"), -1)
	wantInt(t, run(s, 0, "EXPIRE k 5"), 1)
	wantInt(t, run(s, 0, "PERSIST k"), 1)
	wantInt(t, run(s, 0, "TTL k"), -1)
	wantInt(t, run(s, 0, "EXPIRE k 0"), 1)
	wantInt(t, run(s, 0, "EXISTS k"), 0)
}

func TestIncrKeepsTTL(t *testing.T) {
	s := New()
	run(s, 0, "SET n 1 EX 10")
	run(s, 0, "INCR n")
	wantInt(t, run(s, 0, "TTL n"), 10)
}

func TestKeysAndDBSize(t *testing.T) {
	s := New()
	run(s, 0, "SET user:2 b")
	run(s, 0, "SET user:1 a")
	run(s, 0, "SET order:1 c")
	run(s, 0, "SET user:3 gone PX 1")
	v := run(s, 10, "KEYS user:*")
	if len(v.Elems) != 2 || string(v.Elems[0].Bulk) != "user:1" || string(v.Elems[1].Bulk) != "user:2" {
		t.Fatalf("KEYS: %+v", v)
	}
	wantInt(t, run(s, 10, "DBSIZE"), 3)
}

func TestValidate(t *testing.T) {
	s := New()
	wantError(t, run(s, 0, "NOPE"), "ERR unknown command")
	wantError(t, run(s, 0, "GET"), "ERR wrong number of arguments for 'get'")
	wantError(t, run(s, 0, "SET k v EX"), "ERR syntax error")
	wantError(t, run(s, 0, "SET k v EX 0"), "ERR invalid expire time")
	wantError(t, run(s, 0, "SET k v EX 1 PX 1"), "ERR syntax error")
}

func TestApplyDecodesEncodedCommand(t *testing.T) {
	s := New()
	data, err := Encode([][]byte{[]byte("SET"), []byte("k"), []byte("v")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if v := s.Apply(1, data).(resp.Value); v.Str != "OK" {
		t.Fatalf("Apply: %+v", v)
	}
	wantBulk(t, run(s, 0, "GET k"), "v")
}

func TestSnapshotRestore(t *testing.T) {
	s := New()
	run(s, 1_000, "SET a 1")
	run(s, 1_000, "SET b 2 EX 60")
	run(s, 1_000, "SET gone x PX 10")
	run(s, 2_000, "INCR a")
	data, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	r := New()
	if err := r.Restore(data); err != nil {
		t.Fatal(err)
	}
	wantBulk(t, run(r, 2_000, "GET a"), "2")
	wantInt(t, run(r, 2_000, "TTL b"), 59)
	wantInt(t, run(r, 2_000, "EXISTS gone"), 0)

	// Restoring replaces the old state, and equal states give equal bytes.
	run(r, 2_000, "SET extra 1")
	r.Restore(data)
	wantInt(t, run(r, 2_000, "EXISTS extra"), 0)
	again, _ := r.Snapshot()
	if string(again) != string(data) {
		t.Fatal("snapshot of a restored store differs from the original")
	}
}

func TestQueryDoesNotChangeTheStore(t *testing.T) {
	s := New()
	run(s, 1_000, "SET k v PX 100")
	run(s, 1_000, "SET live v")
	data, _ := Encode([][]byte{[]byte("GET"), []byte("k")}, 5_000)

	wantNull(t, s.Query(data).(resp.Value))
	// The expired key is hidden but still stored, and the clock did not
	// move, exactly like on replicas that never saw the query.
	if len(s.items) != 2 || s.now != 1_000 {
		t.Fatalf("query changed the store: %d items, clock %d", len(s.items), s.now)
	}
	wantBulk(t, s.read([][]byte{[]byte("GET"), []byte("live")}, 5_000), "v")
	wantError(t, s.read([][]byte{[]byte("SET"), []byte("k"), []byte("v")}, 0), "ERR 'SET' is not a read-only command")
}

func TestIsReadOnly(t *testing.T) {
	for cmd, want := range map[string]bool{"GET k": true, "ttl k": true, "KEYS *": true, "SET k v": false, "INCR k": false, "NOPE": false} {
		if got := IsReadOnly(toArgs(cmd)); got != want {
			t.Errorf("IsReadOnly(%q) = %v, want %v", cmd, got, want)
		}
	}
}
