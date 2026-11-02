package resp

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"array", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n", []string{"SET", "k", "hello"}},
		{"binary-safe", "*2\r\n$3\r\nGET\r\n$4\r\na\r\nb\r\n", []string{"GET", "a\r\nb"}},
		{"inline", "SET k   hello\r\n", []string{"SET", "k", "hello"}},
		{"inline with bare LF", "PING\n", []string{"PING"}},
		{"skips empty lines", "\r\n\r\nPING\r\n", []string{"PING"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, err := NewReader(strings.NewReader(tt.input)).ReadCommand()
			if err != nil {
				t.Fatal(err)
			}
			if got := toStrings(args); !slices.Equal(got, tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadCommandPipelined(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\n*1\r\n$4\r\nPING\r\n"))
	for i := range 2 {
		if _, err := r.ReadCommand(); err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want EOF", err)
	}
}

func TestReadCommandRejectsBadInput(t *testing.T) {
	for _, input := range []string{
		"*1\r\n:5\r\n",          // argument is not a bulk string
		"*1\r\n$3\r\nabcde\r\n", // bulk string longer than declared
		"*-5\r\n",               // invalid length
		"*1\r\n$x\r\n",          // length is not a number
	} {
		_, err := NewReader(strings.NewReader(input)).ReadCommand()
		if !errors.Is(err, ErrProtocol) {
			t.Errorf("%q: got %v, want ErrProtocol", input, err)
		}
	}
}

func TestWriteValue(t *testing.T) {
	tests := []struct {
		v    Value
		want string
	}{
		{OK(), "+OK\r\n"},
		{Error("ERR bad\r\nthing"), "-ERR bad  thing\r\n"},
		{Integer(-42), ":-42\r\n"},
		{Bulk([]byte("hi")), "$2\r\nhi\r\n"},
		{Bulk([]byte{}), "$0\r\n\r\n"},
		{Null(), "$-1\r\n"},
		{Array(Integer(1), Bulk([]byte("a"))), "*2\r\n:1\r\n$1\r\na\r\n"},
		{Array(), "*0\r\n"},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		w.WriteValue(tt.v)
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if buf.String() != tt.want {
			t.Errorf("got %q, want %q", buf.String(), tt.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	want := Array(Simple("OK"), Error("ERR x"), Integer(7), Bulk([]byte("v")), Null(), Array(Integer(1)))
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteValue(want)
	w.Flush()

	got, err := NewReader(&buf).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Elems) != len(want.Elems) {
		t.Fatalf("got %d elements, want %d", len(got.Elems), len(want.Elems))
	}
	for i := range want.Elems {
		g, w := got.Elems[i], want.Elems[i]
		if g.Kind != w.Kind || g.Str != w.Str || g.Int != w.Int || string(g.Bulk) != string(w.Bulk) || g.Null != w.Null {
			t.Errorf("element %d: got %+v, want %+v", i, g, w)
		}
	}
}

func toStrings(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}
