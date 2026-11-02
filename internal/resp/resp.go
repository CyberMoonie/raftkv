// Package resp implements the parts of RESP2, the Redis wire protocol, that
// raftkv needs: reading commands and replies, and writing them back.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxLineSize = 64 << 10 // same limit Redis uses for inline commands
	maxBulkSize = 64 << 20
	maxElems    = 1 << 20
)

// ErrProtocol is returned for input that is not valid RESP.
var ErrProtocol = errors.New("protocol error")

// Kind is the type of a Value, written as its first byte on the wire.
type Kind byte

const (
	KindSimple  Kind = '+'
	KindError   Kind = '-'
	KindInteger Kind = ':'
	KindBulk    Kind = '$'
	KindArray   Kind = '*'
)

// Value is one RESP value. Null marks a nil bulk string or array.
type Value struct {
	Kind  Kind
	Str   string  // KindSimple, KindError
	Int   int64   // KindInteger
	Bulk  []byte  // KindBulk
	Elems []Value // KindArray
	Null  bool
}

func OK() Value                  { return Simple("OK") }
func Simple(s string) Value      { return Value{Kind: KindSimple, Str: s} }
func Error(msg string) Value     { return Value{Kind: KindError, Str: msg} }
func Integer(n int64) Value      { return Value{Kind: KindInteger, Int: n} }
func Bulk(b []byte) Value        { return Value{Kind: KindBulk, Bulk: b} }
func Null() Value                { return Value{Kind: KindBulk, Null: true} }
func Array(elems ...Value) Value { return Value{Kind: KindArray, Elems: elems} }

// Errorf builds an error reply. By convention msg starts with an error code
// such as "ERR".
func Errorf(format string, args ...any) Value { return Error(fmt.Sprintf(format, args...)) }

// Reader reads RESP values from a connection.
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, maxLineSize)}
}

// Buffered returns how many bytes are already read but not yet consumed. A
// server uses it to flush replies once per pipelined batch instead of once per
// command.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one client command. Clients send an array of bulk
// strings; a plain text line (an "inline command", as typed into nc or
// telnet) is accepted too. Empty lines and empty arrays are skipped.
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		first, err := r.br.Peek(1)
		if err != nil {
			return nil, err
		}
		if Kind(first[0]) != KindArray {
			line, err := r.readLine()
			if err != nil {
				return nil, err
			}
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			args := make([][]byte, len(fields))
			for i, f := range fields {
				args[i] = []byte(f)
			}
			return args, nil
		}

		v, err := r.ReadValue()
		if err != nil {
			return nil, err
		}
		if len(v.Elems) == 0 {
			continue
		}
		args := make([][]byte, len(v.Elems))
		for i, e := range v.Elems {
			if e.Kind != KindBulk || e.Null {
				return nil, fmt.Errorf("%w: command arguments must be bulk strings", ErrProtocol)
			}
			args[i] = e.Bulk
		}
		return args, nil
	}
}

// ReadValue reads any RESP value. Clients use it to read replies.
func (r *Reader) ReadValue() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if line == "" {
		return Value{}, fmt.Errorf("%w: empty line", ErrProtocol)
	}

	kind, rest := Kind(line[0]), line[1:]
	switch kind {
	case KindSimple, KindError:
		return Value{Kind: kind, Str: rest}, nil

	case KindInteger:
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("%w: invalid integer %q", ErrProtocol, rest)
		}
		return Integer(n), nil

	case KindBulk:
		n, err := parseLength(rest, maxBulkSize)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Null(), nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return Value{}, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return Value{}, fmt.Errorf("%w: bulk string not terminated by CRLF", ErrProtocol)
		}
		return Bulk(buf[:n]), nil

	case KindArray:
		n, err := parseLength(rest, maxElems)
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return Value{Kind: KindArray, Null: true}, nil
		}
		// The length comes from the peer, so it only caps the initial
		// allocation; the slice grows as real elements arrive.
		elems := make([]Value, 0, min(n, 1024))
		for range n {
			e, err := r.ReadValue()
			if err != nil {
				return Value{}, err
			}
			elems = append(elems, e)
		}
		return Array(elems...), nil
	}
	return Value{}, fmt.Errorf("%w: unexpected type byte %q", ErrProtocol, line[0])
}

// readLine reads up to the next newline and strips the line ending. RESP
// requires CRLF; a bare LF is accepted so inline commands typed by hand work.
func (r *Reader) readLine() (string, error) {
	line, err := r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", fmt.Errorf("%w: line too long", ErrProtocol)
	}
	if err != nil {
		return "", err
	}
	s := strings.TrimSuffix(string(line), "\n")
	return strings.TrimSuffix(s, "\r"), nil
}

func parseLength(s string, limit int) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < -1 || n > limit {
		return 0, fmt.Errorf("%w: invalid length %q", ErrProtocol, s)
	}
	return n, nil
}

// Writer writes RESP values. Write errors are kept by the underlying
// bufio.Writer and returned by Flush.
type Writer struct {
	bw *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

// Simple strings and errors are one line on the wire, so line breaks inside
// them would corrupt the stream.
var lineBreaks = strings.NewReplacer("\r", " ", "\n", " ")

func (w *Writer) WriteValue(v Value) {
	switch v.Kind {
	case KindSimple, KindError:
		w.bw.WriteByte(byte(v.Kind))
		w.bw.WriteString(lineBreaks.Replace(v.Str))
		w.bw.WriteString("\r\n")

	case KindInteger:
		w.writeHeader(KindInteger, v.Int)

	case KindBulk:
		if v.Null {
			w.bw.WriteString("$-1\r\n")
			return
		}
		w.writeHeader(KindBulk, int64(len(v.Bulk)))
		w.bw.Write(v.Bulk)
		w.bw.WriteString("\r\n")

	case KindArray:
		if v.Null {
			w.bw.WriteString("*-1\r\n")
			return
		}
		w.writeHeader(KindArray, int64(len(v.Elems)))
		for _, e := range v.Elems {
			w.WriteValue(e)
		}
	}
}

// WriteCommand writes a command the way clients send it: an array of bulk
// strings.
func (w *Writer) WriteCommand(args ...string) {
	elems := make([]Value, len(args))
	for i, a := range args {
		elems[i] = Bulk([]byte(a))
	}
	w.WriteValue(Array(elems...))
}

func (w *Writer) Flush() error { return w.bw.Flush() }

func (w *Writer) writeHeader(kind Kind, n int64) {
	w.bw.WriteByte(byte(kind))
	w.bw.WriteString(strconv.FormatInt(n, 10))
	w.bw.WriteString("\r\n")
}
