// Package resp implements the subset of the RESP (REdis Serialization Protocol)
// needed to talk to standard Redis clients such as redis-cli.
//
// Requests arrive as an array of bulk strings (the form every Redis client
// sends); an inline space-separated fallback is also accepted for convenience
// when poking at the server with a raw socket. Replies use the simple-string,
// error, integer, bulk-string and array types.
package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// ErrProtocol indicates a malformed client request.
var ErrProtocol = errors.New("resp: protocol error")

// Protocol limits matching Redis's defaults, enforced before any allocation so a
// tiny crafted header (e.g. "$9223372036854775806\r\n" or "*2000000000\r\n")
// cannot drive a huge/overflowing allocation and OOM or panic the server.
const (
	// MaxMultiBulk caps the number of elements in a command array.
	MaxMultiBulk = 1 << 20 // 1,048,576
	// MaxBulkLen caps the byte length of a single bulk string (512 MiB).
	MaxBulkLen = 512 << 20
)

// Reader parses client commands from a connection.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReader(r)} }

// ReadCommand reads one command and returns its arguments. The first element is
// the command name. It returns io.EOF when the client disconnects.
func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return r.ReadCommand() // tolerate blank lines between commands
	}

	if line[0] != '*' {
		return splitInline(line), nil // inline command
	}

	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 || n > MaxMultiBulk {
		return nil, ErrProtocol
	}
	// Cap the initial capacity so a large-but-undelivered count can't force a
	// big up-front allocation; append grows it as real elements arrive.
	args := make([][]byte, 0, min(n, 64))
	for i := 0; i < n; i++ {
		hdr, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, ErrProtocol
		}
		length, err := strconv.Atoi(string(hdr[1:]))
		if err != nil || length < 0 || length > MaxBulkLen {
			return nil, ErrProtocol
		}
		buf := make([]byte, length+2) // payload + trailing CRLF
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		args = append(args, buf[:length])
	}
	return args, nil
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return trimCRLF(line), nil
}

func trimCRLF(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

func splitInline(line []byte) [][]byte {
	var args [][]byte
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}
		start := i
		for i < len(line) && line[i] != ' ' {
			i++
		}
		args = append(args, line[start:i])
	}
	return args
}

// Writer serializes replies to a connection. Callers must Flush after writing a
// complete reply.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer { return &Writer{w: bufio.NewWriter(w)} }

func (w *Writer) Flush() error { return w.w.Flush() }

// WriteSimple writes a simple string: +<s>\r\n
func (w *Writer) WriteSimple(s string) error {
	w.w.WriteByte('+')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteError writes an error reply: -<s>\r\n
func (w *Writer) WriteError(s string) error {
	w.w.WriteByte('-')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteInt writes an integer reply: :<n>\r\n
func (w *Writer) WriteInt(n int64) error {
	w.w.WriteByte(':')
	w.w.WriteString(strconv.FormatInt(n, 10))
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteBulk writes a bulk string. A nil slice is encoded as the null bulk
// string ($-1), which Redis clients render as (nil).
func (w *Writer) WriteBulk(b []byte) error {
	if b == nil {
		return w.WriteNull()
	}
	w.w.WriteByte('$')
	w.w.WriteString(strconv.Itoa(len(b)))
	w.w.WriteString("\r\n")
	w.w.Write(b)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteNull writes the null bulk string: $-1\r\n
func (w *Writer) WriteNull() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// WriteNullArray writes the null array: *-1\r\n. Redis uses this (not the null
// bulk string) for an aborted EXEC.
func (w *Writer) WriteNullArray() error {
	_, err := w.w.WriteString("*-1\r\n")
	return err
}

// WriteArrayHeader writes an array header: *<n>\r\n. The caller then writes n
// elements.
func (w *Writer) WriteArrayHeader(n int) error {
	w.w.WriteByte('*')
	w.w.WriteString(strconv.Itoa(n))
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteCommand encodes args as a client command: a RESP array of bulk strings.
// This is the form ReadCommand consumes, so it is used to persist commands to
// the AOF and to stream them to replicas.
func (w *Writer) WriteCommand(args [][]byte) error {
	if err := w.WriteArrayHeader(len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if err := w.WriteBulk(a); err != nil {
			return err
		}
	}
	return nil
}
