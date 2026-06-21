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
	if err != nil || n < 0 {
		return nil, ErrProtocol
	}
	args := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, ErrProtocol
		}
		length, err := strconv.Atoi(string(hdr[1:]))
		if err != nil || length < 0 {
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

// WriteArrayHeader writes an array header: *<n>\r\n. The caller then writes n
// elements.
func (w *Writer) WriteArrayHeader(n int) error {
	w.w.WriteByte('*')
	w.w.WriteString(strconv.Itoa(n))
	_, err := w.w.WriteString("\r\n")
	return err
}
