// Package resp implements the subset of the RESP (REdis Serialization Protocol)
// needed to talk to standard Redis clients such as redis-cli.
//
// Requests arrive as an array of bulk strings (the form every Redis client
// sends); an inline space-separated fallback is also accepted for convenience
// when poking at the server with a raw socket. Requests are identical in RESP2
// and RESP3, so only the reply side is version-aware.
//
// # Protocol versions
//
// A Writer serializes replies in either RESP2 or RESP3, selected per connection
// by HELLO and stored on the Writer because the reply encoding is a property of
// one socket and of nothing else. RESP2 is the default, so a connection that
// never sends HELLO -- and every internal writer, such as the one an AOF replay
// discards its replies into -- keeps the original encoding byte for byte.
//
// The RESP3 types are written by dedicated methods (WriteMapHeader,
// WriteSetHeader, WriteDouble, WriteBool, WriteBigNumber, WriteVerbatim,
// WritePushHeader) that fall back to the RESP2 encoding of the same value when
// the connection speaks RESP2. Keeping the fallback inside the writer rather
// than at every call site is what makes it impossible for a handler to emit a
// RESP3 type to a RESP2 client: there is one place that decides, and it is the
// place that knows the version.
//
// The one exception is the attribute type, which has no RESP2 encoding at all
// (real Redis omits the attribute entirely for a RESP2 client). A caller must
// therefore guard WriteAttributeHeader with a Proto() check, since only the
// caller knows how to leave the attribute out.
//
// # Why the reply writers report no error
//
// The version-aware writers return nothing. Writes go into a bufio.Writer whose
// error is sticky, so a failure part-way through a reply is reported by the
// Flush the connection loop already checks -- there is nothing a handler could
// do with an error here that the loop does not already do better, and a return
// value every call site has to ignore is worse than no return value at all. The
// streaming methods (WriteCommand, WriteRaw, Flush) do return errors, because
// their callers -- a replica feed, a snapshot -- act on them by ending the
// stream.
package resp

import (
	"bufio"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
)

// Protocol versions a Writer can serialize replies in.
const (
	// ProtoRESP2 is the original protocol: simple strings, errors, integers,
	// bulk strings and arrays, with $-1/*-1 for the nulls.
	ProtoRESP2 = 2
	// ProtoRESP3 adds the map, set, double, boolean, big-number, verbatim-string,
	// null, push and attribute types.
	ProtoRESP3 = 3
)

// ErrProtocol indicates a malformed client request.
var ErrProtocol = errors.New("resp: protocol error")

// The specific protocol violations, each carrying the detail Redis reports for it.
//
// They exist so the server can answer a malformed request with a diagnosable sentence
// before hanging up. Closing the connection silently -- which is what this did -- is
// indistinguishable from a crash or a network fault to every client library, and it
// costs the caller the one piece of information that would tell them what they sent
// wrong. Redis names the offending byte, so these do too.
// Each wraps ErrProtocol, so a caller that only cares whether the request was malformed
// keeps working with errors.Is(err, ErrProtocol) while one that wants to report the
// detail can ask for it.
var (
	// ErrInvalidMultibulk is a "*" header that is not a number, or is beyond MaxMultiBulk.
	ErrInvalidMultibulk = &protocolError{detail: "invalid multibulk length"}
	// ErrInvalidBulk is a "$" header that is not a number, is negative, or is beyond
	// MaxBulkLen.
	ErrInvalidBulk = &protocolError{detail: "invalid bulk length"}
	// ErrExpectedBulk is an element header that does not start with "$".
	ErrExpectedBulk = &protocolError{detail: "expected '$'"}
	// ErrUnbalancedQuotes is an inline command whose quoting does not close, or whose
	// closing quote is not followed by a separator.
	ErrUnbalancedQuotes = &protocolError{detail: "unbalanced quotes in request"}
)

// protocolError is one specific malformed-request case. It unwraps to ErrProtocol so the
// specific and the general test both work.
type protocolError struct{ detail string }

func (e *protocolError) Error() string { return "resp: " + e.detail }
func (e *protocolError) Unwrap() error { return ErrProtocol }

// ProtocolErrorText renders the "-ERR Protocol error: ..." detail for a read error, or
// "" when the error is not a protocol violation (an EOF or a network fault, which have
// nobody to report to). The wording matches Redis, because clients match on it and
// because the byte named in "expected '$', got 'f'" is the whole diagnostic value.
func ProtocolErrorText(err error) string {
	if err == nil {
		return ""
	}
	// The specific cases come first, and deliberately: every one of them unwraps to
	// ErrProtocol, so testing the general case earlier would swallow all of them and
	// report "invalid request" for a violation that could have named itself.
	var e *expectedBulkError
	if errors.As(err, &e) {
		return e.Error() // carries the byte it found
	}
	switch {
	case errors.Is(err, ErrInvalidMultibulk):
		return "Protocol error: invalid multibulk length"
	case errors.Is(err, ErrInvalidBulk):
		return "Protocol error: invalid bulk length"
	case errors.Is(err, ErrUnbalancedQuotes):
		return "Protocol error: unbalanced quotes in request"
	case errors.Is(err, ErrProtocol):
		return "Protocol error: invalid request"
	}
	return ""
}

// expectedBulkError reports an element header that did not begin with "$", naming the
// byte found there the way Redis does.
type expectedBulkError struct{ got byte }

func (e *expectedBulkError) Error() string {
	return "Protocol error: expected '$', got '" + string(e.got) + "'"
}

// Unwraps to ErrExpectedBulk, which in turn unwraps to ErrProtocol, so both the
// specific and the general test match.
func (e *expectedBulkError) Unwrap() error { return ErrExpectedBulk }

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
	n int64 // bytes consumed, for replication offset accounting
}

func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReader(r)} }

// Buffered reports how many bytes are already buffered and available without a
// read syscall. The server uses it to coalesce replies for a pipelined client:
// it keeps processing while more input is buffered and flushes once the buffer
// drains.
func (r *Reader) Buffered() int { return r.r.Buffered() }

// Consumed reports how many bytes have been read and parsed so far, counting the
// protocol framing. A replica needs it to report its replication offset: the offset
// is a byte count over the master's stream, so it can only be derived from the bytes
// actually taken off that stream, not from the number of commands applied.
//
// It counts consumed bytes, not buffered ones, which is why it is maintained here
// rather than by wrapping the underlying reader -- a wrapper would count a whole
// buffer fill as processed the moment it arrived.
func (r *Reader) Consumed() int64 { return r.n }

// WaitReadable blocks until at least one byte of input is available or the read
// fails, and consumes nothing.
//
// It exists for the blocking commands. A blocked client's connection goroutine is
// parked in the wait rather than in a read, so nothing would notice that client
// hanging up; peeking is how the socket can be watched without taking a byte the
// command loop will need afterwards. A successful return means input arrived (a
// pipelined command, still there to be read), not that the connection is healthy in
// any stronger sense.
//
// The caller must guarantee no other goroutine is using the Reader while this runs,
// and must observe its return before reading again -- see watchClientGone, which
// forces it to return with a read deadline and then waits for it.
func (r *Reader) WaitReadable() error {
	_, err := r.r.Peek(1)
	return err
}

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
		args, err := splitInline(line)
		if err != nil {
			return nil, err
		}
		if len(args) == 0 {
			// A line of nothing but separators is not a command; Redis ignores it and
			// keeps the connection.
			return r.ReadCommand()
		}
		return args, nil
	}

	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n > MaxMultiBulk {
		return nil, ErrInvalidMultibulk
	}
	if n <= 0 {
		// A negative count is the legacy null multibulk and a zero count is an empty
		// command. Redis skips both and reads the next request rather than treating
		// either as fatal, so a client that emits one is not dropped.
		return r.ReadCommand()
	}
	// Cap the initial capacity so a large-but-undelivered count can't force a
	// big up-front allocation; append grows it as real elements arrive.
	args := make([][]byte, 0, min(n, 64))
	for i := 0; i < n; i++ {
		hdr, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 {
			return nil, &expectedBulkError{got: '\r'}
		}
		if hdr[0] != '$' {
			return nil, &expectedBulkError{got: hdr[0]}
		}
		length, err := strconv.Atoi(string(hdr[1:]))
		if err != nil || length < 0 || length > MaxBulkLen {
			return nil, ErrInvalidBulk
		}
		buf := make([]byte, length+2) // payload + trailing CRLF
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		r.n += int64(len(buf))
		args = append(args, buf[:length])
	}
	return args, nil
}

// ReadStatus reads a single-line reply and returns its payload. A simple string
// yields its text; an error reply is returned as a Go error carrying the server's
// message. It exists for the replication handshake, which is the one place this
// server acts as a client and has to read a reply rather than write one.
func (r *Reader) ReadStatus() (string, error) {
	line, err := r.readLine()
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", ErrProtocol
	}
	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return "", errors.New(string(line[1:]))
	}
	return "", ErrProtocol
}

// ReadBulk reads a bulk-string reply and returns its payload, or nil for the null bulk
// string. An error reply is returned as a Go error carrying the server's message, as
// ReadStatus does.
//
// It is the companion ReadStatus needed once this server started acting as a client for
// something other than the replication handshake: a node introducing itself to another
// (CLUSTER MEET) reads that node's CLUSTER NODES table, which is a bulk string. A
// general reply decoder is deliberately not offered -- the node-to-node conversations
// this server has are a handful of statuses and one bulk string, and a decoder for
// every RESP type would be a second protocol implementation to keep correct with no
// caller that needs it.
func (r *Reader) ReadBulk() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrProtocol
	}
	switch line[0] {
	case '-':
		return nil, errors.New(string(line[1:]))
	case '$', '=':
	default:
		return nil, ErrProtocol
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < -1 || n > MaxBulkLen {
		return nil, ErrProtocol
	}
	if n < 0 {
		return nil, nil // the null bulk string
	}
	buf := make([]byte, n+2) // payload plus the trailing CRLF
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, err
	}
	r.n += int64(len(buf))
	return buf[:n], nil
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadBytes('\n')
	r.n += int64(len(line)) // counted even on error: those bytes are gone from the stream
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

func splitInline(line []byte) ([][]byte, error) {
	var args [][]byte
	i := 0
	for {
		for i < len(line) && isInlineSpace(line[i]) {
			i++
		}
		if i >= len(line) {
			return args, nil
		}

		var cur []byte
		inQuote, inSingle, done := false, false, false
		for !done {
			switch {
			case inQuote:
				if i >= len(line) {
					return nil, ErrUnbalancedQuotes
				}
				switch {
				case line[i] == '\\' && i+3 < len(line) && line[i+1] == 'x' &&
					isHexDigit(line[i+2]) && isHexDigit(line[i+3]):
					cur = append(cur, hexVal(line[i+2])<<4|hexVal(line[i+3]))
					i += 4
				case line[i] == '\\' && i+1 < len(line):
					cur = append(cur, unescape(line[i+1]))
					i += 2
				case line[i] == '"':
					// A closing quote must end the argument: anything other than a
					// separator after it is malformed, not the start of more text.
					if i+1 < len(line) && !isInlineSpace(line[i+1]) {
						return nil, ErrUnbalancedQuotes
					}
					i++
					done = true
				default:
					cur = append(cur, line[i])
					i++
				}

			case inSingle:
				if i >= len(line) {
					return nil, ErrUnbalancedQuotes
				}
				switch {
				case line[i] == '\\' && i+1 < len(line) && line[i+1] == '\'':
					cur = append(cur, '\'')
					i += 2
				case line[i] == '\'':
					if i+1 < len(line) && !isInlineSpace(line[i+1]) {
						return nil, ErrUnbalancedQuotes
					}
					i++
					done = true
				default:
					cur = append(cur, line[i])
					i++
				}

			default:
				if i >= len(line) {
					done = true
					break
				}
				switch line[i] {
				case ' ', '\t', '\n', '\r', '\v', '\f':
					done = true
				case '"':
					inQuote = true
					i++
				case '\'':
					inSingle = true
					i++
				default:
					cur = append(cur, line[i])
					i++
				}
			}
		}
		if cur == nil {
			// An empty quoted argument ("" ) is still an argument.
			cur = []byte{}
		}
		args = append(args, cur)
	}
}

func isInlineSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}

// unescape maps the escapes Redis recognises inside a double-quoted inline argument.
// An unrecognised escape yields the character itself, as Redis does.
func unescape(c byte) byte {
	switch c {
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'b':
		return '\b'
	case 'a':
		return '\a'
	default:
		return c
	}
}

// Writer serializes replies to a connection. Callers must Flush after writing a
// complete reply.
//
// proto is the protocol version this connection negotiated with HELLO. It is
// touched only by the connection's own goroutine (HELLO sets it, that
// connection's handlers read it), so it needs no synchronization -- the Pub/Sub
// pump writes through the same Writer but only ever reads proto, and it cannot
// run before the SUBSCRIBE that starts it has returned.
type Writer struct {
	w     *bufio.Writer
	proto int
	// out is the destination the buffer normally writes to, kept so that reply
	// suppression can point the buffer at io.Discard and back. See Suppress.
	out io.Writer
	// suppressed is CLIENT REPLY OFF|SKIP: every reply written is thrown away instead of
	// sent. pushDepth counts the out-of-band frames currently being written, which are
	// exempt -- see BeginPush. Both are touched only by the connection's own goroutine and
	// by its pumps under the session's writer lock, which is the same discipline proto
	// lives under.
	suppressed bool
	pushDepth  int
	// errs counts the error replies written on this connection. It exists so the
	// server's per-command statistics can report failed calls without every one of
	// ~180 handlers having to say "I answered with an error": the caller reads the
	// counter before and after running a command and compares. Like proto it is touched
	// only by the connection's own goroutine.
	errs int64
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w), out: w, proto: ProtoRESP2}
}

// Suppress makes every reply written from now on be discarded rather than sent, which is
// what CLIENT REPLY OFF and CLIENT REPLY SKIP ask for. Resume undoes it.
//
// The redirection is applied to the buffer rather than checked in each Write method, and
// that is the point: every byte this type emits goes through w.w, so pointing that at
// io.Discard suppresses the whole reply surface by construction -- including replies
// written by handlers added later, which a per-method check would silently miss. The cost
// is that a suppressed reply is still formatted; it is never buffered beyond one reply
// and never reaches a syscall, and correctness here is worth more than the formatting.
//
// Suppress does not flush first. A caller with replies already buffered that must still
// be sent -- CLIENT REPLY OFF, which follows commands that were answered normally --
// flushes before calling it, because Reset discards whatever the buffer holds.
func (w *Writer) Suppress() {
	if w.suppressed {
		return
	}
	w.suppressed = true
	w.w.Reset(io.Discard)
}

// Resume restores delivery. Anything written while suppressed is dropped.
func (w *Writer) Resume() {
	if !w.suppressed {
		return
	}
	w.suppressed = false
	w.w.Reset(w.out)
}

// Suppressed reports whether replies are currently being discarded.
func (w *Writer) Suppressed() bool { return w.suppressed }

// BeginPush and EndPush bracket an out-of-band frame -- a delivered Pub/Sub message or a
// subscription confirmation -- which is delivered even while replies are suppressed.
//
// That exemption is Redis's, not an invention: its CLIENT_PUSHING flag "disables the reply
// silencing flags", and its own Pub/Sub tests subscribe on a connection that has just sent
// CLIENT REPLY OFF and then read the confirmations. The reasoning is that CLIENT REPLY OFF
// says "I am not going to read your answers to my commands"; a pushed message is not an
// answer to a command, and dropping it would lose data the client asked to receive rather
// than an acknowledgement it asked to skip.
//
// They nest, so a caller need not know whether it is already inside a frame.
func (w *Writer) BeginPush() {
	w.pushDepth++
	if w.pushDepth == 1 && w.suppressed {
		w.w.Reset(w.out)
	}
}

// EndPush closes the frame, flushing it before the buffer is pointed back at the void --
// Reset discards what it holds, so the frame has to leave first.
func (w *Writer) EndPush() {
	if w.pushDepth == 0 {
		return
	}
	w.pushDepth--
	if w.pushDepth == 0 && w.suppressed {
		_ = w.w.Flush()
		w.w.Reset(io.Discard)
	}
}

// SetProto selects the protocol version replies are encoded in. HELLO is the only
// caller: a client asks for a version, and every later reply on that connection is
// written in it.
func (w *Writer) SetProto(v int) { w.proto = v }

// Proto reports the connection's protocol version. Handlers consult it where the
// two protocols differ by more than an encoding -- where RESP3 nests pairs a RESP2
// reply flattens, or where a push replaces a reply entirely.
func (w *Writer) Proto() int { return w.proto }

func (w *Writer) Flush() error { return w.w.Flush() }

// WriteSimple writes a simple string: +<s>\r\n
func (w *Writer) WriteSimple(s string) error {
	w.w.WriteByte('+')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// ErrorsWritten reports how many error replies have been written on this
// connection, which is what lets a caller tell whether the command it just ran
// failed. See the errs field.
func (w *Writer) ErrorsWritten() int64 { return w.errs }

// WriteError writes an error reply: -<s>\r\n
func (w *Writer) WriteError(s string) error {
	w.errs++
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

// WriteNull writes a null: _\r\n in RESP3, the null bulk string $-1\r\n in RESP2.
func (w *Writer) WriteNull() error {
	if w.proto >= ProtoRESP3 {
		_, err := w.w.WriteString("_\r\n")
		return err
	}
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// WriteNullArray writes a null where RESP2 spells it as the null *array* rather
// than the null bulk string -- an aborted EXEC, a LPOP on a missing key with a
// count, a BLPOP that timed out. RESP3 has a single null, so the distinction
// disappears there, which is precisely the ambiguity RESP3 set out to remove.
func (w *Writer) WriteNullArray() error {
	if w.proto >= ProtoRESP3 {
		_, err := w.w.WriteString("_\r\n")
		return err
	}
	_, err := w.w.WriteString("*-1\r\n")
	return err
}

// WriteArrayHeader writes an array header: *<n>\r\n. The caller then writes n
// elements.
func (w *Writer) WriteArrayHeader(n int) error {
	w.writeLen('*', n)
	return nil
}

// WriteMapHeader writes a map header for n key/value *pairs*: %<n>\r\n in RESP3,
// and the flat array of 2n elements a RESP2 client receives instead. Either way the
// caller writes 2n values, alternating key and value, so one code path serves both.
func (w *Writer) WriteMapHeader(n int) {
	if w.proto >= ProtoRESP3 {
		w.writeLen('%', n)
		return
	}
	w.writeLen('*', n*2)
}

// WriteSetHeader writes a set header: ~<n>\r\n in RESP3, an array in RESP2. The
// type says the elements are unordered and distinct; the elements themselves are
// encoded identically, so the RESP2 form is byte-for-byte the array it always was.
func (w *Writer) WriteSetHeader(n int) {
	if w.proto >= ProtoRESP3 {
		w.writeLen('~', n)
		return
	}
	w.writeLen('*', n)
}

// WritePushHeader writes an out-of-band push header: ><n>\r\n in RESP3.
//
// A push is what lets a RESP3 client stay subscribed and still issue ordinary
// commands: pushes are tagged, so the client can route a delivered message to its
// message handler instead of matching it against the reply it is waiting for. RESP2
// has no such frame, which is the whole reason a RESP2 subscriber is restricted to
// the subscribe family -- so for RESP2 this writes the plain array those clients
// already demultiplex positionally.
func (w *Writer) WritePushHeader(n int) {
	if w.proto >= ProtoRESP3 {
		w.writeLen('>', n)
		return
	}
	w.writeLen('*', n)
}

// WriteAttributeHeader writes an attribute header (|<n>\r\n) introducing n
// key/value pairs of out-of-band metadata that describe the reply that follows
// rather than being part of it.
//
// It is RESP3-only and, unlike every other method here, has no RESP2 fallback:
// there is nowhere in a RESP2 stream to put metadata a client is not expecting. A
// caller must therefore check Proto() and skip the attribute (and its pairs)
// entirely for a RESP2 client, which is what real Redis does.
func (w *Writer) WriteAttributeHeader(n int) {
	if w.proto < ProtoRESP3 {
		return
	}
	w.writeLen('|', n)
}

func (w *Writer) writeLen(prefix byte, n int) {
	w.w.WriteByte(prefix)
	w.w.WriteString(strconv.Itoa(n))
	w.w.WriteString("\r\n")
}

// WriteBool writes a boolean: #t\r\n / #f\r\n in RESP3, :1 / :0 in RESP2.
func (w *Writer) WriteBool(b bool) {
	if w.proto < ProtoRESP3 {
		if b {
			w.WriteInt(1)
			return
		}
		w.WriteInt(0)
		return
	}
	if b {
		w.w.WriteString("#t\r\n")
		return
	}
	w.w.WriteString("#f\r\n")
}

// WriteDouble writes a double: ,<value>\r\n in RESP3, and the same text as a bulk
// string in RESP2 -- which is the encoding every RESP2 client already parses scores
// out of, so the bytes it sees do not change.
func (w *Writer) WriteDouble(f float64) {
	s := FormatDouble(f)
	if w.proto < ProtoRESP3 {
		w.WriteBulk([]byte(s))
		return
	}
	w.w.WriteByte(',')
	w.w.WriteString(s)
	w.w.WriteString("\r\n")
}

// WriteBigNumber writes an integer too large for the int64 an integer reply
// carries: (<digits>\r\n in RESP3, a bulk string in RESP2. digits must already be a
// base-10 integer literal, optionally signed; the writer does not validate it,
// because the only honest source of such a number is a value the server already
// holds in that form.
func (w *Writer) WriteBigNumber(digits string) {
	if w.proto < ProtoRESP3 {
		w.WriteBulk([]byte(digits))
		return
	}
	w.w.WriteByte('(')
	w.w.WriteString(digits)
	w.w.WriteString("\r\n")
}

// WriteVerbatim writes a verbatim string: =<len>\r\n<format>:<payload>\r\n in
// RESP3, a plain bulk string in RESP2.
//
// The format is a three-character hint ("txt", "mkd") telling the client the
// payload is meant to be displayed as-is rather than quoted and escaped. INFO is
// the reason it exists: a bulk string full of \r\n renders as one long escaped line
// in redis-cli, while a verbatim string renders as the report an operator wanted.
func (w *Writer) WriteVerbatim(format string, payload []byte) {
	if w.proto < ProtoRESP3 {
		w.WriteBulk(payload)
		return
	}
	w.w.WriteByte('=')
	w.w.WriteString(strconv.Itoa(len(format) + 1 + len(payload)))
	w.w.WriteString("\r\n")
	w.w.WriteString(format)
	w.w.WriteByte(':')
	w.w.Write(payload)
	w.w.WriteString("\r\n")
}

// ParseDouble reads a double the way Redis's string2d does. It is the inverse of
// FormatDouble and lives beside it for the same reason: a value's text and its
// interpretation are one decision, and a server with two of them disagrees with itself.
//
// Go's strconv.ParseFloat is not that decision on its own, because it implements *Go's*
// float literal grammar, which permits underscores as digit separators. So
// strconv.ParseFloat("1_0", 64) is 10 with no error, while Redis answers
// "ERR value is not a valid float" -- measured against redis:7.2-alpine for "1_0" and
// "1_000.5" through ZADD, ZINCRBY, INCRBYFLOAT and HINCRBYFLOAT, all four of which
// reject it. Accepting it is the worse half of a wire-compatibility gap: the underscore
// is not merely tolerated but silently *dropped*, so `SET k 1_0` followed by
// `INCRBYFLOAT k 1` answered 11 where Redis refuses the operation, and a client that
// sent a thousands separator by accident got a number two orders of magnitude out with
// nothing reporting it.
//
// Two known differences from Redis are left alone deliberately, because both were
// measured and neither is a silent reinterpretation:
//
//   - Redis's strtod accepts C99 hex literals ("0x10" is 16); Go requires the binary
//     exponent ("0x1p3" is 8, and that form both accept). Rejecting "0x10" answers an
//     error where Redis answers a number -- a missing spelling, not a wrong one.
//   - An underflowing literal ("1e-400") is accepted here as zero. Redis is itself
//     inconsistent about it: string2d rejects it, so ZADD does, while string2ld accepts
//     it, so INCRBYFLOAT does. Matching the accepting half keeps the store's stored-value
//     parse in agreement with the command that writes it.
func ParseDouble(s string) (float64, bool) {
	if strings.IndexByte(s, '_') >= 0 {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

// FormatDouble renders a floating-point value the way Redis does: the shortest
// form that round-trips, with the infinities spelled "inf"/"-inf" (the spelling
// Redis both emits and accepts back) rather than Go's "+Inf".
//
// It lives here because it is the wire encoding of a double, and both protocols
// need the identical text: RESP2 puts it in a bulk string and RESP3 after a comma,
// and a server whose two protocols disagreed about how to spell a score would be
// reporting two different values.
func FormatDouble(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	// A value that is exactly an integer is written as one, with no exponent, which is
	// what Redis does (its d2string tries an integer conversion first). It matters for the
	// large integral scores GEOADD produces: a 52-bit geohash rendered as "3.4790999e+15"
	// is the same number, but not the same text a client comparing against Redis's ZSCORE
	// expects. The bound is 2^53, past which a float64 no longer represents every integer
	// and "integral" stops being a meaningful thing to preserve.
	if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return strconv.FormatInt(int64(f), 10)
	}

	// Otherwise: shortest round-trip digits, in fixed notation over the range where Redis
	// uses fixed notation, and scientific outside it.
	//
	// Go's 'g' verb cannot be used directly. It switches to an exponent far earlier than
	// Redis does -- it renders 17179869185.5 as "1.71798691855e+10" where Redis returns
	// "17179869185.5" -- and that is not merely a cosmetic reply difference here. Per the
	// effect-propagation invariant, INCRBYFLOAT ships its result as `SET key <result>`, so
	// the formatted text is what reaches the AOF and every replica; a later increment then
	// has to re-parse it. A value whose scale changes its serialisation is a bad thing to
	// build durability on.
	//
	// The bounds were measured against redis:7.2 rather than derived: exponent 18 formats
	// fixed and 19 scientific, -6 formats fixed and -7 scientific.
	exp := decimalExponent(f)
	if exp >= -6 && exp <= 18 {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return trimExponentZeros(strconv.FormatFloat(f, 'e', -1, 64))
}

// decimalExponent is floor(log10(|f|)), computed from the formatted digits rather than
// with math.Log10 so it cannot disagree with the digits actually emitted at a power-of-ten
// boundary (where Log10 rounding would put the value on the wrong side of a bound).
func decimalExponent(f float64) int {
	s := strconv.FormatFloat(f, 'e', -1, 64)
	i := strings.IndexByte(s, 'e')
	if i < 0 {
		return 0
	}
	exp, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return 0
	}
	return exp
}

// trimExponentZeros rewrites Go's zero-padded exponent into the form Redis prints: Go
// renders 1e-7 as "1e-07", Redis as "1e-7". Clients compare these strings.
func trimExponentZeros(s string) string {
	i := strings.IndexByte(s, 'e')
	if i < 0 || i+2 >= len(s) {
		return s
	}
	mantissa, sign, digits := s[:i], s[i+1], s[i+2:]
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
	}
	return mantissa + "e" + string(sign) + digits
}

// WriteRaw writes bytes that are already in RESP form, without re-encoding them.
// A partial resync uses it to replay the replication backlog, which stores the exact
// bytes the replica would have received: decoding them into commands only to encode
// them again would cost the copy and, worse, risk producing a stream that differs by
// a byte from the one the offsets were counted over.
func (w *Writer) WriteRaw(p []byte) error {
	_, err := w.w.Write(p)
	return err
}

// CommandSize reports how many bytes WriteCommand writes for args. It is the single
// place that knows the encoding's size, so the AOF's growth accounting and the
// replication offset both measure the stream with the same ruler the writer uses --
// two independent size formulas would eventually disagree with the bytes on the wire.
func CommandSize(args [][]byte) int {
	n := 1 + len(strconv.Itoa(len(args))) + 2 // *<count>\r\n
	for _, a := range args {
		n += 1 + len(strconv.Itoa(len(a))) + 2 + len(a) + 2 // $<len>\r\n<payload>\r\n
	}
	return n
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
