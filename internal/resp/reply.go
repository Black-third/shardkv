package resp

// Reply decoding: the inverse of Writer, for the one caller that issues commands
// instead of serving them.
//
// Until the embedded API existed there was no such caller, and this file deliberately
// did not exist -- see ReadBulk, which says so. What changed is that a Go program can
// now run a command in-process (see server.EmbeddedSession): the handler writes its
// reply through the same Writer a socket client's would go through, into a buffer, and
// something has to turn those bytes back into values. Doing it here rather than in the
// facade is what keeps the encoder and the decoder in one file's reading distance --
// the same argument invariant 7 makes about key extraction, applied to the protocol. A
// decoder living in the public package would drift from the writer the first time a
// reply type was added, and the drift would be silent in the direction that matters:
// the facade would keep returning a value for a shape it no longer understood.

import (
	"io"
	"strconv"
)

// ReplyError is an error reply, carried as a *value* rather than returned as this
// package's error.
//
// The distinction is not pedantic, and one reply shape forces it: EXEC answers with an
// array in which any element may be an error, because a transaction runs every queued
// command whether or not an earlier one failed. A decoder that turned an error reply
// into its own error return would have to abandon the array at the first failed
// command, and so could not report the results of the ones that succeeded -- which is
// most of what EXEC is for. So a transport failure (a short read, a byte that cannot
// begin a reply) is this package's error, and whatever the server said is a value.
//
// It implements error, so a caller asks whether a value is an error reply with a type
// assertion to error and never has to name this type. That is what lets the public
// facade hand these values back without exposing an internal package in its contract.
type ReplyError string

func (e ReplyError) Error() string { return string(e) }

// ErrReplyType is a byte that cannot begin a reply. It wraps ErrProtocol so a caller
// that only distinguishes "malformed" from "I/O failure" needs one test.
var ErrReplyType = &protocolError{detail: "unknown reply type"}

// ErrReplyTooDeep is a reply nested past maxReplyDepth.
var ErrReplyTooDeep = &protocolError{detail: "reply nested too deeply"}

// maxReplyDepth bounds recursion in ReadReply.
//
// The replies this server produces are shallow -- XINFO STREAM FULL is the deepest at
// six levels, and CLUSTER SLOTS is four -- so the limit is never reached by a real
// reply and exists for the stream that is not one. Nesting is the one thing a reply
// header can ask for unboundedly with almost no bytes: "*1" repeated 100,000 times is
// 400 KB of input and 100,000 stack frames, which is a stack overflow rather than an
// error, and a stack overflow is not recoverable. The element and length caps
// (MaxMultiBulk, MaxBulkLen) bound the other two dimensions already; this bounds the
// third.
const maxReplyDepth = 32

// ReadReply decodes one reply and returns it as Go values. It is the counterpart of
// the Writer's reply methods, and it accepts both protocol versions.
//
// The mapping, and the reasoning behind the two entries that could have gone either
// way:
//
//	+simple, $bulk, =verbatim, (big number   ->  string
//	-error                                   ->  ReplyError (a value; see above)
//	:integer                                 ->  int64
//	,double                                  ->  float64
//	#boolean                                 ->  bool
//	$-1, *-1, _null                          ->  nil
//	*array, ~set, >push                      ->  []any
//	%map                                     ->  map[string]any
//
// A bulk string decodes to string rather than []byte because RESP is binary-safe and
// so is a Go string: nothing is lost, the value compares with ==, and an array of them
// is directly comparable element by element, which []byte is not. A caller that wants
// the bytes converts back for the cost of a copy it would have paid anyway.
//
// A set and a push decode to the same []any an array does, and a verbatim string to
// the same string a bulk does, and that is the point. RESP3 added those three purely
// as *tags* over data RESP2 already expressed the same way, so decoding them alike
// makes a reply come out as the same Go value whichever protocol the caller
// negotiated -- SMEMBERS is []any either way. The two shapes RESP3 genuinely changed,
// the map and the double, are the two that come out differently, which is exactly the
// difference a caller asked for by sending HELLO 3.
//
// An attribute (|) is metadata about the reply rather than part of it, so it is
// decoded and discarded and the reply behind it is returned. That mirrors what the
// Writer does in the other direction, where an attribute is simply omitted for a
// RESP2 client: neither side lets it change the value.
func (r *Reader) ReadReply() (any, error) { return r.readReply(0) }

func (r *Reader) readReply(depth int) (any, error) {
	if depth > maxReplyDepth {
		return nil, ErrReplyTooDeep
	}
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrReplyType
	}
	body := string(line[1:])
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return ReplyError(body), nil
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return nil, ErrProtocol
		}
		return n, nil
	case '_':
		return nil, nil
	case '#':
		switch body {
		case "t":
			return true, nil
		case "f":
			return false, nil
		}
		return nil, ErrProtocol
	case ',':
		// Through ParseDouble rather than strconv, so "inf", "-inf" and "nan" decode to
		// the values the Writer wrote them from. FormatDouble spells those three as words
		// because Redis does, and strconv.ParseFloat does not accept "inf".
		f, ok := ParseDouble(body)
		if !ok {
			return nil, ErrProtocol
		}
		return f, nil
	case '(':
		// A big number is returned as its digits. It is an integer that by definition does
		// not fit the int64 an integer reply carries, so the only lossless target in the
		// standard library is math/big -- and reaching for it here would put a decoder for
		// a type this server emits from exactly one place (DEBUG JMAP-style diagnostics)
		// on the path of every reply. The digits are what the server holds and what the
		// caller can parse if it cares.
		return body, nil
	case '$', '=':
		return r.readReplyBulk(body, line[0] == '=')
	case '*', '~', '>':
		return r.readReplyArray(body, depth)
	case '%':
		return r.readReplyMap(body, depth)
	case '|':
		// The attribute's own pairs are read and dropped, then the reply it describes is
		// returned. Recursing for the pairs (rather than skipping bytes) is what makes a
		// nested value inside an attribute consume exactly as much input as it occupies.
		if _, err := r.readReplyMap(body, depth); err != nil {
			return nil, err
		}
		return r.readReply(depth)
	}
	return nil, ErrReplyType
}

// readReplyBulk reads the payload of a bulk or verbatim string whose header said body.
// A verbatim string's three-character format hint and its colon are stripped, so it
// decodes to the same string its RESP2 bulk fallback would.
func (r *Reader) readReplyBulk(body string, verbatim bool) (any, error) {
	n, err := strconv.Atoi(body)
	if err != nil || n < -1 || n > MaxBulkLen {
		return nil, ErrInvalidBulk
	}
	if n < 0 {
		return nil, nil // the null bulk string
	}
	buf := make([]byte, n+2) // payload plus the trailing CRLF
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, err
	}
	r.n += int64(len(buf))
	payload := buf[:n]
	if verbatim && len(payload) >= 4 && payload[3] == ':' {
		payload = payload[4:]
	}
	return string(payload), nil
}

func (r *Reader) readReplyArray(body string, depth int) (any, error) {
	n, err := strconv.Atoi(body)
	if err != nil || n < -1 || n > MaxMultiBulk {
		return nil, ErrInvalidMultibulk
	}
	if n < 0 {
		return nil, nil // the null array, which is how RESP2 spells a missing collection
	}
	// Capped for the same reason ReadCommand caps its own: a large count that no elements
	// follow must not turn into a large allocation before the read that would fail.
	out := make([]any, 0, min(n, 64))
	for i := 0; i < n; i++ {
		v, err := r.readReply(depth + 1)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// readReplyMap reads n key/value pairs into a map.
//
// A key that is not a string is rendered as one rather than refused. Every map this
// server writes has bulk-string keys (see writeMapStrings and its relatives), so the
// fallback is unreachable from this server's own replies -- but a decoder that returned
// an error for a well-formed stream it merely found surprising would fail a caller for
// something the caller cannot fix.
func (r *Reader) readReplyMap(body string, depth int) (any, error) {
	n, err := strconv.Atoi(body)
	if err != nil || n < 0 || n > MaxMultiBulk {
		return nil, ErrInvalidMultibulk
	}
	out := make(map[string]any, min(n, 64))
	for i := 0; i < n; i++ {
		k, err := r.readReply(depth + 1)
		if err != nil {
			return nil, err
		}
		v, err := r.readReply(depth + 1)
		if err != nil {
			return nil, err
		}
		out[replyKey(k)] = v
	}
	return out, nil
}

func replyKey(v any) string {
	switch k := v.(type) {
	case string:
		return k
	case ReplyError:
		return string(k)
	case int64:
		return strconv.FormatInt(k, 10)
	case float64:
		return FormatDouble(k)
	case bool:
		return strconv.FormatBool(k)
	case nil:
		return ""
	}
	return ""
}
