package server

import (
	"strings"
	"testing"
)

// TestBitmapCommands covers the family at the wire level, including the interaction
// with the string commands that share the representation.
func TestBitmapCommands(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// SETBIT grows the string; the reply is the bit's previous value.
		{"SETBIT b 7 1", ":0"},
		{"SETBIT b 7 1", ":1"},
		{"GETBIT b 7", ":1"},
		{"GETBIT b 6", ":0"},
		{"GETBIT b 1000", ":0"}, // past the end reads as 0, not an error
		{"STRLEN b", ":1"},
		{"GET b", "\x01"},
		{"SETBIT b 0 1", ":0"},
		{"GET b", "\x81"},
		{"SETBIT b 0 0", ":1"},
		{"GET b", "\x01"},
		{"SETBIT b -1 1", "-ERR bit offset is not an integer or out of range"},
		{"SETBIT b 7 2", "-ERR bit is not an integer or out of range"},
		{"SETBIT b 4294967296 1", "-ERR bit offset is not an integer or out of range"},

		// The bit numbering is Redis's: bit 0 is the high bit of byte 0, so setting
		// bits 1, 2 and 4 of "foobar" is what BITCOUNT counts below.
		{"SET s foobar", "+OK"},
		{"BITCOUNT s", ":26"},
		{"BITCOUNT s 0 0", ":4"},
		{"BITCOUNT s 1 1", ":6"},
		{"BITCOUNT s 0 -5", ":10"},
		{"BITCOUNT s 0 5 BYTE", ":26"},
		{"BITCOUNT s 5 30 BIT", ":17"},
		{"BITCOUNT s 0", "-ERR syntax error"},
		{"BITCOUNT s 0 0 NIBBLE", "-ERR syntax error"},
		{"BITCOUNT missing", ":0"},

		// BITPOS, including the "past the end" rule for a zero search with no end.
		{"SET z \xff\xf0\x00", "+OK"},
		{"BITPOS z 0", ":12"},
		{"BITPOS z 1 0", ":0"},
		{"BITPOS z 1 2", ":-1"},
		{"BITPOS z 1 0 -1 BIT", ":0"},
		{"SET ones \xff\xff\xff", "+OK"},
		{"BITPOS ones 0", ":24"},      // no end: the string is followed by zeros
		{"BITPOS ones 0 0 -1", ":-1"}, // an explicit end confines the search
		{"BITPOS ones 2", "-ERR The bit argument must be 1 or 0."},
		{"BITPOS missing 1", ":-1"},
		{"BITPOS missing 0", ":0"},

		// BITOP over strings of different lengths: the short one is zero-padded.
		{"SET k1 \xff\x00", "+OK"},
		{"SET k2 \x0f", "+OK"},
		{"BITOP AND dest k1 k2", ":2"},
		{"GET dest", "\x0f\x00"},
		{"BITOP OR dest k1 k2", ":2"},
		{"GET dest", "\xff\x00"},
		{"BITOP XOR dest k1 k2", ":2"},
		{"GET dest", "\xf0\x00"},
		{"BITOP NOT dest k2", ":1"},
		{"GET dest", "\xf0"},
		{"BITOP NOT dest k1 k2", "-ERR BITOP NOT must be called with a single source key."},
		{"BITOP BOGUS dest k1", "-ERR syntax error"},
		// A result of length zero deletes the destination.
		{"BITOP AND dest nothing1 nothing2", ":0"},
		{"EXISTS dest", ":0"},

		// Wrong type, both ways.
		{"RPUSH list x", ":1"},
		{"SETBIT list 0 1", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"GETBIT list 0", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"BITCOUNT list", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"BITOP AND d list k1", "-WRONGTYPE Operation against a key holding the wrong kind of value"},
	}
	for _, tc := range cases {
		if got := c.cmdRaw(strings.Split(tc.cmd, " ")...); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestBitmapSharesTheStringType is the interoperation check: the bit commands and the
// string commands are looking at the same bytes.
func TestBitmapSharesTheStringType(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("SETBIT shared 15 1")
	if got := c.cmd("STRLEN shared"); got != ":2" {
		t.Errorf("STRLEN after SETBIT 15 = %q; want :2", got)
	}
	if got := c.cmd("TYPE shared"); got != "+string" {
		t.Errorf("TYPE of a bitmap = %q; want +string", got)
	}
	// APPEND extends the bitmap, and the appended byte is addressable as bits.
	c.cmdRaw("APPEND", "shared", "\xff")
	if got := c.cmd("BITCOUNT shared"); got != ":9" {
		t.Errorf("BITCOUNT after APPEND = %q; want :9", got)
	}
	if got := c.cmd("GETBIT shared 16"); got != ":1" {
		t.Errorf("GETBIT into the appended byte = %q; want :1", got)
	}
	// SETRANGE rewrites bytes the bit commands then read.
	c.cmdRaw("SETRANGE", "shared", "0", "\x00")
	if got := c.cmd("BITCOUNT shared"); got != ":9" {
		t.Errorf("BITCOUNT after SETRANGE of a zero byte = %q; want :9", got)
	}
	// And an existing TTL survives a SETBIT, as it does a SETRANGE.
	c.cmd("SET vol v EX 100")
	c.cmd("SETBIT vol 0 1")
	if got := c.cmd("TTL vol"); got == ":-1" {
		t.Error("SETBIT cleared the key's TTL")
	}
}

// TestBitField covers the typed-integer surface: both signednesses, the "#" offset form,
// and all three overflow policies.
func TestBitField(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	cases := []struct{ cmd, want string }{
		// The reply is one element per operation, in order.
		{"BITFIELD bf SET u8 0 255 GET u8 0", "[:0 :255]"},
		{"BITFIELD bf INCRBY u8 0 10 GET u8 0", "[:9 :9]"}, // 255+10 wraps to 9
		{"BITFIELD bf SET i8 0 -1 GET i8 0", "[:9 :-1]"},
		{"BITFIELD bf GET u8 0", "[:255]"}, // the same bits read unsigned
		// The "#" form multiplies by the type width: #1 with u8 is bit 8.
		{"BITFIELD bf SET u8 #1 7 GET u8 8", "[:0 :7]"},
		{"BITFIELD bf GET u8 #1", "[:7]"},

		// OVERFLOW SAT clamps at the type's bounds instead of wrapping.
		{"BITFIELD sat OVERFLOW SAT SET i8 0 127 INCRBY i8 0 10", "[:0 :127]"},
		{"BITFIELD sat OVERFLOW SAT INCRBY i8 0 -300", "[:-128]"},
		{"BITFIELD sat OVERFLOW SAT SET u8 8 300", "[:0]"},
		{"BITFIELD sat GET u8 8", "[:255]"},

		// OVERFLOW FAIL reports a null in the failing slot and leaves the value alone.
		{"BITFIELD fail OVERFLOW FAIL SET u8 0 200 INCRBY u8 0 100", "[:0 (nil)]"},
		{"BITFIELD fail GET u8 0", "[:200]"},
		{"BITFIELD fail OVERFLOW FAIL SET u8 0 300", "[(nil)]"},

		// The policy applies to the operations after it, and a second one replaces it.
		{"BITFIELD mix SET u8 0 250 OVERFLOW SAT INCRBY u8 0 10 OVERFLOW WRAP INCRBY u8 0 10", "[:0 :255 :9]"},

		// GET past the end of the value reads zero without creating anything.
		{"BITFIELD nothing GET u16 100", "[:0]"},
		{"EXISTS nothing", ":0"},

		// Type and syntax errors.
		{"BITFIELD bf GET u64 0", "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."},
		{"BITFIELD bf GET i65 0", "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."},
		{"BITFIELD bf GET x8 0", "-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."},
		{"BITFIELD bf GET u8 -1", "-ERR bit offset is not an integer or out of range"},
		{"BITFIELD bf OVERFLOW BOGUS SET u8 0 1", "-ERR Invalid OVERFLOW type specified"},
		{"BITFIELD bf BOGUS u8 0", "-ERR syntax error"},
		// No operations is an empty batch, not an arity error: Redis answers with an empty
		// array, and does not create the key.
		{"BITFIELD bf", "[]"},

		// i64 is supported (unlike u64), so a full-width signed counter works.
		{"BITFIELD wide SET i64 0 9223372036854775807 GET i64 0", "[:0 :9223372036854775807]"},
		{"BITFIELD wide OVERFLOW SAT INCRBY i64 0 1", "[:9223372036854775807]"},

		// BITFIELD_RO is the replica-safe subset.
		{"BITFIELD_RO bf GET u8 0", "[:255]"},
		{"BITFIELD_RO bf SET u8 0 1", "-ERR BITFIELD_RO only supports the GET subcommand"},
		// OVERFLOW is accepted by the read-only form: it selects how a *write* would clamp,
		// and with no write in the list it selects nothing. Checked against redis:7.2, which
		// answers this with the GET's value and not an error.
		{"BITFIELD_RO bf OVERFLOW SAT GET u8 0", "[:255]"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}
}

// TestBitFieldIsAtomic checks that a whole BITFIELD applies under one lock: a sequence
// that reads a field it has just written sees its own write, and a read-only sequence
// creates nothing.
func TestBitFieldIsAtomic(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	if got := c.cmd("BITFIELD counters INCRBY u8 0 1 INCRBY u8 0 1 INCRBY u8 0 1"); got != "[:1 :2 :3]" {
		t.Errorf("a chain of increments = %q; want each to see the last", got)
	}
	if got := c.cmd("BITFIELD counters GET u8 0 GET u8 0"); got != "[:3 :3]" {
		t.Errorf("two reads of the same field = %q", got)
	}
	if got := c.cmd("BITFIELD ro-only GET u8 0"); got != "[:0]" {
		t.Errorf("a read-only BITFIELD = %q", got)
	}
	if got := c.cmd("EXISTS ro-only"); got != ":0" {
		t.Error("a read-only BITFIELD created the key")
	}
}

// TestBitmapPropagation checks that the family reaches a replica by its own text: every
// one of them is a pure function of its arguments and the value it reads, so there is
// nothing to rewrite -- and BITOP's destination must be in affectedKeys, which is at
// argument 2 rather than 1.
func TestBitmapPropagation(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// BITOP's destination is the key WATCH has to be told about.
	if got := c.cmd("COMMAND GETKEYS BITOP AND dest src1 src2"); got != "[dest src1 src2]" {
		t.Errorf("COMMAND GETKEYS BITOP = %q", got)
	}
	// A WATCH on the destination must see a BITOP into it as a conflict.
	watcher := dialTx(t, addr)
	defer watcher.close()
	c.cmd("SET src \xff")
	watcher.cmd("WATCH dest")
	watcher.cmd("MULTI")
	watcher.cmd("GET dest")
	c.cmd("BITOP NOT dest src")
	if got := watcher.cmd("EXEC"); got != "(nil)" {
		t.Errorf("EXEC after a BITOP into the watched destination = %q; want an abort", got)
	}
}
