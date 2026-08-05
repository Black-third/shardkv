package server

import "testing"

// TestKeySlotMatchesRealRedis pins the slot function against a live redis:7-alpine.
//
// Every expected value here was read out of `redis-cli CLUSTER KEYSLOT` on a
// cluster-enabled redis:7-alpine, one key at a time -- not derived from the
// documentation and not computed by this implementation. That distinction is the whole
// value of the test: a client library computes the slot itself in order to route
// without a round trip, so an implementation that disagreed with Redis by one bit would
// send a MOVED to a client that had already picked the node it was told to leave, and
// the two would disagree forever without either being able to say why.
//
// The cases are chosen for the edges of Redis's tag parsing, which are specific:
//
//   - a tag makes two different keys share a slot ({user1000}.following and
//     .followers, both equal to the untagged user1000);
//   - an empty tag is not a tag ({}foo hashes the whole key, and does not collapse
//     onto the slot of the empty string);
//   - an unclosed brace is not a tag ({tag);
//   - a closing brace with no opening one is not a tag (tag});
//   - braces do not nest -- the scan for '}' does not restart at an inner '{' -- so
//     {a{b}c hashes "a{b", and {{double}} hashes "{double";
//   - only the first tag counts ({tag}{tag}{tag} and }{tag} both hash "tag");
//   - a tag of one space is a perfectly good tag ({ }).
func TestKeySlotMatchesRealRedis(t *testing.T) {
	cases := []struct {
		key  string
		slot int
	}{
		// Untagged keys.
		{"", 0},
		{"a", 15495},
		{"0", 13907},
		{"foo", 12182},
		{"bar", 5061},
		{"hello", 866},
		{"key:1", 6657},
		{"mylist", 5282},
		{"counter", 6680},
		{"user1000", 3443},
		{"abcdefghijklmnopqrstuvwxyz", 9132},

		// Tags: the point of the feature.
		{"{user1000}", 3443},
		{"{user1000}.following", 3443},
		{"{user1000}.followers", 3443},
		{"foo{bar}baz", 5061}, // == crc16("bar") == slot of "bar"
		{"somekey{tag}", 8338},
		{"{tag}{tag}{tag}", 8338}, // only the first tag counts
		{"}{tag}", 8338},          // a stray '}' before the tag is not the closing one
		{"{a}{b}", 15495},         // == slot of "a"
		{"{ }", 9314},             // a space is a tag

		// Empty tag: hash the whole key, never the empty string.
		{"{}", 15257},
		{"{}foo", 9500},
		{"foo{}", 5542},
		{"{}{}", 15786},
		{"x{}{y}", 14166}, // the first '{' pairs with the first '}' after it, and it is empty

		// Unbalanced braces: hash the whole key.
		{"{tag", 15608},
		{"tag}", 6488},
		{"}{", 12793},

		// Braces do not nest.
		{"{a{b}c", 13340},    // hashes "a{b"
		{"a{b{c}d}e", 15725}, // hashes "b{c"
		{"{{double}}", 5037}, // hashes "{double"
	}
	for _, tc := range cases {
		if got := KeySlot(tc.key); got != tc.slot {
			t.Errorf("KeySlot(%q) = %d; real Redis says %d", tc.key, got, tc.slot)
		}
	}
}

// TestKeySlotInRange is the property the table cannot state: whatever the key, the
// answer indexes the slot map. A slot outside the range would be an out-of-bounds
// panic on the redirect path rather than a wrong reply.
func TestKeySlotInRange(t *testing.T) {
	for i := 0; i < 4096; i++ {
		for _, key := range []string{
			string(rune(i)), "k" + string(rune(i)), "{" + string(rune(i)) + "}suffix",
		} {
			if slot := KeySlot(key); slot < 0 || slot >= numSlots {
				t.Fatalf("KeySlot(%q) = %d, outside 0..%d", key, slot, numSlots-1)
			}
		}
	}
}

// TestCRC16XMODEM checks the checksum itself against the published XMODEM check value,
// so a failure separates "the CRC is wrong" from "the tag parsing is wrong". The
// standard's check vector is the CRC of "123456789".
func TestCRC16XMODEM(t *testing.T) {
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Errorf("CRC-16/XMODEM(\"123456789\") = %#04x; want 0x31c3", got)
	}
	if got := crc16(nil); got != 0 {
		t.Errorf("CRC-16/XMODEM of the empty string = %#04x; want 0", got)
	}
}
