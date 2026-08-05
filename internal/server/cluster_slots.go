package server

// Hash slots: the function that decides which node may serve a key.
//
// Redis Cluster partitions the keyspace into 16384 slots and gives each slot to
// exactly one master. A key's slot is CRC16 of the key -- or, when the key contains a
// hash tag, CRC16 of just the tag -- modulo 16384. Everything else about cluster mode
// follows from this one function: which node a client is redirected to, which keys a
// multi-key command may name, and what a slot migration moves.
//
// It has to be bit-exact with Redis. A client library computes the same slot locally to
// route without a round trip, so a server that disagreed by one bit would send MOVED
// replies to a client that had already decided otherwise, and the two would ping-pong.
// The implementation is therefore checked against a live redis:7-alpine (see
// TestKeySlotMatchesRealRedis for the vectors), not only against itself.

// numSlots is the size of the hash space. It is Redis's 16384 and not configurable:
// the number is baked into every client library's routing table and into the
// CLUSTER SLOTS/SHARDS replies.
const numSlots = 16384

// crc16Table is the CRC-16/XMODEM (CCITT-FALSE with a zero seed) byte table, generated
// rather than written out: a 256-entry literal is 256 chances to transcribe a digit
// wrongly, and a wrong entry would only show up as a slot that disagrees with Redis for
// some keys and not others.
var crc16Table = makeCRC16Table()

func makeCRC16Table() [256]uint16 {
	// The XMODEM parameters: polynomial 0x1021, initial value 0, no input or output
	// reflection, no final XOR. Redis's crc16.c uses exactly these.
	const poly = 0x1021
	var t [256]uint16
	for i := 0; i < 256; i++ {
		crc := uint16(i) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ poly
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return t
}

func crc16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc = crc<<8 ^ crc16Table[byte(crc>>8)^c]
	}
	return crc
}

// KeySlot returns the hash slot a key belongs to, applying Redis's hash-tag rule.
//
// The tag is what makes multi-key operations usable at all: "{user1000}.following" and
// "{user1000}.followers" hash to the same slot, so they live on the same node and MGET,
// SINTERSTORE or a transaction over both is legal. Without it every multi-key command
// spanning two keys would be a CROSSSLOT error.
//
// The rule has three edges that are easy to get wrong, and each of them is a case where
// the *whole key* is hashed rather than a tag:
//
//   - No '{' at all: the ordinary case.
//   - A '{' with no '}' after it ("{unclosed"): there is no tag, only a brace.
//   - An empty tag ("{}anything"): the braces are adjacent, so there is nothing to
//     hash, and hashing the empty string would collapse every such key onto one slot.
//
// Otherwise the tag is what lies between the *first* '{' and the first '}' after it,
// which is why "{a{b}c" hashes "a{b" -- the scan for the closing brace does not restart
// at the inner opening one, so braces do not nest. All four are pinned against a real
// Redis in the tests.
func KeySlot(key string) int {
	start := -1
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			start = i
			break
		}
	}
	if start < 0 {
		return int(crc16([]byte(key)) & (numSlots - 1))
	}
	end := -1
	for i := start + 1; i < len(key); i++ {
		if key[i] == '}' {
			end = i
			break
		}
	}
	if end < 0 || end == start+1 {
		return int(crc16([]byte(key)) & (numSlots - 1))
	}
	return int(crc16([]byte(key[start+1:end])) & (numSlots - 1))
}
