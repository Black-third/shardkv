package server

// XINFO: STREAM, STREAM ... FULL, GROUPS and CONSUMERS.
//
// Split from commands_stream.go because it is purely a *reporting* surface -- it mutates
// nothing -- and because the FULL form alone is a third of it. What makes it worth its own
// file is that its correctness is entirely about reply *shape*: nine fields in one order for
// FULL and ten in another for the summary, three different pending-entry tuple widths, and a
// nullable entries-read. Every one of those was established by measuring redis:7.2 rather
// than by reading a specification.

import (
	"errors"
	"math"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

// --- XINFO --------------------------------------------------------------------

// cmdXInfo implements XINFO STREAM key | GROUPS key | CONSUMERS key group | HELP.
func cmdXInfo(s *Server, w *resp.Writer, args [][]byte) bool {
	sub := strings.ToUpper(string(args[1]))
	if sub == "HELP" {
		writeHelp(w, "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", []string{
			"CONSUMERS <key> <groupname>",
			"    Show consumers of <groupname>.",
			"GROUPS <key>",
			"    Show the stream consumer groups.",
			"STREAM <key>",
			"    Show information about the stream.",
		})
		return false
	}
	// The subcommand is validated before the argument count, because an unknown one is
	// not a syntax error: `XINFO NOPE` answered "ERR syntax error" here, where Redis
	// answers "unknown subcommand 'NOPE'. Try XINFO HELP." -- so a client that had
	// mistyped a subcommand was told its *arguments* were wrong.
	switch sub {
	case "STREAM", "GROUPS", "CONSUMERS":
	default:
		writeUnknownSubcommand(w, "XINFO", args[1])
		return false
	}
	// ...and the count is then an arity error naming the subcommand, which is what
	// CONSUMERS below already answered. STREAM and GROUPS shared a bare "syntax error",
	// so `XINFO STREAM` with the key forgotten did not say which argument was missing.
	if len(args) < 3 {
		w.WriteError("ERR wrong number of arguments for 'xinfo|" +
			strings.ToLower(sub) + "' command")
		return false
	}
	key := string(args[1+1])

	// Surplus arguments, per subcommand and measured. GROUPS takes exactly a key, so a
	// fourth argument is an arity error. STREAM accepts an optional FULL, so a fourth
	// argument is an unrecognised *option* and gets the other message. Both previously
	// went unchecked, and `XINFO GROUPS k extra` silently answered as if the extra
	// argument had not been sent -- a client passing an option this server does not
	// support was told it had worked.
	switch sub {
	case "GROUPS":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'xinfo|groups' command")
			return false
		}
	case "STREAM":
		// STREAM takes an optional FULL, itself taking an optional COUNT, so anything else
		// is an unrecognised *option* rather than a surplus argument -- which is why it gets
		// the other of Redis's two messages. Both spellings were measured.
		if len(args) != 3 && !xinfoFullForm(args) {
			writeSubcommandSyntaxError(w, "XINFO", args[1])
			return false
		}
	}

	switch sub {
	case "STREAM":
		if xinfoFullForm(args) {
			count, valid := xinfoFullCount(args)
			if !valid {
				w.WriteError("ERR value is not an integer or out of range")
				return false
			}
			info, ok, err := s.store.XInfoStreamFull(key, count)
			if err != nil {
				writeStoreErr(w, err)
				return false
			}
			if !ok {
				w.WriteError("ERR no such key")
				return false
			}
			writeXInfoStreamFull(w, info)
			return false
		}
		info, ok, err := s.store.XInfoStream(key)
		if err != nil {
			writeStoreErr(w, err)
			return false
		}
		if !ok {
			w.WriteError("ERR no such key")
			return false
		}
		writeXInfoStream(w, info)

	case "GROUPS":
		groups, ok, err := s.store.XInfoGroups(key, false)
		if err != nil {
			writeStoreErr(w, err)
			return false
		}
		if !ok {
			w.WriteError("ERR no such key")
			return false
		}
		w.WriteArrayHeader(len(groups))
		for _, g := range groups {
			writeXInfoGroup(w, g)
		}

	case "CONSUMERS":
		if len(args) != 4 {
			w.WriteError("ERR wrong number of arguments for 'xinfo|consumers' command")
			return false
		}
		group := string(args[3])
		consumers, err := s.store.XInfoConsumers(key, group)
		if err != nil {
			if errors.Is(err, store.ErrNoSuchStreamKey) {
				w.WriteError("ERR no such key")
				return false
			}
			writeStreamStoreErr(w, err, key, group, nogroupAdmin)
			return false
		}
		w.WriteArrayHeader(len(consumers))
		for _, c := range consumers {
			w.WriteMapHeader(4)
			w.WriteBulkString("name")
			w.WriteBulkString(c.Name)
			w.WriteBulkString("pending")
			w.WriteInt(c.Pending)
			w.WriteBulkString("idle")
			w.WriteInt(c.IdleMs)
			w.WriteBulkString("inactive")
			w.WriteInt(c.InactiveMs)
		}

	default:
		writeUnknownSubcommand(w, "XINFO", args[1])
	}
	return false
}

func writeXInfoStream(w *resp.Writer, info store.StreamInfo) {
	w.WriteMapHeader(10)
	w.WriteBulkString("length")
	w.WriteInt(info.Length)
	w.WriteBulkString("radix-tree-keys")
	// Both radix-tree fields are reported as the entry count rather than invented: this
	// server keeps entries in a sorted slice, not a radix tree of listpacks, so there are no
	// internal nodes to count. They exist because clients read them, and the honest value for
	// a structure with one level is "one node per entry" -- which is also why they are equal
	// here and are not in Redis.
	w.WriteInt(info.Length)
	w.WriteBulkString("radix-tree-nodes")
	w.WriteInt(info.Length)
	w.WriteBulkString("last-generated-id")
	w.WriteBulkString(info.LastID.String())
	w.WriteBulkString("max-deleted-entry-id")
	w.WriteBulkString(info.MaxDeletedID.String())
	w.WriteBulkString("entries-added")
	w.WriteInt(info.EntriesAdded)
	w.WriteBulkString("recorded-first-entry-id")
	w.WriteBulkString(info.RecordedFirstID.String())
	// The number of consumer groups on the stream. It was computed and then dropped, which
	// is the worst of the three options: a client reading XINFO STREAM to decide whether a
	// stream has consumers found the field missing rather than zero.
	w.WriteBulkString("groups")
	w.WriteInt(info.Groups)
	w.WriteBulkString("first-entry")
	if info.HasEntries {
		writeStreamEntry(w, info.First)
	} else {
		w.WriteNull()
	}
	w.WriteBulkString("last-entry")
	if info.HasEntries {
		writeStreamEntry(w, info.Last)
	} else {
		w.WriteNull()
	}
}

func writeXInfoGroup(w *resp.Writer, g store.StreamGroupInfo) {
	w.WriteMapHeader(6)
	w.WriteBulkString("name")
	w.WriteBulkString(g.Name)
	w.WriteBulkString("consumers")
	w.WriteInt(g.Consumers)
	w.WriteBulkString("pending")
	w.WriteInt(g.Pending)
	w.WriteBulkString("last-delivered-id")
	w.WriteBulkString(g.LastDelivered.String())
	w.WriteBulkString("entries-read")
	// A null entries-read means the count is not established, which is different from
	// zero: a group created at "$" and one created at "0" have both read nothing, but the
	// first is caught up and the second is the whole stream behind. Reporting 0 for both
	// told a client the caught-up group had fallen behind.
	if g.HasEntriesRead {
		w.WriteInt(g.EntriesRead)
	} else {
		w.WriteNull()
	}
	w.WriteBulkString("lag")
	// A null lag means "not knowable": the group sits inside a stream with a hole ahead of
	// it, so there is no cheap way to count what it has left to read. Redis will sometimes
	// estimate here and this does not -- a deliberate difference, because the lag is the
	// number an operator pages on and a wrong one is worse than an absent one. See
	// stream.groupLag for the cases that *are* answered, all measured against redis:7.2.
	if g.HasLag {
		w.WriteInt(g.Lag)
	} else {
		w.WriteNull()
	}
}

// xinfoFullForm reports whether this XINFO STREAM asked for the FULL report:
// `XINFO STREAM key FULL` or `XINFO STREAM key FULL COUNT n`, and nothing else.
func xinfoFullForm(args [][]byte) bool {
	if len(args) < 4 || !strings.EqualFold(string(args[3]), "FULL") {
		return false
	}
	switch len(args) {
	case 4:
		return true
	case 6:
		return strings.EqualFold(string(args[4]), "COUNT")
	}
	return false
}

// xinfoFullCount is FULL's entry limit, and reports whether the operand was valid.
//
// It defaults to 10 -- Redis's default, chosen so a FULL report on a large stream does not
// serialise the whole thing by accident -- and 0 means "all". Both boundaries were
// measured on redis:7.2, and they are not the same rule: a *negative* count silently falls
// back to the default, while a non-numeric one is refused with
// "value is not an integer or out of range". Treating the two alike (as the first version
// of this did) meant `XINFO STREAM s FULL COUNT x` quietly answered with ten entries
// instead of telling the caller its argument was nonsense.
func xinfoFullCount(args [][]byte) (int, bool) {
	const defaultCount = 10
	if len(args) != 6 {
		return defaultCount, true
	}
	n, ok := parseInt64(args[5])
	if !ok {
		return 0, false
	}
	switch {
	case n < 0:
		return defaultCount, true
	case n == 0 || n > math.MaxInt32:
		return 0, true // all of them
	}
	return int(n), true
}

// writeXInfoStreamFull writes the FULL report. The field order is Redis's, because a
// client may read the reply positionally, and the nesting is what a RESP3 client
// dispatches on: the report and each group are maps, while entries, groups, consumers and
// both pending lists are arrays. All of it was captured from redis:7.2 over both protocols.
func writeXInfoStreamFull(w *resp.Writer, info store.StreamFullInfo) {
	w.WriteMapHeader(9)
	w.WriteBulkString("length")
	w.WriteInt(info.Length)
	// Both radix-tree fields are the entry count rather than an invention: this server
	// keeps entries in a sorted slice, so there are no internal nodes to count. See
	// writeXInfoStream, which reports them the same way and explains why.
	w.WriteBulkString("radix-tree-keys")
	w.WriteInt(info.Length)
	w.WriteBulkString("radix-tree-nodes")
	w.WriteInt(info.Length)
	w.WriteBulkString("last-generated-id")
	w.WriteBulkString(info.LastID.String())
	w.WriteBulkString("max-deleted-entry-id")
	w.WriteBulkString(info.MaxDeletedID.String())
	w.WriteBulkString("entries-added")
	w.WriteInt(info.EntriesAdded)
	w.WriteBulkString("recorded-first-entry-id")
	w.WriteBulkString(info.RecordedFirstID.String())
	w.WriteBulkString("entries")
	w.WriteArrayHeader(len(info.Entries))
	for _, ent := range info.Entries {
		writeStreamEntry(w, ent)
	}
	w.WriteBulkString("groups")
	w.WriteArrayHeader(len(info.Groups))
	for _, g := range info.Groups {
		w.WriteMapHeader(7)
		w.WriteBulkString("name")
		w.WriteBulkString(g.Name)
		w.WriteBulkString("last-delivered-id")
		w.WriteBulkString(g.LastDelivered.String())
		w.WriteBulkString("entries-read")
		if g.HasEntriesRead {
			w.WriteInt(g.EntriesRead)
		} else {
			w.WriteNull()
		}
		w.WriteBulkString("lag")
		if g.HasLag {
			w.WriteInt(g.Lag)
		} else {
			w.WriteNull()
		}
		w.WriteBulkString("pel-count")
		w.WriteInt(g.PelCount)
		w.WriteBulkString("pending")
		w.WriteArrayHeader(len(g.Pending))
		for _, p := range g.Pending {
			// Four fields in a group's list: the entry, who holds it, when it was last
			// delivered and how many times. The consumer's own list below omits the name.
			w.WriteArrayHeader(4)
			w.WriteBulkString(p.ID.String())
			w.WriteBulkString(p.Consumer)
			w.WriteInt(p.DeliveryMs)
			w.WriteInt(p.DeliveryCount)
		}
		w.WriteBulkString("consumers")
		w.WriteArrayHeader(len(g.Consumers))
		for _, c := range g.Consumers {
			w.WriteMapHeader(5)
			w.WriteBulkString("name")
			w.WriteBulkString(c.Name)
			// Absolute instants, not durations -- see StreamFullConsumerInfo for why the
			// full form differs from XINFO CONSUMERS here.
			w.WriteBulkString("seen-time")
			w.WriteInt(c.SeenMs)
			w.WriteBulkString("active-time")
			w.WriteInt(c.ActiveMs)
			w.WriteBulkString("pel-count")
			w.WriteInt(c.PelCount)
			w.WriteBulkString("pending")
			w.WriteArrayHeader(len(c.Pending))
			for _, p := range c.Pending {
				w.WriteArrayHeader(3)
				w.WriteBulkString(p.ID.String())
				w.WriteInt(p.DeliveryMs)
				w.WriteInt(p.DeliveryCount)
			}
		}
	}
}
