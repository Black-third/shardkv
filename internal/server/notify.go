package server

// Keyspace notifications: the events Redis publishes on the reserved
// __keyspace@<db>__:<key> and __keyevent@<db>__:<event> channels when a key changes,
// expires, or is evicted.
//
// The feature has to cost nothing when it is off, because the check sits on the write
// path of every command. It is therefore one atomic load of a bitmask, tested before
// any string is built: with no classes enabled, notifyWrite does not even upper-case
// the command name, let alone format a channel name. That is the reason the enabled
// classes are a bitmask on the Server rather than, say, a parsed struct behind a
// mutex.
//
// Notifications are deliberately local: they are not replicated and not persisted.
// Each node generates the events for the changes it applies -- a replica applying a
// write from its master fires the event for it, and expires its own keys through its
// own absolute deadlines -- so a subscriber sees the events of the node it is talking
// to. Shipping them would double every event for a client subscribed to a replica.

import (
	"strconv"
	"strings"
)

// notifyFlags is the set of enabled notification classes, matching Redis's
// notify-keyspace-events flag characters.
type notifyFlags int32

const (
	notifyKeyspace notifyFlags = 1 << iota // K: publish to __keyspace@0__:<key>
	notifyKeyevent                         // E: publish to __keyevent@0__:<event>
	notifyGeneric                          // g: DEL, EXPIRE, RENAME, ...
	notifyString                           // $: SET, APPEND, INCRBY, ...
	notifyList                             // l: LPUSH, LPOP, LTRIM, ...
	notifySet                              // s: SADD, SREM, SPOP, ...
	notifyHash                             // h: HSET, HDEL, HINCRBY, ...
	notifyZSet                             // z: ZADD, ZREM, ZPOPMIN, ...
	notifyExpired                          // x: a key reaching its deadline
	notifyEvicted                          // e: a key removed by the eviction policy
	notifyStream                           // t: XADD, XTRIM, XGROUP-CREATE, ...
)

// notifyAll is Redis's "A": every class except the K/E delivery selectors, which say
// where events go rather than which events exist.
const notifyAll = notifyGeneric | notifyString | notifyList | notifySet | notifyHash |
	notifyZSet | notifyExpired | notifyEvicted | notifyStream

// notifyClasses maps each flag character to the class it enables.
var notifyClasses = map[byte]notifyFlags{
	'K': notifyKeyspace,
	'E': notifyKeyevent,
	'g': notifyGeneric,
	'$': notifyString,
	'l': notifyList,
	's': notifySet,
	'h': notifyHash,
	'z': notifyZSet,
	'x': notifyExpired,
	'e': notifyEvicted,
	't': notifyStream,
	'A': notifyAll,
}

// notifyOrder is the order flags are rendered back in, so CONFIG GET returns a stable
// spelling rather than one that depends on map iteration.
var notifyOrder = []struct {
	ch   byte
	flag notifyFlags
}{
	{'g', notifyGeneric}, {'$', notifyString}, {'l', notifyList}, {'s', notifySet},
	{'h', notifyHash}, {'z', notifyZSet}, {'x', notifyExpired}, {'e', notifyEvicted},
	{'t', notifyStream}, {'K', notifyKeyspace}, {'E', notifyKeyevent},
}

// parseNotifyFlags parses a notify-keyspace-events specification, returning the flags
// and whether every character was recognized. An unknown character is rejected rather
// than ignored: silently dropping a letter would leave an operator believing they had
// enabled a class of events that never arrives.
func parseNotifyFlags(spec string) (notifyFlags, bool) {
	var flags notifyFlags
	for i := 0; i < len(spec); i++ {
		f, ok := notifyClasses[spec[i]]
		if !ok {
			return 0, false
		}
		flags |= f
	}
	return flags, true
}

// String renders the flags back into the specification form, collapsing a full set of
// event classes to "A" the way Redis does.
func (f notifyFlags) String() string {
	if f == 0 {
		return ""
	}
	var b strings.Builder
	if f&notifyAll == notifyAll {
		b.WriteByte('A')
	}
	for _, entry := range notifyOrder {
		if f&notifyAll == notifyAll && entry.flag&notifyAll != 0 {
			continue // already covered by the A
		}
		if f&entry.flag != 0 {
			b.WriteByte(entry.ch)
		}
	}
	return b.String()
}

// SetNotifyKeyspaceEvents enables the given notification classes, using Redis's flag
// characters (K, E, g, $, l, s, h, z, x, e, A). An empty string disables
// notifications, which is the default. It reports whether the specification was valid,
// and leaves the current setting untouched when it was not.
//
// Delivery needs at least one of K or E: a specification naming only event classes
// says which events matter but not where to send them, and Redis treats that as
// notifications being off.
func (s *Server) SetNotifyKeyspaceEvents(spec string) bool {
	flags, ok := parseNotifyFlags(spec)
	if !ok {
		return false
	}
	if flags&(notifyKeyspace|notifyKeyevent) == 0 {
		flags = 0
	}
	s.notifyFlags.Store(int32(flags))
	return true
}

// NotifyKeyspaceEvents reports the enabled classes in specification form, for
// CONFIG GET notify-keyspace-events.
func (s *Server) NotifyKeyspaceEvents() string {
	return notifyFlags(s.notifyFlags.Load()).String()
}

// notifySpec describes the event a command reports.
type notifySpec struct {
	class notifyFlags
	event string
	// second, when set, is the event for the command's second key. The two-key
	// commands describe their halves differently: a rename is a rename_from on the
	// source and a rename_to on the destination, and an LMOVE is a pop on one list and
	// a push on the other.
	second string
}

// notifyEvents maps a command to the event it reports. The names are Redis's, because
// the whole point of the feature is that an existing consumer of Redis notifications
// works unchanged.
//
// A command absent from this table reports nothing, which is correct for every read:
// a read changes no key, so there is no keyspace event to report.
var notifyEvents = map[string]notifySpec{
	// generic
	"DEL":       {notifyGeneric, "del", ""},
	"UNLINK":    {notifyGeneric, "unlink", ""},
	"EXPIRE":    {notifyGeneric, "expire", ""},
	"PEXPIRE":   {notifyGeneric, "expire", ""},
	"EXPIREAT":  {notifyGeneric, "expire", ""},
	"PEXPIREAT": {notifyGeneric, "expire", ""},
	"PERSIST":   {notifyGeneric, "persist", ""},
	"GETEX":     {notifyGeneric, "expire", ""}, // "persist" for GETEX PERSIST; see notifyWrite
	"GETDEL":    {notifyGeneric, "del", ""},
	"RESTORE":   {notifyGeneric, "restore", ""},
	// MIGRATE reports what it did *here*, which is a deletion. It only reaches this
	// table when it actually removed something: a MIGRATE ... COPY, or one that moved
	// nothing, reports no change and so fires no event.
	"MIGRATE":  {notifyGeneric, "del", ""},
	"RENAME":   {notifyGeneric, "rename_from", "rename_to"},
	"RENAMENX": {notifyGeneric, "rename_from", "rename_to"},
	"COPY":     {notifyGeneric, "copy_to", "copy_to"},
	"MOVE":     {notifyGeneric, "move_from", ""}, // move_to fires in the destination; see notifyWrite

	// strings
	"SET":         {notifyString, "set", ""},
	"SETNX":       {notifyString, "set", ""},
	"SETEX":       {notifyString, "set", ""},
	"PSETEX":      {notifyString, "set", ""},
	"GETSET":      {notifyString, "set", ""},
	"MSET":        {notifyString, "set", ""},
	"SETRANGE":    {notifyString, "setrange", ""},
	"APPEND":      {notifyString, "append", ""},
	"INCR":        {notifyString, "incrby", ""},
	"INCRBY":      {notifyString, "incrby", ""},
	"DECR":        {notifyString, "decrby", ""},
	"DECRBY":      {notifyString, "decrby", ""},
	"INCRBYFLOAT": {notifyString, "incrbyfloat", ""},

	// lists
	"LPUSH":     {notifyList, "lpush", ""},
	"RPUSH":     {notifyList, "rpush", ""},
	"LPOP":      {notifyList, "lpop", ""},
	"RPOP":      {notifyList, "rpop", ""},
	"LSET":      {notifyList, "lset", ""},
	"LINSERT":   {notifyList, "linsert", ""},
	"LREM":      {notifyList, "lrem", ""},
	"LTRIM":     {notifyList, "ltrim", ""},
	"RPOPLPUSH": {notifyList, "rpop", "lpush"},
	"LMOVE":     {notifyList, "lpop", "lpush"},

	// hashes
	"HSET":         {notifyHash, "hset", ""},
	"HSETNX":       {notifyHash, "hset", ""},
	"HDEL":         {notifyHash, "hdel", ""},
	"HINCRBY":      {notifyHash, "hincrby", ""},
	"HINCRBYFLOAT": {notifyHash, "hincrbyfloat", ""},

	// sets
	"SADD":        {notifySet, "sadd", ""},
	"SREM":        {notifySet, "srem", ""},
	"SPOP":        {notifySet, "spop", ""},
	"SMOVE":       {notifySet, "srem", "sadd"},
	"SINTERSTORE": {notifySet, "sinterstore", ""},
	"SUNIONSTORE": {notifySet, "sunionstore", ""},
	"SDIFFSTORE":  {notifySet, "sdiffstore", ""},

	// sorted sets
	"ZADD":             {notifyZSet, "zadd", ""},
	"ZINCRBY":          {notifyZSet, "zincr", ""},
	"ZREM":             {notifyZSet, "zrem", ""},
	"ZPOPMIN":          {notifyZSet, "zpopmin", ""},
	"ZPOPMAX":          {notifyZSet, "zpopmax", ""},
	"ZREMRANGEBYRANK":  {notifyZSet, "zremrangebyrank", ""},
	"ZREMRANGEBYSCORE": {notifyZSet, "zremrangebyscore", ""},

	// streams. XREADGROUP, XACK, XCLAIM and XAUTOCLAIM are deliberately absent: what they
	// change is a group's bookkeeping rather than the stream's contents, and a live
	// redis:7-alpine fires no event of their own for any of them -- the only event a group
	// read or a claim produces is the xgroup-createconsumer for a consumer it created
	// implicitly, which the handlers fire themselves (see notifyConsumerCreated). This
	// table was checked against that server command by command rather than derived from
	// the documentation, because an event that exists here and not there is exactly the
	// kind of difference a consumer of notifications would build on and then be surprised
	// by.
	"XADD":   {notifyStream, "xadd", ""},
	"XTRIM":  {notifyStream, "xtrim", ""},
	"XDEL":   {notifyStream, "xdel", ""},
	"XSETID": {notifyStream, "xsetid", ""},
}

// xgroupEvents maps an XGROUP subcommand to its event name. XGROUP is the one command
// whose event depends on its subcommand rather than its name, so it is resolved
// separately (see notifyWrite).
var xgroupEvents = map[string]string{
	"CREATE":         "xgroup-create",
	"SETID":          "xgroup-setid",
	"DESTROY":        "xgroup-destroy",
	"CREATECONSUMER": "xgroup-createconsumer",
	"DELCONSUMER":    "xgroup-delconsumer",
}

// notifyWrite reports the event a dirty write had, on every key it touched.
//
// The first line is the whole cost when notifications are disabled: one atomic load
// and a comparison, before any allocation or string work. Everything after it runs
// only for a server that asked for events.
//
// It is called with the command as the client sent it, not with the propagated form: a
// consumer expects to see "expire", not the PEXPIREAT that carries it to the replicas.
func (s *Server) notifyWrite(args [][]byte) {
	flags := notifyFlags(s.notifyFlags.Load())
	if flags == 0 {
		return
	}
	name := strings.ToUpper(string(args[0]))
	// XGROUP's event is named after its subcommand, so it is resolved before the table
	// lookup that every other command's event comes from.
	if name == "XGROUP" {
		if flags&notifyStream == 0 || len(args) < 3 {
			return
		}
		if event, ok := xgroupEvents[strings.ToUpper(string(args[1]))]; ok {
			s.notifyKeyspaceEvent(flags, notifyStream, event, string(args[2]))
		}
		return
	}
	spec, ok := notifyEvents[name]
	if !ok || flags&spec.class == 0 {
		return
	}
	event := spec.event
	// GETEX is the one command whose event depends on its options rather than its
	// name: it either sets a deadline or clears one.
	if name == "GETEX" {
		for _, a := range args[2:] {
			if strings.EqualFold(string(a), "PERSIST") {
				event = "persist"
			}
		}
	}
	// MOVE is the one command whose two events land in two different databases, so the
	// second one is reported through the destination's view rather than this one.
	if name == "MOVE" {
		if db, key, ok := crossDBTarget(name, args); ok && db < s.Databases() {
			s.notifyKeyspaceEvent(flags, spec.class, "move_from", key)
			s.forDB(db).notifyKeyspaceEvent(flags, spec.class, "move_to", key)
		}
		return
	}
	keys := affectedKeys(name, args)
	if spec.second != "" && len(keys) >= 2 {
		s.notifyKeyspaceEvent(flags, spec.class, event, keys[0])
		s.notifyKeyspaceEvent(flags, spec.class, spec.second, keys[1])
		return
	}
	for _, key := range keys {
		s.notifyKeyspaceEvent(flags, spec.class, event, key)
	}
}

// notifyKeyspaceEvent publishes one event on whichever of the two reserved channel
// families is enabled. The two carry the same information transposed: the keyspace
// channel is named after the key and carries the event, the keyevent channel is named
// after the event and carries the key, so a consumer can subscribe by whichever of
// the two it wants to filter on.
func (s *Server) notifyKeyspaceEvent(flags, class notifyFlags, event, key string) {
	if flags&class == 0 {
		return
	}
	// The database is part of the channel name, as in Redis: a consumer subscribed to
	// __keyspace@0__:* must not be handed an event about the same key name in database 3.
	// (The channels themselves are global -- Pub/Sub has no databases -- so the database
	// can only appear in the name.)
	db := strconv.Itoa(s.db)
	if flags&notifyKeyspace != 0 {
		s.publishMessage("__keyspace@"+db+"__:"+key, []byte(event))
	}
	if flags&notifyKeyevent != 0 {
		s.publishMessage("__keyevent@"+db+"__:"+event, []byte(key))
	}
}

// notifyRemoved reports a key the store removed on its own: an expiration or an
// eviction. It is wired into the store's removal hook, which is the only place that
// knows the difference -- and the only place that learns about a key nothing ever read
// again.
func (s *Server) notifyRemoved(key string, evicted bool) {
	flags := notifyFlags(s.notifyFlags.Load())
	if flags == 0 {
		return
	}
	if evicted {
		s.notifyKeyspaceEvent(flags, notifyEvicted, "evicted", key)
		return
	}
	s.notifyKeyspaceEvent(flags, notifyExpired, "expired", key)
}
