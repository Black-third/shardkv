package server

// Observability: the slow log, MONITOR, the latency monitor, per-command statistics,
// MEMORY USAGE/DOCTOR and COMMAND GETKEYS.
//
// # The cost when nothing is watching
//
// Every one of these features sits on the path of every command, so each has to be
// free when it is not in use, and the whole group has to be cheap even when it is.
// What that means concretely:
//
//   - Per-command statistics are always collected, because INFO commandstats is
//     always available. They are four atomic adds on the *command the dispatcher
//     already holds -- no map lookup, no lock, no allocation. That is why they live on
//     the table entry rather than in a side map keyed by name: a map keyed by name
//     would cost a hash and, worse, a lock or a sync.Map probe per command.
//   - The slow log is gated by one atomic load of its threshold. Under the threshold
//     (the default is 10ms, so almost always) nothing is built; only a command that
//     actually was slow pays for copying its arguments.
//   - The latency monitor is gated the same way, by a threshold that defaults to off.
//   - MONITOR is gated by an atomic count of attached monitors, so a server nobody is
//     monitoring never formats a line, never copies an argument, and never touches the
//     registry lock.
//
// TestObservabilityCostsNothingWhenUnused pins all of that with AllocsPerRun.
//
// # Why a monitor cannot stall the server
//
// A MONITOR feed uses the same contract Pub/Sub uses for a slow subscriber: a bounded
// queue drained by the connection's own goroutine, and a producer that finds the queue
// full drops the monitor rather than waiting. A monitoring client is by definition
// incidental to the workload -- it is watching, not participating -- so blocking every
// other client behind one that stopped reading its socket would be the worst possible
// trade. Redis makes the same call with client-output-buffer-limit.
//
// # What a monitor must never see
//
// MONITOR echoes the commands clients send, which makes it a credential leak by
// construction. AUTH's arguments are replaced with "(redacted)", as real Redis does,
// and so are the credentials inside HELLO ... AUTH -- the other command that carries a
// password.

import (
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("SLOWLOG", -2, false, cmdSlowlog)
	register("LATENCY", -2, false, cmdLatency)
	register("MEMORY", -2, false, cmdMemory)
	registerSession("MONITOR", 1, cmdMonitor)
}

// Defaults matching Redis's, so an operator's existing thresholds mean the same here.
const (
	defaultSlowlogThresholdUs = 10000 // 10ms
	defaultSlowlogMaxLen      = 128
	// latencyHistoryLen is how many samples one latency event retains, as in Redis.
	latencyHistoryLen = 160
	// monitorQueue bounds how far behind a MONITOR feed may fall before the server
	// hangs up on it. Deliberately small, for the reason pubsubQueue is.
	monitorQueue = 512
	// slowlogMaxArgs and slowlogMaxArgLen are Redis's truncation rules: at most 32
	// arguments are kept, with a synthetic final element counting the rest, and an
	// argument longer than 128 bytes is cut with a note of how much was dropped. A slow
	// log that retained whole values would be a second copy of the dataset.
	slowlogMaxArgs   = 32
	slowlogMaxArgLen = 128
)

// --- per-command statistics ---------------------------------------------------

// latencyBuckets is the number of buckets in a command's latency histogram. Bucket i
// holds the calls whose duration in microseconds has bit-length i, i.e. bucket 0 is
// "under 1us", bucket 1 is [1,2), bucket 11 is [1024,2048), and the last one is
// everything above. 64 buckets cover every int64 duration, and the whole histogram is
// 512 bytes per command -- small enough to keep on the table entry.
const latencyBuckets = 64

// record adds one call to the command's statistics. It is the whole cost the
// observability surface imposes unconditionally: three atomic adds on memory the
// dispatcher already had a pointer to.
func (c *command) record(dur time.Duration, failed bool) {
	us := dur.Microseconds()
	c.calls.Add(1)
	c.usec.Add(us)
	c.latency[bits.Len64(uint64(us))].Add(1)
	if failed {
		c.failed.Add(1)
	}
}

// percentileUs reports the approximate duration below which the given fraction of
// calls completed, in microseconds, read out of the histogram.
//
// The answer is the upper bound of the bucket the percentile falls in, so it is exact
// to within a factor of two and never *under*-reports -- an operator reading a p99 of
// 2048us knows no more than 1% of calls took longer than that. A histogram is what
// makes the statistic affordable at all: keeping exact percentiles would mean
// retaining every sample, and Redis's own latencystats is an approximation
// (t-digest) for the same reason.
func (c *command) percentileUs(fraction float64) float64 {
	total := c.calls.Load()
	if total == 0 {
		return 0
	}
	want := int64(math.Ceil(fraction * float64(total)))
	var seen int64
	for i := 0; i < latencyBuckets; i++ {
		seen += c.latency[i].Load()
		if seen >= want {
			if i == 0 {
				return 1 // "under a microsecond"; Redis reports a positive value too
			}
			return math.Ldexp(1, i) // 2^i us, the bucket's upper bound
		}
	}
	return 0
}

// resetStatsFor clears every command's counters, for CONFIG RESETSTAT.
//
// A container's per-subcommand entries are *discarded* rather than zeroed, which is what
// makes `LATENCY HISTOGRAM` report an empty histogram after a reset instead of a list of
// commands with zero calls: an entry exists only because its subcommand has been run since
// the last reset.
func resetCommandStats() {
	for _, c := range commandTable {
		c.reset()
		if !c.container {
			continue
		}
		c.subMu.Lock()
		c.subs = nil
		c.subMu.Unlock()
	}
}

func (c *command) reset() {
	c.calls.Store(0)
	c.usec.Store(0)
	c.rejected.Store(0)
	c.failed.Store(0)
	for i := range c.latency {
		c.latency[i].Store(0)
	}
}

// --- the slow log -------------------------------------------------------------

// slowlogEntry is one recorded slow command, in the shape SLOWLOG GET returns.
type slowlogEntry struct {
	id       int64
	unixSecs int64
	durUs    int64
	args     []string
	addr     string
	name     string
}

// SetSlowlogPolicy configures the slow log: commands taking longer than thresholdUs
// microseconds are recorded, keeping at most maxLen of them. A negative threshold
// disables recording entirely and zero records every command, which are Redis's
// meanings for the same values.
func (s *Server) SetSlowlogPolicy(thresholdUs int64, maxLen int64) {
	if maxLen < 0 {
		maxLen = 0
	}
	s.slowlogThreshold.Store(thresholdUs)
	s.slowlogMaxLen.Store(maxLen)
	s.slowlogMu.Lock()
	if int64(len(s.slowlog)) > maxLen {
		s.slowlog = s.slowlog[:maxLen]
	}
	s.slowlogMu.Unlock()
}

// SlowlogThresholdUs and SlowlogMaxLen report the policy, for CONFIG GET.
func (s *Server) SlowlogThresholdUs() int64 { return s.slowlogThreshold.Load() }
func (s *Server) SlowlogMaxLen() int64      { return s.slowlogMaxLen.Load() }

// recordSlowlog appends a command to the slow log if it was slow enough. The caller
// has already tested the threshold, so reaching here means the entry is wanted.
func (s *Server) recordSlowlog(sess *session, args [][]byte, dur time.Duration) {
	maxLen := s.slowlogMaxLen.Load()
	if maxLen == 0 {
		return
	}
	e := slowlogEntry{
		id:       s.slowlogNextID.Add(1) - 1,
		unixSecs: time.Now().Unix(),
		durUs:    dur.Microseconds(),
		args:     slowlogArgs(args),
	}
	if sess != nil {
		e.addr, e.name = sess.addr, sess.clientName()
	}
	s.slowlogMu.Lock()
	// Newest first, which is the order SLOWLOG GET reports and therefore the order that
	// makes "give me the last 10" a prefix rather than a scan from the far end.
	s.slowlog = append(s.slowlog, slowlogEntry{})
	copy(s.slowlog[1:], s.slowlog)
	s.slowlog[0] = e
	if int64(len(s.slowlog)) > maxLen {
		s.slowlog = s.slowlog[:maxLen]
	}
	s.slowlogMu.Unlock()
}

// slowlogArgs renders a command's arguments for the log, applying Redis's truncation
// rules so one enormous SET cannot pin a copy of its value in memory for as long as
// the log holds it.
func slowlogArgs(args [][]byte) []string {
	n := len(args)
	trimmed := false
	if n > slowlogMaxArgs {
		n = slowlogMaxArgs - 1
		trimmed = true
	}
	out := make([]string, 0, n+1)
	for _, a := range args[:n] {
		if len(a) > slowlogMaxArgLen {
			out = append(out, string(a[:slowlogMaxArgLen])+
				"... ("+strconv.Itoa(len(a)-slowlogMaxArgLen)+" more bytes)")
			continue
		}
		out = append(out, string(a))
	}
	if trimmed {
		out = append(out, "... ("+strconv.Itoa(len(args)-n)+" more arguments)")
	}
	return out
}

// cmdSlowlog implements SLOWLOG GET [count] | LEN | RESET | HELP.
func cmdSlowlog(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "GET":
		count := 10 // Redis's default when no count is given
		if len(args) == 3 {
			n, ok := parseInt64(args[2])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return false
			}
			// -1 means "everything", which is how a tool asks for the whole log without
			// having had to call SLOWLOG LEN first and race a concurrent write. Anything
			// below that is refused rather than treated as another spelling of it: a caller
			// that computed -2 has a bug, and silently answering it with the whole log hides
			// the bug behind a plausible reply.
			if n < -1 {
				w.WriteError("ERR count should be greater than or equal to -1")
				return false
			}
			if n < 0 {
				count = -1
			} else {
				count = int(min(n, math.MaxInt32))
			}
		} else if len(args) > 3 {
			w.WriteError("ERR wrong number of arguments for 'slowlog|get' command")
			return false
		}
		s.slowlogMu.Lock()
		entries := s.slowlog
		if count >= 0 && count < len(entries) {
			entries = entries[:count]
		}
		out := make([]slowlogEntry, len(entries))
		copy(out, entries)
		s.slowlogMu.Unlock()
		writeSlowlog(w, out)

	case "LEN":
		s.slowlogMu.Lock()
		n := len(s.slowlog)
		s.slowlogMu.Unlock()
		w.WriteInt(int64(n))

	case "RESET":
		s.slowlogMu.Lock()
		s.slowlog = nil
		s.slowlogMu.Unlock()
		w.WriteSimple("OK")

	case "HELP":
		writeHelp(w, "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", []string{
			"GET [<count>]",
			"    Return the top <count> entries from the slowlog (default: 10, -1: all).",
			"LEN",
			"    Return the number of entries in the slowlog.",
			"RESET",
			"    Reset the slowlog.",
		})

	default:
		writeUnknownSubcommand(w, "SLOWLOG", args[1])
	}
	return false
}

// writeSlowlog renders the log in Redis's shape: one array per entry of
// [id, timestamp, microseconds, [arguments], client-address, client-name].
func writeSlowlog(w *resp.Writer, entries []slowlogEntry) {
	w.WriteArrayHeader(len(entries))
	for _, e := range entries {
		w.WriteArrayHeader(6)
		w.WriteInt(e.id)
		w.WriteInt(e.unixSecs)
		w.WriteInt(e.durUs)
		w.WriteArrayHeader(len(e.args))
		for _, a := range e.args {
			w.WriteBulk([]byte(a))
		}
		w.WriteBulk([]byte(e.addr))
		w.WriteBulk([]byte(e.name))
	}
}

// --- the latency monitor ------------------------------------------------------

// latencySample is one recorded spike: when it happened and how long it took.
type latencySample struct {
	unixSecs int64
	ms       int64
}

// latencyTimeSeries is the retained history of one named event, plus the largest
// sample ever seen -- which has to be kept separately because the history is bounded
// and the peak an operator is chasing may have already aged out of it.
type latencyTimeSeries struct {
	samples []latencySample
	maxMs   int64
}

// SetLatencyThresholdMs configures the latency monitor: an event taking at least
// thresholdMs milliseconds is recorded. Zero disables it, as in Redis.
func (s *Server) SetLatencyThresholdMs(ms int64) {
	if ms < 0 {
		ms = 0
	}
	s.latencyThreshold.Store(ms)
}

// LatencyThresholdMs reports the configured threshold, for CONFIG GET.
func (s *Server) LatencyThresholdMs() int64 { return s.latencyThreshold.Load() }

// recordLatency adds a sample to a named event's time series. The caller has already
// tested the threshold.
//
// Consecutive samples in the same second are merged by keeping the larger, which is
// what Redis does: the series is a record of spikes, and a burst of slow commands
// inside one second is one spike whose size is the worst of them.
func (s *Server) recordLatency(event string, dur time.Duration) {
	ms := dur.Milliseconds()
	now := time.Now().Unix()

	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()
	if s.latencyEvents == nil {
		s.latencyEvents = make(map[string]*latencyTimeSeries)
	}
	ts := s.latencyEvents[event]
	if ts == nil {
		ts = &latencyTimeSeries{}
		s.latencyEvents[event] = ts
	}
	if ms > ts.maxMs {
		ts.maxMs = ms
	}
	if n := len(ts.samples); n > 0 && ts.samples[n-1].unixSecs == now {
		if ms > ts.samples[n-1].ms {
			ts.samples[n-1].ms = ms
		}
		return
	}
	ts.samples = append(ts.samples, latencySample{unixSecs: now, ms: ms})
	if len(ts.samples) > latencyHistoryLen {
		ts.samples = ts.samples[len(ts.samples)-latencyHistoryLen:]
	}
}

// cmdLatency implements LATENCY HISTORY event | RESET [event...] | LATEST | HELP.
func cmdLatency(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "HISTORY":
		if len(args) != 3 {
			w.WriteError("ERR wrong number of arguments for 'latency|history' command")
			return false
		}
		s.latencyMu.Lock()
		var samples []latencySample
		if ts := s.latencyEvents[string(args[2])]; ts != nil {
			samples = append(samples, ts.samples...)
		}
		s.latencyMu.Unlock()
		// An unknown event is an empty array rather than an error: a monitoring agent
		// polls for events that only exist once they have happened at least once.
		w.WriteArrayHeader(len(samples))
		for _, sample := range samples {
			w.WriteArrayHeader(2)
			w.WriteInt(sample.unixSecs)
			w.WriteInt(sample.ms)
		}

	case "RESET":
		// The reply is the number of event time series that were actually discarded, so a
		// caller can tell "there was nothing to reset" from "I reset something".
		n := 0
		s.latencyMu.Lock()
		if len(args) == 2 {
			n = len(s.latencyEvents)
			s.latencyEvents = nil
		} else {
			for _, e := range args[2:] {
				if _, ok := s.latencyEvents[string(e)]; ok {
					delete(s.latencyEvents, string(e))
					n++
				}
			}
		}
		s.latencyMu.Unlock()
		w.WriteInt(int64(n))

	case "LATEST":
		type latest struct {
			event         string
			secs, ms, max int64
		}
		var out []latest
		s.latencyMu.Lock()
		for event, ts := range s.latencyEvents {
			if len(ts.samples) == 0 {
				continue
			}
			last := ts.samples[len(ts.samples)-1]
			out = append(out, latest{event, last.unixSecs, last.ms, ts.maxMs})
		}
		s.latencyMu.Unlock()
		// Sorted by name so the reply is stable rather than following map iteration order.
		sort.Slice(out, func(i, j int) bool { return out[i].event < out[j].event })
		w.WriteArrayHeader(len(out))
		for _, l := range out {
			w.WriteArrayHeader(4)
			w.WriteBulk([]byte(l.event))
			w.WriteInt(l.secs)
			w.WriteInt(l.ms)
			w.WriteInt(l.max)
		}

	case "HISTOGRAM":
		latencyHistogram(w, args[2:])

	case "HELP":
		writeHelp(w, "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", []string{
			"HISTORY <event>",
			"    Return time-latency samples for <event>.",
			"LATEST",
			"    Return the latest latency samples for all events.",
			"HISTOGRAM [<command> ...]",
			"    Return a cumulative distribution of latencies, per command.",
			"RESET [<event> ...]",
			"    Reset latency data of one or more <event> classes.",
		})

	default:
		writeUnknownSubcommand(w, "LATENCY", args[1])
	}
	return false
}

// latencyHistogram answers LATENCY HISTOGRAM [command ...]: the per-command latency
// distribution, as a cumulative histogram.
//
// It reports the same measurements INFO latencystats reads -- the per-command bucket
// counts kept on the table entry -- in the shape Redis publishes them, which is a map of
// command name to {calls, histogram_usec}, where histogram_usec maps a bucket's upper
// bound in microseconds to the number of calls at or below it. Cumulative, so the last
// value equals calls; that is what makes any percentile readable off it without the client
// having to sum anything.
//
// A named command with no calls recorded is omitted rather than reported as empty, and so
// is a name that is not a command at all -- an empty reply for "nothing has been measured"
// is Redis's answer and the useful one for a monitoring agent that polls a fixed list.
//
// The bucket bounds are powers of two because that is the histogram this server keeps
// (bucket i holds the calls whose duration has bit-length i, so it spans
// [2^(i-1), 2^i - 1] and its upper bound is 2^i - 1). Redis's are hdr_histogram's, which
// are finer; both are approximations, and reporting this one's real boundaries is better
// than interpolating onto Redis's and implying a precision the data does not have.
func latencyHistogram(w *resp.Writer, names [][]byte) {
	type entry = statsEntry
	var out []entry
	if len(names) == 0 {
		for _, c := range statsEntries() {
			if c.calls.Load() > 0 {
				out = append(out, entry{c.lowerName, c})
			}
		}
		// statsEntries is already sorted by command name; sorting again puts a container's
		// subcommands in overall alphabetical order, which is the order Redis's reply is in.
		sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	} else {
		for _, n := range names {
			out = append(out, latencyHistogramFor(string(n))...)
		}
	}

	w.WriteMapHeader(len(out))
	for _, e := range out {
		w.WriteBulk([]byte(e.name))
		buckets, calls := e.cmd.cumulativeLatency()
		w.WriteMapHeader(2)
		w.WriteBulk([]byte("calls"))
		w.WriteInt(calls)
		w.WriteBulk([]byte("histogram_usec"))
		w.WriteMapHeader(len(buckets))
		for _, b := range buckets {
			w.WriteInt(b.upperUs)
			w.WriteInt(b.cumulative)
		}
	}
}

// latencyHistogramFor resolves one name a caller asked for into the entries to report.
//
// Three spellings, all of which Redis accepts: an ordinary command name; a container's name,
// which expands to every subcommand of it that has been called (and not to the container
// itself); and a fully qualified "container|subcommand". A name that is not a command, or one
// with nothing recorded, contributes nothing -- an empty reply for "nothing measured" is what
// lets a monitoring agent poll a fixed list.
func latencyHistogramFor(name string) []statsEntry {
	type entry = statsEntry
	parent, sub, qualified := strings.Cut(name, "|")
	c, ok := commandTable[strings.ToUpper(parent)]
	if !ok {
		return nil
	}
	if qualified {
		for _, e := range c.subStats() {
			if e.lowerName == c.lowerName+"|"+strings.ToLower(sub) && e.calls.Load() > 0 {
				return []entry{{e.lowerName, e}}
			}
		}
		return nil
	}
	if c.container {
		var out []entry
		for _, e := range c.subStats() {
			if e.calls.Load() > 0 {
				out = append(out, entry{e.lowerName, e})
			}
		}
		return out
	}
	if c.calls.Load() == 0 {
		return nil
	}
	return []entry{{c.lowerName, c}}
}

// statsEntry pairs a reported name with the entry holding its measurements. It is a named
// type rather than an anonymous struct so that the two functions building these lists share
// one shape.
type statsEntry struct {
	name string
	cmd  *command
}

// latencyBucket is one point of a cumulative histogram: an upper bound in microseconds
// and how many calls completed at or below it.
type latencyBucket struct {
	upperUs    int64
	cumulative int64
}

// cumulativeLatency reads the command's histogram out as a cumulative distribution,
// skipping the buckets nothing fell into. The total is taken from the buckets rather than
// from the calls counter so that the reply is internally consistent: the two are separate
// atomics, and a concurrent call landing between the two reads would otherwise produce a
// histogram whose last value disagreed with the call count beside it.
func (c *command) cumulativeLatency() ([]latencyBucket, int64) {
	var out []latencyBucket
	var total int64
	for i := 0; i < latencyBuckets; i++ {
		n := c.latency[i].Load()
		if n == 0 {
			continue
		}
		total += n
		// Bucket 0 is "took less than a microsecond", whose highest represented value is 0.
		var upper int64
		if i > 0 {
			upper = int64(1)<<uint(i) - 1
		}
		out = append(out, latencyBucket{upperUs: upper, cumulative: total})
	}
	return out, total
}

// --- MONITOR ------------------------------------------------------------------

// monitorSink is one attached monitor: a bounded queue of already-formatted lines and
// the one-shot signal that ends the connection when it falls too far behind. It is the
// same shape as a Pub/Sub subscriber, for the same reason.
type monitorSink struct {
	ch      chan string
	dropped chan struct{}
	once    atomic.Bool
}

func newMonitorSink() *monitorSink {
	return &monitorSink{ch: make(chan string, monitorQueue), dropped: make(chan struct{})}
}

// drop marks the monitor as too far behind, reporting whether this call was the one
// that did it.
func (m *monitorSink) drop() bool {
	if m.once.CompareAndSwap(false, true) {
		close(m.dropped)
		return true
	}
	return false
}

// cmdMonitor turns the calling connection into a monitor: every command any client
// runs afterwards is echoed to it.
//
// It is a session command because what it changes is one socket, and it is not a write
// for the same reason -- nothing it does survives the connection, so there is nothing
// for the AOF or a replica to reconstruct.
func cmdMonitor(s *Server, sess *session, w *resp.Writer, args [][]byte) {
	if sess.monitor != nil {
		w.WriteSimple("OK") // already monitoring; idempotent, as in Redis
		return
	}
	sink := newMonitorSink()
	sess.monitor = sink
	s.monitorMu.Lock()
	if s.monitors == nil {
		s.monitors = make(map[*session]*monitorSink)
	}
	s.monitors[sess] = sink
	s.monitorCount.Store(int64(len(s.monitors)))
	s.monitorMu.Unlock()
	// The connection loop notices this and starts the feed's pump once the reply is out.
	sess.monitoring = true
	w.WriteSimple("OK")
}

// detachMonitor unregisters a monitoring connection. It runs from closeSession, so a
// monitor that hangs up cannot leave the command path formatting lines for a queue
// nobody drains.
func (s *Server) detachMonitor(sess *session) {
	if sess.monitor == nil {
		return
	}
	s.monitorMu.Lock()
	delete(s.monitors, sess)
	s.monitorCount.Store(int64(len(s.monitors)))
	s.monitorMu.Unlock()
}

// pumpMonitor writes the fed lines to the monitoring connection. Like the Pub/Sub
// pump it is one goroutine per connection and it takes the session's writer lock, so a
// line is never interleaved with a command reply.
func (s *Server) pumpMonitor(sess *session, w *resp.Writer) {
	sink := sess.monitor
	for {
		select {
		case <-sess.done:
			return
		case <-sink.dropped:
			if sess.conn != nil {
				sess.conn.Close()
			}
			return
		case line := <-sink.ch:
			sess.wmu.Lock()
			w.WriteSimple(line)
			err := w.Flush()
			sess.wmu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// feedMonitors echoes one command to every attached monitor.
//
// The first line is the whole cost on a server nobody is monitoring: a single atomic
// load, before the line is formatted or an argument is copied. A monitor whose queue is
// full is dropped rather than waited for -- see the file comment.
func (s *Server) feedMonitors(sess *session, db int, args [][]byte) {
	if s.monitorCount.Load() == 0 {
		return
	}
	line := monitorLine(sess, db, args, time.Now())

	var slow []*session
	s.monitorMu.Lock()
	for other, sink := range s.monitors {
		// A monitor does not see its own MONITOR, and Redis excludes the monitoring
		// connection's own traffic from its feed as well.
		if other == sess {
			continue
		}
		select {
		case sink.ch <- line:
		default:
			slow = append(slow, other)
		}
	}
	s.monitorMu.Unlock()

	for _, other := range slow {
		if other.monitor.drop() {
			s.monitorDrops.Add(1)
		}
	}
}

// monitorLine formats one fed command the way Redis does:
//
//	<unix-seconds>.<microseconds> [<db> <addr>] "CMD" "arg" ...
//
// Redaction happens here rather than at the call site so there is exactly one place
// that decides what a monitor may see. See redactedArgs.
func monitorLine(sess *session, db int, args [][]byte, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d.%06d [%d %s]", now.Unix(), now.Nanosecond()/1000, db, monitorAddr(sess))
	for _, a := range redactedArgs(args) {
		b.WriteString(" ")
		b.WriteString(strconv.Quote(a))
	}
	return b.String()
}

func monitorAddr(sess *session) string {
	if sess == nil || sess.addr == "" {
		// A command with no client socket behind it: an AOF replay or a master's stream.
		// Redis labels those "lua" or "unix:0"; naming the source honestly is better than
		// borrowing a label that means something else.
		return "internal:0"
	}
	return sess.addr
}

// redactedArgs returns the command's arguments with any credential replaced.
//
// A monitor is a plaintext echo of everything clients send, which makes it the one
// place a password reliably ends up in a log file. AUTH is redacted whole (both its
// one-argument and its user/password form), and HELLO's AUTH clause is redacted in
// place, because those are the two commands that carry a secret. Real Redis redacts
// exactly these.
func redactedArgs(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	if len(out) == 0 {
		return out
	}
	switch strings.ToUpper(out[0]) {
	case "AUTH":
		for i := 1; i < len(out); i++ {
			out[i] = "(redacted)"
		}
	case "HELLO", "CONFIG":
		// HELLO 3 AUTH <user> <pass>, and CONFIG SET requirepass|masterauth <secret>.
		for i := 1; i < len(out); i++ {
			if strings.EqualFold(out[i], "AUTH") {
				for j := i + 1; j < len(out); j++ {
					out[j] = "(redacted)"
				}
				break
			}
			if strings.EqualFold(out[i], "requirepass") || strings.EqualFold(out[i], "masterauth") {
				for j := i + 1; j < len(out); j++ {
					out[j] = "(redacted)"
				}
				break
			}
		}
	}
	return out
}

// --- the command observation hook ---------------------------------------------

// observeStart is called before a command runs and returns the instant to measure
// from, plus the number of errors already written to this connection -- which is how
// the "failed calls" counter learns whether the command answered with an error
// without every handler having to report it.
func observeStart(w *resp.Writer) (time.Time, int64) {
	return time.Now(), w.ErrorsWritten()
}

// observeCommand records everything the observability surface wants about a command
// that has just finished: its per-command statistics, a slow-log entry if it was slow,
// and a latency sample if it crossed the latency-monitor threshold.
//
// cmd is passed in rather than looked up by name because the caller already resolved
// it, and because looking it up here would mean upper-casing the name again -- an
// allocation on the path this function exists to keep allocation-free.
func (s *Server) observeCommand(sess *session, cmd *command, args [][]byte, start time.Time, errs0 int64, w *resp.Writer) {
	dur := time.Since(start)
	if sess != nil {
		// Time the client spent parked in a blocking wait is not time this server spent
		// working. See session.blockedFor.
		dur -= sess.blockedFor
		if dur < 0 {
			dur = 0
		}
	}
	// Recorded against the subcommand for a container command, which is what Redis reports
	// and the only figure that means anything for one: CONFIG GET and CONFIG RESETSTAT are
	// not the same command in any sense a latency reading cares about. For everything else
	// statsTarget returns the entry the dispatcher already holds, so this is still the same
	// three atomic adds it was.
	cmd.statsTarget(args).record(dur, w.ErrorsWritten() != errs0)

	if t := s.slowlogThreshold.Load(); t >= 0 && dur.Microseconds() >= t {
		s.recordSlowlog(sess, args, dur)
	}
	if t := s.latencyThreshold.Load(); t > 0 && dur.Milliseconds() >= t {
		// One event name, "command", which is the one this server can report honestly: the
		// others in Redis's list (fork, aof-fsync-always, expire-cycle) name work done by
		// machinery this server either does not have or does not do on a measurable
		// boundary. Inventing those names would let an operator read a zero as "this never
		// happens" rather than "this is not measured here".
		s.recordLatency("command", dur)
	}
}

// --- MEMORY -------------------------------------------------------------------

// cmdMemory implements MEMORY USAGE key [SAMPLES n] | DOCTOR | HELP.
func cmdMemory(s *Server, w *resp.Writer, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "USAGE":
		if len(args) < 3 {
			w.WriteError("ERR wrong number of arguments for 'memory|usage' command")
			return false
		}
		// SAMPLES is accepted and ignored: it bounds how many elements of a large
		// collection Redis samples before extrapolating, and this estimate walks the
		// whole value instead, so there is nothing for the option to trade off. Rejecting
		// it would break a client that always sends it.
		if len(args) > 3 {
			if len(args) != 5 || !strings.EqualFold(string(args[3]), "SAMPLES") {
				w.WriteError("ERR syntax error")
				return false
			}
			if n, ok := parseInt64(args[4]); !ok || n < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return false
			}
		}
		n, ok := s.store.MemoryUsage(string(args[2]))
		if !ok {
			w.WriteNull()
			return false
		}
		w.WriteInt(n)

	case "STATS":
		if len(args) != 2 {
			w.WriteError("ERR wrong number of arguments for 'memory|stats' command")
			return false
		}
		s.memoryStats(w)

	case "DOCTOR":
		w.WriteVerbatim("txt", []byte(s.memoryDoctor()))

	case "HELP":
		writeHelp(w, "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", []string{
			"USAGE <key> [SAMPLES <count>]",
			"    Estimate the memory usage of <key>.",
			"STATS",
			"    Return information about the memory usage of the server.",
			"DOCTOR",
			"    Report memory problems and advice.",
		})

	default:
		writeUnknownSubcommand(w, "MEMORY", args[1])
	}
	return false
}

// memoryStats answers MEMORY STATS: a RESP3 map (a flat name/value array in RESP2) of
// what this server can say about where its memory has gone, with a nested sub-map per
// database, in the field order real Redis emits.
//
// # What is reported, and what is deliberately absent
//
// Redis's reply is 29 fields, and most of them are derived from two inputs this server
// does not have: a live figure from an allocator it controls (jemalloc's allocated /
// active / resident / muzzy), and the heap size recorded at startup. Go's runtime exposes
// neither an allocator report nor a high-water mark of live heap bytes. So the choice for
// each field was between deriving it from something true and inventing something
// plausible, and every field that could only have been invented is *omitted* rather than
// filled with a zero or a ratio -- a reply that is missing a field says "this server
// cannot tell you"; a reply carrying a fabricated 1.0 fragmentation ratio says something
// false about the process, which is what invariant 12 forbids by name.
//
// Omitted, with the reason:
//
//	peak.allocated        Go exposes no peak of live heap bytes. runtime.MemStats has
//	                      HeapAlloc (now) and HeapSys (address space obtained), neither of
//	                      which is the high-water mark of the dataset. A peak sampled at
//	                      each MEMORY STATS call would be a peak *among readings*, and an
//	                      operator reads this field as authoritative.
//	startup.allocated     the Go heap at the moment serving began is not recorded, and
//	                      would in any case be dominated by runtime structures rather than
//	                      by the empty-server cost the field names.
//	dataset.percentage    Redis's is dataset / (total.allocated - startup.allocated).
//	peak.percentage       Redis's is total.allocated / peak.allocated.
//	                      Both denominators are omitted above, and computing either from a
//	                      different denominator under Redis's field name would be a
//	                      different statistic wearing its label.
//	aof.buffer            the AOF layer does not expose how many bytes it is holding
//	                      unwritten. 0 would be exactly right under appendfsync always and
//	                      false under everysec, which is the default.
//	allocator.allocated / .active / .resident / .muzzy
//	allocator-fragmentation.ratio / .bytes
//	allocator-rss.ratio / .bytes
//	rss-overhead.ratio / .bytes
//	fragmentation / fragmentation.bytes
//	                      all of these describe an allocator's internal behaviour. Go's
//	                      allocator publishes no equivalent, and fragmentation in
//	                      particular is the field an operator acts on -- a number derived
//	                      from Sys/HeapAlloc would look like jemalloc's and mean something
//	                      else entirely.
//	overhead.db.hashtable.lut / .rehashing, db.dict.rehashing.count
//	                      redis 7.4 additions describing its dict's rehashing state. Go's
//	                      maps rehash too but expose nothing about it.
//
// Inside the per-database sub-map, overhead.hashtable.expires and
// overhead.hashtable.slot-to-keys are omitted for a structural reason rather than a
// measurement one: there is no separate expires table here (a key's deadline lives in the
// entry itself, so its cost is already inside overhead.hashtable.main), and there is no
// slot-to-keys index -- which real Redis 7 also reports as a constant 0, the field having
// outlived the structure.
//
// # The identities the reply does hold
//
// dataset.bytes is the sum of the same per-key estimate MEMORY USAGE reports, less the
// keyspace bookkeeping, so a caller cannot get two different answers from the two
// commands. clients.normal and clients.slaves are the sums of exactly the tot-mem field
// CLIENT LIST reports for those connections. keys.bytes-per-key is dataset.bytes over
// keys.count -- note that Redis's is (total.allocated - startup.allocated) / keys.count
// instead, which also charges each key a share of the server's fixed cost; this one is
// the data alone, which is the reading its name supports.
//
// It is O(live keys): the size of a value is a property of the value, so no counter could
// stand in for the walk (see store.Footprint). Nothing but this command pays for it.
func (s *Server) memoryStats(w *resp.Writer) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// The replication backlog's retained bytes -- the same number INFO reports as
	// repl_backlog_histlen, and the same one Redis's replication.backlog field carries
	// (it reports histlen, not the ring's capacity).
	s.mu.Lock()
	backlog := s.backlog.histLen()
	s.mu.Unlock()

	// Client buffers, split the way Redis splits them, and summed from the same
	// per-connection accounting CLIENT LIST publishes as tot-mem. Two sums of one number
	// rather than a second estimate, so INFO, CLIENT LIST and MEMORY STATS cannot
	// disagree about what a connection costs.
	var clientsNormal, clientsSlaves int64
	for _, sess := range s.snapshotSessions() {
		mem := clientReadBufSize + clientWriteBufSize + sess.multiMem.Load()
		if sess.isReplicaFeed.Load() {
			clientsSlaves += mem
		} else {
			clientsNormal += mem
		}
	}

	// Per database, in Redis's order: every database gets an entry, including an empty
	// one, because Redis emits db.N for every database that exists in its dict array and
	// a client counting databases would otherwise see them appear and vanish.
	type dbEntry struct {
		index int
		fp    store.Footprint
	}
	dbs := make([]dbEntry, 0, len(s.dbs))
	var overheadDBs, keys, dataset int64
	for i := range s.dbs {
		fp := s.dbs[i].Footprint()
		if fp.Keys == 0 {
			continue // as in Redis, which skips a database holding nothing
		}
		dbs = append(dbs, dbEntry{index: i, fp: fp})
		overheadDBs += fp.Overhead
		keys += fp.Keys
		dataset += fp.Dataset
	}
	overheadTotal := backlog + clientsNormal + clientsSlaves + overheadDBs
	var bytesPerKey int64
	if keys > 0 {
		bytesPerKey = dataset / keys
	}

	// The reply. Field order is redis 7.2.15's and 7.4.10's (they agree on every field
	// emitted here), measured rather than taken from documentation.
	fields := []struct {
		name  string
		value int64
	}{
		// The dataset, which is what INFO reports as used_memory and what maxmemory is
		// compared against. Redis's total.allocated is zmalloc_used_memory(), i.e. exactly
		// its used_memory, so reporting anything else here would put two numbers under one
		// name. This used to be the Go heap, which was the honest answer while nothing
		// measured the dataset; now that something does, the heap is reported separately
		// as INFO's used_memory_go_heap and this field agrees with used_memory.
		{"total.allocated", s.UsedMemory()},
		{"replication.backlog", backlog},
		{"clients.slaves", clientsSlaves},
		{"clients.normal", clientsNormal},
		// No cluster bus is implemented, so there are no cluster links to hold memory.
		// 0 is the measurement, not a placeholder.
		{"cluster.links", 0},
		// No Lua and no functions: nothing is cached because nothing can be loaded.
		{"lua.caches", 0},
		{"functions.caches", 0},
	}
	w.WriteMapHeader(len(fields) + len(dbs) + 4)
	for _, f := range fields {
		w.WriteBulk([]byte(f.name))
		w.WriteInt(f.value)
	}
	for _, db := range dbs {
		w.WriteBulk([]byte("db." + strconv.Itoa(db.index)))
		// A nested map: RESP3 writes a map, RESP2 the flat array Redis sends there.
		w.WriteMapHeader(1)
		w.WriteBulk([]byte("overhead.hashtable.main"))
		w.WriteInt(db.fp.Overhead)
	}
	w.WriteBulk([]byte("overhead.total"))
	w.WriteInt(overheadTotal)
	w.WriteBulk([]byte("keys.count"))
	w.WriteInt(keys)
	w.WriteBulk([]byte("keys.bytes-per-key"))
	w.WriteInt(bytesPerKey)
	w.WriteBulk([]byte("dataset.bytes"))
	w.WriteInt(dataset)
}

// memoryDoctor reports what this server can actually diagnose about its own memory.
//
// Redis's version reads a set of thresholds off its allocator statistics. This one has
// no allocator statistics to read, so rather than inventing findings it reports the
// facts it does have -- the key counts, the eviction policy and whether eviction is
// running -- and says plainly which of Redis's checks it cannot perform. A doctor that
// answered "Sam, I detected a few issues" from no evidence would be worse than one
// that says what it looked at.
func (s *Server) memoryDoctor() string {
	var b strings.Builder
	total, volatile := 0, 0
	for i := range s.dbs {
		keys, expires := s.dbs[i].LenWithExpires()
		total += keys
		volatile += expires
	}
	cap0 := s.dbs[0].MaxKeys()
	used, maxmem := s.UsedMemory(), s.MaxMemory()
	policy := s.maxmemoryPolicy()
	b.WriteString("Memory doctor report for shardkv " + Version + ".\n")
	fmt.Fprintf(&b, "Live keys across %d databases: %d (%d with a TTL).\n", len(s.dbs), total, volatile)
	fmt.Fprintf(&b, "Dataset: %s (%d bytes) by the MEMORY USAGE estimate.\n",
		formatMemoryHuman(used), used)
	switch {
	case maxmem == 0:
		b.WriteString("No byte budget is set (maxmemory 0): the keyspace grows until the " +
			"process runs out of memory. Set maxmemory with a policy that evicts, set " +
			"maxkeys, or give every key a TTL.\n")
	case used > maxmem:
		fmt.Fprintf(&b, "The dataset is over its budget (%s > maxmemory %s) under "+
			"maxmemory-policy %s. %s\n", formatMemoryHuman(used), formatMemoryHuman(maxmem),
			policy, overBudgetAdvice(policy, volatile))
	default:
		fmt.Fprintf(&b, "The dataset is within its budget (%s of %s) under maxmemory-policy "+
			"%s; %d keys evicted so far.\n", formatMemoryHuman(used),
			formatMemoryHuman(maxmem), policy, s.EvictedKeys())
	}
	switch {
	case cap0 == 0:
		// Nothing to say: the key cap is off, and the byte budget above is the bound that
		// matters.
	case total > cap0:
		fmt.Fprintf(&b, "The keyspace is also over its key cap (%d keys > maxkeys %d); the "+
			"janitor is evicting. Sustained overshoot means writes are arriving faster than "+
			"the eviction pass reclaims.\n", total, cap0)
	default:
		fmt.Fprintf(&b, "The keyspace is within its key cap (maxkeys %d).\n", cap0)
	}
	b.WriteString("The byte figures are the dataset only -- keys, values and the keyspace's " +
		"per-key overhead. They exclude the Go runtime, the replication backlog and client " +
		"buffers, so there is no fragmentation ratio and no allocator report to diagnose: " +
		"Go publishes neither. MEMORY USAGE estimates a single key's footprint from its " +
		"contents.\n")
	return b.String()
}

// overBudgetAdvice says what an operator can do about a dataset that is over its budget,
// which depends entirely on the policy: noeviction is refusing writes right now, and a
// volatile-* policy over a keyspace with nothing volatile is refusing them for a reason that
// looks like a bug until it is named.
func overBudgetAdvice(policy string, volatile int) string {
	switch {
	case policy == "noeviction":
		return "Writes that could grow the dataset are being refused with an OOM error. " +
			"Raise maxmemory, delete keys, or choose a policy that evicts."
	case strings.HasPrefix(policy, "volatile-") && volatile == 0:
		return "No key carries a TTL, so this policy has nothing it is allowed to evict and " +
			"writes are being refused. Give keys a TTL, or switch to an allkeys-* policy."
	default:
		return "Eviction runs on the next write that would exceed the budget."
	}
}

// --- COMMAND GETKEYS ----------------------------------------------------------

// commandGetKeys reports the keys a command would act on, which is what COMMAND
// GETKEYS answers. errMsg is non-empty when the request cannot be answered, using
// Redis's messages.
//
// It is deliberately built on the same key extraction the rest of the server uses --
// affectedKeys for the write side, plus the read-side positions -- rather than a
// second table. One table would drift from the other, and the one that drifted
// silently is the one WATCH depends on.
func commandGetKeys(args [][]byte) (keys []string, errMsg string) {
	name := strings.ToUpper(string(args[0]))
	cmd, ok := commandTable[name]
	if !ok {
		return nil, "ERR Invalid command specified"
	}
	if !arityOK(cmd.arity, len(args)) {
		return nil, "ERR Invalid number of arguments specified for command"
	}
	keys = commandKeys(name, args)
	if len(keys) == 0 {
		return nil, "ERR The command has no key arguments"
	}
	return keys, ""
}

// commandKeys returns every key position a command names, reads or writes.
//
// affectedKeys answers the write half (and must, for WATCH), but a read command names
// keys too and several commands carry a key count as an operand. The cases here are
// exactly those: everything else falls through to affectedKeys, so a command added
// with a correct affectedKeys entry gets a correct COMMAND GETKEYS for free.
func commandKeys(name string, args [][]byte) []string {
	switch name {
	case "MGET", "EXISTS", "TOUCH", "WATCH", "SUNION", "SINTER", "SDIFF", "PFCOUNT":
		return byteStrings(args[1:])
	case "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE", "PFMERGE":
		return byteStrings(args[1:])
	case "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE":
		// The destination, then the numkeys-counted inputs. The count itself is an operand
		// and not a key, which is exactly why these cannot share the case above: reporting
		// "2" as a key would route the command by the slot of a number in cluster mode.
		return append([]string{string(args[1])}, numkeysKeys(args, 2)...)
	case "BITOP":
		// BITOP <op> <dest> <src...>: the operation sits where every other command puts
		// its first key, which is exactly why it needs a case of its own.
		if len(args) < 4 {
			return nil
		}
		return byteStrings(args[2:])
	case "SINTERCARD", "LMPOP", "ZMPOP", "ZDIFF", "ZUNION", "ZINTER", "ZINTERCARD":
		return numkeysKeys(args, 1)
	case "BLMPOP", "BZMPOP":
		return numkeysKeys(args, 2)
	case "GEOSEARCHSTORE":
		if len(args) < 3 {
			return nil
		}
		return []string{string(args[1]), string(args[2])}
	case "XREAD", "XREADGROUP":
		return xreadKeys(args)
	case "XGROUP", "XINFO":
		// Both put their key after a subcommand, so neither has a key at a fixed position.
		if len(args) >= 3 {
			return []string{string(args[2])}
		}
		return nil
	case "MEMORY":
		if len(args) >= 3 && strings.EqualFold(string(args[1]), "USAGE") {
			return []string{string(args[2])}
		}
		return nil
	case "OBJECT":
		if len(args) >= 3 {
			return []string{string(args[2])}
		}
		return nil
	}
	if keylessCommands[name] {
		return nil
	}
	return affectedKeys(name, args)
}

// numkeysKeys reads the "numkeys key [key...]" shape whose key count is an operand at
// args[idx].
func numkeysKeys(args [][]byte, idx int) []string {
	if idx >= len(args) {
		return nil
	}
	n, ok := parseInt(args[idx])
	if !ok || n <= 0 || idx+1+n > len(args) {
		return nil
	}
	return byteStrings(args[idx+1 : idx+1+n])
}

// --- shared reply helpers -----------------------------------------------------

// writeHelp writes a container command's HELP reply: a title, the subcommand lines,
// and the trailing HELP entry every one of them shares.
func writeHelp(w *resp.Writer, title string, lines []string) {
	w.WriteArrayHeader(len(lines) + 3)
	w.WriteSimple(title)
	for _, l := range lines {
		w.WriteSimple(l)
	}
	w.WriteSimple("HELP")
	w.WriteSimple("    Print this help.")
}
