package server

// SORT and SORT_RO.
//
// SORT is the one command in Redis whose *key set is data-dependent*: BY and GET take
// glob patterns into which each element of the collection is substituted, so the keys it
// reads are not in its arguments at all and cannot be known until the collection has been
// read. Three consequences follow, and each is handled explicitly below.
//
//  1. It cannot be one atomic step. The collection is read under its shard's lock; every
//     weight and every GET is a separate read afterwards. A concurrent write to a weight
//     key can therefore be observed by an in-flight SORT. Redis has no such window because
//     it is single-threaded -- there is no version of this that keeps sharded concurrency
//     and closes it, short of locking the whole keyspace for the duration. The consequence
//     is bounded: the *reply* may mix two instants of the weight keys, and the collection
//     itself never does.
//
//  2. Only the collection and the STORE destination are keys as far as WATCH, COMMAND
//     GETKEYS and cluster routing are concerned (see sortKeys) -- which is Redis's own
//     answer, and it is why cluster mode refuses a BY or GET pattern that could reach
//     another slot rather than trying to route by it. See sortClusterCheck.
//
//  3. It still propagates verbatim. The output is fully determined by the data: ties are
//     broken by the element itself, and a set sorted with BY nosort into a destination is
//     forced to sort alphabetically (as Redis forces it) precisely so that the order does
//     not come from a hash table's iteration. A replica replaying the command reads the
//     same weights -- they arrived on the same stream, ahead of this command -- and
//     reaches the same list.

import (
	"sort"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
)

func init() {
	register("SORT", -2, true, cmdSort)
	// SORT_RO is the read-only form, which exists so a client can send it to a replica.
	// It rejects STORE rather than silently ignoring it.
	register("SORT_RO", -2, false, cmdSortRO)
}

// sortSpec is a parsed SORT invocation.
type sortSpec struct {
	byPattern  string
	hasBy      bool
	dontsort   bool // a BY pattern with no '*' names one constant key, so nothing to sort by
	getPattern []string
	alpha      bool
	desc       bool
	limitStart int
	limitCount int // negative: everything from the start on
	hasLimit   bool
	store      string
	hasStore   bool
}

func cmdSort(s *Server, w *resp.Writer, args [][]byte) bool {
	return runSort(s, w, args, false)
}

func cmdSortRO(s *Server, w *resp.Writer, args [][]byte) bool {
	runSort(s, w, args, true)
	return false
}

func runSort(s *Server, w *resp.Writer, args [][]byte, readOnly bool) bool {
	spec, errMsg := parseSort(args, readOnly)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	if s.ClusterEnabled() {
		if errMsg := sortClusterCheck(string(args[1]), spec); errMsg != "" {
			w.WriteError(errMsg)
			return false
		}
	}

	elems, kind, err := s.store.SortSource(string(args[1]))
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	// A set has no order of its own, so sorting it with BY nosort would answer in hash
	// table order -- which is not reproducible, and which a STORE would then persist. Redis
	// forces alphabetical sorting in exactly that case; doing the same is what makes
	// SORT ... STORE safe to propagate verbatim.
	if spec.dontsort && kind == "set" && spec.hasStore {
		spec.dontsort = false
		spec.alpha = true
		spec.hasBy = false
	}

	ordered, errMsg := s.sortElements(elems, kind, spec)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	ordered = applySortLimit(ordered, spec)

	if spec.hasStore {
		return s.sortStore(w, spec, ordered)
	}
	s.writeSortReply(w, spec, ordered)
	return false
}

// parseSort reads SORT's option list. readOnly refuses STORE, for SORT_RO.
func parseSort(args [][]byte, readOnly bool) (sortSpec, string) {
	spec := sortSpec{limitCount: -1}
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "ASC":
			spec.desc = false
		case "DESC":
			spec.desc = true
		case "ALPHA":
			spec.alpha = true
		case "BY":
			if i+1 >= len(args) {
				return spec, "ERR syntax error"
			}
			spec.byPattern, spec.hasBy = string(args[i+1]), true
			// A pattern with no '*' has nothing to substitute, so every element would weigh
			// the same and the sort would be a no-op. Redis treats that as "do not sort" --
			// which is what makes `BY nosort` the documented way to ask for no ordering.
			spec.dontsort = !strings.Contains(spec.byPattern, "*")
			i++
		case "GET":
			if i+1 >= len(args) {
				return spec, "ERR syntax error"
			}
			spec.getPattern = append(spec.getPattern, string(args[i+1]))
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return spec, "ERR syntax error"
			}
			start, ok1 := parseInt(args[i+1])
			count, ok2 := parseInt(args[i+2])
			if !ok1 || !ok2 {
				return spec, "ERR value is not an integer or out of range"
			}
			spec.limitStart, spec.limitCount, spec.hasLimit = start, count, true
			i += 2
		case "STORE":
			if readOnly {
				return spec, "ERR syntax error"
			}
			if i+1 >= len(args) {
				return spec, "ERR syntax error"
			}
			spec.store, spec.hasStore = string(args[i+1]), true
			i++
		default:
			return spec, "ERR syntax error"
		}
	}
	return spec, ""
}

// sortClusterCheck refuses a BY or GET pattern that might name a key in another slot.
//
// The rule is Redis's, and it is the only safe one available: the keys a pattern expands
// to depend on the data, so a node cannot know in advance whether they are its own. A
// pattern carrying a hash tag that hashes to the sorted key's slot *is* known to stay on
// this node, and is allowed; anything else -- including a pattern with no tag at all -- is
// refused with the error a cluster-aware client is written to expect. "#" is exempt because
// it expands to the element rather than to a key.
func sortClusterCheck(key string, spec sortSpec) string {
	slot := KeySlot(key)
	if spec.hasBy && !spec.dontsort && patternSlot(spec.byPattern) != slot {
		return "ERR BY option of SORT denied in Cluster mode when " +
			"keys formed by the pattern may be in different slots."
	}
	for _, p := range spec.getPattern {
		if p == "#" {
			continue
		}
		if patternSlot(p) != slot {
			return "ERR GET option of SORT denied in Cluster mode when " +
				"keys formed by the pattern may be in different slots."
		}
	}
	return ""
}

// patternSlot is the slot every key a pattern can expand to must land in, or -1 when the
// pattern gives no such guarantee. Only a hash tag provides one, because the tag is the
// only part of a key the slot is computed from -- so a pattern without one can expand to
// any slot at all.
func patternSlot(pattern string) int {
	start := strings.IndexByte(pattern, '{')
	if start < 0 {
		return -1
	}
	end := strings.IndexByte(pattern[start+1:], '}')
	if end < 0 || end == 0 {
		return -1
	}
	return KeySlot(pattern[start : start+end+2])
}

// sortElements orders the collection according to the spec, resolving the BY pattern's
// weights as it goes.
func (s *Server) sortElements(elems []string, kind string, spec sortSpec) ([]string, string) {
	if spec.dontsort {
		// The collection's own order: a list's insertion order, a sorted set's score order,
		// or -- for a set with no STORE -- whatever order it came out in, which is what Redis
		// answers too.
		//
		// DESC still applies to the two types that *have* an order, because reversing a known
		// order is meaningful; for a set there is no order to reverse, so DESC is ignored
		// rather than applied to a hash table's iteration. Redis draws the line in the same
		// place -- it walks a list or a skip list backwards and leaves a set's iterator alone.
		if spec.desc && (kind == "list" || kind == "zset") {
			for i, j := 0, len(elems)-1; i < j; i, j = i+1, j-1 {
				elems[i], elems[j] = elems[j], elems[i]
			}
		}
		return elems, ""
	}

	type item struct {
		elem   string
		score  float64
		byVal  string
		byNull bool
	}
	items := make([]item, 0, len(elems))
	for _, e := range elems {
		it := item{elem: e}
		val := e
		if spec.hasBy {
			v, ok := s.lookupByPattern(spec.byPattern, e)
			if !ok {
				// A missing weight leaves the element at score 0 (numeric) or sorting before
				// every element that has one (alphabetic), which is Redis's behaviour.
				it.byNull = true
				items = append(items, it)
				continue
			}
			val = v
		}
		if spec.alpha {
			it.byVal = val
		} else {
			f, ok := parseFloat([]byte(val))
			if !ok {
				return nil, "ERR One or more scores can't be converted into double"
			}
			it.score = f
		}
		items = append(items, it)
	}

	// A stable sort with the element itself as the final tie-break, so the answer is fully
	// determined by the data. Redis breaks numeric ties by element for the same reason ("we
	// don't want the comparison to be undefined"); doing it for the alphabetic case as well
	// costs nothing and is what lets SORT ... STORE propagate its own text.
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		switch {
		case a.byNull != b.byNull:
			return a.byNull // a missing weight sorts first
		case a.byNull:
			return a.elem < b.elem
		}
		if spec.alpha && spec.hasBy {
			if a.byVal != b.byVal {
				return a.byVal < b.byVal
			}
			return a.elem < b.elem
		}
		if spec.alpha {
			return a.elem < b.elem
		}
		if a.score != b.score {
			return a.score < b.score
		}
		return a.elem < b.elem
	})
	if spec.desc {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.elem)
	}
	return out, ""
}

// applySortLimit applies the LIMIT clause, with Redis's clamping: a negative start is 0, a
// negative count means "to the end", and a start past the collection selects nothing.
func applySortLimit(ordered []string, spec sortSpec) []string {
	if !spec.hasLimit {
		return ordered
	}
	start := spec.limitStart
	if start < 0 {
		start = 0
	}
	if start >= len(ordered) {
		return nil
	}
	ordered = ordered[start:]
	if spec.limitCount >= 0 && spec.limitCount < len(ordered) {
		ordered = ordered[:spec.limitCount]
	}
	return ordered
}

// writeSortReply writes the reply: the elements themselves, or one entry per GET pattern
// per element, interleaved in the order the patterns were given.
func (s *Server) writeSortReply(w *resp.Writer, spec sortSpec, ordered []string) {
	if len(spec.getPattern) == 0 {
		w.WriteArrayHeader(len(ordered))
		for _, e := range ordered {
			w.WriteBulkString(e)
		}
		return
	}
	w.WriteArrayHeader(len(ordered) * len(spec.getPattern))
	for _, e := range ordered {
		for _, p := range spec.getPattern {
			v, ok := s.lookupGetPattern(p, e)
			if !ok {
				w.WriteNull()
				continue
			}
			w.WriteBulkString(v)
		}
	}
}

// sortStore replaces the destination with the result as a list, replying with its length.
// An empty result deletes the destination, which is Redis's rule -- so 0 means "there is
// nothing there now" rather than "nothing happened".
func (s *Server) sortStore(w *resp.Writer, spec sortSpec, ordered []string) bool {
	var vals [][]byte
	if len(spec.getPattern) == 0 {
		vals = make([][]byte, 0, len(ordered))
		for _, e := range ordered {
			vals = append(vals, []byte(e))
		}
	} else {
		vals = make([][]byte, 0, len(ordered)*len(spec.getPattern))
		for _, e := range ordered {
			for _, p := range spec.getPattern {
				v, _ := s.lookupGetPattern(p, e)
				// A missing GET key stores an empty string rather than nothing, so the stored
				// list keeps one element per pattern per element and a reader can still index it.
				vals = append(vals, []byte(v))
			}
		}
	}
	n, err := s.store.ReplaceList(spec.store, vals)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(n))
	// Always dirty: the destination is replaced whatever the result, and an empty result
	// deletes it.
	return true
}

// lookupGetPattern resolves one GET pattern for one element. "#" is the element itself,
// which is how a caller asks for the sorted members alongside the values it fetched.
func (s *Server) lookupGetPattern(pattern, elem string) (string, bool) {
	if pattern == "#" {
		return elem, true
	}
	return s.lookupByPattern(pattern, elem)
}

// lookupByPattern substitutes elem for the first '*' in the pattern and reads the result:
// a string value, or a hash field when the pattern carries a "->field" suffix.
//
// A pattern with no '*' resolves to nothing at all rather than to a constant key, which is
// what makes "BY nosort" mean "do not sort" instead of "weigh everything by the key
// literally called nosort".
func (s *Server) lookupByPattern(pattern, elem string) (string, bool) {
	star := strings.IndexByte(pattern, '*')
	if star < 0 {
		return "", false
	}
	// The hash-field suffix is looked for after the '*', so a '-' or '>' inside the
	// substituted element cannot be mistaken for the separator.
	spec := pattern[:star] + elem + pattern[star+1:]
	if idx := strings.Index(pattern[star:], "->"); idx > 0 {
		cut := star + idx
		field := pattern[cut+2:]
		key := pattern[:star] + elem + pattern[star+1:cut]
		v, ok, err := s.store.HGet(key, field)
		if err != nil || !ok {
			return "", false
		}
		return string(v), true
	}
	v, ok, err := s.store.GetString(spec)
	if err != nil || !ok {
		return "", false
	}
	return string(v), true
}

// sortKeys is the key list SORT and SORT_RO report: the collection, plus the STORE
// destination when there is one.
//
// The BY and GET patterns are deliberately *not* here, and that is Redis's answer too. The
// keys they name depend on the data, so they cannot be listed before the command runs --
// which is exactly why cluster mode refuses a pattern that might leave the slot rather
// than trying to route by it (see sortClusterCheck). What this list must not miss is the
// destination, which is not at a fixed position: a WATCH on it has to see the overwrite,
// and in cluster mode the command has to be refused if it is in another slot.
func sortKeys(args [][]byte) []string {
	if len(args) < 2 {
		return nil
	}
	keys := []string{string(args[1])}
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "LIMIT":
			i += 2
		case "BY", "GET":
			i++
		case "STORE":
			if i+1 < len(args) {
				// The *last* STORE wins, so only the last destination is a key: an earlier one
				// is parsed and then overwritten, and it is never written to. Reporting all of
				// them would invalidate WATCHes on keys the command does not touch, and Redis's
				// own COMMAND GETKEYS reports just the last.
				keys = append(keys[:1], string(args[i+1]))
				i++
			}
		}
	}
	return keys
}
