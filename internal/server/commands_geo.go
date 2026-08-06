package server

// The GEO commands: GEOADD, GEOPOS, GEODIST, GEOSEARCH, GEOSEARCHSTORE and GEOHASH.
//
// There is no geo data type. A geo set *is* a sorted set whose scores are 52-bit
// geohashes, which is what Redis does and is the reason the whole family is a thin layer:
// GEOADD is a ZADD with a computed score, GEOPOS is a ZMSCORE plus a decode, and
// GEOSEARCH is a handful of ZRANGEBYSCOREs plus a distance filter. Every sorted-set
// command therefore works on a geo set -- ZCARD counts the members, ZREM removes one,
// ZSCORE shows the raw hash -- exactly as it does in Redis, and clients rely on that.
//
// See geohash.go for the encoding and the search geometry.
//
// Propagation: GEOADD and GEOSEARCHSTORE propagate verbatim. Both are pure functions of
// their arguments -- the encoding is deterministic and the distance filter is arithmetic
// -- so a replica applying the same command computes the same scores. There is no clock
// and no randomness anywhere in the family.

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/Black-third/shardkv/internal/resp"
	"github.com/Black-third/shardkv/internal/store"
)

func init() {
	register("GEOADD", -5, true, cmdGeoAdd)
	register("GEOPOS", -2, false, cmdGeoPos)
	register("GEODIST", -4, false, cmdGeoDist)
	register("GEOHASH", -2, false, cmdGeoHash)
	register("GEOSEARCH", -7, false, cmdGeoSearch)
	register("GEOSEARCHSTORE", -8, true, cmdGeoSearchStore)
}

const errInvalidLonLat = "ERR invalid longitude,latitude pair "

// errGeoNoMember reports that the member a FROMMEMBER search was centred on is not in
// the set, which is a different failure from a missing key or a wrong type.
var errGeoNoMember = errors.New("could not decode requested zset member")

// cmdGeoAdd implements GEOADD key [NX|XX] [CH] longitude latitude member ...
//
// The reply is the number of *new* members, or -- with CH -- the number changed, which
// is ZADD's contract because this is a ZADD.
func cmdGeoAdd(s *Server, w *resp.Writer, args [][]byte) bool {
	var o store.ZAddOptions
	i := 2
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			o.NX = true
		case "XX":
			o.XX = true
		case "CH":
			o.CH = true
		default:
			goto triples
		}
	}
triples:
	rest := args[i:]
	if len(rest) == 0 || len(rest)%3 != 0 {
		w.WriteError("ERR syntax error")
		return false
	}
	// GEOADD answers a plain "syntax error" here, where ZADD names the two flags. The
	// difference is real -- verified side by side against redis:7.2 -- and it is what
	// Redis's own geo test asserts on: GEOADD parses its own option prefix and rejects
	// the combination before it ever builds the ZADD, so it never reaches the message
	// ZADD would have produced.
	if o.NX && o.XX {
		w.WriteError("ERR syntax error")
		return false
	}

	members := make([]store.ZMember, 0, len(rest)/3)
	for j := 0; j < len(rest); j += 3 {
		lon, ok1 := parseFloat(rest[j])
		lat, ok2 := parseFloat(rest[j+1])
		if !ok1 || !ok2 {
			w.WriteError("ERR value is not a valid float")
			return false
		}
		bits, ok := geoEncode(lon, lat)
		if !ok {
			w.WriteError(errInvalidLonLat + formatFloat(lon) + "," + formatFloat(lat))
			return false
		}
		members = append(members, store.ZMember{
			Member: string(rest[j+2]),
			// The score is the geohash as an exact integer in a float64, which is the whole
			// reason 52 bits and not more.
			Score: float64(bits),
		})
	}
	added, changed, err := s.store.ZAddMulti(string(args[1]), o, members)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if o.CH {
		w.WriteInt(int64(changed))
	} else {
		w.WriteInt(int64(added))
	}
	return changed > 0
}

// formatGeoCoord renders a coordinate the way Redis does: seventeen digits after the
// decimal point, with trailing zeros stripped.
//
// Not the shortest round-tripping form, which is what Go's -1 precision gives: a
// coordinate is the centre of a geohash cell, and a client comparing GEOPOS output
// against real Redis byte for byte -- which is the only way to check a geo set moved
// between the two servers unchanged -- has to see the same digits. Seventeen is where
// Redis stops because that is where a float64's decimal expansion stops being
// meaningful.
func formatGeoCoord(f float64) string {
	s := strconv.FormatFloat(f, 'f', 17, 64)
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// geoPositions resolves members to coordinates, reporting which were absent. It is the
// shared lookup of GEOPOS, GEODIST, GEOHASH and a FROMMEMBER search.
func (s *Server) geoPositions(key string, members []string) (lons, lats []float64, present []bool, err error) {
	scores, present, err := s.store.ZMScore(key, members...)
	if err != nil {
		return nil, nil, nil, err
	}
	lons = make([]float64, len(members))
	lats = make([]float64, len(members))
	for i, ok := range present {
		if !ok {
			continue
		}
		lons[i], lats[i] = geoDecode(uint64(scores[i]))
	}
	return lons, lats, present, nil
}

// cmdGeoPos implements GEOPOS key [member ...]. A missing member is a null element, so a
// client can index the reply against its request.
func cmdGeoPos(s *Server, w *resp.Writer, args [][]byte) bool {
	members := byteStrings(args[2:])
	lons, lats, present, err := s.geoPositions(string(args[1]), members)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(members))
	for i := range members {
		if !present[i] {
			w.WriteNullArray()
			continue
		}
		w.WriteArrayHeader(2)
		// Doubles, not bulk strings: that is what a RESP3 client dispatches on, and what
		// real Redis sends. The *text* is still the 17-decimal geo form -- see
		// WriteDoubleText for why the spelling and the type tag are decided separately.
		w.WriteDoubleText(formatGeoCoord(lons[i]))
		w.WriteDoubleText(formatGeoCoord(lats[i]))
	}
	return false
}

// cmdGeoDist implements GEODIST key member1 member2 [m|km|ft|mi].
func cmdGeoDist(s *Server, w *resp.Writer, args [][]byte) bool {
	unit := 1.0
	if len(args) == 5 {
		u, ok := geoUnit(string(args[4]))
		if !ok {
			w.WriteError("ERR unsupported unit provided. please use M, KM, FT, MI")
			return false
		}
		unit = u
	} else if len(args) > 5 {
		w.WriteError("ERR syntax error")
		return false
	}
	members := []string{string(args[2]), string(args[3])}
	lons, lats, present, err := s.geoPositions(string(args[1]), members)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	if !present[0] || !present[1] {
		w.WriteNull() // either member missing: a null, not a zero distance
		return false
	}
	d := geoDistance(lons[0], lats[0], lons[1], lats[1]) / unit
	// Four decimal places, as Redis formats it -- a tenth of a millimetre in metres,
	// which is far below the model's own error and so is where Redis stops.
	w.WriteBulk([]byte(strconv.FormatFloat(d, 'f', 4, 64)))
	return false
}

// cmdGeoHash implements GEOHASH key [member ...], returning the standard 11-character
// base-32 geohash string for each member.
func cmdGeoHash(s *Server, w *resp.Writer, args [][]byte) bool {
	members := byteStrings(args[2:])
	lons, lats, present, err := s.geoPositions(string(args[1]), members)
	if err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteArrayHeader(len(members))
	for i := range members {
		if !present[i] {
			w.WriteNull()
			continue
		}
		w.WriteBulk([]byte(geoHashString(lons[i], lats[i])))
	}
	return false
}

// --- GEOSEARCH ----------------------------------------------------------------

// geoSearchOpts is the parsed option set of GEOSEARCH and GEOSEARCHSTORE.
type geoSearchOpts struct {
	fromMember string
	hasMember  bool
	lon, lat   float64
	hasLonLat  bool

	byRadius  bool
	radius    float64
	byBox     bool
	width     float64
	height    float64
	unit      float64
	sortAsc   bool
	sortDesc  bool
	count     int
	any       bool
	withCoord bool
	withDist  bool
	withHash  bool
	storeDist bool
}

// parseGeoSearch parses the option tail starting at args[from].
func parseGeoSearch(args [][]byte, from int, allowWith bool) (geoSearchOpts, string) {
	o := geoSearchOpts{unit: 1}
	name := strings.ToLower(string(args[0]))
	for i := from; i < len(args); i++ {
		word := strings.ToUpper(string(args[i]))
		switch {
		case word == "FROMMEMBER" && i+1 < len(args):
			o.fromMember, o.hasMember = string(args[i+1]), true
			i++
		case word == "FROMLONLAT" && i+2 < len(args):
			lon, ok1 := parseFloat(args[i+1])
			lat, ok2 := parseFloat(args[i+2])
			if !ok1 || !ok2 {
				return o, "ERR value is not a valid float"
			}
			o.lon, o.lat, o.hasLonLat = lon, lat, true
			i += 2
		case word == "BYRADIUS" && i+2 < len(args):
			r, ok := parseFloat(args[i+1])
			if !ok || r < 0 {
				return o, "ERR value is not a valid float"
			}
			u, ok := geoUnit(string(args[i+2]))
			if !ok {
				return o, "ERR unsupported unit provided. please use M, KM, FT, MI"
			}
			o.byRadius, o.radius = true, r
			o.unit = u
			i += 2
		case word == "BYBOX" && i+3 < len(args):
			wd, ok1 := parseFloat(args[i+1])
			ht, ok2 := parseFloat(args[i+2])
			if !ok1 || !ok2 || wd < 0 || ht < 0 {
				return o, "ERR value is not a valid float"
			}
			u, ok := geoUnit(string(args[i+3]))
			if !ok {
				return o, "ERR unsupported unit provided. please use M, KM, FT, MI"
			}
			o.byBox, o.width, o.height = true, wd, ht
			o.unit = u
			i += 3
		case word == "ASC":
			o.sortAsc = true
		case word == "DESC":
			o.sortDesc = true
		case word == "COUNT" && i+1 < len(args):
			n, ok := parseInt(args[i+1])
			if !ok || n <= 0 {
				return o, "ERR COUNT must be > 0"
			}
			o.count = n
			i++
			// ANY, when present, follows the count. It says "stop as soon as you have that
			// many" rather than "find the nearest that many", which is a real difference: with
			// ANY the results are not the closest ones.
			if i+1 < len(args) && strings.EqualFold(string(args[i+1]), "ANY") {
				o.any = true
				i++
			}
		case word == "WITHCOORD" && allowWith:
			o.withCoord = true
		case word == "WITHDIST" && allowWith:
			o.withDist = true
		case word == "WITHHASH" && allowWith:
			o.withHash = true
		case word == "STOREDIST" && !allowWith:
			o.storeDist = true
		default:
			return o, "ERR syntax error"
		}
	}
	switch {
	case o.hasMember && o.hasLonLat, o.byRadius && o.byBox:
		// Two centres, or two shapes: a plain syntax error, which is what Redis answers here
		// and what its own test asserts. Naming the clauses would read better, but the text is
		// what a client matches on.
		//
		// *Neither* is reported differently, below, and the asymmetry is Redis's: giving both
		// is a malformed command, while giving neither is a command missing a mandatory
		// clause, and the second message says which clause.
		return o, "ERR syntax error"
	case !o.hasMember && !o.hasLonLat:
		// The command's own name, lower-cased, as Redis spells it here -- so GEOSEARCHSTORE
		// says "geosearchstore" rather than borrowing GEOSEARCH's name.
		return o, "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for " + name
	case !o.byRadius && !o.byBox:
		return o, "ERR exactly one of BYRADIUS and BYBOX can be specified for " + name
	case o.sortAsc && o.sortDesc:
		return o, "ERR syntax error"
	}
	return o, ""
}

// geoResult is one member a search found.
type geoResult struct {
	member string
	score  float64
	dist   float64 // in the requested unit
}

// geoSearch runs the search and returns the results in the requested order.
//
// The shape is: resolve the centre, resolve the query area to nine geohash cells, ask the
// sorted set for the members in those cells' score ranges (one lock, so one consistent
// cut), then filter each candidate by its real distance. The filter is what makes the
// answer exact: a cell is a rectangle, and BYRADIUS is a circle.
func (s *Server) geoSearch(key string, o geoSearchOpts) ([]geoResult, error) {
	lon, lat := o.lon, o.lat
	if o.hasMember {
		// A *missing key* is an empty search rather than a missing centre: there is nothing
		// there to search and nothing to report, which is the answer Redis gives. It is only a
		// key that exists and does not hold this member that is an error the caller can act on.
		if !s.store.Exists(key) {
			return nil, nil
		}
		lons, lats, present, err := s.geoPositions(key, []string{o.fromMember})
		if err != nil {
			return nil, err
		}
		if !present[0] {
			return nil, errGeoNoMember
		}
		lon, lat = lons[0], lats[0]
	}

	// The radius the cell estimate has to cover. For a box it is the half-diagonal, so the
	// cells are guaranteed to contain the whole rectangle.
	var coverMeters float64
	if o.byRadius {
		coverMeters = o.radius * o.unit
	} else {
		wHalf, hHalf := o.width*o.unit/2, o.height*o.unit/2
		coverMeters = math.Hypot(wHalf, hHalf)
	}

	ranges := geoSearchRanges(lon, lat, coverMeters)
	sranges := make([]store.ScoreRange, 0, len(ranges))
	for _, r := range ranges {
		// Half-open: the upper bound is the first score of the next cell.
		sranges = append(sranges, store.ScoreRange{Min: r.min, Max: r.max, MaxExcl: true})
	}
	candidates, err := s.store.ZRangeByScoreMulti(key, sranges)
	if err != nil {
		return nil, err
	}

	out := make([]geoResult, 0, len(candidates))
	for _, cand := range candidates {
		clon, clat := geoDecode(uint64(cand.Score))
		if o.byRadius {
			d := geoDistance(lon, lat, clon, clat)
			if d > o.radius*o.unit {
				continue
			}
			out = append(out, geoResult{member: cand.Member, score: cand.Score, dist: d / o.unit})
			continue
		}
		// BYBOX measures the two axes separately, and *where* the east-west axis is measured
		// is the part worth stating: along the **candidate's** parallel, not the query's.
		//
		// That is not the obvious reading -- it means the box flares outwards with latitude
		// rather than being a box on the map -- so it was established by measurement rather
		// than argument. For a candidate at a given longitude offset, the box width at which
		// redis:7.2 flips from excluding it to including it was binary-searched over eight
		// query/candidate latitude pairs. Every one matched the distance along the
		// *candidate's* parallel and none matched the query's; where the two agree (candidate
		// at the query's own latitude) both predict the same number, which is why a sweep
		// over random points is nearly blind to the difference and a designed probe is not.
		//
		// Measured thresholds, half-width in km, query latitude / candidate latitude /
		// longitude offset:
		//
		//	10 / 70 / 20   ->  757.4   (query's parallel would say 2190.4)
		//	 0 / 60 / 90   -> 4605.8   (query's parallel would say 10010.4)
		//	45 / 80 / 60   -> 1108.0   (query's parallel would say 4605.8)
		//	 0 /  0 / 90   -> 10010.4  (the two agree here)
		//
		// The north-south extent has no such subtlety: a degree of latitude is the same
		// length everywhere on a sphere.
		latDist := geoDistance(lon, lat, lon, clat)
		lonDist := geoDistance(clon, clat, lon, clat)
		if latDist > o.height*o.unit/2 || lonDist > o.width*o.unit/2 {
			continue
		}
		d := geoDistance(lon, lat, clon, clat)
		out = append(out, geoResult{member: cand.Member, score: cand.Score, dist: d / o.unit})
		if o.any && o.count > 0 && len(out) >= o.count {
			break
		}
	}
	if o.any && o.count > 0 && len(out) > o.count {
		out = out[:o.count]
	}

	// Sorted by distance unless the caller asked for neither order, in which case a COUNT
	// still has to take the *nearest* ones -- which means sorting anyway. Redis does the
	// same: COUNT without ANY is documented as returning the closest matches.
	switch {
	case o.sortDesc:
		sort.SliceStable(out, func(i, j int) bool { return out[i].dist > out[j].dist })
	case o.sortAsc:
		sort.SliceStable(out, func(i, j int) bool { return out[i].dist < out[j].dist })
	case o.count > 0 && !o.any:
		// A COUNT with no explicit order still has to take the *nearest* matches, which means
		// sorting. ANY is the exception and the reason this is not just "count > 0": ANY asks
		// for any N matches rather than the closest N, so sorting them would quietly turn it
		// back into the exhaustive search it exists to avoid -- and Redis's own test checks
		// that GEORADIUS ... ANY comes back unsorted.
		sort.SliceStable(out, func(i, j int) bool { return out[i].dist < out[j].dist })
	}
	if o.count > 0 && len(out) > o.count {
		out = out[:o.count]
	}
	return out, nil
}

// writeGeoSearchErr reports why a search could not run. The missing-centre-member case
// has its own message because it is the one failure a caller can act on: the member has
// to be added before the search can be repeated.
func writeGeoSearchErr(w *resp.Writer, err error) {
	if errors.Is(err, errGeoNoMember) {
		w.WriteError("ERR could not decode requested zset member")
		return
	}
	writeStoreErr(w, err)
}

// cmdGeoSearch implements GEOSEARCH key <FROMMEMBER m | FROMLONLAT lon lat>
// <BYRADIUS r unit | BYBOX w h unit> [ASC|DESC] [COUNT n [ANY]]
// [WITHCOORD] [WITHDIST] [WITHHASH].
func cmdGeoSearch(s *Server, w *resp.Writer, args [][]byte) bool {
	o, errMsg := parseGeoSearch(args, 2, true)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	results, err := s.geoSearch(string(args[1]), o)
	if err != nil {
		writeGeoSearchErr(w, err)
		return false
	}
	writeGeoResults(w, results, o)
	return false
}

// writeGeoResults writes the reply. With no WITH* option it is a flat array of member
// names; with any of them each element becomes an array of the member and the requested
// extras, in Redis's fixed order (distance, hash, coordinates).
func writeGeoResults(w *resp.Writer, results []geoResult, o geoSearchOpts) {
	withAny := o.withCoord || o.withDist || o.withHash
	w.WriteArrayHeader(len(results))
	for _, r := range results {
		if !withAny {
			w.WriteBulk([]byte(r.member))
			continue
		}
		n := 1
		if o.withDist {
			n++
		}
		if o.withHash {
			n++
		}
		if o.withCoord {
			n++
		}
		w.WriteArrayHeader(n)
		w.WriteBulk([]byte(r.member))
		if o.withDist {
			w.WriteBulk([]byte(strconv.FormatFloat(r.dist, 'f', 4, 64)))
		}
		if o.withHash {
			// The raw 52-bit score as an integer, which is what a client uses to do its own
			// cell arithmetic.
			w.WriteInt(int64(r.score))
		}
		if o.withCoord {
			lon, lat := geoDecode(uint64(r.score))
			w.WriteArrayHeader(2)
			w.WriteDoubleText(formatGeoCoord(lon))
			w.WriteDoubleText(formatGeoCoord(lat))
		}
	}
}

// cmdGeoSearchStore implements GEOSEARCHSTORE dest src <the GEOSEARCH options>
// [STOREDIST].
//
// By default the stored scores are the geohashes, so the destination is itself a geo set
// that can be searched again. STOREDIST stores the distances instead, which makes it an
// ordinary sorted set ordered by proximity -- useful, but no longer a geo set, which is
// why it is not the default.
func cmdGeoSearchStore(s *Server, w *resp.Writer, args [][]byte) bool {
	o, errMsg := parseGeoSearch(args, 3, false)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	results, err := s.geoSearch(string(args[2]), o)
	if err != nil {
		writeGeoSearchErr(w, err)
		return false
	}
	dst := string(args[1])
	// An empty result deletes the destination, as every *STORE command here does: a
	// destination left holding a previous result would answer a query nobody asked.
	if len(results) == 0 {
		deleted := s.store.Del(dst)
		w.WriteInt(0)
		return deleted
	}
	members := make([]store.ZMember, 0, len(results))
	for _, r := range results {
		score := r.score
		if o.storeDist {
			score = r.dist
		}
		members = append(members, store.ZMember{Member: r.member, Score: score})
	}
	// Replace rather than merge: the destination is the result of this query.
	s.store.Del(dst)
	if _, _, err := s.store.ZAddMulti(dst, store.ZAddOptions{}, members); err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(len(members)))
	return true
}

// --- GEORADIUS and GEORADIUSBYMEMBER ------------------------------------------

// The deprecated radius searches. GEOSEARCH replaced them in 6.2 and expresses everything
// they do, but a decade of client code and examples sends these, and Redis still ships
// them -- so a server that answers "unknown command" is not wire-compatible with the
// clients people actually run.
//
// They are the same search as GEOSEARCH over a different argument layout, so they share
// geoSearchOpts and s.geoSearch outright. Two differences are worth naming:
//
//   - The centre is positional (lon/lat or a member) rather than introduced by FROMLONLAT
//     or FROMMEMBER, and the radius is positional too.
//   - STORE and STOREDIST each take a *key operand* here, where GEOSEARCHSTORE's STOREDIST
//     is a bare flag. That is why they are parsed here rather than by parseGeoSearch.
//
// The _RO forms exist so a client can send a radius search to a replica: they are the same
// command with STORE and STOREDIST refused, which is why they are registered as reads.
func init() {
	register("GEORADIUS", -6, true, cmdGeoRadius)
	register("GEORADIUSBYMEMBER", -5, true, cmdGeoRadiusByMember)
	register("GEORADIUS_RO", -6, false, cmdGeoRadiusRO)
	register("GEORADIUSBYMEMBER_RO", -5, false, cmdGeoRadiusByMemberRO)
}

func cmdGeoRadius(s *Server, w *resp.Writer, args [][]byte) bool {
	return geoRadius(s, w, args, false, false)
}

func cmdGeoRadiusByMember(s *Server, w *resp.Writer, args [][]byte) bool {
	return geoRadius(s, w, args, true, false)
}

func cmdGeoRadiusRO(s *Server, w *resp.Writer, args [][]byte) bool {
	geoRadius(s, w, args, false, true)
	return false
}

func cmdGeoRadiusByMemberRO(s *Server, w *resp.Writer, args [][]byte) bool {
	geoRadius(s, w, args, true, true)
	return false
}

// geoRadius is the shared body. byMember selects the centre's spelling; readOnly refuses
// the two storing clauses.
func geoRadius(s *Server, w *resp.Writer, args [][]byte, byMember, readOnly bool) bool {
	o := geoSearchOpts{unit: 1, byRadius: true}
	// The centre and the radius are positional: key member radius unit, or
	// key longitude latitude radius unit.
	next := 3
	if byMember {
		o.fromMember, o.hasMember = string(args[2]), true
	} else {
		next = 4
		if len(args) < 6 {
			w.WriteError("ERR wrong number of arguments for '" + strings.ToLower(string(args[0])) + "' command")
			return false
		}
		lon, ok1 := parseFloat(args[2])
		lat, ok2 := parseFloat(args[3])
		if !ok1 || !ok2 {
			w.WriteError("ERR value is not a valid float")
			return false
		}
		o.lon, o.lat, o.hasLonLat = lon, lat, true
	}
	radius, ok := parseFloat(args[next])
	if !ok || radius < 0 {
		w.WriteError("ERR value is not a valid float")
		return false
	}
	unit, ok := geoUnit(string(args[next+1]))
	if !ok {
		w.WriteError("ERR unsupported unit provided. please use M, KM, FT, MI")
		return false
	}
	o.radius, o.unit = radius, unit

	storeKey, storeDistKey, errMsg := parseGeoRadiusTail(args, next+2, &o, readOnly)
	if errMsg != "" {
		w.WriteError(errMsg)
		return false
	}
	// STORE and the WITH* options ask for two different replies -- a stored sorted set and
	// an annotated array -- so Redis refuses the combination rather than choosing one. The
	// message names all three WITH options because any of them conflicts.
	if (storeKey != "" || storeDistKey != "") && (o.withCoord || o.withDist || o.withHash) {
		w.WriteError("ERR STORE option in GEORADIUS is not compatible with " +
			"WITHDIST, WITHHASH and WITHCOORD options")
		return false
	}

	results, err := s.geoSearch(string(args[1]), o)
	if err != nil {
		writeGeoSearchErr(w, err)
		return false
	}
	if storeKey == "" && storeDistKey == "" {
		writeGeoResults(w, results, o)
		return false
	}
	// STOREDIST wins when both are given, which is what Redis does: the later clause
	// replaces the earlier one, and STOREDIST is the more specific request.
	dst, byDist := storeKey, false
	if storeDistKey != "" {
		dst, byDist = storeDistKey, true
	}
	return s.geoStoreResults(w, dst, results, byDist)
}

// parseGeoRadiusTail parses the option tail these four commands share, which is
// GEOSEARCH's minus the FROM*/BY* clauses and plus the two storing ones.
func parseGeoRadiusTail(args [][]byte, from int, o *geoSearchOpts, readOnly bool) (storeKey, storeDistKey, errMsg string) {
	sawCount := false
	for i := from; i < len(args); i++ {
		switch word := strings.ToUpper(string(args[i])); {
		case word == "WITHCOORD":
			o.withCoord = true
		case word == "WITHDIST":
			o.withDist = true
		case word == "WITHHASH":
			o.withHash = true
		case word == "ASC":
			o.sortAsc = true
		case word == "DESC":
			o.sortDesc = true
		case word == "ANY":
			// ANY on its own is meaningless: it says "stop as soon as you have enough", and
			// without a COUNT there is no "enough". Redis names the missing operand rather
			// than answering a generic syntax error, because that is the fix.
			if !sawCount {
				return "", "", "ERR the ANY argument requires COUNT argument"
			}
			o.any = true
		case word == "COUNT" && i+1 < len(args):
			n, ok := parseInt(args[i+1])
			if !ok || n <= 0 {
				return "", "", "ERR COUNT must be > 0"
			}
			o.count, sawCount = n, true
			i++
		case word == "STORE" && i+1 < len(args) && !readOnly:
			storeKey = string(args[i+1])
			i++
		case word == "STOREDIST" && i+1 < len(args) && !readOnly:
			storeDistKey = string(args[i+1])
			i++
		default:
			return "", "", "ERR syntax error"
		}
	}
	if o.sortAsc && o.sortDesc {
		return "", "", "ERR syntax error"
	}
	return storeKey, storeDistKey, ""
}

// geoStoreResults replaces dst with the search's results: the geohashes as scores, or the
// distances when byDist. It is shared with GEOSEARCHSTORE, so the two spellings of "store
// this search" cannot store different things.
func (s *Server) geoStoreResults(w *resp.Writer, dst string, results []geoResult, byDist bool) bool {
	if len(results) == 0 {
		// An empty result deletes the destination: one left holding a previous result would
		// answer a query nobody asked.
		deleted := s.store.Del(dst)
		w.WriteInt(0)
		return deleted
	}
	members := make([]store.ZMember, 0, len(results))
	for _, r := range results {
		score := r.score
		if byDist {
			score = r.dist
		}
		members = append(members, store.ZMember{Member: r.member, Score: score})
	}
	s.store.Del(dst) // replace rather than merge: the destination is this query's result
	if _, _, err := s.store.ZAddMulti(dst, store.ZAddOptions{}, members); err != nil {
		writeStoreErr(w, err)
		return false
	}
	w.WriteInt(int64(len(members)))
	return true
}
