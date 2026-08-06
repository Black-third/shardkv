package server

// The 52-bit geohash: the encoding that lets a sorted set answer geographic queries.
//
// # Why a sorted set can do this at all
//
// A geohash interleaves the bits of a longitude and a latitude, each expressed as an
// index into a 2^26 subdivision of its range. Interleaving is what makes the result
// useful as a *score*: two points that share a long prefix of interleaved bits are close
// together in both dimensions, so a contiguous range of scores is a rectangular cell on
// the Earth's surface. A radius query therefore becomes a handful of score ranges over
// the same skip list ZRANGEBYSCORE already walks, and GEOADD is literally a ZADD.
//
// 26 bits per coordinate gives 52 bits in total, which is exactly the largest integer a
// float64 score holds without loss -- which is why 26 and not 27. The resulting cell is
// about 0.6 metres on a side, so the encoding is not the limit on accuracy; the sphere
// model is.
//
// This is Redis's representation, bit for bit, so a sorted set of geohash scores written
// here reads correctly in Redis and vice versa: GEOADD's score is a number a client can
// see with ZSCORE, and clients do.
//
// # The search
//
// A search resolves its circle or box to a geohash cell size that comfortably contains
// it, takes that cell and its eight neighbours, and turns each into a score range. Every
// candidate that comes back is then checked against the real distance, because a cell is
// a rectangle and the query is (usually) a circle. That is Redis's algorithm, and the
// reason for it is the nine-cell trick: a query area smaller than a cell can still
// straddle a cell boundary, so the centre cell alone is not enough, and its neighbours
// are.

import (
	"math"
	"sort"
	"strings"
)

// The geohash geometry and the Earth model, all matching Redis's constants so that a
// distance computed here equals the one Redis computes for the same two points.
const (
	// geoStep is the number of bits per coordinate. 26+26 = 52, the largest integer a
	// float64 holds exactly.
	geoStep = 26
	// geoLatMin and friends are the coordinate ranges Redis uses. The latitude range is
	// deliberately not the full -90..90: the Mercator projection the encoding is built on
	// diverges at the poles, so Redis clips at ±85.05112878 and refuses anything outside.
	geoLatMin = -85.05112878
	geoLatMax = 85.05112878
	geoLonMin = -180.0
	geoLonMax = 180.0
	// earthRadiusMeters is Redis's Earth radius. The value matters: a different radius
	// would make every GEODIST disagree with Redis's by a constant factor.
	earthRadiusMeters = 6372797.560856
	// mercatorMax is the Mercator projection's half-extent in metres, used to pick a cell
	// size for a given radius.
	mercatorMax = 20037726.37
)

// geoUnits are the distance units the GEO commands accept, as metres per unit.
var geoUnits = map[string]float64{
	"m":  1,
	"km": 1000,
	"mi": 1609.34,
	"ft": 0.3048,
}

// geoUnit looks up a unit keyword, case-insensitively.
func geoUnit(s string) (float64, bool) {
	u, ok := geoUnits[strings.ToLower(s)]
	return u, ok
}

// --- bit interleaving ---------------------------------------------------------

// interleave64 spreads the 32 bits of x and y into the even and odd bits of a 64-bit
// word, x in the even positions.
//
// The shift-and-mask ladder is the standard bit-spreading trick: each step doubles the
// gap between bits by splitting the word in half and moving the halves apart. It is
// branch-free and constant-time, which matters because it runs once per point per
// command.
func interleave64(x, y uint32) uint64 {
	var b = [...]uint64{
		0x5555555555555555, 0x3333333333333333,
		0x0f0f0f0f0f0f0f0f, 0x00ff00ff00ff00ff,
		0x0000ffff0000ffff,
	}
	var shifts = [...]uint{1, 2, 4, 8, 16}

	xx, yy := uint64(x), uint64(y)
	for i := len(shifts) - 1; i >= 0; i-- {
		xx = (xx | (xx << shifts[i])) & b[i]
		yy = (yy | (yy << shifts[i])) & b[i]
	}
	return xx | (yy << 1)
}

// deinterleave64 is interleave64's inverse: it collects the even bits into x and the odd
// bits into y.
func deinterleave64(bits uint64) (x, y uint32) {
	var b = [...]uint64{
		0x5555555555555555, 0x3333333333333333,
		0x0f0f0f0f0f0f0f0f, 0x00ff00ff00ff00ff,
		0x0000ffff0000ffff, 0x00000000ffffffff,
	}
	var shifts = [...]uint{0, 1, 2, 4, 8, 16}

	xx := bits & b[0]
	yy := (bits >> 1) & b[0]
	for i := 1; i < len(shifts); i++ {
		xx = (xx | (xx >> shifts[i])) & b[i]
		yy = (yy | (yy >> shifts[i])) & b[i]
	}
	return uint32(xx), uint32(yy)
}

// --- encoding and decoding ----------------------------------------------------

// geoEncode turns a longitude/latitude pair into its 52-bit geohash. ok is false for a
// coordinate outside the representable range, which the caller reports rather than
// clamping: a clamped coordinate would be stored as a place the client did not name.
func geoEncode(lon, lat float64) (uint64, bool) {
	if lon < geoLonMin || lon > geoLonMax || lat < geoLatMin || lat > geoLatMax {
		return 0, false
	}
	// The fraction of the way through each range, quantized to 2^26 steps.
	latOffset := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOffset := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	latOffset *= 1 << geoStep
	lonOffset *= 1 << geoStep
	return interleave64(uint32(latOffset), uint32(lonOffset)), true
}

// geoDecode turns a 52-bit geohash back into the centre of the cell it names.
//
// The result is the cell's midpoint, not the original coordinate: the encoding is lossy
// by construction, so a GEOPOS never returns exactly what GEOADD was given. Redis has
// the same property and documents the error as under 0.6 metres.
func geoDecode(bits uint64) (lon, lat float64) {
	ilato, ilono := deinterleave64(bits)
	latMin := geoLatMin + (float64(ilato)/(1<<geoStep))*(geoLatMax-geoLatMin)
	latMax := geoLatMin + (float64(ilato+1)/(1<<geoStep))*(geoLatMax-geoLatMin)
	lonMin := geoLonMin + (float64(ilono)/(1<<geoStep))*(geoLonMax-geoLonMin)
	lonMax := geoLonMin + (float64(ilono+1)/(1<<geoStep))*(geoLonMax-geoLonMin)
	return (lonMin + lonMax) / 2, (latMin + latMax) / 2
}

// --- distance -----------------------------------------------------------------

// degRad converts degrees to radians. The multiplication is by the precomputed constant
// rather than "d * Pi / 180", because the two round differently in the last bit and Redis
// uses the constant -- a GEODIST that differed from Redis's in the sixteenth digit would
// be a difference a test comparing the two servers would keep tripping over.
func degRad(d float64) float64 { return d * (math.Pi / 180.0) }

// geoDistance is the great-circle distance between two points in metres, by the
// haversine formula on a sphere of earthRadiusMeters.
//
// A sphere rather than an ellipsoid, and this radius rather than WGS84's: both are
// Redis's choices, and matching them is what makes GEODIST agree between the two servers.
// The error against a proper geodesic is a few tenths of a percent, which is why Redis
// documents GEODIST as having up to 0.5% error.
func geoDistance(lon1, lat1, lon2, lat2 float64) float64 {
	lon1r, lon2r := degRad(lon1), degRad(lon2)
	v := math.Sin((lon2r - lon1r) / 2)
	if v == 0 {
		// Same meridian: the distance is a pure latitude difference, and computing it
		// directly avoids putting a zero through the haversine's square root. Redis takes
		// the same shortcut, so this is also what keeps the two in exact agreement for a
		// north-south pair.
		return earthRadiusMeters * math.Abs(degRad(lat2)-degRad(lat1))
	}
	lat1r, lat2r := degRad(lat1), degRad(lat2)
	u := math.Sin((lat2r - lat1r) / 2)
	a := u*u + math.Cos(lat1r)*math.Cos(lat2r)*v*v
	return 2.0 * earthRadiusMeters * math.Asin(math.Sqrt(a))
}

// --- the base-32 geohash string -----------------------------------------------

// geoAlphabet is the base-32 alphabet the standard geohash string uses. It is not
// Redis's own choice: it is the one geohash.org uses, so the string GEOHASH returns can
// be pasted into a map service.
const geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// geoHashString renders a point as the 11-character standard geohash string.
//
// It re-encodes rather than reusing the stored score, because the two use different
// latitude ranges: the score's latitude spans ±85.05112878 (the Mercator limit) while a
// standard geohash spans the full ±90. Emitting the stored bits in base 32 would produce
// a string that named a different place. Redis re-encodes here for the same reason.
func geoHashString(lon, lat float64) string {
	// A 55-bit interleave (11 characters x 5 bits) built from 27 bits per coordinate,
	// against the full latitude range.
	latOffset := (lat - (-90)) / 180
	lonOffset := (lon - (-180)) / 360
	latOffset *= 1 << 26
	lonOffset *= 1 << 26
	bits := interleave64(uint32(latOffset), uint32(lonOffset))

	var b strings.Builder
	b.Grow(11)
	for i := 0; i < 11; i++ {
		if i == 10 {
			// Eleven characters of five bits need 55, and there are only 52. Redis emits a
			// literal '0' for the last one rather than the two bits that remain, so the string
			// it produces is the one geohash.org shows -- and a client that compares the two
			// has to see the same eleventh character.
			b.WriteByte(geoAlphabet[0])
			continue
		}
		idx := (bits >> uint(52-((i+1)*5))) & 0x1f
		b.WriteByte(geoAlphabet[idx])
	}
	return b.String()
}

// --- the search area ----------------------------------------------------------

// geoHashBits is a geohash truncated to a given number of bits per coordinate, which is
// what naming a cell of a particular size amounts to.
type geoHashBits struct {
	bits uint64
	step uint8
}

// geoEstimateSteps picks how many bits per coordinate give a cell comfortably larger
// than a query of the given range.
//
// It halves the Mercator extent until it is below the range, then backs off two steps so
// the query area is well inside a cell rather than straddling one -- and backs off
// further near the poles, where a cell of a given bit depth covers far less ground
// east-to-west than it does at the equator. This is Redis's function, and the constants
// are its constants.
func geoEstimateSteps(rangeMeters, lat float64) uint8 {
	if rangeMeters == 0 {
		return geoStep
	}
	step := 1
	for rangeMeters < mercatorMax {
		rangeMeters *= 2
		step++
	}
	step -= 2
	if lat > 66 || lat < -66 {
		step--
		if lat > 80 || lat < -80 {
			step--
		}
	}
	if step < 1 {
		step = 1
	}
	if step > geoStep {
		step = geoStep
	}
	return uint8(step)
}

// geoEncodeStep encodes a point to the given number of bits per coordinate.
func geoEncodeStep(lon, lat float64, step uint8) geoHashBits {
	latOffset := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOffset := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	latOffset *= float64(uint64(1) << step)
	lonOffset *= float64(uint64(1) << step)
	return geoHashBits{bits: interleave64(uint32(latOffset), uint32(lonOffset)), step: step}
}

// moveX returns the cell d cells east (d > 0) or west (d < 0).
//
// The arithmetic is the point: adding one to the *interleaved* x bits, with the y bits
// masked out, increments the x coordinate without disturbing y. The "zz" mask is the y
// bit positions, and adding zz+1 carries through them exactly once.
func (h geoHashBits) moveX(d int) geoHashBits {
	if d == 0 {
		return h
	}
	x := h.bits & 0xaaaaaaaaaaaaaaaa
	y := h.bits & 0x5555555555555555
	zz := uint64(0x5555555555555555) >> (64 - h.step*2)
	if d > 0 {
		x += zz + 1
	} else {
		x |= zz
		x -= zz + 1
	}
	x &= 0xaaaaaaaaaaaaaaaa >> (64 - h.step*2)
	return geoHashBits{bits: x | y, step: h.step}
}

// moveY is moveX for the other coordinate, with the masks swapped.
func (h geoHashBits) moveY(d int) geoHashBits {
	if d == 0 {
		return h
	}
	x := h.bits & 0xaaaaaaaaaaaaaaaa
	y := h.bits & 0x5555555555555555
	zz := uint64(0xaaaaaaaaaaaaaaaa) >> (64 - h.step*2)
	if d > 0 {
		y += zz + 1
	} else {
		y |= zz
		y -= zz + 1
	}
	y &= 0x5555555555555555 >> (64 - h.step*2)
	return geoHashBits{bits: x | y, step: h.step}
}

// align52 widens a truncated geohash to the 52-bit space scores live in, which turns a
// cell into the score range [align52(cell), align52(cell+1)).
func (h geoHashBits) align52() uint64 {
	return h.bits << (52 - h.step*2)
}

// geoSearchRanges returns the score ranges covering a cell and its eight neighbours,
// which together are guaranteed to contain every point within radiusMeters of the
// centre.
//
// Nine cells rather than one because a query area smaller than a cell can still straddle
// a cell boundary -- in the worst case a corner, which is why all eight neighbours are
// needed and not just the two adjacent sides.
func geoSearchRanges(lon, lat, radiusMeters float64) []scoreRange {
	step := geoEstimateSteps(radiusMeters, lat)
	center := geoEncodeStep(lon, lat, step)

	// The nine cells are the centre and its neighbours, and they are only a valid cover if
	// they actually extend past the radius in every direction. Near a cell edge they do
	// not: the point sits close to one side of its own cell, so the ring on that side stops
	// short and a member inside the radius is never visited. Redis checks the four cardinal
	// neighbours for exactly this and widens the cells once when any of them falls short.
	//
	// This was measured, not inferred: without the check,
	// `GEOSEARCH ... FROMLONLAT 179.9 0 BYRADIUS 5000 km` omitted a member that redis:7.2
	// returns. The failure mode is the bad one -- a *missing* result from a search that
	// reports success, rather than an error.
	if step > 1 && !geoCoverReachesRadius(lon, lat, radiusMeters, center) {
		step--
		center = geoEncodeStep(lon, lat, step)
	}

	// A search area that reaches the top or bottom of the representable latitude range spans
	// *every* longitude, and no ring of three columns can cover that. Meridians converge, so
	// a circle whose northern edge is at 85 degrees is only a few hundred kilometres wide in
	// longitude terms at that latitude while being global in longitude *degrees*.
	//
	// The check above cannot catch this, because moveY wraps the row index rather than
	// clamping it: the "north" neighbour of a top-row cell is the bottom row, so the distance
	// to it is half a world and the cover looks sufficient. Measured consequence, on a
	// 172-member set: `FROMLONLAT -7.103421 53.468338 BYRADIUS 5000 km` omitted a member at
	// 83.5 degrees north and 118 degrees east that redis:7.2 returns -- 4514 km away, well
	// inside the radius, and both servers agree on that distance. The cover simply never
	// visited it.
	//
	// Step 1 is the only cover that spans all longitudes (two columns of 180 degrees, and the
	// ring of three wraps onto both), so a pole-reaching search scans the whole set and lets
	// the per-candidate distance test do the work. That is the cost of being correct here,
	// and it is bounded: such a circle genuinely does reach every meridian.
	latDeltaDeg := (radiusMeters / earthRadiusMeters) * (180.0 / math.Pi)
	if lat+latDeltaDeg >= geoLatMax || lat-latDeltaDeg <= geoLatMin {
		center = geoEncodeStep(lon, lat, 1)
	}

	out := make([]scoreRange, 0, 9)
	for dx := -1; dx <= 1; dx++ {
		col := center.moveX(dx)
		for dy := -1; dy <= 1; dy++ {
			c := col.moveY(dy)
			lo := c.align52()
			hi := geoHashBits{bits: c.bits + 1, step: c.step}.align52()
			out = append(out, scoreRange{min: float64(lo), max: float64(hi)})
		}
	}
	return mergeScoreRanges(out)
}

// geoCoverReachesRadius reports whether the nine cells around center extend at least
// radiusMeters from the search point in all four cardinal directions.
//
// Only the four edge-adjacent neighbours are tested, which is what Redis tests: a corner
// neighbour reaches further than either of the edges it touches, so an edge that reaches is
// enough to make the corner reach too.
func geoCoverReachesRadius(lon, lat, radiusMeters float64, center geoHashBits) bool {
	_, _, _, northMax := geoCellBounds(center.moveY(1))
	_, southMin, _, _ := geoCellBounds(center.moveY(-1))
	_, _, eastMax, _ := geoCellBounds(center.moveX(1))
	westMin, _, _, _ := geoCellBounds(center.moveX(-1))
	return geoDistance(lon, lat, lon, northMax) >= radiusMeters &&
		geoDistance(lon, lat, lon, southMin) >= radiusMeters &&
		geoDistance(lon, lat, eastMax, lat) >= radiusMeters &&
		geoDistance(lon, lat, westMin, lat) >= radiusMeters
}

// geoCellBounds returns the box a cell covers, in degrees, at the cell's own step.
func geoCellBounds(h geoHashBits) (lonMin, latMin, lonMax, latMax float64) {
	ilat, ilon := deinterleave64(h.bits)
	scale := float64(uint64(1) << h.step)
	latMin = geoLatMin + (float64(ilat)/scale)*(geoLatMax-geoLatMin)
	latMax = geoLatMin + (float64(ilat+1)/scale)*(geoLatMax-geoLatMin)
	lonMin = geoLonMin + (float64(ilon)/scale)*(geoLonMax-geoLonMin)
	lonMax = geoLonMin + (float64(ilon+1)/scale)*(geoLonMax-geoLonMin)
	return lonMin, latMin, lonMax, latMax
}

// mergeScoreRanges sorts the cover's ranges and merges any that touch or overlap, so no
// score can fall in two of them.
//
// Without this a member is *returned more than once*. The nine cells stop being distinct as
// soon as the step is small enough that the grid wraps around the world -- at step 1 there
// are only two columns and two rows, so the ring repeats cells. Measured on a 172-member
// set: `BYRADIUS 20000 km` answered with 331 entries, some members four times over. A
// client iterating the reply then processes the same member repeatedly, and `COUNT n`
// returns fewer than n distinct members while looking like it succeeded.
//
// Redis avoids the same thing by skipping a neighbour identical to the previously processed
// one. Merging is strictly stronger: it also collapses ranges that *overlap* without being
// equal, and it does not depend on duplicates being adjacent in the iteration order.
func mergeScoreRanges(in []scoreRange) []scoreRange {
	if len(in) < 2 {
		return in
	}
	sort.Slice(in, func(i, j int) bool { return in[i].min < in[j].min })
	out := in[:1]
	for _, r := range in[1:] {
		last := &out[len(out)-1]
		if r.min <= last.max {
			// Overlapping or touching: extend rather than add. Ranges are half-open, so
			// "touching" (r.min == last.max) is also a merge and not a gap.
			if r.max > last.max {
				last.max = r.max
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// scoreRange is a half-open score interval [min, max).
type scoreRange struct{ min, max float64 }
