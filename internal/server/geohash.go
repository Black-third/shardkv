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

	cells := make([]geoHashBits, 0, 9)
	for dx := -1; dx <= 1; dx++ {
		col := center.moveX(dx)
		for dy := -1; dy <= 1; dy++ {
			cells = append(cells, col.moveY(dy))
		}
	}
	out := make([]scoreRange, 0, 9)
	for _, c := range cells {
		lo := c.align52()
		hi := geoHashBits{bits: c.bits + 1, step: c.step}.align52()
		out = append(out, scoreRange{min: float64(lo), max: float64(hi)})
	}
	return out
}

// scoreRange is a half-open score interval [min, max).
type scoreRange struct{ min, max float64 }
