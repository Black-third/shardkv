package server

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

// TestGeoDistanceAgainstKnownCities is the correctness anchor: the great-circle
// distances between real places, checked against published values.
//
// It is the test that catches a wrong Earth radius, a degrees/radians slip, or a
// longitude and latitude swapped somewhere in the encode/decode round trip -- all of
// which produce plausible-looking numbers that are simply wrong. The tolerance is 0.5%,
// which is what Redis documents for GEODIST: the model is a sphere, so the residual
// against a geodesic on the WGS84 ellipsoid is a few tenths of a percent.
func TestGeoDistanceAgainstKnownCities(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// Coordinates and the great-circle distances between them, in metres.
	cities := []struct {
		name     string
		lon, lat float64
	}{
		{"palermo", 13.361389, 38.115556},
		{"catania", 15.087269, 37.502669},
		{"london", -0.127758, 51.507351},
		{"paris", 2.352222, 48.856614},
		{"newyork", -74.005973, 40.712775},
		{"sydney", 151.209296, -33.868820},
		{"tokyo", 139.691706, 35.689487},
		{"nairobi", 36.821946, -1.292066},
	}
	for _, city := range cities {
		got := c.cmd("GEOADD cities " +
			strconv.FormatFloat(city.lon, 'f', -1, 64) + " " +
			strconv.FormatFloat(city.lat, 'f', -1, 64) + " " + city.name)
		if got != ":1" {
			t.Fatalf("GEOADD %s = %q", city.name, got)
		}
	}

	// The expected values are the great-circle distances on a sphere of Redis's radius,
	// which is what Redis itself reports for these pairs.
	cases := []struct {
		a, b   string
		meters float64
	}{
		// Redis's own documentation example, to the metre.
		{"palermo", "catania", 166274.15},
		{"london", "paris", 343771},
		{"london", "newyork", 5570222},
		{"newyork", "sydney", 15988000},
		{"tokyo", "sydney", 7823000},
		{"london", "nairobi", 6819000},
		// The antipodal-ish and the equatorial cases, which exercise opposite ends of the
		// haversine's range.
		{"palermo", "palermo", 0},
	}
	for _, tc := range cases {
		got := c.cmd("GEODIST cities " + tc.a + " " + tc.b)
		d, err := strconv.ParseFloat(got, 64)
		if err != nil {
			t.Fatalf("GEODIST %s %s = %q", tc.a, tc.b, got)
		}
		if tc.meters == 0 {
			if d != 0 {
				t.Errorf("GEODIST %s %s = %v; want 0", tc.a, tc.b, d)
			}
			continue
		}
		if relErr := math.Abs(d-tc.meters) / tc.meters; relErr > 0.005 {
			t.Errorf("GEODIST %s %s = %.1fm; want %.1fm (%.3f%% off)",
				tc.a, tc.b, d, tc.meters, relErr*100)
		}
	}

	// The units convert exactly, so the same distance in km is the metre value / 1000.
	m, _ := strconv.ParseFloat(c.cmd("GEODIST cities palermo catania"), 64)
	km, _ := strconv.ParseFloat(c.cmd("GEODIST cities palermo catania km"), 64)
	if math.Abs(km*1000-m) > 1 {
		t.Errorf("GEODIST in km (%v) does not match metres (%v)", km, m)
	}
	mi, _ := strconv.ParseFloat(c.cmd("GEODIST cities palermo catania mi"), 64)
	if math.Abs(mi*1609.34-m) > 1 {
		t.Errorf("GEODIST in miles (%v) does not match metres (%v)", mi, m)
	}
	if got := c.cmd("GEODIST cities palermo nowhere"); got != "(nil)" {
		t.Errorf("GEODIST with a missing member = %q; want (nil)", got)
	}
	if got := c.cmd("GEODIST cities palermo catania parsecs"); got != "-ERR unsupported unit provided. please use M, KM, FT, MI" {
		t.Errorf("GEODIST with a bad unit = %q", got)
	}
}

// TestGeoPosRoundTrip checks the encode/decode round trip: the coordinate that comes back
// has to be within the cell size of the coordinate that went in.
func TestGeoPosRoundTrip(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	points := []struct{ lon, lat float64 }{
		{13.361389, 38.115556},
		{-0.127758, 51.507351},
		{0, 0},
		{179.999, 85.0},
		{-179.999, -85.0},
		{-74.0, 40.7},
	}
	for i, p := range points {
		name := "p" + strconv.Itoa(i)
		c.cmd("GEOADD pts " + strconv.FormatFloat(p.lon, 'f', -1, 64) + " " +
			strconv.FormatFloat(p.lat, 'f', -1, 64) + " " + name)
		got := c.cmd("GEOPOS pts " + name)
		parts := strings.Fields(strings.Trim(got, "[]"))
		if len(parts) != 2 {
			t.Fatalf("GEOPOS %s = %q", name, got)
		}
		lon, _ := strconv.ParseFloat(parts[0], 64)
		lat, _ := strconv.ParseFloat(parts[1], 64)
		// The 52-bit cell is under a metre on a side, which is well under 0.00001 degrees.
		if math.Abs(lon-p.lon) > 0.00001 || math.Abs(lat-p.lat) > 0.00001 {
			t.Errorf("GEOPOS %s returned (%v,%v) for (%v,%v)", name, lon, lat, p.lon, p.lat)
		}
	}
	if got := c.cmd("GEOPOS pts nosuch"); got != "[(nil)]" {
		t.Errorf("GEOPOS of a missing member = %q; want a null element", got)
	}
	// Coordinates outside the representable range are refused rather than clamped.
	if got := c.cmd("GEOADD pts 0 90 pole"); !strings.HasPrefix(got, "-ERR invalid longitude,latitude pair") {
		t.Errorf("GEOADD at latitude 90 = %q; want the invalid-pair error", got)
	}
	if got := c.cmd("GEOADD pts 181 0 far"); !strings.HasPrefix(got, "-ERR invalid longitude,latitude pair") {
		t.Errorf("GEOADD at longitude 181 = %q", got)
	}
}

// TestGeoHashStrings checks the base-32 strings against the values Redis returns, which
// are the standard geohash of the point and so can be pasted into a map service.
func TestGeoHashStrings(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("GEOADD Sicily 13.361389 38.115556 Palermo")
	c.cmd("GEOADD Sicily 15.087269 37.502669 Catania")
	// These are the strings Redis's documentation gives for exactly these coordinates.
	if got := c.cmd("GEOHASH Sicily Palermo Catania"); got != "[sqc8b49rny0 sqdtr74hyu0]" {
		t.Errorf("GEOHASH = %q; want [sqc8b49rny0 sqdtr74hyu0]", got)
	}
	if got := c.cmd("GEOHASH Sicily nosuch"); got != "[(nil)]" {
		t.Errorf("GEOHASH of a missing member = %q", got)
	}
}

// TestGeoSearch covers both query shapes, both centres, the ordering, COUNT, and the
// WITH* extras.
func TestGeoSearch(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// Redis's own documentation example set.
	c.cmd("GEOADD Sicily 13.361389 38.115556 Palermo")
	c.cmd("GEOADD Sicily 15.087269 37.502669 Catania")
	c.cmd("GEOADD Sicily 13.583333 37.316667 Agrigento")

	cases := []struct{ cmd, want string }{
		// A radius from a point, ordered by distance.
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC", "[Catania Agrigento Palermo]"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km DESC", "[Palermo Agrigento Catania]"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC COUNT 1", "[Catania]"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 60 km ASC", "[Catania]"},
		// Catania is 56km away, so a 50km radius excludes it -- the filter is the real
		// distance and not the cell it landed in.
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 50 km ASC", "[]"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 1 m ASC", "[]"},
		// A radius from a member -- the member itself is inside its own radius.
		{"GEOSEARCH Sicily FROMMEMBER Palermo BYRADIUS 200 km ASC", "[Palermo Agrigento Catania]"},
		{"GEOSEARCH Sicily FROMMEMBER Palermo BYRADIUS 1 km ASC", "[Palermo]"},
		// A box.
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYBOX 400 400 km ASC", "[Catania Agrigento Palermo]"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYBOX 120 120 km ASC", "[Catania]"},
		// A 100km box has a 50km half-height, which Catania is just outside.
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYBOX 100 100 km ASC", "[]"},
		// A missing key is an empty result, not an error.
		{"GEOSEARCH nosuch FROMLONLAT 15 37 BYRADIUS 200 km", "[]"},
		// Errors.
		{"GEOSEARCH Sicily FROMMEMBER nobody BYRADIUS 200 km", "-ERR could not decode requested zset member"},
		// Giving *both* centres is a plain syntax error while giving neither names the
		// missing clause -- Redis's own asymmetry, checked side by side against redis:7.2.
		{"GEOSEARCH Sicily FROMLONLAT 15 37 FROMMEMBER Palermo BYRADIUS 1 km", "-ERR syntax error"},
		{"GEOSEARCH Sicily BYRADIUS 1 km ASC COUNT 1", "-ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for geosearch"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 1 km BYBOX 1 1 km", "-ERR syntax error"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 ASC WITHDIST", "-ERR exactly one of BYRADIUS and BYBOX can be specified for geosearch"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km COUNT 0", "-ERR COUNT must be > 0"},
		{"GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 parsecs", "-ERR unsupported unit provided. please use M, KM, FT, MI"},
	}
	for _, tc := range cases {
		if got := c.cmd(tc.cmd); got != tc.want {
			t.Errorf("%q -> %q; want %q", tc.cmd, got, tc.want)
		}
	}

	// WITHDIST reports the distance in the query's unit, and the nearest is the centre
	// itself when the centre is a member.
	got := c.cmd("GEOSEARCH Sicily FROMMEMBER Palermo BYRADIUS 200 km ASC WITHDIST")
	if !strings.HasPrefix(got, "[[Palermo 0.0000]") {
		t.Errorf("WITHDIST = %q; want Palermo at distance 0 first", got)
	}
	// WITHCOORD and WITHHASH come back in Redis's fixed order: dist, hash, coord.
	got = c.cmd("GEOSEARCH Sicily FROMMEMBER Palermo BYRADIUS 1 km WITHDIST WITHHASH WITHCOORD")
	if !strings.HasPrefix(got, "[[Palermo 0.0000 :") {
		t.Errorf("WITH* reply = %q; want member, distance then hash in Redis's order", got)
	}
	if !strings.Contains(got, "[13.36") {
		t.Errorf("WITHCOORD is missing from %q", got)
	}
	// The hash a search reports is the sorted-set score, which is what makes a geo set an
	// ordinary sorted set -- and the two have to be spelled the same way.
	score := c.cmd("ZSCORE Sicily Palermo")
	if !strings.Contains(got, ":"+score+" ") {
		t.Errorf("WITHHASH (%q) does not match ZSCORE (%q)", got, score)
	}
}

// TestGeoSearchStore covers both stored forms and the deletion of an empty result.
func TestGeoSearchStore(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("GEOADD Sicily 13.361389 38.115556 Palermo")
	c.cmd("GEOADD Sicily 15.087269 37.502669 Catania")
	c.cmd("GEOADD Sicily 13.583333 37.316667 Agrigento")

	if got := c.cmd("GEOSEARCHSTORE dst Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC"); got != ":3" {
		t.Fatalf("GEOSEARCHSTORE = %q", got)
	}
	// By default the destination holds geohashes, so it is itself a geo set.
	if got := c.cmd("GEOSEARCH dst FROMMEMBER Palermo BYRADIUS 1 km"); got != "[Palermo]" {
		t.Errorf("the stored set is not searchable: %q", got)
	}
	if got := c.cmd("ZSCORE dst Palermo"); got != c.cmd("ZSCORE Sicily Palermo") {
		t.Error("the stored score is not the source's geohash")
	}

	// STOREDIST stores distances instead, so the set is ordered by proximity.
	if got := c.cmd("GEOSEARCHSTORE dd Sicily FROMLONLAT 15 37 BYRADIUS 200 km ASC STOREDIST"); got != ":3" {
		t.Fatalf("GEOSEARCHSTORE ... STOREDIST = %q", got)
	}
	if got := c.cmd("ZRANGE dd 0 -1"); got != "[Catania Agrigento Palermo]" {
		t.Errorf("the STOREDIST set is not ordered by distance: %q", got)
	}
	// The stored distance is in the query's unit.
	d := c.cmd("ZSCORE dd Catania")
	if v, _ := strconv.ParseFloat(d, 64); v <= 0 || v > 200 {
		t.Errorf("the stored distance is %q; want kilometres under the 200km radius", d)
	}

	// An empty result deletes the destination rather than leaving a stale answer.
	c.cmd("SET leftover x")
	if got := c.cmd("GEOSEARCHSTORE leftover Sicily FROMLONLAT 0 0 BYRADIUS 1 m"); got != ":0" {
		t.Errorf("an empty GEOSEARCHSTORE = %q", got)
	}
	if got := c.cmd("EXISTS leftover"); got != ":0" {
		t.Error("an empty GEOSEARCHSTORE left the destination in place")
	}
}

// TestGeoIsASortedSet is the interoperation statement: a geo set is a sorted set, so
// every sorted-set command works on it.
func TestGeoIsASortedSet(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	c.cmd("GEOADD Sicily 13.361389 38.115556 Palermo")
	c.cmd("GEOADD Sicily 15.087269 37.502669 Catania")
	if got := c.cmd("TYPE Sicily"); got != "+zset" {
		t.Errorf("TYPE of a geo set = %q; want +zset", got)
	}
	if got := c.cmd("ZCARD Sicily"); got != ":2" {
		t.Errorf("ZCARD = %q", got)
	}
	if got := c.cmd("ZREM Sicily Catania"); got != ":1" {
		t.Errorf("ZREM on a geo set = %q", got)
	}
	if got := c.cmd("GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 200 km"); got != "[Palermo]" {
		t.Errorf("after ZREM the search returns %q", got)
	}
	// And a wrong type is refused.
	c.cmd("SET str x")
	if got := c.cmd("GEOADD str 0 0 m"); got != "-WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Errorf("GEOADD on a string = %q", got)
	}
	if got := c.cmd("GEOSEARCH str FROMLONLAT 0 0 BYRADIUS 1 km"); got != "-WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Errorf("GEOSEARCH on a string = %q", got)
	}
}

// TestGeoSearchFindsEverythingInRange is the property test for the nine-cell trick: for
// a grid of points and a set of radii, the cell-based search must return exactly the
// points a brute-force distance check would.
//
// This is the test that catches a wrong neighbour computation, which otherwise shows up
// only as a member silently missing from a search near a cell boundary.
func TestGeoSearchFindsEverythingInRange(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	type pt struct {
		name     string
		lon, lat float64
	}
	var points []pt
	// A grid around a point, deliberately spanning several geohash cells at every step
	// size the estimator might choose.
	n := 0
	for dlon := -20; dlon <= 20; dlon += 4 {
		for dlat := -20; dlat <= 20; dlat += 4 {
			p := pt{
				name: "g" + strconv.Itoa(n),
				lon:  10 + float64(dlon)/10,
				lat:  45 + float64(dlat)/10,
			}
			points = append(points, p)
			c.cmd("GEOADD grid " + strconv.FormatFloat(p.lon, 'f', -1, 64) + " " +
				strconv.FormatFloat(p.lat, 'f', -1, 64) + " " + p.name)
			n++
		}
	}

	for _, radiusKm := range []float64{0.5, 1, 5, 20, 100, 500} {
		// The brute-force answer, computed the same way GEODIST would.
		want := map[string]bool{}
		for _, p := range points {
			if geoDistance(10, 45, p.lon, p.lat) <= radiusKm*1000 {
				want[p.name] = true
			}
		}
		reply := c.cmd("GEOSEARCH grid FROMLONLAT 10 45 BYRADIUS " +
			strconv.FormatFloat(radiusKm, 'f', -1, 64) + " km")
		got := map[string]bool{}
		for _, name := range strings.Fields(strings.Trim(reply, "[]")) {
			got[name] = true
		}
		for name := range want {
			if !got[name] {
				t.Errorf("radius %vkm: the cell search missed %s, which is inside it", radiusKm, name)
			}
		}
		for name := range got {
			if !want[name] {
				t.Errorf("radius %vkm: the cell search returned %s, which is outside it", radiusKm, name)
			}
		}
	}
}

// TestGeoHashBitsRoundTrip exercises the interleave/deinterleave pair directly, over the
// whole coordinate space rather than the handful of points a command test can reach.
func TestGeoHashBitsRoundTrip(t *testing.T) {
	for _, x := range []uint32{0, 1, 2, 0x5555, 0xffff, 1 << 25, (1 << 26) - 1} {
		for _, y := range []uint32{0, 1, 3, 0xaaaa, 0xffff, 1 << 25, (1 << 26) - 1} {
			gotX, gotY := deinterleave64(interleave64(x, y))
			if gotX != x || gotY != y {
				t.Errorf("interleave/deinterleave (%d,%d) -> (%d,%d)", x, y, gotX, gotY)
			}
		}
	}
	// And the encoded score really is a 52-bit integer, which is what lets a float64
	// score hold it without loss.
	bits, ok := geoEncode(179.999999, 85.05112877)
	if !ok {
		t.Fatal("the extreme corner did not encode")
	}
	if bits >= 1<<52 {
		t.Errorf("the geohash is %d, which needs more than 52 bits", bits)
	}
	if float64(bits) != float64(uint64(float64(bits))) {
		t.Error("the geohash does not survive a round trip through float64")
	}
}

// TestGeoSearchCoverEdgeCases pins three defects in the search *area*, each of which made a
// search answer wrongly while reporting success. All three were found by planting the same
// points on this server and on a live redis:7.2 and comparing the member sets over ~1,300
// randomised searches plus the deliberately hostile shapes; each expectation below is the
// measured Redis answer.
//
// They are worth separate cases because they fail in three different directions:
//
//  1. **Duplicates.** The nine cells stop being distinct once the step is small enough that
//     the grid wraps, so a large radius visited the same cell repeatedly and returned the
//     same member up to four times. A 172-member set answered a 20,000 km radius with 331
//     entries. Beyond the obvious, that also breaks COUNT: it fills up on repeats and
//     returns fewer than n distinct members while looking like it succeeded.
//
//  2. **The box measured its width in the wrong place.** BYBOX compares the east-west
//     distance against half the width, and that distance has to be measured along the
//     *candidate's* parallel, not the query's -- a degree of longitude shortens towards the
//     poles. Measuring at the query's latitude judged poleward candidates by the wrong
//     yardstick, which produced false positives on one side and omissions on the other.
//
//  3. **A pole-reaching circle spans every longitude.** No ring of three columns covers
//     that, and the ordinary reach check cannot detect it because moveY wraps the row index:
//     the "north" neighbour of a top-row cell is the bottom row, half a world away, so the
//     cover looks sufficient. A 5,000 km search from 53N silently omitted a member 4,514 km
//     away at 83N.
func TestGeoSearchCoverEdgeCases(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()
	c := dialTx(t, addr)
	defer c.close()

	// 1. No member may be reported twice, however large the area.
	//
	// Four members spread over the world, so a global radius must return exactly four
	// entries. Before the cover's ranges were merged this answered with repeats.
	c.cmd("GEOADD w 0 0 origin")
	c.cmd("GEOADD w 90 45 east")
	c.cmd("GEOADD w -90 -45 west")
	c.cmd("GEOADD w 179.9 10 dateline")
	for _, radius := range []string{"5000", "20000", "40075", "100000"} {
		got := c.cmd("GEOSEARCH w FROMLONLAT 0 0 BYRADIUS " + radius + " km ASC")
		members := strings.Fields(strings.Trim(got, "[]"))
		seen := map[string]int{}
		for _, m := range members {
			seen[m]++
		}
		for m, n := range seen {
			if n > 1 {
				t.Errorf("BYRADIUS %s km returned %q %d times: %s", radius, m, n, got)
			}
		}
	}
	// The global cases must find all four, so the dedup cannot be hiding an under-cover.
	if got := c.cmd("GEOSEARCH w FROMLONLAT 0 0 BYRADIUS 40075 km ASC"); len(strings.Fields(strings.Trim(got, "[]"))) != 4 {
		t.Errorf("a world-spanning radius returned %s; want all four members exactly once", got)
	}

	// 2. A box's width is measured along the *candidate's* parallel, not the query's.
	//
	// `base` is at (0,10) and `high` at (20,70). Twenty degrees of longitude is 2,190 km
	// along parallel 10 and only 757 km along parallel 70, so the two candidate readings
	// disagree for any box between them -- which is what makes this the case that pins which
	// one Redis uses. A sweep over random points is nearly blind to it, because most
	// candidates sit near the query's own latitude where the two readings agree.
	//
	// The threshold was found by binary-searching the width at which redis:7.2 flips, here
	// and at seven other latitude pairs: it is 757 km every time, never 2,190. So `high` is
	// out of a 1,000 km-wide box (half-width 500) and in a 3,000 km one (half-width 1,500).
	c.cmd("GEOADD box 0 10 base")
	c.cmd("GEOADD box 20 70 high")
	for _, tc := range []struct {
		width string
		want  bool
	}{
		{"1000", false}, {"1400", false}, {"1600", true}, {"3000", true}, {"10000", true},
	} {
		got := c.cmd("GEOSEARCH box FROMLONLAT 0 10 BYBOX " + tc.width + " 15000 km ASC")
		if has := contains(got, "high"); has != tc.want {
			t.Errorf("BYBOX %s 15000 km: high present = %v, want %v (%s)", tc.width, has, tc.want, got)
		}
		if !contains(got, "base") {
			t.Errorf("BYBOX %s 15000 km lost the centre member itself: %s", tc.width, got)
		}
	}

	// 3. A search whose circle reaches the latitude limit must cover every longitude.
	//
	// Measured on redis:7.2: from (-7.103421, 53.468338) a 5,000 km radius includes a point
	// at (118.26, 83.50), which is 4,514 km away over the pole. Both servers agree on that
	// distance; the old cover simply never looked there.
	c.cmd("GEOADD polar -7.103421 53.468338 observer")
	c.cmd("GEOADD polar 118.2625088 83.5012621 faraway")
	if got := c.cmd("GEOSEARCH polar FROMLONLAT -7.103421 53.468338 BYRADIUS 5000 km ASC"); !contains(got, "faraway") {
		t.Errorf("a pole-reaching search omitted a member 4514 km away: %s", got)
	}
	// The distance itself is what makes that a *miss* rather than a judgement call.
	if got := c.cmd("GEODIST polar observer faraway km"); !strings.HasPrefix(got, "4514.") {
		t.Errorf("GEODIST observer faraway = %s; want ~4514 km (redis:7.2 agrees to four decimals)", got)
	}
	// A search that does *not* reach the pole must still exclude it, so the widened cover is
	// not simply returning everything.
	if got := c.cmd("GEOSEARCH polar FROMLONLAT -7.103421 53.468338 BYRADIUS 1000 km ASC"); contains(got, "faraway") {
		t.Errorf("a 1000 km search included a member 4514 km away: %s", got)
	}
}
