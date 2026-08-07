#!/usr/bin/env bash
#
# End-to-end throughput and latency, shardkv against a real Redis, under
# conditions as close to identical as two processes can be given: the same host,
# the same container runtime, the same network, the same `redis-benchmark` binary
# out of the same image, the same base image for both servers, the same profiles
# in the same order, and neither server persisting anything.
#
#     ./test/bench/vs-redis.sh                 # every suite, 5 repetitions (~35 min)
#     SCALE=20 REPS=2 ./test/bench/vs-redis.sh # plumbing check, measures nothing
#     MULT=4 SUITES=sweep ./test/bench/vs-redis.sh  # longer measurements, less noise
#     KEEP=1 ./test/bench/vs-redis.sh          # leave the servers running
#
# ---------------------------------------------------------------------------
# Read this before quoting a number out of it
#
# The script repeats every measurement and reports the *spread*, not a single
# figure, because a single figure from a shared host is not a measurement -- it is
# a sample of the noise. It computes a coefficient of variation per cell and
# refuses to summarise anything above CV_LIMIT (default 10%), printing the spread
# instead. If your run trips that, the answer is not to average harder: it is that
# the host cannot answer the question. The host's load average is recorded before
# and after the run for exactly this reason; if it is not a small number, stop.
#
# Four methodological choices are load-bearing, and getting any of them wrong
# produces a number that looks fine and means nothing.
#
# 1. **The keyspace has to be spread.** `redis-benchmark` without `-r` sends every
#    request to the *same key* -- literally `key:__rand_int__`, because the
#    placeholder is only substituted when `-r` is given. Verify it yourself: run
#    `redis-benchmark -t set -n 1000` and then `DBSIZE`, and the answer is 1. On a
#    sharded store that funnels the entire load through one of the 256 shards, so
#    it measures single-shard lock contention and is precisely blind to the thing
#    this project claims. Every suite here passes `-r`, and `single_key` keeps the
#    unspread case deliberately, as the control that shows what sharding cannot do
#    for you.
#
# 2. **The client must not be the bottleneck.** `redis-benchmark` is
#    single-threaded unless told otherwise, and one thread saturates before a
#    multi-core server does -- at which point both servers return the same number
#    and the benchmark is measuring the client. Each spec therefore carries a
#    `--threads` count sized to its connection count. It is measurably still a
#    partial bottleneck at the top of the sweep: on the host in the README, c=128
#    unpipelined SET rose from 38.9k to 49.6k (shardkv) and 33.4k to 48.7k (redis)
#    going from one client thread to eight. Both sides gain, so the *ratio* is
#    roughly preserved, but every absolute figure here is a floor rather than the
#    server's ceiling.
#
# 3. **The load generator shares the host with the servers.** There is one machine,
#    so the client's threads compete with the server's for the same cores. That is
#    also why `--threads` is capped at 4 rather than 8: a client that takes every
#    core starves the server it is measuring. The cap holds both servers back and is
#    stated rather than hidden; a two-machine setup with the client pinned away from
#    the server is the only way to remove it.
#
# 4. **This compares process to process, not core to core.** Neither container gets
#    a CPU limit, so shardkv's Go runtime uses all of the VM's cores while Redis
#    executes commands on one, by design. A shardkv win at high concurrency is
#    therefore a win for "one shardkv process vs one redis process" and not evidence
#    of a faster per-operation path -- the per-connection latency columns are where
#    that question is actually answered, and it is answered against us.
#
#    Redis is left at its own defaults, which means `io-threads 1`. Redis 7 can move
#    *network* I/O (not command execution) onto more threads, and a reader who wants
#    the strongest possible Redis should re-run with `--io-threads 4` appended to the
#    `redis-server` line in start_servers. That closes part of the unpipelined gap,
#    since that gap is substantially syscall-bound; it does not parallelise the
#    command execution itself, which stays on one thread.
#
# On macOS specifically: both servers and the client run inside the Docker Desktop
# VM on a user-defined bridge, so the host's userland port proxy is *not* on the
# path (it only sits in front of published ports, and nothing here publishes one).
# What remains is the VM's fixed CPU allocation and whatever else the host is
# doing, which is what the load-average lines are there to expose.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
out="$here/.bin"
results="$here/.results"

NET=${NET:-shardkv-bench}
PREFIX=${PREFIX:-skbench}
REDIS_IMAGE=${REDIS_IMAGE:-redis:7-alpine}
REPS=${REPS:-5}
CV_LIMIT=${CV_LIMIT:-10}
KEEP=${KEEP:-0}
PORT=6380
# Divides every spec's request count. SCALE=20 turns a half-hour run into a
# ninety-second check that the plumbing works; it is not a setting to measure with,
# because a short run is mostly connection setup.
SCALE=${SCALE:-1}
# Multiplies every spec's request count, i.e. makes each measurement run longer.
#
# This is the knob that buys comparability on a contended host, and the effect was
# measured rather than assumed: on the machine in the README (load average ~330),
# `sweep c=128 SET` over four paired repetitions gave a ratio CV of 27.2% at the
# default count and 11.9% at five times the count. A longer measurement averages over
# more of the host's bursts, so the noise that does not cancel in the pairing shrinks.
# It costs time linearly -- MULT=4 on the default suites is roughly two hours -- which
# is the trade a noisy host imposes.
MULT=${MULT:-1}

# ---------------------------------------------------------------------------
# Specs: suite|conns|pipeline|requests|keyspace|threads|tests|extra
#
# `keyspace` is the -r argument, and 0 means "omit -r", i.e. hammer one key.
# The sweep is the headline -- 1 to 512 connections is where a per-shard lock
# either buys something or does not -- and `pipelined` is the same sweep with the
# syscall cost amortised 16 ways, which is where a Go server's per-op work shows
# up undisguised by the read/write pair around it.
# ---------------------------------------------------------------------------
SPECS=(
	"sweep|1|1|20000|100000|1|set,get|"
	"sweep|8|1|60000|100000|2|set,get|"
	"sweep|32|1|120000|100000|4|set,get|"
	"sweep|128|1|150000|100000|4|set,get|"
	"sweep|512|1|150000|100000|4|set,get|"

	"pipelined|1|16|100000|100000|1|set,get|"
	"pipelined|8|16|400000|100000|2|set,get|"
	"pipelined|32|16|400000|100000|4|set,get|"
	"pipelined|128|16|400000|100000|4|set,get|"
	"pipelined|512|16|400000|100000|4|set,get|"

	"collections|8|1|48000|100000|2|lpush,lpop,sadd,zadd,hset,incr|"
	"collections|128|1|150000|100000|4|lpush,lpop,sadd,zadd,hset,incr|"

	"large_4kb|32|1|50000|100000|4|set,get|-d 4096"
	"streams|32|1|100000|100000|4|xadd|"
	"single_key|128|1|100000|0|4|set,get|"
)
SUITES=${SUITES:-"sweep pipelined collections large_4kb streams single_key"}

log() { printf '\033[36m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[33m!!\033[0m %s\n' "$*" >&2; }

# Only ever removes what this script created: every container it starts is named
# with $PREFIX-, and the network is $NET. A filter broader than that would reach
# containers belonging to whoever else is using this host.
cleanup() {
	if [ "$KEEP" = "1" ]; then
		warn "KEEP=1: leaving the servers up (docker rm -f ${PREFIX}-shardkv ${PREFIX}-redis ${PREFIX}-cli)"
		return
	fi
	local ids
	ids=$(docker ps -aq --filter "name=^${PREFIX}-" 2>/dev/null || true)
	[ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
	docker network rm "$NET" >/dev/null 2>&1
	return 0
}
trap cleanup EXIT

docker_arch() {
	case "$(docker info --format '{{.Architecture}}' 2>/dev/null)" in
	aarch64 | arm64) echo arm64 ;;
	x86_64 | amd64) echo amd64 ;;
	*) go env GOARCH ;;
	esac
}

loadavg() {
	if [ "$(uname -s)" = Darwin ]; then
		sysctl -n vm.loadavg 2>/dev/null | tr -d '{}'
	else
		cut -d' ' -f1-3 /proc/loadavg 2>/dev/null
	fi
}

# ---------------------------------------------------------------------------
# Environment, recorded in the report. A benchmark without its hardware is an
# anecdote.
# ---------------------------------------------------------------------------
describe_host() {
	echo "date:             $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
	echo "kernel:           $(uname -srm)"
	if [ "$(uname -s)" = Darwin ]; then
		echo "cpu:              $(sysctl -n machdep.cpu.brand_string 2>/dev/null)"
		echo "cores:            $(sysctl -n hw.ncpu 2>/dev/null)"
		echo "memory:           $(($(sysctl -n hw.memsize 2>/dev/null) / 1024 / 1024 / 1024)) GiB"
	else
		echo "cpu:              $(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null)"
		echo "cores:            $(nproc 2>/dev/null)"
		echo "memory:           $(awk '/MemTotal/ {printf "%.0f GiB", $2/1024/1024}' /proc/meminfo 2>/dev/null)"
	fi
	echo "docker:           $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
	echo "docker arch:      $(docker_arch)"
	echo "docker cpus:      $(docker info --format '{{.NCPU}}' 2>/dev/null)"
	echo "docker memory:    $(($(docker info --format '{{.MemTotal}}' 2>/dev/null) / 1024 / 1024 / 1024)) GiB"
	echo "shardkv version:  $(git -C "$root" describe --tags --always --dirty 2>/dev/null)"
	echo "shardkv go:       $(go version 2>/dev/null | awk '{print $3}')"
	echo "redis image:      $REDIS_IMAGE"
	echo "redis version:    $(docker run --rm "$REDIS_IMAGE" redis-server --version 2>/dev/null | awk '{print $3}' | sed 's/v=//')"
	echo "benchmark tool:   redis-benchmark from $REDIS_IMAGE"
	echo "server base:      $REDIS_IMAGE for both (shardkv is a static binary run in it)"
	echo "reps:             $REPS"
	echo "load avg (start): $LOAD_START"
	echo "other containers: $(docker ps -q | wc -l | tr -d ' ') running on this docker daemon"
}

# ---------------------------------------------------------------------------
# Servers. Neither persists: an AOF would be measuring the disk, and -- more to
# the point -- it would measure two *different* durability contracts against each
# other, which is not a comparison of anything.
#
# Both run out of the same image, so the base OS, libc and image layers are not a
# variable. shardkv is a static CGO_ENABLED=0 binary, so it needs nothing from it.
# ---------------------------------------------------------------------------
start_servers() {
	docker network create "$NET" >/dev/null 2>&1
	docker rm -f "$PREFIX-shardkv" "$PREFIX-redis" "$PREFIX-cli" >/dev/null 2>&1

	docker run -d --name "$PREFIX-shardkv" --network "$NET" \
		-v "$out:/w:ro" --entrypoint /w/shardkv "$REDIS_IMAGE" \
		-addr ":$PORT" >/dev/null

	# --save '' turns off RDB snapshots, --appendonly no turns off the AOF: the
	# same "no persistence" shardkv is running with by virtue of no -aof flag.
	docker run -d --name "$PREFIX-redis" --network "$NET" "$REDIS_IMAGE" \
		redis-server --port "$PORT" --save '' --appendonly no >/dev/null

	# One long-lived client container that every benchmark invocation execs into.
	# `docker run --rm` per invocation costs seconds on a busy host -- more than
	# some of the measurements themselves -- and that cost lands unevenly.
	docker run -d --name "$PREFIX-cli" --network "$NET" --entrypoint sh \
		"$REDIS_IMAGE" -c 'while true; do sleep 3600; done' >/dev/null

	local name i ready
	for name in "$PREFIX-shardkv" "$PREFIX-redis"; do
		ready=0
		for i in $(seq 1 60); do
			if [ "$(cli "$name" PING 2>/dev/null | tr -d '\r')" = PONG ]; then
				ready=1
				break
			fi
			sleep 0.5
		done
		if [ "$ready" != 1 ]; then
			warn "$name never answered PING; logs follow"
			docker logs "$name" 2>&1 | tail -20 >&2
			exit 1
		fi
	done
}

cli() {
	local host="$1"
	shift
	docker exec "$PREFIX-cli" redis-cli -h "$host" -p "$PORT" "$@"
}

# run_spec <server-container> <spec> <rep>
# Appends one row per redis-benchmark test case to the results file.
run_spec() {
	local host="$1" spec="$2" rep="$3"
	local label=${host#"$PREFIX"-}
	local suite conns pipeline requests keyspace threads tests extra
	IFS='|' read -r suite conns pipeline requests keyspace threads tests extra <<<"$spec"

	requests=$((requests * MULT / SCALE))
	# redis-benchmark needs at least one request per connection to have anything
	# to report per client.
	[ "$requests" -lt "$conns" ] && requests=$conns

	local args="-t $tests -n $requests -c $conns -P $pipeline --threads $threads"
	[ "$keyspace" != 0 ] && args="$args -r $keyspace"
	[ -n "$extra" ] && args="$args $extra"

	cli "$host" FLUSHALL >/dev/null 2>&1

	# --csv gives one row per test:
	# "name","rps","avg","min","p50","p95","p99","max".
	docker exec "$PREFIX-cli" \
		redis-benchmark -h "$host" -p "$PORT" --csv $args 2>/dev/null |
		tail -n +2 |
		tr -d '"' |
		while IFS=, read -r test rps avg min p50 p95 p99 max; do
			[ -z "${rps:-}" ] && continue
			printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
				"$suite" "$conns" "$pipeline" "$keyspace" \
				"$label" "$rep" "$test" "$rps" "${p50:-}" "${p99:-}"
		done >>"$results/raw.tsv"
}

# Fills the global CHOSEN array with the specs whose suite is in $SUITES, in the
# order SPECS declares them. A function returning them on stdout would be tidier,
# but bash 3.2 -- which is what macOS ships, and so what this has to run on -- has
# neither `mapfile` nor associative arrays.
select_specs() {
	local spec suite want
	CHOSEN=()
	for spec in "${SPECS[@]}"; do
		[ -z "$spec" ] && continue
		suite=${spec%%|*}
		for want in $SUITES; do
			if [ "$suite" = "$want" ]; then
				CHOSEN[${#CHOSEN[@]}]="$spec"
				break
			fi
		done
	done
}

# ---------------------------------------------------------------------------
# Go.
# ---------------------------------------------------------------------------
command -v docker >/dev/null 2>&1 || {
	warn "docker is required"
	exit 1
}
docker info >/dev/null 2>&1 || {
	warn "the docker daemon is not reachable"
	exit 1
}

mkdir -p "$out" "$results"
: >"$results/raw.tsv"
LOAD_START=$(loadavg)

log "building shardkv for linux/$(docker_arch)"
(cd "$root" && CGO_ENABLED=0 GOOS=linux GOARCH="$(docker_arch)" \
	go build -trimpath -o "$out/shardkv" ./cmd/shardkv) || exit 1

log "starting both servers"
start_servers

select_specs
if [ "${#CHOSEN[@]}" = 0 ]; then
	warn "no specs matched SUITES=$SUITES"
	exit 1
fi

# A warm-up pass, discarded. The first run of anything pays for page faults, a
# cold container image cache and a cold branch predictor; a benchmark that
# includes it is measuring startup.
log "warm-up (discarded)"
for host in "$PREFIX-shardkv" "$PREFIX-redis"; do
	docker exec "$PREFIX-cli" redis-benchmark -h "$host" -p "$PORT" \
		-t set,get -n 20000 -c 50 -r 100000 -q >/dev/null 2>&1
done

total=$((${#CHOSEN[@]} * REPS * 2))
done_n=0
for rep in $(seq 1 "$REPS"); do
	for spec in "${CHOSEN[@]}"; do
		# Interleaved rather than all reps of one server then all of the other:
		# if the host's load drifts during the run, interleaving spreads that
		# drift across both servers instead of handing it to one of them.
		for host in "$PREFIX-shardkv" "$PREFIX-redis"; do
			done_n=$((done_n + 1))
			IFS='|' read -r s c p _ _ _ t _ <<<"$spec"
			log "[$done_n/$total] rep $rep/$REPS  $s c=$c P=$p ($t)  ${host#"$PREFIX"-}"
			run_spec "$host" "$spec" "$rep"
		done
	done
done

LOAD_END=$(loadavg)

{
	echo "# shardkv vs redis: end-to-end benchmark"
	echo
	describe_host
	echo "load avg (end):   $LOAD_END"
	echo
} >"$results/report.txt"

python3 "$here/report.py" "$results/raw.tsv" "$CV_LIMIT" text >>"$results/report.txt"
status=$?

{
	echo "<!-- generated by test/bench/vs-redis.sh; do not edit by hand -->"
	echo
	describe_host | sed 's/^/    /'
	echo "    load avg (end):   $LOAD_END"
	echo
} >"$results/report.md"
python3 "$here/report.py" "$results/raw.tsv" "$CV_LIMIT" markdown >>"$results/report.md"

cat "$results/report.txt"
log "raw rows: $results/raw.tsv"
log "report:   $results/report.txt"
log "markdown: $results/report.md"
exit $status
