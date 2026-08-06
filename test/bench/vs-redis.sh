#!/usr/bin/env bash
#
# End-to-end throughput and latency, shardkv against a real Redis, under
# conditions as close to identical as two processes can be given: the same host,
# the same container runtime, the same network, the same `redis-benchmark` binary
# out of the same image, the same profiles in the same order, and neither server
# persisting anything.
#
#     ./test/bench/vs-redis.sh                 # every profile, 3 repetitions
#     REPS=5 ./test/bench/vs-redis.sh          # more repetitions
#     PROFILES="baseline pipelined" ./test/bench/vs-redis.sh
#     KEEP=1 ./test/bench/vs-redis.sh          # leave the servers running
#
# ---------------------------------------------------------------------------
# Read this before quoting a number out of it
#
# The script repeats every profile and reports the *spread*, not a single figure,
# because a single figure from a shared, virtualised host is not a measurement --
# it is a sample of the noise. It computes a coefficient of variation per profile
# and refuses to summarise anything above CV_LIMIT (default 10%), printing the
# spread instead. If your run trips that, the answer is not to average harder: it
# is that the host cannot answer the question.
#
# In particular, **Docker Desktop on macOS cannot**. Client and servers sit behind
# a VM boundary with a userland network proxy, and this project measured a real
# Redis at 78k, 191k and 781k SET/sec across three identical consecutive warm runs
# on such a host -- a tenfold spread with nothing changed between them. Numbers
# from that environment are not evidence about either server.
#
# To get figures worth publishing: a Linux host with Docker running natively, the
# benchmark pinned away from the server's cores (`taskset`), CPU frequency scaling
# fixed at a constant governor, no other tenants, and REPS at 5 or more. Then the
# spread this script prints is the thing to check first, before the means.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
out="$here/.bin"
results="$here/.results"

NET=${NET:-shardkv-bench}
PREFIX=${PREFIX:-skbench}
REDIS_IMAGE=${REDIS_IMAGE:-redis:7-alpine}
SERVER_IMAGE=${SERVER_IMAGE:-alpine:3.21}
REPS=${REPS:-3}
CV_LIMIT=${CV_LIMIT:-10}
KEEP=${KEEP:-0}

# Profiles. Each is a redis-benchmark argument list, and the name is what the
# report calls it. They are chosen to cover the shapes a real deployment has --
# a request at a time, deep pipelining, a fan of connections, big values, the
# collection types, a stream -- rather than to flatter either server.
declare -A PROFILE_ARGS=(
	[baseline]="-t set,get -n 100000 -c 50 -P 1"
	[pipelined]="-t set,get -n 500000 -c 50 -P 16"
	[many_connections]="-t set,get -n 100000 -c 500 -P 1"
	[large_values]="-t set,get -n 50000 -c 50 -d 4096"
	[collections]="-t lpush,rpush,lpop,sadd,spop,zadd,hset,incr -n 100000 -c 50"
	[streams]="-t xadd -n 100000 -c 50"
)
PROFILES=${PROFILES:-"baseline pipelined many_connections large_values collections streams"}

log() { printf '\033[36m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[33m!!\033[0m %s\n' "$*" >&2; }

cleanup() {
	if [ "$KEEP" = "1" ]; then
		warn "KEEP=1: leaving the servers up"
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

# ---------------------------------------------------------------------------
# Environment, recorded in the report. A benchmark without its hardware is an
# anecdote.
# ---------------------------------------------------------------------------
describe_host() {
	echo "date:            $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
	echo "kernel:          $(uname -srm)"
	if [ "$(uname -s)" = Darwin ]; then
		echo "cpu:             $(sysctl -n machdep.cpu.brand_string 2>/dev/null)"
		echo "cores:           $(sysctl -n hw.ncpu 2>/dev/null)"
		echo "memory:          $(($(sysctl -n hw.memsize 2>/dev/null) / 1024 / 1024 / 1024)) GiB"
	else
		echo "cpu:             $(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null)"
		echo "cores:           $(nproc 2>/dev/null)"
		echo "memory:          $(awk '/MemTotal/ {printf "%.0f GiB", $2/1024/1024}' /proc/meminfo 2>/dev/null)"
	fi
	echo "docker:          $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
	echo "docker arch:     $(docker_arch)"
	echo "docker cpus:     $(docker info --format '{{.NCPU}}' 2>/dev/null)"
	echo "docker memory:   $(($(docker info --format '{{.MemTotal}}' 2>/dev/null) / 1024 / 1024 / 1024)) GiB"
	echo "shardkv version: $(git -C "$root" describe --tags --always --dirty 2>/dev/null)"
	echo "redis image:     $REDIS_IMAGE ($(docker run --rm "$REDIS_IMAGE" redis-server --version 2>/dev/null | head -1))"
	echo "reps:            $REPS"
	if [ "$(uname -s)" = Darwin ]; then
		echo
		echo "WARNING: this is macOS. Docker Desktop runs the containers inside a VM behind"
		echo "a userland network proxy, and the resulting spread has been measured at 10x"
		echo "across identical consecutive runs. Treat every number below as unusable for"
		echo "comparison and read only the coefficients of variation."
	fi
}

# ---------------------------------------------------------------------------
# Servers. Neither persists: an AOF would be measuring the disk, and -- more to
# the point -- it would measure two *different* durability contracts against each
# other, which is not a comparison of anything.
# ---------------------------------------------------------------------------
start_servers() {
	docker network create "$NET" >/dev/null 2>&1

	docker rm -f "$PREFIX-shardkv" "$PREFIX-redis" >/dev/null 2>&1
	docker run -d --name "$PREFIX-shardkv" --network "$NET" \
		-v "$out:/w:ro" "$SERVER_IMAGE" /w/shardkv -addr :6380 >/dev/null

	# --save '' turns off RDB snapshots, --appendonly no turns off the AOF: the
	# same "no persistence" shardkv is running with by virtue of no -aof flag.
	docker run -d --name "$PREFIX-redis" --network "$NET" "$REDIS_IMAGE" \
		redis-server --port 6380 --save '' --appendonly no >/dev/null

	local name i
	for name in "$PREFIX-shardkv" "$PREFIX-redis"; do
		for i in $(seq 1 60); do
			if [ "$(bench_cli "$name" PING 2>/dev/null | tr -d '\r')" = PONG ]; then
				break
			fi
			sleep 0.5
		done
	done
}

bench_cli() {
	local host="$1"
	shift
	docker run --rm --network "$NET" "$REDIS_IMAGE" redis-cli -h "$host" -p 6380 "$@"
}

# run_profile <server-container> <profile> <rep>
# Appends one row per redis-benchmark test case to the results file.
run_profile() {
	local host="$1" profile="$2" rep="$3"
	local args="${PROFILE_ARGS[$profile]}"
	local label=${host#"$PREFIX"-}

	bench_cli "$host" FLUSHALL >/dev/null 2>&1

	# --csv gives one row per test: "name","rps","avg","min","p50","p95","p99","max".
	docker run --rm --network "$NET" "$REDIS_IMAGE" \
		redis-benchmark -h "$host" -p 6380 --csv $args 2>/dev/null |
		tail -n +2 |
		tr -d '"' |
		while IFS=, read -r test rps avg min p50 p95 p99 max; do
			[ -z "${rps:-}" ] && continue
			printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
				"$profile" "$label" "$rep" "$test" "$rps" "${p50:-}" "${p99:-}"
		done >>"$results/raw.tsv"
}

# ---------------------------------------------------------------------------
# Go.
# ---------------------------------------------------------------------------
mkdir -p "$out" "$results"
: >"$results/raw.tsv"

log "building shardkv for linux/$(docker_arch)"
(cd "$root" && CGO_ENABLED=0 GOOS=linux GOARCH="$(docker_arch)" \
	go build -trimpath -o "$out/shardkv" ./cmd/shardkv) || exit 1

log "starting both servers"
start_servers

# A warm-up pass, discarded. The first run of anything pays for page faults, a
# cold container image cache and a JIT-less but still cold branch predictor; a
# benchmark that includes it is measuring startup.
log "warm-up (discarded)"
for host in "$PREFIX-shardkv" "$PREFIX-redis"; do
	docker run --rm --network "$NET" "$REDIS_IMAGE" \
		redis-benchmark -h "$host" -p 6380 -t set,get -n 20000 -c 50 -q >/dev/null 2>&1
done

for rep in $(seq 1 "$REPS"); do
	for profile in $PROFILES; do
		if [ -z "${PROFILE_ARGS[$profile]:-}" ]; then
			warn "unknown profile: $profile"
			continue
		fi
		# Interleaved rather than all reps of one server then all of the other:
		# if the host's load drifts during the run, interleaving spreads that
		# drift across both servers instead of handing it to one of them.
		for host in "$PREFIX-shardkv" "$PREFIX-redis"; do
			log "rep $rep/$REPS  $profile  ${host#"$PREFIX"-}"
			run_profile "$host" "$profile" "$rep"
		done
	done
done

{
	echo "# shardkv vs redis: end-to-end benchmark"
	echo
	describe_host
	echo
} >"$results/report.txt"

python3 "$here/report.py" "$results/raw.tsv" "$CV_LIMIT" >>"$results/report.txt"
cat "$results/report.txt"
log "raw rows: $results/raw.tsv"
log "report:   $results/report.txt"
