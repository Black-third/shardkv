#!/usr/bin/env python3
"""Summarise the raw benchmark rows, refusing to summarise noise.

The interesting output of a benchmark on shared hardware is not the mean. It is
whether the repetitions agree with each other at all: if they do not, every
comparison drawn from them is a comparison of scheduling luck.

So this reports two different things and gates on the second one.

*Absolute* throughput is reported as a median with its full range and a coefficient
of variation. On a contended host that CV is large, and it should be: the number
genuinely is unstable, and hiding that would be the dishonest choice.

The *comparison*, though, does not have to inherit that instability, because the
runner interleaves the two servers within each repetition -- shardkv then redis,
back to back, same rep. Whatever the host was doing during one is very nearly what
it was doing during the other, so the noise is largely common-mode and cancels in a
ratio taken *within* a repetition. The headline is therefore the median of the
per-repetition ratios, gated on the CV of those ratios rather than on the CV of the
absolute figures. A ratio-of-medians would throw that pairing away.

That still cannot rescue a host whose noise is not common-mode. When the paired
ratios disagree with each other, the ratio is withheld, and the answer is a quieter
machine rather than more averaging.

Usage: report.py <raw.tsv> [cv-limit] [text|markdown]

The two output formats carry the same numbers and the same verdicts. `markdown` is
what goes in the README; `text` is what a terminal gets.
"""

from __future__ import annotations

import collections
import statistics
import sys

# Above this coefficient of variation (%) *among the paired ratios*, the
# repetitions disagree too much for a comparison to mean anything, and it is
# withheld rather than printed with a caveat nobody reads.
DEFAULT_CV_LIMIT = 10.0

# raw.tsv columns, in order.
FIELDS = ("suite", "conns", "pipeline", "keyspace", "server", "rep", "test", "rps", "p50", "p99")

SERVERS = ("shardkv", "redis")

# What each suite is for. Printed above its table, because a table of numbers whose
# shape the reader has to reverse-engineer is not evidence either.
SUITE_NOTES = {
    "sweep": (
        "GET/SET, one request in flight per connection, keys spread over 100k slots. "
        "This is the headline: if a per-shard lock buys anything it appears as the "
        "connection count climbs, and if it costs anything it appears at c=1."
    ),
    "pipelined": (
        "The same sweep with 16 requests in flight per connection (-P 16). Pipelining "
        "amortises the syscall pair away, so what is left is per-operation work: "
        "parsing, dispatch, allocation. This is the least flattering shape for a Go "
        "server against hand-tuned C."
    ),
    "collections": (
        "The typed collections -- list, set, sorted set, hash -- plus INCR, at low and "
        "high concurrency. Each is a different store method behind the same shard lock, "
        "so this checks that the sweep's result is a property of the store and not of "
        "the string path alone."
    ),
    "large_4kb": (
        "4 KiB values, where per-operation cost is dominated by copying bytes rather "
        "than by protocol parsing or locking."
    ),
    "streams": (
        "XADD, the write path with the most bookkeeping per operation: id minting, "
        "entry append, per-key counters."
    ),
    "single_key": (
        "The control, and the case sharding cannot help: every request targets one key, "
        "so every connection serialises on one of the 256 shard locks. Published because "
        "a sweep that only shows the spread case is hiding its own worst shape."
    ),
}


def cv(values):
    """Coefficient of variation, as a percentage."""
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    if mean == 0:
        return 0.0
    return 100.0 * statistics.stdev(values) / mean


def load(path):
    """Read raw.tsv into cells keyed by (suite, conns, pipeline, test).

    A cell maps server -> metric -> {rep: value}. Keeping the repetition number
    rather than flattening to a list is what makes the paired ratio possible.
    `order` preserves first appearance so the report follows the run's own sequence.
    """
    cells = collections.defaultdict(
        lambda: collections.defaultdict(lambda: collections.defaultdict(dict))
    )
    order = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < len(FIELDS):
                continue
            row = dict(zip(FIELDS, parts))
            try:
                conns = int(row["conns"])
                pipeline = int(row["pipeline"])
                rep = int(row["rep"])
            except ValueError:
                continue
            key = (row["suite"], conns, pipeline, row["test"])
            if key not in cells:
                order.append(key)
            cell = cells[key][row["server"]]
            for name in ("rps", "p50", "p99"):
                try:
                    cell[name][rep] = float(row[name])
                except ValueError:
                    pass
    return cells, order


class Cell:
    """One (suite, conns, pipeline, test) measurement for both servers."""

    def __init__(self, key, raw):
        self.suite, self.conns, self.pipeline, self.test = key
        self.median = {}
        self.cv = {}
        self.lo = {}
        self.hi = {}
        self.p50 = {}
        self.p99 = {}
        self._rps = {}
        for server in SERVERS:
            metrics = raw.get(server)
            if not metrics or not metrics.get("rps"):
                continue
            by_rep = metrics["rps"]
            values = list(by_rep.values())
            self._rps[server] = by_rep
            self.median[server] = statistics.median(values)
            self.cv[server] = cv(values)
            self.lo[server] = min(values)
            self.hi[server] = max(values)
            self.p50[server] = (
                statistics.median(list(metrics["p50"].values())) if metrics.get("p50") else None
            )
            self.p99[server] = (
                statistics.median(list(metrics["p99"].values())) if metrics.get("p99") else None
            )

        # Paired per-repetition ratios: only reps where both servers produced a
        # figure, so a dropped run cannot silently pair rep 2 against rep 3.
        self.paired = []
        if all(s in self._rps for s in SERVERS):
            shared = sorted(set(self._rps["shardkv"]) & set(self._rps["redis"]))
            for rep in shared:
                denominator = self._rps["redis"][rep]
                if denominator:
                    self.paired.append(self._rps["shardkv"][rep] / denominator)

    @property
    def comparable(self):
        return bool(self.paired)

    @property
    def wins(self):
        """How many paired repetitions shardkv won."""
        return sum(1 for r in self.paired if r > 1)

    @property
    def unanimous(self):
        """True when every paired repetition agreed on which server was faster.

        This is the one thing a contended host cannot take away. The magnitude of a
        ratio needs a quiet machine; its *sign* only needs the pairing to hold, and
        `k` agreeing repetitions out of `k` is a sign test at p = 2^-(k-1) -- 6% for
        five, which is weak on its own and decisive when it lines up with a mechanism
        that predicted the direction in advance.
        """
        n = len(self.paired)
        return n >= 3 and (self.wins == n or self.wins == 0)

    @property
    def abs_cv(self):
        """Worst absolute-throughput CV of the two servers: the host's noise."""
        return max(self.cv.values()) if self.cv else 0.0

    @property
    def ratio(self):
        """Median of the per-repetition shardkv/redis ratios."""
        return statistics.median(self.paired) if self.paired else None

    @property
    def ratio_cv(self):
        """How much the paired ratios disagreed: the noise that did not cancel."""
        return cv(self.paired)

    def verdict(self, limit):
        """(kind, text) where kind is win|loss|dir-win|dir-loss|noisy.

        `win`/`loss` mean the magnitude is certifiable on this host. `dir-win`/
        `dir-loss` mean only the direction is: every repetition agreed on who was
        faster, but they disagreed too much about by how much.
        """
        ratio = self.ratio
        n = len(self.paired)
        if ratio is None:
            return "noisy", "incomplete"
        if n < 2:
            return "noisy", "1 rep"
        if self.ratio_cv <= limit:
            if ratio >= 1:
                return "win", "shardkv %.2fx" % ratio
            return "loss", "redis %.2fx" % (1 / ratio)
        if self.unanimous:
            side = "shardkv" if self.wins == n else "redis"
            return (
                "dir-win" if self.wins == n else "dir-loss",
                "%s faster in %d/%d reps, magnitude withheld (ratio CV %.1f%%)"
                % (side, max(self.wins, n - self.wins), n, self.ratio_cv),
            )
        return "noisy", "ratio CV %.1f%%, direction split %d/%d" % (self.ratio_cv, self.wins, n)

    @property
    def label(self):
        return "%s c=%d P=%d %s" % (self.suite, self.conns, self.pipeline, self.test)


def preamble(limit):
    return [
        "ops/sec is the median of the repetitions and `range` is the full spread. `abs CV`",
        "is how much that absolute figure disagreed with itself across repetitions -- the",
        "host's noise, reported rather than hidden.",
        "",
        "`ratio` is NOT those two medians divided. The runner interleaves the servers within",
        "each repetition, so a ratio taken inside one repetition cancels the noise both",
        "servers saw together; `ratio` is the median of those per-repetition ratios and",
        "`ratio CV` is how much they disagreed. A magnitude is withheld when `ratio CV`",
        "exceeds %.0f%%, because past that it is a measurement of the host." % limit,
        "",
        "`won` is how many of the paired repetitions shardkv was faster in. When that is",
        "unanimous the *direction* stands even though the magnitude does not: the sign of a",
        "paired difference survives noise that its size does not. A row reading `0/5` or",
        "`5/5` with a withheld magnitude is a real result about which server is faster and",
        "no result at all about by how much.",
        "",
        "shardkv uses every core it is given and Redis is single-threaded by design, so a",
        "ratio above 1 is one process beating one process -- not a faster per-operation",
        "path. The p50/p99 columns are where that question gets its real answer.",
    ]


def emit_text(cells, order, limit):
    for line in preamble(limit):
        print(line)
    print()

    header = "%-30s %-8s %12s %7s %8s %8s   %-23s" % (
        "suite / c / P / test",
        "server",
        "ops/sec",
        "abs CV",
        "p50 ms",
        "p99 ms",
        "range (ops/sec)",
    )
    print(header)
    print("-" * len(header))

    results = []
    suite = None
    for key in order:
        cell = Cell(key, cells[key])
        results.append(cell)
        if cell.suite != suite:
            suite = cell.suite
            print()
        label = cell.label
        for server in SERVERS:
            if server not in cell.median:
                continue
            p50 = cell.p50[server]
            p99 = cell.p99[server]
            print(
                "%-30s %-8s %12s %6.1f%% %8s %8s   %s - %s"
                % (
                    label,
                    server,
                    "{:,.0f}".format(cell.median[server]),
                    cell.cv[server],
                    "n/a" if p50 is None else "%.3f" % p50,
                    "n/a" if p99 is None else "%.3f" % p99,
                    "{:,.0f}".format(cell.lo[server]),
                    "{:,.0f}".format(cell.hi[server]),
                )
            )
            label = ""
        kind, text = cell.verdict(limit)
        if kind in ("win", "loss"):
            text = "%s  (ratio CV %.1f%%, won %d/%d)" % (
                text,
                cell.ratio_cv,
                cell.wins,
                len(cell.paired),
            )
        elif kind == "noisy":
            text = "no conclusion: " + text
        print("%-30s %-8s %s" % ("", "", text))
    print()
    return results


def md_row(values):
    return "| " + " | ".join(values) + " |"


def emit_markdown(cells, order, limit):
    print("### Results")
    print()
    for line in preamble(limit):
        print(line)
    print()

    results = []
    by_suite = collections.OrderedDict()
    for key in order:
        cell = Cell(key, cells[key])
        results.append(cell)
        by_suite.setdefault(cell.suite, []).append(cell)

    for suite, group in by_suite.items():
        print("#### %s" % suite)
        print()
        note = SUITE_NOTES.get(suite)
        if note:
            print(note)
            print()
        head = [
            "conns",
            "P",
            "test",
            "shardkv ops/sec",
            "redis ops/sec",
            "ratio",
            "won",
            "ratio CV",
            "abs CV",
            "shardkv p50/p99 ms",
            "redis p50/p99 ms",
        ]
        print(md_row(head))
        print(md_row(["---"] * len(head)))
        for cell in group:
            kind, text = cell.verdict(limit)
            if kind == "loss":
                text = "**%s**" % text
            elif kind == "dir-win":
                text = "_shardkv (size withheld)_"
            elif kind == "dir-loss":
                text = "_**redis** (size withheld)_"
            elif kind == "noisy":
                text = "_no conclusion_"

            def num(mapping, server, fmt="{:,.0f}"):
                value = mapping.get(server)
                return "n/a" if value is None else fmt.format(value)

            def latency(server):
                p50, p99 = cell.p50.get(server), cell.p99.get(server)
                if p50 is None or p99 is None:
                    return "n/a"
                return "%.3f / %.3f" % (p50, p99)

            print(
                md_row(
                    [
                        str(cell.conns),
                        str(cell.pipeline),
                        cell.test,
                        num(cell.median, "shardkv"),
                        num(cell.median, "redis"),
                        text,
                        "%d/%d" % (cell.wins, len(cell.paired)) if cell.comparable else "n/a",
                        "%.1f%%" % cell.ratio_cv if cell.comparable else "n/a",
                        "%.1f%%" % cell.abs_cv,
                        latency("shardkv"),
                        latency("redis"),
                    ]
                )
            )
        print()
    return results


def emit_verdicts(results, limit, markdown):
    def bucket(kind):
        return [c for c in results if c.verdict(limit)[0] == kind]

    wins, losses = bucket("win"), bucket("loss")
    dir_wins, dir_losses = bucket("dir-win"), bucket("dir-loss")
    noisy = bucket("noisy")

    def section(title, group):
        if not group:
            return
        if markdown:
            print("**%s**" % title)
            print()
            for cell in group:
                print("- `%s` — %s" % (cell.label, cell.verdict(limit)[1]))
            print()
        else:
            print("%s:" % title)
            for cell in group:
                print("  %-34s %s" % (cell.label, cell.verdict(limit)[1]))
            print()

    # Losses first, deliberately: a table that leads with its wins is advertising,
    # not evidence. Same order inside the direction-only pair.
    section("Where shardkv loses (magnitude certified)", losses)
    section("Where shardkv wins (magnitude certified)", wins)
    section("Where shardkv loses, direction only", dir_losses)
    section("Where shardkv wins, direction only", dir_wins)

    if noisy:
        section("No conclusion on this host (the repetitions disagreed on direction too)", noisy)
        for line in (
            "Nothing in the withheld rows should be quoted as a comparison. Re-run on a",
            "host that is otherwise idle -- ideally Linux with Docker running natively, a",
            "fixed CPU governor, and the client pinned away from the servers -- with",
            "REPS>=5, and check these coefficients again before reading any mean.",
        ):
            print(line)
        print()
        return 2
    return 0


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        return 1
    path = sys.argv[1]
    limit = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_CV_LIMIT
    fmt = sys.argv[3] if len(sys.argv) > 3 else "text"

    cells, order = load(path)
    if not order:
        print("no rows; the benchmark produced nothing")
        return 1

    if fmt == "markdown":
        results = emit_markdown(cells, order, limit)
    else:
        results = emit_text(cells, order, limit)
    return emit_verdicts(results, limit, fmt == "markdown")


if __name__ == "__main__":
    sys.exit(main())
