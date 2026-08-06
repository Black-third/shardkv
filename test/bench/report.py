#!/usr/bin/env python3
"""Summarise the raw benchmark rows, refusing to summarise noise.

The interesting output of a benchmark on shared hardware is not the mean. It is
whether the repetitions agree with each other at all: if they do not, every
comparison drawn from them is a comparison of scheduling luck. So each cell is
reported as median with its full range, and the coefficient of variation decides
whether a ratio between the two servers is printed at all.
"""

from __future__ import annotations

import collections
import statistics
import sys

# Above this coefficient of variation (%), a cell's repetitions disagree too much
# for a ratio to mean anything, and the ratio is withheld rather than printed with
# a caveat nobody reads.
DEFAULT_CV_LIMIT = 10.0


def cv(values: list[float]) -> float:
    """Coefficient of variation, as a percentage."""
    if len(values) < 2:
        return 0.0
    mean = statistics.fmean(values)
    if mean == 0:
        return 0.0
    return 100.0 * statistics.stdev(values) / mean


def main() -> int:
    path = sys.argv[1]
    limit = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_CV_LIMIT

    # (profile, test) -> server -> metric -> [values]
    rows: dict[tuple[str, str], dict[str, dict[str, list[float]]]] = collections.defaultdict(
        lambda: collections.defaultdict(lambda: collections.defaultdict(list))
    )
    order: list[tuple[str, str]] = []

    with open(path, encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 7:
                continue
            profile, server, _rep, test, rps, p50, p99 = parts[:7]
            key = (profile, test)
            if key not in rows:
                order.append(key)
            cell = rows[key][server]
            for name, raw in (("rps", rps), ("p50", p50), ("p99", p99)):
                try:
                    cell[name].append(float(raw))
                except ValueError:
                    pass

    if not order:
        print("no rows; the benchmark produced nothing")
        return 1

    print("Each cell is the median of the repetitions, with the full range beneath it.")
    print("CV is the coefficient of variation across repetitions: how much the same")
    print(f"measurement disagreed with itself. A ratio is withheld above {limit:.0f}% CV,")
    print("because at that point it is a measurement of the host and not of either server.")
    print()

    header = (
        f"{'profile / test':<26} {'server':<8} {'ops/sec':>12} {'CV':>7} "
        f"{'p50 ms':>8} {'p99 ms':>8}   {'range (ops/sec)':<24}"
    )
    print(header)
    print("-" * len(header))

    noisy: list[str] = []
    verdicts: list[tuple[str, str]] = []

    for profile, test in order:
        cell = rows[(profile, test)]
        label = f"{profile} / {test}"
        medians: dict[str, float] = {}
        cvs: dict[str, float] = {}
        for server in ("shardkv", "redis"):
            metrics = cell.get(server)
            if not metrics or not metrics["rps"]:
                continue
            rps = metrics["rps"]
            medians[server] = statistics.median(rps)
            cvs[server] = cv(rps)
            p50 = statistics.median(metrics["p50"]) if metrics["p50"] else float("nan")
            p99 = statistics.median(metrics["p99"]) if metrics["p99"] else float("nan")
            print(
                f"{label:<26} {server:<8} {medians[server]:>12,.0f} {cvs[server]:>6.1f}% "
                f"{p50:>8.3f} {p99:>8.3f}   "
                f"{min(rps):,.0f} - {max(rps):,.0f}"
            )
            label = ""

        if "shardkv" in medians and "redis" in medians:
            worst_cv = max(cvs.values())
            if worst_cv > limit:
                noisy.append(f"{profile}/{test} (CV {worst_cv:.1f}%)")
                print(f"{'':<26} {'':8} {'ratio withheld: CV ' + f'{worst_cv:.1f}%':>12}")
            else:
                ratio = medians["shardkv"] / medians["redis"]
                if ratio >= 1:
                    verdict = f"shardkv {ratio:.2f}x redis"
                else:
                    verdict = f"redis {1 / ratio:.2f}x shardkv"
                verdicts.append((f"{profile}/{test}", verdict))
                print(f"{'':<26} {'':8} {verdict:>12}")
        print()

    if verdicts:
        print("Comparable results (CV within the limit):")
        wins = [v for v in verdicts if v[1].startswith("shardkv")]
        losses = [v for v in verdicts if v[1].startswith("redis")]
        # Losses first, deliberately: a table that leads with its wins is
        # advertising, not evidence.
        for name, verdict in losses:
            print(f"  {name:<32} {verdict}   <- shardkv slower")
        for name, verdict in wins:
            print(f"  {name:<32} {verdict}")
        print()

    if noisy:
        print("NOT COMPARABLE on this host -- the repetitions disagreed:")
        for entry in noisy:
            print(f"  {entry}")
        print()
        print("Nothing in this run should be quoted as a comparison. Re-run on a Linux")
        print("host with Docker running natively, no other tenants, a fixed CPU governor")
        print("and REPS>=5, and check these coefficients again before reading the means.")
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
