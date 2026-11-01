#!/usr/bin/env python3
"""Builds the README result tables from bench/results/*.jsonl.

    python3 bench/tables.py          # rewrite the generated blocks in README.md
    python3 bench/tables.py --check  # exit 1 if README.md is out of date (CI)

A block is everything between <!-- generated:NAME --> and <!-- /generated -->.
"""
import json
import re
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"


def load(name):
    with open(RESULTS / name) as f:
        return [json.loads(line) for line in f if line.strip()]


def n(x):
    return f"{round(x):,}"


def span(values, fmt=n, unit=""):
    lo, hi = fmt(min(values)), fmt(max(values))
    return (lo if lo == hi else f"{lo}–{hi}") + unit


def sec(ms):
    return f"{ms / 1000:.1f}"


def kilo(x):
    return f"{x / 1000:.1f}k"


def by(rows, *keys):
    groups = {}
    for r in rows:
        groups.setdefault(tuple(r[k] for k in keys), []).append(r)
    return groups


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def latency():
    data = load("latency.jsonl")
    groups = by([r for r in data if "target_rate" in r], "target_rate")
    rec = [r for r in data if "es_live_docs" in r]
    rows, pipe_p50 = [], []
    for (rate,), g in sorted(groups.items()):
        rows.append([
            n(rate),
            span([r["achieved_rate"] for r in g]),
            n(statistics.median(r["p50_ms"] for r in g)) + " ms",
            n(statistics.median(r["p90_ms"] for r in g)) + " ms",
            span([r["p99_ms"] for r in g], unit=" ms"),
            sec(max(r["max_ms"] for r in g)) + " s",
            f"{sum(r['samples'] for r in g):,}, {sum(r['not_found'] for r in g):,}",
        ])
        pipe_p50.append(statistics.median(r["pipeline_p50_ms"] for r in g))
    runs = sorted({len(g) for g in groups.values()})
    pipe_p99 = [r["pipeline_p99_ms"] for g in groups.values() for r in g]
    return "\n".join([
        table(["target writes/s", "achieved", "p50", "p90", f"p99 ({'/'.join(map(str, runs))} runs)", "max",
               "probes, not found"], rows),
        "",
        f"p50/p90 are medians of the runs. Pipeline part (WAL message → bulk acknowledged, indexer histogram):"
        f" p50 {span(pipe_p50, unit=' ms')} (median per rate), p99 {span(pipe_p99, sec, ' s')} across runs."
        + (f" Reconcile after the series: {rec[-1]['missing'] + rec[-1]['stale'] + rec[-1]['extra']} mismatches"
           f" on {rec[-1].get('pg_rows', rec[-1].get('source_rows')):,} rows." if rec else ""),
    ])


CONNECTORS = {"gopqcdc": "go-pq-cdc-kafka v1.4.5", "debezium": "Debezium 3.2 (Kafka Connect)"}


def catchup():
    groups = by(load("catchup.jsonl"), "cpus", "connector")
    order = sorted(groups, key=lambda k: (int(k[0]), k[1] != "gopqcdc"))
    rows = []
    for cpus, conn in order:
        g = groups[(cpus, conn)]
        t = sorted(r["elapsed_s"] for r in g)
        rows.append([
            f"{cpus} CPU, {g[0]['mem_limit'][:-1]} {g[0]['mem_limit'][-1].upper()}B",
            CONNECTORS.get(conn, conn),
            " / ".join(f"{x:.1f}" for x in t) + f" s ({len(t)} run{'s' * (len(t) > 1)})",
            f"{statistics.median(r['rows_per_s'] for r in g) / 1000:.0f}k",
            span([r["cpu_s"] for r in g]),
            span([r["peak_anon_mb"] for r in g], unit=" MB"),
        ])

    def ratio(cpus, key):
        a = statistics.median(r[key] for r in groups[(cpus, "debezium")])
        b = statistics.median(r[key] for r in groups[(cpus, "gopqcdc")])
        return f"{a / b:.1f}×"

    cpus = sorted({c for c, _ in groups}, key=int)
    lines = [f"At {cpus[0]} CPU go-pq-cdc-kafka is {ratio(cpus[0], 'elapsed_s')} faster with"
             f" {ratio(cpus[0], 'cpu_s')} less CPU and {ratio(cpus[0], 'peak_anon_mb')} less memory"]
    lines += [f"at {c} CPUs {ratio(c, 'elapsed_s')} faster" for c in cpus[1:]]
    return table(["limits", "connector", "time", "rows/s (median)", "CPU-seconds", "peak anon memory"], rows) + \
        "\n\n" + "; ".join(lines) + " (ratios of medians)."


SCENARIOS = {
    "kill-indexer": "kill -9 indexer, back after 10 s",
    "kill-indexer-3x": "kill -9 indexer three times",
    "kill-cdc": "kill -9 cdc connector, 10 s",
    "kill-broker": "kill -9 Redpanda (the broker), 20 s",
    "restart-postgres": "restart PostgreSQL",
    "es-429-every-2nd": "gaos: 429 on every 2nd `_bulk`",
    "es-429-60s": "gaos: 429 on every `_bulk` for 60 s",
    "es-429-60s-kill-indexer": "… plus kill -9 indexer inside the window",
    "es-429-60s-kill-in-drain": "… plus kill -9 while cronsumer drains",
}


def chaos():
    data = load("chaos.jsonl")
    groups = by(data, "scenario")
    rows = []
    for (s,), g in groups.items():
        rec = [r["reconcile"] for r in g]
        label = SCENARIOS.get(s, s) + (f" ({len(g)} runs)" if len(g) > 1 else "")
        bad = [sum(x[k] for x in rec) for k in ("missing", "stale", "extra")]
        ok = all(r["result"] == "ok" for r in g)
        rows.append([
            label if ok else label + " **(failed)**",
            span([r["retry_topic_msgs"] for r in g]),
            span([r["dlq_msgs"] for r in g]),
            " / ".join(map(str, bad)),
            span([r["converge_s"] for r in g], unit=" s"),
        ])
    sizes = [x.get("pg_rows", x.get("source_rows")) for x in (r["reconcile"] for r in data)]
    passes = [float(r["reconcile"]["elapsed"].rstrip("s")) for r in data]
    return table(["scenario", "through retry topic", "DLQ", "missing / stale / extra", "converged in"], rows) + \
        f"\n\n{span(sizes, lambda x: f'{x / 1e6:.2f}')}M rows at the time of the checks; one full reconcile" \
        f" pass took {span(passes, unit=' s')}."


def couchbase():
    data = load("couchbase.jsonl")
    runs = [r for r in data if "target_rate" in r]
    rec = [r for r in data if "source_rows" in r]
    rows = []
    for (rate,), g in sorted(by(runs, "target_rate").items()):
        rows.append([
            n(rate),
            span([r["achieved_rate"] for r in g]),
            span([r["p50_ms"] for r in g], sec, " s"),
            span([r["p99_ms"] for r in g], sec, " s"),
            f"{span([r['dcp_lag_mean'] for r in g], kilo)} / {span([r['dcp_lag_max'] for r in g], kilo)}",
        ])
    out = table(["target", "achieved", "p50", "p99", "DCP lag mean / max (mutations)"], rows)
    if rec:
        r = rec[-1]
        out += (f"\n\nReconcile afterwards: {r['source_rows']:,} documents,"
                f" {r['missing'] + r['stale'] + r['extra']} mismatches."
                f" Highest achieved N1QL write rate: {n(max(x['achieved_rate'] for x in runs))}/s.")
    return out


BLOCKS = {"latency": latency, "catchup": catchup, "chaos": chaos, "couchbase": couchbase}


def render(readme):
    def sub(m):
        return f"<!-- generated:{m[1]} -->\n{BLOCKS[m[1]]()}\n<!-- /generated -->"
    return re.sub(r"<!-- generated:(\w+) -->\n.*?<!-- /generated -->", sub, readme, flags=re.S)


def main():
    path = ROOT / "README.md"
    old = path.read_text()
    new = render(old)
    if "--check" in sys.argv:
        if new != old:
            sys.exit("README.md tables are out of date: run `make tables`")
        return
    path.write_text(new)


if __name__ == "__main__":
    main()
