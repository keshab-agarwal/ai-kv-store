#!/usr/bin/env python3
"""
Compare YCSB benchmark results between the custom KV store and Redis.

Usage:
    python3 compare.py <kvstore_run_report.json> <redis_run_report.json> [output_dir]

Produces:
    - A text table printed to stdout
    - comparison_report.json with structured deltas
    - comparison_chart.png (if matplotlib is available)
"""
import sys
import os
import json


def load_report(path):
    with open(path) as f:
        return json.load(f)


def pct_diff(a, b):
    """Percentage difference: positive means b is worse (higher latency / lower throughput)."""
    if a == 0 and b == 0:
        return 0.0
    if a == 0:
        return float("inf")
    return ((b - a) / abs(a)) * 100.0


def format_pct(val):
    if val == float("inf"):
        return "  N/A"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.1f}%"


def print_header(kv_path, redis_path):
    print()
    print("=" * 78)
    print("  YCSB Benchmark Comparison: Custom KV Store vs Redis")
    print("=" * 78)
    print(f"  KV Store report : {kv_path}")
    print(f"  Redis report    : {redis_path}")
    print()


def compare_throughput(kv, redis):
    kv_tp = kv.get("throughput_ops_sec", 0)
    redis_tp = redis.get("throughput_ops_sec", 0)
    delta = pct_diff(redis_tp, kv_tp)

    print("  Throughput (ops/sec)")
    print(f"    {'':20s} {'KV Store':>12s} {'Redis':>12s} {'Delta':>10s}")
    print(f"    {'ops/sec':20s} {kv_tp:>12.0f} {redis_tp:>12.0f} {format_pct(delta):>10s}")
    print()
    return {
        "kvstore_ops_sec": kv_tp,
        "redis_ops_sec": redis_tp,
        "delta_pct": round(delta, 2),
    }


def compare_latency(kv, redis):
    kv_ops = kv.get("per_operation", {})
    redis_ops = redis.get("per_operation", {})
    all_ops = sorted(set(list(kv_ops.keys()) + list(redis_ops.keys())))

    metrics = ["p50_ms", "p95_ms", "p99_ms", "p999_ms", "mean_ms"]
    labels = ["p50", "p95", "p99", "p99.9", "mean"]

    result = {}

    print("  Latency (ms) — lower is better")
    print(f"    {'':20s} {'KV Store':>12s} {'Redis':>12s} {'Delta':>10s}")
    print("    " + "-" * 56)

    for op in all_ops:
        kv_stats = kv_ops.get(op, {})
        redis_stats = redis_ops.get(op, {})
        result[op] = {}

        for metric, label in zip(metrics, labels):
            kv_val = kv_stats.get(metric, 0)
            redis_val = redis_stats.get(metric, 0)
            delta = pct_diff(redis_val, kv_val)
            result[op][metric] = {
                "kvstore": round(kv_val, 3),
                "redis": round(redis_val, 3),
                "delta_pct": round(delta, 2),
            }
            tag = f"{op} {label}"
            print(f"    {tag:20s} {kv_val:>12.3f} {redis_val:>12.3f} {format_pct(delta):>10s}")

        print()

    return result


def write_chart(kv, redis, out_dir):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        return

    kv_ops = kv.get("per_operation", {})
    redis_ops = redis.get("per_operation", {})
    all_ops = sorted(set(list(kv_ops.keys()) + list(redis_ops.keys())))

    metrics = ["p50_ms", "p95_ms", "p99_ms"]
    labels = ["p50", "p95", "p99"]

    fig, axes = plt.subplots(1, len(all_ops), figsize=(7 * len(all_ops), 5), squeeze=False)

    for i, op in enumerate(all_ops):
        ax = axes[0][i]
        kv_stats = kv_ops.get(op, {})
        redis_stats = redis_ops.get(op, {})

        kv_vals = [kv_stats.get(m, 0) for m in metrics]
        redis_vals = [redis_stats.get(m, 0) for m in metrics]

        x = np.arange(len(labels))
        width = 0.35
        ax.bar(x - width / 2, kv_vals, width, label="KV Store", color="steelblue", alpha=0.85)
        ax.bar(x + width / 2, redis_vals, width, label="Redis", color="#e74c3c", alpha=0.85)

        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"{op}")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()
        ax.grid(axis="y", alpha=0.3)

    plt.suptitle("KV Store vs Redis — Latency Comparison", fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    chart_path = os.path.join(out_dir, "comparison_chart.png")
    plt.savefig(chart_path, dpi=150)
    print(f"  Chart saved to {chart_path}")


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <kvstore_report.json> <redis_report.json> [output_dir]", file=sys.stderr)
        sys.exit(1)

    kv_path = sys.argv[1]
    redis_path = sys.argv[2]
    out_dir = sys.argv[3] if len(sys.argv) > 3 else "."

    kv = load_report(kv_path)
    redis_report = load_report(redis_path)

    print_header(kv_path, redis_path)

    tp_result = compare_throughput(kv, redis_report)
    lat_result = compare_latency(kv, redis_report)

    result = {
        "kvstore_report": kv_path,
        "redis_report": redis_path,
        "throughput": tp_result,
        "latency": lat_result,
    }

    os.makedirs(out_dir, exist_ok=True)
    report_path = os.path.join(out_dir, "comparison_report.json")
    with open(report_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  Comparison JSON written to {report_path}")

    write_chart(kv, redis_report, out_dir)
    print()


if __name__ == "__main__":
    main()
