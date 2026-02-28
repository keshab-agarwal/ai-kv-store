#!/usr/bin/env python3
"""
Plot YCSB latency distributions from CSV output.

Usage:
    python3 plot_latency.py ycsb-results/run_latencies.csv [output_dir]
"""
import sys
import os
import csv
from collections import defaultdict

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <latencies.csv> [output_dir]", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(csv_path) or "."

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib/numpy not installed. Install with: pip install matplotlib numpy", file=sys.stderr)
        print("Falling back to text summary.", file=sys.stderr)
        text_summary(csv_path)
        return

    latencies = defaultdict(list)
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            op = row["operation"]
            us = float(row["latency_us"])
            latencies[op].append(us / 1000.0)  # convert to ms

    fig, axes = plt.subplots(1, len(latencies), figsize=(6 * len(latencies), 5), squeeze=False)

    for idx, (op, lats) in enumerate(sorted(latencies.items())):
        ax = axes[0][idx]
        lats_arr = np.array(lats)

        p50 = np.percentile(lats_arr, 50)
        p95 = np.percentile(lats_arr, 95)
        p99 = np.percentile(lats_arr, 99)

        ax.hist(lats_arr, bins=100, alpha=0.7, color="steelblue", edgecolor="black", linewidth=0.3)
        ax.axvline(p50, color="green", linestyle="--", label=f"p50={p50:.2f}ms")
        ax.axvline(p95, color="orange", linestyle="--", label=f"p95={p95:.2f}ms")
        ax.axvline(p99, color="red", linestyle="--", label=f"p99={p99:.2f}ms")
        ax.set_title(f"{op} Latency (n={len(lats)})")
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("Count")
        ax.legend(fontsize=8)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "latency_histogram.png")
    plt.savefig(out_path, dpi=150)
    print(f"Histogram saved to {out_path}")

    # CDF plot
    fig2, axes2 = plt.subplots(1, len(latencies), figsize=(6 * len(latencies), 5), squeeze=False)
    for idx, (op, lats) in enumerate(sorted(latencies.items())):
        ax = axes2[0][idx]
        lats_sorted = np.sort(lats)
        cdf = np.arange(1, len(lats_sorted) + 1) / len(lats_sorted)
        ax.plot(lats_sorted, cdf, color="steelblue", linewidth=1.5)
        ax.axhline(0.95, color="orange", linestyle=":", alpha=0.7, label="p95")
        ax.axhline(0.99, color="red", linestyle=":", alpha=0.7, label="p99")
        ax.set_title(f"{op} Latency CDF")
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("Cumulative Fraction")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    cdf_path = os.path.join(out_dir, "latency_cdf.png")
    plt.savefig(cdf_path, dpi=150)
    print(f"CDF saved to {cdf_path}")


def text_summary(csv_path):
    latencies = defaultdict(list)
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            latencies[row["operation"]].append(float(row["latency_us"]) / 1000.0)

    for op, lats in sorted(latencies.items()):
        lats.sort()
        n = len(lats)
        print(f"\n{op}: n={n}")
        print(f"  min={lats[0]:.3f}ms  max={lats[-1]:.3f}ms  mean={sum(lats)/n:.3f}ms")
        print(f"  p50={lats[int(n*0.50)]:.3f}ms  p95={lats[int(n*0.95)]:.3f}ms  p99={lats[int(n*0.99)]:.3f}ms")


if __name__ == "__main__":
    main()
