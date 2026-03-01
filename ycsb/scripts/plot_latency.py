#!/usr/bin/env python3
"""
Plot latency histogram and CDF from a YCSB run_latencies.csv file.

Usage:
    python plot_latency.py <input_csv> <output_dir>

Produces:
    <output_dir>/latency_histogram.png
    <output_dir>/latency_cdf.png
"""

import csv
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")  # headless backend
import matplotlib.pyplot as plt
import numpy as np


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input_csv> <output_dir>", file=sys.stderr)
        sys.exit(1)

    input_csv = sys.argv[1]
    output_dir = sys.argv[2]
    os.makedirs(output_dir, exist_ok=True)

    # Read latencies grouped by operation type.
    latencies = defaultdict(list)  # op -> list of latency_us
    with open(input_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            op = row["operation"]
            lat = float(row["latency_us"])
            latencies[op].append(lat)

    if not latencies:
        print("No data found in CSV.", file=sys.stderr)
        sys.exit(1)

    ops = sorted(latencies.keys())

    # ---- Histogram ----
    fig, ax = plt.subplots(figsize=(10, 6))
    for op in ops:
        vals = np.array(latencies[op]) / 1000.0  # convert to ms
        ax.hist(vals, bins=100, alpha=0.6, label=op)
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Count")
    ax.set_title("Latency Histogram by Operation Type")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "latency_histogram.png"), dpi=150)
    plt.close(fig)
    print(f"Wrote {os.path.join(output_dir, 'latency_histogram.png')}")

    # ---- CDF ----
    fig, ax = plt.subplots(figsize=(10, 6))
    for op in ops:
        vals = np.sort(np.array(latencies[op]) / 1000.0)
        cdf = np.arange(1, len(vals) + 1) / float(len(vals))
        ax.plot(vals, cdf, label=op)
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title("Latency CDF by Operation Type")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "latency_cdf.png"), dpi=150)
    plt.close(fig)
    print(f"Wrote {os.path.join(output_dir, 'latency_cdf.png')}")


if __name__ == "__main__":
    main()
