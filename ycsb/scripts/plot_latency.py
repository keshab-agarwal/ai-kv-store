#!/usr/bin/env python3
"""
plot_latency.py — Generate latency histogram and CDF plots from a YCSB latency CSV.

Usage:
    python3 plot_latency.py <latencies.csv> <outdir>

Input CSV format:
    op,latency_us,status
    READ,123,found
    UPDATE,456,ok

Output:
    <outdir>/latency_histogram.png  — per-operation latency histogram (log x-axis)
    <outdir>/latency_cdf.png        — per-operation latency CDF

If matplotlib is not installed, prints a message and exits with code 0.
"""

import sys
import csv
import os
import collections


def read_csv(path):
    """Read the latency CSV and return a dict of op -> list of latency_us (float)."""
    data = collections.defaultdict(list)
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            op = row.get('op', '').strip()
            try:
                lat = float(row.get('latency_us', 0))
            except (ValueError, TypeError):
                continue
            if op:
                data[op].append(lat)
    return dict(data)


def percentile(values, p):
    """Return the p-th percentile of values (p in [0, 100])."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * p / 100.0)
    if idx >= len(sorted_vals):
        idx = len(sorted_vals) - 1
    return sorted_vals[idx]


def plot_histogram(data, outdir):
    """Generate a latency histogram (log x-axis, microseconds) for each operation."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    ops = sorted(data.keys())

    for i, op in enumerate(ops):
        lats = data[op]
        if not lats:
            continue
        color = colors[i % len(colors)]

        # Use log-spaced bins from 1 µs to max latency.
        min_lat = max(1.0, min(lats))
        max_lat = max(lats)
        if max_lat <= min_lat:
            max_lat = min_lat * 10

        bins = np.logspace(
            np.log10(min_lat),
            np.log10(max_lat),
            num=50
        )
        ax.hist(lats, bins=bins, alpha=0.6, label=op, color=color, density=False)

    ax.set_xscale('log')
    ax.set_xlabel('Latency (µs)', fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_title('Operation Latency Histogram', fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(True, which='both', linestyle='--', alpha=0.4)

    # Annotate p99 for each op.
    for i, op in enumerate(ops):
        lats = data[op]
        if not lats:
            continue
        p99 = percentile(lats, 99)
        ax.axvline(x=p99, color=colors[i % len(colors)],
                   linestyle=':', linewidth=1.5,
                   label=f'{op} p99={p99/1000:.1f}ms')

    ax.legend(fontsize=9)

    out_path = os.path.join(outdir, 'latency_histogram.png')
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


def plot_cdf(data, outdir):
    """Generate a CDF of latencies (log x-axis, microseconds) for each operation."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    ops = sorted(data.keys())

    for i, op in enumerate(ops):
        lats = sorted(data[op])
        if not lats:
            continue
        color = colors[i % len(colors)]

        n = len(lats)
        cdf_y = np.arange(1, n + 1) / float(n)
        ax.plot(lats, cdf_y, label=op, color=color, linewidth=1.8)

    ax.set_xscale('log')
    ax.set_xlabel('Latency (µs)', fontsize=12)
    ax.set_ylabel('CDF', fontsize=12)
    ax.set_title('Operation Latency CDF', fontsize=14)
    ax.set_ylim(0, 1.05)
    ax.axhline(y=0.50, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
    ax.axhline(y=0.95, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
    ax.axhline(y=0.99, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
    ax.axhline(y=0.999, color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
    ax.text(ax.get_xlim()[0] if ax.get_xlim()[0] > 0 else 1,
            0.501, ' p50', fontsize=8, color='gray', va='bottom')
    ax.text(ax.get_xlim()[0] if ax.get_xlim()[0] > 0 else 1,
            0.951, ' p95', fontsize=8, color='gray', va='bottom')
    ax.text(ax.get_xlim()[0] if ax.get_xlim()[0] > 0 else 1,
            0.991, ' p99', fontsize=8, color='gray', va='bottom')
    ax.legend(fontsize=10)
    ax.grid(True, which='both', linestyle='--', alpha=0.4)

    out_path = os.path.join(outdir, 'latency_cdf.png')
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


def print_summary(data):
    """Print a text summary of latency statistics."""
    ops = sorted(data.keys())
    print(f"\n{'Op':<10} {'Count':>8} {'p50 ms':>10} {'p95 ms':>10} {'p99 ms':>10} {'p999 ms':>10}")
    print('-' * 60)
    for op in ops:
        lats = data[op]
        if not lats:
            continue
        p50 = percentile(lats, 50) / 1000.0
        p95 = percentile(lats, 95) / 1000.0
        p99 = percentile(lats, 99) / 1000.0
        p999 = percentile(lats, 99.9) / 1000.0
        print(f"{op:<10} {len(lats):>8} {p50:>10.2f} {p95:>10.2f} {p99:>10.2f} {p999:>10.2f}")
    print()


def main():
    if len(sys.argv) < 3:
        print("Usage: plot_latency.py <csv> <outdir>")
        sys.exit(1)

    csv_path = sys.argv[1]
    outdir = sys.argv[2]

    if not os.path.isfile(csv_path):
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(outdir, exist_ok=True)

    data = read_csv(csv_path)
    if not data:
        print("No data found in CSV.", file=sys.stderr)
        sys.exit(1)

    print_summary(data)

    try:
        import matplotlib  # noqa: F401
    except ImportError:
        print("matplotlib not available, skipping plots")
        sys.exit(0)

    plot_histogram(data, outdir)
    plot_cdf(data, outdir)


if __name__ == '__main__':
    main()
