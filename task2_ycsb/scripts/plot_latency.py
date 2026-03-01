#!/usr/bin/env python3
import csv
import sys
import matplotlib.pyplot as plt

if len(sys.argv) != 3:
    print(f"usage: {sys.argv[0]} <input.csv> <output.png>")
    sys.exit(1)

rows = []
with open(sys.argv[1]) as f:
    r = csv.DictReader(f)
    for row in r:
        rows.append(row)

metrics = {row['metric']: float(row['value']) for row in rows}
labels = ['p95_us', 'p99_us']
values = [metrics.get('p95_us', 0.0), metrics.get('p99_us', 0.0)]

plt.figure(figsize=(6,4))
plt.bar(labels, values)
plt.ylabel('Latency (microseconds)')
plt.title('YCSB tail latency')
plt.tight_layout()
plt.savefig(sys.argv[2], dpi=160)
