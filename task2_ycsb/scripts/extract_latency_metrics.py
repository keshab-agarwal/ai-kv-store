#!/usr/bin/env python3
import re
import sys
from pathlib import Path

if len(sys.argv) != 2:
    print(f"usage: {sys.argv[0]} <ycsb_output.txt>", file=sys.stderr)
    sys.exit(1)

text = Path(sys.argv[1]).read_text(errors="ignore")
throughput = re.findall(r"\[OVERALL\], Throughput\(ops/sec\), ([0-9.]+)", text)
p95 = re.findall(r"95thPercentileLatency\(us\), ([0-9.]+)", text)
p99 = re.findall(r"99thPercentileLatency\(us\), ([0-9.]+)", text)

print("metric,value")
if throughput:
    print(f"throughput_ops_sec,{throughput[-1]}")
if p95:
    print(f"p95_us,{p95[-1]}")
if p99:
    print(f"p99_us,{p99[-1]}")

if not throughput and not p95 and not p99:
    print("warn,no_matching_metrics_found", file=sys.stderr)
    print("hint,check ycsb_run.out for startup/build errors", file=sys.stderr)
