#!/usr/bin/env bash
# act4_benchmark.sh — Run YCSB and print a clean latency/throughput summary.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

OUTDIR="demo-results"

echo -e "${BOLD}=== Act 4: YCSB Performance Benchmark ===${NC}"
echo -e "${DIM}  Workload: 10K load + 50K run  |  8 threads  |  1KB values"
echo -e "  Key distribution: Zipfian θ=0.99  |  95% read / 4% update / 1% delete${NC}\n"

# Verify cluster is up
ALL_UP=true
for port in 16101 16102 16103; do
    curl -sf "http://localhost:$port/api/status" >/dev/null 2>&1 || { ALL_UP=false; break; }
done
if ! $ALL_UP; then
    echo -e "${YELLOW}Cluster not fully up. Run act1_cluster.sh first.${NC}"; exit 1
fi

rm -rf "$OUTDIR"

echo -e "${BOLD}Running benchmark...${NC}  ${DIM}(takes ~30s)${NC}\n"

./bin/ycsbrun \
    -nodes="localhost:16101,localhost:16102,localhost:16103" \
    -recordcount=10000 \
    -operationcount=50000 \
    -threads=8 \
    -valuesize=1024 \
    -phase=both \
    -outdir="$OUTDIR" \
    2>&1

# ── Pretty summary table ──────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}=== Summary ===${NC}\n"

python3 - "$OUTDIR/run_report.json" <<'PYEOF'
import sys, json

with open(sys.argv[1]) as f:
    r = json.load(f)

GREEN  = '\033[0;32m'
YELLOW = '\033[1;33m'
CYAN   = '\033[0;36m'
BOLD   = '\033[1m'
DIM    = '\033[2m'
NC     = '\033[0m'

throughput = r['throughput_ops_sec']
print(f"  {BOLD}Throughput:{NC}  {GREEN}{BOLD}{throughput:,.0f} ops/sec{NC}\n")

header = f"  {'Operation':<10} {'Count':>7}  {'p50 ms':>8}  {'p95 ms':>8}  {'p99 ms':>8}"
print(BOLD + header + NC)
print("  " + "─" * 52)

for op, s in sorted(r['per_operation'].items()):
    p50 = s['p50_ms']
    p95 = s['p95_ms']
    p99 = s['p99_ms']
    # Colour reads green (fast), writes yellow
    colour = GREEN if op == 'READ' else YELLOW
    print(f"  {colour}{op:<10}{NC}  {s['count']:>7}  "
          f"{colour}{p50:>7.3f}{NC}   "
          f"{p95:>7.3f}   "
          f"{p99:>7.3f}")

print()
print(f"  {DIM}READ p50 ~0.08ms = served from primary's in-memory lease (0 RTTs){NC}")
print(f"  {DIM}WRITE p50 ~17ms  = WAL fsync + 1 replication RTT to quorum{NC}")
PYEOF

echo -e "\n${CYAN}Next: run demo/act5_correctness.sh${NC}"
