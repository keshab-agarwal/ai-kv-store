#!/usr/bin/env bash
# act5_correctness.sh — Run Porcupine linearizability check with fault injection.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

OUTDIR="demo-harness"

echo -e "${BOLD}=== Act 5: Linearizability (Porcupine) ===${NC}"
echo -e "${DIM}  3-node cluster  |  4 workers  |  30s  |  1 node crash + recovery${NC}\n"

echo -e "  ${BOLD}What this test does:${NC}"
echo -e "  ${DIM}1. Starts a fresh 3-node cluster${NC}"
echo -e "  ${DIM}2. Runs concurrent reads/writes from 4 goroutines${NC}"
echo -e "  ${DIM}3. At 25–75% of runtime, crashes one node then recovers it${NC}"
echo -e "  ${DIM}4. Records every operation's call time, return time, and result${NC}"
echo -e "  ${DIM}5. Feeds the full history to Porcupine — checks every possible${NC}"
echo -e "  ${DIM}   linearization order exhaustively${NC}\n"

rm -rf "$OUTDIR"

echo -e "${BOLD}Running...${NC}  ${DIM}(takes ~45s)${NC}\n"

if ./bin/harness \
    -bin=./bin/kvnode \
    -nodes=3 \
    -duration=30s \
    -workers=4 \
    -faults \
    -seed=42 \
    -checktimeout=120s \
    -outdir="$OUTDIR" 2>&1; then
    VERDICT="PASS"
else
    VERDICT="FAIL"
fi

echo ""

# ── Parse and display summary.json ────────────────────────────────────────────
if [ -f "$OUTDIR/summary.json" ]; then
    python3 - "$OUTDIR/summary.json" "$VERDICT" <<'PYEOF'
import sys, json

with open(sys.argv[1]) as f:
    s = json.load(f)

verdict = sys.argv[2]
GREEN  = '\033[0;32m'
RED    = '\033[0;31m'
YELLOW = '\033[1;33m'
BOLD   = '\033[1m'
DIM    = '\033[2m'
NC     = '\033[0m'

colour = GREEN if verdict == 'PASS' else RED
print(f"  {'─'*48}")
print(f"  {BOLD}Total ops:{NC}       {s['total_ops']:,}")
print(f"  {BOLD}Errors:{NC}          {s['errors']}")
print(f"  {BOLD}Timeouts:{NC}        {s['timeouts']}")
print(f"  {BOLD}Fault events:{NC}    {s['fault_events']}")
print(f"  {BOLD}Linearizable:{NC}    {GREEN+'Yes' if s['linearizable'] else RED+'No'}{NC}")
print(f"  {BOLD}Checker time:{NC}    {s['check_duration_ms']}ms")
print(f"  {'─'*48}")
print(f"\n  {colour}{BOLD}{'✓ PASS' if verdict == 'PASS' else '✗ FAIL'}{NC}")

if verdict == 'PASS':
    print(f"\n  {DIM}Porcupine verified that every read/write, including those across")
    print(f"  the crash window, is consistent with some legal sequential execution")
    print(f"  of a key-value store. The crash was transparent to clients.{NC}")
PYEOF
fi

echo -e "\n${CYAN}Demo complete. Run demo/teardown.sh to clean up.${NC}"
