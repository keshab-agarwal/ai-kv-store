#!/usr/bin/env bash
#
# evaluate.sh — Evaluation harness for the distributed KV store.
#
# Runs build, vet, self-test, Porcupine (no faults + faults), YCSB,
# TLC (normal + bug mode), and test coverage. Writes results to a
# timestamped directory under evaluation/runs/.
#
# Usage:
#   ./evaluation/evaluate.sh [run-name]
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_NAME="${1:-run-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="$SCRIPT_DIR/runs/$RUN_NAME"

mkdir -p "$RUN_DIR"

YCSB_RPC_BASE=17001
YCSB_HTTP_BASE=16001

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
NC='\033[0m'

pass_count=0
fail_count=0
skip_count=0

SCORES_FILE="$RUN_DIR/scores.tmp"
: > "$SCORES_FILE"

score_set() {
    echo "$1=$2" >> "$SCORES_FILE"
}

score_get() {
    grep "^$1=" "$SCORES_FILE" 2>/dev/null | tail -1 | cut -d= -f2-
}

record() {
    local name="$1" result="$2" detail="${3:-}"
    score_set "$name" "$result"
    if [ "$result" = "PASS" ]; then
        echo -e "  ${GREEN}✓ PASS${NC}  $name ${detail:+($detail)}"
        pass_count=$((pass_count + 1))
    elif [ "$result" = "FAIL" ]; then
        echo -e "  ${RED}✗ FAIL${NC}  $name ${detail:+($detail)}"
        fail_count=$((fail_count + 1))
    elif [ "$result" = "SKIP" ]; then
        echo -e "  ${YELLOW}○ SKIP${NC}  $name ${detail:+($detail)}"
        skip_count=$((skip_count + 1))
    fi
}

cleanup_ycsb_cluster() {
    for pid_file in "$RUN_DIR"/ycsb_node_*.pid; do
        [ -f "$pid_file" ] && kill "$(cat "$pid_file")" 2>/dev/null || true
    done
    rm -f "$RUN_DIR"/ycsb_node_*.pid
    sleep 1
}

cleanup_ycsb_data() {
    # YCSB nodes use ./data-<id> in the repo root when no -data flag is given.
    # Remove them so stale WAL data from previous runs doesn't interfere.
    rm -rf "$REPO_ROOT/data-1" "$REPO_ROOT/data-2" "$REPO_ROOT/data-3"
}

kill_stale_kvnodes() {
    pkill -f "kvnode -id=" 2>/dev/null || true
    sleep 1
}

trap 'kill_stale_kvnodes; cleanup_ycsb_cluster; cleanup_ycsb_data' EXIT

cd "$REPO_ROOT"

# Ensure no stale kvnode processes from previous runs
kill_stale_kvnodes

echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  KV Store Evaluation Harness${NC}"
echo -e "${BOLD}  Run: $RUN_NAME${NC}"
echo -e "${BOLD}  Output: $RUN_DIR${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""

# ─────────────────────────────────────────────────
# Phase 1: Build
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 1: Build${NC}"

build_ok=true
go build -o bin/kvnode ./kvstore/cmd/kvnode/ > "$RUN_DIR/build.log" 2>&1 || build_ok=false
go build -o bin/ycsbrun ./ycsb/cmd/ycsbrun/ >> "$RUN_DIR/build.log" 2>&1 || build_ok=false
go build -o bin/harness ./correctness/cmd/harness/ >> "$RUN_DIR/build.log" 2>&1 || build_ok=false
go build -o bin/checker ./correctness/cmd/checker/ >> "$RUN_DIR/build.log" 2>&1 || build_ok=false

if $build_ok; then
    record "go_build" "PASS" "all 4 binaries"
else
    record "go_build" "FAIL" "see build.log"
    echo -e "  ${RED}Build failed — cannot continue.${NC}"
    cat "$RUN_DIR/build.log"
    exit 1
fi

# ─────────────────────────────────────────────────
# Phase 2: Vet
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 2: Vet${NC}"

if go vet ./... > "$RUN_DIR/vet.log" 2>&1; then
    record "go_vet" "PASS"
else
    record "go_vet" "FAIL" "see vet.log"
fi

# ─────────────────────────────────────────────────
# Phase 3: Porcupine self-test
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 3: Porcupine Self-Test${NC}"

if ./bin/checker -selftest > "$RUN_DIR/selftest.log" 2>&1; then
    record "porcupine_selftest" "PASS"
else
    record "porcupine_selftest" "FAIL" "see selftest.log"
fi

# ─────────────────────────────────────────────────
# Phase 4: Porcupine — no faults
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 4: Porcupine Harness (no faults, 15s)${NC}"

harness_nofault_dir="$RUN_DIR/harness-nofault"
if ./bin/harness -bin=./bin/kvnode -nodes=3 -duration=15s -workers=4 \
   -checktimeout=120s -outdir="$harness_nofault_dir" > "$RUN_DIR/harness-nofault.log" 2>&1; then
    record "porcupine_nofault" "PASS"
else
    record "porcupine_nofault" "FAIL" "see harness-nofault.log"
fi

if [ -f "$harness_nofault_dir/summary.json" ]; then
    python3 -c "
import json, sys
d = json.load(open('$harness_nofault_dir/summary.json'))
print('    ops=%d errors=%d timeouts=%d' % (d.get('total_ops',0), d.get('errors',0), d.get('timeouts',0)))
" 2>/dev/null || true

    python3 -c "
import json
d = json.load(open('$harness_nofault_dir/summary.json'))
with open('$SCORES_FILE', 'a') as f:
    f.write('nofault_ops=%d\n' % d.get('total_ops',0))
    f.write('nofault_errors=%d\n' % d.get('errors',0))
    f.write('nofault_timeouts=%d\n' % d.get('timeouts',0))
" 2>/dev/null || true
fi

# Kill any leftover nodes from Phase 4
kill_stale_kvnodes

# ─────────────────────────────────────────────────
# Phase 5: Porcupine — with faults
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 5: Porcupine Harness (with faults, 20s, seed=42)${NC}"

harness_fault_dir="$RUN_DIR/harness-fault"
if ./bin/harness -bin=./bin/kvnode -nodes=3 -duration=15s -workers=4 \
   -faults -seed=42 -checktimeout=120s -outdir="$harness_fault_dir" > "$RUN_DIR/harness-fault.log" 2>&1; then
    record "porcupine_fault" "PASS"
else
    record "porcupine_fault" "FAIL" "see harness-fault.log"
fi

if [ -f "$harness_fault_dir/summary.json" ]; then
    python3 -c "
import json
d = json.load(open('$harness_fault_dir/summary.json'))
print('    ops=%d errors=%d timeouts=%d faults=%d' % (d.get('total_ops',0), d.get('errors',0), d.get('timeouts',0), d.get('fault_events',0)))
with open('$SCORES_FILE', 'a') as f:
    f.write('fault_ops=%d\n' % d.get('total_ops',0))
    f.write('fault_errors=%d\n' % d.get('errors',0))
    f.write('fault_timeouts=%d\n' % d.get('timeouts',0))
" 2>/dev/null || true
fi

# ─────────────────────────────────────────────────
# Phase 6: YCSB Benchmark
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 6: YCSB Benchmark (load 10K + run 50K ops)${NC}"

# Kill any leftover nodes from Phase 5
kill_stale_kvnodes
cleanup_ycsb_cluster
cleanup_ycsb_data

peers1="2=localhost:$((YCSB_RPC_BASE+1)),3=localhost:$((YCSB_RPC_BASE+2))"
peers2="1=localhost:$((YCSB_RPC_BASE)),3=localhost:$((YCSB_RPC_BASE+2))"
peers3="1=localhost:$((YCSB_RPC_BASE)),2=localhost:$((YCSB_RPC_BASE+1))"

./bin/kvnode -id=1 -rpc=":$((YCSB_RPC_BASE))"   -client=":$((YCSB_HTTP_BASE))"   -peers="$peers1" > "$RUN_DIR/ycsb_node1.log" 2>&1 &
echo $! > "$RUN_DIR/ycsb_node_1.pid"
./bin/kvnode -id=2 -rpc=":$((YCSB_RPC_BASE+1))" -client=":$((YCSB_HTTP_BASE+1))" -peers="$peers2" > "$RUN_DIR/ycsb_node2.log" 2>&1 &
echo $! > "$RUN_DIR/ycsb_node_2.pid"
./bin/kvnode -id=3 -rpc=":$((YCSB_RPC_BASE+2))" -client=":$((YCSB_HTTP_BASE+2))" -peers="$peers3" > "$RUN_DIR/ycsb_node3.log" 2>&1 &
echo $! > "$RUN_DIR/ycsb_node_3.pid"

echo "  waiting for cluster to elect leader..."
sleep 4

ycsb_nodes="localhost:$((YCSB_HTTP_BASE)),localhost:$((YCSB_HTTP_BASE+1)),localhost:$((YCSB_HTTP_BASE+2))"
ycsb_outdir="$RUN_DIR/ycsb-results"

if ./bin/ycsbrun \
    -nodes="$ycsb_nodes" \
    -recordcount=10000 \
    -operationcount=50000 \
    -threads=8 \
    -phase=both \
    -outdir="$ycsb_outdir" \
    > "$RUN_DIR/ycsb.log" 2>&1; then
    record "ycsb_run" "PASS"
else
    record "ycsb_run" "FAIL" "see ycsb.log"
fi

cleanup_ycsb_cluster

if [ -f "$ycsb_outdir/run_report.json" ]; then
    python3 -c "
import json
d = json.load(open('$ycsb_outdir/run_report.json'))
tp = d.get('throughput_ops_sec', 0)
print('    throughput: %.0f ops/sec' % tp)
for op, stats in d.get('per_operation', {}).items():
    print('    %s: p50=%.2fms p95=%.2fms p99=%.2fms p999=%.2fms' % (
        op, stats.get('p50_ms',0), stats.get('p95_ms',0), stats.get('p99_ms',0), stats.get('p999_ms',0)))

with open('$SCORES_FILE', 'a') as f:
    f.write('ycsb_throughput=%.0f\n' % tp)
    r = d.get('per_operation', {}).get('READ', {})
    f.write('ycsb_read_p50_ms=%.3f\n' % r.get('p50_ms', -1))
    f.write('ycsb_read_p95_ms=%.3f\n' % r.get('p95_ms', -1))
    f.write('ycsb_read_p99_ms=%.3f\n' % r.get('p99_ms', -1))
    f.write('ycsb_read_p999_ms=%.3f\n' % r.get('p999_ms', -1))
    u = d.get('per_operation', {}).get('UPDATE', {})
    f.write('ycsb_update_p99_ms=%.3f\n' % u.get('p99_ms', -1))
" 2>/dev/null || true
fi

if python3 -c "import matplotlib" 2>/dev/null && [ -f "$ycsb_outdir/run_latencies.csv" ]; then
    echo "  generating latency plots..."
    python3 "$REPO_ROOT/ycsb/scripts/plot_latency.py" "$ycsb_outdir/run_latencies.csv" "$ycsb_outdir" > /dev/null 2>&1 && \
        echo "    saved: $ycsb_outdir/latency_histogram.png, latency_cdf.png" || true
fi

# ─────────────────────────────────────────────────
# Phase 7: TLC Model Checker
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 7: TLC Model Checker${NC}"

TLA_DIR="$REPO_ROOT/tlaplus"
TLC_JAR="$TLA_DIR/tla2tools.jar"

# Find a working java (system java on macOS may be a non-functional stub)
JAVA_CMD=""
for _candidate in \
    "$(command -v java 2>/dev/null)" \
    /opt/homebrew/opt/openjdk/bin/java \
    /usr/local/opt/openjdk/bin/java \
    /opt/homebrew/bin/java; do
    if [ -n "$_candidate" ] && [ -x "$_candidate" ] && \
       "$_candidate" -version >/dev/null 2>&1; then
        JAVA_CMD="$_candidate"
        break
    fi
done

if [ -z "$JAVA_CMD" ]; then
    record "tlc_normal" "SKIP" "java not found"
    record "tlc_bug" "SKIP" "java not found"
elif [ ! -f "$TLC_JAR" ]; then
    record "tlc_normal" "SKIP" "tla2tools.jar not found"
    record "tlc_bug" "SKIP" "tla2tools.jar not found"
else
    # Normal mode — should PASS (no invariant violations)
    echo "  running TLC normal mode..."
    tlc_normal_exit=0
    (cd "$TLA_DIR" && "$JAVA_CMD" -XX:+UseParallelGC -jar tla2tools.jar \
        -config KVStore.cfg KVStore.tla \
        -workers 2 -cleanup) \
        > "$RUN_DIR/tlc-normal.log" 2>&1 || tlc_normal_exit=$?

    if grep -q "Model checking completed. No error has been found" "$RUN_DIR/tlc-normal.log" 2>/dev/null; then
        tlc_states=$(grep -o '[0-9]* distinct states' "$RUN_DIR/tlc-normal.log" | head -1 || echo "? states")
        record "tlc_normal" "PASS" "$tlc_states"
    elif grep -q "Error:" "$RUN_DIR/tlc-normal.log" 2>/dev/null; then
        tlc_err=$(grep "Error:" "$RUN_DIR/tlc-normal.log" | head -1)
        record "tlc_normal" "FAIL" "$tlc_err"
    else
        record "tlc_normal" "FAIL" "exit=$tlc_normal_exit, see tlc-normal.log"
    fi

    # Bug mode — should FAIL (find a counterexample)
    echo "  running TLC bug mode..."
    tlc_bug_exit=0
    (cd "$TLA_DIR" && "$JAVA_CMD" -XX:+UseParallelGC -jar tla2tools.jar \
        -config KVStoreBug.cfg KVStore.tla \
        -workers 2 -cleanup) \
        > "$RUN_DIR/tlc-bug.log" 2>&1 || tlc_bug_exit=$?

    if grep -q "Error:" "$RUN_DIR/tlc-bug.log" 2>/dev/null && [ "$tlc_bug_exit" -ne 0 ]; then
        record "tlc_bug_counterexample" "PASS" "counterexample found as expected"
    elif grep -q "Model checking completed. No error has been found" "$RUN_DIR/tlc-bug.log" 2>/dev/null; then
        record "tlc_bug_counterexample" "FAIL" "no counterexample — bug mode didn't trigger violation"
    else
        record "tlc_bug_counterexample" "FAIL" "unexpected output, see tlc-bug.log"
    fi
fi

# ─────────────────────────────────────────────────
# Phase 8: Test Coverage
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 8: Test Coverage${NC}"

if go test ./... -count=1 -timeout=60s > "$RUN_DIR/test.log" 2>&1; then
    record "go_test" "PASS"
else
    test_exit=$?
    if grep -q "no test files" "$RUN_DIR/test.log" && ! grep -q "FAIL" "$RUN_DIR/test.log"; then
        record "go_test" "PASS" "no test files found"
    else
        record "go_test" "FAIL" "exit=$test_exit, see test.log"
    fi
fi

go test ./... -cover -count=1 -timeout=60s > "$RUN_DIR/coverage.log" 2>&1 || true

avg_cover=$(python3 -c "
import re
lines = open('$RUN_DIR/coverage.log').read()
pcts = [float(m) for m in re.findall(r'coverage: ([0-9.]+)% of statements', lines)]
avg = sum(pcts)/len(pcts) if pcts else 0
print('%.1f' % avg)
" 2>/dev/null || echo "0.0")
score_set "test_coverage_pct" "$avg_cover"
record "test_coverage" "PASS" "${avg_cover}% average"

# ─────────────────────────────────────────────────
# Phase 9: Code Metrics
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 9: Code Metrics${NC}"

loc=$(find kvstore ycsb correctness -name '*.go' -exec cat {} + 2>/dev/null | wc -l | tr -d ' ')
file_count=$(find kvstore ycsb correctness -name '*.go' 2>/dev/null | wc -l | tr -d ' ')
score_set "lines_of_code" "$loc"
score_set "go_file_count" "$file_count"
echo "    Go files: $file_count"
echo "    Lines of code: $loc"

# ─────────────────────────────────────────────────
# Write scores.json
# ─────────────────────────────────────────────────

python3 -c "
import json

scores = {}
for line in open('$SCORES_FILE'):
    line = line.strip()
    if '=' not in line:
        continue
    k, v = line.split('=', 1)
    try:
        if '.' in v:
            scores[k] = float(v)
        else:
            scores[k] = int(v)
    except ValueError:
        scores[k] = v

scores['run_name'] = '$RUN_NAME'
with open('$RUN_DIR/scores.json', 'w') as f:
    json.dump(scores, f, indent=2, sort_keys=True)
" 2>/dev/null || echo '{}' > "$RUN_DIR/scores.json"

rm -f "$SCORES_FILE"

# ─────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  RESULTS: $RUN_NAME${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "  ${GREEN}Passed: $pass_count${NC}  ${RED}Failed: $fail_count${NC}  ${YELLOW}Skipped: $skip_count${NC}"
echo ""

if [ -f "$RUN_DIR/scores.json" ]; then
    python3 -c "
import json
s = json.load(open('$RUN_DIR/scores.json'))
print('  Correctness')
for k in ['porcupine_selftest','porcupine_nofault','porcupine_fault','tlc_normal','tlc_bug_counterexample']:
    v = s.get(k, '?')
    print('    %-28s %s' % (k+':', v))
print()
print('  Performance')
for k in ['ycsb_throughput','ycsb_read_p50_ms','ycsb_read_p95_ms','ycsb_read_p99_ms','ycsb_read_p999_ms','ycsb_update_p99_ms']:
    v = s.get(k, '?')
    unit = 'ops/sec' if 'throughput' in k else 'ms'
    print('    %-28s %s %s' % (k+':', v, unit))
print()
print('  Quality')
for k in ['test_coverage_pct','lines_of_code','go_file_count']:
    v = s.get(k, '?')
    print('    %-28s %s' % (k+':', v))
" 2>/dev/null || true
fi

echo ""
echo "  Full results: $RUN_DIR/"
echo "  Scores JSON:  $RUN_DIR/scores.json"
echo ""

if [ "$fail_count" -gt 0 ]; then
    exit 1
fi
