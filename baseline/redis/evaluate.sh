#!/usr/bin/env bash
#
# evaluate.sh — Redis benchmark evaluation harness.
#
# Runs the same YCSB workload against Redis that evaluate.sh runs against
# the custom KV store, producing directly comparable output.
#
# Prerequisites:
#   - Redis server running (or docker-compose up from this directory)
#   - Go 1.20+
#
# Usage:
#   ./redis/evaluate.sh [run-name] [redis-addr]
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_NAME="${1:-redis-$(date +%Y%m%d-%H%M%S)}"
REDIS_ADDR="${2:-localhost:6379}"
RUN_DIR="$SCRIPT_DIR/runs/$RUN_NAME"

mkdir -p "$RUN_DIR"

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

score_set() { echo "$1=$2" >> "$SCORES_FILE"; }

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

MANAGED_REDIS_PID=""
cleanup() {
    if [ -n "$MANAGED_REDIS_PID" ]; then
        echo "  stopping managed Redis server (pid $MANAGED_REDIS_PID)..."
        kill "$MANAGED_REDIS_PID" 2>/dev/null || true
        wait "$MANAGED_REDIS_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

cd "$REPO_ROOT"

echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  Redis Benchmark Evaluation${NC}"
echo -e "${BOLD}  Run: $RUN_NAME${NC}"
echo -e "${BOLD}  Target: $REDIS_ADDR${NC}"
echo -e "${BOLD}  Output: $RUN_DIR${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""

# ─────────────────────────────────────────────────
# Phase 0: Redis connectivity check
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 0: Redis Connectivity${NC}"

redis_host="${REDIS_ADDR%%:*}"
redis_port="${REDIS_ADDR##*:}"

check_redis() {
    if command -v redis-cli &>/dev/null; then
        redis-cli -h "$redis_host" -p "$redis_port" PING 2>/dev/null | grep -q "PONG"
    else
        # Fallback: try TCP connection
        (echo PING; sleep 0.2) | nc -w2 "$redis_host" "$redis_port" 2>/dev/null | grep -q "PONG"
    fi
}

if check_redis; then
    record "redis_connectivity" "PASS" "$REDIS_ADDR"
else
    # Try starting Redis via docker-compose if available
    if [ -f "$SCRIPT_DIR/docker-compose.yml" ] && command -v docker &>/dev/null; then
        echo "  Redis not reachable, attempting docker-compose up..."
        (cd "$SCRIPT_DIR" && docker compose up -d 2>/dev/null || docker-compose up -d 2>/dev/null) \
            > "$RUN_DIR/docker.log" 2>&1
        sleep 3
        if check_redis; then
            record "redis_connectivity" "PASS" "started via docker-compose"
        else
            record "redis_connectivity" "FAIL" "could not reach Redis at $REDIS_ADDR"
            echo -e "  ${RED}Redis not reachable — cannot continue.${NC}"
            echo "  Start Redis with: docker compose -f redis/docker-compose.yml up -d"
            echo "  Or:               redis-server --daemonize yes"
            exit 1
        fi
    elif command -v redis-server &>/dev/null; then
        echo "  Redis not reachable, starting local redis-server..."
        redis-server --daemonize no --port "$redis_port" --save "" --appendonly no \
            > "$RUN_DIR/redis-server.log" 2>&1 &
        MANAGED_REDIS_PID=$!
        sleep 2
        if check_redis; then
            record "redis_connectivity" "PASS" "started local redis-server (pid $MANAGED_REDIS_PID)"
        else
            record "redis_connectivity" "FAIL" "redis-server started but not responding"
            exit 1
        fi
    else
        record "redis_connectivity" "FAIL" "redis not reachable and no way to start it"
        echo -e "  ${RED}Install Redis or Docker to continue.${NC}"
        exit 1
    fi
fi

# ─────────────────────────────────────────────────
# Phase 1: Build
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 1: Build${NC}"

if go build -o bin/redisbench ./redis/cmd/redisbench/ > "$RUN_DIR/build.log" 2>&1; then
    record "go_build" "PASS" "redisbench"
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

if go vet ./redis/... > "$RUN_DIR/vet.log" 2>&1; then
    record "go_vet" "PASS"
else
    record "go_vet" "FAIL" "see vet.log"
fi

# ─────────────────────────────────────────────────
# Phase 3: YCSB Benchmark — small (quick smoke test)
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 3: YCSB Smoke Test (load 10K + run 50K ops, 8 threads)${NC}"

smoke_outdir="$RUN_DIR/ycsb-smoke"
if ./bin/redisbench \
    -addrs="$REDIS_ADDR" \
    -recordcount=10000 \
    -operationcount=50000 \
    -threads=8 \
    -phase=both \
    -outdir="$smoke_outdir" \
    > "$RUN_DIR/ycsb-smoke.log" 2>&1; then
    record "ycsb_smoke" "PASS"
else
    record "ycsb_smoke" "FAIL" "see ycsb-smoke.log"
fi

if [ -f "$smoke_outdir/run_report.json" ]; then
    python3 -c "
import json
d = json.load(open('$smoke_outdir/run_report.json'))
tp = d.get('throughput_ops_sec', 0)
print('    throughput: %.0f ops/sec' % tp)
for op, stats in d.get('per_operation', {}).items():
    print('    %s: p50=%.2fms p95=%.2fms p99=%.2fms p999=%.2fms' % (
        op, stats.get('p50_ms',0), stats.get('p95_ms',0), stats.get('p99_ms',0), stats.get('p999_ms',0)))
" 2>/dev/null || true
fi

# ─────────────────────────────────────────────────
# Phase 4: YCSB Benchmark — full
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 4: YCSB Full Benchmark (load 100K + run 500K ops, 8 threads)${NC}"

full_outdir="$RUN_DIR/ycsb-results"
if ./bin/redisbench \
    -addrs="$REDIS_ADDR" \
    -recordcount=100000 \
    -operationcount=500000 \
    -threads=8 \
    -phase=both \
    -outdir="$full_outdir" \
    > "$RUN_DIR/ycsb.log" 2>&1; then
    record "ycsb_full" "PASS"
else
    record "ycsb_full" "FAIL" "see ycsb.log"
fi

if [ -f "$full_outdir/run_report.json" ]; then
    python3 -c "
import json
d = json.load(open('$full_outdir/run_report.json'))
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

# ─────────────────────────────────────────────────
# Phase 5: Latency plots
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 5: Latency Plots${NC}"

if python3 -c "import matplotlib" 2>/dev/null && [ -f "$full_outdir/run_latencies.csv" ]; then
    if python3 "$SCRIPT_DIR/scripts/plot_latency.py" "$full_outdir/run_latencies.csv" "$full_outdir" \
        > /dev/null 2>&1; then
        record "latency_plots" "PASS"
        echo "    saved: $full_outdir/latency_histogram.png, latency_cdf.png"
    else
        record "latency_plots" "FAIL" "plot generation error"
    fi
else
    record "latency_plots" "SKIP" "matplotlib not installed or no CSV"
fi

# ─────────────────────────────────────────────────
# Phase 6: Comparison (if KV store results exist)
# ─────────────────────────────────────────────────
echo -e "${BOLD}▸ Phase 6: KV Store Comparison${NC}"

# Look for the most recent KV store evaluation results
KV_RESULTS=""
for d in "$REPO_ROOT"/evaluation/runs/*/ycsb-results/run_report.json; do
    [ -f "$d" ] && KV_RESULTS="$d"
done

if [ -n "$KV_RESULTS" ] && [ -f "$full_outdir/run_report.json" ]; then
    compare_dir="$RUN_DIR/comparison"
    mkdir -p "$compare_dir"
    if python3 "$SCRIPT_DIR/scripts/compare.py" "$KV_RESULTS" "$full_outdir/run_report.json" "$compare_dir" \
        > "$RUN_DIR/comparison.log" 2>&1; then
        record "comparison" "PASS"
        cat "$RUN_DIR/comparison.log"
    else
        record "comparison" "FAIL" "see comparison.log"
    fi
else
    record "comparison" "SKIP" "no KV store results found for comparison"
    echo "    Run evaluation/evaluate.sh first to produce KV store results."
fi

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
scores['target'] = 'redis'
scores['redis_addr'] = '$REDIS_ADDR'
with open('$RUN_DIR/scores.json', 'w') as f:
    json.dump(scores, f, indent=2, sort_keys=True)
" 2>/dev/null || echo '{}' > "$RUN_DIR/scores.json"

rm -f "$SCORES_FILE"

# ─────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  RESULTS: $RUN_NAME (Redis)${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "  ${GREEN}Passed: $pass_count${NC}  ${RED}Failed: $fail_count${NC}  ${YELLOW}Skipped: $skip_count${NC}"
echo ""

if [ -f "$RUN_DIR/scores.json" ]; then
    python3 -c "
import json
s = json.load(open('$RUN_DIR/scores.json'))
print('  Performance')
for k in ['ycsb_throughput','ycsb_read_p50_ms','ycsb_read_p95_ms','ycsb_read_p99_ms','ycsb_read_p999_ms','ycsb_update_p99_ms']:
    v = s.get(k, '?')
    unit = 'ops/sec' if 'throughput' in k else 'ms'
    print('    %-28s %s %s' % (k+':', v, unit))
" 2>/dev/null || true
fi

echo ""
echo "  Full results: $RUN_DIR/"
echo "  Scores JSON:  $RUN_DIR/scores.json"
echo ""

if [ "$fail_count" -gt 0 ]; then
    exit 1
fi
