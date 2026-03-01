#!/usr/bin/env bash
#
# run_bench.sh - Run the YCSB-compatible benchmark against the KV store.
#
# Usage:
#   ./run_bench.sh                    # Default: 1M keys, 10M ops, zipfian
#   ./run_bench.sh --recordcount 100  # Quick smoke test
#
# Scaling instructions:
#   To scale from 1M to 100M keys:
#     ./run_bench.sh --recordcount 100000000 --operationcount 1000000000 --threads 64
#
#   Memory considerations for 100M keys:
#   - Each key is 16 bytes + 1024 byte value = ~1 KB per record
#   - 100M records = ~100 GB of data in the store
#   - Ensure the KV store has sufficient memory/disk
#   - Consider running load and run phases separately:
#       ./run_bench.sh --phase load --recordcount 100000000
#       ./run_bench.sh --phase run  --recordcount 100000000 --operationcount 1000000000
#
# The benchmark tool is built and run from the repository root.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Create timestamped output directory
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_DIR="${SCRIPT_DIR}/results/${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

echo "=============================================="
echo "  KV Store YCSB Benchmark"
echo "  Output: ${OUTPUT_DIR}"
echo "=============================================="

# Build the benchmark tool
echo "Building benchmark tool..."
cd "$REPO_ROOT"
go build -o "${SCRIPT_DIR}/bench" ./task2_ycsb/cmd/bench

echo "Build complete."
echo ""

# Default parameters (can be overridden via command-line args)
DEFAULT_ARGS=(
    --target "localhost:9000"
    --threads 16
    --recordcount 1000000
    --operationcount 10000000
    --distribution zipfian
    --read-proportion 0.95
    --update-proportion 0.04
    --delete-proportion 0.01
    --fieldlength 1024
    --phase both
    --output-dir "$OUTPUT_DIR"
)

# Run benchmark with defaults, allowing user overrides
"${SCRIPT_DIR}/bench" "${DEFAULT_ARGS[@]}" "$@"

echo ""
echo "=============================================="
echo "  Benchmark Complete"
echo "=============================================="
echo ""
echo "Results written to: ${OUTPUT_DIR}"
echo ""

# Print summary if it exists
if [ -f "${OUTPUT_DIR}/summary.json" ]; then
    echo "--- Summary ---"
    cat "${OUTPUT_DIR}/summary.json"
    echo ""
fi

echo ""
echo "Files:"
ls -la "$OUTPUT_DIR"
