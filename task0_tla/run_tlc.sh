#!/bin/bash
# Run TLC model checker on the LinearKV specification.
#
# Usage:
#   ./run_tlc.sh          Run normal mode (BugMode=FALSE, should pass)
#   ./run_tlc.sh bug      Run bug mode (BugMode=TRUE, should find counterexample)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

TLC_JAR="../tlaplus/tla2tools.jar"

if [ ! -f "$TLC_JAR" ]; then
    echo "ERROR: TLC jar not found at $TLC_JAR"
    echo "Expected location: $(cd .. && pwd)/tlaplus/tla2tools.jar"
    exit 1
fi

# Create states directory for TLC output
mkdir -p states

if [ "$1" = "bug" ]; then
    CONFIG="LinearKV_Bug.cfg"
    echo "============================================================"
    echo "  Running TLC in BUG MODE (BugMode=TRUE)"
    echo "  Expecting a counterexample: stale read violates linearizability"
    echo "============================================================"
else
    CONFIG="LinearKV.cfg"
    echo "============================================================"
    echo "  Running TLC in NORMAL MODE (BugMode=FALSE)"
    echo "  Expecting all invariants to hold (no violations)"
    echo "  Note: With MaxOps=3, this explores millions of states"
    echo "  and may take several minutes."
    echo "============================================================"
fi

echo ""
echo "Config: $CONFIG"
echo "Spec:   LinearKV.tla"
echo ""

java -XX:+UseParallelGC -jar "$TLC_JAR" \
    -config "$CONFIG" \
    -workers auto \
    -metadir states \
    LinearKV.tla

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "=== TLC finished: No violations found ==="
else
    if [ "$1" = "bug" ]; then
        echo "=== TLC found a violation (expected in bug mode) ==="
        echo ""
        echo "The counterexample above shows how a stale read from a"
        echo "follower replica breaks linearizability. Read the state"
        echo "trace to see the violation."
    else
        echo "=== TLC finished with exit code $EXIT_CODE ==="
    fi
fi

exit $EXIT_CODE
