#!/usr/bin/env bash
# setup.sh — Run once before the demo to verify binaries and clean state.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'

echo -e "${BOLD}=== Demo Setup ===${NC}"

# Kill any leftover processes
pkill -f "bin/kvnode" 2>/dev/null && echo -e "  ${YELLOW}Killed leftover kvnode processes${NC}" || true
pkill -f "bin/harness" 2>/dev/null || true
sleep 0.5

# Clean data directories
rm -rf /tmp/kv-demo-{1,2,3} demo-results demo-harness
echo -e "  Cleared data directories"

# Build binaries if missing
MISSING=false
for bin in kvnode ycsbrun harness; do
    if [ ! -x "bin/$bin" ]; then
        echo -e "  ${YELLOW}Building $bin...${NC}"
        go build -o "bin/$bin" "./${bin/kvnode/kvstore\/cmd\/kvnode}/" 2>/dev/null || \
        go build -o "bin/$bin" "./$(find . -name "main.go" | xargs grep -l "^package main" | \
            xargs grep -l "$bin" 2>/dev/null | head -1 | xargs dirname)/" 2>/dev/null || true
        MISSING=true
    fi
done

# Simpler build check
if [ ! -x "bin/kvnode" ] || [ ! -x "bin/ycsbrun" ] || [ ! -x "bin/harness" ]; then
    echo -e "  ${YELLOW}Building all binaries...${NC}"
    go build -o bin/kvnode  ./kvstore/cmd/kvnode/
    go build -o bin/ycsbrun ./ycsb/cmd/ycsbrun/
    go build -o bin/harness ./correctness/cmd/harness/
fi

echo -e "  ${GREEN}✓ kvnode${NC}   $(bin/kvnode --help 2>&1 | head -1 || echo 'ready')"
echo -e "  ${GREEN}✓ ycsbrun${NC}  ready"
echo -e "  ${GREEN}✓ harness${NC}  ready"

# Check ports are free
BUSY=false
for p in 16101 16102 16103 17101 17102 17103; do
    if lsof -ti ":$p" >/dev/null 2>&1; then
        echo -e "  ${RED}✗ Port $p is in use${NC}"
        BUSY=true
    fi
done

if $BUSY; then
    echo -e "\n${RED}Some ports are busy. Run: pkill -f bin/kvnode${NC}"
    exit 1
fi

echo -e "\n${GREEN}${BOLD}Setup complete. Ready to run act1_cluster.sh${NC}"
