#!/usr/bin/env bash
# teardown.sh — Stop all demo processes and clean up data directories.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

YELLOW='\033[1;33m'; GREEN='\033[0;32m'; BOLD='\033[1m'; NC='\033[0m'

echo -e "${BOLD}=== Teardown ===${NC}"

pkill -f "bin/kvnode"  2>/dev/null && echo -e "  ${YELLOW}Stopped kvnode processes${NC}"  || echo -e "  No kvnode processes running"
pkill -f "bin/harness" 2>/dev/null && echo -e "  ${YELLOW}Stopped harness processes${NC}" || true
sleep 0.5

rm -rf /tmp/kv-demo-{1,2,3} /tmp/kv-demo-node*.log
echo -e "  Removed /tmp/kv-demo-* data directories"

echo -e "\n${GREEN}Done.${NC}"
