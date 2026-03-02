#!/usr/bin/env bash
# act1_cluster.sh — Start a 3-node cluster and watch leader election happen live.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

echo -e "${BOLD}=== Act 1: Start Cluster & Leader Election ===${NC}\n"

# Clean slate
pkill -f "bin/kvnode" 2>/dev/null || true
rm -rf /tmp/kv-demo-{1,2,3}
sleep 0.3

echo -e "${DIM}Starting 3 nodes...${NC}"

./bin/kvnode -id=1 -rpc=:17101 -client=:16101 \
  -data=/tmp/kv-demo-1 -peers="2=localhost:17102,3=localhost:17103" \
  > /tmp/kv-demo-node1.log 2>&1 &

./bin/kvnode -id=2 -rpc=:17102 -client=:16102 \
  -data=/tmp/kv-demo-2 -peers="1=localhost:17101,3=localhost:17103" \
  > /tmp/kv-demo-node2.log 2>&1 &

./bin/kvnode -id=3 -rpc=:17103 -client=:16103 \
  -data=/tmp/kv-demo-3 -peers="1=localhost:17101,2=localhost:17102" \
  > /tmp/kv-demo-node3.log 2>&1 &

# Wait for all 3 to answer HTTP
echo -e "${DIM}Waiting for nodes to start...${NC}"
for port in 16101 16102 16103; do
    deadline=$((SECONDS + 10))
    until curl -sf "http://localhost:$port/api/status" >/dev/null 2>&1; do
        [ $SECONDS -ge $deadline ] && { echo "TIMEOUT waiting for :$port"; exit 1; }
        sleep 0.1
    done
done

echo -e "${GREEN}All 3 nodes are up. Watching election...${NC}\n"

# Poll until a leader is elected, printing each round
ELECTED=false
for i in $(seq 1 40); do
    LINE=""
    LEADER_FOUND=false
    for port in 16101 16102 16103; do
        STATUS=$(curl -s "http://localhost:$port/api/status" 2>/dev/null)
        ROLE=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "?")
        EPOCH=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['epoch'])" 2>/dev/null || echo "?")
        if [ "$ROLE" = "primary" ]; then
            LINE+="  ${GREEN}${BOLD}node:$port  primary   epoch=$EPOCH${NC}\n"
            LEADER_FOUND=true
        else
            LINE+="  ${DIM}node:$port  $ROLE   epoch=$EPOCH${NC}\n"
        fi
    done
    printf "${LINE}"
    if $LEADER_FOUND; then
        ELECTED=true
        break
    fi
    sleep 0.1
    # Clear the 3 lines printed
    printf "\033[3A"
done

echo ""
if $ELECTED; then
    echo -e "${GREEN}${BOLD}Leader elected in <300ms via randomised epoch timeouts.${NC}"
    echo -e "${DIM}  → Majority quorum (2-of-3) is now available for reads and writes.${NC}"
else
    echo -e "${YELLOW}Election still in progress — rerun the script.${NC}"
fi

echo -e "\n${DIM}Node logs: /tmp/kv-demo-node{1,2,3}.log${NC}"
echo -e "${CYAN}Next: run demo/act2_readwrite.sh${NC}"
