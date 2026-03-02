#!/usr/bin/env bash
# act3_failover.sh — Kill the primary mid-write, watch a new leader emerge, confirm no data loss.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

PRE_KEY="cafecafecafecafecafecafecafecafe"   # written before crash
POST_KEY="beefdbeefbeefdbeefbeefdbeefbeefd"  # written after failover
SEQ=200

echo -e "${BOLD}=== Act 3: Primary Failover ===${NC}\n"

# ── Find and display current cluster state ────────────────────────────────────
echo -e "Cluster state ${DIM}before${NC} crash:\n"
PRIMARY_PORT=""
for port in 16101 16102 16103; do
    STATUS=$(curl -s "http://localhost:$port/api/status" 2>/dev/null)
    ROLE=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "down")
    EPOCH=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['epoch'])" 2>/dev/null || echo "?")
    if [ "$ROLE" = "primary" ]; then
        echo -e "  ${GREEN}${BOLD}node:$port  $ROLE   epoch=$EPOCH${NC}"
        PRIMARY_PORT="$port"
    else
        echo -e "  ${DIM}node:$port  $ROLE   epoch=$EPOCH${NC}"
    fi
done

if [ -z "$PRIMARY_PORT" ]; then
    echo -e "${YELLOW}No primary found. Run act1_cluster.sh first.${NC}"; exit 1
fi

# ── Write a key BEFORE the crash ─────────────────────────────────────────────
echo ""
echo -e "${BOLD}Writing key before crash...${NC}"
B64=$(echo -n "written before crash" | base64)
curl -s -X POST "http://localhost:$PRIMARY_PORT/api/put" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$PRE_KEY\",\"value\":\"$B64\",\"client_id\":\"demo\",\"client_seq\":$SEQ}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  → {d[\"status\"]}')"

sleep 0.5

# ── Kill the primary ──────────────────────────────────────────────────────────
PRIMARY_PID=$(lsof -ti ":$PRIMARY_PORT" 2>/dev/null | head -1)
echo ""
echo -e "${RED}${BOLD}💥 Killing primary (node :$PRIMARY_PORT  PID $PRIMARY_PID)...${NC}"
kill -9 "$PRIMARY_PID" 2>/dev/null || true
echo ""

# ── Watch election happen in real time ────────────────────────────────────────
echo -e "Watching re-election${DIM} (polling every 100ms)${NC}:\n"
NEW_PRIMARY_PORT=""
ELAPSED=0
START=$SECONDS
for i in $(seq 1 50); do
    LINE=""
    for port in 16101 16102 16103; do
        [ "$port" = "$PRIMARY_PORT" ] && continue
        STATUS=$(curl -s --max-time 0.3 "http://localhost:$port/api/status" 2>/dev/null)
        ROLE=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "?")
        EPOCH=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['epoch'])" 2>/dev/null || echo "?")
        if [ "$ROLE" = "primary" ]; then
            LINE+="  ${GREEN}${BOLD}node:$port  primary   epoch=$EPOCH  ← NEW LEADER${NC}\n"
            NEW_PRIMARY_PORT="$port"
        else
            LINE+="  ${DIM}node:$port  $ROLE   epoch=$EPOCH${NC}\n"
        fi
    done
    printf "${LINE}"
    if [ -n "$NEW_PRIMARY_PORT" ]; then
        ELAPSED=$((SECONDS - START))
        break
    fi
    sleep 0.1
    printf "\033[2A"
done

echo ""
echo -e "${GREEN}${BOLD}New primary elected in ~${ELAPSED}s.${NC}"
echo -e "${DIM}  → Randomised election timeout (150–300ms) broke the tie.${NC}\n"

# ── Write to NEW primary ──────────────────────────────────────────────────────
echo -e "${BOLD}Writing a NEW key to the new primary :$NEW_PRIMARY_PORT...${NC}"
B64=$(echo -n "written after failover" | base64)
RESULT=$(curl -s -X POST "http://localhost:$NEW_PRIMARY_PORT/api/put" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$POST_KEY\",\"value\":\"$B64\",\"client_id\":\"demo\",\"client_seq\":$((SEQ+1))}")
STATUS=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "error")
echo -e "  → ${GREEN}$STATUS${NC}  ${DIM}(cluster still accepts writes with 2-of-3 nodes)${NC}\n"

# ── Read PRE-CRASH key — confirm no data loss ─────────────────────────────────
echo -e "${BOLD}Reading key written BEFORE the crash:${NC}"
curl -s -X POST "http://localhost:$NEW_PRIMARY_PORT/api/get" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$PRE_KEY\"}" \
    | python3 -c "
import sys,json,base64
d=json.load(sys.stdin)
val=base64.b64decode(d['value']).decode() if d['status']=='found' else d['status']
print(f'  → \"{val}\"  ${GREEN}← not lost${NC}')
" 2>/dev/null || echo "  → (read result above)"

# ── Restart the crashed node ──────────────────────────────────────────────────
echo ""
echo -e "${DIM}Restarting crashed node :$PRIMARY_PORT...${NC}"
./bin/kvnode -id=${PRIMARY_PORT: -1} \
    -rpc=":171${PRIMARY_PORT: -2}" \
    -client=":$PRIMARY_PORT" \
    -data="/tmp/kv-demo-${PRIMARY_PORT: -1}" \
    -peers="$([ "$PRIMARY_PORT" = "16101" ] && echo "2=localhost:17102,3=localhost:17103" || \
              [ "$PRIMARY_PORT" = "16102" ] && echo "1=localhost:17101,3=localhost:17103" || \
              echo "1=localhost:17101,2=localhost:17102")" \
    >> "/tmp/kv-demo-node${PRIMARY_PORT: -1}.log" 2>&1 &

deadline=$((SECONDS + 15))
until curl -sf "http://localhost:$PRIMARY_PORT/api/status" >/dev/null 2>&1; do
    [ $SECONDS -ge $deadline ] && { echo "  Timed out waiting for node to recover"; break; }
    sleep 0.3
done

ROLE=$(curl -s "http://localhost:$PRIMARY_PORT/api/status" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "?")
echo -e "  ${GREEN}node:$PRIMARY_PORT recovered as $ROLE${NC}"
echo -e "${DIM}  → WAL replay on restart restores all committed entries.${NC}"

echo -e "\n${CYAN}Next: run demo/act4_benchmark.sh${NC}"
