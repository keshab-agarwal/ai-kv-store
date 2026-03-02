#!/usr/bin/env bash
# act2_readwrite.sh — Write a key, read it back, show redirect, then delete it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

KEY="deadbeefdeadbeefdeadbeefdeadbeef"
VALUE="hello from the Photon Quorum store"
SEQ=100

echo -e "${BOLD}=== Act 2: Reads & Writes ===${NC}\n"

# ── Find primary ──────────────────────────────────────────────────────────────
PRIMARY=""
SECONDARY=""
for port in 16101 16102 16103; do
    ROLE=$(curl -s "http://localhost:$port/api/status" 2>/dev/null | \
        python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
    if [ "$ROLE" = "primary" ]; then
        PRIMARY="localhost:$port"
    else
        SECONDARY="localhost:$port"
    fi
done

if [ -z "$PRIMARY" ]; then
    echo -e "${YELLOW}No primary found. Run act1_cluster.sh first.${NC}"; exit 1
fi
echo -e "  Primary:   ${GREEN}${BOLD}$PRIMARY${NC}"
echo -e "  Secondary: ${DIM}$SECONDARY${NC}\n"

# ── Write ─────────────────────────────────────────────────────────────────────
echo -e "${BOLD}PUT${NC}  key=${KEY:0:8}...  value=\"$VALUE\""
B64=$(echo -n "$VALUE" | base64)
curl -s -X POST "http://$PRIMARY/api/put" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$KEY\",\"value\":\"$B64\",\"client_id\":\"demo\",\"client_seq\":$SEQ}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  → \033[0;32m{d[\"status\"]}\033[0m  (WAL fsync + replicated to quorum)')"
sleep 0.3
echo ""

# ── Read from PRIMARY (lease path — 0 RTTs) ───────────────────────────────────
echo -e "${BOLD}GET${NC}  from ${GREEN}primary${NC} $PRIMARY  ${DIM}(lease read — 0 network hops)${NC}"
python3 - "$PRIMARY" "$KEY" <<'PYEOF'
import sys, json, base64, time
from urllib.request import urlopen, Request

host, key = sys.argv[1], sys.argv[2]

# Warmup: establish connection (TCP handshake cost)
req = Request(f"http://{host}/api/get",
              data=json.dumps({"key": key}).encode(),
              headers={"Content-Type": "application/json"})
urlopen(req).read()

# Measured request — reuses the OS TCP stack warmup
RUNS = 5
total = 0.0
val = ""
for _ in range(RUNS):
    t0 = time.perf_counter()
    resp = urlopen(Request(
        f"http://{host}/api/get",
        data=json.dumps({"key": key}).encode(),
        headers={"Content-Type": "application/json"})).read()
    total += (time.perf_counter() - t0) * 1000
    d = json.loads(resp)
    if d["status"] == "found":
        val = base64.b64decode(d["value"]).decode()

avg_ms = total / RUNS
YELLOW = '\033[1;33m'; BOLD = '\033[1m'; DIM = '\033[2m'; NC = '\033[0m'
print(f'  \u2192 "{val}"')
print(f'  \u2192 avg over {RUNS} requests: {YELLOW}{BOLD}{avg_ms:.3f}ms{NC}  '
      f'{DIM}(in-memory map read within 40ms lease window){NC}')
PYEOF
sleep 0.3

# ── Read from SECONDARY (shows redirect) ─────────────────────────────────────
echo -e "${BOLD}GET${NC}  from ${DIM}secondary${NC} $SECONDARY  ${DIM}(non-primary node)${NC}"
REDIRECT_RESP=$(curl -s -X POST "http://$SECONDARY/api/get" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$KEY\"}")
REDIRECT_STATUS=$(echo "$REDIRECT_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
REDIRECT_LEADER=$(echo "$REDIRECT_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('leader',''))" 2>/dev/null)
echo -e "  → status=${YELLOW}$REDIRECT_STATUS${NC}  leader=${GREEN}$REDIRECT_LEADER${NC}"
echo -e "  ${DIM}Secondaries don't serve reads — they redirect to the primary.${NC}"
echo -e "  ${DIM}The client library follows this automatically.${NC}\n"
sleep 0.3

# ── Update ────────────────────────────────────────────────────────────────────
NEW_VALUE="updated value — write #2"
echo -e "${BOLD}UPDATE${NC}  same key  →  \"$NEW_VALUE\""
B64=$(echo -n "$NEW_VALUE" | base64)
curl -s -X POST "http://$PRIMARY/api/put" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$KEY\",\"value\":\"$B64\",\"client_id\":\"demo\",\"client_seq\":$((SEQ+1))}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  → \033[0;32m{d[\"status\"]}\033[0m')"
sleep 0.3
echo ""

# ── Read again ────────────────────────────────────────────────────────────────
echo -e "${BOLD}GET${NC}  (confirm update)"
curl -s -X POST "http://$PRIMARY/api/get" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$KEY\"}" \
    | python3 -c "
import sys,json,base64; d=json.load(sys.stdin)
print(f'  → \"{base64.b64decode(d[\"value\"]).decode()}\"')"
sleep 0.3
echo ""

# ── Delete ────────────────────────────────────────────────────────────────────
echo -e "${BOLD}DELETE${NC}  key"
curl -s -X POST "http://$PRIMARY/api/delete" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$KEY\",\"client_id\":\"demo\",\"client_seq\":$((SEQ+2))}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  → \033[0;32m{d[\"status\"]}\033[0m')"
sleep 0.3
echo ""

echo -e "${BOLD}GET${NC}  (confirm deletion)"
curl -s -X POST "http://$PRIMARY/api/get" \
    -H 'Content-Type: application/json' \
    -d "{\"key\":\"$KEY\"}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  → {d[\"status\"]}')"

echo -e "\n${CYAN}Next: run demo/act3_failover.sh${NC}"
