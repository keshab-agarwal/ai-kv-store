#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
mkdir -p data-0 data-1 data-2
go run ./cmd/node -id=0 -port=8000 -peers=localhost:8000,localhost:8001,localhost:8002 &
PID0=$!
go run ./cmd/node -id=1 -port=8001 -peers=localhost:8000,localhost:8001,localhost:8002 &
PID1=$!
go run ./cmd/node -id=2 -port=8002 -peers=localhost:8000,localhost:8001,localhost:8002 &
PID2=$!
echo "Cluster started. PIDs: $PID0 $PID1 $PID2"
echo "Connect clients to localhost:8000, localhost:8001, or localhost:8002"
wait
