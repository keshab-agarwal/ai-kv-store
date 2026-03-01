#!/usr/bin/env bash
set -euo pipefail

YCSB_HOME=${YCSB_HOME:-$HOME/ycsb-0.17.0}
BINDING_DIR=$(cd "$(dirname "$0")/../binding" && pwd)
WORKLOAD_DIR=$(cd "$(dirname "$0")/../workloads" && pwd)

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: java is not installed. Install a JDK (17+ recommended)." >&2
  exit 1
fi

if ! command -v mvn >/dev/null 2>&1; then
  echo "ERROR: Maven is not installed (missing 'mvn')." >&2
  echo "Install with: brew install maven" >&2
  exit 1
fi

if [ ! -x "$YCSB_HOME/bin/ycsb" ]; then
  echo "ERROR: YCSB not found at $YCSB_HOME/bin/ycsb" >&2
  echo "Set YCSB_HOME to your extracted ycsb-0.17.0 directory." >&2
  exit 1
fi

pushd "$BINDING_DIR" >/dev/null
mvn -q -DskipTests package
popd >/dev/null

CP="$BINDING_DIR/target/kv-ycsb-binding-1.0.0.jar"

"$YCSB_HOME/bin/ycsb" load basic \
  -P "$WORKLOAD_DIR/workload_zipfian_load.properties" \
  -p db=com.example.kv.KvHttpYcsbClient \
  -cp "$CP" \
  -p kv.endpoints=http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003

"$YCSB_HOME/bin/ycsb" run basic \
  -P "$WORKLOAD_DIR/workload_zipfian_run.properties" \
  -p db=com.example.kv.KvHttpYcsbClient \
  -cp "$CP" \
  -p kv.endpoints=http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003 \
  -s
