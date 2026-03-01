# Task 1: Distributed In-Memory KV Store

## API
- `PUT /kv/{keyHex32}` with JSON body `{"value":"<base64>"}`
- `GET /kv/{keyHex32}`
- `DELETE /kv/{keyHex32}`

Responses:
- Put/Delete: `{"status":"OK"}` or `{"status":"TIMEOUT"|"ERROR"...}`
- Get: `{"status":"FOUND","value":"<base64>"}` or `{"status":"NOT_FOUND"}`

## Build
```bash
go build -o bin/kvnode ./task1_kv/cmd/node
go build -o bin/clusterctl ./task1_kv/cmd/clusterctl
```

## Local cluster
```bash
./bin/clusterctl -cmd start -nodes 3 -base-port 9001 -state-dir .run/cluster -node-bin ./bin/kvnode
./bin/clusterctl -cmd status -state-dir .run/cluster
./bin/clusterctl -cmd crash -state-dir .run/cluster -node 1
./bin/clusterctl -cmd recover -state-dir .run/cluster -node 1 -node-bin ./bin/kvnode
./bin/clusterctl -cmd stop -state-dir .run/cluster
```
