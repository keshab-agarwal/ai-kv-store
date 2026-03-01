.PHONY: build test clean

build:
	go build -o bin/kvnode ./task1_kv/cmd/node
	go build -o bin/clusterctl ./task1_kv/cmd/clusterctl
	go build -o bin/harness ./task3_harness/cmd/harness
	go build -o bin/porcupine_check ./task3_harness/cmd/porcupine_check

test:
	go test ./...

clean:
	rm -rf bin artifacts .run
