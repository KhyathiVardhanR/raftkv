.PHONY: build proto test bench failover clean

build:
	go build -o bin/ ./cmd/...

proto:
	protoc -I proto --go_out=proto/raftkvpb --go_opt=paths=source_relative \
		--go-grpc_out=proto/raftkvpb --go-grpc_opt=paths=source_relative proto/raftkv.proto

test:
	go test -race -count=1 ./...

bench: build
	mkdir -p results
	./bin/raftkv-lab bench -n 3 -duration 10s | grep -E '^\||^###' | tee results/bench-3node.md
	./bin/raftkv-lab bench -n 5 -duration 10s -base-port 7200 | grep -E '^\||^###' | tee results/bench-5node.md

failover: build
	mkdir -p results
	./bin/raftkv-lab failover -n 3 -trials 20 -base-port 7400 2>&1 | tee results/failover-3node.txt
	./bin/raftkv-lab failover -n 5 -trials 20 -base-port 7600 2>&1 | tee results/failover-5node.txt

clean:
	rm -rf bin
