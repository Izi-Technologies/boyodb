RUST_TARGET_DIR ?= target
CARGO ?= cargo
GO ?= go

.PHONY: build-core build-node build-go build-all clean bench-core \
        test test-all lint fmt check \
        docker-build docker-run docker-stop docker-cluster-up docker-cluster-down

# Build targets
build-core:
	$(CARGO) build -p boyodb-core --release

build-server:
	$(CARGO) build -p boyodb-server --release

build-cli:
	$(CARGO) build -p boyodb-cli --release

build-node:
	$(CARGO) build -p boyodb-node --release

# Requires the Rust core built first so the linker can find libcboyodb_core
build-go: build-core
	cd bindings/go && CGO_ENABLED=1 $(GO) build ./...

build-all: build-core build-node build-go build-server build-cli

# Test targets
test:
	$(CARGO) test --workspace

test-all: test
	$(CARGO) test --workspace -- --ignored

# Lint and format targets
lint:
	$(CARGO) clippy --workspace --all-targets -- -D warnings

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

check:
	$(CARGO) check --workspace --all-targets

# Clean
clean:
	$(CARGO) clean
	cd bindings/go && $(GO) clean ./...

# Benchmark
bench-core:
	$(CARGO) run -p boyodb-bench --release -- --rows=1000000 --batch-size=10000 --query-iters=5

# Docker targets
docker-build:
	docker build -t boyodb:latest .

docker-run: docker-build
	docker run -d --name boyodb -p 5555:5555 -v boyodb-data:/var/lib/boyodb/data boyodb:latest

docker-stop:
	docker stop boyodb && docker rm boyodb

docker-dev:
	docker compose --profile dev up -d boyodb-dev

docker-cluster-up:
	docker compose --profile cluster up -d

docker-cluster-down:
	docker compose --profile cluster down

docker-clean:
	docker compose down -v --remove-orphans

