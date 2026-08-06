# shardkv development tasks. Everything here is also what CI runs, so a green
# `make check` locally means a green pipeline.

GO      ?= go
BIN     ?= bin/shardkv
PORT    ?= 6380
FUZZTIME ?= 20s

.PHONY: help
help: ## List the available targets.
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the server into bin/.
	$(GO) build -trimpath -o $(BIN) ./cmd/shardkv

.PHONY: run
run: ## Run the server on $(PORT) with an AOF in ./data.
	@mkdir -p data
	$(GO) run ./cmd/shardkv -addr :$(PORT) -aof data/shardkv.aof

.PHONY: test
test: ## Run the whole suite under the race detector.
	$(GO) test -race -count=1 ./...

.PHONY: test-short
test-short: ## Run the suite without the race detector (faster inner loop).
	$(GO) test -count=1 ./...

.PHONY: fuzz
fuzz: ## Fuzz the RESP parser for $(FUZZTIME).
	$(GO) test -run='^$$' -fuzz=FuzzReadCommand -fuzztime=$(FUZZTIME) ./internal/resp

.PHONY: bench
bench: ## Run the store benchmarks with allocation counts.
	$(GO) test -bench=. -benchmem ./internal/store

.PHONY: bench-vs-redis
bench-vs-redis: ## End-to-end throughput vs a real Redis (docker; reports its own variance).
	./test/bench/vs-redis.sh

.PHONY: compat
compat: ## Drive the server with real client libraries in containers (docker).
	./test/compat/run.sh

.PHONY: compat-tcl
compat-tcl: ## Run Redis's own TCL test suite against the server in external mode (docker).
	./test/compat/run.sh tcl

.PHONY: cover
cover: ## Write a coverage profile and print the total.
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	@$(GO) tool cover -func=coverage.out | tail -1

.PHONY: fmt
fmt: ## Format the tree.
	$(GO) fmt ./...

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint (installs it into GOPATH/bin if missing).
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "installing golangci-lint..."; \
		CGO_ENABLED=0 $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest; \
	}
	@PATH="$$($(GO) env GOPATH)/bin:$$PATH" golangci-lint run

.PHONY: fmt-check
fmt-check: ## Fail if anything is unformatted (what CI checks).
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then echo "unformatted files:"; echo "$$out"; exit 1; fi

.PHONY: check
check: fmt-check vet build test ## Everything CI runs, in CI's order.

.PHONY: docker
docker: ## Build the container image.
	docker build -t shardkv:local .

.PHONY: docker-run
docker-run: docker ## Run the image, publishing $(PORT) and persisting to a volume.
	docker run --rm -p $(PORT):6380 -v shardkv-data:/data shardkv:local

.PHONY: clean
clean: ## Remove build and coverage output.
	rm -rf bin data coverage.out
