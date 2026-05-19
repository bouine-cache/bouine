# bouine — Makefile
#
# Source of truth for local build/test/lint flows. Every target listed in
# AGENTS.md §14.1 is implemented here. Targets are intentionally thin —
# they call the underlying tool, no scripting embedded.

GO            ?= go
GOFLAGS       ?=
BIN_DIR       ?= bin
BIN           := $(BIN_DIR)/bouine
PKGS          ?= ./...
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT        ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE          ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS       := -s -w \
                 -X github.com/thylong/bouine/internal/buildinfo.Version=$(VERSION) \
                 -X github.com/thylong/bouine/internal/buildinfo.Commit=$(COMMIT) \
                 -X github.com/thylong/bouine/internal/buildinfo.Date=$(DATE)

.PHONY: help
help: ## Show this help.
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

.PHONY: all
all: lint test build ## Lint, test, and build.

.PHONY: build
build: ## Build the bouine binary to ./bin/bouine.
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/bouine

.PHONY: test
test: ## Run unit tests with the race detector.
	$(GO) test -race -count=1 -timeout=120s $(PKGS)

.PHONY: test-short
test-short: ## Run unit tests with -short (used by pre-commit).
	$(GO) test -race -count=1 -timeout=60s -short $(PKGS)

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet $(PKGS)

.PHONY: lint
lint: ## Run golangci-lint.
	golangci-lint run

.PHONY: lint-fix
lint-fix: ## Run golangci-lint with --fix.
	golangci-lint run --fix

.PHONY: tidy
tidy: ## go mod tidy.
	$(GO) mod tidy

.PHONY: fuzz
fuzz: ## Run a short fuzz pass on registered targets (phase 1+).
	@echo "fuzz: no fuzz targets registered yet (phase 0)"

.PHONY: bench
bench: ## Run the benchmark suite with gate checks.
	bash bench/run.sh

.PHONY: benchstat
benchstat: ## Compare current bench results against the committed baseline.
	@command -v benchstat >/dev/null || $(GO) install golang.org/x/perf/cmd/benchstat@latest
	@test -f bench/results/baseline.txt || { echo "no baseline — run 'make bench' first and copy current.txt to baseline.txt"; exit 1; }
	benchstat bench/results/baseline.txt bench/results/current.txt

.PHONY: conformance
conformance: build ## Run the http-tests/cache-tests conformance harness.
	bash test/cachetests/run.sh

.PHONY: conformance-view
conformance-view: build ## Run conformance tests then open the comparison UI in a browser.
	bash test/cachetests/view.sh

.PHONY: integration
integration: testcerts ## Run docker-compose integration scenarios (phase 1+).
	go test -race -count=1 -timeout=10m -tags=integration ./test/integration/...

.PHONY: testcerts
testcerts: ## Generate ephemeral TLS certificates for integration tests.
	@mkdir -p test/integration/.tls
	@$(GO) run ./scripts/gen-testcerts -out test/integration/.tls
	@echo "test certs written to test/integration/.tls"

.PHONY: docs
docs: ## Build the documentation site (phase 4+).
	@echo "docs: site harness lands in phase 4.5"

.PHONY: schema
schema: ## Regenerate the JSON schema and SDK types (phase 3+).
	@echo "schema: generator lands in phase 3"

.PHONY: hooks
hooks: ## Install pre-commit hooks (commit + commit-msg + pre-push).
	@command -v pre-commit >/dev/null || { \
		echo "pre-commit is required. Install with: pip install pre-commit"; \
		exit 1; \
	}
	pre-commit install
	pre-commit install --hook-type commit-msg
	pre-commit install --hook-type pre-push
	@echo "pre-commit hooks installed."

.PHONY: hooks-run
hooks-run: ## Run pre-commit on all files (mirrors CI's first stage).
	pre-commit run --all-files --show-diff-on-failure

.PHONY: govulncheck
govulncheck: ## Run govulncheck.
	@command -v govulncheck >/dev/null || $(GO) install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck $(PKGS)

.PHONY: ci
ci: vet lint test-short build hooks-run ## Run the CI gate locally (vet, lint, test, build, hooks).

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BIN_DIR) coverage.* cover.html
