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
bench: ## Run the benchmark suite (phase 1+).
	@echo "bench: harness lands in phase 1"

.PHONY: benchstat
benchstat: ## Compare HEAD bench results against main (phase 1+).
	@echo "benchstat: harness lands in phase 1"

.PHONY: conformance
conformance: ## Run the http-tests/cache-tests harness (phase 3+).
	@echo "conformance: harness lands in phase 3"

.PHONY: integration
integration: ## Run docker-compose integration scenarios (phase 4+).
	@echo "integration: harness lands in phase 4"

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
