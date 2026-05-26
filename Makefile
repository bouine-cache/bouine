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

.PHONY: templ
templ: ## Regenerate dashboard _templ.go files from *.templ sources.
	go generate ./internal/dashboard/templates/

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

.PHONY: test-k8s-setup
test-k8s-setup: build ## Build images and deploy bouine + test origin on Kubernetes.
	docker build -t bouine:dev .
	docker build -t bouine-test-origin:dev test/integration/origin/
	-kubectl create namespace bouine-test 2>/dev/null
	-kubectl -n bouine-test delete pod origin --force 2>/dev/null
	-kubectl -n bouine-test delete svc origin 2>/dev/null
	kubectl -n bouine-test run origin --image=bouine-test-origin:dev --image-pull-policy=Never --port=8080
	kubectl -n bouine-test expose pod origin --port=8080
	kubectl -n bouine-test wait --for=condition=ready pod/origin --timeout=60s
	helm upgrade --install bouine deploy/helm/bouine \
		--namespace bouine-test \
		--set image.repository=bouine \
		--set image.tag=dev \
		--set image.pullPolicy=Never \
		--set replicaCount=3 \
		--set config.listen.https="" \
		--set config.listen.http3="" \
		--set config.storage.hot_max_bytes=256MiB \
		--set config.storage.warm_dir="" \
		--set warmVolume.enabled=false \
		--set topologySpreadConstraints="" \
		--set config.cluster.enabled=true \
		--set "config.upstream_pools[0].name=origin" \
		--set "config.upstream_pools[0].targets[0]=origin.bouine-test.svc:8080" \
		--set "config.routes[0].pool=origin"
	kubectl -n bouine-test rollout status statefulset/bouine --timeout=120s
	@echo ""
	@echo ">>> bouine deployed. Run tests with:"
	@echo ">>>   kubectl -n bouine-test port-forward svc/bouine 8080:80 &"
	@echo ">>>   curl -sI http://localhost:8080/hit"
	@echo ">>> See test/integration/TESTPLAN.md for all scenarios."

.PHONY: test-k8s-teardown
test-k8s-teardown: ## Remove bouine + test origin from Kubernetes.
	-helm uninstall bouine --namespace bouine-test
	-kubectl delete namespace bouine-test

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BIN_DIR) coverage.* cover.html

.PHONY: admin-token
admin-token: ## Show the admin bearer token (usage: make admin-token CONFIG=config.yaml).
	@CONFIG=$${CONFIG:-config/default.yaml}; \
	if [ ! -f "$$CONFIG" ]; then \
		echo "Config not found: $$CONFIG  (override with CONFIG=path/to/config.yaml)"; \
		echo "If bouine is running without a config token, check its logs:"; \
		echo "  ./bin/bouine serve ... 2>&1 | grep 'admin token'"; \
		exit 1; \
	fi; \
	TOKEN=$$(grep -E '^\s*token:' "$$CONFIG" | head -1 | sed 's/.*token:[[:space:]]*//' | tr -d '"' | tr -d "'"); \
	if [ -z "$$TOKEN" ]; then \
		echo "No token set in $$CONFIG."; \
		echo "bouine auto-generates one at startup — check logs:"; \
		echo "  ./bin/bouine serve --config $$CONFIG 2>&1 | grep 'admin token'"; \
		echo "Or set it explicitly:"; \
		echo "  admin:"; \
		echo "    token: your-secret-token"; \
	else \
		echo "$$TOKEN"; \
	fi

.PHONY: release
release: ## Create a GitHub release (usage: make release TAG=v0.1.0).
	@test -n "$(TAG)" || { echo "usage: make release TAG=v0.1.0"; exit 1; }
	@command -v gh >/dev/null || { echo "gh CLI is required: https://cli.github.com"; exit 1; }
	@printf "Release description (one line): "; \
	read -r DESC; \
	REPO=$$(gh repo view --json nameWithOwner -q .nameWithOwner); \
	PREV=$$(git describe --tags --abbrev=0 2>/dev/null || echo ""); \
	if [ -n "$$PREV" ]; then \
		COMMITS=$$(git log --format='- %s ([%h](https://github.com/'"$$REPO"'/commit/%H))' $$PREV..HEAD --no-merges); \
	else \
		COMMITS=$$(git log --format='- %s ([%h](https://github.com/'"$$REPO"'/commit/%H))' --no-merges); \
	fi; \
	NOTES=$$(printf "%s\n\n### Changes\n\n%s" "$$DESC" "$$COMMITS"); \
	gh release create $(TAG) --target main \
		--title "$(TAG)" \
		--notes "$$NOTES"
