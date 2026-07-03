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
	$(GO) test -race -count=1 -timeout=60s -parallel=8 $(PKGS)

.PHONY: test-short
test-short: ## Run unit tests with -short (used by pre-commit).
	$(GO) test -race -count=1 -timeout=30s -short -parallel=8 $(PKGS)

.PHONY: lint
lint: ## Run golangci-lint.
	golangci-lint run

.PHONY: vet
vet: ## Run go vet across all packages.
	$(GO) vet ./...

.PHONY: integration
integration: test-integration-cluster ## Alias: run the cluster integration suite.

.PHONY: chaos
chaos: test-chaos ## Alias: run the chaos scenarios.


.PHONY: bench
bench: ## Run the benchmark suite and compare against the committed baseline.
	bash bench/run.sh
	@command -v benchstat >/dev/null || $(GO) install golang.org/x/perf/cmd/benchstat@latest
	@test -f bench/results/baseline.txt || { echo "no baseline — copy bench/results/current.txt to bench/results/baseline.txt first"; exit 1; }
	benchstat bench/results/baseline.txt bench/results/current.txt

.PHONY: conformance
conformance: build ## Run the http-tests/cache-tests conformance harness.
	bash test/cachetests/run.sh

.PHONY: conformance-view
conformance-view: build ## Run conformance tests then open the comparison UI in a browser.
	bash test/cachetests/view.sh

.PHONY: test-integration-cluster
test-integration-cluster: test-integration-cluster-strong test-integration-cluster-eventual test-integration-cluster-full ## Run all 3 cluster consistency mode tests sequentially.

.PHONY: test-integration-cluster-strong
test-integration-cluster-strong: ## Run strong-mode cluster integration tests.
	@echo ">>> Cluster integration: STRONG mode"
	go test -v -race -count=1 -timeout=3m -tags=integration \
	    -run TestStrong ./test/integration/...

.PHONY: test-integration-cluster-eventual
test-integration-cluster-eventual: ## Run eventual-mode cluster integration tests.
	@echo ">>> Cluster integration: EVENTUAL mode"
	go test -v -race -count=1 -timeout=3m -tags=integration \
	    -run TestEventual ./test/integration/...

.PHONY: test-integration-cluster-full
test-integration-cluster-full: ## Run full-replication cluster integration tests.
	@echo ">>> Cluster integration: FULL mode"
	go test -v -race -count=1 -timeout=3m -tags=integration \
	    -run TestFull ./test/integration/...

.PHONY: test-chaos
test-chaos: ## Run chaos test scenarios in-process (one test per process to avoid registry collisions).
	@echo ">>> Chaos test suite"
	@for test in PeerKill OriginFlap PartialPartition SlowOrigin RollingRestart OriginDown ConcurrentPurgeUnderLoad NodeRejoinAfterLongPartition; do \
		echo "  $$test"; \
		go test -v -race -count=1 -timeout=5m -tags=integration -run "TestChaos_$$test" ./test/chaos/... || exit 1; \
		sleep 3; \
	done
	@echo ">>> All chaos tests passed"

.PHONY: soak
soak: ## Run a 24-hour (default) soak against a live cluster. Override with DURATION_H=N RPS=N.
	@echo ">>> Soak test ($(DURATION_H)h @ $(RPS) rps)"
	@test/chaos/soak.sh

.PHONY: testcerts
testcerts: ## Generate ephemeral TLS certificates for integration tests.
	@mkdir -p test/integration/.tls
	@$(GO) run ./scripts/gen-testcerts -out test/integration/.tls
	@echo "test certs written to test/integration/.tls"

.PHONY: templ
templ: ## Regenerate dashboard _templ.go files from *.templ sources.
	go generate ./internal/dashboard/templates/

.PHONY: hooks
hooks: ## Install pre-commit hooks (commit + commit-msg + pre-push).
	@command -v pre-commit >/dev/null || { \
		echo "pre-commit is required. Install with: brew install pre-commit"; \
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
ci: lint test-short build hooks-run ## Run the CI gate locally (lint, test, build, hooks).

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

# ---------------------------------------------------------------------------
# Load-test targets
# ---------------------------------------------------------------------------

LOADTEST_DIR := bench/loadtest
RESULTS_DIR  := $(LOADTEST_DIR)/results
CHARTS_DIR   := $(RESULTS_DIR)/charts
PYTHON       ?= python3

.PHONY: loadtest-setup
loadtest-setup: build ## Build all TUT images + origin for load testing.
	docker build -t bouine:loadtest .
	docker build -t bouine-test-origin:loadtest test/integration/origin/
	@echo "bouine:loadtest and bouine-test-origin:loadtest images built."
	@echo "Pull NGINX/Varnish/Envoy base images:"
	docker compose -f $(LOADTEST_DIR)/docker-compose.yaml pull nginx varnish envoy 2>/dev/null || true

.PHONY: loadtest-cluster
loadtest-cluster: ## Run cluster and stress scenarios (requires K8s).
	@mkdir -p $(RESULTS_DIR)
	@for scenario in 4.1_cluster_scaling 4.2_gossip_convergence 4.3_peer_fetch_pressure \
	                  4.4_hedging 4.5_rolling_update \
	                  5.1_connection_exhaustion 5.2_large_body 5.3_slow_origin \
	                  5.4_request_collapsing 5.5_purge_broadcast; do \
		echo "--- Running $$scenario ---"; \
		bash $(LOADTEST_DIR)/scenarios/$$scenario/run.sh; \
	done

.PHONY: loadtest-dashboard
loadtest-dashboard: ## Run dashboard-under-load scenarios.
	@mkdir -p $(RESULTS_DIR)
	@for scenario in 5.6a_dashboard_polling 5.6b_fanout_saturation \
	                  5.6c_dashboard_invalidation 5.6d_config_reload; do \
		echo "--- Running $$scenario ---"; \
		bash $(LOADTEST_DIR)/scenarios/$$scenario/run.sh; \
	done
	@echo "Note: the 6h ring test must be run manually:"
	@echo "  RING_TEST_DURATION=6h bash $(LOADTEST_DIR)/scenarios/5.6e_ring_memory_pressure/run.sh"

.PHONY: loadtest-clean
loadtest-clean: ## Remove load-test result files and charts.
	rm -rf $(RESULTS_DIR)
	@echo "Load test results cleaned."

.PHONY: loadtest
loadtest: loadtest-setup ## Run single-node scenarios and generate report.
	@mkdir -p $(RESULTS_DIR)
	docker compose -f $(LOADTEST_DIR)/docker-compose.yaml up -d bouine nginx varnish envoy origin
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@for scenario in 3.1_throughput_ramp 3.2_hit_only 3.3_miss_storm \
	                  3.4_working_set_overflow 3.5_vary_blowup 3.6_mixed_realistic; do \
		echo "--- Running $$scenario ---"; \
		docker compose -f $(LOADTEST_DIR)/docker-compose.yaml run --rm load-gen \
			bash /scenarios/$$scenario/run.sh; \
	done
	docker compose -f $(LOADTEST_DIR)/docker-compose.yaml down
	@command -v $(PYTHON) >/dev/null || { echo "python3 is required"; exit 1; }
	@$(PYTHON) -c "import plotly, kaleido" 2>/dev/null || \
		$(PYTHON) -m pip install -q plotly kaleido
	mkdir -p $(CHARTS_DIR)
	cd $(LOADTEST_DIR) && $(PYTHON) analysis/plot.py \
		--results-dir results \
		--output-dir results/charts
	cd $(LOADTEST_DIR) && $(PYTHON) analysis/report.py \
		--results-dir results \
		--charts-dir results/charts \
		--output REPORT.md
	@echo "Report: $(LOADTEST_DIR)/REPORT.md"
	@echo "Charts: $(CHARTS_DIR)/"

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
