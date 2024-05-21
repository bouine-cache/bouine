 .DEFAULT_GOAL := help

NAME = bouine
GOLANGCI_LINT_TIMEOUT ?= 1m

.PHONY: pre-commit-install
pre-commit-hooks-install: ## Install the pre-commit hooks
	pre-commit install

.PHONY: build
build: build-server build-cli build-docker-image ## Build Go binaries & Docker image

.PHONY: build-server
build-server: ## Build Go server for present architecture
	GOOS=linux GOARCH=amd64 go build -o build/$(NAME) ./cmd

.PHONY: build-cli
build-cli: ## Build bouine CLI to manipulate bouine clusters
	go build -o build/$(NAME)ctl ./cmd/cli

.PHONY: build-docker-image
build-docker-image: ## Build Docker image
	docker build -t thylong/$(NAME):latest .
	docker-compose build bouine

.PHONY: push-docker-image
push-docker-image: ## Push Docker image
	docker trust sign thylong/$(NAME):latest

.PHONY: scan-docker-image
scan-docker-image: ## Scan latest local bouine image (using docker scan from Snyk)
	docker scan --dependency-tree --severity=low thylong/bouine

.PHONY: docker-cleanup
docker-cleanup: ## Delete all the docker-compose containers
	docker-compose down --remove-orphans

.PHONY: test
test: lint license-check scan-docker-image test-bench test-unit test-e2e test-perf test-cleanup ## Launch all tests sequentially

.PHONY: lint
lint: ## Scan repository with linters
	golangci-lint run -c .golangci.yaml --timeout $(GOLANGCI_LINT_TIMEOUT)

.PHONY: test-bench
test-bench: ## Launch Go benchmark tests
	go test -bench -benchmem ./...

.PHONY: test-unit
test-unit: ## Launch Go unit tests
	go test -cover ./...

.PHONY: test-e2e
test-e2e: ## Launch k6 e2e tests (k6 debug option: --http-debug)
	docker-compose down --remove-orphans; \
	docker-compose run --env BASE_URL="http://bouine1:8080" --rm test-client run --http-debug /scenarios/e2e-tests-rfc7234.js

.PHONY: test-e2e-distributed
test-distributed-e2e: ## Launch k6 e2e tests (k6 debug option: --http-debug)
	docker-compose down --remove-orphans && \
	docker-compose up -d --wait nginx bouine1 bouine2 bouine3 && \
	sleep 2 && \
	raftadmin localhost:50051 add_voter nodeB bouine2:50052 0 && \
	raftadmin --leader multi:///localhost:50051,localhost:50052 add_voter nodeC bouine3:50053 0 && \
	docker-compose run --env BASE_URL="http://nginx:4000" --rm test-client run --http-debug /scenarios/e2e-tests-rfc7234.js

.PHONY: test-perf
test-perf: ## Launch Go unit tests (k6 debug option: --http-debug)
	docker-compose down --remove-orphans; \
	docker-compose run --rm test-client run --out influxdb=http://influxdb:8086/myk6db /scenarios/e2e-tests-rfc7234.js

.PHONY: test-perf
test-cleanup: ## Launch Go unit tests (k6 debug option: --http-debug)
	rm -rf /tmp/bouine

.PHONY: doc
doc: ## Update documentation
	go doc -http:=6060

.PHONY: license-check
license-check: ## Check if licence headers are missing on any files
	docker run -it --rm -v $(PWD):/github/workspace apache/skywalking-eyes header check

.PHONY: license-fix
license-fix: ## Fix missing licence headers on any files
	docker run -it --rm -v $(PWD):/github/workspace apache/skywalking-eyes header fix

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
