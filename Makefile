.DEFAULT_GOAL := help

NAME = bouine
GOLANGCI_LINT_TIMEOUT ?= 1m

.PHONY: pre-commit-install
pre-commit-hooks-install: ## Install the pre-commit hooks
	pre-commit install

.PHONY: build
build: build-binary build-docker-image build-packer-images ## Build Go binaries, Packer images & Docker image

.PHONY: build-packer-images
build-packer-images: build-client-vm-image build-server-vm-image ## Build k6 client & bouine server instance base images with Packer

.PHONY: build-client-vm-image
build-client-vm-image: ## Build k6 client instance base image with Packer
	packer build -var "project_id=$(SCW_DEFAULT_PROJECT_ID)" -var="access_key=$(SCW_ACCESS_KEY)" -var="secret_key=$(SCW_SECRET_KEY)" build/client/packer.json

.PHONY: build-server-vm-image
build-server-vm-image: ## Build bouine server instance base image with Packer
	packer build -var "project_id=$(SCW_DEFAULT_PROJECT_ID)" -var="access_key=$(SCW_ACCESS_KEY)" -var="secret_key=$(SCW_SECRET_KEY)" build/server/packer.json

.PHONY: build-binary
build-binary: ## Build Go binary for present architecture
	GOOS=linux GOARCH=amd64 go build -o build/server/files/$(NAME) ./api

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
test: lint license-check scan-docker-image test-bench test-unit test-smoke test-perf  ## Launch all tests sequentially

.PHONY: lint
lint: ## Scan repository with linters
	golangci-lint run -c .golangci.yaml --timeout $(GOLANGCI_LINT_TIMEOUT)

.PHONY: test-bench
test-bench: ## Launch Go benchmark tests
	go test -bench -benchmem ./...

.PHONY: test-unit
test-unit: ## Launch Go unit tests
	go test -cover ./...

.PHONY: test-smoke
test-smoke: ## Launch k6 smoke tests (k6 debug option: --http-debug)
	docker-compose down --remove-orphans; \
	docker-compose run --rm test-client run --http-debug /scenarios/smoke-tests-rfc7234.js

.PHONY: test-perf
test-perf: ## Launch Go unit tests (k6 debug option: --http-debug)
	docker-compose down --remove-orphans; \
	docker-compose run --rm test-client run --out influxdb=http://influxdb:8086/myk6db /scenarios/smoke-tests-rfc7234.js

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
