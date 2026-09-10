# Single entry point for common development tasks.
# Run `make help` to list every target.

.DEFAULT_GOAL := help

# Local Postgres DSN matching docker-compose.yml.
DATABASE_URL ?= postgres://sync:sync@localhost:5432/sync?sslmode=disable

# Pinned so local runs and CI use the same linter build.
GOLANGCI_VERSION ?= v2.13.2
GOLANGCI := docker run --rm -v "$(CURDIR)":/app -w /app golangci/golangci-lint:$(GOLANGCI_VERSION) golangci-lint

# Version stamped into the binary at build time.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: run
run: ## Run the service from source
	go run ./cmd/syncd

.PHONY: build
build: ## Build the binary into bin/syncd
	go build -ldflags="$(LDFLAGS)" -o bin/syncd ./cmd/syncd

.PHONY: test
test: ## Run tests with the race detector
	go test ./... -race -count=1

.PHONY: cover
cover: ## Run tests and open an HTML coverage report
	go test ./... -race -count=1 -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html
	@echo "report: coverage.html"

.PHONY: lint
lint: ## Run golangci-lint (via Docker, no local install needed)
	go vet ./...
	$(GOLANGCI) run ./...

.PHONY: fmt
fmt: ## Apply gofumpt + gci formatting
	$(GOLANGCI) fmt ./...

.PHONY: config-verify
config-verify: ## Validate .golangci.yml against the linter's schema
	$(GOLANGCI) config verify

.PHONY: tidy
tidy: ## Tidy go.mod / go.sum
	go mod tidy

.PHONY: docker-build
docker-build: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t data-sync-service:$(VERSION) .

.PHONY: compose-up
compose-up: ## Start the local Postgres container
	docker compose up -d

.PHONY: compose-down
compose-down: ## Stop the local Postgres container (keeps the volume)
	docker compose down

.PHONY: compose-clean
compose-clean: ## Stop the local Postgres container and delete its volume
	docker compose down -v

.PHONY: install-hooks
install-hooks: ## Install the repository's git pre-commit hook
	@ln -sf ../../scripts/hooks/pre-commit .git/hooks/pre-commit
	@echo "installed .git/hooks/pre-commit"

.PHONY: clean
clean: ## Remove build artifacts and coverage output
	rm -rf bin coverage.out coverage.html
