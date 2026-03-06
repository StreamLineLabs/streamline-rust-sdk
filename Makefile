.PHONY: integration-test build test lint fmt clean help check

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the SDK
	cargo build

test: ## Run tests
	cargo test

lint: ## Run clippy lints
	cargo clippy --all-targets -- -D warnings

fmt: ## Format code
	cargo fmt

fmt-check: ## Check formatting
	cargo fmt --all -- --check

clean: ## Clean build artifacts
	cargo clean

check: fmt-check lint test ## Run all checks

doc: ## Build documentation
	cargo doc --no-deps --open

integration-test: ## Run integration tests (requires Docker)
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for Streamline server..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health/live > /dev/null 2>&1; then \
			echo "Server ready"; \
			break; \
		fi; \
		sleep 2; \
	done
	cargo test --features integration-tests -- --test-threads=1 || true
	docker compose -f docker-compose.test.yml down -v
