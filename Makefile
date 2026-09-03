.PHONY: integration-test live-conformance build test lint fmt fmt-check clean help check doc

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the SDK
	cargo build

test: ## Run tests
	cargo test

lint: ## Run clippy lints
	cargo clippy --no-default-features --lib --tests -- -D warnings
	cargo clippy --all-features --all-targets -- -D warnings

fmt: ## Format code
	cargo fmt

fmt-check: ## Check formatting
	cargo fmt --all -- --check

clean: ## Clean build artifacts
	cargo clean

check: fmt-check lint ## Run all local release checks
	cargo check --no-default-features
	cargo check --all-features
	cargo test --no-default-features --lib --tests
	cargo test --all-features --all-targets
	cargo doc --all-features --no-deps

doc: ## Build documentation
	cargo doc --no-deps --open

integration-test: ## Run the container smoke test (requires Docker + STREAMLINE_TEST_IMAGE digest)
	@test -n "$(STREAMLINE_TEST_IMAGE)" || { \
		echo "STREAMLINE_TEST_IMAGE is not set."; \
		echo "Set it to an immutable digest, e.g."; \
		echo "  STREAMLINE_TEST_IMAGE=ghcr.io/streamlinelabs/streamline@sha256:<64 hex digits> make integration-test"; \
		exit 1; }
	cargo test --manifest-path testcontainers/Cargo.toml tests::container_starts_and_exposes_ports -- --ignored --exact

live-conformance: ## Run live conformance against a running broker (requires STREAMLINE_BOOTSTRAP_SERVERS)
	@test -n "$(STREAMLINE_BOOTSTRAP_SERVERS)" || { \
		echo "STREAMLINE_BOOTSTRAP_SERVERS is not set (for example 127.0.0.1:9092)."; \
		exit 1; }
	cargo test --test live_conformance -- --ignored
