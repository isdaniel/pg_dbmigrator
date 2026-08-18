.PHONY: check build format audit test doc-check before-git-push deps-check deps-bump deps-bump-dry \
       integration integration-offline integration-online \
       integration-install integration-install-arm64

deps-check:
	@echo "=== Checking for outdated dependencies ==="
	@cargo update --dry-run 2>&1 | grep -i "updating\|unchanged\|locking" || echo "All dependencies up to date"

deps-bump:
	@echo "=== Bumping dependencies to latest compatible versions ==="
	cargo update
	@echo "=== Verifying build (workspace) ==="
	cargo check --workspace --all-targets
	@echo "=== Running tests ==="
	cargo test --workspace
	@echo "Done. Review changes with: git diff Cargo.lock"

deps-bump-dry:
	@echo "=== Dry-run: what would be updated ==="
	@cargo update --dry-run 2>&1

check:
	cargo check
	cargo clippy --workspace --all-targets --locked -- -D warnings

build:
	cargo build

format:
	cargo fmt

audit:
	cargo audit

test:
	cargo test

doc-check:
	cargo doc --no-deps --all-features

before-git-push: check build format audit test doc-check 

integration:
	bash tests/integration/run_all.sh all

integration-offline:
	bash tests/integration/run_all.sh offline

integration-online:
	bash tests/integration/run_all.sh online

# install.sh end-to-end: builds a static musl binary, serves it as a fake
# release, then installs it in clean ubuntu/rocky/alpine containers.
integration-install:
	bash tests/integration/run_install_script.sh x86_64

# Same, for aarch64. Needs a cross toolchain and qemu on an x86_64 host:
#   cargo install cross --locked
#   docker run --privileged --rm tonistiigi/binfmt --install arm64
integration-install-arm64:
	bash tests/integration/run_install_script.sh aarch64
