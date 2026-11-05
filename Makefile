# Reproduces the checks and experiments reported in README.md. Experiments
# need valid credentials in .env and write logs to results/.

BIN := ./target/release/engineering-challenge

.PHONY: help fmt check validate secrets hooks audit install build test lint smoke experiments baseline socket-size backlog link summary clean-results

help: ## Lists the targets and the program's own help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | expand -t 18
	@echo
	@cargo run --quiet --release -- --help

fmt: ## Formats the Rust code
	cargo fmt --all

check: lint test audit validate ## Runs every local CI check

validate: ## Checks YAML, TOML, shell scripts and workflows
	./scripts/checkYaml.sh lefthook.yml .github/dependabot.yml .github/workflows/*.yml
	./scripts/checkToml.sh Cargo.toml deny.toml .gitleaks.toml
	./scripts/checkShell.sh scripts/*.sh
	./scripts/checkActions.sh .github/workflows/*.yml

secrets: ## Scans for secrets (needs gitleaks)
	gitleaks dir . --config .gitleaks.toml --redact --no-banner

hooks: ## Installs the git hooks (needs lefthook)
	lefthook install

audit: ## Checks dependencies and unused crates
	cargo deny check
	cargo machete

install: ## Installs engineering-challenge on your PATH
	cargo install --path . --locked

build: ## Builds the release binary
	cargo build --release

test: ## Runs the automated tests
	cargo test --locked

lint: ## Checks formatting and lints
	cargo fmt --all --check
	cargo clippy --all-targets -- -D warnings

smoke: build ## Records 100 markets for 60 seconds, then decodes the file
	KEEP_OUTPUT=1 scripts/run_experiment.sh smoke 60 --max-markets 100
	$(BIN) verify results/smoke.sbe

baseline: build ## Scan with no sockets, 5 minutes
	scripts/run_experiment.sh baseline 300

socket-size: build ## 2,000 then 500 markets per socket, 15 minutes each
	scripts/run_experiment.sh size-2000 900 --markets-per-socket 2000 --feeds 1
	scripts/run_experiment.sh size-500 900 --markets-per-socket 500 --feeds 1

backlog: build ## 1,000 per socket with throughput and backlog sampling (macOS)
	SAMPLE=1 scripts/run_experiment.sh backlog 1350 --markets-per-socket 1000 --feeds 1

link: build ## Speed test idle, then during collection (macOS)
	mkdir -p results
	networkQuality -s | tee results/link-idle.txt
	LOAD_TEST_AFTER=720 scripts/run_experiment.sh link 840 --markets-per-socket 1000 --feeds 1

experiments: baseline socket-size backlog link summary ## Runs every experiment in order, about 1 hour

summary: ## Prints the tables from results/
	@scripts/summarise.sh

clean-results: ## Deletes results/
	rm -rf results
