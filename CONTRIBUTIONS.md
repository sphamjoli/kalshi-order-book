# Contributions

## Setup

Copy `.env.example` to `.env`, then fill in your Kalshi key ID and private key path. Never commit `.env` or key files.

## Usage

The [README](README.md#usage) covers every command with examples.

## Git hooks and CI

Install the hooks once, after installing [lefthook](https://github.com/evilmartians/lefthook):

```bash
make hooks
```

Commits are checked for formatting, valid YAML, TOML, shell scripts and workflows, and a conventional commit subject. Pushes run clippy and the tests. GitHub Actions repeats these checks and adds `cargo-deny`, `cargo-machete` and a gitleaks secret scan.

Run every check locally, the same as CI:

```bash
make check
```

That covers `make lint`, `make test`, `make audit` and `make validate`, which can also run alone. It needs `cargo-deny` and `cargo-machete`. The secret scan is separate and needs gitleaks:

```bash
make secrets
```

## Development loop

Before sending a change, run the tests and lints:

```bash
make test
```

```bash
make lint
```

If the change touches sockets or output, also run an end-to-end check against Kalshi:

```bash
make smoke
```

## Reproducing the evaluation

Run all experiments in order, about an hour in total:

```bash
make experiments
```

Or run a single experiment:

```bash
make socket-size
```

Then print the results:

```bash
make summary
```

Experiments need valid credentials. `make backlog` and `make link` use macOS tools. Logs go to `results/`, which is not committed.

## Testing

```bash
cargo test
```

All tests live in `tests/`, not in inline modules.

## Conventions

Run `make fmt` before committing. `make lint`, the commit hook and CI all reject unformatted Rust, so formatting stays the same everywhere. `.editorconfig` sets the same rules for other files in your editor.

## SBE schema

The schema is defined in `sbe_schemas/engineering_challenge/challenge_orderbook.yaml`, which generates the XML and the Rust types. These files should not need editing. Timestamps are `chrono::DateTime` in Rust and microseconds since the Unix epoch on the wire.

## Diagrams

Diagrams are drawn in Excalidraw. Sources are the `.excalidraw` files in `docs/diagrams`.
