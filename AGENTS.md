# Repository Guidelines

## Project Structure & Module Organization

This Rust 2024 workspace targets Linux and macOS:

- `ssh-auth-helper/`: executable, Clap options, async entry point, detached launcher, and CLI/process tests.
- `ssh-auth-helper-lib/`: runtime security, daemon supervision, registry, control client/server, probes, and pinned relays.
- `ssh-auth-helper-api/`: management request/response schemas and the `Request` trait.
- `jsonrpc-uds/`: bounded typed JSON-RPC codec and wire types.

Unit tests live beside modules; Rust integration tests live in each crate's `tests/`. `examples/` contains manual shell/tmux integration, `docs/integration.md` explains installation, and `tests/integration_examples.py` checks the examples. `Cargo.lock` records dependencies; `target/` is ignored.

## Build, Test, and Development Commands

Use stable Rust from `rust-toolchain.toml` (declared minimum: 1.99); members inherit workspace lints.

- `cargo build --locked --workspace`: build all packages.
- `cargo run --locked -p ssh-auth-helper -- --help`: inspect current CLI commands.
- `cargo test --locked --workspace --all-targets`: run ordinary Rust tests.
- `cargo fmt --all --check`: check formatting; apply with `cargo fmt --all`.
- `cargo clippy --locked --workspace --all-targets -- -D warnings`: reject lint warnings.
- `python3 tests/integration_examples.py --binary "$PWD/target/debug/ssh-auth-helper"`: check examples after building; requires Python 3, with optional shell/tmux checks.

## Coding Style & Naming Conventions

Use rustfmt defaults, four-space indentation, and one imported item per line. Use `snake_case` functions/modules, `PascalCase` types, and `SCREAMING_SNAKE_CASE` constants. Prefer type-owned construction/validation and `From`/`Into`; name operation payloads `XxxRequest`/`XxxResponse`. Use Camino paths, `camino-tempfile` fixtures, `parking_lot` locks, Jiff timestamps, and Tokio I/O/deadlines. Return contextual errors for expected failures.

## Testing Guidelines

Use Rust tests and `#[tokio::test]` with descriptive behavior names. No numeric coverage threshold is set. Cover startup races, malformed frames, silent agents, pinning, and no replay. Keep environment changes in child processes; construct options directly for unit tests. Process fixtures require Python 3 and `ps`. See README for opt-in OpenSSH and isolated Linux root tests; never run privileged fixtures on the host or alter personal SSH/shell/tmux configuration.

## Commit & Pull Request Guidelines

Use **jj exclusively**, never direct Git commands. Write concise, descriptive commit messages. Pull requests should explain behavior, reference relevant issues, and report checks and platform limits.

## Agent Discovery & Security

When graph tools are available, confirm project/generation, prefer graph discovery, and check index coverage. Read source for gaps; use text search for configuration or unavailable tools.

Preserve upstream agent protocol state. Keep runtime sockets private, bound resource usage, and exclude signing payloads and credentials from diagnostics.
