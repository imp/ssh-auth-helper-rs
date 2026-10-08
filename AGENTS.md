# Repository Guidelines

## Project Structure & Module Organization

`ssh-auth-helper` is a Rust 2024 binary crate. `src/main.rs` currently contains only the executable scaffold; the agent proxy is not implemented. `Cargo.toml` defines the package, `Cargo.lock` records dependency resolution, and ignored `target/` contains build outputs. Add focused modules under `src/` as behavior develops. Place unit tests beside their modules and integration tests under `tests/`.

## Build, Test, and Development Commands

- `cargo build --locked`: build using the recorded dependency versions.
- `cargo run --locked`: run the current executable.
- `cargo test --locked --all-targets`: run unit and integration tests.
- `cargo fmt --check`: check standard Rust formatting; use `cargo fmt` to apply it.
- `cargo clippy --locked --all-targets -- -D warnings`: lint all targets and reject warnings.

## Coding Style & Naming Conventions

Use rustfmt defaults, including four-space indentation. Use `snake_case` for modules, functions, and variables; `PascalCase` for types; and `SCREAMING_SNAKE_CASE` for constants. Keep socket handling, upstream selection, and management responsibilities distinct. Return contextual errors for expected failures rather than panicking.

## Testing Guidelines

Use Rust's built-in test framework and descriptive behavior names, such as `short_login_does_not_replace_live_agent`. No numeric coverage threshold is established. Cover changed behavior, especially concurrent startup, silent upstream failures, connection pinning, and malformed control messages. Use temporary directories and fake agents; real SSH/tmux checks must record versions and outcomes. Never edit personal SSH configuration during automated tests.

## Commit & Pull Request Guidelines

Use **jj exclusively** for repository operations, including `jj status`, `jj diff`, and `jj commit`. History is minimal; use concise, descriptive imperative messages. Pull requests should explain behavior changes, reference relevant requirements or issues, and report checks and platform limitations.

## Agent Discovery & Security

Confirm the graph project and generation with `index_status` before discovery. Prefer `search_graph`, `trace_path`, and `get_code_snippet`; check relevant paths with `check_index_coverage` and read source where coverage is incomplete. Use text search for configuration and literals.

Preserve upstream agent protocol state. Keep runtime sockets private, bound resource usage, and exclude signing payloads and credentials from diagnostics.
