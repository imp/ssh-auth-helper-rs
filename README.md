# ssh-auth-helper

A per-user SSH agent proxy for Linux and macOS, written in Rust. It keeps a
stable agent socket usable across remote SSH connections and long-lived tmux
sessions while at least one registered forwarded agent remains reachable.

The current CLI provides path discovery and a foreground daemon with private
sockets, singleton locking, bounded control readiness, crash recovery,
identity-checked cleanup, socket registration/status, sticky upstream selection
and transparent agent forwarding. Registration starts a detached daemon when needed.

For daily use, follow the [SSH, shell and tmux setup guide](docs/integration.md).
It includes manually installed examples, readiness checks, original-socket
recovery and tmux configuration that preserves unrelated environment updates.

## Commands

Build from source with stable Rust using `cargo build --locked --workspace`.
The executable is `target/debug/ssh-auth-helper`; the setup guide includes a
manual installation command. Use `ssh-auth-helper --help` for current options.

| Command | Behavior |
| --- | --- |
| `socket-path` | Print the stable agent path; no filesystem changes or startup |
| `serve` | Run in the foreground until SIGINT/SIGTERM |
| `register` | Register an original socket, starting a detached daemon if needed |
| `unregister` | Idempotently remove an instance-qualified registration ID |
| `status` | Inspect the running daemon without probing upstream agents |

`register`, `unregister` and `status` accept `--json`. Explicit selection and
watch/events are not implemented. `hello` is a control-protocol method, not a
CLI subcommand.

```sh
cargo run -p ssh-auth-helper --locked -- socket-path
cargo run -p ssh-auth-helper --locked -- --runtime-dir /tmp/my-helper socket-path
```

## Runtime paths and safety

An explicit runtime directory takes precedence over `XDG_RUNTIME_DIR`.
Otherwise the runtime directory is `$XDG_RUNTIME_DIR/ssh-auth-helper`, or
`/tmp/ssh-auth-helper-<uid>` when that variable is absent. Path discovery makes
no filesystem changes and does not establish that a directory is safe to use.
Inputs must be absolute UTF-8 paths without traversal, NULs, or line breaks;
socket path lengths are checked in bytes for Linux and macOS.

The library also provides `RuntimeDirectory::prepare`: it creates only the
final helper directory, retains its descriptor, checks ownership and mode
`0700`, and inspects existing endpoint types, owners, and mode `0600` without
replacing them. Ancestors must be trusted; only root-owned ancestor aliases
are accepted, and the helper directory itself cannot be a symlink. Existing
unsafe objects are rejected without permission repair. Peer authorization
uses kernel Unix-stream credentials and requires the same UID on both listeners.
The stable agent endpoint is `agent.sock`; management uses the separate
`control.sock` endpoint in the same directory.
The daemon holds `daemon.lock` for its lifetime and leaves that file in place.
Only the lockholder recovers owned stale sockets; any successful connection
protects an existing listener, even if silent or incompatible.

Run the daemon in the foreground using an isolated path whose parent exists:

```sh
cargo run -p ssh-auth-helper --locked -- --runtime-dir /tmp/my-helper serve
```

SIGINT or SIGTERM stops acceptance, joins connection tasks, and removes only
this instance's matching socket entries. A crash may leave socket files; the
next lockholder recovers them after connection refusal and identity checks.
Unexpected objects are preserved and reported. Recovery and cleanup operate
within a same-user trust boundary; pathname checks are not atomic protection
against a malicious process running as that same user. Runtime storage must
be on a local filesystem.

## Control protocol

A local client can check readiness independently of agent availability:

```python
import socket
with socket.socket(socket.AF_UNIX) as client:
    client.settimeout(2)
    client.connect("/tmp/my-helper/control.sock")
    client.sendall(b'{"jsonrpc":"2.0","id":"check","method":"hello"}\n')
    print(client.makefile("r").readline().strip())
```

The reply contains the daemon instance UUID, `ready`, and both endpoint paths.
Readiness means listeners/handlers are initialized, not that a forwarded agent
is available. Agent clients close without a fabricated reply when no upstream
can be selected within the setup budget. The control socket supports `hello`, `register`, `unregister` and `status`;
other operations return JSON-RPC `Method not found` (-32601). Envelopes require exactly
`jsonrpc: "2.0"`; responses contain one result or error and echo the request ID.
IDs may be nonempty strings of 1..128 UTF-8 bytes, unsigned integers or null.
Negative/fractional IDs are outside this documented profile. An absent ID is
a notification and receives no response. Method params use strict schemas.

Frames are limited to 64 KiB excluding LF, incomplete requests and writes to
two seconds, and active control tasks to 32. LF, CRLF and a complete final
line at EOF are accepted. Invalid JSON receives Parse error (-32700); malformed
envelopes receive Invalid Request (-32600); invalid method params receive
Invalid params (-32602). Duplicate envelope fields are rejected. Method payloads
use ordinary JSON values; duplicate keys inside params/results keep the last
value. Output is serialized into a fixed-capacity buffer before it is queued,
and string newlines are escaped. This control protocol uses a restricted
JSON-RPC profile: each line contains exactly one message object. Top-level
arrays, including batches, receive one Invalid Request (-32600) response with
null ID, without executing any member. Multiple JSON values on one line receive
Parse error (-32700). Arrays inside params/results are allowed. A connection
can carry successive individual messages; invalid input does not prevent a
later valid call unless a framing error closes the stream. Outgoing replies obey
the same 64 KiB limit; overflow closes the stream without sending a partial frame.
Clients never automatically replay uncertain operations. Automatic startup,
readiness and registration share a five-second budget.

## Registration and daemon lifecycle

Register an original SSH-forwarded socket; the daemon starts if needed:

```sh
ssh-auth-helper --runtime-dir /tmp/my-helper register --socket "$SSH_AUTH_SOCK" --label laptop --json
ssh-auth-helper --runtime-dir /tmp/my-helper status --json
ssh-auth-helper --runtime-dir /tmp/my-helper unregister INSTANCE_UUID/REGISTRATION_UUID
```

`register` defaults to the `SSH_AUTH_SOCK` captured by Clap; an explicit
`--socket` overrides it. The selected path must be absolute UTF-8 and resolve
to a current-user-owned Unix socket. Direct paths, symlinks and hard links to
either helper endpoint are rejected. Successful registration and unregistration
produce no stdout unless `--json` is requested. `status` prints a concise summary
by default. Only `register` ensures startup; `status` and `unregister` require
an already running daemon. `socket-path` only prints a path.
Clap captures `XDG_RUNTIME_DIR` and `SSH_AUTH_SOCK` when parsing CLI options;
library runtime discovery takes explicit inputs without reading the environment.

Automatic startup executes the same binary in a separate session with standard
streams redirected to `/dev/null` and extra inherited descriptors closed on exec.
Concurrent registrations converge through the lifetime lock and authenticated
hello handshake. A silent or incompatible control listener never authorizes
takeover. Startup failures are reported on stderr; use foreground `serve` with
the same runtime directory to diagnose a failed startup when no daemon is running.
The registering command's exit or failure does not stop the daemon or undo other
registrations. Host logout policies can still terminate detached user processes;
use a persistent user service where required by the host. No service installation
or SSH/shell/tmux configuration is performed automatically.

Repeat registration of the same device/inode returns its original ID and
metadata without changing its order. Replacement at the same path retires the
old entry and creates a new instance-qualified ID. Unregister is idempotent;
an old or foreign-instance ID cannot remove a replacement. A daemon restart
loses its registry; surviving original sockets need to be registered again.
Register the original forwarded socket, rather than the helper's stable socket.

The registry holds at most 64 candidates and 64 recent retirements. Labels are
limited to 128 UTF-8 bytes. A snapshot-size budget also applies: old retirement
records are trimmed first, and registration is rejected without mutation if
the remaining snapshot would exceed the control-message budget. Socket
validation and snapshot preparation must finish before the mutation deadline.
Use local filesystem socket paths. Registration does not connect to an agent;
new registrations have `unobserved` health until an agent client triggers a probe.
Status then reports the last observed health/time, retry delay, active registration,
relay counts, setup failure and neutral relay-error metadata. Admission reserves
space for later health/counter growth so status remains within the frame limit.

Application errors carry metadata-only `error.data.kind` values:

| Code | Kind | Meaning |
| --- | --- | --- |
| -32001 | `unsafe_path` | Missing, invalid, non-socket or foreign-owned upstream |
| -32002 | `recursive_upstream` | Helper endpoint or its filesystem alias |
| -32003 | `capacity_exceeded` | Candidate or snapshot-size capacity exhausted |
| -32004 | `deadline_exceeded` | Operation budget exhausted before mutation |
| -32602 | `label_too_long` | Label exceeds 128 UTF-8 bytes |

## Library client

The library exposes `control::Client`: `Client::connect(&paths).await?` authorizes
the peer and validates hello under a two-second connect/hello deadline. Its
`register`, `unregister` and `status` methods reuse that authenticated connection,
each with a fresh two-second send/receive deadline. The CLI connects for its one
command and drops the client on return. Ordinary commands can use up to four
seconds for connection and operation; `register` also allows detached startup
within its five-second total budget. The library Client never starts a process.

```rust
let mut client = ssh_auth_helper_lib::control::Client::connect(&paths).await?;
let registration = client.register(request).await?;
let snapshot = client.status().await?;
```

Transport errors, invalid replies, timeouts and cancellation close the client
connection; subsequent methods return `ControlError::Disconnected`. Construct a
new client explicitly to reconnect. A correlated RPC error leaves the connection
usable. The daemon still closes control connections idle for two seconds, so an
idle client may need to reconnect. There are no automatic retries or keepalives.
An unusable reply after registration/unregistration can leave an uncertain
outcome; inspect status through a new client before deciding to repeat a mutation.

## Workspace and development

The workspace declares Rust 1.99 as its minimum version. Use the stable
toolchain selected by `rust-toolchain.toml`:

```sh
cargo build --locked --workspace
cargo test --locked --workspace --all-targets
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

The Cargo workspace has four peer packages, all inheriting shared Rust/Clippy lints:

- `ssh-auth-helper`: executable package with CLI parsing, command dispatch,
  the current-thread Tokio entry point, detached process launch and output. It
  builds the `ssh-auth-helper` binary. CLI/process integration tests live here.
- `ssh-auth-helper-lib`: core library for runtime safety, lifetime locking,
  daemon supervision, registry/selection, control client/server and forwarding.
  It has no executable target, detached launcher or Clap dependency.
- `ssh-auth-helper-api`: our helper's management API schemas, including
  `HelloRequest`/`HelloResponse`. Operation payloads use `XxxRequest`/`XxxResponse`.
- `jsonrpc-uds`: bounded JSON-RPC parsing and serialization with typed
  messages through `Framed<_, JsonRpcCodec>`. Requests, notifications and responses
  are typed; params/results use `serde_json::Value`. The core library
  owns dispatch, deadlines and readiness calls over the typed stream. Callers
  authorize endpoints and peers before constructing the framed stream.

The wire types are small owned Serde types, with no RPC framework or raw JSON
payloads. Each API request implements `api::Request`, associating its method name
with its response type. Client calls and server dispatch share typed encoding and
decoding while per-operation validation stays in the core library. Readiness sends
one identified request and checks its response under one absolute
connect/send/receive budget. Every return closes that connection;
there are no background client tasks or automatic request replays.

`RpcError` owns standard error codes and constructors; `RpcMessage::decode_request`
extracts requests/notifications or returns an error response for rejected input.
API schemas define wire contracts; the core library validates filesystem state
and operation semantics. Configuration/environment parsing stays in the executable.

Implementation uses Camino paths, `camino-tempfile` for Rust fixtures,
`parking_lot` for short shared-state locks, Jiff UTC timestamps, and Tokio for
asynchronous I/O and monotonic deadlines. Members inherit workspace Rust/Clippy
lints. Import one item per line and use `XxxRequest`/`XxxResponse` payload names.

## Selection and forwarding

Register each SSH session's original socket before selecting the stable path.
The [setup guide](docs/integration.md) and [shell example](examples/shell-agent.sh)
retain that original for recovery and check daemon readiness before exporting
the proxy path.

Each new client sends a bounded identities probe on a fresh upstream connection.
A structurally valid empty identities reply is healthy. The selected upstream
stays active while healthy; a brief registration of another agent does not
preempt it. Failed probes allow fallback in registration order. Transient failures
use exponential backoff; missing, replaced or unsafe socket objects retire the
registration. A recovered older agent does not preempt the current healthy one.
Availability does not guarantee a particular key or signing permission: different
laptops may hold different identities, and signing can still require confirmation.

The relay uses the exact successfully probed stream, preserving per-connection
agent state. After the probe, bytes pass unchanged in both directions, including
extensions. Existing clients remain pinned when registrations change. An uncertain
signing request is never replayed on another agent. There is no short signing or
idle timeout; bidirectional copy propagates half-closes and applies backpressure.
An unattributed relay I/O error is diagnostic metadata, not proof that an upstream
is dead. The next client probes again. This transparency does not itself establish
compatibility with every agent extension or destination-constraint policy.

Default forwarding bounds (also returned by `status --json`):

| Bound | Default |
| --- | --- |
| Per-upstream connect/probe | 750 ms |
| Total setup, including probe-queue waiting | 3 s |
| Retry backoff | 1 s, doubling to 30 s |
| Identities reply body | 1 MiB |
| Concurrent probes per registration | 1 |
| Agent setup/relay clients | 128, independent of 32 control clients |
| Relay buffer per direction | 64 KiB |

The library's `ForwardingConfig` permits validated smaller limits for embedding;
the CLI uses these defaults.

## Tests and validation limits

The ordinary workspace suite covers the codec, management client/server, runtime
security, registry, forwarding and CLI/process lifecycle. Detached startup
process tests require Python 3 and `ps`, plus permission to create local Unix
sockets and inspect owned processes. They use temporary runtime directories.

Opt-in OpenSSH tests require `ssh-agent`, `ssh-keygen` and `ssh-add`. They create
temporary keys and agents, exercise listing/signing and foreground sticky
failover or automatic detached startup, and clean up:

```sh
cargo test -p ssh-auth-helper --locked --test real_agent -- --ignored
cargo test -p ssh-auth-helper --locked --test detached real_agent -- --ignored
```

Run the shell/tmux example checks separately with
`python3 tests/integration_examples.py --binary "$PWD/target/debug/ssh-auth-helper"`
after a build. They use temporary homes and private tmux servers; unavailable
optional shells/tmux are reported as skips.

Linux tests requiring real foreign ownership or peer UIDs are explicitly
ignored in the ordinary test run. Run them only in an isolated root container
with Python 3 available:

```sh
cargo test --locked -p ssh-auth-helper-lib --lib -- --ignored
cargo test --locked -p ssh-auth-helper-lib --test runtime -- --ignored
cargo test --locked -p ssh-auth-helper --test startup -- --ignored
```

Native Linux and macOS checks, both OpenSSH scenarios, six isolated Linux root
tests, and live SSH/tmux failover have been exercised. The integration examples
also passed private-server attachment/reattachment, attachment without forwarding,
new-pane, restart/re-registration and rollback checks. Shell checks used Bash/zsh;
tmux versions 3.4 and 3.8 were tested.

These results do not establish compatibility with every agent extension or
destination-constrained key policy. Destination constraints/onward forwarding,
host logout/service-manager policies, actual account rc/X11 installation, and
forced supervision-panic/spawn/setsid failure injection remain unverified. Tests
do not modify personal SSH, shell or tmux configuration. Installation is manual;
no installer or service unit is provided.
