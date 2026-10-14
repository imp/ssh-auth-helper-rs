# ssh-auth-helper

A Rust helper under development for keeping a stable SSH agent socket usable
across remote SSH connections and long-lived tmux sessions.

The current CLI provides path discovery and a foreground daemon with private
sockets, singleton locking, bounded control readiness, crash recovery,
identity-checked cleanup and socket registration/status. Agent probing,
selection, forwarding and automatic detached startup are not implemented yet.

```sh
cargo run -p ssh-auth-helper --locked -- socket-path
cargo run -p ssh-auth-helper --locked -- --runtime-dir /tmp/my-helper socket-path
```

An explicit runtime directory takes precedence over `XDG_RUNTIME_DIR`.
Otherwise the planned directory is `$XDG_RUNTIME_DIR/ssh-auth-helper`, or
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
is available. Agent clients currently close immediately without a fabricated
reply. The control socket supports `hello`, `register`, `unregister` and `status`;
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
Clients never automatically replay uncertain operations. Startup/readiness
checks share a five-second budget.

With the foreground daemon running, register an original SSH-forwarded socket
from another terminal:

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
by default. These commands require an already running daemon and do not start one.

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
health is `unobserved`, no observation time is claimed, and active selection and
relay counts remain empty/zero until forwarding is implemented.

Application errors carry metadata-only `error.data.kind` values:

| Code | Kind | Meaning |
| --- | --- | --- |
| -32001 | `unsafe_path` | Missing, invalid, non-socket or foreign-owned upstream |
| -32002 | `recursive_upstream` | Helper endpoint or its filesystem alias |
| -32003 | `capacity_exceeded` | Candidate or snapshot-size capacity exhausted |
| -32004 | `deadline_exceeded` | Operation budget exhausted before mutation |
| -32602 | `label_too_long` | Label exceeds 128 UTF-8 bytes |

Management clients authorize the peer and validate hello before sending a
correlated operation on that same connection. Connect, hello and the operation
share a two-second deadline. Every return closes the connection. A missing or
invalid response after registration/unregistration can leave an uncertain
outcome; clients report failure and never automatically replay the mutation.
Inspect `status` before deciding whether to issue another command.

Use stable Rust, selected by `rust-toolchain.toml`:

```sh
cargo build --locked --workspace
cargo test --locked --workspace --all-targets
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

The Cargo workspace has four peer packages, all inheriting shared Rust/Clippy lints:

- `ssh-auth-helper`: thin executable package with CLI parsing, command dispatch,
  runtime initialization and process output. It builds the `ssh-auth-helper` binary.
  CLI and process-startup integration tests live here.
- `ssh-auth-helper-lib`: core library for runtime safety, startup, supervision and
  helper operations. It has no executable target or Clap dependency.
- `ssh-auth-helper-api`: our helper's management API schemas, including
  `HelloRequest`/`HelloResponse`. Operation payloads use `XxxRequest`/`XxxResponse`.
- `jsonrpc-uds`: bounded JSON-RPC parsing and serialization with typed
  messages through `Framed<_, JsonRpcCodec>`. Requests, notifications and responses
  are typed; params/results use `serde_json::Value`. The core library
  owns dispatch, deadlines and readiness calls over the typed stream. Callers
  authorize endpoints and peers before constructing the framed stream.

The wire types are small owned Serde types, with no RPC framework or raw JSON
payloads. Readiness sends one identified request and checks its response under
one absolute connect/send/receive budget. Every return closes that connection;
there are no background client tasks or automatic request replays.

Detached startup, selection, watch and agent relay implementations follow next.

The selected foundation uses Camino paths, parking_lot for short shared-state
locks, Jiff UTC timestamps, and Tokio for asynchronous I/O and monotonic
deadlines. Forwarding and real SSH/tmux acceptance remain under development. No installation step alters personal configuration.

Linux tests requiring real foreign ownership or peer UIDs are explicitly
ignored in the ordinary test run. Run them only in an isolated root container
with Python 3 available:

```sh
cargo test --locked --test runtime -- --ignored
cargo test --locked --test startup -- --ignored
```
