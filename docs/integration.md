# SSH, shell and tmux setup

These examples are installed manually on the **remote host**. They register each
login's original forwarded socket, then select the stable proxy for interactive
shells and tmux. Bash, zsh and tmux 3.4 or newer are supported.

## Install and choose one runtime

From the repository root:

```sh
cargo +stable install --locked --path ssh-auth-helper --root "$HOME/.local"
mkdir -p "$HOME/.local/share/ssh-auth-helper"
cp examples/*.sh "$HOME/.local/share/ssh-auth-helper/"
```

The examples explicitly use `/tmp/ssh-auth-helper-$(id -u)`. This keeps rc,
interactive shells and an existing tmux server on the same path even when their
`XDG_RUNTIME_DIR` values differ. To choose another absolute local runtime path,
change the marked assignment in **all three** installed files. If the binary is
installed elsewhere, change its location in each file too. Existing directory
and socket safety checks still apply; no example repairs unsafe permissions.

At least one live forwarded SSH connection is needed. Host logout policies or
runtime-directory cleanup may stop a detached daemon; use a persistent user
service if the host requires it. These examples do not install a service.

## Register from SSH rc

Integrate the [registration fragment](../examples/ssh-rc.sh) into your existing
`~/.ssh/rc`, for example by sourcing the installed fragment at a suitable point:

```sh
. "$HOME/.local/share/ssh-auth-helper/ssh-rc.sh"
```

Preserve existing initialization and X11 handling. **Do not replace the rc file
with this fragment.** Creating a new `~/.ssh/rc` also changes how system rc and
X11 setup are performed: first preserve the applicable `/etc/ssh/sshrc` duties
and the [OpenSSH SSHRC cookie-handling recipe](https://man.openbsd.org/sshd.8#SSHRC).
The fragment is not a complete replacement for either.

Registration skips logins without `SSH_AUTH_SOCK`, consumes no rc input, and
produces no stdout. Failures go to stderr and do not abort the requested command,
including when the surrounding rc uses `set -e`. The helper's async registration
budget is five seconds. No logout trap is installed: multiplexed SSH sessions can
share the same forwarded socket.

## Select the proxy in interactive shells

After other agent-environment setup, add this to `.bashrc` or `.zshrc`:

```sh
. "$HOME/.local/share/ssh-auth-helper/shell-agent.sh"
```

For Bash login shells, retain your existing arrangement for loading `.bashrc`
from the login profile. The [shell fragment](../examples/shell-agent.sh) does
nothing in noninteractive shells. It checks readiness before changing
`SSH_AUTH_SOCK`; failure leaves the current socket unchanged. This check does not
start a daemon and has the existing client budget of up to four seconds.

A distinct original path is saved as `SSH_AUTH_HELPER_ORIGINAL_SOCK`. Re-sourcing
the fragment when already using the proxy preserves that saved value. In each
preexisting tmux pane, source the fragment once: changing tmux's environment
cannot change the environment of an already running shell.

## Protect tmux's environment

Before the first setup, save the affected settings for inspection and rollback:

```sh
tmux show-options -g update-environment
tmux show-environment -g SSH_AUTH_SOCK
tmux list-sessions -F '#{session_id}'
# For every listed ID, also save its options/environment (example ID: '$0'):
tmux show-options -t '$0' | sed -n '/^update-environment\[/p'
tmux show-environment -t '$0' SSH_AUTH_SOCK
```

Keep these outputs in your own notes. No local option entries means that session
inherits the global list; retain that inheritance on rollback. A missing environment variable may return
a nonzero status; a leading `-SSH_AUTH_SOCK` means it is marked for removal.

After your existing `update-environment` configuration in `.tmux.conf`, add:

```tmux
run-shell 'sh "$HOME/.local/share/ssh-auth-helper/tmux-agent.sh" #{q:socket_path}'
```

Use synchronous `run-shell`, without `-b`, so setup finishes before the first
pane. Reload that file in the intended existing server, or run the helper there:

```sh
tmux run-shell 'sh "$HOME/.local/share/ssh-auth-helper/tmux-agent.sh" #{q:socket_path}'
```

For a nondefault server, select it explicitly with `tmux -S /path/to/socket` in
these commands. The [tmux helper](../examples/tmux-agent.sh) receives that socket
and changes only that server. It removes only indexed `SSH_AUTH_SOCK` entries
from global/session `update-environment` lists, preserving other entries and
session overrides. It sets the stable socket in the global and existing session
environments; new panes inherit it. Other environment variables are untouched.
See [tmux's environment documentation](https://man.openbsd.org/tmux.1#GLOBAL_AND_SESSION_ENVIRONMENT).

Custom patterns such as `SSH_*` could import the socket too. The helper rejects
such a configuration before making changes; replace it with the explicit names
you want updated. Setup is idempotent. If later configuration or a plugin changes
these options, rerun the helper. It does not install hooks to override them.

## Recovery and rollback

After a daemon restart, registrations are lost but endpoint paths stay the same.
If this shell's saved original forwarding socket is still alive, re-register it:

```sh
if [ -n "${SSH_AUTH_HELPER_ORIGINAL_SOCK-}" ]; then
    "$HOME/.local/bin/ssh-auth-helper" \
        --runtime-dir "/tmp/ssh-auth-helper-$(id -u)" \
        register --socket "$SSH_AUTH_HELPER_ORIGINAL_SOCK"
fi
```

A saved path can be stale, especially when inherited by a later pane. Otherwise
open a fresh login with forwarding enabled. Registering the proxy itself is
rejected; use the original path explicitly. Existing signing connections cannot
survive a daemon restart.

To roll back, remove only the added source/run-shell lines and restore the
captured tmux settings. Restore removed entries at their original indices, for
example `tmux set-option -g 'update-environment[3]' SSH_AUTH_SOCK` if your saved
output had that entry there; use `-t '$0'` for a session-local entry. Restore the
saved global/session environment value with `set-environment`, use `-u` if it was
absent, or `-r` if it was marked for removal. Keep unrelated settings intact.

In each existing shell, restore a surviving original explicitly:

```sh
if [ -n "${SSH_AUTH_HELPER_ORIGINAL_SOCK-}" ]; then
    export SSH_AUTH_SOCK="$SSH_AUTH_HELPER_ORIGINAL_SOCK"
    unset SSH_AUTH_HELPER_ORIGINAL_SOCK
fi
```

Otherwise start a fresh forwarded login. Rollback does not unregister other
sessions or stop the shared daemon. Stopping an unused daemon is optional.

## Check the examples

Run the isolated shell/tmux tests, optionally including the real helper timeout
check after a build:

```sh
python3 tests/integration_examples.py
python3 tests/integration_examples.py --binary "$PWD/target/debug/ssh-auth-helper"
```

The tests use temporary homes and private tmux sockets. They never install rc or
profile files in your real account. Missing optional shells/tmux are reported as
skips; live SSH attachment tests are recorded separately from these local checks.
