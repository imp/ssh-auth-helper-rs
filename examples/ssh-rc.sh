# Registration fragment for an existing ~/.ssh/rc; this is not a complete rc.
# Preserve existing initialization, especially system rc and X11 cookie handling.
(
    if [ -n "${SSH_AUTH_SOCK-}" ]; then
        # Keep this runtime choice identical in all three integration examples.
        runtime="/tmp/ssh-auth-helper-$(id -u)"
        "$HOME/.local/bin/ssh-auth-helper" --runtime-dir "$runtime" \
            register --socket "$SSH_AUTH_SOCK" </dev/null >/dev/null || :
    fi
)
