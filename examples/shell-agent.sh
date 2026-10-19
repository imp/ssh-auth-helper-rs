# Source from an interactive Bash/zsh startup file, or once in an existing pane.
case $- in
    *i*)
        # Keep this runtime choice identical in all three integration examples.
        _ssh_auth_helper_runtime="/tmp/ssh-auth-helper-$(id -u)"
        if [ -x "$HOME/.local/bin/ssh-auth-helper" ] &&
            _ssh_auth_helper_proxy=$("$HOME/.local/bin/ssh-auth-helper" \
                --runtime-dir "$_ssh_auth_helper_runtime" socket-path) &&
            "$HOME/.local/bin/ssh-auth-helper" \
                --runtime-dir "$_ssh_auth_helper_runtime" status >/dev/null
        then
            if [ -n "${SSH_AUTH_SOCK-}" ] &&
                [ "$SSH_AUTH_SOCK" != "$_ssh_auth_helper_proxy" ]
            then
                export SSH_AUTH_HELPER_ORIGINAL_SOCK="$SSH_AUTH_SOCK"
            fi
            export SSH_AUTH_SOCK="$_ssh_auth_helper_proxy"
        fi
        unset _ssh_auth_helper_runtime _ssh_auth_helper_proxy
        ;;
esac
