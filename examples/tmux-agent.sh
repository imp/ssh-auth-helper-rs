#!/bin/sh
# Run through tmux run-shell with that server's socket path as the sole argument.
set -eu

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
    printf '%s\n' 'usage: tmux-agent.sh TMUX_SOCKET' >&2
    exit 2
fi
socket=$1
# Keep this runtime choice identical in all three integration examples.
runtime="/tmp/ssh-auth-helper-$(id -u)"
proxy=$("$HOME/.local/bin/ssh-auth-helper" --runtime-dir "$runtime" socket-path)

tmux_command() {
    tmux -S "$socket" "$@"
}

# Read indexed names separately from raw values; never evaluate tmux output.
update_scope() {
    action=$1
    shift
    # Without -A or a named option, session output contains only local overrides.
    entries=$(tmux_command show-options "$@")
    printf '%s\n' "$entries" |
        while IFS=' ' read -r option ignored; do
            case $option in
                update-environment\[*\]) ;;
                *) continue ;;
            esac
            value=$(tmux_command show-options "$@" -v "$option")
            if [ "$value" = SSH_AUTH_SOCK ]; then
                if [ "$action" = apply ]; then
                    tmux_command set-option "$@" -u "$option"
                fi
            else
                # Intentional pattern match: preserve other variables covered by globs.
                case SSH_AUTH_SOCK in
                    $value)
                        printf '%s\n' \
                            "tmux-agent: unsupported update-environment pattern '$value'; use explicit variable names" >&2
                        exit 1
                        ;;
                esac
            fi
        done
}

sessions=$(tmux_command list-sessions -F '#{session_id}')
# Validate every scope before changing any option or environment value.
update_scope check -g
for session in $sessions; do
    update_scope check -t "$session"
done

update_scope apply -g
tmux_command set-environment -g SSH_AUTH_SOCK "$proxy"
for session in $sessions; do
    update_scope apply -t "$session"
    tmux_command set-environment -t "$session" SSH_AUTH_SOCK "$proxy"
done
