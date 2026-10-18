"""Owned-process fixtures for detached startup; no user configuration is changed."""

import fcntl
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import time
import uuid


BINARY, BASE, SCENARIO = sys.argv[1:]
BASE = Path(BASE)
RUNTIME = BASE / "helper"
CONTROL = RUNTIME / "control.sock"
AGENT = RUNTIME / "agent.sock"
LOCK = RUNTIME / "daemon.lock"
ENV = os.environ.copy()
ENV.pop("SSH_AUTH_SOCK", None)
ENV.pop("SSH_AGENT_PID", None)
CHILDREN = []
SOCKETS = []


def command(*args):
    return [BINARY, "--runtime-dir", str(RUNTIME), *map(str, args)]


def start(args, **kwargs):
    child = subprocess.Popen(args, env=ENV, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, **kwargs)
    CHILDREN.append(child)
    return child


def output(child, success=True):
    stdout, stderr = child.communicate(timeout=8)
    assert (child.returncode == 0) == success, (child.returncode, stdout, stderr)
    if success:
        assert stderr == b"", stderr
    else:
        assert stdout == b"" and stderr, (stdout, stderr)
    return stdout, stderr


def run(*args, success=True, **kwargs):
    return output(start(command(*args), **kwargs), success)


def upstream(name="upstream"):
    path = BASE / name
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(path))
    listener.listen(16)
    SOCKETS.append(listener)
    return path


def status():
    return json.loads(run("status", "--json")[0])


def daemon_pids():
    # Match the full executable/arguments and unique temporary runtime, never a name.
    expected = " ".join(command("serve", "--detached"))
    table = subprocess.check_output(["ps", "-axww", "-o", "pid=,command="], timeout=3)
    matches = []
    for row in table.decode().splitlines():
        fields = row.strip().split(None, 1)
        if len(fields) == 2 and fields[1] == expected:
            matches.append(int(fields[0]))
    return matches


def wait_for(predicate, seconds=3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.01)
    raise AssertionError("fixture condition did not become true")


def only_daemon():
    pids = wait_for(lambda: daemon_pids())
    assert len(pids) == 1, pids
    return pids[0]


def cleanup():
    for child in CHILDREN:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=3)
    # Recheck identity before each signal; these are only our isolated instances.
    for pid in daemon_pids():
        if pid in daemon_pids():
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    wait_for(lambda: not daemon_pids(), seconds=5)
    for listener in SOCKETS:
        listener.close()


def detachment():
    original = upstream()
    read_fd, write_fd = os.pipe()
    try:
        # Deliberately inherit a non-CLOEXEC descriptor into the CLI. Its child must
        # drop it on exec; stdin/stdout/stderr must also be detached.
        os.set_inheritable(write_fd, True)
        child = start(command("register", "--socket", original),
                      stdin=subprocess.PIPE, pass_fds=(write_fd,), start_new_session=True)
        os.close(write_fd)
        write_fd = None
        assert output(child)[0] == b""
        assert select.select([read_fd], [], [], 2)[0], "extra descriptor retained"
        assert os.read(read_fd, 1) == b"", "extra pipe not at EOF"
        pid = only_daemon()
        assert os.getsid(pid) == pid and os.getpgid(pid) == pid
        first = status()
        assert first["registrations"][0]["socket"] == str(original)

        # A shell-like session leader stays alive after register, then loses its
        # process group to SIGHUP. The independent daemon must continue serving.
        shell = start([sys.executable, "-c", """
import signal, subprocess, sys
subprocess.run(sys.argv[1:], check=True)
print('registered', flush=True)
signal.pause()
""", *command("register", "--socket", original)], start_new_session=True)
        assert select.select([shell.stdout], [], [], 8)[0]
        assert shell.stdout.readline() == b"registered\n"
        assert os.getsid(pid) != shell.pid
        os.killpg(shell.pid, signal.SIGHUP)
        stdout, stderr = shell.communicate(timeout=3)
        assert shell.returncode == -signal.SIGHUP and stdout == stderr == b""
        assert status() == first
        assert daemon_pids() == [pid]
    finally:
        os.close(read_fd)
        if write_fd is not None:
            os.close(write_fd)


def concurrent():
    originals = [upstream(f"agent-{i}") for i in range(8)]
    starters = [start(command("register", "--socket", path, "--json"))
                for path in originals]
    registrations = [json.loads(output(child)[0])["registration"] for child in starters]
    snapshot = status()
    assert len(snapshot["registrations"]) == 8
    assert {item["socket"] for item in snapshot["registrations"]} == set(map(str, originals))
    assert {item["id"].split("/")[0] for item in registrations} == {snapshot["instance"]}
    pid = only_daemon()
    assert run("register", "--socket", originals[0])[0] == b""
    assert status() == snapshot
    run("register", "--socket", BASE / "missing", success=False)
    run("register", "--socket", originals[0], "--label", "x" * 129, success=False)
    assert status() == snapshot and daemon_pids() == [pid]
    os.kill(pid, signal.SIGSTOP)
    try:
        run("register", "--socket", originals[0], success=False)
        assert daemon_pids() == [pid], "timed-out caller killed/replaced the daemon"
    finally:
        os.kill(pid, signal.SIGCONT)
    assert status() == snapshot


def no_start():
    for args in [("register",), ("register", "--socket", b"/tmp/\xff"),
                 ("status",), ("unregister", f"{uuid.uuid4()}/{uuid.uuid4()}")]:
        # Preserve raw bytes for the non-UTF8 CLI-input case.
        argv = [BINARY, "--runtime-dir", str(RUNTIME), *args]
        output(start(argv), success=False)
        assert not RUNTIME.exists() and not daemon_pids()
    run("socket-path")
    assert not RUNTIME.exists()
    original = upstream()
    RUNTIME.mkdir(mode=0o755)
    RUNTIME.chmod(0o755)
    run("register", "--socket", original, success=False)
    assert RUNTIME.stat().st_mode & 0o777 == 0o755
    assert not LOCK.exists() and not daemon_pids()
    RUNTIME.chmod(0o700)
    AGENT.write_bytes(b"preserve")
    AGENT.chmod(0o600)
    inode = AGENT.stat().st_ino
    run("register", "--socket", original, success=False)
    assert AGENT.read_bytes() == b"preserve" and AGENT.stat().st_ino == inode
    assert not LOCK.exists() and not daemon_pids()


def lock():
    original = upstream()
    RUNTIME.mkdir(mode=0o700)
    with LOCK.open("wb") as held:
        LOCK.chmod(0o600)
        held.write(b"owned fixture lock")
        held.flush()
        inode = LOCK.stat().st_ino
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = time.monotonic()
        _, error = run("register", "--socket", original, success=False)
        assert 4.5 < time.monotonic() - before < 7
        assert b"child exit: Some" in error, error
        assert not CONTROL.exists() and not AGENT.exists()
        assert LOCK.stat().st_ino == inode and LOCK.read_bytes() == b"owned fixture lock"
        assert not daemon_pids()
    run("register", "--socket", original)
    assert len(status()["registrations"]) == 1


def fake_listener():
    RUNTIME.mkdir(mode=0o700)
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(CONTROL))
    CONTROL.chmod(0o600)
    listener.listen(16)
    listener.settimeout(6)
    SOCKETS.append(listener)
    return listener


def silent():
    listener = fake_listener()
    inode = CONTROL.stat().st_ino
    before = time.monotonic()
    run("register", "--socket", BASE / "original", success=False)
    assert 4.5 < time.monotonic() - before < 7
    assert CONTROL.stat().st_ino == inode and not LOCK.exists()
    assert not daemon_pids()
    # The listener was not required to accept to keep its connections silent.
    assert listener.fileno() >= 0


def incompatible():
    listener = fake_listener()
    inode = CONTROL.stat().st_ino
    child = start(command("register", "--socket", BASE / "original"))
    connection, _ = listener.accept()
    with connection:
        connection.sendall(b'{"jsonrpc":"3.0","id":null,"result":{}}\n')
        output(child, success=False)
    assert CONTROL.stat().st_ino == inode and not LOCK.exists()
    assert not daemon_pids()


def silent_then_absent():
    listener = fake_listener()
    child = start(command("register", "--socket", BASE / "original"))
    # Wait until a hello was sent, then leave enough time for its two-second
    # timeout. Removing the pathname after ambiguity must not authorize a launch.
    connection, _ = listener.accept()
    with connection:
        connection.settimeout(3)
        assert b'"hello"' in connection.recv(4096)
        time.sleep(2.2)
        CONTROL.unlink()
        output(child, success=False)
    assert not LOCK.exists() and not daemon_pids()


def reply_timeout(delayed=False):
    listener = fake_listener()
    before = time.monotonic()
    child = start(command("register", "--socket", BASE / "original"))
    if delayed:
        # Consume the first connect/hello budget, then succeed on its retry.
        first, _ = listener.accept()
        first.settimeout(4)
        with first, first.makefile("rb") as wire:
            assert json.loads(wire.readline())["method"] == "hello"
            assert wire.read(1) == b""
    connection, _ = listener.accept()
    connection.settimeout(4)
    with connection, connection.makefile("rwb", buffering=0) as wire:
        hello = json.loads(wire.readline())
        assert hello["method"] == "hello"
        if delayed:
            time.sleep(1.5)
        response = {"jsonrpc": "2.0", "id": hello["id"], "result": {
            "instance": str(uuid.uuid4()), "supported_versions": [1], "ready": True,
            "endpoints": {"agent": str(AGENT), "control": str(CONTROL)}}}
        wire.write(json.dumps(response).encode() + b"\n")
        request = json.loads(wire.readline())
        assert request["method"] == "register"
        _, error = output(child, success=False)
        if delayed:
            elapsed = time.monotonic() - before
            assert b"startup/registration timed out" in error, error
            assert 4.5 < elapsed < 7, elapsed
        assert wire.read(1) == b"", "client retained/replayed failed connection"
    listener.settimeout(0.1)
    try:
        listener.accept()
    except TimeoutError:
        pass
    else:
        raise AssertionError("registration reconnected")
    assert not LOCK.exists() and not daemon_pids()


def total_budget():
    reply_timeout(delayed=True)


def real_agent():
    key = BASE / "key"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)],
                   check=True, timeout=5, capture_output=True)
    original = BASE / "original.sock"
    start(["ssh-agent", "-D", "-a", str(original)])
    wait_for(original.exists)
    env = dict(ENV, SSH_AUTH_SOCK=str(original))
    subprocess.run(["ssh-add", str(key)], env=env, check=True, timeout=5, capture_output=True)
    assert run("register", "--socket", original)[0] == b""
    only_daemon()
    env["SSH_AUTH_SOCK"] = str(AGENT)
    listed = subprocess.run(["ssh-add", "-L"], env=env, check=True,
                            timeout=5, capture_output=True).stdout
    assert listed == key.with_suffix(".pub").read_bytes()
    subprocess.run(["ssh-add", "-T", str(key.with_suffix(".pub"))], env=env,
                   check=True, timeout=5, capture_output=True)


if __name__ == "__main__":
    try:
        globals()[SCENARIO]()
    finally:
        cleanup()
