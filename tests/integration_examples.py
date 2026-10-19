"""Isolated shell/tmux example tests; Python standard library only."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--binary', type=Path)
OPTIONS, TEST_ARGS = parser.parse_known_args()
BINARY = OPTIONS.binary.resolve() if OPTIONS.binary else None
TMUX = shutil.which('tmux')
DEFAULT_RUNTIME = '"/tmp/ssh-auth-helper-$(id -u)"'
FAKE = '''#!/usr/bin/env python3
import json, pathlib, sys
base = pathlib.Path(__file__).parent
config = json.loads((base/'config.json').read_text())
args = sys.argv[1:]
assert args[:1] == ['--runtime-dir'], args
runtime, verb = args[1:3]
call = {'runtime':runtime, 'verb':verb, 'args':args[3:]}
if verb == 'register': call['stdin'] = sys.stdin.read()
with (base/'calls.jsonl').open('a') as log: log.write(json.dumps(call)+'\\n')
code = config.get(verb+'_code',0)
if verb == 'socket-path' and not code: print(config['proxy'])
if verb in ['register','status']: print('fixture stdout must be suppressed')
if code: print('fixture '+verb+' failed',file=sys.stderr)
raise SystemExit(code)
'''


def wait_file(path):
    end = time.monotonic() + 5
    while not path.exists():
        if time.monotonic() >= end:
            raise AssertionError(f'file not created: {path}')
        time.sleep(.01)
    return path.read_text()


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='sah-', dir='/tmp')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.home = self.base/'home with spaces'
        self.bin = self.home/'.local/bin'
        self.bin.mkdir(parents=True)
        self.runtime = self.base/'runtime'
        self.proxy = str(self.runtime/'agent.sock')
        self.raw = str(self.base/'original socket')
        self.helper = self.bin/'ssh-auth-helper'
        self.helper.write_text(FAKE)
        self.helper.chmod(0o700)
        self.config = {'proxy':self.proxy}
        self.save_config()
        self.scripts = {}
        for name in ['ssh-rc.sh','shell-agent.sh','tmux-agent.sh']:
            source = (ROOT/'examples'/name).read_text()
            assert source.count(DEFAULT_RUNTIME) == 1
            path = self.base/name
            path.write_text(source.replace(DEFAULT_RUNTIME,shlex.quote(str(self.runtime))))
            self.scripts[name] = path
        self.env = dict(os.environ, HOME=str(self.home), SSH_AUTH_SOCK=self.raw,
                        XDG_RUNTIME_DIR='/unrelated/xdg')
        self.env.pop('SSH_AUTH_HELPER_ORIGINAL_SOCK',None)
        self.env.pop('TMUX',None)

    def save_config(self):
        (self.bin/'config.json').write_text(json.dumps(self.config))

    def calls(self):
        path = self.bin/'calls.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def run_process(self,args,check=True,**kwargs):
        result = subprocess.run(args,env=self.env,capture_output=True,text=True,timeout=12,**kwargs)
        if check:
            self.assertEqual(result.returncode,0,(args,result.stdout,result.stderr))
        return result

    def source(self,script,shell='sh',interactive=True,repeated=False):
        switches = {'sh':[], 'bash':['--noprofile','--norc'], 'zsh':['-f']}[shell]
        if interactive: switches += ['-i']
        body = 'set -eu; . "$1"; '
        if repeated: body += '. "$1"; '
        body += 'printf "%s\\n%s\\n" "${SSH_AUTH_SOCK-UNSET}" "${SSH_AUTH_HELPER_ORIGINAL_SOCK-UNSET}"'
        return self.run_process([shell,*switches,'-c',body,'fixture',str(self.scripts[script])])


class ShellTests(Fixture):
    def test_rc_skips_missing_forwarding(self):
        self.env.pop('SSH_AUTH_SOCK')
        result = self.run_process(['sh','-ec','. "$1"; printf continued','fixture',str(self.scripts['ssh-rc.sh'])])
        self.assertEqual(result.stdout,'continued')
        self.assertEqual(self.calls(),[])

    def test_rc_preserves_x11_input_and_continues_on_failure(self):
        for code in [0,1]:
            with self.subTest(code=code):
                self.config['register_code'] = code
                self.save_config()
                result = self.run_process(['sh','-ec','. "$1"; read -r proto cookie; printf "%s %s continued" "$proto" "$cookie"',
                                           'fixture',str(self.scripts['ssh-rc.sh'])],input='test-proto test-cookie\n')
                self.assertEqual(result.stdout,'test-proto test-cookie continued')
                call = self.calls()[-1]
                self.assertEqual(call,{'runtime':str(self.runtime),'verb':'register','args':['--socket',self.raw],'stdin':''})

    def test_rc_missing_helper_does_not_abort(self):
        self.helper.unlink()
        result = self.run_process(['sh','-ec','. "$1"; printf continued','fixture',str(self.scripts['ssh-rc.sh'])])
        self.assertEqual(result.stdout,'continued')
        self.assertTrue(result.stderr)

    def test_shell_saves_original_and_repeated_source_preserves_it(self):
        for shell in ['sh','bash','zsh']:
            if not shutil.which(shell): continue
            with self.subTest(shell=shell):
                result = self.source('shell-agent.sh',shell,repeated=True)
                self.assertEqual(result.stdout.splitlines(),[self.proxy,self.raw])
                self.assertTrue(all(c['runtime']==str(self.runtime) for c in self.calls()))

    def test_shell_failure_preserves_both_variables(self):
        self.env['SSH_AUTH_HELPER_ORIGINAL_SOCK'] = 'previous-original'
        for verb in ['socket-path','status']:
            with self.subTest(verb=verb):
                self.config[verb+'_code'] = 1
                self.save_config()
                result = self.source('shell-agent.sh')
                self.assertEqual(result.stdout.splitlines(),[self.raw,'previous-original'])
                self.config.pop(verb+'_code')
        self.helper.unlink()
        self.assertEqual(self.source('shell-agent.sh').stdout.splitlines(),[self.raw,'previous-original'])

    def test_proxy_inheritance_and_noninteractive_source(self):
        self.env['SSH_AUTH_SOCK'] = self.proxy
        self.env['SSH_AUTH_HELPER_ORIGINAL_SOCK'] = self.raw
        self.assertEqual(self.source('shell-agent.sh').stdout.splitlines(),[self.proxy,self.raw])
        before = self.calls()
        self.env['SSH_AUTH_SOCK'] = self.raw
        self.assertEqual(self.source('shell-agent.sh',interactive=False).stdout.splitlines(),[self.raw,self.raw])
        self.assertEqual(self.calls(),before)

    @unittest.skipUnless(BINARY,'pass --binary to run the real helper timeout contract')
    def test_real_registration_timeout_preserves_input_and_command(self):
        self.helper.unlink()
        self.helper.symlink_to(BINARY)
        self.runtime.mkdir(mode=0o700)
        listener = socket.socket(socket.AF_UNIX)
        self.addCleanup(listener.close)
        listener.bind(str(self.runtime/'control.sock'))
        (self.runtime/'control.sock').chmod(0o600)
        listener.listen(16)
        before = time.monotonic()
        result = self.run_process(['sh','-ec','. "$1"; read -r proto cookie; printf "%s %s continued" "$proto" "$cookie"',
                                   'fixture',str(self.scripts['ssh-rc.sh'])],input='test-proto test-cookie\n')
        elapsed = time.monotonic()-before
        self.assertGreater(elapsed,4.5)
        self.assertLess(elapsed,8)
        self.assertEqual(result.stdout,'test-proto test-cookie continued')
        self.assertIn('timed out',result.stderr)
        self.assertFalse((self.runtime/'daemon.lock').exists())


@unittest.skipUnless(TMUX,'tmux is not installed')
class TmuxTests(Fixture):
    def setUp(self):
        super().setUp()
        self.socket = str(self.base/'tmux.sock')
        self.addCleanup(self.stop,self.socket)
        self.tmux('new-session','-d','-s','main','/bin/sh')

    def tmux(self,*args,socket_path=None,check=True):
        return self.run_process([TMUX,'-S',socket_path or self.socket,'-f','/dev/null',*args],check=check)

    def stop(self,socket_path):
        self.tmux('kill-server',socket_path=socket_path,check=False)

    def setup_example(self,check=True):
        return self.run_process(['sh',str(self.scripts['tmux-agent.sh']),self.socket],check=check)

    def array(self,*scope):
        return [line for line in self.tmux('show-options',*scope).stdout.splitlines()
                if line.startswith('update-environment[')]

    def environment(self,*scope):
        result = self.tmux('show-environment',*scope,'SSH_AUTH_SOCK',check=False)
        return result.stdout.strip() if result.returncode==0 else None

    def pane_value(self,target='main:0.0',source=False):
        output = self.base/'pane-value'
        output.unlink(missing_ok=True)
        prefix = '. '+shlex.quote(str(self.scripts['shell-agent.sh']))+'; ' if source else ''
        body = prefix+'printf "%s\\n%s\\n" "$SSH_AUTH_SOCK" "${SSH_AUTH_HELPER_ORIGINAL_SOCK-UNSET}" > '+shlex.quote(str(output))
        self.tmux('send-keys','-t',target,'-l',body)
        self.tmux('send-keys','-t',target,'Enter')
        return wait_file(output).splitlines()

    def test_custom_scopes_indices_markers_and_idempotence(self):
        self.tmux('set-option','-g','update-environment','KEEP SSH_AUTH_SOCK OTHER SSH_AUTH_SOCK')
        self.tmux('set-option','-t','main','update-environment','LOCAL SSH_AUTH_SOCK')
        self.tmux('new-session','-d','-s','inherits','/bin/sh')
        self.tmux('set-environment','-r','-t','main','SSH_AUTH_SOCK')
        self.tmux('set-environment','-g','KEEP','unchanged')
        self.setup_example()
        self.assertEqual(self.array('-g'),['update-environment[0] KEEP','update-environment[2] OTHER'])
        self.assertEqual(self.array('-t','main'),['update-environment[0] LOCAL'])
        self.assertEqual(self.array('-t','inherits'),[])
        for scope in [('-g',),('-t','main'),('-t','inherits')]:
            self.assertEqual(self.environment(*scope),'SSH_AUTH_SOCK='+self.proxy)
        self.assertEqual(self.tmux('show-environment','-g','KEEP').stdout,'KEEP=unchanged\n')
        before = [self.array('-g'),self.array('-t','main'),self.array('-t','inherits')]
        self.setup_example()
        self.assertEqual(before,[self.array('-g'),self.array('-t','main'),self.array('-t','inherits')])
        self.assertEqual({c['verb'] for c in self.calls()},{'socket-path'})

    def test_pattern_failure_has_no_partial_changes(self):
        self.tmux('set-option','-g','update-environment','KEEP SSH_AUTH_SOCK')
        self.tmux('set-option','-t','main','update-environment','LOCAL SSH_*')
        before = [self.array('-g'),self.array('-t','main'),self.environment('-g'),self.environment('-t','main')]
        result = self.setup_example(check=False)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('unsupported',result.stderr)
        self.assertEqual(before,[self.array('-g'),self.array('-t','main'),self.environment('-g'),self.environment('-t','main')])

    def test_server_isolation_new_panes_and_existing_shell_update(self):
        other = str(self.base/'other.sock')
        self.addCleanup(self.stop,other)
        self.tmux('new-session','-d','-s','other','/bin/sh',socket_path=other)
        before = self.tmux('show-options','-g',socket_path=other).stdout
        self.setup_example()
        self.assertEqual(self.tmux('show-options','-g',socket_path=other).stdout,before)
        self.assertEqual(self.environment('-g'), 'SSH_AUTH_SOCK='+self.proxy)
        self.assertEqual(self.pane_value()[0],self.raw)
        self.assertEqual(self.pane_value(source=True),[self.proxy,self.raw])
        output = self.base/'new-pane'
        self.tmux('new-window','-d','-t','main','/bin/sh','-c',
                  'printf "%s" "$SSH_AUTH_SOCK" > '+shlex.quote(str(output)))
        self.assertEqual(wait_file(output),self.proxy)
        self.tmux('new-session','-d','-s','later','/bin/sh')
        self.assertEqual(self.pane_value('later:0.0')[0],self.proxy)

    def test_first_server_config_initializes_before_first_pane(self):
        fresh = str(self.base/'fresh.sock')
        self.addCleanup(self.stop,fresh)
        config = self.base/'tmux.conf'
        # tmux's q: modifier quotes the explicit socket argument for run-shell.
        command = 'sh '+shlex.quote(str(self.scripts['tmux-agent.sh']))+' #{q:socket_path}'
        config.write_text('set -g update-environment "KEEP SSH_AUTH_SOCK"\nrun-shell '+shlex.quote(command)+'\n')
        self.run_process([TMUX,'-S',fresh,'-f',str(config),'new-session','-d','-s','first','/bin/sh'])
        self.assertEqual(self.tmux('show-environment','-g','SSH_AUTH_SOCK',socket_path=fresh).stdout,
                         'SSH_AUTH_SOCK='+self.proxy+'\n')
        self.assertNotIn('SSH_AUTH_SOCK',self.tmux('show-options','-g','update-environment',socket_path=fresh).stdout)

    def test_restore_only_removed_entries_and_original_environment(self):
        self.tmux('set-option','-g','update-environment','KEEP SSH_AUTH_SOCK')
        self.tmux('set-option','-t','main','update-environment','LOCAL SSH_AUTH_SOCK')
        for state in ['value','absent','removed']:
            if state == 'absent':
                self.tmux('set-environment','-u','-t','main','SSH_AUTH_SOCK')
            elif state == 'removed':
                self.tmux('set-environment','-r','-t','main','SSH_AUTH_SOCK')
            else:
                self.tmux('set-environment','-t','main','SSH_AUTH_SOCK',self.raw)
            before = [self.array('-g'),self.array('-t','main'),self.environment('-g'),self.environment('-t','main')]
            self.setup_example()
            # These are the indexed restore commands illustrated in the guide.
            self.tmux('set-option','-g','update-environment[1]','SSH_AUTH_SOCK')
            self.tmux('set-option','-t','main','update-environment[1]','SSH_AUTH_SOCK')
            for scope,old in [(('-g',),before[2]),(('-t','main'),before[3])]:
                if old is None:
                    self.tmux('set-environment',*scope,'-u','SSH_AUTH_SOCK')
                elif old=='-SSH_AUTH_SOCK':
                    self.tmux('set-environment',*scope,'-r','SSH_AUTH_SOCK')
                else:
                    self.tmux('set-environment',*scope,'SSH_AUTH_SOCK',old.split('=',1)[1])
            self.assertEqual(before,[self.array('-g'),self.array('-t','main'),self.environment('-g'),self.environment('-t','main')])



if __name__=='__main__':
    unittest.main(argv=[sys.argv[0],*TEST_ARGS],verbosity=2)
