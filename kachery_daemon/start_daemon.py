import os
import subprocess
from typing import List
from ._shellscript import ShellScript
from ._daemon_connection import _probe_daemon
from ._daemon_connection import _daemon_port, _daemon_host
from .version import __version__

def start_daemon(*, label: str, owner: str, method: str, verbose: int, node_arg: List[str], auth_group: str, kachery_hub_url: str):
    if _probe_daemon() is not None:
        raise Exception('Cannot start daemon. Already running.')

    daemon_port = _daemon_port()
    daemon_host = _daemon_host()

    start_args = []
    start_args.append(f'--verbose {verbose}')
    if auth_group:
        start_args.append(f'--auth-group {auth_group}')
    if kachery_hub_url:
        start_args.append(f'--kachery-hub-url {kachery_hub_url}')
    start_args.append(f'--label {label}')
    if owner:
        start_args.append(f'--owner {owner}')

    assert method in ['npm', 'dev'], f'Invalid method for start_daemon: {method}'

    thisdir = os.path.dirname(os.path.realpath(__file__))
    if method == 'npm':
        try:
            subprocess.check_call(['npx', 'check-node-version', '--print', '--node', '>=12'])
        except:
            raise Exception('Please install nodejs version >=12. This is required in order to run a kachery daemon.')
        
        
        for na in node_arg:
            start_args.append(f'--node-arg={na}')

        npm_package = f'{thisdir}/kachery-daemon-node-{__version__}.tgz'
        if not os.path.exists(npm_package):
            raise Exception(f'No such file: {npm_package}')

        script = f'''
        #!/bin/bash
        set -ex

        export KACHERY_DAEMON_PORT="{daemon_port}"
        export KACHERY_DAEMON_HOST="{daemon_host}"
        npm install -g -y {npm_package}
        '''

        script = script + f'''
        exec kachery-daemon-node start {' '.join(start_args)}
        '''
    
        ss = ShellScript(script)
        ss.start()
        try:
            retcode = ss.wait()
        finally:
            ss.stop()
            ss.kill()
    elif method == 'dev':
        ss = ShellScript(f'''
        #!/bin/bash
        set -ex

        export KACHERY_DAEMON_PORT="{daemon_port}"
        export KACHERY_DAEMON_HOST="{daemon_host}"
        cd {thisdir}/../daemon
        # exec node_modules/ts-node/dist/bin.js {' '.join(node_arg)} ./src/cli.ts start {' '.join(start_args)}
        exec node {' '.join(node_arg)} -r ts-node/register -r tsconfig-paths/register ./src/cli.ts start {' '.join(start_args)}
        ''')
        ss.start()
        try:
            retcode = ss.wait()
        finally:
            ss.stop()
            ss.kill()
    else:
        raise Exception(f'Invalid method for starting daemon: {method}')