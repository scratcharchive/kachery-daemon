import json
import os
import sys
from typing import Any, List, Union, cast
from .version import __version__
from .start_daemon import start_daemon
from ._daemon_connection import _get_node_id, _read_client_auth_code

import click


@click.group(help=f'Kachery daemon command-line client (version {__version__})')
def cli():
    pass

@click.command(help="Start the kachery daemon.")
@click.option('--label', required=True, help='Label for this node')
@click.option('--owner', default='', help='Owner of this node (google ID)')
@click.option('--verbose', default=0, help='Verbosity level')
@click.option('--method', default='npm', help='Method for starting daemon: npm (default) or dev')
@click.option('--node-arg', multiple=True, help='Additional arguments to send to node')
@click.option('--auth-group', default='', help='The os group that has access to this daemon')
@click.option('--kachery-hub-url', default='https://kacheryhub.org', help='The url for the kacheryhub app')
def start(label: str, owner: str, method: str, verbose: int, node_arg: List[str], auth_group: str, kachery_hub_url: str):
    start_daemon(
        label=label,
        owner=owner,
        method=method,
        verbose=verbose,
        node_arg=node_arg,
        auth_group=auth_group,
        kachery_hub_url=kachery_hub_url
    )

@click.command(help="Print information about this node.")
def info():
    node_id = _get_node_id()
    try:
        client_auth_code = _read_client_auth_code()
    except:
        client_auth_code = None
    print(f'Node ID: {node_id}')
    if client_auth_code:
        print('You have access to this daemon')
    else:
        print('You do not have access to this daemon')
        

@click.command(help="Display kachery_daemon version and exit.")
def version():
    click.echo(f"This is kachery_daemon version {__version__}")
    exit()

cli.add_command(start)
cli.add_command(info)
cli.add_command(version)
