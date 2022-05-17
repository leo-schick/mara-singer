"""Command line interface for running singer default pipelines"""

import sys
import click

from mara_app.monkey_patch import patch
from mara_pipelines import pipelines
import mara_pipelines.ui.cli


@click.command()
@click.option('--tap-name', required=True,
              help='The tap name, e.g. tap-exchangeratesapi')
@click.option('--config-file-name',
              help='The config file name in the singer config path. Default: <tap-name>.json',
              default=None)
@click.option('--catalog-file-name',
              help='The destination catalog file name in the singer catalog path. Default: <tap-name>.json',
              default=None)
@click.option('--disable-colors', default=False, is_flag=True,
              help='Output logs without coloring them.')
def discover(tap_name: str, config_file_name: str = None, catalog_file_name: str = None, disable_colors: bool = False):
    """Run discover for a singer tap"""

    from mara_pipelines.pipelines import Pipeline, Task
    from .commands.singer import SingerTapDiscover



    pipeline = Pipeline(
        id='_singer',
        description="Internal Singer.io management pipeline")

    tap_pipeline = Pipeline(
        id=tap_name.replace('-','_'),
        description=f'Package {tap_name}')

    tap_pipeline.add(
        Task(id='discover',
             description=f'Reload the {tap_name} catalog',
             commands=[
                 SingerTapDiscover(tap_name=tap_name,
                                   config_file_name=config_file_name,
                                   catalog_file_name=catalog_file_name)
             ]))

    pipeline.add(tap_pipeline)

    root_pipeline = Pipeline(
        id='mara_singer',
        description='Root pipeline of module mara_singer')

    root_pipeline.add(pipeline)

    patch(mara_pipelines.config.root_pipeline)(lambda: root_pipeline)

    # the pipeline to run
    for node in pipeline.nodes:
        print(node)
    pipeline, found = pipelines.find_node(['_singer',tap_name.replace('-','_')])
    if not found:
        print(f'Could not find pipeline. You have to add {tap_name} to config mara_singer.config.tap_names to be able to use this command', file=sys.stderr)
        sys.exit(-1)
    if not isinstance(pipeline, pipelines.Pipeline):
        print(f'Internal error: Note is not a pipeline, but a {pipeline.__class__.__name__}', file=sys.stderr)
        sys.exit(-1)

    # a list of nodes to run selectively in the pipeline
    nodes = set()
    nodes.add(pipeline.nodes.get('discover'))

    if not mara_pipelines.ui.cli.run_pipeline(pipeline, nodes, interactively_started=False, disable_colors=disable_colors):
        sys.exit(-1)
