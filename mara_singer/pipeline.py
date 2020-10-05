"""Contains an internal pipeline for singer tasks runable """

from mara_pipelines.commands.bash import RunBash
from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.pipelines import Pipeline, Task

from mara_singer.commands.singer import SingerTapDiscover

import mara_singer.config

pipeline = Pipeline(
    id='_singer',
    description="Internal Singer.io management pipeline")

for tap_name in mara_singer.config.tap_names():
    tap_pipeline = Pipeline(
        id=tap_name.replace('-','_'),
        description=f'Package {tap_name}')

    tap_pipeline.add(
        Task(id='discover',
            description=f'Reload the {tap_name} catalog',
            commands=[SingerTapDiscover(tap_name=tap_name)]))

    pipeline.add(tap_pipeline)

def _internal_root_pipeline():
    root_pipeline = Pipeline(
        id='mara_singer',
        description='Root pipeline of module mara_singer')

    root_pipeline.add(pipeline)

    return root_pipeline
