# Mara Singer

This package contains a mara implementation for [singer.io](https://www.singer.io/).

&nbsp;

## Installation

To use the library directly:

```bash
pip install git+https://github.com/hz-lschick/mara-singer.git
```

&nbsp;

## Example

Here is a sample pipeline for loading Snpachat campaign statisitcs to your datawarehouse into schema `snapchat`.

```python
from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.pipelines import Pipeline, Task

from mara_singer.commands.sql import SingerTapToDB
from mara_singer.commands.singer import SingerTapDiscover


pipeline = Pipeline(
    id="load_snapchat_demo",
    description="Load data from snapchat")

# Note: this does not need to be called on every execution; for convenience only it is put here
pipeline.add(
    Task(id='discover',
         description=f'Load the singer catalog for tap-snapchat-ads',
         commands=[SingerTapDiscover(tap_name='tap-snapchat-ads')]
    )
)

pipeline.add(
    Task(id='load_to_db',
         description='Load streams to db',
         commands=[
             ExecuteSQL(sql_statement='CREATE SCHEMA IF NOT EXISTS snapchat;'),
             SingerTapToDB(tap_name='tap-snapchat-ads',
                           target_schema='snapchat',
                           stream_selection=[
                               'organizations',
                               'ad_accounts',
                               'campaigns',
                               'campaign_stats_daily'
                           ])
         ]
    ), upstreams=['discover']
)
```


&nbsp;

## Getting started

TBD
