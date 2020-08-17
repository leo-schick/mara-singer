# Mara Singer

This package contains a mara implementation for [singer.io](https://www.singer.io/).

:warning: This package is under development, breaking changes might be done during each minor version iteration.

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

## Install guide

*Note:* This install guide is based on a [Mara Example Project 1](https://github.com/mara/mara-example-project-1). In case you use another project, you might need to make some additional adjustments.

### Step 1 -- add/ensure requirements

Edit the requirements.txt:
* make sure that mara-db uses 4.7.0 or higher
* add the line `-e git+https://github.com/hz-lschick/mara-singer.git@0.7.0#egg=mara-singer` to the file

### Step 2 -- Install module

Call the following shell commands to install the mara-singer package:
```shell
make update-packages
mkdir ./app/singer
mkdir ./app/singer/catalog
mkdir ./app/singer/config
mkdir ./app/singer/state
touch ./app/singer/catalog/.gitkeep
touch ./app/singer/config/.gitkeep
touch ./app/singer/state/.gitkeep
rsync --archive --recursive --itemize-changes  --delete packages/mara-singer/.scripts/ .scripts/mara-singer/
echo '/.singer
/app/singer/config
/app/singer/catalog/*.tmp
/app/singer/state' >> .gitignore
echo '
# singer package install
include .scripts/mara-singer/install.mk' >> Makefile
```

### Step 3 -- Install singer packages

Add a new file `singer-requirements.txt` with the following content:

```requirements.txt
# taps

# ... here you can add the taps you want to sue

# targets
# default targets
target-jsonl==0.1.2
-e git+https://github.com/datamill-co/target-postgres.git@v0.2.4#egg=target-postgres

# Optional for manual catalog management
#-e git+https://github.com/chrisgoddard/singer-discover.git#egg=singer-discover
```

Add the taps you want to use to the `singer-requirements.txt` file.
Then call:

```shell
make install-singer-packages
```

### Step 4 -- Push your changes to git

To finalize the installation, push the changes to git:

```shell
git add *
git add .gitignore
git add .scripts/mara-singer/
git add -f ./app/singer/config/.gitkeep
git add -f ./app/singer/state/.gitkeep
git commit -m 'install mara-singer module'
```

Congratulation :tada: you have now completed the mara-singer package installation!

&nbsp;

## Getting started

TBD
