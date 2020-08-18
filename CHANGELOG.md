# Changelog

## 0.7.1

- add 'config' arg. to commands, making it possible to path the config file

## 0.7.0

- refactoring; breaking changes:
    - base path is now `/app/singer/`, not `/data/singer/` anymore
    - rename `requirements.singer.txt` to `singer-requirements.txt`
    - rename singer venv path from `/.singer-venv` to `/.singer`

## 0.6.0

- add SingerTapToDB command

## 0.5.2

- fix .scripts/install.mk script

## 0.5.1

- fix execute permission for .scripts/singer-cli.sh

## 0.5.0

- add functions for sync. and swipe-up of old transactions
- add replication_method and replication_key to SingerStream

## 0.4.3

- fix do not break when state file is empty (+ add unit test for class SingerTapState)

## 0.4.2

- fix lag reading sinter-tap log from stderr [#3](https://github.com/hz-lschick/mara-singer/issues/3)

## 0.4.1

- add SingerConfig/SingerTapState classes

## 0.4.0

- implement log handling for singer taps
- update dependencies for BigQuery requirement

## 0.3.3

- add support DATE data type in create-table from jsonschema function

## 0.3.2

- add proper error message when destination path for SingerTapToFile does not exist
- fix issue with create-table from jsonschema function when type is not defined in array
- fix issue with BigQuery create-table from jsonschema function when array is not nullable

## 0.3.1

- fix new catalog creation
- fix issue with jsonschema array table creation

## 0.3.0

- add support for git+ installation via singer-cli.sh
- add support for requirements installation via singer-cli.sh
- fix: when catalog file does not exist, create a new catalog

## 0.2.0

- add support for stream property selection

## 0.1.0

- Initial version
