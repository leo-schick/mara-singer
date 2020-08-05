# Changelog

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
