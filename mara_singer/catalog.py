from functools import singledispatch, update_wrapper
import pathlib

from mara_db import dbs

from . import config

def methdispatch(func):
    dispatcher = singledispatch(func)
    def wrapper(*args, **kw):
        return dispatcher.dispatch(args[1].__class__)(*args, **kw)
    wrapper.register = dispatcher.register
    update_wrapper(wrapper, func)
    return wrapper

class SingerCatalog:
    def __init__(self, catalog_file_name: str) -> None:
        self.catalog_file_name = catalog_file_name
        
    def catalog_file_path(self) -> pathlib.Path:
        return pathlib.Path(config.catalog_dir()) / self.catalog_file_name

    def get_streams(self) -> [str]:
        streams = []
        import json
        from io import StringIO
        with open(self.catalog_file_path()) as catalog_file:
            catalog = json.load(catalog_file)
            for stream in catalog['streams']:
                streams.append(stream['stream'])

    def stream_to_create_table(self, db: object, stream_name: str, schema_name: str = None, create_if_not_exists: bool = False) -> str:
        import json
        from io import StringIO
        with open(self.catalog_file_path()) as catalog_file:
            catalog = json.load(catalog_file)
            for stream in catalog['streams']:
                if stream['stream'] == stream_name:
                    return self._stream_to_create_table(db, singer_stream=stream,
                        schema_name=schema_name, create_if_not_exists=create_if_not_exists)
        
        raise Exception(f'Could not find stream {stream_name} in catalog')

    @methdispatch
    def _stream_to_create_table(self, db: object, singer_stream, schema_name: str = None, create_if_not_exists: bool = False):
        raise NotImplementedError(f'Please implement SingerCatalog._stream_to_create_table for type "{db.__class__.__name__}"')

    @_stream_to_create_table.register(str)
    def __(self, alias: str, singer_stream, schema_name: str = None, create_if_not_exists: bool = False):
        return self._stream_to_create_table(dbs.db(alias), singer_stream=singer_stream, schema_name=schema_name, create_if_not_exists=create_if_not_exists)

    @_stream_to_create_table.register(dbs.BigQueryDB)
    def __(self, db: dbs.BigQueryDB, singer_stream, schema_name: str = None, create_if_not_exists: bool = False):
        if 'type' not in singer_stream['schema'] or singer_stream['schema']['type'] != 'object':
            raise Exception('The JSON schema must be of type object to be convertable to a SQL table')
        if 'additionalProperties' in singer_stream['schema'] and singer_stream['schema']['additionalProperties'] == True:
            raise Exception('The JSON schema must not allow additional properties in its main object to be convertable to a SQL table')

        key_properties = []
        if 'key_properties' in singer_stream:
            for key_property in singer_stream['key_properties']:
                key_properties.append(key_property)

        fields = []
        for property_name, property_definition in singer_stream['schema']['properties'].items():
            fields.append(self._stream_to_create_table__bigquery__get_property(property_name, property_definition, key_properties))

        table_name=singer_stream['stream']
        if schema_name:
            table_name = f'{schema_name}.{table_name}'

        if create_if_not_exists:
            sql = 'CREATE TABLE IF NOT EXISTS {} (\n  {}\n)'.format(table_name, ',\n  '.join(fields))
        else:
            sql = 'CREATE TABLE {} (\n  {}\n)'.format(table_name, ',\n  '.join(fields))

        return sql

    def _stream_to_create_table__bigquery__get_property(self, property_name, property_definition, key_properties=[]):
        field_type = None
        is_nullable = None

        if not field_type and 'type' in property_definition:
            for type in property_definition['type']:
                if type == "null":
                    is_nullable = True
                elif type == "string":
                    field_type = "STRING"
                    if 'format' in property_definition:
                        if property_definition['format'] == 'date-time':
                            field_type = 'timestamp'
                elif type == "boolean":
                    field_type = "BOOL"
                elif type == "integer":
                    field_type = "INT64"
                elif type == "number":
                    field_type = "NUMERIC"
                elif type == "object":
                    if 'properties' in property_definition:
                        sub_properties = []
                        for sub_property_name, sub_property_definition in property_definition['properties'].items():
                            sub_properties.append(self._stream_to_create_table__bigquery__get_property(sub_property_name, sub_property_definition))

                        if len(sub_properties) == 0:
                            field_type = 'STRING'
                        else:
                            field_type = 'STRUCT<{}>'.format(', '.join(sub_properties))
                    else:
                        raise Exception(f'Unknown usage of type {type} for property {property_name}')

        if not field_type and 'anyOf' in property_definition:
            if property_definition['anyOf'][0]['type'] != "array":
                raise "Unexpected type for property {}".format(property_name)

            field_type = 'ARRAY<{}>'.format(property_definition['anyOf'][0]['items']['type'])

            if property_definition['anyOf'][1]['type'] == "null":
                is_nullable = True

        if not field_type:
            raise Exception(f'Could not determine field type for property {property_name}')

        if is_nullable and property_name in key_properties:
            is_nullable = False

        if is_nullable:
            return '{} {}'.format(property_name, field_type)
        else:
            return '{} {} NOT NULL'.format(property_name, field_type)
