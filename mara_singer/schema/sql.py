from functools import singledispatch
import typing as t

from mara_db import dbs

from . import Table, Column, DataType, StructDataType


@singledispatch
def create_table(db:object, table: Table, if_not_exists: bool = False) -> str:
    raise NotImplementedError(f'Please implement function create_table for type "{db.__class__.__name__}"')

@create_table.register(str)
def __(alias: str, table: Table, if_not_exists: bool = False) -> str:
    return create_table(dbs.db(alias), table, if_not_exists=if_not_exists)

@create_table.register(dbs.PostgreSQLDB)
def __(db: dbs.PostgreSQLDB, table: Table, if_not_exists: bool = False) -> str:
    columns: [str] = []
    for column in table.columns:
        columns.append(f'{column.name} {datatype_definition(db, column.type)}'
                       +('[]' if column.is_array else '')
                       +(' NOT NULL' if not column.nullable else ''))

    primary_key_columns: str = ', '.join(column.name for column in table.primary_key_columns)

    return ('CREATE TABLE '
            +('IF NOT EXISTS ' if if_not_exists else '')
            +(f'{table.schema_name}.' if table.schema_name else '')
            +table.table_name
            +' (\n  '
            +(',\n  '.join(columns))
            +(f',\n  PRIMARY KEY ({primary_key_columns})' if primary_key_columns else '')
            +'\n)')

@create_table.register(dbs.BigQueryDB)
def __(db: dbs.BigQueryDB, table: Table, if_not_exists: bool = False) -> str:
    columns: [str] = []
    for column in table.columns:
        columns.append(f'{column.name} '
                       +('ARRAY<' if column.is_array else '')
                       +datatype_definition(db, column.type)
                       +('>' if column.is_array else '')
                       +(' NOT NULL' if not column.nullable else ''))

    return ('CREATE TABLE '
            +('IF NOT EXISTS ' if if_not_exists else '')
            +(f'{table.schema_name}.' if table.schema_name else '')
            +table.table_name
            +' (\n  '
            +(',\n  '.join(columns))
            +'\n)')


@singledispatch
def drop_table(db: object, table: Table, if_exists: bool = False) -> str:
    raise NotImplementedError(f'Please implement function drop_table for type "{db.__class__.__name__}"')

@drop_table.register(str)
def __(alias: str, table: Table, if_exists: bool = False) -> str:
    return drop_table(dbs.db(alias), table, if_exists=if_exists)

@drop_table.register(dbs.DB)
def __(db: dbs.DB, table: Table, if_exists: bool = False) -> str:
    return ('DROP TABLE '
            + ('IF EXISTS ' if if_exists else '')
            + (f'{table.schema_name}.' if table.schema_name else '')
            + table.table_name)



@singledispatch
def delete_from(db: object, table: Table) -> str:
    raise NotImplementedError(f'Please implement function delete_from for type "{db.__class__.__name__}"')

@delete_from.register(str)
def __(alias: str, table: Table) -> str:
    return delete_from(dbs.db(alias), table)

@delete_from.register(dbs.DB)
def __(db: dbs.DB, table: Table):
    return ('DELETE FROM '
            + (f'{table.schema_name}.' if table.schema_name else '')
            + table.table_name)

@delete_from.register(dbs.BigQueryDB)
def __(db: dbs.BigQueryDB, table: Table):
    return ('DELETE FROM '
            + (f'{table.schema_name}.' if table.schema_name else '')
            + table.table_name
            + ' WHERE 1=1') # BigQuery needs a where clause!

@delete_from.register(dbs.SQLServerDB)
def __(db: dbs.DB, table: Table):
    return ('TRUNCATE TABLE '
            + (f'{table.schema_name}.' if table.schema_name else '')
            + table.table_name)


@singledispatch
def datatype_definition(db: object, datatype: t.Union[DataType, StructDataType]) -> str:
    raise NotImplementedError(f'Please implement function datatype_definition for type "{db.__class__.__name__}"')

@datatype_definition.register(str)
def __(alias: str, datatype: t.Union[DataType, StructDataType]) -> str:
    return datatype_definition(dbs.db(alias), datatype=datatype)

@datatype_definition.register(dbs.PostgreSQLDB)
def __(db: dbs.PostgreSQLDB, datatype: t.Union[DataType, StructDataType]):
    if isinstance(datatype, StructDataType):
        return 'jsonb' # for PostgreSQL we just save structured data in a JSONB object
    else:
        if datatype == DataType.INT:
            return 'bigint'
        elif datatype == DataType.NUMBER:
            return 'numeric'
        elif datatype == DataType.TEXT:
            return 'text'
        elif datatype == DataType.DATE:
            return 'date'
        elif datatype == DataType.TIMESTAMP:
            return 'timestamp'
        elif datatype == DataType.TIMESTAMPTZ:
            return 'timestamp with time zone'
        elif datatype == DataType.BOOL:
            return 'boolean'
        elif datatype == DataType.JSON:
            return 'jsonb'
        #elif datatype == DataType.XML:
        #    return ''
        else:
            raise NotImplementedError(f'Unexpected data type for PostgreSQLDB {datatype}')

@datatype_definition.register(dbs.BigQueryDB)
def __(db: dbs.BigQueryDB, datatype: t.Union[DataType, StructDataType]):
    if isinstance(datatype, StructDataType):
        field_definition = []
        for field in datatype.fields:
            field_definition.append(
                ((f'{field.name} ' if field.name else '')
                 + ('ARRAY<' if field.is_array else '')
                 + datatype_definition(db, field.type)
                 + ('>' if field.is_array else '')
                 + (' NOT NULL' if not field.nullable else '')))
        return 'STRUCT<{}>'.format(', '.join(field_definition))
    else:
        if datatype == DataType.INT:
            return 'INT64'
        elif datatype == DataType.NUMBER:
            return 'NUMERIC'
        elif datatype == DataType.TEXT:
            return 'STRING'
        elif datatype == DataType.DATE:
            return 'DATE'
        elif datatype == DataType.TIMESTAMP:
            return 'TIMESTAMP'
        elif datatype == DataType.TIMESTAMPTZ:
            return 'TIMESTAMP'
        elif datatype == DataType.BOOL:
            return 'BOOL'
        elif datatype == DataType.JSON:
            return 'STRING'
        elif datatype == DataType.XML:
            return 'STRING'
        else:
            raise NotImplementedError(f'Unexpected DataType for BigQueryDB {datatype}')


from ..catalog import ReplicationMethod

@singledispatch
def extract_jsondoctable_to_target_table(db: object, table: Table, source_table_name: str, replication_method: ReplicationMethod = ReplicationMethod.FULL_TABLE) -> str:
    """
    Expecting you imported JSON data from a table with a
        'data' column (JSON OBJECT)
        'row' column (running increment integer)
    into a temp table, this function will return a sync. query to transfer all data
    from your temp data into the table.

    The target table name is taken from 'table'.

    Args:
        table: the destination table structure
        source_table_name: the source table with a 'data' column with the JSON data
        replication_method: defines the replication method which will be used to generate the table:
            INCREMENTAL --> creates an INSERT INTO .. SELECT DISTINCT ON (..)  .. ORDER BY .., row DESC ON CONFLICT UPDATE query statement
            FULL_TABLE --> creates an INSERT INTO .. SELECT query statement (it is expected that you execute a DELETE before; this is not done with this statement)
            LOG_BASED --> creates an INSERT INTO .. SELECT query statement
    """
    raise NotImplementedError(f'Please implement function extract_jsondoctable_to_target for type "{db.__class__.__name__}"')

@extract_jsondoctable_to_target_table.register(str)
def __(alias: str, table: Table, source_table_name: str, replication_method: ReplicationMethod = ReplicationMethod.FULL_TABLE) -> str:
    return extract_jsondoctable_to_target_table(dbs.db(alias), table, source_table_name, replication_method=replication_method)

@extract_jsondoctable_to_target_table.register(dbs.PostgreSQLDB)
def __(db: dbs.PostgreSQLDB, table: Table, source_table_name: str, replication_method: ReplicationMethod = ReplicationMethod.FULL_TABLE) -> str:
    insert_fields = []
    select_fields = []
    distinct_on = []
    for column in table.columns:
        insert_fields.append(column.name)

        column_type = datatype_definition(db, column.type)

        select_json_property = f'data -> \'{column.name}\''

        if column.is_array:
            select_field = f'ARRAY(SELECT CAST( p AS {column_type} ) FROM jsonb_array_elements({select_json_property}) p)'
        elif column_type == 'timestamp with time zone':
            select_field = f'CAST(CAST({select_json_property} AS TEXT) AS timestamptz)'
        else:
            select_field = f'CAST({select_json_property} AS {column_type})'

        if replication_method == ReplicationMethod.INCREMENTAL and column in table.primary_key_columns:
            distinct_on.append(select_field)

        select_field = f'{select_field} AS {column.name}'
        select_fields.append(select_field)

    field_list = ',\n  '.join(insert_fields)
    select_clause = ',\n  '.join(select_fields)

    sql_statement = ('INSERT INTO '
                     +(f'{table.schema_name}.' if table.schema_name else '')
                     +table.table_name
                     +f'\n(\n  {field_list}\n)')

    sql_statement += '\nSELECT'

    distinct_on_final = None
    if replication_method == ReplicationMethod.INCREMENTAL and distinct_on:
        if len(distinct_on) > 1:
            raise NotImplementedError('distinct on with multiple columns has not been properly tested!')
        distinct_on_final = ' || '.join(distinct_on)
        sql_statement += f'\n  DISTINCT ON ({distinct_on_final})'
    sql_statement += f'\n  {select_clause}\nFROM {source_table_name}'

    if replication_method == ReplicationMethod.INCREMENTAL:
        key_columns = ', '.join(column.name for column in table.primary_key_columns)

        update_set_expressions: [str] = []
        for column in table.columns:
            if column not in table.primary_key_columns:
                update_set_expressions.append(f'{column.name} = EXCLUDED.{column.name}')

        sql_statement += f'\nORDER BY {distinct_on_final}, row DESC'
        sql_statement += f'\nON CONFLICT ({key_columns})'
        if update_set_expressions:
            sql_statement += '\nDO\n  UPDATE SET\n    '
            sql_statement += ',\n    '.join(update_set_expressions)
        else:
            sql_statement += '\nDO NOTHING'

    return sql_statement
