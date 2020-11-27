from functools import singledispatch
import typing as t

from mara_db import dbs

from . import Table, Column, DataType, StructDataType

# source: https://www.postgresql.org/docs/8.1/sql-keywords-appendix.html
POSTGRESQL_RESERVED_KEYWORDS = [
    'ALL', 'ANALYSE', 'ANALYZE', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', 'ASYMMETRIC', 'BOTH', 'CASE', 'CAST', 'CHECK',
    'COLLATE', 'COLUMN', 'CONSTRAINT', 'CREATE', 'CURRENT_DATE', 'CURRENT_ROLE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
    'CURRENT_USER', 'DEFAULT', 'DEFERRABLE', 'DESC', 'DISTINCT', 'DO', 'ELSE', 'END', 'EXCEPT', 'FALSE', 'FOR',
    'FOREIGN', 'FROM', 'GRANT', 'GROUP', 'HAVING', 'IN', 'INITIALLY', 'INTERSECT', 'INTO', 'LEADING', 'LIMIT',
    'LOCALTIME', 'LOCALTIMESTAMP', 'NEW', 'NOT', 'NULL', 'OFF', 'OFFSET', 'OLD', 'ON', 'ONLY', 'OR', 'ORDER',
    'PLACING', 'PRIMARY', 'REFERENCES', 'SELECT', 'SESSION_USER', 'SOME', 'SYMMETRIC', 'TABLE', 'THEN', 'TO',
    'TRAILING', 'TRUE', 'UNION', 'UNIQUE', 'USER', 'USING', 'WHEN', 'WHERE', 'AUTHORIZATION', 'BETWEEN', 'BINARY',
    'CROSS', 'FREEZE', 'FULL', 'ILIKE', 'INNER', 'IS', 'ISNULL', 'JOIN', 'LEFT', 'LIKE', 'NATURAL', 'NOTNULL', 'OUTER',
    'OVERLAPS', 'RIGHT', 'SIMILAR', 'VERBOSE'
]

# source: https://www.postgresql.org/docs/8.1/sql-keywords-appendix.html
POSTGRESQL_NON_RESERVED_KEYWORDS = [
    'ABORT', 'ABSOLUTE', 'ACCESS', 'ACTION', 'ADD', 'ADMIN', 'AFTER', 'AGGREGATE', 'ALSO', 'ALTER', 'ASSERTION',
    'ASSIGNMENT', 'AT', 'BACKWARD', 'BEFORE', 'BEGIN', 'BY', 'CACHE', 'CALLED', 'CASCADE', 'CHAIN', 'CHARACTERISTICS',
    'CHECKPOINT', 'CLASS', 'CLOSE', 'CLUSTER', 'COMMENT', 'COMMIT', 'COMMITTED', 'CONNECTION', 'CONSTRAINTS',
    'CONVERSION', 'COPY', 'CREATEDB', 'CREATEROLE', 'CREATEUSER', 'CSV', 'CURSOR', 'CYCLE', 'DATABASE', 'DAY',
    'DEALLOCATE', 'DECLARE', 'DEFAULTS', 'DEFERRED', 'DEFINER', 'DELETE', 'DELIMITER', 'DELIMITERS', 'DISABLE',
    'DOMAIN', 'DOUBLE', 'DROP', 'EACH', 'ENABLE', 'ENCODING', 'ENCRYPTED', 'ESCAPE', 'EXCLUDING', 'EXCLUSIVE',
    'EXECUTE', 'EXPLAIN', 'EXTERNAL', 'FETCH', 'FIRST', 'FORCE', 'FORWARD', 'FUNCTION', 'GLOBAL', 'GRANTED', 'HANDLER',
    'HEADER', 'HOLD', 'HOUR', 'IMMEDIATE', 'IMMUTABLE', 'IMPLICIT', 'INCLUDING', 'INCREMENT', 'INDEX', 'INHERIT',
    'INHERITS', 'INPUT', 'INSENSITIVE', 'INSERT', 'INSTEAD', 'INVOKER', 'ISOLATION', 'KEY', 'LANCOMPILER', 'LANGUAGE',
    'LARGE', 'LAST', 'LEVEL', 'LISTEN', 'LOAD', 'LOCAL', 'LOCATION', 'LOCK', 'LOGIN', 'MATCH', 'MAXVALUE', 'MINUTE',
    'MINVALUE', 'MODE', 'MONTH', 'MOVE', 'NAMES', 'NEXT', 'NO', 'NOCREATEDB', 'NOCREATEROLE', 'NOCREATEUSER',
    'NOINHERIT', 'NOLOGIN', 'NOSUPERUSER', 'NOTHING', 'NOTIFY', 'NOWAIT', 'OBJECT', 'OF', 'OIDS', 'OPERATOR', 'OPTION',
    'OWNER', 'PARTIAL', 'PASSWORD', 'PREPARE', 'PREPARED', 'PRESERVE', 'PRIOR', 'PRIVILEGES', 'PROCEDURAL', 'PROCEDURE',
    'QUOTE', 'READ', 'RECHECK', 'REINDEX', 'RELATIVE', 'RELEASE', 'RENAME', 'REPEATABLE', 'REPLACE', 'RESET',
    'RESTART', 'RESTRICT', 'RETURNS', 'REVOKE', 'ROLE', 'ROLLBACK', 'ROWS', 'RULE', 'SAVEPOINT', 'SCHEMA', 'SCROLL',
    'SECOND', 'SECURITY', 'SEQUENCE', 'SERIALIZABLE', 'SESSION', 'SET', 'SHARE', 'SHOW', 'SIMPLE', 'STABLE', 'START',
    'STATEMENT', 'STATISTICS', 'STDIN', 'STDOUT', 'STORAGE', 'STRICT', 'SUPERUSER', 'SYSID', 'SYSTEM', 'TABLESPACE',
    'TEMP', 'TEMPLATE', 'TEMPORARY', 'TOAST', 'TRANSACTION', 'TRIGGER', 'TRUNCATE', 'TRUSTED', 'TYPE', 'UNCOMMITTED',
    'UNENCRYPTED', 'UNKNOWN', 'UNLISTEN', 'UNTIL', 'UPDATE', 'VACUUM', 'VALID', 'VALIDATOR', 'VALUES', 'VARYING',
    'VIEW', 'VOLATILE', 'WITH', 'WITHOUT', 'WORK', 'WRITE', 'YEAR', 'ZONE', 'BIGINT', 'BIT', 'BOOLEAN', 'CHAR',
    'CHARACTER', 'COALESCE', 'CONVERT', 'DEC', 'DECIMAL', 'EXISTS', 'EXTRACT', 'FLOAT', 'GREATEST', 'INOUT', 'INT',
    'INTEGER', 'INTERVAL', 'LEAST', 'NATIONAL', 'NCHAR', 'NONE', 'NULLIF', 'NUMERIC', 'OUT', 'OVERLAY', 'POSITION',
    'PRECISION', 'REAL', 'ROW', 'SETOF', 'SMALLINT', 'SUBSTRING', 'TIME', 'TIMESTAMP', 'TREAT', 'TRIM', 'VARCHAR'
]

# source: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#reserved_keywords
BIGQUERYDB_RESERVED_KEYWORDS = [
    'ALL', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', 'ASSERT_ROWS_MODIFIED', 'AT', 'BETWEEN', 'BY', 'CASE', 'CAST', 'COLLATE',
    'CONTAINS', 'CREATE', 'CROSS', 'CUBE', 'CURRENT', 'DEFAULT', 'DEFINE', 'DESC', 'DISTINCT', 'ELSE', 'END', 'ENUM',
    'ESCAPE', 'EXCEPT', 'EXCLUDE', 'EXISTS', 'EXTRACT', 'FALSE', 'FETCH', 'FOLLOWING', 'FOR', 'FROM', 'FULL', 'GROUP',
    'GROUPING', 'GROUPS', 'HASH', 'HAVING', 'IF', 'IGNORE', 'IN', 'INNER', 'INTERSECT', 'INTERVAL', 'INTO', 'IS',
    'JOIN', 'LATERAL', 'LEFT', 'LIKE', 'LIMIT', 'LOOKUP', 'MERGE', 'NATURAL', 'NEW', 'NO', 'NOT', 'NULL', 'NULLS', 'OF',
    'ON', 'OR', 'ORDER', 'OUTER', 'OVER', 'PARTITION', 'PRECEDING', 'PROTO', 'RANGE', 'RECURSIVE', 'RESPECT', 'RIGHT',
    'ROLLUP', 'ROWS', 'SELECT', 'SET', 'SOME', 'STRUCT', 'TABLESAMPLE', 'THEN', 'TO', 'TREAT', 'TRUE', 'UNBOUNDED',
    'UNION', 'UNNEST', 'USING', 'WHEN', 'WHERE', 'WINDOW', 'WITH', 'WITHIN'
]

# source: https://docs.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql?view=sql-server-ver15
SQLSERVERDB_RESERVED_KEYWORDS = [
    'ADD', 'EXTERNAL', 'PROCEDURE', 'ALL', 'FETCH', 'PUBLIC', 'ALTER', 'FILE', 'RAISERROR', 'AND', 'FILLFACTOR', 'READ',
    'ANY', 'FOR', 'READTEXT', 'AS', 'FOREIGN', 'RECONFIGURE', 'ASC', 'FREETEXT', 'REFERENCES', 'AUTHORIZATION',
    'FREETEXTTABLE', 'REPLICATION', 'BACKUP', 'FROM', 'RESTORE', 'BEGIN', 'FULL', 'RESTRICT', 'BETWEEN', 'FUNCTION',
    'RETURN', 'BREAK', 'GOTO', 'REVERT', 'BROWSE', 'GRANT', 'REVOKE', 'BULK', 'GROUP', 'RIGHT', 'BY', 'HAVING',
    'ROLLBACK', 'CASCADE', 'HOLDLOCK', 'ROWCOUNT', 'CASE', 'IDENTITY', 'ROWGUIDCOL', 'CHECK', 'IDENTITY_INSERT', 'RULE',
    'CHECKPOINT', 'IDENTITYCOL', 'SAVE', 'CLOSE', 'IF', 'SCHEMA', 'CLUSTERED', 'IN', 'SECURITYAUDIT', 'COALESCE',
    'INDEX', 'SELECT', 'COLLATE', 'INNER', 'SEMANTICKEYPHRASETABLE', 'COLUMN', 'INSERT',
    'SEMANTICSIMILARITYDETAILSTABLE', 'COMMIT', 'INTERSECT', 'SEMANTICSIMILARITYTABLE', 'COMPUTE', 'INTO',
    'SESSION_USER', 'CONSTRAINT', 'IS', 'SET', 'CONTAINS', 'JOIN', 'SETUSER', 'CONTAINSTABLE', 'KEY', 'SHUTDOWN',
    'CONTINUE', 'KILL', 'SOME', 'CONVERT', 'LEFT', 'STATISTICS', 'CREATE', 'LIKE', 'SYSTEM_USER', 'CROSS', 'LINENO',
    'TABLE', 'CURRENT', 'LOAD', 'TABLESAMPLE', 'CURRENT_DATE', 'MERGE', 'TEXTSIZE', 'CURRENT_TIME', 'NATIONAL', 'THEN',
    'CURRENT_TIMESTAMP', 'NOCHECK', 'TO', 'CURRENT_USER', 'NONCLUSTERED', 'TOP', 'CURSOR', 'NOT', 'TRAN', 'DATABASE',
    'NULL', 'TRANSACTION', 'DBCC', 'NULLIF', 'TRIGGER', 'DEALLOCATE', 'OF', 'TRUNCATE', 'DECLARE', 'OFF', 'TRY_CONVERT',
    'DEFAULT', 'OFFSETS', 'TSEQUAL', 'DELETE', 'ON', 'UNION', 'DENY', 'OPEN', 'UNIQUE', 'DESC', 'OPENDATASOURCE',
    'UNPIVOT', 'DISK', 'OPENQUERY', 'UPDATE', 'DISTINCT', 'OPENROWSET', 'UPDATETEXT', 'DISTRIBUTED', 'OPENXML', 'USE',
    'DOUBLE', 'OPTION', 'USER', 'DROP', 'OR', 'VALUES', 'DUMP', 'ORDER', 'VARYING', 'ELSE', 'OUTER', 'VIEW', 'END',
    'OVER', 'WAITFOR', 'ERRLVL', 'PERCENT', 'WHEN', 'ESCAPE', 'PIVOT', 'WHERE', 'EXCEPT', 'PLAN', 'WHILE', 'EXEC',
    'PRECISION', 'WITH', 'EXECUTE', 'PRIMARY', 'WITHIN GROUP', 'EXISTS', 'PRINT', 'WRITETEXT', 'EXIT', 'PROC'
]

@singledispatch
def quote_name(db: object, name: str, enforce: bool = False) -> str:
    """
    Makes sure that a SQL identifier is properly quoted if that is necessary

    Args:
        db: the db config or alias
        name: the name of the identifier to be quoted
        enforce: enforces quotation even it might not be necessary
    """
    raise NotImplementedError(f'Please implement function quote_name for type "{db.__class__.__name__}"')

@quote_name.register(str)
def __(alias: str, name: str, enforce: bool = False) -> str:
    return quote_name(dbs.db(alias), name, enforce=enforce)

@quote_name.register(dbs.DB)
def __(db: dbs.PostgreSQLDB, name: str, enforce: bool = False):
    # this here is the default implementation in case this function is not implemented for a specific DB

    do_quote = enforce

    if not do_quote:
        if ' ' in name:
            do_quote = True

    # using SQL standard quotation
    return f'"{name}"' if do_quote else name

@quote_name.register(dbs.PostgreSQLDB)
def __(db: dbs.PostgreSQLDB, name: str, enforce: bool = False):
    do_quote = enforce

    if not do_quote:
        if ' ' in name:
            do_quote = True

    if not do_quote:
        if name.upper() in POSTGRESQL_RESERVED_KEYWORDS:
            do_quote = True
        elif name.upper() in POSTGRESQL_NON_RESERVED_KEYWORDS:
            do_quote = True

    return f'"{name}"' if do_quote else name

@quote_name.register(dbs.BigQueryDB)
def __(db: dbs.BigQueryDB, name: str, enforce: bool = False):
    do_quote = enforce

    if not do_quote:
        if ' ' in name:
            do_quote = True

    if not do_quote:
        if name.upper() in BIGQUERYDB_RESERVED_KEYWORDS:
            do_quote = True

    return f'"{name}"' if do_quote else name

@quote_name.register(dbs.SQLServerDB)
def __(db: dbs.SQLServerDB, name: str, enforce: bool = False):
    do_quote = enforce

    if not do_quote:
        if ' ' in name:
            do_quote = True

    if not do_quote:
        if name.upper() in SQLSERVERDB_RESERVED_KEYWORDS:
            do_quote = True

    return f'[{name}]' if do_quote else name



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
        columns.append(f'{quote_name(db, column.name)} {datatype_definition(db, column.type)}'
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
        columns.append(f'{quote_name(db, column.name)} '
                       +('ARRAY<' if column.is_array else '')
                       +datatype_definition(db, column.type)
                       +('>' if column.is_array else '')
                       +(' NOT NULL' if not column.nullable and not column.is_array else ''))

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
        elif datatype == DataType.XML:
            return 'xml'
        else:
            raise NotImplementedError(f'Unexpected data type for PostgreSQLDB {datatype}')

@datatype_definition.register(dbs.BigQueryDB)
def __(db: dbs.BigQueryDB, datatype: t.Union[DataType, StructDataType]):
    if isinstance(datatype, StructDataType):
        field_definition = []
        for field in datatype.fields:
            field_definition.append(
                ((f'{quote_name(db, field.name)} ' if field.name else '')
                 + ('ARRAY<' if field.is_array else '')
                 + datatype_definition(db, field.type)
                 + ('>' if field.is_array else '')
                 + (' NOT NULL' if not field.nullable and not field.is_array else '')))
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
        sql_column_name = quote_name(db, column.name)
        insert_fields.append(sql_column_name)

        column_type = datatype_definition(db, column.type)

        select_json_property = f'data -> \'{column.name}\''

        if column_type in ['text']:
            # raw text content
            if column.is_array:
                select_field = f'ARRAY(SELECT p FROM jsonb_array_elements_text({select_json_property}) p)'
            else:
                select_field = f'data ->> \'{column.name}\''
        elif column_type in ['date','timestamp','timestamp with time zone','jsonb','xml']:
            # text content to be casted
            if column.is_array:
                select_field = f'ARRAY(SELECT p :: {column_type} FROM jsonb_array_elements_text({select_json_property}) p)'
            else:
                select_field = f'(data ->> \'{column.name}\') :: {column_type}'
        else:
            # value to be casted
            if column.is_array:
                select_field = f'ARRAY(SELECT p :: {column_type} FROM jsonb_array_elements({select_json_property}) p)'
            else:
                if column.nullable:
                    select_field = f"CASE WHEN jsonb_typeof({select_json_property}) != 'null' THEN ({select_json_property}) :: {column_type} END"
                else:
                    select_field = f'({select_json_property}) :: {column_type}'

        if replication_method == ReplicationMethod.INCREMENTAL and column in table.primary_key_columns:
            distinct_on.append(select_field)

        select_field = f'{select_field} AS {sql_column_name}'
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

    if replication_method == ReplicationMethod.INCREMENTAL and distinct_on:
        key_columns = ', '.join(column.name for column in table.primary_key_columns)

        update_set_expressions: [str] = []
        for column in table.columns:
            if column not in table.primary_key_columns:
                sql_column_name = quote_name(db, column.name)
                update_set_expressions.append(f'{sql_column_name} = EXCLUDED.{sql_column_name}')

        sql_statement += f'\nORDER BY {distinct_on_final}, row DESC'
        sql_statement += f'\nON CONFLICT ({key_columns})'
        if update_set_expressions:
            sql_statement += '\nDO\n  UPDATE SET\n    '
            sql_statement += ',\n    '.join(update_set_expressions)
        else:
            sql_statement += '\nDO NOTHING'

    return sql_statement
