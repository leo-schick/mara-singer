import enum
import typing as t


class DataType(enum.EnumMeta):
    INT = 'int'
    NUMBER = 'number'
    TEXT = 'text'
    DATE = 'date'
    TIMESTAMP = 'timestamp' # a date time without timezone
    TIMESTAMPTZ = 'timestamptz' # a date time with timezone
    BOOL = 'bool'

    # special types
    STRUCT = 'struct' # a complex type
    JSON = 'json'
    XML = 'xml'

class StructField():
    def __init__(self, name: t.Union[str, None], type: object, # type: t.Union[DataType,StructDataType]
                 nullable: bool = True, is_array: bool = False):
        """
        A field of a struct

        Args:
            name: the name of the field. Can be None
            type: the data type of the field
        """
        self.name = name
        self.type = type
        self.nullable = nullable
        self.is_array = is_array

class StructDataType():
    def __init__(self, name: t.Union[str, None]):
        """
        A struct data type
        Args:
            name: the name for the struct. Can be None
        """
        self.name = name
        self.fields = []
    
    def add_field(self, name: t.Union[str, None], type: DataType, nullable: bool = True, is_array: bool = False):
        self.fields.append(
            StructField(name=name, type=type, nullable=nullable, is_array=is_array))

class Column():
    def __init__(self, name: str, type: t.Union[DataType,StructDataType], nullable: bool = True, is_array: bool = False):
        """
        A column of a table
        Args:
            name: the corresponding name in the database table
            type: The type of the column
        """
        self.name = name
        self.type = type
        self.nullable = nullable
        self.is_array = is_array


class Table():
    def __init__(self, table_name: str, schema_name: str = None):
        self.table_name = table_name
        self.schema_name = schema_name
        self.columns = []
        self.primary_key_columns = []
    
    def add_column(self, name: str, type: t.Union[DataType,StructDataType], nullable: bool = None, is_array: bool = False, is_primary_key: bool = False):
        if is_primary_key and nullable:
            raise ValueError('A primary key column can not be nullable')
        if nullable is None:
            nullable = not is_primary_key

        column = Column(name, type, nullable=nullable, is_array=is_array)
        self.columns.append(column)
        if is_primary_key:
            self.primary_key_columns.append(column)
