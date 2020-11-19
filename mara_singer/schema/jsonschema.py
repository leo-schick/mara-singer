import typing as t

from . import Column, DataType, StructDataType


def property_defintion_to_datatype(property_definition) -> (t.Union[DataType, StructDataType], bool, bool):
    field_type = None
    is_nullable = False
    is_array = False

    if not field_type and 'type' in property_definition:
        if isinstance(property_definition['type'], list):
            typeList = property_definition['type']
        else:
            typeList = [property_definition['type']]

        for type in typeList:
            if type == "null":
                is_nullable = True
            elif type == "object":
                if 'properties' in property_definition:
                    struct = StructDataType(name=None)
                    for sub_property_name, sub_property_definition in property_definition['properties'].items():
                        (sub_type, sub_is_nullable, sub_is_array) = property_defintion_to_datatype(sub_property_definition)
                        if sub_property_name == 'amount':
                            print(f'sub_property_name: amount; nullable={sub_is_nullable}')
                        struct.add_field(name=sub_property_name,
                                         type=sub_type,
                                         nullable=sub_is_nullable,
                                         is_array=sub_is_array)

                    field_type = struct
                else:
                    raise Exception(f'Unknown usage of type {type}')
            elif type == "array":
                if 'items' in property_definition:
                    # TODO: duplicated array is not read here!
                    (field_type, _, _) = property_defintion_to_datatype(property_definition=property_definition['items'])
                    is_array = True
            else:
                if 'format' in property_definition:
                    field_type = property_type_to_datatype(type=type, format=property_definition['format'])
                else:
                    field_type = property_type_to_datatype(type=type)

    if not field_type and 'anyOf' in property_definition:
        if property_definition['anyOf'][0]['type'] != "array":
            raise "Unexpected type"

        # TODO: duplicated array is not read here!
        (field_type, _, _) = property_defintion_to_datatype(property_definition=property_definition['anyOf'][0]['items'])
        is_array = True

        if property_definition['anyOf'][1]['type'] == "null":
            is_nullable = True

    return (field_type, is_nullable, is_array)

def property_type_to_datatype(type: str, format: str = None) -> DataType:
    if type == 'string':
        if format == 'date':
            return DataType.DATE
        if format == 'date-time':
            return DataType.TIMESTAMPTZ
        return DataType.TEXT
    if type == 'boolean':
        return DataType.BOOL
    if type == 'integer':
        return DataType.INT
    if type == 'number':
        return DataType.NUMBER

    raise Exception(f'Could not map type \'{type}\' with format \'{format}\'')
