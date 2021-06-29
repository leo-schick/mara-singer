import os
import json
import pathlib
import enum
import typing as t

from .singer import catalog as singer_catalog
from .singer import metadata as singer_metadata
from .singer import schema as singer_schema

from . import config
from .schema import Table

class ReplicationMethod(enum.EnumMeta):
    """Different replication methods for a stream"""
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    LOG_BASED = "LOG_BASED"

class SingerCatalog:
    def __init__(self, catalog_file_name: str) -> None:
        self.catalog_file_name = catalog_file_name

        # cache for loaded
        self._catalog = None
        self._streams = None

    def catalog_file_path(self) -> pathlib.Path:
        return pathlib.Path(config.catalog_dir()) / self.catalog_file_name

    def _load_catalog(self) -> singer_catalog.Catalog:
        if not self._catalog:
            file_path = self.catalog_file_path()
            if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                self._catalog = singer_catalog.Catalog.load(file_path)
            else:
                self._catalog = singer_catalog.Catalog(streams=[])
        return self._catalog

    @property
    def streams(self) -> t.List[str]:
        if not self._streams:
            self._streams = {}
            catalog = self._load_catalog()

            for stream in catalog.streams:
                self._streams[stream.tap_stream_id] = SingerStream(name=stream.tap_stream_id, stream=stream)

        return self._streams

    def save(self, catalog_file_path: str = None):
        """
        Saves the changes of a catalog file
        
        Args:
            catalog_file_path: (Optional) When you don't want to modify the existing file, but want to save the changes into another file
        """

        if not self._catalog: # nothing changed
            if catalog_file_path:
                self._load_catalog() 
            else:
                return # nothing changed and no other file name give --> no need to save

        if not catalog_file_path:
            catalog_file_path = self.catalog_file_path()

        with open(catalog_file_path,'w') as catalog_file:
            json.dump(self._catalog.to_dict(), catalog_file)


class SingerStream:
    def __init__(self, name: str, stream: singer_catalog.CatalogEntry) -> None:
        self.name = name
        self.stream = stream

    @property
    def key_properties(self) -> t.List[str]:
        """The key properties of the stream"""
        key_properties = self.stream.key_properties
        if not key_properties:
            mdata = singer_metadata.to_map(self.stream.metadata)
            key_properties = singer_metadata.get(mdata, (), 'table-key-properties') or singer_metadata.get(mdata, (), 'view-key-properties')
        return key_properties

    @property
    def schema(self):
        """The JSON schema for the stream"""
        return self.stream.schema.to_dict()

    @property
    def is_selected(self):
        mdata = singer_metadata.to_map(self.stream.metadata)
        schema_dict = self.schema
        if 'selected' in schema_dict:
            return schema_dict['selected']
        return singer_metadata.get(mdata, (), 'selected')

    def property_is_selected(self, property_name: str):
        mdata = singer_metadata.to_map(self.stream.metadata)
        return singer_metadata.get(mdata, ('properties', property_name), 'selected')

    def unmark_as_selected(self):
        schema_dict = self.schema
        if 'selected' in schema_dict:
            schema_dict['selected'] = False
            self.stream.schema = singer_metadata.Schema.from_dict(schema_dict)

        mdata = singer_metadata.to_map(self.stream.metadata)
        if singer_metadata.get(mdata, (), 'selected'):
            mdata = singer_metadata.write(mdata, (), 'selected', False)

            # set properties to not selected
            for metadata in singer_metadata.to_list(mdata):
                if len(metadata.breadcrumb) == 2 and metadata.breadcrumb[1] == 'properties':
                    if singer_metadata.get(mdata, metadata.breadcrumb, 'selected'):
                        mdata = singer_metadata.write(mdata, (), 'selected', False)

            self.stream.metadata = singer_metadata.to_list(mdata)

    def mark_as_selected(self, properties: t.List[str] = None):
        mdata = singer_metadata.to_map(self.stream.metadata)
        mdata = singer_metadata.write(mdata, (), 'selected', True)

        def breadcrumb_name(breadcrumb):
            name = ".".join(breadcrumb)
            name = name.replace('properties.', '')
            name = name.replace('.items', '[]')
            return name

        for breadcrumb, _ in mdata.items():
            if breadcrumb != ():
                selected = False

                property_name = breadcrumb_name(breadcrumb)

                if singer_metadata.get(mdata, breadcrumb, 'inclusion') == 'automatic':
                    selected = True
                elif properties:
                    selected = property_name in properties
                elif singer_metadata.get(mdata, breadcrumb, 'selected-by-default'):
                    selected = True

                if property_name in (properties or []):
                    selected = True

                mdata = singer_metadata.write(mdata, breadcrumb, 'selected', selected)

        self.stream.metadata = singer_metadata.to_list(mdata)

        if not properties:
            # legacy implementation
            schema_dict = self.schema
            schema_dict['selected'] = True

            self.stream.schema = singer_schema.Schema.from_dict(schema_dict)

    @property
    def replication_method(self) -> str:
        """Either FULL_TABLE, INCREMENTAL, or LOG_BASED. The replication method to use for a stream."""

        # check for deprecated way of saving the replication_method
        replication_method = self.stream.to_dict().get('replication_method')
        if replication_method: 
            return replication_method

        mdata = singer_metadata.to_map(self.stream.metadata)
        return singer_metadata.get(mdata, (), 'forced-replication-method') or singer_metadata.get(mdata, (), 'replication-method')

    @property
    def replication_key(self) -> str:
        """The name of a property in the source to use as a "bookmark". For example, this will often be an "updated-at" field or an auto-incrementing primary key (requires replication-method)."""

        # check for deprecated way of saving the replication_key
        replication_key = self.stream.to_dict().get('replication_key')
        if replication_key: 
            return replication_key

        mdata = singer_metadata.to_map(self.stream.metadata)
        return singer_metadata.get(mdata, (), 'replication-key')

    def to_table(self) -> Table:
        """
        Creates a Table object from the JSON schema behind the singer stream
        Only the selected properties will be added as columns to the table. When no selection marks
        exist, the default selection applies
        """
        schema_dict = self.schema
        if 'type' not in schema_dict or 'object' not in schema_dict['type']:
            raise Exception(f'The JSON schema for stream {self.name} must be of type object to be convertable to a SQL table')

        from .schema import jsonschema
        from .schema import Table

        mdata = singer_metadata.to_map(self.stream.metadata)
        schema_name = singer_metadata.get(mdata, (), 'schema-name')

        table = Table(
            table_name=self.name,
            schema_name=schema_name)

        key_properties = self.key_properties or []

        use_property_selection = self.is_selected

        for property_name, property_definition in schema_dict['properties'].items():
            selected = False
            if use_property_selection:
                if self.property_is_selected(property_name):
                    selected = True
            else:
                # use default selection
                if singer_metadata.get(mdata, ('properties', property_name), 'inclusion') == 'automatic':
                    selected = True
                elif singer_metadata.get(mdata, ('properties', property_name), 'selected-by-default'):
                    selected = True

            if selected:
                (datatype, is_nullable, is_array) = jsonschema.property_defintion_to_datatype(property_definition)

                if is_nullable and property_name in key_properties:
                    is_nullable = False

                table.add_column(
                    name=property_name,
                    type=datatype,
                    nullable=is_nullable,
                    is_array=is_array,
                    is_primary_key=(property_name in key_properties))

        return table
