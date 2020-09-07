import os
import json
import pathlib
import enum
import singer.catalog
import singer.metadata

from mara_db import dbs

from . import config

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

    def _load_catalog(self) -> singer.catalog.Catalog:
        if not self._catalog:
            file_path = self.catalog_file_path()
            if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                self._catalog = singer.catalog.Catalog.load(file_path)
            else:
                self._catalog = singer.catalog.Catalog(streams=[])
        return self._catalog

    @property
    def streams(self) -> [str]:
        if not self._streams:
            self._streams = {}
            catalog = self._load_catalog()

            for stream in catalog.streams:
                self._streams[stream.tap_stream_id] = SingerStream(stream)

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
    def __init__(self, stream: singer.catalog.CatalogEntry) -> None:
        self.stream = stream

    @property
    def key_properties(self) -> [str]:
        """The key properties of the stream"""
        return self.stream.key_properties

    @property
    def schema(self):
        """The JSON schema for the stream"""
        return self.stream.schema.to_dict()

    @property
    def is_selected(self):
        mdata = singer.metadata.to_map(self.stream.metadata)
        return self.schema.selected or singer.metadata.get(mdata, (), 'selected')

    def unmark_as_selected(self):
        schema_dict = self.stream.schema.to_dict()
        if 'selected' in schema_dict:
            schema_dict['selected'] = False
            self.stream.schema = singer.schema.Schema.from_dict(schema_dict)

        mdata = singer.metadata.to_map(self.stream.metadata)
        if singer.metadata.get(mdata, (), 'selected'):
            mdata = singer.metadata.write(mdata, (), 'selected', False)

            # set properties to not selected
            for metadata in singer.metadata.to_list(mdata):
                if len(metadata.breadcrumb) == 2 and metadata.breadcrumb[1] == 'properties':
                    if singer.metadata.get(mdata, metadata.breadcrumb, 'selected'):
                        mdata = singer.metadata.write(mdata, (), 'selected', False)

            self.stream.metadata = singer.metadata.to_list(mdata)

    def mark_as_selected(self, properties: [str] = None):
        if properties:
            mdata = singer.metadata.to_map(self.stream.metadata)
            mdata = singer.metadata.write(mdata, (), 'selected', True)

            for property_name in properties:
                mdata = singer.metadata.write(mdata, ('properties', property_name), 'selected', True)

            self.stream.metadata = singer.metadata.to_list(mdata)
        else:
            schema_dict = self.stream.schema.to_dict()
            schema_dict['selected'] = True
            self.stream.schema = singer.schema.Schema.from_dict(schema_dict)

    @property
    def replication_method(self) -> str:
        """Either FULL_TABLE, INCREMENTAL, or LOG_BASED. The replication method to use for a stream."""

        # check for deprecated way of saving the replication_method
        replication_method = self.stream.to_dict().get('replication_method')
        if replication_method: 
            return replication_method

        mdata = singer.metadata.to_map(self.stream.metadata)
        return singer.metadata.get(mdata, (), 'forced-replication-method') or singer.metadata.get(mdata, (), 'replication-method')

    @property
    def replication_key(self) -> str:
        """The name of a property in the source to use as a "bookmark". For example, this will often be an "updated-at" field or an auto-incrementing primary key (requires replication-method)."""

        # check for deprecated way of saving the replication_key
        replication_key = self.stream.to_dict().get('replication_key')
        if replication_key: 
            return replication_key

        mdata = singer.metadata.to_map(self.stream.metadata)
        return singer.metadata.get(mdata, (), 'replication-key')
