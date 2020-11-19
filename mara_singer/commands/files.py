import enum
import os
import pathlib
import typing as t

import mara_db.config
from mara_db.shell import Format
from mara_pipelines.logging.logger import log
from mara_pipelines.pipelines import Task
from mara_page import _

from ..catalog import SingerCatalog, SingerStream, ReplicationMethod
from .singer import _SingerTapReadCommand
from .. import config

class FileFormat(enum.EnumMeta):
    """Different destination file formats"""
    CSV = 'csv'
    JSONL = 'jsonl'

class SingerTapToFile(_SingerTapReadCommand):
    def __init__(self,
        tap_name: str, stream_selection: t.Union[t.List[str], t.Dict[str, t.List[str]]],
        target_format: FileFormat, destination_dir: str = '',
        config: dict = None,

        # optional args for manual config/catalog/state file handling; NOTE might be removed some day!
        config_file_name: str = None, catalog_file_name: str = None, state_file_name: str = None,

        # optional args for special calls; NOTE might be removed some day!
        use_state_file: bool = True,
        pass_state_file: bool = True) -> None:
        """
        Reads data from a singer.io tab and writes the content to file per stream.

        Args:
            tap_name: The tap command name (e.g. tap-exchangeratesapi)
            stream_selection: The selected streams, when the tap supports several streams. Can be given as stream array or as dict with the properties as value array.
            target_format: The target format, see enum FileFormat
            destination_dir: (default: '') The path to which the files will be written.
            config: (default: None) A dict which is used to path the tap config file (when it exists) or create a temp config file (when it does not exists)
            config_file_name: (default: {tap_name}.json) The tap config file name
            catalog_file_name: (default: {tap_name}.json) The catalog file name
            state_file_name: (default: {tap_name}.json) The state file name
            use_state_file: (default: True) If the state file name should be passed to the tap command
            pass_state_file: (default: False) If the state file shall be passed to the tap. Is only passed when state_file_name is given.
        """
        super().__init__(tap_name,
            stream_selection=stream_selection,
            config=config, config_file_name=config_file_name,
            catalog_file_name=catalog_file_name if catalog_file_name else f'{tap_name}.json',
            state_file_name=state_file_name if state_file_name else (f'{tap_name}.json' if use_state_file else None),
            pass_state_file=pass_state_file)

        self.target_format = target_format

        self.destination_dir = destination_dir

    def catalog_file_path(self) -> pathlib.Path:
        path = super().catalog_file_path()
        if self.stream_selection:
            path = pathlib.Path(f'{path}.tmp')
        return path

    def destination_path(self) -> pathlib.Path:
        return pathlib.Path(config.data_dir()) / self.destination_dir

    def _target_name(self):
        return {
            FileFormat.CSV: 'target-csv',
            FileFormat.JSONL: 'target-jsonl'
        }[self.target_format]

    def _create_target_config(self, config: dict):
        if self.target_format == FileFormat.JSONL:
            config.update({
                'destination_path': f'{self.destination_path()}',
                'do_timestamp_file': False
            })

        if self.target_format == FileFormat.CSV:
            # TODO: we miss here a property to disable the timestamp in the file, like tap-jsonl
            config.update({
                'delimiter': '\t',
                'quotechar': '"',
                'destination_path': f'{self.destination_path()}'
            })

    def _pre_run(self) -> bool:
        if not os.path.exists(self.destination_path()):
            log(message=f"The destination path '{self.destination_path()}' does not exist.", is_error=True)
            return False
        return True

    def html_doc_items(self) -> [(str, str)]:
        doc = super().html_doc_items() + [
            ('taget format', self.target_format),
            ('destination dir', self.destination_dir)
        ]
        return doc

class SingerSyncFromFile(Task):
    def __init__(self, id: str, description: str, file_name: str, file_format: FileFormat,
                 stream: SingerStream, target_table: str, db_alias: str = None,
                 max_retries: int = None, labels: {str, str} = None) -> None:
        """
        Synchronizes a singer stream to a database table based on a downloaded file via SingerTapToFile.

        Args:
            id: See class Task
            description: See class Task
            labels: See class Task
            max_retries: See class Task

            stream: the singer stream which was used to download the data
            file_name: the local file name
            file_format: the file format used when downloading the data
            target_table: the target table name
            db_alias: the target db alias
        """
        if file_format not in [FileFormat.JSONL]:
            raise ValueError(f'Unsupported file format for ReadSingerStreamFile: {file_format}')

        from mara_pipelines.commands.files import ReadFile, Compression
        from mara_pipelines.commands.sql import ExecuteSQL
        from ..schema.sql import delete_from, extract_jsondoctable_to_target_table
        import mara_singer.schema.sql

        _db_alias = db_alias if db_alias else mara_db.config.default_db_alias()
        replication_method = stream.replication_method if stream.replication_method else ReplicationMethod.FULL_TABLE
        commands = []

        ## build commands list

        table = stream.to_table()
        table.table_name = target_table
        table.schema_name = None

        if replication_method == ReplicationMethod.FULL_TABLE:
            # drop data from the table, depending on the sync. mode from the stream
            commands.append(
                ExecuteSQL(sql_statement=delete_from(_db_alias, table),
                           db_alias=_db_alias))

        db = mara_db.dbs.db(_db_alias)

        # read new data into the table
        if isinstance(db, mara_db.dbs.BigQueryDB):
            if replication_method not in [ReplicationMethod.FULL_TABLE]:
                raise NotImplementedError(f'Unsupported replication method for ReadSingerStreamFile with BigQueryDB: {replication_method}')

            ReadFile(file_name=file_name,
                     compression=Compression.NONE,
                     target_table=target_table,
                     db_alias=_db_alias)

        elif isinstance(db, mara_db.dbs.PostgreSQLDB):
            commands.append(
                ExecuteSQL(sql_statement=f'DROP TABLE IF EXISTS {target_table}__tmp;', db_alias=_db_alias))
            commands.append(
                ExecuteSQL(sql_statement=f'CREATE TABLE IF NOT EXISTS {target_table}__tmp (data jsonb, row BIGINT GENERATED ALWAYS AS IDENTITY);', db_alias=_db_alias))

            commands.append(
                ReadFile(file_name=file_name,
                         format=Format.JSONL,
                         compression=Compression.NONE,
                         target_table=f'{target_table}__tmp',
                         db_alias=_db_alias))
            commands.append(
                ExecuteSQL(sql_statement=extract_jsondoctable_to_target_table(_db_alias, table,
                                                                              source_table_name=f'{target_table}__tmp',
                                                                              replication_method=replication_method),
                           db_alias=_db_alias))
            commands.append(
                # cleanup
                ExecuteSQL(sql_statement=f'DROP TABLE IF EXISTS {target_table}__tmp;', db_alias=_db_alias))

        else:
            raise NotImplementedError(f'Please implement ImportSingerStreamJsonlFile.__init__ for type "{db.__class__.__name__}"')

        super().__init__(id, description, commands=commands, max_retries=max_retries, labels=labels)
