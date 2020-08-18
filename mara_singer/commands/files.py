import enum
import os
import pathlib
import typing as t

from mara_pipelines.logging.logger import log
from mara_page import _

from ..catalog import SingerCatalog
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
        pass_state_file: bool = True,
        use_legacy_properties_arg: bool = False) -> None:
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
            use_legacy_properties_arg: (default: False) Some old taps do not support the --catalog parameter but still require the deprecated --properties parameter
        """
        super().__init__(tap_name,
            stream_selection=stream_selection,
            config=config, config_file_name=config_file_name,
            catalog_file_name=catalog_file_name if catalog_file_name else f'{tap_name}.json',
            state_file_name=state_file_name if state_file_name else (f'{tap_name}.json' if use_state_file else None),
            pass_state_file=pass_state_file, use_legacy_properties_arg=use_legacy_properties_arg)

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
