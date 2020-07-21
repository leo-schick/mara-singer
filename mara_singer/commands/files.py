import enum
import os
import pathlib
import json
import typing as t

from mara_pipelines.logging.logger import log
from mara_page import _, html

from ..catalog import SingerCatalog
from .singer import _SingerTapCommand

from .. import config

class FileFormat(enum.EnumMeta):
    """Different destination file formats"""
    CSV = 'csv'
    JSONL = 'jsonl'

class SingerTapToFile(_SingerTapCommand):
    def __init__(self, tap_name: str, target_format: FileFormat, destination_dir: str = '', stream_selection: t.Union[t.List[str], t.Dict[str, t.List[str]]] = None,
        config_file_name: str = None, catalog_file_name: str = None, state_file_name: str = None, use_state_file: bool = True, pass_state_file: bool = False,
        use_legacy_properties_arg: bool = False) -> None:
        """
        Runs a tap and writs it data to a file.
        Supported formats: CSV, JSON

        Args:
            tap_name: The tap command name (e.g. tap-exchangeratesapi)
            target_format: The target format, see enum FileFormat
            destination_dir: (default: '') The path to which the files will be written.
            stream_selection: (default: None) The selected streams, when the tap supports several streams. Can be given as stream array or as dict with the properties as value array.
            config_file_name: (default: {tap_name}.json) The tap config file name
            catalog_file_name: (default: {tap_name}.json) The catalog file name
            state_file_name: (default: {tap_name}.json) The state file name
            use_state_file: (default: True) If the state file name should be passed to the tap command
            pass_state_file: (default: False) If the state file shall be passed to the tap. Is only passed when state_file_name is given.
            use_legacy_properties_arg: (default: False) Some old taps do not support the --catalog parameter but still require the deprecated --properties parameter
        """
        super().__init__(tap_name,
            config_file_name=config_file_name,
            catalog_file_name=catalog_file_name if catalog_file_name else f'{tap_name}.json',
            state_file_name=state_file_name if state_file_name else (f'{tap_name}.json' if use_state_file else None),
            pass_state_file=pass_state_file, use_legacy_properties_arg=use_legacy_properties_arg)

        self.target_format = target_format

        self.destination_dir = destination_dir
        self.stream_selection = stream_selection

    def _target_name(self):
        return {
            FileFormat.CSV: 'target-csv',
            FileFormat.JSONL: 'target-jsonl'
        }[self.target_format]

    def catalog_file_path(self) -> pathlib.Path:
        path = super().catalog_file_path()
        if self.stream_selection:
            path = pathlib.Path(f'{path}.tmp')
        return path

    def destination_path(self) -> pathlib.Path:
        return pathlib.Path(config.data_dir()) / self.destination_dir

    def run(self) -> bool:

        # create temp catalog (if necessary)
        tmp_catalog_file_path = None
        if self.stream_selection:
            tmp_catalog_file_path = self.catalog_file_path()

            catalog = SingerCatalog(self.catalog_file_name)
            has_error = False
            if isinstance(self.stream_selection, list):
                for stream_name in self.stream_selection:
                    if stream_name in catalog.streams:
                        catalog.streams[stream_name].mark_as_selected()
                    else:
                        log(message=f"Could not find stream '{stream_name}' in catalog for selection", is_error=True)
                        has_error = True
            elif isinstance(self.stream_selection, dict):
                for stream_name, properties in self.stream_selection.items():
                    if stream_name in catalog.streams:
                        catalog.streams[stream_name].mark_as_selected(properties=properties)
                    else:
                        log(message=f"Could not find stream '{stream_name}' in catalog for selection", is_error=True)
                        has_error = True
            else:
                raise Exception(f'Unexpected type of stream_selection: {self.stream_selection.__class__.__name__}')

            if has_error:
                return False

            catalog.save(tmp_catalog_file_path)

        # create temp target config file
        tmp_target_config_file_path = pathlib.Path(config.config_dir()) / f'{self._target_name()}.json.tmp'

        if self.target_format == FileFormat.JSONL:
            target_config = {
                'destination_path': f'{self.destination_path()}',
                'do_timestamp_file': False
            }

        if self.target_format == FileFormat.CSV:
            # TODO: we miss here a property to disable the timestamp in the file, like tap-jsonl
            target_config = {
                'delimiter': '\t',
                'quotechar': '"',
                'destination_path': f'{self.destination_path()}'
            }

        with open(tmp_target_config_file_path, 'w') as target_config_file:
            json.dump(target_config, target_config_file)

        # run command
        try:
            if not super().run():
                return False
        finally:
            if tmp_catalog_file_path:
                os.remove(tmp_catalog_file_path)
            os.remove(tmp_target_config_file_path)

        return True

    def shell_command(self):
        tmp_target_config_file_path = pathlib.Path(config.config_dir()) / f'{self._target_name()}.json.tmp'

        command = ((super().shell_command() + ' \\\n')
                   + f'  | {self._target_name()} --config {tmp_target_config_file_path}')

        if self.state_file_name:
            command += (f' >> {self.state_file_path()} \\\n'
                        + f'  ; tail -1 {self.state_file_path()} > {self.state_file_path()}.tmp && mv {self.state_file_path()}.tmp {self.state_file_path()}')

        return command

    def html_doc_items(self) -> [(str, str)]:
        doc = super().html_doc_items() + [
            ('stream selection', html.highlight_syntax(json.dumps(self.stream_selection), 'json') if self.stream_selection else None),
            ('taget format', self.target_format),
            ('destination dir', self.destination_dir)
        ]
        return doc
