import os
import pathlib
import json

from mara_pipelines.logging.logger import log

from .singer import PipeFormat, _SingerTapCommand

from .. import config

class SingerTapToFile(_SingerTapCommand):
    def __init__(self, tap_name: str, target_format: PipeFormat, destination_dir: str = '', selected_streams: [str] = None, config_file_name: str = None, catalog_file_name: str = None, state_file_name: str = None, use_state_file: bool = None) -> None:
        """
        Runs a tap and writs it data to a file.
        Supported formats: CSV, JSON

        Args:
            tap_name: The tap command name (e.g. tap-exchangeratesapi)
            target_format: The target format, see enum PipeFormat
            destination_dir: (default: '') The path to which the files will be written.
            selected_streams: (default: None) The selected streams, when the tap supports several streams
            config_file_name: (default: {tap_name}.json) The tap config file name
            catalog_file_name: (default: {tap_name}.json) The catalog file name
            state_file_name: (default: {tap_name}.json) The state file name
            use_state_file: (default: None) If the state file name should be passed to the tap command
        """
        super().__init__(tap_name,
            config_file_name=config_file_name,
            catalog_file_name=catalog_file_name if catalog_file_name else f'{tap_name}.json',
            state_file_name=state_file_name if state_file_name else (f'{tap_name}.json' if use_state_file else None))

        self.target_format = target_format

        self.destination_dir = destination_dir
        self.selected_streams = selected_streams

    def _target_name(self):
        return {
            PipeFormat.CSV: 'target-csv',
            PipeFormat.JSONL: 'target-jsonl'
        }[self.target_format]

    def catalog_file_path(self) -> pathlib.Path:
        path = super().catalog_file_path()
        if self.selected_streams:
            path = pathlib.Path(f'{path}.tmp')
        return path

    def destination_path(self) -> pathlib.Path:
        return pathlib.Path(config.data_dir()) / self.destination_dir

    def run(self) -> bool:

        # create temp catalog (if necessary)
        tmp_catalog_file_path = None
        if self.selected_streams and len(self.selected_streams) > 0:
            tmp_catalog_file_path = self.catalog_file_path()

            with open(super().catalog_file_path()) as catalog_file:
                catalog = json.load(catalog_file)
            selected_streams = self.selected_streams.copy()
            for catalog_stream in catalog['streams']:
                if 'stream' in catalog_stream:
                    to_select = False
                    if catalog_stream['stream'] in selected_streams:
                        selected_streams.remove(catalog_stream['stream'])
                        to_select = True

                    if 'schema' in catalog_stream:
                        if 'selected' in catalog_stream['schema']:
                            if catalog_stream['schema']['selected'] != to_select:
                                catalog_stream['schema']['selected'] = to_select
                        elif to_select == True:
                            catalog_stream['schema']['selected'] = True
                    elif to_select == True:
                        catalog_stream['schema']['selected'] = True
            if len(selected_streams) > 0:
                log(message="Could not find stream(s) '{}' in catalog for selection".format("', '".join(selected_streams)), is_error=True)
                return False

            with open(tmp_catalog_file_path, 'w') as catalog_file:
                json.dump(catalog, catalog_file)

        # create temp target config file
        tmp_target_config_file_path = pathlib.Path(config.config_dir()) / f'{self._target_name()}.json.tmp'

        if self.target_format == PipeFormat.JSONL:
            target_config = {
                'destination_path': f'{self.destination_path()}',
                'do_timestamp_file': False
            }

        if self.target_format == PipeFormat.CSV:
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
            ('selected streams', self.selected_streams),
            ('taget format', self.target_format),
            ('destination dir', self.destination_dir)
        ]
        return doc
