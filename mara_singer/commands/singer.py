import os
import pathlib

from mara_pipelines.pipelines import Command
from mara_page import _, html

from .. import config

class _SingerTapCommand(Command):
    def __init__(self, tap_name: str, config_file_name: str = None, catalog_file_name: str = None, state_file_name: str = None, pass_state_file: bool = None, use_legacy_properties_arg: bool = False) -> None:
        #assert all(v is None for v in [config_file_name]), f"unimplemented parameter for _SingerTapCommand"
        self.tap_name = tap_name
        self.config_file_name = config_file_name if config_file_name else f'{tap_name}.json'
        self.state_file_name = state_file_name
        self.pass_state_file = pass_state_file
        self.catalog_file_name = catalog_file_name
        self.use_legacy_properties_arg = use_legacy_properties_arg

    def config_file_path(self) -> pathlib.Path:
        return pathlib.Path(config.config_dir()) / self.config_file_name

    def state_file_path(self) -> pathlib.Path:
        return pathlib.Path(config.state_dir()) / self.config_file_name

    def catalog_file_path(self) -> pathlib.Path:
        return pathlib.Path(config.catalog_dir()) / self.catalog_file_name

    def shell_command(self):
        state_file_path = None
        if self.state_file_name and os.path.exists(self.state_file_path()) and os.stat(self.state_file_path()).st_size != 0:
            state_file_path = self.state_file_path()

        command = (f'{self.tap_name}'
                + f' --config {self.config_file_path()}'
                + (f' --state {state_file_path}' if state_file_path and self.pass_state_file else ''))

        if self.use_legacy_properties_arg:
            command += f' --properties {self.catalog_file_path()}' if self.catalog_file_name else ''
        else:
            command += f' --catalog {self.catalog_file_path()}' if self.catalog_file_name else ''

        return command

    def html_doc_items(self) -> [(str, str)]:
        config = self.config_file_path().read_text().strip('\n') if self.config_file_path().exists() else '-- file not found'

        doc = [
            ('tap name', self.tap_name)
        ]

        if self.config_file_name:
            doc.append(('config file name', _.i[self.config_file_name]))
            doc.append((_.i['config file content'], html.highlight_syntax(config, 'json')))

        if self.state_file_name:
            doc.append(('state file name', _.i[self.state_file_name]))

        if self.catalog_file_name:
            doc.append(('catalog file name', _.i[self.catalog_file_name]))

        return doc


class SingerTapDiscover(_SingerTapCommand):
    def __init__(self, tap_name: str, config_file_name: str = None, catalog_file_name: str = None) -> None:
        """
        Runs a tap discover and writes it to a catalog file.
        See also: https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode

        Args:
            tap_name: The tap command name (e.g. tap-exchangeratesapi)
            config_file_name: (default: {tap_name}.json) The tap config file name
            catalog_file_name: (default: {tap_name}.json) The catalog file name
        """
        assert all(v is None for v in [config_file_name, catalog_file_name]), f"unimplemented parameter for TapDiscover"
        super().__init__(tap_name, config_file_name=config_file_name)
        self.new_catalog_file_name = catalog_file_name if catalog_file_name else f'{tap_name}.json'

    def new_catalog_file_path(self) -> pathlib.Path:
        return pathlib.Path(config.catalog_dir()) / self.new_catalog_file_name

    def shell_command(self):
        return (super().shell_command() + f" --discover > {self.new_catalog_file_path()}")

    def html_doc_items(self) -> [(str, str)]:
        doc = super().html_doc_items()
        doc.append(('catalog file name', _.i[self.new_catalog_file_name]))
        return doc
