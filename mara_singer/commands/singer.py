import os
import json
import pathlib
import typing as t

from mara_pipelines.logging.logger import log
from mara_pipelines.pipelines import Command
from mara_page import _, html

from ..catalog import SingerCatalog
from .. import config

class _SingerTapCommand(Command):
    """A base command class for interacting with a singer tab"""
    def __init__(self, tap_name: str,
        # optional args for manual config/catalog/state file handling; NOTE might be removed some day!
        config_file_name: str = None, catalog_file_name: str = None, state_file_name: str = None,

        # optional args for special calls; NOTE might be removed some day!
        pass_state_file: bool = None,
        use_legacy_properties_arg: bool = False) -> None:
        #assert all(v is None for v in [config_file_name]), f"unimplemented parameter for _SingerTapCommand"
        self.tap_name = tap_name
        self.config_file_name = config_file_name if config_file_name else f'{tap_name}.json'
        self.state_file_name = state_file_name
        self.pass_state_file = pass_state_file
        self.catalog_file_name = catalog_file_name
        self.use_legacy_properties_arg = use_legacy_properties_arg

    def run(self) -> bool:
        """
        Runs the command

        Returns:
            False on failure
        """
        from .. import shell
        shell_command = self.shell_command()

        return shell.singer_run_shell_command(shell_command)

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
        state = self.state_file_path().read_text().strip('\n') if self.state_file_path().exists() else '-- file not found'

        doc = [
            ('tap name', self.tap_name)
        ]

        if self.config_file_name:
        #    doc.append(('config file name', _.i[self.config_file_name]))
            doc.append((_.i['config file content'], html.highlight_syntax(config, 'json')))

        if self.state_file_name:
        #    doc.append(('state file name', _.i[self.state_file_name]))
            doc.append((_.i['state file content'], html.highlight_syntax(state, 'json')))

        #if self.catalog_file_name:
        #    doc.append(('catalog file name', _.i[self.catalog_file_name]))

        return doc


class _SingerTapReadCommand(_SingerTapCommand):
    """A base command for interacting with a singer tab to read data"""

    def __init__(self, tap_name: str, stream_selection: t.Union[t.List[str], t.Dict[str, t.List[str]]] = None,
        config_file_name: str = None, catalog_file_name: str = None, state_file_name: str = None, use_state_file: bool = True, pass_state_file: bool = False,
        use_legacy_properties_arg: bool = False) -> None:
        super().__init__(tap_name,
            config_file_name=config_file_name,
            catalog_file_name=catalog_file_name if catalog_file_name else f'{tap_name}.json',
            state_file_name=state_file_name if state_file_name else (f'{tap_name}.json' if use_state_file else None),
            pass_state_file=pass_state_file, use_legacy_properties_arg=use_legacy_properties_arg)

        self.stream_selection = stream_selection
 
    def catalog_file_path(self) -> pathlib.Path:
        path = super().catalog_file_path()
        if self.stream_selection:
            path = pathlib.Path(f'{path}.tmp')
        return path

    def _pre_run(self) -> bool:
        """Is called before the tap is called. This is a good place for """
        return True

    def _create_target_config(self, config: dict):
        raise NotImplementedError(f'Please implement _create_target_config() for type "{self.__class__.__name__}"')

    def _target_name(self):
        raise NotImplementedError(f'Please implement _target_name() for type "{self.__class__.__name__}"')

    def _target_config_path(self):
        return pathlib.Path(config.config_dir()) / f'{self._target_name()}.json.tmp'

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
        target_config = {}
        self._create_target_config(target_config)
        tmp_target_config_path = self._target_config_path()
        with open(tmp_target_config_path, 'w') as target_config_file:
            json.dump(target_config, target_config_file)

        # run command
        try:
            # run pre-checks before calling run
            if not self._pre_run():
                return False

            # execute shell command
            if not super().run():
                return False
        finally:
            if self.stream_selection:
                os.remove(tmp_catalog_file_path)
            os.remove(tmp_target_config_path)

        return True

    def shell_command(self):
        command = ((super().shell_command() + ' \\\n')
                   + f'  | {self._target_name()} --config {self._target_config_path()}')

        if self.state_file_name:
            command += (f' >> {self.state_file_path()} \\\n'
                        + f'  ; tail -1 {self.state_file_path()} > {self.state_file_path()}.tmp && mv {self.state_file_path()}.tmp {self.state_file_path()}')

        return command

    def html_doc_items(self) -> [(str, str)]:
        doc = super().html_doc_items() + [
            ('stream selection', html.highlight_syntax(json.dumps(self.stream_selection), 'json') if self.stream_selection else None)
        ]
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
