import pathlib

def data_dir() -> str:
    """Where to find local data files"""
    return str(pathlib.Path('data').absolute())

def config_dir():
    """The directory where persistent config files are stored"""
    return pathlib.Path('./app/singer/config')

def state_dir():
    """The directory where state files are stored"""
    return pathlib.Path('./app/singer/state')

def catalog_dir():
    """The directory where state files are stored"""
    return pathlib.Path('./app/singer/catalog')

import os
import json

class SingerConfig:
    def __init__(self, command_name: str) -> None:
        """Config of a tap or target command"""
        self.command_name = command_name

        # cache for loaded
        self._config = None

    def config_file_path(self) -> pathlib.Path:
        return pathlib.Path(config_dir()) / f'{self.command_name}.json'

    def _load_config(self):
        if not self._config:
            file_path = self.config_file_path()
            if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                with open(file_path,'r') as config_file:
                    self._config = json.load(config_file)
            else:
                self._config = {} # no config file exists -> create an empty config

    def __getitem__(self, key):
        if not self._config:
            self._load_config()

        return self._config[key]

    def __setitem__(self, key, item):
        if not self._config:
            self._load_config()

        self._config[key] = item

    def get(self, k, default=None):
        if not self._config:
            self._load_config()

        if default:
            return self._config.get(k, default)
        else:
            return self._config.get(k)

    def save(self):
        """Saves the changes of a config file"""

        if not self._config:
            return # nothing loaded --> nothing changed --> no need to save

        with open(self.config_file_path(),'w') as config_file:
            json.dump(self._config, config_file)
