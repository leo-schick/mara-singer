import os
import pathlib
import json

import singer.bookmarks

from . import config

class SingerTapState:
    def __init__(self, tap_name: str) -> None:
        """State for a singer tap"""
        self.tap_name = tap_name

        # cache for loaded
        self._state = None

    def state_file_path(self) -> pathlib.Path:
        return pathlib.Path(config.state_dir()) / f'{self.tap_name}.json'

    def _load_state(self):
        if not self._state:
            if os.path.isfile(self.state_file_path()):
                with open(self.state_file_path(),'r') as state_file:
                    self._state = json.load(state_file)
            else:
                self._state = {} # no config file exists -> create an empty config

    def save(self):
        """Saves the changes of a state file"""

        if not self._state: 
            return # nothing loaded --> nothing changed --> no need to save

        with open(self.state_file_path(),'w') as state_file:
            json.dump(self._state, state_file)

    def get_bookmark(self, tap_stream_id, key, default=None):
        if not self._state:
            self._load_state()

        return singer.bookmarks.get_bookmark(self._state, tap_stream_id, key, default=default)
