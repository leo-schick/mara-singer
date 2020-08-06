import pytest

from mara_app.monkey_patch import patch

from mara_singer.state import SingerTapState
from mara_singer import config

def test_state_read_not_existing_file():
    patch(config.state_dir)(lambda: './tests/')
    state = SingerTapState(tap_name='does_not_exist-state')
    bk_value = state.get_bookmark(tap_stream_id='STREAM_NAME', key='date')
    assert bk_value == None

def test_state_read_empty_file():
    patch(config.state_dir)(lambda: './tests/')
    state = SingerTapState(tap_name='empty-state')
    bk_value = state.get_bookmark(tap_stream_id='STREAM_NAME', key='date')
    assert bk_value == None

def test_state_read_sample_state_file():
    patch(config.state_dir)(lambda: './tests/')
    state = SingerTapState(tap_name='sample-state1')
    bk_value = state.get_bookmark(tap_stream_id='STREAM_NAME', key='date')
    assert bk_value == '2020-01-01T00:00:00.000000Z'


if __name__ == '__main__':
    test_state_read_not_existing_file()
    test_state_read_empty_file()
    test_state_read_sample_state_file()
    print("Done.")
