import pathlib

def data_dir() -> str:
    """Where to find local data files"""
    return str(pathlib.Path('data').absolute())

def config_dir():
    """The directory where persistent config files are stored"""
    return pathlib.Path('./data/singer/config')

def state_dir():
    """The directory where state files are stored"""
    return pathlib.Path('./data/singer/state')

def catalog_dir():
    """The directory where state files are stored"""
    return pathlib.Path('./data/singer/catalog')
