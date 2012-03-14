from rdbtools.parser import RdbCallback, RdbParser

__version__ = '0.1.1'
VERSION = tuple(map(int, __version__.split('.')))

__all__ = [
    'RdbParser', 'RdbCallback']

