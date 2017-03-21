#!/usr/bin/env python
import os
from rdbtools import __version__

long_description = '''
Parse Redis dump.rdb files, Analyze Memory, and Export Data to JSON

Rdbtools is a parser for Redis' dump.rdb files. The parser generates events similar to an xml sax parser, and is very efficient memory wise.

In addition, rdbtools provides utilities to :

 1. Generate a Memory Report of your data across all databases and keys
 2. Convert dump files to JSON
 3. Compare two dump files using standard diff tools

Rdbtools is written in Python, though there are similar projects in other languages. See FAQs (https://github.com/sripathikrishnan/redis-rdb-tools/wiki/FAQs) for more information.
'''


sdict = {
    'name' : 'rdbtools',
    'version' : __version__,
    'description' : 'Utilities to convert Redis RDB files to JSON or SQL formats',
    'long_description' : long_description,
    'url': 'https://github.com/sripathikrishnan/redis-rdb-tools',
    'download_url': 'https://github.com/sripathikrishnan/redis-rdb-tools/archive/rdbtools-%s.tar.gz' % __version__,
    'author': 'Sripathi Krishnan, Redis Labs',
    'author_email' : 'Sripathi.Krishnan@gmail.com',
    'maintainer': 'Sripathi Krishnan, Redis Labs',
    'maintainer_email': 'oss@redislabs.com',
    'keywords' : ['Redis', 'RDB', 'Export', 'Dump', 'Memory Profiler'],
    'license' : 'MIT',
    'packages' : ['rdbtools', 'rdbtools.cli'],
    'package_data' : {
        'rdbtools': ['templates/*'],
    },
    'test_suite' : 'tests.all_tests',
    'install_requires': ['redis'],
    'entry_points' : {
        'console_scripts' : [
            'rdb = rdbtools.cli.rdb:main',
            'redis-memory-for-key = rdbtools.cli.redis_memory_for_key:main',
            'redis-profiler = rdbtools.cli.redis_profiler:main'],
    },
    'classifiers' : [
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'],
}

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(**sdict)

