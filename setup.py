#!/usr/bin/env python
import os
from rdbtools import __version__

f = open(os.path.join(os.path.dirname(__file__), 'README.md'))
long_description = f.read()
f.close()

sdict = {
    'name' : 'rdbtools',
    'version' : __version__,
    'description' : 'Utilities to convert Redis RDB files to JSON or SQL formats',
    'long_description' : long_description,
    'url': 'https://github.com/sripathikrishnan/redis-rdb-tools',
    'download_url' : 'http://cloud.github.com/downloads/andymccurdy/redis-py/redis-%s.tar.gz' % __version__,
    'author' : 'Sripathi Krishnan',
    'author_email' : 'Sripathi.Krishnan@gmail.com',
    'maintainer' : 'Sripathi Krishnan',
    'maintainer_email' : 'Sripathi.Krishnan@gmail.com',
    'keywords' : ['Redis', 'RDB', 'Export', 'Dump'],
    'license' : 'MIT',
    'packages' : ['rdbtools', 'rdbtools.cli'],
    'package_data' : {'rdbtools.cli': ['*.template']},
    'test_suite' : 'tests.all_tests',
    'entry_points' : {
        'console_scripts' : [
            'rdb = rdbtools.cli.rdb:main',
            'redis-profiler = rdbtools.cli.redis_profiler:main'],
    },
    'classifiers' : [
        'Development Status :: 2 - Development/Experimental',
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

