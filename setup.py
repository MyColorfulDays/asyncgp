#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: setup.py
Author: Me
Email: yourname@email.com
Github: https://github.com/yourname
Description: setup.py
"""

from setuptools import setup

setup(
    name='asyncgp',
    version='0.2',
    description=(
        'An asyncio Greenplum driver'
    ),
    long_description=open('README.md').read(),
    classifiers=[
        'Development Status :: 1 - Planning',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database :: Front-Ends',
    ],
    platforms=['macOS', 'POSIX', 'Windows'],
    python_requires='>=3.5.0',
    author='MyColorfulDays',
    author_email='my_colorful_days@163.com',
    url='https://github.com/MyColorfulDays/asyncgp.git',
    license='Apache License, Version 2.0',
    install_requires=[
        'asyncpg==0.18.3',
    ]
)
