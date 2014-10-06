#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'mnowotka'

import sys

try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

if sys.version_info < (2, 7, 3) or sys.version_info >= (3, 0, 0):
    raise Exception('ChEMBL software stack requires python 2.7.3 - 3.0.0')

setup(
    name='chembl_core_db',
    version='0.5.10',
    author='Michal Nowotka',
    author_email='mnowotka@ebi.ac.uk',
    description='Core ChEMBL python library',
    url='https://www.ebi.ac.uk/chembl/',
    license='CC BY-SA 3.0',
    packages=['chembl_core_db',
              'chembl_core_db.cache',
              'chembl_core_db.cache.backends',
              'chembl_core_db.db',
              'chembl_core_db.db.backends',
              'chembl_core_db.db.backends.oracleChEmbl',
              'chembl_core_db.db.models',
              'chembl_core_db.testing'],
    long_description=open('README.rst').read(),
    install_requires=['Django==1.5.5'],
    include_package_data=False,
    classifiers=['Development Status :: 2 - Pre-Alpha',
                 'Environment :: Web Environment',
                 'Framework :: Django',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: MIT License',
                 'Operating System :: POSIX :: Linux',
                 'Programming Language :: Python :: 2.7',
                 'Topic :: Scientific/Engineering :: Chemistry'],
    zip_safe=False,
)
