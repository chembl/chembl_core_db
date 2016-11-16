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

setup(
    name='chembl_core_db',
    version='0.8.3',
    author='Michal Nowotka',
    author_email='mnowotka@ebi.ac.uk',
    description='Core ChEMBL python library',
    url='https://www.ebi.ac.uk/chembl/',
    license='Apache Software License',
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
    classifiers=['Development Status :: 4 - Beta',
                 'Environment :: Web Environment',
                 'Framework :: Django',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: Apache Software License',
                 'Operating System :: POSIX :: Linux',
                 'Programming Language :: Python :: 2.7',
                 'Topic :: Scientific/Engineering :: Chemistry'],
    zip_safe=False,
)
