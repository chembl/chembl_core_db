chembl_core_db
======

.. image:: https://img.shields.io/pypi/v/chembl_core_db.svg
    :target: https://pypi.python.org/pypi/chembl_core_db/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/dm/chembl_core_db.svg
    :target: https://pypi.python.org/pypi/chembl_core_db/
    :alt: Downloads

.. image:: https://img.shields.io/pypi/pyversions/chembl_core_db.svg
    :target: https://pypi.python.org/pypi/chembl_core_db/
    :alt: Supported Python versions

.. image:: https://img.shields.io/pypi/status/chembl_core_db.svg
    :target: https://pypi.python.org/pypi/chembl_core_db/
    :alt: Development Status

.. image:: https://img.shields.io/pypi/l/chembl_core_db.svg
    :target: https://pypi.python.org/pypi/chembl_core_db/
    :alt: License

.. image:: https://badge.waffle.io/chembl/chembl_core_db.png?label=ready&title=Ready 
 :target: https://waffle.io/chembl/chembl_core_db
 :alt: 'Stories in Ready'    

This is chembl_core_db package developed at Chembl group, EMBL-EBI, Cambridge, UK.

It's a core library providing custom fields intended to use with ChEMBL database, such as BlobField (for storing binary data - not supported by django until 1.6), or ChemblIntegerField, which create database constraint for default value (not supported by standard django IntegerField).
Using these fields will create a database that is maximally similar to the original oracle CheMBL database, makes interacting with existing database easier and provides database agnostic abstraction layer for creating and using ChEMBL db and it's data on all popular database engines.

It also provides custom query managers to perform strictly chemical operations, such as similarity and substructure search in database independent manner.
(No miracles here - it still requires certain db cartridge being installed in target database).

For Oracle, the package provides custom backend, which solves some bugs and arbitrary design decisions made in standard django Oracle backend.
It still requires cx_oracle as db driver and original Oracle backend as it's base.
Most of the custom fields defined by the package can be well used with standard Oracle backend but it's still recommended to use the new one.

Additionally the package contains some utilities, such as validators for some popular chemical formats (smiles, uniprot, refeq etc.), used by dependent packages.
