===========
Release 309
===========

General Changes
---------------

* Fix incorrect match result for :doc:`/functions/regexp` when pattern ends
  with a word boundary matcher. This only affects the default ``JONI`` library.
  (:issue:`661`)
* Fix failures for queries involving spatial joins. (:issue:`652`)
* Add support for ``SphericalGeography`` to :func:`ST_Area()`. (:issue:`383`)

Security Changes
----------------

* Add option for specifying the Kerberos GSS name type. (:issue:`645`)

Server RPM Changes
------------------

* Update default JVM configuration to recommended settings (see :doc:`/installation/deployment`).
  (:issue:`642`)

Hive Connector Changes
----------------------

* Fix rare failure when reading ``DECIMAL`` values from ORC files. (:issue:`664`)
* Add a hidden ``$properties`` table for each table that describes its Hive table
  properties. For example, a table named ``example`` will have an associated
  properties table named ``example$properties``. (:issue:`268`)

MySQL Connector Changes
-----------------------

* Match schema and table names case insensitively. This behavior can be enabled by setting
  the ``case-insensitive-name-matching`` catalog configuration option to true. (:issue:`614`)

PostgreSQL Connector Changes
----------------------------

* Add support for ``ARRAY`` type. (:issue:`317`)
* Add support writing ``TINYINT`` values. (:issue:`317`)
* Match schema and table names case insensitively. This behavior can be enabled by setting
  the ``case-insensitive-name-matching`` catalog configuration option to true. (:issue:`614`)


Redshift Connector Changes
--------------------------

* Match schema and table names case insensitively. This behavior can be enabled by setting
  the ``case-insensitive-name-matching`` catalog configuration option to true. (:issue:`614`)


SQL Server Connector Changes
----------------------------

* Match schema and table names case insensitively. This behavior can be enabled by setting
  the ``case-insensitive-name-matching`` catalog configuration option to true. (:issue:`614`)

Cassandra Connector Changes
---------------------------

* Allow reading from tables which have Cassandra column types that are not supported by Presto.
  These columns will not be visible in Presto. (:issue:`592`)

SPI Changes
-----------

* Add session parameter to the ``applyFilter()`` and ``applyLimit()`` methods in
  ``ConnectorMetadata``. (:issue:`636`)

.. note::

    This is a backwards incompatible changes with the previous SPI.
    If you have written a connector that implements these methods,
    you will need to update your code before deploying this release.
