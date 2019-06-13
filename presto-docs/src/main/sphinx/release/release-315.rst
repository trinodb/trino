===========
Release 315
===========

General Changes
---------------

* Fix incorrect results when dividing certain decimal numbers. (:issue:`958`)
* Add support for ``FETCH FIRST ... WITH TIES`` syntax. (:issue:`832`)
* Add locality awareness to default split scheduler. (:issue:`680`)
* Add :func:`format` function. (:issue:`548`)

Server RPM Changes
------------------

* Require JDK version 8u161+ during installation, which is the version the server requires. (:issue:`983`)

CLI Changes
-----------

* Fix alignment of nulls for numeric columns in aligned output format. (:issue:`871`)

Hive Connector Changes
----------------------

* Correctly identify EMRFS as S3 when deciding to use a temporary location for writes. (:issue:`935`)
* Add support for UTF-8 ORC bloom filters. (:issue:`914`)
* Disable usage of old, non UTF-8, ORC bloom filters for ``VARCHAR`` and ``CHAR``. (:issue:`914`)
* Allow logging all calls to Hive thrift metastore service. This can be enabled
  by turning on DEBUG logging for ``io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient`` (:issue:`946`)
* Add support for ``DATE``, ``TIMESTAMP`` and ``REAL`` in ORC bloom filters. (:issue:`967`)
* Allow creating external tables pointing at S3 even if the location does not exist. (:issue:`935`)

MongoDB Connector Changes
-------------------------

* Fix query failure when  ``row`` with a ``ObjectId`` field is used as a join key. (:issue:`933`)
* Add cast from ``ObjectId`` to ``varchar``. (:issue:`933`)

SPI Changes
-----------

* Allow connectors to provide view definitions. ``ConnectorViewDefinition`` now contains
  the real view definition rather than an opaque blob. Connectors that support view storage
  can use the JSON representation of that class as a stable storage format. The JSON
  representation is the same as the previous opaque blob, thus all existing view
  definitions will continue to work. (:issue:`976`)
* Add ``getView()`` method to ``ConnectorMetadata`` as a replacement for ``getViews()``.
  The ``getViews()`` method now exists only as an optional method for connectors that
  can efficiently support bulk retrieval of views and has a different signature. (:issue:`976`)

.. note::

    These are backwards incompatible changes with the previous SPI.
    If you have written a connector that supports views, you will
    need to update your code before deploying this release.
