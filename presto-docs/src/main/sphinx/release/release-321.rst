===========
Release 321
===========

.. warning:: The server RPM is broken in this release.

General Changes
---------------

* Fix incorrect result of :func:`round` when applied to a ``tinyint``, ``smallint``,
  ``integer``, or ``bigint`` type with negative decimal places. (:issue:`42`)
* Improve performance of queries with ``LIMIT`` over ``information_schema`` tables. (:issue:`1543`)
* Improve performance for broadcast joins by using dynamic filtering. This can be enabled
  via the ``experimental.enable-dynamic-filtering`` configuration option or the
  ``enable_dynamic_filtering`` session property. (:issue:`1686`)

Security Changes
----------------

* Improve the security of query results with one-time tokens. (:issue:`1654`)

Hive Connector Changes
----------------------

* Fix reading ``TEXT`` file collection delimiter set by Hive versions earlier
  than 3.0. (:issue:`1714`)
* Fix a regression that prevented Presto from using the AWS Glue metastore. (:issue:`1698`)
* Allow skipping header or footer lines for ``CSV`` format tables via the
  ``skip_header_line_count`` and ``skip_footer_line_count`` table properties. (:issue:`1090`)
* Rename table property ``textfile_skip_header_line_count`` to ``skip_header_line_count``
  and ``textfile_skip_footer_line_count`` to ``skip_footer_line_count``. (:issue:`1090`)
* Add support for LZOP compressed (``.lzo``) files. Previously, queries accessing LZOP compressed
  files would fail, unless all files were small. (:issue:`1701`)
* Allow caching directory listings for all tables or schemas. (:issue:`1668`)
* Add support for dynamic filtering for broadcast joins. (:issue:`1686`)

PostgreSQL Connector Changes
----------------------------

* Support reading PostgreSQL arrays as the ``JSON`` data type. This can be enabled by
  setting the ``postgresql.experimental.array-mapping`` configuration property or the
  ``array_mapping`` catalog session property to ``AS_JSON``. (:issue:`682`)

Elasticsearch Connector Changes
-------------------------------

* Add support for Amazon Elasticsearch Service. (:issue:`1693`)

Cassandra Connector Changes
---------------------------

* Add TLS support. (:issue:`1680`)

JMX Connector Changes
---------------------

* Add support for wildcards in configuration of history tables. (:issue:`1572`)

SPI Changes
-----------

* Fix ``QueryStatistics.getWallTime()`` to report elapsed time rather than total
  scheduled time. (:issue:`1719`)
