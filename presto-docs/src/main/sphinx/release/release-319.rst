===========
Release 319
===========

General Changes
---------------
* Fix planning failure for queries involving ``UNION`` and ``DISTINCT`` aggregates. (:issue:`1510`)
* Fix excessive runtime when parsing expressions involving ``CASE``. (:issue:`1407`)
* Fix fragment output size ``EXPLAIN ANALYZE`` output. (:issue:`1345`)
* Fix a rare failure when running ``EXPLAIN ANALYZE`` on a query containing
  window functions. (:issue:`1401`)
* Fix failure when querying ``/v1/resourceGroupState`` endpoint for non-existing resource
  group. (:issue:`1368`)
* Fix incorrect results when reading ``information_schema.table_privileges`` with
  an equality predicate on ``table_name`` but without a predicate on ``table_schema``.
  (:issue:`1534`)
* Improve performance of queries against ``information_schema`` tables. (:issue:`1329`, :issue:`1308`)
* Improve performance of certain queries involving coercions and complex expressions in ``JOIN``
  conditions. (:issue:`1390`)
* Include cost estimates in output of ``EXPLAIN (TYPE IO)``. (:issue:`806`)
* Improve coercion handling for correlated subqueries. (:issue:`1453`)
* Improve support for correlated subqueries involving ``ORDER BY`` or ``LIMIT``. (:issue:`1415`)
* Improve performance of certain ``JOIN`` queries when automatic join ordering is enabled. (:issue:`1431`)
* Allow setting the default session catalog and schema via the ``sql.default-catalog``
  and ``sql.default-schema`` configuration options. (:issue:`1524`)
* Add support for ``INNER`` and ``OUTER`` joins involving ``UNNEST``. (:issue:`1522`)
* Rename ``legacy`` and ``flat`` [scheduler policies](link to properties node scheduler section) to
  ``uniform`` and ``topology`` respectively.  These can be configured via the ``node-scheduler.policy``
  property. (:issue:`10491`)
* Add ``file`` [network topology provider](link to properties node scheduler section) which can be
  configured via the ``node-scheduler.network-topology.type`` property. (:issue:`1500`)
* Add optional support for authorization over HTTP for forwarded requests containing the
  ``X-Forwarded-Proto`` header. (:issue:`1442`)
* Add support for ``SphericalGeography`` to :func:`ST_Length`. (:issue:`1551`)


Security Changes
----------------
* Allow configuring read-only access in :doc:`/security/built-in-system-access-control`. (:issue:`1153`)
* Add missing checks for schema adding, renaming and dropping in file-based ``SystemAccessControl``. (:issue:`1153`)

Web UI Changes
--------------
* Fix rendering bug in Query Timeline resulting in inconsistency of presented information after
  query finishes. (:issue:`1371`)
* Show total memory in Query Timeline instead of user memory. (:issue:`1371`)

CLI Changes
-----------
* Add ``--insecure`` option to skip validation of server certificates for debugging. (:issue:`1484`)

Hive Connector Changes
----------------------
* Fix reading from ``information_schema``, as well as ``SHOW SCHEMAS``, ``SHOW TABLES``, ``SHOW COLUMNS``
  when connecting to Hive 3 Metastore with ``information_schema`` schema created. (:issue:`1192`)
* Improve performance when reading data from GCS. (:issue:`1443`)
* Allow accessing tables in Glue metastore that do not have a table type. (:issue:`1343`)
* Add support for Azure Data Lake (``adl``) file system. (:issue:`1499`)
* Add support for custom S3 filesystems. (:issue:`1397`)
* Add support for instance and custom credentials provider for Glue. (:issue:`1363`)
* Allow to specify only ``hive.metastore-cache-ttl`` when enabling Hive Metastore caching (without setting
  ``hive.metastore-refresh-interval``, which is disabled by default). (:issue:`1473`)
* Add ``textfile_field_separator`` and ``textfile_field_separator_escape`` Hive table properties to support
  custom field separator for Hive ``format=TEXTFILE`` tables. (:issue:`1439`)
* Add ``$file_size`` and ``$file_modified_time`` hidden columns. (:issue:`1428`)
* ``hive.metastore-timeout`` property is now accepted only when using ``thrift`` metastore.
  Previously it was accepted for other metastore types too, but was ignored. (:issue:`1346`)
* Prevent read from transactional tables. Previously Presto would attempt to read, but
  would not find data. (:issue:`1218`)
* Prevent writes to transactional tables. Previously Presto would attempt to write, but
  would write data incorrectly. (:issue:`1218`)
