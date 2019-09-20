===========
Release 319
===========

General Changes
---------------
* Fix planning failure for queries involving ``UNION`` and ``DISTINCT`` aggregates. (:issue:`1510`)
* Fix excessive runtime when parsing expressions involving ``CASE``. (:issue:`1407`)
* Fix fragment's output size in the output of ``EXPLAIN ANALYZE``. (:issue:`1345`)
* Improve performance of queries against ``information_schema`` tables. (:issue:`1329`, :issue:`1308`)
* Eliminate cross joins in some queries with coercions and complex expressions
  on join conditions. (:issue:`1390`)
* Fix a rare failure when running ``EXPLAIN ANALYZE`` on a query containing
  window functions. (:issue:`1401`)
* Include cost estimates in output of ``EXPLAIN (TYPE IO)``. (:issue:`806`)
* Improve coercion handling for correlated subqueries. (:issue:`1453`)
* Improve support for correlated subqueries involving ``ORDER BY`` or ``LIMIT``. (:issue:`1415`)
* Add support for projections to joins reordering. (:issue:`1431`)
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

Security Changes
----------------
* Allow configuring read-only access in :doc:`/security/built-in-system-access-control`. (:issue:`1153`)

Hive Connector Changes
----------------------

* Fix reading from ``information_schema``, as well as ``SHOW SCHEMAS``, ``SHOW TABLES``, ``SHOW COLUMNS``
  when connecting to Hive 3 Metastore with ``information_schema`` schema created. (:issue:`1192`)
* Add support for custom S3 filesystems. (:issue:`1397`)
* Add support for instance and custom credentials provider for Glue. (:issue:`1363`)
* Allow to to specify only ``hive.metastore-cache-ttl`` when enabling Hive Metastore caching (without setting
  ``hive.metastore-refresh-interval``, which is disabled by default). (:issue:`1473`)
* Add ``textfile_field_separator`` and ``textfile_field_separator_escape`` Hive table properties to support
  custom field separator for Hive ``format=TEXTFILE`` tables. (:issue:`1439`)
* Add ``$file_size`` and ``$file_modified_time`` hidden columns. (:issue:`1428`)
