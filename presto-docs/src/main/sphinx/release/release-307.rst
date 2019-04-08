===========
Release 307
===========

General Changes
---------------

* Fix cleanup of spill files for queries using window functions or ``ORDER BY``. (:issue:`543`)
* Optimize queries containing ``ORDER BY`` together with ``LIMIT`` over an ``OUTER JOIN``
  by pushing ``ORDER BY`` and ``LIMIT`` to the outer side of the join. (:issue:`419`)
* Improve performance of table scans for data sources that produce tiny pages. (:issue:`467`)
* Improve performance of ``IN`` subquery expressions that contain a ``DISTINCT`` clause. (:issue:`551`)
* Expand support of types handled in ``EXPLAIN (TYPE IO)``. (:issue:`509`)
* Add support for outer joins involving lateral derived tables (i.e., ``LATERAL``). (:issue:`390`)
* Add support for setting table comments via the :doc:`/sql/comment` syntax. (:issue:`200`)

Web UI Changes
--------------

* Allow UI to work when opened as ``/ui`` (no trailing slash). (:issue:`500`)

Security Changes
----------------

* Make query result and cancellation URIs secure. Previously, an authenticated
  user could potentially steal the result data of any running query.

Server RPM Changes
------------------

* Prevent JVM from allocating large amounts of native memory. The new configuration is applied
  automatically when Presto is installed from RPM. When Presto is installed another way, or when
  you provide your own ``jvm.config``, we recommend adding ``-Djdk.nio.maxCachedBufferSize=2000000``
  to your ``jvm.config``. See :doc:`/installation/deployment` for details. (:issue:`542`)

CLI Changes
-----------

* Always abort query in batch mode when CLI is killed. (:issue:`508`, :issue:`580`)

JDBC Driver Changes
-------------------

* Abort query synchronously when the ``ResultSet`` is closed or when the
  ``Statement`` is cancelled. Previously, the abort was sent in the background,
  allowing the JVM to exit before the abort was received by the server. (:issue:`580`)

Hive Connector Changes
----------------------

* Add safety checks for Hive bucketing version. Hive 3.0 introduced a new
  bucketing version that uses an incompatible hash function. The Hive connector
  will treat such tables as not bucketed when reading and disallows writing. (:issue:`512`)
* Add support for setting table comments via the :doc:`/sql/comment` syntax. (:issue:`200`)

MySQL Connector Changes
-----------------------

See `Base-JDBC Connector Library Changes <#base-jdbc-connector-library-changes>`__.

PostgreSQL Connector Changes
----------------------------

See `Base-JDBC Connector Library Changes <#base-jdbc-connector-library-changes>`__.

Redshift Connector Changes
--------------------------

See `Base-JDBC Connector Library Changes <#base-jdbc-connector-library-changes>`__.

SQL Server Connector Changes
----------------------------

See `Base-JDBC Connector Library Changes <#base-jdbc-connector-library-changes>`__.

Base-JDBC Connector Library Changes
-----------------------------------

* Fix reading and writing of ``timestamp`` values. Previously, an incorrect value
  could be read, depending on the Presto JVM time zone. (:issue:`495`)
* Add support for using a client-provided username and password. The credential
  names can be configured using the ``user-credential-name`` and ``password-credential-name``
  configuration properties. (:issue:`482`)

SPI Changes
-----------

* ``LongDecimalType`` and ``IpAddressType`` now use ``Int128ArrayBlock`` instead
  of ``FixedWithBlock``. Any code that creates blocks directly, rather than using
  the ``BlockBuilder`` returned from the ``Type``, will need to be updated. (:issue:`492`)
* Remove ``FixedWidthBlock``. Use one of the ``*ArrayBlock`` classes instead. (:issue:`492`)
* Add support for simple constraint pushdown into connectors via the
  ``ConnectorMetadata.applyFilter()`` method. (:issue:`541`)
