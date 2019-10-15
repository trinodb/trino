===========
Release 318
===========

General Changes
---------------

* Fix query failure when using ``DISTINCT FROM`` with the ``UUID`` or
  ``IPADDRESS`` types. (:issue:`1180`)
* Improve query performance when ``optimize_hash_generation`` is enabled. (:issue:`1071`)
* Improve performance of information schema tables. (:issue:`999`, :issue:`1306`)
* Rename ``http.server.authentication.*`` configuration options to ``http-server.authentication.*``. (:issue:`1270`)
* Change query CPU tracking for resource groups to update periodically while
  the query is running. Previously, CPU usage would only update at query
  completion. This improves resource management fairness when using
  CPU-limited resource groups. (:issue:`1128`)
* Remove ``distributed_planning_time_ms`` column from ``system.runtime.queries``. (:issue:`1084`)
* Add support for ``Asia/Qostanay`` time zone. (:issue:`1221`)
* Add session properties that allow overriding the query per-node memory limits:
  ``query_max_memory_per_node`` and ``query_max_total_memory_per_node``. These properties
  can be used to decrease limits for a query, but not to increase them. (:issue:`1212`)
* Add :doc:`/connector/googlesheets`. (:issue:`1030`)
* Add ``planning_time_ms`` column to the ``system.runtime.queries`` table that shows
  the time spent on query planning. This is the same value that used to be in the
  ``analysis_time_ms`` column, which was a misnomer. (:issue:`1084`)
* Add :func:`last_day_of_month` function. (:issue:`1295`)
* Add support for cancelling queries via the ``system.runtime.kill_query`` procedure when
  they are in the queue or in the semantic analysis stage. (:issue:`1079`)
* Add queries that are in the queue or in the semantic analysis stage to the
  ``system.runtime.queries`` table. (:issue:`1079`)

Web UI Changes
--------------

* Display information about queries that are in the queue or in the semantic analysis
  stage. (:issue:`1079`)
* Add support for cancelling queries that are in the queue or in the semantic analysis
  stage. (:issue:`1079`)

Hive Connector Changes
----------------------

* Fix query failure due to missing credentials while writing empty bucket files. (:issue:`1298`)
* Fix bucketing of ``NaN`` values of ``real`` type. Previously ``NaN`` values
  could be assigned a wrong bucket. (:issue:`1336`)
* Fix reading ``RCFile`` collection delimiter set by Hive version earlier than 3.0. (:issue:`1321`)
* Return proper error when selecting ``"$bucket"`` column from a table using
  Hive bucketing v2. (:issue:`1336`)
* Improve performance of S3 object listing. (:issue:`1232`)
* Improve performance when reading data from GCS. (:issue:`1200`)
* Add support for reading data from S3 Requester Pays buckets. This can be enabled
  using the ``hive.s3.requester-pays.enabled`` configuration property. (:issue:`1241`)
* Allow inserting into bucketed, unpartitioned tables. (:issue:`1127`)
* Allow inserting into existing partitions of bucketed, partitioned tables. (:issue:`1347`)

PostgreSQL Connector Changes
----------------------------

* Add support for providing JDBC credential in a separate file. This can be enabled by
  setting the ``credential-provider.type=FILE`` and ``connection-credential-file``
  config options in the catalog properties file. (:issue:`1124`)
* Allow logging all calls to ``JdbcClient``. This can be enabled by turning
  on ``DEBUG`` logging for ``io.prestosql.plugin.jdbc.JdbcClient``. (:issue:`1274`)
* Add possibility to force mapping of certain types to ``varchar``. This can be enabled
  by setting ``jdbc-types-mapped-to-varchar`` to comma-separated list of type names. (:issue:`186`)
* Add support for PostgreSQL ``timestamp[]`` type. (:issue:`1023`, :issue:`1262`, :issue:`1328`)

MySQL Connector Changes
-----------------------

* Add support for providing JDBC credential in a separate file. This can be enabled by
  setting the ``credential-provider.type=FILE`` and ``connection-credential-file``
  config options in the catalog properties file. (:issue:`1124`)
* Allow logging all calls to ``JdbcClient``. This can be enabled by turning
  on ``DEBUG`` logging for ``io.prestosql.plugin.jdbc.JdbcClient``. (:issue:`1274`)
* Add possibility to force mapping of certain types to ``varchar``. This can be enabled
  by setting ``jdbc-types-mapped-to-varchar`` to comma-separated list of type names. (:issue:`186`)

Redshift Connector Changes
--------------------------

* Add support for providing JDBC credential in a separate file. This can be enabled by
  setting the ``credential-provider.type=FILE`` and ``connection-credential-file``
  config options in the catalog properties file. (:issue:`1124`)
* Allow logging all calls to ``JdbcClient``. This can be enabled by turning
  on ``DEBUG`` logging for ``io.prestosql.plugin.jdbc.JdbcClient``. (:issue:`1274`)
* Add possibility to force mapping of certain types to ``varchar``. This can be enabled
  by setting ``jdbc-types-mapped-to-varchar`` to comma-separated list of type names. (:issue:`186`)

SQL Server Connector Changes
----------------------------

* Add support for providing JDBC credential in a separate file. This can be enabled by
  setting the ``credential-provider.type=FILE`` and ``connection-credential-file``
  config options in the catalog properties file. (:issue:`1124`)
* Allow logging all calls to ``JdbcClient``. This can be enabled by turning
  on ``DEBUG`` logging for ``io.prestosql.plugin.jdbc.JdbcClient``. (:issue:`1274`)
* Add possibility to force mapping of certain types to ``varchar``. This can be enabled
  by setting ``jdbc-types-mapped-to-varchar`` to comma-separated list of type names. (:issue:`186`)

SPI Changes
-----------

* Add ``Block.isLoaded()`` method. (:issue:`1216`)
* Update security APIs to accept the new ``ConnectorSecurityContext``
  and ``SystemSecurityContext`` classes. (:issue:`171`)
* Allow connectors to override minimal schedule split batch size. (:issue:`1251`)
