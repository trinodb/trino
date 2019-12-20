===========
Release 301
===========

General Changes
---------------

* Fix reporting of aggregate input data size stats. (:issue:`100`)
* Add support for role management (see :doc:`/sql/create-role`).  Note, using :doc:`/sql/set-role`
  requires an up-to-date client library. (:issue:`90`)
* Add ``INVOKER`` security mode for :doc:`/sql/create-view`. (:issue:`30`)
* Add ``ANALYZE`` SQL statement for collecting table statistics. (:issue:`99`)
* Add :func:`log` function with arbitrary base. (:issue:`36`)
* Remove the ``deprecated.legacy-log-function`` configuration option. The legacy behavior
  (reverse argument order) for the :func:`log` function is no longer available. (:issue:`36`)
* Remove the ``deprecated.legacy-array-agg`` configuration option. The legacy behavior
  (ignoring nulls) for :func:`array_agg` is no longer available. (:issue:`77`)
* Improve performance of ``COALESCE`` expressions. (:issue:`35`)
* Improve error message for unsupported :func:`reduce_agg` state type. (:issue:`55`)
* Improve performance of queries involving ``SYSTEM`` table sampling and computations over the
  columns of the sampled table. (:issue:`29`)

Server RPM Changes
------------------

* Do not allow uninstalling RPM while server is still running. (:issue:`67`)

Security Changes
----------------

* Support LDAP with anonymous bind disabled. (:issue:`97`)

Hive Connector Changes
----------------------

* Add procedure for dumping metastore recording to a file. (:issue:`54`)
* Add Metastore recorder support for Glue. (:issue:`61`)
* Add ``hive.temporary-staging-directory-enabled`` configuration property and
  ``temporary_staging_directory_enabled`` session property to control whether a temporary staging
  directory should be used for write operations. (:issue:`70`)
* Add ``hive.temporary-staging-directory-path`` configuration property and
  ``temporary_staging_directory_path`` session property to control the location of temporary
  staging directory that is used for write operations. The ``${USER}`` placeholder can be used to
  use a different location for each user (e.g., ``/tmp/${USER}``). (:issue:`70`)

Kafka Connector Changes
-----------------------

* The minimum supported Kafka broker version is now 0.10.0. (:issue:`53`)

Base-JDBC Connector Library Changes
-----------------------------------

* Add support for defining procedures. (:issue:`73`)
* Add support for providing table statistics. (:issue:`72`)

SPI Changes
-----------

* Include session trace token in ``QueryCreatedEvent`` and ``QueryCompletedEvent``. (:issue:`24`)
* Fix regression in ``NodeManager`` where node list was not being refreshed on workers.  (:issue:`27`)
