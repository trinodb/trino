==============
Hudi connector
==============

.. raw:: html

  <img src="../_static/img/hudi.png" class="connector-logo">

The Hudi connector enables querying `Hudi <https://hudi.apache.org/docs/overview/>`_ tables.

Requirements
------------

To use the Hudi connector, you need:

* Hudi version 0.12.2 or higher.
* Network access from the Trino coordinator and workers to the Hudi storage.
* Access to the Hive metastore service (HMS).
* Network access from the Trino coordinator to the HMS.

Configuration
-------------

The connector requires a Hive metastore for table metadata and supports the same
metastore configuration properties as the :doc:`Hive connector
</connector/hive>`. At a minimum, ``hive.metastore.uri`` must be configured.
The connector recognizes Hudi tables synced to the metastore by the
`Hudi sync tool <https://hudi.apache.org/docs/syncing_metastore>`_.

To create a catalog that uses the Hudi connector, create a catalog properties
file ``etc/catalog/example.properties`` that references the ``hudi`` connector.
Update the ``hive.metastore.uri`` with the URI of your Hive metastore Thrift
service:

.. code-block:: properties

    connector.name=hudi
    hive.metastore.uri=thrift://example.net:9083

Additionally, following configuration properties can be set depending on the use-case.

.. list-table:: Hudi configuration properties
    :widths: 30, 55, 15
    :header-rows: 1

    * - Property name
      - Description
      - Default
    * - ``hudi.metadata-enabled``
      - Fetch the list of file names and sizes from metadata rather than storage.
      - ``false``
    * - ``hudi.columns-to-hide``
      - List of column names that are hidden from the query output.
        It can be used to hide Hudi meta fields. By default, no fields are hidden.
      -
    * - ``hudi.parquet.use-column-names``
      - Access Parquet columns using names from the file. If disabled, then columns
        are accessed using the index. Only applicable to Parquet file format.
      - ``true``
    * - ``parquet.optimized-reader.enabled``
      - Whether batched column readers should be used when reading Parquet files
        for improved performance. Set this property to ``false`` to disable the
        optimized parquet reader by default. The equivalent catalog session
        property is ``parquet_optimized_reader_enabled``.
      - ``true``
    * - ``parquet.optimized-nested-reader.enabled``
      - Whether batched column readers should be used when reading ARRAY, MAP
        and ROW types from Parquet files for improved performance. Set this
        property to ``false`` to disable the optimized parquet reader by default
        for structural data types. The equivalent catalog session property is
        ``parquet_optimized_nested_reader_enabled``.
      - ``true``
    * - ``hudi.min-partition-batch-size``
      - Minimum number of partitions returned in a single batch.
      - ``10``
    * - ``hudi.max-partition-batch-size``
      - Maximum number of partitions returned in a single batch.
      - ``100``
    * - ``hudi.size-based-split-weights-enabled``
      - Unlike uniform splitting, size-based splitting ensures that each batch of splits
        has enough data to process. By default, it is enabled to improve performance.
      - ``true``
    * - ``hudi.standard-split-weight-size``
      - The split size corresponding to the standard weight (1.0)
        when size-based split weights are enabled.
      - ``128MB``
    * - ``hudi.minimum-assigned-split-weight``
      - Minimum weight that a split can be assigned
        when size-based split weights are enabled.
      - ``0.05``
    * - ``hudi.max-splits-per-second``
      - Rate at which splits are queued for processing.
        The queue is throttled if this rate limit is breached.
      - ``Integer.MAX_VALUE``
    * - ``hudi.max-outstanding-splits``
      - Maximum outstanding splits in a batch enqueued for processing.
      - ``1000``

Supported file types
--------------------

The connector supports Parquet file type.

SQL support
-----------

The connector provides read access to data in the Hudi table that has been synced to
Hive metastore. The :ref:`globally available <sql-globally-available>`
and :ref:`read operation <sql-read-operations>` statements are supported.

Supported query types
^^^^^^^^^^^^^^^^^^^^^

Hudi supports `two types of tables <https://hudi.apache.org/docs/table_types>`_
depending on how the data is indexed and laid out on the file system. The following
table displays a support matrix of tables types and query types for the connector.

=========================== =============================================
Table type                  Supported query type
=========================== =============================================
Copy on write               Snapshot queries

Merge on read               Read optimized queries
=========================== =============================================

Examples queries
^^^^^^^^^^^^^^^^

In the queries below, ``stock_ticks_cow`` is a Hudi copy-on-write table that we refer
in the Hudi `quickstart <https://hudi.apache.org/docs/docker_demo/>`_ documentation.

Here are some sample queries:

.. code-block:: sql

    USE example.example_schema;

    SELECT symbol, max(ts)
    FROM stock_ticks_cow
    GROUP BY symbol
    HAVING symbol = 'GOOG';

.. code-block:: text

      symbol   |        _col1         |
    -----------+----------------------+
     GOOG      | 2018-08-31 10:59:00  |
    (1 rows)

.. code-block:: sql

    SELECT dt, symbol
    FROM stock_ticks_cow
    WHERE symbol = 'GOOG';

.. code-block:: text

        dt      | symbol |
    ------------+--------+
     2018-08-31 |  GOOG  |
    (1 rows)

.. code-block:: sql

    SELECT dt, count(*)
    FROM stock_ticks_cow
    GROUP BY dt;
.. code-block:: text

        dt      | _col1 |
    ------------+--------+
     2018-08-31 |  99  |
    (1 rows)

.. _hudi-metadata-tables:

Metadata tables
---------------

The connector exposes a metadata table for each Hudi table.
The metadata table contains information about the internal structure
of the Hudi table. You can query each metadata table by appending the
metadata table name to the table name::

   SELECT * FROM "test_table$timeline"

``$timeline`` table
^^^^^^^^^^^^^^^^^^^^

The ``$timeline`` table provides a detailed view of meta-data instants
in the Hudi table. Instants are specific points in time.

You can retrieve the information about the timeline of the Hudi table
``test_table`` by using the following query::

    SELECT * FROM "test_table$timeline"

.. code-block:: text

     timestamp          | action  | state
    --------------------+---------+-----------
    8667764846443717831 | commit  | COMPLETED
    7860805980949777961 | commit  | COMPLETED

The output of the query has the following columns:

.. list-table:: Timeline columns
  :widths: 20, 30, 50
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``timestamp``
    - ``varchar``
    - Instant time is typically a timestamp when the actions performed
  * - ``action``
    - ``varchar``
    - `Type of action <https://hudi.apache.org/docs/concepts/#timeline>`_ performed on the table
  * - ``state``
    - ``varchar``
    - Current state of the instant
