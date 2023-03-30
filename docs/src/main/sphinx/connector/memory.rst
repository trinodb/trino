================
Memory connector
================

The Memory connector stores all data and metadata in RAM on workers
and both are discarded when Trino restarts.

Configuration
-------------

To configure the Memory connector, create a catalog properties file
``etc/catalog/example.properties`` with the following contents:

.. code-block:: text

    connector.name=memory
    memory.max-data-per-node=128MB

``memory.max-data-per-node`` defines memory limit for pages stored in this
connector per each node (default value is 128MB).

Examples
--------

Create a table using the Memory connector::

    CREATE TABLE example.default.nation AS
    SELECT * from tpch.tiny.nation;

Insert data into a table in the Memory connector::

    INSERT INTO example.default.nation
    SELECT * FROM tpch.tiny.nation;

Select from the Memory connector::

    SELECT * FROM example.default.nation;

Drop table::

    DROP TABLE example.default.nation;

.. _memory-type-mapping:

Type mapping
------------

Trino supports all data types used within the Memory schemas so no mapping is
required.

.. _memory-sql-support:

SQL support
-----------

The connector provides read and write access to temporary data and metadata
stored in memory. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`
* :doc:`/sql/comment`

DROP TABLE
^^^^^^^^^^

Upon execution of a ``DROP TABLE`` operation, memory is not released
immediately. It is instead released after the next write operation to the
catalog.

.. _memory_dynamic_filtering:

Dynamic filtering
-----------------

The Memory connector supports the :doc:`dynamic filtering </admin/dynamic-filtering>` optimization.
Dynamic filters are pushed into local table scan on worker nodes for broadcast joins.

Delayed execution for dynamic filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For the Memory connector, a table scan is delayed until the collection of dynamic filters.
This can be disabled by using the configuration property ``memory.enable-lazy-dynamic-filtering``
in the catalog file.

Limitations
-----------

* When one worker fails/restarts, all data that was stored in its
  memory is lost. To prevent silent data loss the
  connector throws an error on any read access to such
  corrupted table.
* When a query fails for any reason during writing to memory table,
  the table enters an undefined state. The table should be dropped
  and recreated manually. Reading attempts from the table may fail,
  or may return partial data.
* When the coordinator fails/restarts, all metadata about tables is
  lost. The tables remain on the workers, but become inaccessible.
* This connector does not work properly with multiple
  coordinators, since each coordinator has different
  metadata.
