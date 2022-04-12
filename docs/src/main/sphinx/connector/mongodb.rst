=================
MongoDB connector
=================

.. raw:: html

  <img src="../_static/img/mongodb.png" class="connector-logo">

The ``mongodb`` connector allows the use of `MongoDB <https://www.mongodb.com/>`_ collections as tables in Trino.


Requirements
------------

To connect to MongoDB, you need:

* MongoDB 4.0 or higher.
* Network access from the Trino coordinator and workers to MongoDB.
  Port 27017 is the default port.
* Write access to the :ref:`schema information collection <table-definition-label>`
  in MongoDB.

Configuration
-------------

To configure the MongoDB connector, create a catalog properties file
``etc/catalog/mongodb.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: text

    connector.name=mongodb
    mongodb.connection-url=mongodb://user:pass@sample.host:27017/

Multiple MongoDB clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
MongoDB clusters, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Trino
will create a catalog named ``sales`` using the configured connector.

Configuration properties
------------------------

The following configuration properties are available:

========================================== ==============================================================
Property Name                              Description
========================================== ==============================================================
``mongodb.seeds``                          List of all MongoDB servers
``mongodb.connection-url``                 The connection url that the driver uses to connect to a MongoDB deployment
``mongodb.schema-collection``              A collection which contains schema information
``mongodb.case-insensitive-name-matching`` Match database and collection names case insensitively
``mongodb.credentials``                    List of credentials
``mongodb.min-connections-per-host``       The minimum size of the connection pool per host
``mongodb.connections-per-host``           The maximum size of the connection pool per host
``mongodb.max-wait-time``                  The maximum wait time
``mongodb.max-connection-idle-time``       The maximum idle time of a pooled connection
``mongodb.connection-timeout``             The socket connect timeout
``mongodb.socket-timeout``                 The socket timeout
``mongodb.ssl.enabled``                    Use TLS/SSL for connections to mongod/mongos
``mongodb.read-preference``                The read preference
``mongodb.write-concern``                  The write concern
``mongodb.required-replica-set``           The required replica set name
``mongodb.cursor-batch-size``              The number of elements to return in a batch
========================================== ==============================================================

``mongodb.seeds``
^^^^^^^^^^^^^^^^^

Comma-separated list of ``hostname[:port]`` all MongoDB servers in the same replica set, or a list of MongoDB servers in the same sharded cluster. If a port is not specified, port 27017 will be used.

This property is deprecated and will be removed in a future release. Use ``mongodb.connection-url`` property instead.

``mongodb.connection-url``
^^^^^^^^^^^^^^^^^^^^^^^^^^

A connection string containing the protocol, credential, and host info for use
inconnection to your MongoDB deployment.

For example, the connection string may use the format
``mongodb://<user>:<pass>@<host>:<port>/?<options>`` or
``mongodb+srv://<user>:<pass>@<host>/?<options>``, depending on the protocol
used. The user/pass credentials must be for a user with write access to the
:ref:`schema information collection <table-definition-label>`.

See the `MongoDB Connection URI <https://docs.mongodb.com/drivers/java/sync/current/fundamentals/connection/#connection-uri>`_ for more information.

This property is required; there is no default. A connection url or seeds must be provided to connect to a MongoDB deployment.

``mongodb.schema-collection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As MongoDB is a document database, there is no fixed schema information in the system. So a special collection in each MongoDB database should define the schema of all tables. Please refer the :ref:`table-definition-label` section for the details.

At startup, the connector tries to guess the data type of fields based on the mapping in the following table.

================== ================ ================================================
MongoDB            Trino            Notes
================== ================ ================================================
``Boolean``        ``BOOLEAN``
``Int32``          ``BIGINT``
``Int64``          ``BIGINT``
``Double``         ``DOUBLE``
``Date``           ``TIMESTAMP(3)``
``String``         ``VARCHAR``
``Binary``         ``VARBINARY``
``ObjectId``       ``ObjectId``
``Object``         ``ROW``
``Array``          ``ARRAY``        Map to ``ROW`` if the element type is not unique
``DBRef``          ``ROW``
================== ================ ================================================

The initial guess can be incorrect for your specific collection. In that case, you need to modify it manually. Please refer the :ref:`table-definition-label` section for the details.

Creating new tables using ``CREATE TABLE`` and ``CREATE TABLE AS SELECT`` automatically create an entry for you.

This property is optional; the default is ``_schema``.

``mongodb.case-insensitive-name-matching``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Match database and collection names case insensitively.

This property is optional; the default is ``false``.

``mongodb.credentials``
^^^^^^^^^^^^^^^^^^^^^^^

A comma separated list of ``username:password@database`` credentials.

This property is optional; no default value. The ``database`` should be the authentication database for the user (e.g. ``admin``).

``mongodb.min-connections-per-host``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The minimum number of connections per host for this MongoClient instance. Those connections are kept in a pool when idle, and the pool ensures over time that it contains at least this minimum number.

This property is optional; the default is ``0``.

``mongodb.connections-per-host``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of connections allowed per host for this MongoClient instance. Those connections are kept in a pool when idle. Once the pool is exhausted, any operation requiring a connection blocks waiting for an available connection.

This property is optional; the default is ``100``.

``mongodb.max-wait-time``
^^^^^^^^^^^^^^^^^^^^^^^^^

The maximum wait time in milliseconds, that a thread may wait for a connection to become available.
A value of ``0`` means that it does not wait. A negative value means to wait indefinitely for a connection to become available.

This property is optional; the default is ``120000``.

``mongodb.max-connection-idle-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The maximum idle time of a pooled connection in milliseconds. A value of ``0`` indicates no limit to the idle time.
A pooled connection that has exceeded its idle time will be closed and replaced when necessary by a new connection.

This property is optional; the default is ``0``.

``mongodb.connection-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connection timeout in milliseconds. A value of ``0`` means no timeout. It is used solely when establishing a new connection.

This property is optional; the default is ``10000``.

``mongodb.socket-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^

The socket timeout in milliseconds. It is used for I/O socket read and write operations.

This property is optional; the default is ``0`` and means no timeout.

``mongodb.ssl.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^

This flag enables SSL connections to MongoDB servers.

This property is optional; the default is ``false``.

``mongodb.read-preference``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The read preference to use for queries, map-reduce, aggregation, and count.
The available values are ``PRIMARY``, ``PRIMARY_PREFERRED``, ``SECONDARY``, ``SECONDARY_PREFERRED`` and ``NEAREST``.

This property is optional; the default is ``PRIMARY``.

``mongodb.write-concern``
^^^^^^^^^^^^^^^^^^^^^^^^^

The write concern to use. The available values are
``ACKNOWLEDGED``, ``JOURNALED``, ``MAJORITY`` and ``UNACKNOWLEDGED``.

This property is optional; the default is ``ACKNOWLEDGED``.

``mongodb.required-replica-set``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The required replica set name. With this option set, the MongoClient instance performs the following actions::

#. Connect in replica set mode, and discover all members of the set based on the given servers
#. Make sure that the set name reported by all members matches the required set name.
#. Refuse to service any requests, if any member of the seed list is not part of a replica set with the required name.

This property is optional; no default value.

``mongodb.cursor-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Limits the number of elements returned in one batch. A cursor typically fetches a batch of result objects and stores them locally.
If batchSize is 0, Driver's default are used.
If batchSize is positive, it represents the size of each batch of objects retrieved. It can be adjusted to optimize performance and limit data transfer.
If batchSize is negative, it limits the number of objects returned, that fit within the max batch size limit (usually 4MB), and the cursor is closed. For example if batchSize is -10, then the server returns a maximum of 10 documents, and as many as can fit in 4MB, then closes the cursor.

.. note:: Do not use a batch size of ``1``.

This property is optional; the default is ``0``.

.. _table-definition-label:

Table definition
----------------

MongoDB maintains table definitions on the special collection where ``mongodb.schema-collection`` configuration value specifies.

.. note::

    There's no way for the plugin to detect a collection is deleted.
    You need to delete the entry by ``db.getCollection("_schema").remove( { table: deleted_table_name })`` in the Mongo Shell.
    Or drop a collection by running ``DROP TABLE table_name`` using Trino.

A schema collection consists of a MongoDB document for a table.

.. code-block:: text

    {
        "table": ...,
        "fields": [
              { "name" : ...,
                "type" : "varchar|bigint|boolean|double|date|array(bigint)|...",
                "hidden" : false },
                ...
            ]
        }
    }

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``table``       required  string         Trino table name
``fields``      required  array          A list of field definitions. Each field definition creates a new column in the Trino table.
=============== ========= ============== =============================

Each field definition:

.. code-block:: text

    {
        "name": ...,
        "type": ...,
        "hidden": ...
    }

=============== ========= ========= =============================
Field           Required  Type      Description
=============== ========= ========= =============================
``name``        required  string    Name of the column in the Trino table.
``type``        required  string    Trino type of the column.
``hidden``      optional  boolean   Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.
=============== ========= ========= =============================

There is no limit on field descriptions for either key or message.

ObjectId
--------

MongoDB collection has the special field ``_id``. The connector tries to follow the same rules for this special field, so there will be hidden field ``_id``.

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS orders (
        orderkey bigint,
        orderstatus varchar,
        totalprice double,
        orderdate date
    );

    INSERT INTO orders VALUES(1, 'bad', 50.0, current_date);
    INSERT INTO orders VALUES(2, 'good', 100.0, current_date);
    SELECT _id, * FROM orders;

.. code-block:: text

                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
     55 b1 51 67 38 64 d6 43 8c 61 a9 cf |        2 | good        |      100.0 | 2015-07-23
    (2 rows)

.. code-block:: sql

    SELECT _id, * FROM orders WHERE _id = ObjectId('55b151633864d6438c61a9ce');

.. code-block:: text

                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
    (1 row)

You can render the ``_id`` field to readable values with a cast to ``VARCHAR``:

.. code-block:: sql

    SELECT CAST(_id AS VARCHAR), * FROM orders WHERE _id = ObjectId('55b151633864d6438c61a9ce');

.. code-block:: text

               _id             | orderkey | orderstatus | totalprice | orderdate
    ---------------------------+----------+-------------+------------+------------
     55b151633864d6438c61a9ce  |        1 | bad         |       50.0 | 2015-07-23
    (1 row)

ObjectId timestamp functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first four bytes of each `ObjectId <https://docs.mongodb.com/manual/reference/method/ObjectId>`_ represent
an embedded timestamp of its creation time. Trino provides a couple of functions to take advantage of this MongoDB feature.

.. function:: objectid_timestamp(ObjectId) -> timestamp

    Extracts the timestamp with time zone from a given ObjectId::

        SELECT objectid_timestamp(ObjectId('507f191e810c19729de860ea'));
        -- 2012-10-17 20:46:22.000 UTC

.. function:: timestamp_objectid(timestamp) -> ObjectId

    Creates an ObjectId from a timestamp with time zone::

        SELECT timestamp_objectid(TIMESTAMP '2021-08-07 17:51:36 +00:00');
        -- 61 0e c8 28 00 00 00 00 00 00 00 00

In MongoDB, you can filter all the documents created after ``2021-08-07 17:51:36``
with a query like this:

.. code-block:: text

    db.collection.find({"_id": {"$gt": ObjectId("610ec8280000000000000000")}})

In Trino, the same can be achieved with this query:

.. code-block:: sql

    SELECT *
    FROM collection
    WHERE _id > timestamp_objectid(TIMESTAMP '2021-08-07 17:51:36 +00:00');

.. _mongodb-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
MongoDB. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/alter-table`
* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`
* :doc:`/sql/comment`

ALTER TABLE
^^^^^^^^^^^

The connector does not support ``ALTER TABLE RENAME`` operations. Other uses of
``ALTER TABLE`` are supported.
