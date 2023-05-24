=================
MongoDB connector
=================

.. raw:: html

  <img src="../_static/img/mongodb.png" class="connector-logo">

The ``mongodb`` connector allows the use of `MongoDB <https://www.mongodb.com/>`_ collections as tables in Trino.


Requirements
------------

To connect to MongoDB, you need:

* MongoDB 4.2 or higher.
* Network access from the Trino coordinator and workers to MongoDB.
  Port 27017 is the default port.
* Write access to the :ref:`schema information collection <table-definition-label>`
  in MongoDB.

Configuration
-------------

To configure the MongoDB connector, create a catalog properties file
``etc/catalog/example.properties`` with the following contents,
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
Property name                              Description
========================================== ==============================================================
``mongodb.connection-url``                 The connection url that the driver uses to connect to a MongoDB deployment
``mongodb.schema-collection``              A collection which contains schema information
``mongodb.case-insensitive-name-matching`` Match database and collection names case insensitively
``mongodb.min-connections-per-host``       The minimum size of the connection pool per host
``mongodb.connections-per-host``           The maximum size of the connection pool per host
``mongodb.max-wait-time``                  The maximum wait time
``mongodb.max-connection-idle-time``       The maximum idle time of a pooled connection
``mongodb.connection-timeout``             The socket connect timeout
``mongodb.socket-timeout``                 The socket timeout
``mongodb.tls.enabled``                    Use TLS/SSL for connections to mongod/mongos
``mongodb.tls.keystore-path``              Path to the  or JKS key store
``mongodb.tls.truststore-path``            Path to the  or JKS trust store
``mongodb.tls.keystore-password``          Password for the key store
``mongodb.tls.truststore-password``        Password for the trust store
``mongodb.read-preference``                The read preference
``mongodb.write-concern``                  The write concern
``mongodb.required-replica-set``           The required replica set name
``mongodb.cursor-batch-size``              The number of elements to return in a batch
========================================== ==============================================================

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

This property is required; there is no default. A connection URL must be
provided to connect to a MongoDB deployment.

``mongodb.schema-collection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As MongoDB is a document database, there is no fixed schema information in the system. So a special collection in each MongoDB database should define the schema of all tables. Please refer the :ref:`table-definition-label` section for the details.

At startup, the connector tries to guess the data type of fields based on the :ref:`type mapping <mongodb-type-mapping>`.

The initial guess can be incorrect for your specific collection. In that case, you need to modify it manually. Please refer the :ref:`table-definition-label` section for the details.

Creating new tables using ``CREATE TABLE`` and ``CREATE TABLE AS SELECT`` automatically create an entry for you.

This property is optional; the default is ``_schema``.

``mongodb.case-insensitive-name-matching``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Match database and collection names case insensitively.

This property is optional; the default is ``false``.

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

``mongodb.tls.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^

This flag enables TLS connections to MongoDB servers.

This property is optional; the default is ``false``.

``mongodb.tls.keystore-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the :doc:`PEM </security/inspect-pem>` or
:doc:`JKS </security/inspect-jks>` key store.

This property is optional.

``mongodb.tls.truststore-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to :doc:`PEM </security/inspect-pem>` or
:doc:`JKS </security/inspect-jks>` trust store.

This property is optional.

``mongodb.tls.keystore-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The key password for the key store specified by ``mongodb.tls.keystore-path``.

This property is optional.

``mongodb.tls.truststore-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The key password for the trust store specified by ``mongodb.tls.truststore-path``.

This property is optional.

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
#. Refuse to service any requests, if authenticated user is not part of a replica set with the required name.

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

The connector quotes the fields for a row type when auto-generating the schema.
However, if the schema is being fixed manually in the collection then
the fields need to be explicitly quoted. ``row("UpperCase" varchar)``

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

.. _mongodb-type-mapping:

Type mapping
------------

Because Trino and MongoDB each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

MongoDB to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps MongoDB types to the corresponding Trino types following
this table:

.. list-table:: MongoDB to Trino type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - MongoDB type
    - Trino type
    - Notes
  * - ``Boolean``
    - ``BOOLEAN``
    -
  * - ``Int32``
    - ``BIGINT``
    -
  * - ``Int64``
    - ``BIGINT``
    -
  * - ``Double``
    - ``DOUBLE``
    -
  * - ``Decimal128``
    - ``DECIMAL(p, s)``
    -
  * - ``Date``
    - ``TIMESTAMP(3)``
    -
  * - ``String``
    - ``VARCHAR``
    -
  * - ``Binary``
    - ``VARBINARY``
    -
  * - ``ObjectId``
    - ``ObjectId``
    -
  * - ``Object``
    - ``ROW``
    -
  * - ``Array``
    - ``ARRAY``
    -   Map to ``ROW`` if the element type is not unique.
  * - ``DBRef``
    - ``ROW``
    -

No other types are supported.

Trino to MongoDB type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Trino types to the corresponding MongoDB types following
this table:

.. list-table:: Trino to MongoDB type mapping
  :widths: 30, 20
  :header-rows: 1

  * - Trino type
    - MongoDB type
  * - ``BOOLEAN``
    - ``Boolean``
  * - ``BIGINT``
    - ``Int64``
  * - ``DOUBLE``
    - ``Double``
  * - ``DECIMAL(p, s)``
    - ``Decimal128``
  * - ``TIMESTAMP(3)``
    - ``Date``
  * - ``VARCHAR``
    - ``String``
  * - ``VARBINARY``
    - ``Binary``
  * - ``ObjectId``
    - ``ObjectId``
  * - ``ROW``
    - ``Object``
  * - ``ARRAY``
    - ``Array``

No other types are supported.

.. _mongodb-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
MongoDB. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/insert`
* :doc:`/sql/delete`
* :doc:`/sql/create-table`
* :doc:`/sql/create-table-as`
* :doc:`/sql/drop-table`
* :doc:`/sql/alter-table`
* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`
* :doc:`/sql/comment`

ALTER TABLE
^^^^^^^^^^^

The connector supports ``ALTER TABLE RENAME TO``, ``ALTER TABLE ADD COLUMN``
and ``ALTER TABLE DROP COLUMN`` operations.
Other uses of ``ALTER TABLE`` are not supported.

.. _mongodb-fte-support:

Fault-tolerant execution support
--------------------------------

The connector supports :doc:`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.

Table functions
---------------

The connector provides specific :doc:`table functions </functions/table>` to
access MongoDB.

.. _mongodb-query-function:

``query(database, collection, filter) -> table``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``query`` function allows you to query the underlying MongoDB directly. It
requires syntax native to MongoDB, because the full query is pushed down and
processed by MongoDB. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

For example, get all rows where ``regionkey`` field is 0::

    SELECT
      *
    FROM
      TABLE(
        example.system.query(
          database => 'tpch',
          collection => 'region',
          filter => '{ regionkey: 0 }'
        )
      );
