===============
Kafka connector
===============

.. toctree::
    :maxdepth: 1
    :hidden:

    Tutorial <kafka-tutorial>

Overview
--------

This connector allows the use of `Apache Kafka <https://kafka.apache.org/>`_
topics as tables in Trino. Each message is presented as a row in Trino.

Topics can be live. Rows appear as data arrives, and disappear as
segments get dropped. This can result in strange behavior if accessing the
same table multiple times in a single query (e.g., performing a self join).

The connector reads and writes message data from Kafka topics in parallel across
workers to achieve a significant performance gain. The size of data sets for this
parallelization is configurable and can therefore be adapted to your specific
needs.

See the :doc:`kafka-tutorial`.

Requirements
------------

To connect to Kafka, you need:

* Kafka broker version 0.10.0 or higher.
* Network access from the Trino coordinator and workers to the Kafka nodes.
  Port 9092 is the default port.

Configuration
-------------

To configure the Kafka connector, create a catalog properties file
``etc/catalog/kafka.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: text

    connector.name=kafka
    kafka.table-names=table1,table2
    kafka.nodes=host1:port,host2:port

Multiple Kafka clusters
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Kafka clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Trino
creates a catalog named ``sales`` using the configured connector.

Configuration properties
------------------------

The following configuration properties are available:

========================================================== ==============================================================================
Property Name                                              Description
========================================================== ==============================================================================
``kafka.default-schema``                                   Default schema name for tables
``kafka.nodes``                                            List of nodes in the Kafka cluster
``kafka.buffer-size``                                      Kafka read buffer size
``kafka.hide-internal-columns``                            Controls whether internal columns are part of the table schema or not
``kafka.messages-per-split``                               Number of messages that are processed by each Trino split, defaults to 100000
``kafka.timestamp-upper-bound-force-push-down-enabled``    Controls if upper bound timestamp push down is enabled for topics using ``CreateTime`` mode
``kafka.security-protocol``                                Security protocol for connection to Kafka cluster, defaults to ``PLAINTEXT``
``kafka.ssl.keystore.location``                            Location of the keystore file
``kafka.ssl.keystore.password``                            Password for the keystore file
``kafka.ssl.keystore.type``                                File format of the keystore file, defaults to ``JKS``
``kafka.ssl.truststore.location``                          Location of the truststore file
``kafka.ssl.truststore.password``                          Password for the truststore file
``kafka.ssl.truststore.type``                              File format of the truststore file, defaults to ``JKS``
``kafka.ssl.key.password``                                 Password for the private key in the keystore file
``kafka.ssl.endpoint-identification-algorithm``            Endpoint identification algorithm used by clients to validate server host name, defaults to ``https``
========================================================== ==============================================================================

In addition, you need to configure :ref:`table schema and schema registry usage
<kafka-table-schema-registry>` with the relevant properties.


``kafka.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema which contains all tables that were defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``kafka.nodes``
^^^^^^^^^^^^^^^

A comma separated list of ``hostname:port`` pairs for the Kafka data nodes.

This property is required; there is no default and at least one node must be defined.

.. note::

    Trino must still be able to connect to all nodes of the cluster
    even if only a subset is specified here as segment files may be
    located only on a specific node.

``kafka.buffer-size``
^^^^^^^^^^^^^^^^^^^^^

Size of the internal data buffer for reading data from Kafka. The data
buffer must be able to hold at least one message and ideally can hold many
messages. There is one data buffer allocated per worker and data node.

This property is optional; the default is ``64kb``.

``kafka.timestamp-upper-bound-force-push-down-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The upper bound predicate on ``_timestamp`` column
is pushed down only for topics using ``LogAppendTime`` mode.

For topics using ``CreateTime`` mode, upper bound push down must be explicitly
allowed via ``kafka.timestamp-upper-bound-force-push-down-enabled`` config property
or ``timestamp_upper_bound_force_push_down_enabled`` session property.

This property is optional; the default is ``false``.

``kafka.hide-internal-columns``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the data columns defined in a table description file, the
connector maintains a number of additional columns for each table. If
these columns are hidden, they can still be used in queries but do not
show up in ``DESCRIBE <table-name>`` or ``SELECT *``.

This property is optional; the default is ``true``.

``kafka.security-protocol``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Protocol used to communicate with brokers.
Valid values are: PLAINTEXT, SSL.

This property is optional; default is ``PLAINTEXT``.

``kafka.ssl.keystore.location``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Location of the keystore file used for connection to Kafka cluster.

This property is optional.

``kafka.ssl.keystore.password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Password for the keystore file used for connection to Kafka cluster.

This property is optional, but required when ``kafka.ssl.keystore.location`` is given.

``kafka.ssl.keystore.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

File format of the keystore file.
Valid values are: JKS, PKCS12.

This property is optional; default is ``JKS``.

``kafka.ssl.truststore.location``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Location of the truststore file used for connection to Kafka cluster.

This property is optional.

``kafka.ssl.truststore.password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Password for the truststore file used for connection to Kafka cluster.

This property is optional, but required when ``kafka.ssl.truststore.location`` is given.

``kafka.ssl.truststore.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

File format of the truststore file.
Valid values are: JKS, PKCS12.

This property is optional; default is ``JKS``.

``kafka.ssl.key.password``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Password for the private key in the keystore file used for connection to Kafka cluster.

This property is optional. This is required for clients only if two-way authentication is configured i.e. ``ssl.client.auth=required``.

``kafka.ssl.endpoint-identification-algorithm``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The endpoint identification algorithm used by clients to validate server host name for connection to Kafka cluster.
Kafka uses ``https`` as default. Use ``disabled`` to disable server host name validation.

This property is optional; default is ``https``.

Internal columns
----------------

For each defined table, the connector maintains the following columns:

======================= =============================== =============================
Column name             Type                            Description
======================= =============================== =============================
``_partition_id``       BIGINT                          ID of the Kafka partition which contains this row.
``_partition_offset``   BIGINT                          Offset within the Kafka partition for this row.
``_segment_start``      BIGINT                          Lowest offset in the segment (inclusive) which contains this row. This offset is partition specific.
``_segment_end``        BIGINT                          Highest offset in the segment (exclusive) which contains this row. The offset is partition specific. This is the same value as ``_segment_start`` of the next segment (if it exists).
``_segment_count``      BIGINT                          Running count for the current row within the segment. For an uncompacted topic, ``_segment_start + _segment_count`` is equal to ``_partition_offset``.
``_message_corrupt``    BOOLEAN                         True if the decoder could not decode the message for this row. When true, data columns mapped from the message should be treated as invalid.
``_message``            VARCHAR                         Message bytes as an UTF-8 encoded string. This is only useful for a text topic.
``_message_length``     BIGINT                          Number of bytes in the message.
``_headers``            map(VARCHAR, array(VARBINARY))  Headers of the message where values with the same key are grouped as array.
``_key_corrupt``        BOOLEAN                         True if the key decoder could not decode the key for this row. When true, data columns mapped from the key should be treated as invalid.
``_key``                VARCHAR                         Key bytes as an UTF-8 encoded string. This is only useful for textual keys.
``_key_length``         BIGINT                          Number of bytes in the key.
``_timestamp``          TIMESTAMP                       Message timestamp.
======================= =============================== =============================

For tables without a table definition file, the ``_key_corrupt`` and
``_message_corrupt`` columns will always be ``false``.

.. _kafka-table-schema-registry:

Table schema and schema registry usage
--------------------------------------

The table schema for the messages can be supplied to the connector with a
configuration file or a schema registry. It also provides a mechanism for the
connector to discover tables.

You have to configure the supplier with the ``kafka.table-description-supplier``
property, setting it to ``FILE`` or ``CONFLUENT``. Each table description
supplier has a separate set of configuration properties.

Refer to the following subsections for more detail. The ``FILE`` table
description supplier is the default and the value is case insensitive.

File table description supplier
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to use the file based table description supplier, the
``kafka.table-description-supplier`` must be set to ``FILE`` which is the
default.

In addition, you must set ``kafka.table-names`` and
``kafka.table-description-dir`` as described in the following sections:

``kafka.table-names``
"""""""""""""""""""""

Comma-separated list of all tables provided by this catalog. A table name can be
unqualified (simple name), and is then placed into the default schema (see
below), or it can be qualified with a schema name
(``<schema-name>.<table-name>``).

For each table defined here, a table description file (see below) may exist. If
no table description file exists, the table name is used as the topic name on
Kafka and no data columns are mapped into the table. The table still contains
all internal columns (see below).

This property is required; there is no default and at least one table must be
defined.

``kafka.table-description-dir``
"""""""""""""""""""""""""""""""

References a folder within Trino deployment that holds one or more JSON files
(must end with ``.json``) which contain table description files.

This property is optional; the default is ``etc/kafka``.

Table definition files
""""""""""""""""""""""

Kafka maintains topics only as byte messages and leaves it to producers
and consumers to define how a message should be interpreted. For Trino,
this data must be mapped into columns to allow queries against the data.

.. note::

    For textual topics that contain JSON data, it is entirely possible to not
    use any table definition files, but instead use the Trino
    :doc:`/functions/json` to parse the ``_message`` column which contains
    the bytes mapped into an UTF-8 string. This is, however, pretty
    cumbersome and makes it difficult to write SQL queries. This only works
    when reading data.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary but must end in ``.json``. Place the
file in the directory configured with the ``kafka.table-description-dir``
property. The table definition file must be accessible from all Trino nodes.

.. code-block:: text

    {
        "tableName": ...,
        "schemaName": ...,
        "topicName": ...,
        "key": {
            "dataFormat": ...,
            "fields": [
                ...
            ]
        },
        "message": {
            "dataFormat": ...,
            "fields": [
                ...
           ]
        }
    }

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``tableName``   required  string         Trino table name defined by this file.
``schemaName``  optional  string         Schema which will contain the table. If omitted, the default schema name is used.
``topicName``   required  string         Kafka topic that is mapped.
``key``         optional  JSON object    Field definitions for data columns mapped to the message key.
``message``     optional  JSON object    Field definitions for data columns mapped to the message itself.
=============== ========= ============== =============================

Key and message in Kafka
""""""""""""""""""""""""

Starting with Kafka 0.8, each message in a topic can have an optional key.
A table definition file contains sections for both key and message to map
the data onto table columns.

Each of the ``key`` and ``message`` fields in the table definition is a
JSON object that must contain two fields:

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``dataFormat``  required  string         Selects the decoder for this group of fields.
``fields``      required  JSON array     A list of field definitions. Each field definition creates a new column in the Trino table.
=============== ========= ============== =============================

Each field definition is a JSON object:

.. code-block:: text

    {
        "name": ...,
        "type": ...,
        "dataFormat": ...,
        "mapping": ...,
        "formatHint": ...,
        "hidden": ...,
        "comment": ...
    }

=============== ========= ========= =============================
Field           Required  Type      Description
=============== ========= ========= =============================
``name``        required  string    Name of the column in the Trino table.
``type``        required  string    Trino type of the column.
``dataFormat``  optional  string    Selects the column decoder for this field. Defaults to the default decoder for this row data format and column type.
``dataSchema``  optional  string    The path or URL where the Avro schema resides. Used only for Avro decoder.
``mapping``     optional  string    Mapping information for the column. This is decoder specific, see below.
``formatHint``  optional  string    Sets a column specific format hint to the column decoder.
``hidden``      optional  boolean   Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.
``comment``     optional  string    Adds a column comment which is shown with ``DESCRIBE <table name>``.
=============== ========= ========= =============================

There is no limit on field descriptions for either key or message.

Confluent table description supplier
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Confluent table description supplier uses the `Confluent Schema Registry
<https://docs.confluent.io/1.0/schema-registry/docs/intro.html>`_ to discover
table definitions. It is only tested to work with the Confluent Schema
Registry.

The benefits of using the Confluent table description supplier over the file
table description supplier are:

* New tables can be defined without a cluster restart.
* Schema updates are detected automatically.
* There is no need to define tables manually.

Set ``kafka.table-description-supplier`` to ``CONFLUENT`` to use the
schema registry and configure the additional properties in the following table:

.. note::

    Inserts are not supported and the only data format supported is AVRO.

.. list-table:: Confluent table description supplier properties
  :widths: 30, 55, 15
  :header-rows: 1

  * - Property name
    - Description
    - Default value
  * - ``kafka.confluent-schema-registry-url``
    - Comma-separated list of URL addresses for the Confluent schema registry.
      For example, ``http://schema-registry-1.example.org:8081,http://schema-registry-2.example.org:8081``
    -
  * - ``kafka.confluent-schema-registry-client-cache-size``
    - The maximum number of subjects that can be stored in the local cache. The
      cache stores the schemas locally by subjectId, and is provided by the
      Confluent ``CachingSchemaRegistry`` client.
    - 1000
  * - ``kafka.empty-field-strategy``
    - Avro allows empty struct fields but this is not allowed in Trino. There
      are three strategies for handling empty struct fields:

        - ``IGNORE``, ignore structs with no fields. This propagate to parents.
          For example an array of structs with no fields is ignored.
        - ``FAIL``, fail the query if a struct with no fields is defined.
        - ``DUMMY``, add a dummy boolean field called ``dummy`` which is null.
          This may be desired if the struct represents a marker field.
    - ``IGNORE``
  * - ``kafka.confluent-subjects-cache-refresh-interval``
    - The interval used for refreshing the list of subjects and the definition
      of the schema for the subject in the subjects cache
    - ``1s``


Confluent subject to table name mapping
"""""""""""""""""""""""""""""""""""""""

The `subject naming strategy
<https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#sr-schemas-subject-name-strategy>`_
determines how a subject is resolved from the table name.

The default strategy is the ``TopicNameStrategy`` where the key subject is
defined as ``<topic-name>-key`` and the value subject is defined as
``<topic-name>-value``. If other strategies are used there is no way to
determine the subject name beforehand so it must be specified manually in the
table name.

To manually specify the key and value subjects just append to the table name,
for example: ``<topic name>&key-subject=<key subject>&value-subject=<value
subject``. Both the ``key-subject`` and ``value-subject`` parameters are
optional. If either is not specified then the default ``TopicNameStrategy`` is
used to resolve the subject name via the topic name. Note that a case
insensitive match must be done, as identifiers cannot contain upper case
characters.

.. _kafka-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in Trino
tables populated by Kafka topics. See :ref:`kafka-row-decoding` for more
information.

In addition to the :ref:`globally available <sql-globally-available>`
and :ref:`read operation <sql-read-operations>` statements, the connector
supports the following features:

* :doc:`/sql/insert`, encoded to a specified data format. See also
  :ref:`kafka-sql-inserts`.

.. _kafka-sql-inserts:

Kafka inserts
-------------

The Kafka connector supports the use of :doc:`/sql/insert` statements to write
data to a Kafka topic. Table column data is mapped to Kafka messages as defined
in the `table definition file <#table-definition-files>`__. There are
four supported data formats for key and message encoding:

* `raw format <#raw-encoder>`__
* `CSV format <#csv-encoder>`__
* `JSON format <#json-encoder>`__
* `Avro format <#avro-encoder>`__

These data formats each have an encoder that maps column values into bytes to be
sent to a Kafka topic.

Trino supports at-least-once delivery for Kafka producers. This means that
messages are guaranteed to be sent to Kafka topics at least once. If a producer
acknowledgement times out or if the producer receives an error, it might retry
sending the message. This could result in a duplicate message being sent to the
Kafka topic.

The Kafka connector does not allow the user to define which partition will be
used as the target for a message. If a message includes a key, the producer will
use a hash algorithm to choose the target partition for the message. The same
key will always be assigned the same partition.

Row encoding
------------

Encoding is required to allow writing data and defines how table columns in
Trino map to Kafka keys and message data.

The Kafka connector contains the following encoders:

* `raw encoder <#raw-encoder>`__ - Table columns are mapped to a Kafka
  message as raw bytes
* `CSV encoder <#csv-encoder>`__ - Kafka message is formatted as comma
  separated value
* `JSON encoder <#json-encoder>`__ - Table columns are mapped to JSON
  fields
* `Avro encoder <#avro-encoder>`__ - Table columns are mapped to Avro
  fields based on an Avro schema

.. note::

    A `table definition file <#table-definition-files>`__ must be defined
    for the encoder to work.

Raw encoder
^^^^^^^^^^^

The raw encoder formats the table columns as raw bytes using the mapping
information specified in the
`table definition file <#table-definition-files>`__.

The following field attributes are supported:

* ``dataFormat`` - Specifies the width of the column data type
* ``type`` - Trino data type
* ``mapping`` - start and optional end position of bytes to convert
  (specified as ``start`` or ``start:end``)

The ``dataFormat`` attribute selects the number of bytes converted.
If absent, ``BYTE`` is assumed. All values are signed.

Supported values:

* ``BYTE`` - one byte
* ``SHORT`` - two bytes (big-endian)
* ``INT`` - four bytes (big-endian)
* ``LONG`` - eight bytes (big-endian)
* ``FLOAT`` - four bytes (IEEE 754 format, big-endian)
* ``DOUBLE`` - eight bytes (IEEE 754 format, big-endian)

The ``type`` attribute defines the Trino data type.

Different values of ``dataFormat`` are supported, depending on the Trino data
type:

===================================== =======================================
Trino data type                       ``dataFormat`` values
===================================== =======================================
``BIGINT``                            ``BYTE``, ``SHORT``, ``INT``, ``LONG``
``INTEGER``                           ``BYTE``, ``SHORT``, ``INT``
``SMALLINT``                          ``BYTE``, ``SHORT``
``TINYINT``                           ``BYTE``
``REAL``                              ``FLOAT``
``DOUBLE``                            ``FLOAT``, ``DOUBLE``
``BOOLEAN``                           ``BYTE``, ``SHORT``, ``INT``, ``LONG``
``VARCHAR`` / ``VARCHAR(x)``          ``BYTE``
===================================== =======================================

The ``mapping`` attribute specifies the range of bytes in a key or
message used for encoding.

.. note::

    Both a start and end position must be defined for ``VARCHAR`` types.
    Otherwise, there is no way to know how many bytes the message contains. The
    raw format mapping information is static and cannot be dynamically changed
    to fit the variable width of some Trino data types.

If only a start position is given:

* For fixed width types, the appropriate number of bytes are used for the
  specified ``dataFormat`` (see above).

If both a start and end position are given, then:

* For fixed width types, the size must be equal to number of bytes used by
  specified ``dataFormat``.
* All bytes between start (inclusive) and end (exclusive) are used.

.. note::

    All mappings must include a start position for encoding to work.

The encoding for numeric data types (``BIGINT``, ``INTEGER``, ``SMALLINT``,
``TINYINT``, ``REAL``, ``DOUBLE``) is straightforward. All numeric types use
big-endian. Floating point types use IEEE 754 format.

Example raw field definition in a `table definition file <#table-definition-files>`__
for a Kafka message:

.. code-block:: json

    {
      "tableName": "your-table-name",
      "schemaName": "your-schema-name",
      "topicName": "your-topic-name",
      "key": { "..." },
      "message": {
        "dataFormat": "raw",
        "fields": [
          {
            "name": "field1",
            "type": "BIGINT",
            "dataFormat": "LONG",
            "mapping": "0"
          },
          {
            "name": "field2",
            "type": "INTEGER",
            "dataFormat": "INT",
            "mapping": "8"
          },
          {
            "name": "field3",
            "type": "SMALLINT",
            "dataFormat": "LONG",
            "mapping": "12"
          },
          {
            "name": "field4",
            "type": "VARCHAR(6)",
            "dataFormat": "BYTE",
            "mapping": "20:26"
          }
        ]
      }
    }

Columns should be defined in the same order they are mapped. There can be no
gaps or overlaps between column mappings. The width of the column as defined by
the column mapping must be equivalent to the width of the ``dataFormat`` for all
types except for variable width types.

Example insert query for the above table definition::

    INSERT INTO example_raw_table (field1, field2, field3, field4)
      VALUES (123456789, 123456, 1234, 'abcdef');

When inserting variable width types, the value must be exactly equal to the
width defined in the table definition file. If the inserted value is longer it
gets truncated, resulting in data loss. If the inserted value is shorter the
decoder will not be able to properly read the value because there is no defined
padding character. Due to these constraints the encoder fails if the width of
the inserted value is not equal to the mapping width.

CSV encoder
^^^^^^^^^^^

The CSV encoder formats the values for each row as a line of
comma-separated-values (CSV) using UTF-8 encoding. The CSV line is formatted
with a comma ',' as the column delimiter.

The ``type`` and ``mapping`` attributes must be defined for each field:

* ``type`` - Trino data type
* ``mapping`` - The integer index of the column in the CSV line (the first
  column is 0, the second is 1, and so on)

``dataFormat`` and ``formatHint`` are not supported and must be omitted.

The following Trino data types are supported by the CSV encoder:

* ``BIGINT``
* ``INTEGER``
* ``SMALLINT``
* ``TINYINT``
* ``DOUBLE``
* ``REAL``
* ``BOOLEAN``
* ``VARCHAR`` / ``VARCHAR(x)``

Column values are converted to strings before they are formatted as a CSV line.

Example CSV field definition in a `table definition file <#table-definition-files>`__
for a Kafka message:

.. code-block:: json

    {
      "tableName": "your-table-name",
      "schemaName": "your-schema-name",
      "topicName": "your-topic-name",
      "key": { "..." },
      "message": {
        "dataFormat": "csv",
        "fields": [
          {
            "name": "field1",
            "type": "BIGINT",
            "mapping": "0"
          },
          {
            "name": "field2",
            "type": "VARCHAR",
            "mapping": "1"
          },
          {
            "name": "field3",
            "type": "BOOLEAN",
            "mapping": "2"
          }
        ]
      }
    }

Example insert query for the above table definition::

    INSERT INTO example_csv_table (field1, field2, field3)
      VALUES (123456789, 'example text', TRUE);

JSON encoder
^^^^^^^^^^^^

The JSON encoder maps table columns to JSON fields defined in the
`table definition file <#table-definition-files>`__ according to
:rfc:`4627`.

For fields, the following attributes are supported:

* ``type`` - Trino type of column.
* ``mapping`` - slash-separated list of field names to select a field from the
  JSON object
* ``dataFormat`` - name of formatter (required for temporal types)
* ``formatHint`` - pattern to format temporal data (only use with
  ``custom-date-time`` formatter)

The following Trino data types are supported by the JSON encoder:

+-------------------------------------+
| Trino data types                    |
+=====================================+
| ``BIGINT``                          |
|                                     |
| ``INTEGER``                         |
|                                     |
| ``SMALLINT``                        |
|                                     |
| ``TINYINT``                         |
|                                     |
| ``DOUBLE``                          |
|                                     |
| ``REAL``                            |
|                                     |
| ``BOOLEAN``                         |
|                                     |
| ``VARCHAR``                         |
|                                     |
| ``DATE``                            |
|                                     |
| ``TIME``                            |
|                                     |
| ``TIME WITH TIME ZONE``             |
|                                     |
| ``TIMESTAMP``                       |
|                                     |
| ``TIMESTAMP WITH TIME ZONE``        |
+-------------------------------------+

The following ``dataFormats`` are available for temporal data:

* ``iso8601``
* ``rfc2822``
* ``custom-date-time`` - formats temporal data according to
  `Joda Time <https://www.joda.org/joda-time/key_format.html>`__
  pattern given by ``formatHint`` field
* ``milliseconds-since-epoch``
* ``seconds-since-epoch``

All temporal data in Kafka supports milliseconds precision

The following table defines which temporal data types are supported by
``dataFormats``:

+-------------------------------------+--------------------------------------------------------------------------------+
| Trino data type                     | Decoding rules                                                                 |
+=====================================+================================================================================+
| ``DATE``                            | ``custom-date-time``, ``iso8601``                                              |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``TIME``                            | ``custom-date-time``, ``iso8601``, ``milliseconds-since-epoch``,               |
|                                     | ``seconds-since-epoch``                                                        |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``TIME WITH TIME ZONE``             | ``custom-date-time``, ``iso8601``                                              |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``TIMESTAMP``                       | ``custom-date-time``, ``iso8601``, ``rfc2822``,                                |
|                                     | ``milliseconds-since-epoch``, ``seconds-since-epoch``                          |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``TIMESTAMP WITH TIME ZONE``        | ``custom-date-time``, ``iso8601``, ``rfc2822``, ``milliseconds-since-epoch``,  |
|                                     | ``seconds-since-epoch``                                                        |
+-------------------------------------+--------------------------------------------------------------------------------+

Example JSON field definition in a `table definition file <#table-definition-files>`__
for a Kafka message:

.. code-block:: json

    {
      "tableName": "your-table-name",
      "schemaName": "your-schema-name",
      "topicName": "your-topic-name",
      "key": { "..." },
      "message": {
        "dataFormat": "json",
        "fields": [
          {
            "name": "field1",
            "type": "BIGINT",
            "mapping": "field1"
          },
          {
            "name": "field2",
            "type": "VARCHAR",
            "mapping": "field2"
          },
          {
            "name": "field3",
            "type": "TIMESTAMP",
            "dataFormat": "custom-date-time",
            "formatHint": "yyyy-dd-MM HH:mm:ss.SSS",
            "mapping": "field3"
          }
        ]
      }
    }

Example insert query for the above table definition::

    INSERT INTO example_json_table (field1, field2, field3)
      VALUES (123456789, 'example text', TIMESTAMP '2020-07-15 01:02:03.456');

Avro encoder
^^^^^^^^^^^^

The Avro encoder serializes rows to Avro records as defined by the
`Avro schema <https://avro.apache.org/docs/current/>`_.
Trino does not support schema-less Avro encoding.

.. note::

    The Avro schema is encoded with the table column values in each Kafka message

The ``dataSchema`` must be defined in the table definition file to use the Avro
encoder. It points to the location of the Avro schema file for the key or message

Avro schema files can be retrieved via HTTP or HTTPS from remote server with the
syntax:

``"dataSchema": "http://example.org/schema/avro_data.avsc"``

Local files need to be available on all Trino nodes and use an absolute path in
the syntax, for example:

``"dataSchema": "/usr/local/schema/avro_data.avsc"``

The following field attributes are supported:

* ``name`` - Name of the column in the Trino table.
* ``type`` - Trino type of column.
* ``mapping`` - slash-separated list of field names to select a field from the
  Avro schema. If the field specified in ``mapping`` does not exist
  in the original Avro schema, then a write operation fails.

The following table lists supported Trino types, which can be used in ``type``
for the equivalent Avro field type.

===================================== =======================================
Trino data type                       Avro data type
===================================== =======================================
``BIGINT``                            ``INT``, ``LONG``
``REAL``                              ``FLOAT``
``DOUBLE``                            ``FLOAT``, ``DOUBLE``
``BOOLEAN``                           ``BOOLEAN``
``VARCHAR`` / ``VARCHAR(x)``          ``STRING``
===================================== =======================================

Example Avro field definition in a `table definition file <#table-definition-files>`__
for a Kafka message:

.. code-block:: json

    {
      "tableName": "your-table-name",
      "schemaName": "your-schema-name",
      "topicName": "your-topic-name",
      "key": { "..." },
      "message":
      {
        "dataFormat": "avro",
        "dataSchema": "/avro_message_schema.avsc",
        "fields":
        [
          {
            "name": "field1",
            "type": "BIGINT",
            "mapping": "field1"
          },
          {
            "name": "field2",
            "type": "VARCHAR",
            "mapping": "field2"
          },
          {
            "name": "field3",
            "type": "BOOLEAN",
            "mapping": "field3"
          }
        ]
      }
    }

Example Avro schema definition for the above table definition:

.. code-block:: json

    {
      "type" : "record",
      "name" : "example_avro_message",
      "namespace" : "io.trino.plugin.kafka",
      "fields" :
      [
        {
          "name":"field1",
          "type":["null", "long"],
          "default": null
        },
        {
          "name": "field2",
          "type":["null", "string"],
          "default": null
        },
        {
          "name":"field3",
          "type":["null", "boolean"],
          "default": null
        }
      ],
      "doc:" : "A basic avro schema"
    }

Example insert query for the above table definition::

    INSERT INTO example_avro_table (field1, field2, field3)
      VALUES (123456789, 'example text', FALSE);

.. _kafka-row-decoding:

Row decoding
------------

For key and message, a decoder is used to map message and key data onto table columns.

The Kafka connector contains the following decoders:

* ``raw`` - Kafka message is not interpreted, ranges of raw message bytes are mapped to table columns
* ``csv`` - Kafka message is interpreted as comma separated message, and fields are mapped to table columns
* ``json`` - Kafka message is parsed as JSON and JSON fields are mapped to table columns
* ``avro`` - Kafka message is parsed based on an Avro schema and Avro fields are mapped to table columns

.. note::

    If no table definition file exists for a table, the ``dummy`` decoder is used,
    which does not expose any columns.

``raw`` decoder
^^^^^^^^^^^^^^^

The raw decoder supports reading of raw (byte-based) values from Kafka message
or key and converting it into Trino columns.

For fields, the following attributes are supported:

* ``dataFormat`` - selects the width of the data type converted
* ``type`` - Trino data type (see table below for list of supported data types)
* ``mapping`` - ``<start>[:<end>]``; start and end position of bytes to convert (optional)

The ``dataFormat`` attribute selects the number of bytes converted.
If absent, ``BYTE`` is assumed. All values are signed.

Supported values are:

* ``BYTE`` - one byte
* ``SHORT`` - two bytes (big-endian)
* ``INT`` - four bytes (big-endian)
* ``LONG`` - eight bytes (big-endian)
* ``FLOAT`` - four bytes (IEEE 754 format)
* ``DOUBLE`` - eight bytes (IEEE 754 format)

The ``type`` attribute defines the Trino data type on which the value is mapped.

Depending on Trino type assigned to column different values of dataFormat can be used:

===================================== =======================================
Trino data type                       Allowed ``dataFormat`` values
===================================== =======================================
``BIGINT``                            ``BYTE``, ``SHORT``, ``INT``, ``LONG``
``INTEGER``                           ``BYTE``, ``SHORT``, ``INT``
``SMALLINT``                          ``BYTE``, ``SHORT``
``TINYINT``                           ``BYTE``
``DOUBLE``                            ``DOUBLE``, ``FLOAT``
``BOOLEAN``                           ``BYTE``, ``SHORT``, ``INT``, ``LONG``
``VARCHAR`` / ``VARCHAR(x)``          ``BYTE``
===================================== =======================================

The ``mapping`` attribute specifies the range of the bytes in a key or
message used for decoding. It can be one or two numbers separated by a colon (``<start>[:<end>]``).

If only a start position is given:

* For fixed width types the column will use the appropriate number of bytes for the specified ``dataFormat`` (see above).
* When ``VARCHAR`` value is decoded all bytes from start position till the end of the message will be used.

If start and end position are given, then:

* For fixed width types the size must be equal to number of bytes used by specified ``dataFormat``.
* For ``VARCHAR`` all bytes between start (inclusive) and end (exclusive) are used.

If no ``mapping`` attribute is specified, it is equivalent to setting start position to 0 and leaving end position undefined.

Decoding scheme of numeric data types (``BIGINT``, ``INTEGER``, ``SMALLINT``, ``TINYINT``, ``DOUBLE``) is straightforward.
A sequence of bytes is read from input message and decoded according to either:

* big-endian encoding (for integer types)
* IEEE 754 format for (for ``DOUBLE``).

Length of decoded byte sequence is implied by the ``dataFormat``.

For ``VARCHAR`` data type a sequence of bytes is interpreted according to UTF-8 encoding.

``csv`` decoder
^^^^^^^^^^^^^^^

The CSV decoder converts the bytes representing a message or key into a
string using UTF-8 encoding and then interprets the result as a CSV
(comma-separated value) line.

For fields, the ``type`` and ``mapping`` attributes must be defined:

* ``type`` - Trino data type (see table below for list of supported data types)
* ``mapping`` - the index of the field in the CSV record

``dataFormat`` and ``formatHint`` are not supported and must be omitted.

Table below lists supported Trino types, which can be used in ``type`` and decoding scheme:

+-------------------------------------+--------------------------------------------------------------------------------+
| Trino data type                     | Decoding rules                                                                 |
+=====================================+================================================================================+
| | ``BIGINT``                        | Decoded using Java ``Long.parseLong()``                                        |
| | ``INTEGER``                       |                                                                                |
| | ``SMALLINT``                      |                                                                                |
| | ``TINYINT``                       |                                                                                |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``DOUBLE``                          | Decoded using Java ``Double.parseDouble()``                                    |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``BOOLEAN``                         | "true" character sequence maps to ``true``;                                    |
|                                     | Other character sequences map to ``false``                                     |
+-------------------------------------+--------------------------------------------------------------------------------+
| ``VARCHAR`` / ``VARCHAR(x)``        | Used as is                                                                     |
+-------------------------------------+--------------------------------------------------------------------------------+


``json`` decoder
^^^^^^^^^^^^^^^^

The JSON decoder converts the bytes representing a message or key into a
JSON according to :rfc:`4627`. Note that the message or key *MUST* convert
into a JSON object, not an array or simple type.

For fields, the following attributes are supported:

* ``type`` - Trino type of column.
* ``dataFormat`` - Field decoder to be used for column.
* ``mapping`` - slash-separated list of field names to select a field from the JSON object
* ``formatHint`` - only for ``custom-date-time``, see below

The JSON decoder supports multiple field decoders, with ``_default`` being
used for standard table columns and a number of decoders for date and time
based types.

The table below lists Trino data types, which can be used as in ``type``, and matching field decoders,
which can be specified via ``dataFormat`` attribute.

+-------------------------------------+--------------------------------------------------------------------------------+
| Trino data type                     | Allowed ``dataFormat`` values                                                  |
+=====================================+================================================================================+
| | ``BIGINT``                        | Default field decoder (omitted ``dataFormat`` attribute)                       |
| | ``INTEGER``                       |                                                                                |
| | ``SMALLINT``                      |                                                                                |
| | ``TINYINT``                       |                                                                                |
| | ``DOUBLE``                        |                                                                                |
| | ``BOOLEAN``                       |                                                                                |
| | ``VARCHAR``                       |                                                                                |
| | ``VARCHAR(x)``                    |                                                                                |
+-------------------------------------+--------------------------------------------------------------------------------+
| | ``DATE``                          | ``custom-date-time``, ``iso8601``                                              |
+-------------------------------------+--------------------------------------------------------------------------------+
| | ``TIME``                          | ``custom-date-time``, ``iso8601``, ``milliseconds-since-epoch``,               |
| |                                   | ``seconds-since-epoch``                                                        |
+-------------------------------------+--------------------------------------------------------------------------------+
| | ``TIME WITH TIME ZONE``           | ``custom-date-time``, ``iso8601``                                              |
+-------------------------------------+--------------------------------------------------------------------------------+
| | ``TIMESTAMP``                     | ``custom-date-time``, ``iso8601``, ``rfc2822``,                                |
| |                                   | ``milliseconds-since-epoch``, ``seconds-since-epoch``                          |
+-------------------------------------+--------------------------------------------------------------------------------+
| | ``TIMESTAMP WITH TIME ZONE``      | ``custom-date-time``, ``iso8601``, ``rfc2822``, ``milliseconds-since-epoch``   |
| |                                   | ``seconds-since-epoch``                                                        |
+-------------------------------------+--------------------------------------------------------------------------------+


Default field decoder
^^^^^^^^^^^^^^^^^^^^^

This is the standard field decoder, supporting all the Trino physical data
types. A field value is transformed under JSON conversion rules into
boolean, long, double or string values. For non-date/time based columns,
this decoder should be used.

Date and time decoders
^^^^^^^^^^^^^^^^^^^^^^

To convert values from JSON objects into Trino ``DATE``, ``TIME``, ``TIME WITH TIME ZONE``,
``TIMESTAMP`` or ``TIMESTAMP WITH TIME ZONE`` columns, special decoders must be selected using the
``dataFormat`` attribute of a field definition.

* ``iso8601`` - text based, parses a text field as an ISO 8601 timestamp.
* ``rfc2822`` - text based, parses a text field as an :rfc:`2822` timestamp.
* ``custom-date-time`` - text based, parses a text field according to Joda format pattern
                         specified via ``formatHint`` attribute. Format pattern should conform
                         to https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html.
* ``milliseconds-since-epoch`` - number based, interprets a text or number as number of milliseconds since the epoch.
* ``seconds-since-epoch`` - number based, interprets a text or number as number of milliseconds since the epoch.

For ``TIMESTAMP WITH TIME ZONE`` and ``TIME WITH TIME ZONE`` data types, if timezone information is present in decoded value, it will
be used in Trino value. Otherwise result time zone will be set to ``UTC``.

``avro`` decoder
^^^^^^^^^^^^^^^^

The Avro decoder converts the bytes representing a message or key in
Avro format based on a schema. The message must have the Avro schema embedded.
Trino does not support schema-less Avro decoding.

For key/message, using ``avro`` decoder, the ``dataSchema`` must be defined.
This should point to the location of a valid Avro schema file of the message which needs to be decoded. This location can be a remote web server
(e.g.: ``dataSchema: 'http://example.org/schema/avro_data.avsc'``) or local file system(e.g.: ``dataSchema: '/usr/local/schema/avro_data.avsc'``).
The decoder fails if this location is not accessible from the Trino coordinator node.

For fields, the following attributes are supported:

* ``name`` - Name of the column in the Trino table.
* ``type`` - Trino type of column.
* ``mapping`` - slash-separated list of field names to select a field from the Avro schema. If field specified in ``mapping`` does not exist in the original Avro schema then a read operation returns NULL.

Table below lists supported Trino types which can be used in ``type`` for the equivalent Avro field type/s.

===================================== =======================================
Trino data type                       Allowed Avro data type
===================================== =======================================
``BIGINT``                            ``INT``, ``LONG``
``DOUBLE``                            ``DOUBLE``, ``FLOAT``
``BOOLEAN``                           ``BOOLEAN``
``VARCHAR`` / ``VARCHAR(x)``          ``STRING``
``VARBINARY``                         ``FIXED``, ``BYTES``
``ARRAY``                             ``ARRAY``
``MAP``                               ``MAP``
===================================== =======================================

Avro schema evolution
"""""""""""""""""""""

The Avro decoder supports schema evolution feature with backward compatibility. With backward compatibility,
a newer schema can be used to read Avro data created with an older schema. Any change in the Avro schema must also be
reflected in Trino's topic definition file. Newly added/renamed fields *must* have a default value in the Avro schema file.

The schema evolution behavior is as follows:

* Column added in new schema:
  Data created with an older schema produces a *default* value, when the table is using the new schema.

* Column removed in new schema:
  Data created with an older schema no longer outputs the data from the column that was removed.

* Column is renamed in the new schema:
  This is equivalent to removing the column and adding a new one, and data created with an older schema
  produces a *default* value when table is using the new schema.

* Changing type of column in the new schema:
  If the type coercion is supported by Avro, then the conversion happens. An error is thrown for incompatible types.
