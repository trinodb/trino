===============
Kafka Connector
===============

.. toctree::
    :maxdepth: 1
    :hidden:

    Tutorial <kafka-tutorial>

Overview
--------

This connector allows the use of `Apache Kafka <https://kafka.apache.org/>`_
topics as tables in Presto. Each message is presented as a row in Presto.

Topics can be live. Rows appear as data arrives, and disappear as
segments get dropped. This can result in strange behavior if accessing the
same table multiple times in a single query (e.g., performing a self join).

The connector reads and writes message data from Kafka topics in parallel across
workers to achieve a significant performance gain. The size of data sets for this
parallelization is configurable and can therefore be adapted to your specific
needs.

See the :doc:`kafka-tutorial`.

.. note::

    The minimum supported Kafka broker version is 0.10.0.

Configuration
-------------

To configure the Kafka connector, create a catalog properties file
``etc/catalog/kafka.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=kafka
    kafka.table-names=table1,table2
    kafka.nodes=host1:port,host2:port

Multiple Kafka Clusters
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Kafka clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
creates a catalog named ``sales`` using the configured connector.

Configuration Properties
------------------------

The following configuration properties are available:

=============================== ==============================================================
Property Name                   Description
=============================== ==============================================================
``kafka.table-names``           List of all tables provided by the catalog
``kafka.default-schema``        Default schema name for tables
``kafka.nodes``                 List of nodes in the Kafka cluster
``kafka.buffer-size``           Kafka read buffer size
``kafka.table-description-dir`` Directory containing topic description files
``kafka.hide-internal-columns`` Controls whether internal columns are part of the table schema or not
``kafka.messages-per-split``    Number of messages that are processed by each Presto split, defaults to 100000
=============================== ==============================================================

``kafka.table-names``
^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of all tables provided by this catalog. A table name can be
unqualified (simple name), and is then placed into the default schema (see
below), or it can be qualified with a schema name
(``<schema-name>.<table-name>``).

For each table defined here, a table description file (see below) may
exist. If no table description file exists, the table name is used as the
topic name on Kafka and no data columns are mapped into the table. The
table still contains all internal columns (see below).

This property is required; there is no default and at least one table must be defined.

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

    Presto must still be able to connect to all nodes of the cluster
    even if only a subset is specified here as segment files may be
    located only on a specific node.

``kafka.buffer-size``
^^^^^^^^^^^^^^^^^^^^^

Size of the internal data buffer for reading data from Kafka. The data
buffer must be able to hold at least one message and ideally can hold many
messages. There is one data buffer allocated per worker and data node.

This property is optional; the default is ``64kb``.

``kafka.table-description-dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

References a folder within Presto deployment that holds one or more JSON
files (must end with ``.json``) which contain table description files.

This property is optional; the default is ``etc/kafka``.

``kafka.hide-internal-columns``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the data columns defined in a table description file, the
connector maintains a number of additional columns for each table. If
these columns are hidden, they can still be used in queries but do not
show up in ``DESCRIBE <table-name>`` or ``SELECT *``.

This property is optional; the default is ``true``.

Internal Columns
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
======================= =============================== =============================

For tables without a table definition file, the ``_key_corrupt`` and
``_message_corrupt`` columns will always be ``false``.

Table Definition Files
----------------------

Kafka maintains topics only as byte messages and leaves it to producers
and consumers to define how a message should be interpreted. For Presto,
this data must be mapped into columns to allow queries against the data.

.. note::

    For textual topics that contain JSON data, it is entirely possible to not
    use any table definition files, but instead use the Presto
    :doc:`/functions/json` to parse the ``_message`` column which contains
    the bytes mapped into an UTF-8 string. This is, however, pretty
    cumbersome and makes it difficult to write SQL queries. This only works
    when reading data.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary but must end in ``.json``. Place the
file in the directory configured with the ``kafka.table-description-dir``
property. The table definition file must be accessible from all Presto nodes.

.. code-block:: none

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
``tableName``   required  string         Presto table name defined by this file.
``schemaName``  optional  string         Schema which will contain the table. If omitted, the default schema name is used.
``topicName``   required  string         Kafka topic that is mapped.
``key``         optional  JSON object    Field definitions for data columns mapped to the message key.
``message``     optional  JSON object    Field definitions for data columns mapped to the message itself.
=============== ========= ============== =============================

Key and Message in Kafka
------------------------

Starting with Kafka 0.8, each message in a topic can have an optional key.
A table definition file contains sections for both key and message to map
the data onto table columns.

Each of the ``key`` and ``message`` fields in the table definition is a
JSON object that must contain two fields:

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``dataFormat``  required  string         Selects the decoder for this group of fields.
``fields``      required  JSON array     A list of field definitions. Each field definition creates a new column in the Presto table.
=============== ========= ============== =============================

Each field definition is a JSON object:

.. code-block:: none

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
``name``        required  string    Name of the column in the Presto table.
``type``        required  string    Presto type of the column.
``dataFormat``  optional  string    Selects the column decoder for this field. Defaults to the default decoder for this row data format and column type.
``dataSchema``  optional  string    The path or URL where the Avro schema resides. Used only for Avro decoder.
``mapping``     optional  string    Mapping information for the column. This is decoder specific, see below.
``formatHint``  optional  string    Sets a column specific format hint to the column decoder.
``hidden``      optional  boolean   Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.
``comment``     optional  string    Adds a column comment which is shown with ``DESCRIBE <table name>``.
=============== ========= ========= =============================

There is no limit on field descriptions for either key or message.

Kafka Inserts
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

Presto supports at-least-once delivery for Kafka producers. This means that
messages are guaranteed to be sent to Kafka topics at least once. If a producer
acknowledgement times out or if the producer receives an error, it might retry
sending the message. This could result in a duplicate message being sent to the
Kafka topic.

The Kafka connector does not allow the user to define which partition will be
used as the target for a message. If a message includes a key, the producer will
use a hash algorithm to choose the target partition for the message. The same
key will always be assigned the same partition.

Row Encoding
------------

Encoding is required to allow writing data and defines how table columns in
Presto map to Kafka keys and message data.

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

Raw Encoder
^^^^^^^^^^^

The raw encoder formats the table columns as raw bytes using the mapping
information specified in the
`table definition file <#table-definition-files>`__.

The following field attributes are supported:

* ``dataFormat`` - Specifies the width of the column data type
* ``type`` - Presto data type
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

The ``type`` attribute defines the Presto data type.

Different values of ``dataFormat`` are supported, depending on the Presto data
type:

===================================== =======================================
Presto data type                      ``dataFormat`` values
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
    to fit the variable width of some Presto data types.

If only a start position is given:

* For fixed width types, the appropriate number of bytes are used for the
  specified ``dateFormat`` (see above).

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

CSV Encoder
^^^^^^^^^^^

The CSV encoder formats the values for each row as a line of
comma-separated-values (CSV) using UTF-8 encoding. The CSV line is formatted
with a comma ',' as the column delimiter.

The ``type`` and ``mapping`` attributes must be defined for each field:

* ``type`` - Presto data type
* ``mapping`` - The integer index of the column in the CSV line (the first
  column is 0, the second is 1, and so on)

``dataFormat`` and ``formatHint`` are not supported and must be omitted.

The following Presto data types are supported by the CSV encoder:

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

JSON Encoder
^^^^^^^^^^^^

The JSON encoder maps table columns to JSON fields defined in the
`table definition file <#table-definition-files>`__ according to
:rfc:`4627`.

For fields, the following attributes are supported:

* ``type`` - Presto type of column.
* ``mapping`` - slash-separated list of field names to select a field from the
  JSON object
* ``dataFormat`` - name of formatter (required for temporal types)
* ``formatHint`` - pattern to format temporal data (only use with
  ``custom-date-time`` formatter)

The following Presto data types are supported by the JSON encoder:

+-------------------------------------+
| Presto data types                   |
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
| Presto data type                    | Decoding rules                                                                 |
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
| ``TIMESTAMP WITH TIME ZONE``        | ``custom-date-time``, ``iso8601``, ``rfc2822``,                                |
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

Avro Encoder
^^^^^^^^^^^^

The Avro encoder serializes rows to Avro records as defined by the
`Avro schema <https://avro.apache.org/docs/current/>`_.
Presto does not support schema-less Avro encoding.

.. note::

    The Avro schema is encoded with the table column values in each Kafka message

The ``dataSchema`` must be defined in the table definition file to use the Avro
encoder. It points to the location of the Avro schema file for the key or message

Avro schema files can be retrieved via HTTP or HTTPS from remote server with the
syntax:

``"dataSchema": "http://example.org/schema/avro_data.avsc"``

Local files need to be available on all Presto nodes and use an absolute path in
the syntax, for example:

``"dataSchema": "/usr/local/schema/avro_data.avsc"``

The following field attributes are supported:

* ``name`` - Name of the column in the Presto table.
* ``type`` - Presto type of column.
* ``mapping`` - slash-separated list of field names to select a field from the
  Avro schema. If the field specified in ``mapping`` does not exist
  in the original Avro schema, then a write operation fails.

The following table lists supported Presto types, which can be used in ``type``
for the equivalent Avro field type.

===================================== =======================================
Presto data type                      Avro data type
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
      "namespace" : "io.prestosql.plugin.kafka",
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

Row Decoding
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

``raw`` Decoder
^^^^^^^^^^^^^^^

The raw decoder supports reading of raw (byte-based) values from Kafka message
or key and converting it into Presto columns.

For fields, the following attributes are supported:

* ``dataFormat`` - selects the width of the data type converted
* ``type`` - Presto data type (see table below for list of supported data types)
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

The ``type`` attribute defines the Presto data type on which the value is mapped.

Depending on Presto type assigned to column different values of dataFormat can be used:

===================================== =======================================
Presto data type                      Allowed ``dataFormat`` values
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

* For fixed width types the column will use the appropriate number of bytes for the specified ``dateFormat`` (see above).
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

``csv`` Decoder
^^^^^^^^^^^^^^^

The CSV decoder converts the bytes representing a message or key into a
string using UTF-8 encoding and then interprets the result as a CSV
(comma-separated value) line.

For fields, the ``type`` and ``mapping`` attributes must be defined:

* ``type`` - Presto data type (see table below for list of supported data types)
* ``mapping`` - the index of the field in the CSV record

``dataFormat`` and ``formatHint`` are not supported and must be omitted.

Table below lists supported Presto types, which can be used in ``type`` and decoding scheme:

+-------------------------------------+--------------------------------------------------------------------------------+
| Presto data type                    | Decoding rules                                                                 |
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


``json`` Decoder
^^^^^^^^^^^^^^^^

The JSON decoder converts the bytes representing a message or key into a
JSON according to :rfc:`4627`. Note that the message or key *MUST* convert
into a JSON object, not an array or simple type.

For fields, the following attributes are supported:

* ``type`` - Presto type of column.
* ``dataFormat`` - Field decoder to be used for column.
* ``mapping`` - slash-separated list of field names to select a field from the JSON object
* ``formatHint`` - only for ``custom-date-time``, see below

The JSON decoder supports multiple field decoders, with ``_default`` being
used for standard table columns and a number of decoders for date and time
based types.

The table below lists Presto data types, which can be used as in ``type``, and matching field decoders,
which can be specified via ``dataFormat`` attribute.

+-------------------------------------+--------------------------------------------------------------------------------+
| Presto data type                    | Allowed ``dataFormat`` values                                                  |
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
| | ``TIMESTAMP WITH TIME ZONE``      | ``custom-date-time``, ``iso8601``, ``rfc2822``                                 |
+-------------------------------------+--------------------------------------------------------------------------------+


Default Field decoder
^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the standard field decoder, supporting all the Presto physical data
types. A field value is transformed under JSON conversion rules into
boolean, long, double or string values. For non-date/time based columns,
this decoder should be used.

Date and Time Decoders
^^^^^^^^^^^^^^^^^^^^^^

To convert values from JSON objects into Presto ``DATE``, ``TIME``, ``TIME WITH TIME ZONE``,
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
be used in Presto value. Otherwise result time zone will be set to ``UTC``.

``avro`` Decoder
^^^^^^^^^^^^^^^^

The Avro decoder converts the bytes representing a message or key in
Avro format based on a schema. The message must have the Avro schema embedded.
Presto does not support schema-less Avro decoding.

For key/message, using ``avro`` decoder, the ``dataSchema`` must be defined.
This should point to the location of a valid Avro schema file of the message which needs to be decoded. This location can be a remote web server
(e.g.: ``dataSchema: 'http://example.org/schema/avro_data.avsc'``) or local file system(e.g.: ``dataSchema: '/usr/local/schema/avro_data.avsc'``).
The decoder fails if this location is not accessible from the Presto coordinator node.

For fields, the following attributes are supported:

* ``name`` - Name of the column in the Presto table.
* ``type`` - Presto type of column.
* ``mapping`` - slash-separated list of field names to select a field from the Avro schema. If field specified in ``mapping`` does not exist in the original Avro schema then a read operation returns NULL.

Table below lists supported Presto types which can be used in ``type`` for the equivalent Avro field type/s.

===================================== =======================================
Presto data type                      Allowed Avro data type
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
#####################

The Avro decoder supports schema evolution feature with backward compatibility. With backward compatibility,
a newer schema can be used to read Avro data created with an older schema. Any change in the Avro schema must also be
reflected in Presto's topic definition file. Newly added/renamed fields *must* have a default value in the Avro schema file.

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
