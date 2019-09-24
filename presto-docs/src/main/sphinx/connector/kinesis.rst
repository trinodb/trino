=================
Kinesis Connector
=================
Kinesis is Amazon's fully managed cloud-based service for real-time processing of large, distributed data streams.

This connector allows the use of Kinesis streams as tables in Presto, such that each data-blob (message)
in a kinesis stream is presented as a row in Presto.  A flexible table mapping approach lets us
treat fields of the messages as columns in the table.

Under the hood, a Kinesis shard iterator is used to retrieve the records, along with a series of getRecords calls.
The shard iterator starts by default 24 hours before the current time and works its way forward. To be able to query a stream, table mappings are needed. These table definitions can be
stored on Amazon S3 (preferred) or stored in a local directory on each Presto node.

This connector is a read-only connector. It can only fetch data from kinesis streams, but cannot create streams or push
data into existing streams.

To configure the Kinesis connector, create a catalog properties file, ``etc/catalog/CATALOG_NAME.properties`` with the following
content and replace the properties as appropriate:

.. sourcecode:: bash

    connector.name=kinesis
    kinesis.access-key=XXXXXX
    kinesis.secret-key=XXXXXX

Configuration Properties
------------------------
The following configuration properties are available.

+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| Property Name                     |  Description                                                                                              |
+===================================+===========================================================================================================+
| ``kinesis.default-schema``	    | Default schema name for tables                                                                            |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.table-description-dir`` | Directory containing table description files                                                              |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.table-descriptions-s3`` | Amazon S3 bucket URL with table description files. Leave blank to read from the directory on the server.  |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.access-key``            | Access key to aws account or blank to use default provider chain                                          |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.secret-key``            | Secret key to aws account or blank to use default provider chain                                          |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.hide-internal-columns`` | Controls whether internal columns are part of the table schema or not                                     |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.aws-region``            | AWS region to be used to read kinesis stream from                                                         |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.batch-size``            | Maximum number of records to return in one batch. Maximum Limit 10000                                     |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.fetch-attempts``        | Read attempts made when no records returned and not caught up                                             |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.max-batches``           | Maximum batches to read from Kinesis in one single query                                                  |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.sleep-time``            | Time for thread to sleep waiting to make next attempt to fetch batch                                      |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.iter-from-timestamp``   | Begin iterating from a given timestamp instead of the trim horizon (true by default)                      |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+
| ``kinesis.iter-offset-seconds``   | Number of seconds before current time to start iterating                                                  |
+-----------------------------------+-----------------------------------------------------------------------------------------------------------+

The configuration properties are described in detail here:

* ``kinesis.default-schema``: Defines the schema which will contain all tables that were defined without a qualifying schema name.
  This property is optional; the default is ``default``.
* ``kinesis.table-description-dir``: References a folder within Presto deployment that holds one or more JSON files (must end with .json) which contain table description files.
  This property is optional; the default is ``etc/kinesis``.
* ``kinesis.table-descriptions-s3``: An S3 URL giving the location of the JSON table description files. When this is given, S3 will be used as the source of table description files and table-description-dir is ignored. The S3 bucket and folder will be checked every 10 minutes for updates and changed files.
  This property is optional; the default is blank, which means table-description-dir will be the source of the table definitions.
* ``kinesis.access-key``: Defines the access key ID for AWS root account or IAM roles, which is used to sign programmatic requests to AWS Kinesis.
  This property is optional; if not defined, connector will try to follow ``Default-Credential-Provider-Chain`` provided by AWS in the following order:

  - Environment Variable: Load credentials from environment variables ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``.
  - Java System Variable: Load from java system as ``aws.accessKeyId`` and ``aws.secretKey``
  - Profile Credentials File: Load from file typically located at ~/.aws/credentials
  - Instance profile credentials: These credentials can be used on EC2 instances, and are delivered through the Amazon EC2 metadata service.

* ``kinesis.secret-key``: Defines the secret key for AWS root account or IAM roles, which together with Access Key ID, is used to sign programmatic requests to AWS Kinesis.
  This property is optional; if not defined, connector will try to follow Default- Credential-Provider-Chain same as above.
* ``kinesis.aws-region``: Defines AWS Kinesis regional endpoint. Selecting appropriate region may reduce latency in fetching data.
  This field is optional; The default region is ``us-east-1`` referring to end point 'kinesis.us-east-1.amazonaws.com'.

  **Amazon Kinesis Regions**

  For each Amazon Kinesis account, the following available regions can be used.

    +----------------+-------------+-------------------+---------------------------------------+
    | Region         | Name        | Region            | Endpoint                              |
    +================+=============+===================+=======================================+
    | us-east-1      | US East     | (N. Virginia)     | kinesis.us-east-1.amazonaws.com       |
    +----------------+-------------+-------------------+---------------------------------------+
    | us-west-1      | US West     | (N. California)   | kinesis.us-west-1.amazonaws.com       |
    +----------------+-------------+-------------------+---------------------------------------+
    | us-west-2      | US West     | (Oregon)          | kinesis.us-west-2.amazonaws.com       |
    +----------------+-------------+-------------------+---------------------------------------+
    | eu-west-1      | EU          | (Ireland)         | kinesis.eu-west-1.amazonaws.com       |
    +----------------+-------------+-------------------+---------------------------------------+
    | eu-central-1   | EU          | (Frankfurt)       | kinesis.eu-central-1.amazonaws.com    |
    +----------------+-------------+-------------------+---------------------------------------+
    | ap-southeast-1 | Asia Pacific| (Singapore)       | kinesis.ap-southeast-1.amazonaws.com  |
    +----------------+-------------+-------------------+---------------------------------------+
    | ap-southeast-2 | Asia Pacific| (Sydney)          | kinesis.ap-southeast-2.amazonaws.com  |
    +----------------+-------------+-------------------+---------------------------------------+
    | ap-northeast-1 | Asia Pacific| (Tokyo)           | kinesis.ap-northeast-1.amazonaws.com  |
    +----------------+-------------+-------------------+---------------------------------------+

* ``kinesis.batch-size``: Defines maximum number of records to return in one request to Kinesis Streams. Maximum Limit is 10000 records. If a value greater than 10000 is specified, will throw InvalidArgumentException.
  This field is optional; the default value is 10000.
* ``kinesis.fetch-attempts``: Defines number of attempts made to read a batch from Kinesis Streams when no records are returned and the "millis behind latest" parameter shows we are not yet caught up. When records are returned no additional attempts are necessary.
  It has been found that sometimes GetRecordResult returns empty records, when shard is not empty. That is why multiple attempts need to be made.
  This field is optional; the default value is 2.
* ``kinesis.max-batches``: The maximum number of batches to read in a single query. The default value is 1000.
* ``kinesis.sleep-time``: Defines the milliseconds for which thread needs to sleep between get-record-attempts made to fetch data. The quantity should be followed by 'ms' string.
  This field is optional; the default value is 1000ms.
* ``iter-from-timestamp``: Use an initial shard iterator type of AT_TIMESTAMP starting iterOffsetSeconds before the current time. When this is false, an iterator type of TRIM_HORIZON will be used, meaning it will start from the oldest record in the stream.
  The default is true.
* ``iter-offset-seconds``: When iterFromTimestamp is true, the shard iterator will start at ``iter-offset-seconds`` before the current time.
  The default is 86400 seconds or 24 hours.
* ``kinesis.hide-internal-columns``: In addition to the data columns defined in a table description file, the connector maintains a number of additional columns for each table. If these columns are hidden, they can still be used in queries but do not show up in ``DESCRIBE <table-name>`` or ``SELECT *``.
  This property is optional; the default is true.

Internal Columns
----------------
For each defined table, the connector maintains the following columns:

+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Column name           | Type     | Description                                                                                                                                                                          |
+=======================+==========+======================================================================================================================================================================================+
| ``_shard_id``         | VARCHAR  | ID of the Kinesis stream shard which contains this row                                                                                                                               |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_shard_sequence_id``| VARCHAR  | Sequence id within the Kinesis shard for this row                                                                                                                                    |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_segment_start``    | BIGINT   | Lowest offset in the segment (inclusive) which contains this row. This offset is partition specific.                                                                                 |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_segment_end``      | BIGINT   | Highest offset in the segment (exclusive) which contains this row. The offset is partition specific. This is the same value as ``_segment_start`` of the next segment (if it exists).|
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_segment_count``    | BIGINT   | Running count for the current row within the segment. For an uncompacted topic, ``_segment_start + _segment_count`` is equal to ``_partition_offset``.                               |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_message_valid``    | BOOLEAN  | True if the decoder could decode the message successfully for this row. When false, data columns mapped from the message should be treated as invalid.                               |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_message``          | VARCHAR  | Message bytes as an UTF-8 encoded string. This is only useful for a text topic.                                                                                                      |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_message_length``   | BIGINT   | Number of bytes in the message.                                                                                                                                                      |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_message_timestamp``| TIMESTAMP| Approximate arrival time of the message (milliseconds granularity).                                                                                                                  |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_key``              | VARCHAR  | Key bytes as an UTF-8 encoded string. This is only useful for textual keys.                                                                                                          |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``_partition_key``    | VARCHAR  | Partition Key bytes as an UTF-8 encoded string                                                                                                                                       |
+-----------------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

For tables without a table definition file, the _message_valid column will always be true.

Table Definition
----------------
A table definition file consists of a JSON definition for a table, which corresponds to one stream in Kinesis. The name of the file can be arbitrary but must end in .json. The structure of the table definition is as follows:

.. sourcecode:: bash

  {
        "tableName": ...,
        "schemaName": ...,
        "key": {
            "dataFormat": ...,
            "fields": [
                ...
            ]
        },
        "value": {
            "dataFormat": ...,
            "fields": [
                ...
           ]
        }
    }

+---------------+---------+-------------+----------------------------------------------------------------------------------------------------------------------+
| Field         | Required|  Type       |  Description                                                                                                         |
+===============+=========+=============+======================================================================================================================+
| ``tableName`` | required| string      | Presto table name defined by this file.                                                                              |
+---------------+---------+-------------+----------------------------------------------------------------------------------------------------------------------+
| ``schemaName``| optional| string      | Schema which will contain the table. If omitted, the default schema name is used.                                    |
+---------------+---------+-------------+----------------------------------------------------------------------------------------------------------------------+
| ``streamName``| required| string      | Name of the Kinesis Stream that is mapped                                                                            |
+---------------+---------+-------------+----------------------------------------------------------------------------------------------------------------------+
| ``message``   | optional| JSON object | Field definitions for data columns mapped to the message itself.                                                     |
+---------------+---------+-------------+----------------------------------------------------------------------------------------------------------------------+

Every message in a Kinesis stream can be decoded using the definition provided in the message object. The json object message in the table definition contains two fields.

+---------------+---------+------------+----------------------------------------------------------------------------------------------------------------------+
| Field         | Required|  Type      |  Description                                                                                                         |
+===============+=========+============+======================================================================================================================+
| ``dataFormat``| required|  string    | Selects the decoder for this group of fields.                                                                        |
+---------------+---------+------------+----------------------------------------------------------------------------------------------------------------------+
| ``fields``    | required| JSON array | A list of field definitions. Each field definition creates a new column in the Presto table.                         |
+---------------+---------+------------+----------------------------------------------------------------------------------------------------------------------+

Each field definition is a JSON object. At a minimum, you'll want to provide a name, type, and a mapping. The overall structure looks like this.

.. sourcecode:: bash

    {
        "name": ...,
        "type": ...,
        "dataFormat": ...,
        "mapping": ...,
        "formatHint": ...,
        "hidden": ...,
        "comment": ...
    }

+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+
| Field         | Required|  Type   |  Description                                                                                                         |
+===============+=========+=========+======================================================================================================================+
| ``name``      | required| string  | Name of the column in the Presto table.                                                                              |
+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+
| ``type``      | required| string  | Presto type of the column.                                                                                           |
+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+
| ``dataFormat``| optional| string  | Selects the column decoder for this field. Defaults to the default decoder for this row data format and column type. |
+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+
| ``mapping``   | optional| string  | Mapping information for the column. This is decoder specific, see below.                                             |
+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+
| ``formatHint``| optional| string  | Sets a column specific format hint to the column decoder.                                                            |
+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+
| ``hidden``    | optional| boolean |  Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.                            |
+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+
| ``comment``   | optional| string  |  Adds a column comment which is shown with ``DESCRIBE <table name>``.                                                |
+---------------+---------+---------+----------------------------------------------------------------------------------------------------------------------+

The name field is exposed to presto as the column name, while the mapping field is the portion of the message that gets
mapped to that column. For JSON object messages, this refers to the field name of an object, and can be a path that drills
into the object structure of the message. Additionally, you can map a field of the JSON object to a string column type,
and if it is a more complex type (JSON array or JSON object) then the JSON itself will become the field value.

There is no limit on field descriptions for either key or message.

Developer Setup
---------------
Add the following system properties to run the test cases.

* kinesis.awsAccessKey
* kinesis.awsSecretKey
* kinesis.tableDescriptionS3

  .. sourcecode:: bash

                    <systemPropertyVariables>
                        <kinesis.awsAccessKey>NONE</kinesis.awsAccessKey>
                        <kinesis.awsSecretKey>NONE</kinesis.awsSecretKey>

                        <kinesis.tableDescriptionS3>s3://sample-bucket/unit-test/presto-kinesis</kinesis.tableDescriptionS3>

                    </systemPropertyVariables>