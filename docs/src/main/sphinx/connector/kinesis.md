# Kinesis connector

```{raw} html
<img src="../_static/img/kinesis.png" class="connector-logo">
```

[Kinesis](https://aws.amazon.com/kinesis/) is Amazon's fully managed cloud-based service for real-time processing of large, distributed data streams.

This connector allows the use of Kinesis streams as tables in Trino, such that each data-blob/message
in a Kinesis stream is presented as a row in Trino. A flexible table mapping approach lets us
treat fields of the messages as columns in the table.

Under the hood, a Kinesis
[shard iterator](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html)
is used to retrieve the records, along with a series of
[GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) calls.
The shard iterator starts by default 24 hours before the current time, and works its way forward.
To be able to query a stream, table mappings are needed. These table definitions can be
stored on Amazon S3 (preferred), or stored in a local directory on each Trino node.

This connector is a **read-only** connector. It can only fetch data from Kinesis streams,
but cannot create streams or push data into existing streams.

To configure the Kinesis connector, create a catalog properties file
`etc/catalog/example.properties` with the following contents, replacing the
properties as appropriate:

```text
connector.name=kinesis
kinesis.access-key=XXXXXX
kinesis.secret-key=XXXXXX
```

## Configuration properties

The following configuration properties are available:

| Property name                                | Description                                                           |
| -------------------------------------------- | --------------------------------------------------------------------- |
| `kinesis.access-key`                         | Access key to AWS account or blank to use default provider chain      |
| `kinesis.secret-key`                         | Secret key to AWS account or blank to use default provider chain      |
| `kinesis.aws-region`                         | AWS region to be used to read kinesis stream from                     |
| `kinesis.default-schema`                     | Default schema name for tables                                        |
| `kinesis.table-description-location`         | Directory containing table description files                          |
| `kinesis.table-description-refresh-interval` | How often to get the table description from S3                        |
| `kinesis.hide-internal-columns`              | Controls whether internal columns are part of the table schema or not |
| `kinesis.batch-size`                         | Maximum number of records to return in one batch                      |
| `kinesis.fetch-attempts`                     | Read attempts made when no records returned and not caught up         |
| `kinesis.max-batches`                        | Maximum batches to read from Kinesis in one single query              |
| `kinesis.sleep-time`                         | Time for thread to sleep waiting to make next attempt to fetch batch  |
| `kinesis.iterator-from-timestamp`            | Begin iterating from a given timestamp instead of the trim horizon    |
| `kinesis.iterator-offset-seconds`            | Number of seconds before current time to start iterating              |

### `kinesis.access-key`

Defines the access key ID for AWS root account or IAM roles, which is used to sign programmatic requests to AWS Kinesis.

This property is optional; if not defined, the connector tries to follow `Default-Credential-Provider-Chain` provided by AWS in the following order:

- Environment Variable: Load credentials from environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- Java System Variable: Load from java system as `aws.accessKeyId` and `aws.secretKey`.
- Profile Credentials File: Load from file typically located at `~/.aws/credentials`.
- Instance profile credentials: These credentials can be used on EC2 instances, and are delivered through the Amazon EC2 metadata service.

### `kinesis.secret-key`

Defines the secret key for AWS root account or IAM roles, which together with Access Key ID, is used to sign programmatic requests to AWS Kinesis.

This property is optional; if not defined, connector will try to follow `Default-Credential-Provider-Chain` same as above.

### `kinesis.aws-region`

Defines AWS Kinesis regional endpoint. Selecting appropriate region may reduce latency in fetching data.

This field is optional; The default region is `us-east-1` referring to end point 'kinesis.us-east-1.amazonaws.com'.

See [Kinesis Data Streams regions](https://docs.aws.amazon.com/general/latest/gr/rande.html#ak_region)
for a current list of available regions.

### `kinesis.default-schema`

Defines the schema which contains all tables that were defined without a qualifying schema name.

This property is optional; the default is `default`.

### `kinesis.table-description-location`

References an S3 URL or a folder within Trino deployment that holds one or more JSON files ending with `.json`, which contain table description files.
The S3 bucket and folder will be checked every 10 minutes for updates and changed files.

This property is optional; the default is `etc/kinesis`.

### `kinesis.table-description-refresh-interval`

This property controls how often the table description is refreshed from S3.

This property is optional; the default is `10m`.

### `kinesis.batch-size`

Defines the maximum number of records to return in one request to Kinesis Streams. Maximum limit is `10000` records.

This field is optional; the default value is `10000`.

### `kinesis.max-batches`

The maximum number of batches to read in a single query. The default value is `1000`.

### `kinesis.fetch-attempts`

Defines the number of attempts made to read a batch from Kinesis Streams, when no records are returned and the *millis behind latest*
parameter shows we are not yet caught up. When records are returned no additional attempts are necessary.
`GetRecords` has been observed to return no records even though the shard is not empty.
That is why multiple attempts need to be made.

This field is optional; the default value is `2`.

### `kinesis.sleep-time`

Defines the duration for which a thread needs to sleep between `kinesis.fetch-attempts` made to fetch data.

This field is optional; the default value is `1000ms`.

### `kinesis.iterator-from-timestamp`

Use an initial shard iterator type of `AT_TIMESTAMP` starting `kinesis.iterator-offset-seconds` before the current time.
When this is false, an iterator type of `TRIM_HORIZON` is used, meaning it starts from the oldest record in the stream.

The default is true.

### `kinesis.iterator-offset-seconds`

When `kinesis.iterator-from-timestamp` is true, the shard iterator starts at `kinesis.iterator-offset-seconds` before the current time.

The default is `86400` seconds (24 hours).

### `kinesis.hide-internal-columns`

In addition to the data columns defined in a table description file, the connector maintains a number of additional columns for each table.
If these columns are hidden, they can still be used in queries, but they do not show up in `DESCRIBE <table-name>` or `SELECT *`.

This property is optional; the default is true.

## Internal columns

For each defined table, the connector maintains the following columns:

| Column name          | Type        | Description                                                                                                                                                                         |
| -------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `_shard_id`          | `VARCHAR`   | ID of the Kinesis stream shard which contains this row.                                                                                                                             |
| `_shard_sequence_id` | `VARCHAR`   | Sequence id within the Kinesis shard for this row.                                                                                                                                  |
| `_segment_start`     | `BIGINT`    | Lowest offset in the segment (inclusive) which contains this row. This offset is partition specific.                                                                                |
| `_segment_end`       | `BIGINT`    | Highest offset in the segment (exclusive) which contains this row. The offset is partition specific. This is the same value as `_segment_start` of the next segment (if it exists). |
| `_segment_count`     | `BIGINT`    | Running count for the current row within the segment. For an uncompacted topic, `_segment_start + _segment_count` is equal to `_partition_offset`.                                  |
| `_message_valid`     | `BOOLEAN`   | True if the decoder could decode the message successfully for this row. When false, data columns mapped from the message should be treated as invalid.                              |
| `_message`           | `VARCHAR`   | Message bytes as an UTF-8 encoded string. This is only useful for a text topic.                                                                                                     |
| `_message_length`    | `BIGINT`    | Number of bytes in the message.                                                                                                                                                     |
| `_message_timestamp` | `TIMESTAMP` | Approximate arrival time of the message (milliseconds granularity).                                                                                                                 |
| `_key`               | `VARCHAR`   | Key bytes as an UTF-8 encoded string. This is only useful for textual keys.                                                                                                         |
| `_partition_key`     | `VARCHAR`   | Partition Key bytes as a UTF-8 encoded string.                                                                                                                                      |

For tables without a table definition file, the `_message_valid` column is always `true`.

## Table definition

A table definition file consists of a JSON definition for a table, which corresponds to one stream in Kinesis.
The name of the file can be arbitrary but must end in `.json`. The structure of the table definition is as follows:

```text
{
      "tableName": ...,
      "schemaName": ...,
      "streamName": ...,
      "message": {
          "dataFormat": ...,
          "fields": [
              ...
         ]
      }
  }
```

| Field        | Required | Type        | Description                                                                   |
| ------------ | -------- | ----------- | ----------------------------------------------------------------------------- |
| `tableName`  | required | string      | Trino table name defined by this file.                                        |
| `schemaName` | optional | string      | Schema which contains the table. If omitted, the default schema name is used. |
| `streamName` | required | string      | Name of the Kinesis Stream that is mapped                                     |
| `message`    | optional | JSON object | Field definitions for data columns mapped to the message itself.              |

Every message in a Kinesis stream can be decoded using the definition provided in the message object.
The JSON object message in the table definition contains two fields:

| Field        | Required | Type       | Description                                                                                 |
| ------------ | -------- | ---------- | ------------------------------------------------------------------------------------------- |
| `dataFormat` | required | string     | Selects the decoder for this group of fields.                                               |
| `fields`     | required | JSON array | A list of field definitions. Each field definition creates a new column in the Trino table. |

Each field definition is a JSON object. At a minimum, a name, type, and mapping must be provided.
The overall structure looks like this:

```text
{
    "name": ...,
    "type": ...,
    "dataFormat": ...,
    "mapping": ...,
    "formatHint": ...,
    "hidden": ...,
    "comment": ...
}
```

| Field        | Required | Type    | Description                                                                                                          |
| ------------ | -------- | ------- | -------------------------------------------------------------------------------------------------------------------- |
| `name`       | required | string  | Name of the column in the Trino table.                                                                               |
| `type`       | required | string  | Trino type of the column.                                                                                            |
| `dataFormat` | optional | string  | Selects the column decoder for this field. Defaults to the default decoder for this row data format and column type. |
| `mapping`    | optional | string  | Mapping information for the column. This is decoder specific -- see below.                                           |
| `formatHint` | optional | string  | Sets a column specific format hint to the column decoder.                                                            |
| `hidden`     | optional | boolean | Hides the column from `DESCRIBE <table name>` and `SELECT *`. Defaults to `false`.                                   |
| `comment`    | optional | string  | Adds a column comment which is shown with `DESCRIBE <table name>`.                                                   |

The name field is exposed to Trino as the column name, while the mapping field is the portion of the message that gets
mapped to that column. For JSON object messages, this refers to the field name of an object, and can be a path that drills
into the object structure of the message. Additionally, you can map a field of the JSON object to a string column type,
and if it is a more complex type (JSON array or JSON object) then the JSON itself becomes the field value.

There is no limit on field descriptions for either key or message.

(kinesis-type-mapping)=

## Type mapping

Because Trino and Kinesis each support types that the other does not, this
connector {ref}`maps some types <type-mapping-overview>` when reading data. Type
mapping depends on the RAW, CSV, JSON, and AVRO file formats.

### Row decoding

A decoder is used to map data to table columns.

The connector contains the following decoders:

- `raw`: Message is not interpreted; ranges of raw message bytes are mapped
  to table columns.
- `csv`: Message is interpreted as comma separated message, and fields are
  mapped to table columns.
- `json`: Message is parsed as JSON, and JSON fields are mapped to table
  columns.
- `avro`: Message is parsed based on an Avro schema, and Avro fields are
  mapped to table columns.

:::{note}
If no table definition file exists for a table, the `dummy` decoder is
used, which does not expose any columns.
:::

```{include} raw-decoder.fragment
```

```{include} csv-decoder.fragment
```

```{include} json-decoder.fragment
```

```{include} avro-decoder.fragment
```

(kinesis-sql-support)=

## SQL support

The connector provides {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements to access data and
metadata from Kinesis streams.
