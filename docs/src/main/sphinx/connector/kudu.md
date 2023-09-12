# Kudu connector

```{raw} html
<img src="../_static/img/kudu.png" class="connector-logo">
```

The Kudu connector allows querying, inserting and deleting data in [Apache Kudu].

## Requirements

To connect to Kudu, you need:

- Kudu version 1.13.0 or higher.
- Network access from the Trino coordinator and workers to Kudu. Port 7051 is
  the default port.

## Configuration

To configure the Kudu connector, create a catalog properties file
`etc/catalog/kudu.properties` with the following contents,
replacing the properties as appropriate:

```properties
connector.name=kudu

## Defaults to NONE
kudu.authentication.type = NONE

## List of Kudu master addresses, at least one is needed (comma separated)
## Supported formats: example.com, example.com:7051, 192.0.2.1, 192.0.2.1:7051,
##                    [2001:db8::1], [2001:db8::1]:7051, 2001:db8::1
kudu.client.master-addresses=localhost

## Kudu does not support schemas, but the connector can emulate them optionally.
## By default, this feature is disabled, and all tables belong to the default schema.
## For more details see connector documentation.
#kudu.schema-emulation.enabled=false

## Prefix to use for schema emulation (only relevant if `kudu.schema-emulation.enabled=true`)
## The standard prefix is `presto::`. Empty prefix is also supported.
## For more details see connector documentation.
#kudu.schema-emulation.prefix=

###########################################
### Advanced Kudu Java client configuration
###########################################

## Default timeout used for administrative operations (e.g. createTable, deleteTable, etc.)
#kudu.client.default-admin-operation-timeout = 30s

## Default timeout used for user operations
#kudu.client.default-operation-timeout = 30s

## Disable Kudu client's collection of statistics.
#kudu.client.disable-statistics = false

## Assign Kudu splits to replica host if worker and kudu share the same cluster
#kudu.allow-local-scheduling = false
```

## Kerberos support

In order to connect to a kudu cluster that uses `kerberos`
authentication, you need to configure the following kudu properties:

```properties
kudu.authentication.type = KERBEROS

## The kerberos client principal name
kudu.authentication.client.principal = clientprincipalname

## The path to the kerberos keytab file
## The configured client principal must exist in this keytab file
kudu.authentication.client.keytab = /path/to/keytab/file.keytab

## The path to the krb5.conf kerberos config file
kudu.authentication.config = /path/to/kerberos/krb5.conf

## Optional and defaults to "kudu"
## If kudu is running with a custom SPN this needs to be configured
kudu.authentication.server.principal.primary = kudu
```

## Querying data

Apache Kudu does not support schemas, i.e. namespaces for tables.
The connector can optionally emulate schemas by table naming conventions.

### Default behaviour (without schema emulation)

The emulation of schemas is disabled by default.
In this case all Kudu tables are part of the `default` schema.

For example, a Kudu table named `orders` can be queried in Trino
with `SELECT * FROM example.default.orders` or simple with `SELECT * FROM orders`
if catalog and schema are set to `kudu` and `default` respectively.

Table names can contain any characters in Kudu. In this case, use double quotes.
E.g. To query a Kudu table named `special.table!` use `SELECT * FROM example.default."special.table!"`.

#### Example

- Create a users table in the default schema:

  ```
  CREATE TABLE example.default.users (
    user_id int WITH (primary_key = true),
    first_name VARCHAR,
    last_name VARCHAR
  ) WITH (
    partition_by_hash_columns = ARRAY['user_id'],
    partition_by_hash_buckets = 2
  );
  ```

  On creating a Kudu table you must/can specify additional information about
  the primary key, encoding, and compression of columns and hash or range
  partitioning. For details see the {ref}`kudu-create-table` section.

- Describe the table:

  ```
  DESCRIBE example.default.users;
  ```

  ```text
     Column   |  Type   |                      Extra                      | Comment
  ------------+---------+-------------------------------------------------+---------
   user_id    | integer | primary_key, encoding=auto, compression=default |
   first_name | varchar | nullable, encoding=auto, compression=default    |
   last_name  | varchar | nullable, encoding=auto, compression=default    |
  (3 rows)
  ```

- Insert some data:

  ```
  INSERT INTO example.default.users VALUES (1, 'Donald', 'Duck'), (2, 'Mickey', 'Mouse');
  ```

- Select the inserted data:

  ```
  SELECT * FROM example.default.users;
  ```

(behavior-with-schema-emulation)=

### Behavior with schema emulation

If schema emulation has been enabled in the connector properties, i.e.
`etc/catalog/example.properties`, tables are mapped to schemas depending on
some conventions.

- With `kudu.schema-emulation.enabled=true` and `kudu.schema-emulation.prefix=`,
  the mapping works like:

  | Kudu table name | Trino qualified name  |
  | --------------- | --------------------- |
  | `orders`        | `kudu.default.orders` |
  | `part1.part2`   | `kudu.part1.part2`    |
  | `x.y.z`         | `kudu.x."y.z"`        |

  As schemas are not directly supported by Kudu, a special table named
  `$schemas` is created for managing the schemas.

- With `kudu.schema-emulation.enabled=true` and `kudu.schema-emulation.prefix=presto::`,
  the mapping works like:

  | Kudu table name       | Trino qualified name         |
  | --------------------- | ---------------------------- |
  | `orders`              | `kudu.default.orders`        |
  | `part1.part2`         | `kudu.default."part1.part2"` |
  | `x.y.z`               | `kudu.default."x.y.z"`       |
  | `presto::part1.part2` | `kudu.part1.part2`           |
  | `presto:x.y.z`        | `kudu.x."y.z"`               |

  As schemas are not directly supported by Kudu, a special table named
  `presto::$schemas` is created for managing the schemas.

(kudu-type-mapping)=

## Type mapping

Because Trino and Kudu each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Kudu type to Trino type mapping

The connector maps Kudu types to the corresponding Trino types following
this table:

```{eval-rst}
.. list-table:: Kudu type to Trino type mapping
  :widths: 30, 20
  :header-rows: 1

  * - Kudu type
    - Trino type
  * - ``BOOL``
    - ``BOOLEAN``
  * - ``INT8``
    - ``TINYINT``
  * - ``INT16``
    - ``SMALLINT``
  * - ``INT32``
    - ``INTEGER``
  * - ``INT64``
    - ``BIGINT``
  * - ``FLOAT``
    - ``REAL``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
  * - ``STRING``
    - ``VARCHAR``
  * - ``BINARY``
    - ``VARBINARY``
  * - ``UNIXTIME_MICROS``
    - ``TIMESTAMP(3)``
```

No other types are supported.

### Trino type to Kudu type mapping

The connector maps Trino types to the corresponding Kudu types following
this table:

```{eval-rst}
.. list-table:: Trino type to Kudu type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - Trino type
    - Kudu type
    - Notes
  * - ``BOOLEAN``
    - ``BOOL``
    -
  * - ``TINYINT``
    - ``INT8``
    -
  * - ``SMALLINT``
    - ``INT16``
    -
  * - ``INTEGER``
    - ``INT32``
    -
  * - ``BIGINT``
    - ``INT64``
    -
  * - ``REAL``
    - ``FLOAT``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``DECIMAL(p,s)``
    - ``DECIMAL(p,s)``
    - Only supported for Kudu server >= 1.7.0
  * - ``VARCHAR``
    - ``STRING``
    - The optional maximum length is lost
  * - ``VARBINARY``
    - ``BINARY``
    -
  * - ``DATE``
    - ``STRING``
    -
  * - ``TIMESTAMP(3)``
    - ``UNIXTIME_MICROS``
    - µs resolution in Kudu column is reduced to ms resolution
```

No other types are supported.

(kudu-sql-support)=

## SQL support

The connector provides read and write access to data and metadata in
Kudu. In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`, see also {ref}`kudu-insert`
- {doc}`/sql/delete`
- {doc}`/sql/merge`
- {doc}`/sql/create-table`, see also {ref}`kudu-create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/alter-table`, see also {ref}`kudu-alter-table`
- {doc}`/sql/create-schema`, see also {ref}`kudu-create-schema`
- {doc}`/sql/drop-schema`, see also {ref}`kudu-drop-schema`

(kudu-insert)=

### Inserting into tables

`INSERT INTO ... values` and `INSERT INTO ... select` behave like
`UPSERT`.

```{include} sql-delete-limitation.fragment
```

(kudu-create-schema)=

### Creating schemas

`CREATE SCHEMA` is only allowed if schema emulation is enabled. See the
{ref}`behavior-with-schema-emulation` section.

(kudu-drop-schema)=

### Dropping schemas

`DROP SCHEMA` is only allowed if schema emulation is enabled. See the
{ref}`behavior-with-schema-emulation` section.

(kudu-create-table)=

### Creating a table

On creating a Kudu table, you need to provide the columns and their types, of
course, but Kudu needs information about partitioning and optionally
for column encoding and compression.

Simple Example:

```
CREATE TABLE user_events (
  user_id INTEGER WITH (primary_key = true),
  event_name VARCHAR WITH (primary_key = true),
  message VARCHAR,
  details VARCHAR WITH (nullable = true, encoding = 'plain')
) WITH (
  partition_by_hash_columns = ARRAY['user_id'],
  partition_by_hash_buckets = 5,
  number_of_replicas = 3
);
```

The primary key consists of `user_id` and `event_name`. The table is partitioned into
five partitions by hash values of the column `user_id`, and the `number_of_replicas` is
explicitly set to 3.

The primary key columns must always be the first columns of the column list.
All columns used in partitions must be part of the primary key.

The table property `number_of_replicas` is optional. It defines the
number of tablet replicas, and must be an odd number. If it is not specified,
the default replication factor from the Kudu master configuration is used.

Kudu supports two different kinds of partitioning: hash and range partitioning.
Hash partitioning distributes rows by hash value into one of many buckets.
Range partitions distributes rows using a totally-ordered range partition key.
The concrete range partitions must be created explicitly.
Kudu also supports multi-level partitioning. A table must have at least one
partitioning, either hash or range. It can have at most one range partitioning,
but multiple hash partitioning 'levels'.

For more details see [Partitioning design](kudu-partitioning-design).

(kudu-column-properties)=
### Column properties

Besides column name and type, you can specify some more properties of a column.

| Column property name | Type      | Description                                                                                                                                                                                                                                                                                                                              |
| -------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `primary_key`        | `BOOLEAN` | If `true`, the column belongs to primary key columns. The Kudu primary key enforces a uniqueness constraint. Inserting a second row with the same primary key results in updating the existing row ('UPSERT'). See also [Primary Key Design] in the Kudu documentation.                                                                  |
| `nullable`           | `BOOLEAN` | If `true`, the value can be null. Primary key columns must not be nullable.                                                                                                                                                                                                                                                              |
| `encoding`           | `VARCHAR` | The column encoding can help to save storage space and to improve query performance. Kudu uses an auto encoding depending on the column type if not specified. Valid values are: `'auto'`, `'plain'`, `'bitshuffle'`, `'runlength'`, `'prefix'`, `'dictionary'`, `'group_varint'`. See also [Column encoding] in the Kudu documentation. |
| `compression`        | `VARCHAR` | The encoded column values can be compressed. Kudu uses a default compression if not specified. Valid values are: `'default'`, `'no'`, `'lz4'`, `'snappy'`, `'zlib'`. See also [Column compression] in the Kudu documentation.                                                                                                            |

Example:

```sql
CREATE TABLE example_table (
  name VARCHAR WITH (primary_key = true, encoding = 'dictionary', compression = 'snappy'),
  index BIGINT WITH (nullable = true, encoding = 'runlength', compression = 'lz4'),
  comment VARCHAR WITH (nullable = true, encoding = 'plain', compression = 'default'),
   ...
) WITH (...);
```

(kudu-alter-table)=

### Changing tables

Adding a column to an existing table uses the SQL statement `ALTER TABLE ... ADD COLUMN ...`.
You can specify the same column properties as on creating a table.

Example:

```
ALTER TABLE example_table ADD COLUMN extraInfo VARCHAR WITH (nullable = true, encoding = 'plain')
```

See also [Column properties](kudu-column-properties).

`ALTER TABLE ... RENAME COLUMN` is only allowed if not part of a primary key.

`ALTER TABLE ... DROP COLUMN` is only allowed if not part of a primary key.

## Procedures

- `CALL example.system.add_range_partition` see {ref}`managing-range-partitions`
- `CALL example.system.drop_range_partition` see {ref}`managing-range-partitions`


(kudu-partitioning-design)=
### Partitioning design

A table must have at least one partitioning (either hash or range).
It can have at most one range partitioning, but multiple hash partitioning 'levels'.
For more details see Apache Kudu documentation: [Partitioning].

If you create a Kudu table in Trino, the partitioning design is given by
several table properties.

#### Hash partitioning

You can provide the first hash partition group with two table properties:

The `partition_by_hash_columns` defines the column(s) belonging to the
partition group and `partition_by_hash_buckets` the number of partitions to
split the hash values range into. All partition columns must be part of the
primary key.

Example:

```
CREATE TABLE example_table (
  col1 VARCHAR WITH (primary_key=true),
  col2 VARCHAR WITH (primary_key=true),
  ...
) WITH (
  partition_by_hash_columns = ARRAY['col1', 'col2'],
  partition_by_hash_buckets = 4
)
```

This defines a hash partitioning with the columns `col1` and `col2`
distributed over 4 partitions.

To define two separate hash partition groups, also use the second pair
of table properties named `partition_by_second_hash_columns` and
`partition_by_second_hash_buckets`.

Example:

```
CREATE TABLE example_table (
  col1 VARCHAR WITH (primary_key=true),
  col2 VARCHAR WITH (primary_key=true),
  ...
) WITH (
  partition_by_hash_columns = ARRAY['col1'],
  partition_by_hash_buckets = 2,
  partition_by_second_hash_columns = ARRAY['col2'],
  partition_by_second_hash_buckets = 3
)
```

This defines a two-level hash partitioning, with the first hash partition group
over the column `col1` distributed over 2 buckets, and the second
hash partition group over the column `col2` distributed over 3 buckets.
As a result you have table with 2 x 3 = 6 partitions.

#### Range partitioning

You can provide at most one range partitioning in Apache Kudu. The columns
are defined with the table property `partition_by_range_columns`.
The ranges themselves are given either in the
table property `range_partitions` on creating the table.
Or alternatively, the procedures `kudu.system.add_range_partition` and
`kudu.system.drop_range_partition` can be used to manage range
partitions for existing tables. For both ways see below for more
details.

Example:

```
CREATE TABLE events (
  rack VARCHAR WITH (primary_key=true),
  machine VARCHAR WITH (primary_key=true),
  event_time TIMESTAMP WITH (primary_key=true),
  ...
) WITH (
  partition_by_hash_columns = ARRAY['rack'],
  partition_by_hash_buckets = 2,
  partition_by_second_hash_columns = ARRAY['machine'],
  partition_by_second_hash_buckets = 3,
  partition_by_range_columns = ARRAY['event_time'],
  range_partitions = '[{"lower": null, "upper": "2018-01-01T00:00:00"},
                       {"lower": "2018-01-01T00:00:00", "upper": null}]'
)
```

This defines a tree-level partitioning with two hash partition groups and
one range partitioning on the `event_time` column.
Two range partitions are created with a split at “2018-01-01T00:00:00”.

### Table property `range_partitions`

With the `range_partitions` table property you specify the concrete
range partitions to be created. The range partition definition itself
must be given in the table property `partition_design` separately.

Example:

```
CREATE TABLE events (
  serialno VARCHAR WITH (primary_key = true),
  event_time TIMESTAMP WITH (primary_key = true),
  message VARCHAR
) WITH (
  partition_by_hash_columns = ARRAY['serialno'],
  partition_by_hash_buckets = 4,
  partition_by_range_columns = ARRAY['event_time'],
  range_partitions = '[{"lower": null, "upper": "2017-01-01T00:00:00"},
                       {"lower": "2017-01-01T00:00:00", "upper": "2017-07-01T00:00:00"},
                       {"lower": "2017-07-01T00:00:00", "upper": "2018-01-01T00:00:00"}]'
);
```

This creates a table with a hash partition on column `serialno` with 4
buckets and range partitioning on column `event_time`. Additionally,
three range partitions are created:

1. for all event_times before the year 2017, lower bound = `null` means it is unbound
2. for the first half of the year 2017
3. for the second half the year 2017

This means any attempt to add rows with `event_time` of year 2018 or greater fails, as no partition is defined.
The next section shows how to define a new range partition for an existing table.

(managing-range-partitions)=

#### Managing range partitions

For existing tables, there are procedures to add and drop a range
partition.

- adding a range partition

  ```sql
  CALL example.system.add_range_partition(<schema>, <table>, <range_partition_as_json_string>)
  ```

- dropping a range partition

  ```sql
  CALL example.system.drop_range_partition(<schema>, <table>, <range_partition_as_json_string>)
  ```

  - `<schema>`: schema of the table

  - `<table>`: table names

  - `<range_partition_as_json_string>`: lower and upper bound of the
    range partition as JSON string in the form
    `'{"lower": <value>, "upper": <value>}'`, or if the range partition
    has multiple columns:
    `'{"lower": [<value_col1>,...], "upper": [<value_col1>,...]}'`. The
    concrete literal for lower and upper bound values are depending on
    the column types.

    Examples:

    | Trino data Type | JSON string example                                                          |
    | --------------- | ---------------------------------------------------------------------------- |
    | `BIGINT`        | `‘{“lower”: 0, “upper”: 1000000}’`                                           |
    | `SMALLINT`      | `‘{“lower”: 10, “upper”: null}’`                                             |
    | `VARCHAR`       | `‘{“lower”: “A”, “upper”: “M”}’`                                             |
    | `TIMESTAMP`     | `‘{“lower”: “2018-02-01T00:00:00.000”, “upper”: “2018-02-01T12:00:00.000”}’` |
    | `BOOLEAN`       | `‘{“lower”: false, “upper”: true}’`                                          |
    | `VARBINARY`     | values encoded as base64 strings                                             |

    To specified an unbounded bound, use the value `null`.

Example:

```
CALL example.system.add_range_partition('example_schema', 'events', '{"lower": "2018-01-01", "upper": "2018-06-01"}')
```

This adds a range partition for a table `events` in the schema
`example_schema` with the lower bound `2018-01-01`, more exactly
`2018-01-01T00:00:00.000`, and the upper bound `2018-07-01`.

Use the SQL statement `SHOW CREATE TABLE` to query the existing
range partitions (they are shown in the table property
`range_partitions`).

## Limitations

- Only lower case table and column names in Kudu are supported.

[apache kudu]: https://kudu.apache.org/
[column compression]: https://kudu.apache.org/docs/schema_design.html#compression
[column encoding]: https://kudu.apache.org/docs/schema_design.html#encoding
[partitioning]: https://kudu.apache.org/docs/schema_design.html#partitioning
[primary key design]: http://kudu.apache.org/docs/schema_design.html#primary-keys
