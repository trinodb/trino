# Accumulo connector

```{raw} html
<img src="../_static/img/accumulo.png" class="connector-logo">
```

The Accumulo connector supports reading and writing data from
[Apache Accumulo](https://accumulo.apache.org/).
Please read this page thoroughly to understand the capabilities and features of the connector.

## Installing the iterator dependency

The Accumulo connector uses custom Accumulo iterators in
order to push various information in SQL predicate clauses to Accumulo for
server-side filtering, known as *predicate pushdown*. In order
for the server-side iterators to work, you need to add the `trino-accumulo-iterators`
JAR file to Accumulo's `lib/ext` directory on each TabletServer node.

```bash
# For each TabletServer node:
scp $TRINO_HOME/plugins/accumulo/trino-accumulo-iterators-*.jar [tabletserver_address]:$ACCUMULO_HOME/lib/ext

# TabletServer should pick up new JAR files in ext directory, but may require restart
```

## Requirements

To connect to Accumulo, you need:

- Accumulo versions 1.x starting with 1.7.4. Versions 2.x are not supported.
- Network access from the Trino coordinator and workers to the Accumulo
  Zookeeper server. Port 2181 is the default port.

## Connector configuration

Create `etc/catalog/example.properties` to mount the `accumulo` connector as
the `example` catalog, with the following connector properties as appropriate
for your setup:

```text
connector.name=accumulo
accumulo.instance=xxx
accumulo.zookeepers=xxx
accumulo.username=username
accumulo.password=password
```

Replace the `accumulo.xxx` properties as required.

## Configuration variables

| Property name                                | Default value     | Required | Description                                                                      |
| -------------------------------------------- | ----------------- | -------- | -------------------------------------------------------------------------------- |
| `accumulo.instance`                          | (none)            | Yes      | Name of the Accumulo instance                                                    |
| `accumulo.zookeepers`                        | (none)            | Yes      | ZooKeeper connect string                                                         |
| `accumulo.username`                          | (none)            | Yes      | Accumulo user for Trino                                                          |
| `accumulo.password`                          | (none)            | Yes      | Accumulo password for user                                                       |
| `accumulo.zookeeper.metadata.root`           | `/trino-accumulo` | No       | Root znode for storing metadata. Only relevant if using default Metadata Manager |
| `accumulo.cardinality.cache.size`            | `100000`          | No       | Sets the size of the index cardinality cache                                     |
| `accumulo.cardinality.cache.expire.duration` | `5m`              | No       | Sets the expiration duration of the cardinality cache.                           |

## Usage

Simply begin using SQL to create a new table in Accumulo to begin
working with data. By default, the first column of the table definition
is set to the Accumulo row ID. This should be the primary key of your
table, and keep in mind that any `INSERT` statements containing the same
row ID is effectively an UPDATE as far as Accumulo is concerned, as any
previous data in the cell is overwritten. The row ID can be
any valid Trino datatype. If the first column is not your primary key, you
can set the row ID column using the `row_id` table property within the `WITH`
clause of your table definition.

Simply issue a `CREATE TABLE` statement to create a new Trino/Accumulo table:

```
CREATE TABLE example_schema.scientists (
  recordkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
);
```

```sql
DESCRIBE example_schema.scientists;
```

```text
  Column   |  Type   | Extra |                      Comment
-----------+---------+-------+---------------------------------------------------
 recordkey | varchar |       | Accumulo row ID
 name      | varchar |       | Accumulo column name:name. Indexed: false
 age       | bigint  |       | Accumulo column age:age. Indexed: false
 birthday  | date    |       | Accumulo column birthday:birthday. Indexed: false
```

This command creates a new Accumulo table with the `recordkey` column
as the Accumulo row ID. The name, age, and birthday columns are mapped to
auto-generated column family and qualifier values (which, in practice,
are both identical to the Trino column name).

When creating a table using SQL, you can optionally specify a
`column_mapping` table property. The value of this property is a
comma-delimited list of triples, Trino column **:** Accumulo column
family **:** accumulo column qualifier, with one triple for every
non-row ID column. This sets the mapping of the Trino column name to
the corresponding Accumulo column family and column qualifier.

If you don't specify the `column_mapping` table property, then the
connector auto-generates column names (respecting any configured locality groups).
Auto-generation of column names is only available for internal tables, so if your
table is external you must specify the column_mapping property.

For a full list of table properties, see [Table Properties](accumulo-table-properties).

For example:

```sql
CREATE TABLE example_schema.scientists (
  recordkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date'
);
```

```sql
DESCRIBE example_schema.scientists;
```

```text
  Column   |  Type   | Extra |                    Comment
-----------+---------+-------+-----------------------------------------------
 recordkey | varchar |       | Accumulo row ID
 name      | varchar |       | Accumulo column metadata:name. Indexed: false
 age       | bigint  |       | Accumulo column metadata:age. Indexed: false
 birthday  | date    |       | Accumulo column metadata:date. Indexed: false
```

You can then issue `INSERT` statements to put data into Accumulo.

:::{note}
While issuing `INSERT` statements is convenient,
this method of loading data into Accumulo is low-throughput. You want
to use the Accumulo APIs to write `Mutations` directly to the tables.
See the section on [Loading Data](accumulo-loading-data) for more details.
:::

```sql
INSERT INTO example_schema.scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );
```

```sql
SELECT * FROM example_schema.scientists;
```

```text
 recordkey |     name     | age |  birthday
-----------+--------------+-----+------------
 row1      | Grace Hopper | 109 | 1906-12-09
 row2      | Alan Turing  | 103 | 1912-06-23
(2 rows)
```

As you'd expect, rows inserted into Accumulo via the shell or
programmatically will also show up when queried. (The Accumulo shell
thinks "-5321" is an option and not a number... so we'll just make TBL a
little younger.)

```bash
$ accumulo shell -u root -p secret
root@default> table example_schema.scientists
root@default example_schema.scientists> insert row3 metadata name "Tim Berners-Lee"
root@default example_schema.scientists> insert row3 metadata age 60
root@default example_schema.scientists> insert row3 metadata date 5321
```

```sql
SELECT * FROM example_schema.scientists;
```

```text
 recordkey |      name       | age |  birthday
-----------+-----------------+-----+------------
 row1      | Grace Hopper    | 109 | 1906-12-09
 row2      | Alan Turing     | 103 | 1912-06-23
 row3      | Tim Berners-Lee |  60 | 1984-07-27
(3 rows)
```

You can also drop tables using `DROP TABLE`. This command drops both
metadata and the tables. See the below section on [External
Tables](accumulo-external-tables) for more details on internal and external
tables.

```sql
DROP TABLE example_schema.scientists;
```

## Indexing columns

Internally, the connector creates an Accumulo `Range` and packs it in
a split. This split gets passed to a Trino Worker to read the data from
the `Range` via a `BatchScanner`. When issuing a query that results
in a full table scan, each Trino Worker gets a single `Range` that
maps to a single tablet of the table. When issuing a query with a
predicate (i.e. `WHERE x = 10` clause), Trino passes the values
within the predicate (`10`) to the connector so it can use this
information to scan less data. When the Accumulo row ID is used as part
of the predicate clause, this narrows down the `Range` lookup to quickly
retrieve a subset of data from Accumulo.

But what about the other columns? If you're frequently querying on
non-row ID columns, you should consider using the **indexing**
feature built into the Accumulo connector. This feature can drastically
reduce query runtime when selecting a handful of values from the table,
and the heavy lifting is done for you when loading data via Trino
`INSERT` statements. Keep in mind writing data to Accumulo via
`INSERT` does not have high throughput.

To enable indexing, add the `index_columns` table property and specify
a comma-delimited list of Trino column names you wish to index (we use the
`string` serializer here to help with this example -- you
should be using the default `lexicoder` serializer).

```sql
CREATE TABLE example_schema.scientists (
  recordkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  serializer = 'string',
  index_columns='name,age,birthday'
);
```

After creating the table, we see there are an additional two Accumulo
tables to store the index and metrics.

```text
root@default> tables
accumulo.metadata
accumulo.root
example_schema.scientists
example_schema.scientists_idx
example_schema.scientists_idx_metrics
trace
```

After inserting data, we can look at the index table and see there are
indexed values for the name, age, and birthday columns. The connector
queries this index table

```sql
INSERT INTO example_schema.scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09'),
('row2', 'Alan Turing', 103, DATE '1912-06-23');
```

```text
root@default> scan -t example_schema.scientists_idx
-21011 metadata_date:row2 []
-23034 metadata_date:row1 []
103 metadata_age:row2 []
109 metadata_age:row1 []
Alan Turing metadata_name:row2 []
Grace Hopper metadata_name:row1 []
```

When issuing a query with a `WHERE` clause against indexed columns,
the connector searches the index table for all row IDs that contain the
value within the predicate. These row IDs are bundled into a Trino
split as single-value `Range` objects, the number of row IDs per split
is controlled by the value of `accumulo.index_rows_per_split`, and
passed to a Trino worker to be configured in the `BatchScanner` which
scans the data table.

```sql
SELECT * FROM example_schema.scientists WHERE age = 109;
```

```text
 recordkey |     name     | age |  birthday
-----------+--------------+-----+------------
 row1      | Grace Hopper | 109 | 1906-12-09
(1 row)
```

(accumulo-loading-data)=
## Loading data

The Accumulo connector supports loading data via INSERT statements, however
this method tends to be low-throughput and should not be relied on when
throughput is a concern.

(accumulo-external-tables)=
## External tables

By default, the tables created using SQL statements via Trino are
*internal* tables, that is both the Trino table metadata and the
Accumulo tables are managed by Trino. When you create an internal
table, the Accumulo table is created as well. You receive an error
if the Accumulo table already exists. When an internal table is dropped
via Trino, the Accumulo table, and any index tables, are dropped as
well.

To change this behavior, set the `external` property to `true` when
issuing the `CREATE` statement. This makes the table an *external*
table, and a `DROP TABLE` command **only** deletes the metadata
associated with the table.  If the Accumulo tables do not already exist,
they are created by the connector.

Creating an external table *will* set any configured locality groups as well
as the iterators on the index and metrics tables, if the table is indexed.
In short, the only difference between an external table and an internal table,
is that the connector deletes the Accumulo tables when a `DROP TABLE` command
is issued.

External tables can be a bit more difficult to work with, as the data is stored
in an expected format. If the data is not stored correctly, then you're
gonna have a bad time. Users must provide a `column_mapping` property
when creating the table. This creates the mapping of Trino column name
to the column family/qualifier for the cell of the table. The value of the
cell is stored in the `Value` of the Accumulo key/value pair. By default,
this value is expected to be serialized using Accumulo's *lexicoder* API.
If you are storing values as strings, you can specify a different serializer
using the `serializer` property of the table. See the section on
[Table Properties](accumulo-table-properties) for more information.

Next, we create the Trino external table.

```sql
CREATE TABLE external_table (
  a VARCHAR,
  b BIGINT,
  c DATE
)
WITH (
  column_mapping = 'a:md:a,b:md:b,c:md:c',
  external = true,
  index_columns = 'b,c',
  locality_groups = 'foo:b,c'
);
```

After creating the table, usage of the table continues as usual:

```sql
INSERT INTO external_table VALUES
('1', 1, DATE '2015-03-06'),
('2', 2, DATE '2015-03-07');
```

```sql
SELECT * FROM external_table;
```

```text
 a | b |     c
---+---+------------
 1 | 1 | 2015-03-06
 2 | 2 | 2015-03-06
(2 rows)
```

```sql
DROP TABLE external_table;
```

After dropping the table, the table still exists in Accumulo because it is *external*.

```text
root@default> tables
accumulo.metadata
accumulo.root
external_table
external_table_idx
external_table_idx_metrics
trace
```

If we wanted to add a new column to the table, we can create the table again and specify a new column.
Any existing rows in the table have a value of NULL. This command re-configures the Accumulo
tables, setting the locality groups and iterator configuration.

```sql
CREATE TABLE external_table (
  a VARCHAR,
  b BIGINT,
  c DATE,
  d INTEGER
)
WITH (
  column_mapping = 'a:md:a,b:md:b,c:md:c,d:md:d',
  external = true,
  index_columns = 'b,c,d',
  locality_groups = 'foo:b,c,d'
);

SELECT * FROM external_table;
```

```sql
 a | b |     c      |  d
---+---+------------+------
 1 | 1 | 2015-03-06 | NULL
 2 | 2 | 2015-03-07 | NULL
(2 rows)
```

(accumulo-table-properties)=
## Table properties

Table property usage example:

```sql
CREATE TABLE example_schema.scientists (
  recordkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
  index_columns = 'name,age'
);
```

| Property name     | Default value  | Description                                                                                                                                                                                                                                                                            |
| ----------------- | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `column_mapping`  | (generated)    | Comma-delimited list of column metadata: `col_name:col_family:col_qualifier,[...]`. Required for external tables.  Not setting this property results in auto-generated column names.                                                                                                   |
| `index_columns`   | (none)         | A comma-delimited list of Trino columns that are indexed in this table's corresponding index table                                                                                                                                                                                     |
| `external`        | `false`        | If true, Trino will only do metadata operations for the table. Otherwise, Trino will create and drop Accumulo tables where appropriate.                                                                                                                                                |
| `locality_groups` | (none)         | List of locality groups to set on the Accumulo table. Only valid on internal tables. String format is locality group name, colon, comma delimited list of column families in the group. Groups are delimited by pipes. Example: `group1:famA,famB,famC\|group2:famD,famE,famF\|etc...` |
| `row_id`          | (first column) | Trino column name that maps to the Accumulo row ID.                                                                                                                                                                                                                                    |
| `serializer`      | `default`      | Serializer for Accumulo data encodings. Can either be `default`, `string`, `lexicoder` or a Java class name. Default is `default`, i.e. the value from `AccumuloRowSerializer.getDefault()`, i.e. `lexicoder`.                                                                         |
| `scan_auths`      | (user auths)   | Scan-time authorizations set on the batch scanner.                                                                                                                                                                                                                                     |

## Session properties

You can change the default value of a session property by using {doc}`/sql/set-session`.
Note that session properties are prefixed with the catalog name:

```
SET SESSION example.column_filter_optimizations_enabled = false;
```

| Property name                              | Default value | Description                                                                                                                                                          |
| ------------------------------------------ | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `optimize_locality_enabled`                | `true`        | Set to true to enable data locality for non-indexed scans                                                                                                            |
| `optimize_split_ranges_enabled`            | `true`        | Set to true to split non-indexed queries by tablet splits. Should generally be true.                                                                                 |
| `optimize_index_enabled`                   | `true`        | Set to true to enable usage of the secondary index on query                                                                                                          |
| `index_rows_per_split`                     | `10000`       | The number of Accumulo row IDs that are packed into a single Trino split                                                                                             |
| `index_threshold`                          | `0.2`         | The ratio between number of rows to be scanned based on the index over the total number of rows. If the ratio is below this threshold, the index will be used.        |
| `index_lowest_cardinality_threshold`       | `0.01`        | The threshold where the column with the lowest cardinality will be used instead of computing an intersection of ranges in the index. Secondary index must be enabled |
| `index_metrics_enabled`                    | `true`        | Set to true to enable usage of the metrics table to optimize usage of the index                                                                                      |
| `scan_username`                            | (config)      | User to impersonate when scanning the tables. This property trumps the `scan_auths` table property                                                                   |
| `index_short_circuit_cardinality_fetch`    | `true`        | Short circuit the retrieval of index metrics once any column is less than the lowest cardinality threshold                                                           |
| `index_cardinality_cache_polling_duration` | `10ms`        | Sets the cardinality cache polling duration for short circuit retrieval of index metrics                                                                             |

## Adding columns

Adding a new column to an existing table cannot be done today via
`ALTER TABLE [table] ADD COLUMN [name] [type]` because of the additional
metadata required for the columns to work; the column family, qualifier,
and if the column is indexed.

## Serializers

The Trino connector for Accumulo has a pluggable serializer framework
for handling I/O between Trino and Accumulo. This enables end-users the
ability to programmatically serialized and deserialize their special data
formats within Accumulo, while abstracting away the complexity of the
connector itself.

There are two types of serializers currently available; a `string`
serializer that treats values as Java `String`, and a `lexicoder`
serializer that leverages Accumulo's Lexicoder API to store values. The
default serializer is the `lexicoder` serializer, as this serializer
does not require expensive conversion operations back and forth between
`String` objects and the Trino types -- the cell's value is encoded as a
byte array.

Additionally, the `lexicoder` serializer does proper lexigraphical ordering of
numerical types like `BIGINT` or `TIMESTAMP`.  This is essential for the connector
to properly leverage the secondary index when querying for data.

You can change the default the serializer by specifying the
`serializer` table property, using either `default` (which is
`lexicoder`), `string` or `lexicoder` for the built-in types, or
you could provide your own implementation by extending
`AccumuloRowSerializer`, adding it to the Trino `CLASSPATH`, and
specifying the fully-qualified Java class name in the connector configuration.

```sql
CREATE TABLE example_schema.scientists (
  recordkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
  serializer = 'default'
);
```

```sql
INSERT INTO example_schema.scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );
```

```text
root@default> scan -t example_schema.scientists
row1 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00m
row1 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xA6\x06
row1 metadata:name []    Grace Hopper
row2 metadata:age []    \x08\x80\x00\x00\x00\x00\x00\x00g
row2 metadata:date []    \x08\x7F\xFF\xFF\xFF\xFF\xFF\xAD\xED
row2 metadata:name []    Alan Turing
```

```sql
CREATE TABLE example_schema.stringy_scientists (
  recordkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
  serializer = 'string'
);
```

```sql
INSERT INTO example_schema.stringy_scientists VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );
```

```text
root@default> scan -t example_schema.stringy_scientists
row1 metadata:age []    109
row1 metadata:date []    -23034
row1 metadata:name []    Grace Hopper
row2 metadata:age []    103
row2 metadata:date []    -21011
row2 metadata:name []    Alan Turing
```

```sql
CREATE TABLE example_schema.custom_scientists (
  recordkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  column_mapping = 'name:metadata:name,age:metadata:age,birthday:metadata:date',
  serializer = 'my.serializer.package.MySerializer'
);
```

## Metadata management

Metadata for the Trino/Accumulo tables is stored in ZooKeeper. You can,
and should, issue SQL statements in Trino to create and drop tables.
This is the easiest method of creating the metadata required to make the
connector work. It is best to not mess with the metadata, but here are
the details of how it is stored.

A root node in ZooKeeper holds all the mappings, and the format is as
follows:

```text
/metadata-root/schema/table
```

Where `metadata-root` is the value of `zookeeper.metadata.root` in
the config file (default is `/trino-accumulo`), `schema` is the
Trino schema (which is identical to the Accumulo namespace name), and
`table` is the Trino table name (again, identical to Accumulo name).
The data of the `table` ZooKeeper node is a serialized
`AccumuloTable` Java object (which resides in the connector code).
This table contains the schema (namespace) name, table name, column
definitions, the serializer to use for the table, and any additional
table properties.

If you have a need to programmatically manipulate the ZooKeeper metadata
for Accumulo, take a look at
`io.trino.plugin.accumulo.metadata.ZooKeeperMetadataManager` for some
Java code to simplify the process.

## Converting table from internal to external

If your table is *internal*, you can convert it to an external table by deleting
the corresponding znode in ZooKeeper, effectively making the table no longer exist as
far as Trino is concerned.  Then, create the table again using the same DDL, but adding the
`external = true` table property.

For example:

1\. We're starting with an internal table `foo.bar` that was created with the below DDL.
If you have not previously defined a table property for `column_mapping` (like this example),
be sure to describe the table **before** deleting the metadata.  We need the column mappings
when creating the external table.

```sql
CREATE TABLE foo.bar (a VARCHAR, b BIGINT, c DATE)
WITH (
    index_columns = 'b,c'
);
```

```sql
DESCRIBE foo.bar;
```

```text
 Column |  Type   | Extra |               Comment
--------+---------+-------+-------------------------------------
 a      | varchar |       | Accumulo row ID
 b      | bigint  |       | Accumulo column b:b. Indexed: true
 c      | date    |       | Accumulo column c:c. Indexed: true
```

2\. Using the ZooKeeper CLI, delete the corresponding znode.  Note this uses the default ZooKeeper
metadata root of `/trino-accumulo`

```text
$ zkCli.sh
[zk: localhost:2181(CONNECTED) 1] delete /trino-accumulo/foo/bar
```

3\. Re-create the table using the same DDL as before, but adding the `external=true` property.
Note that if you had not previously defined the column_mapping, you need to add the property
to the new DDL (external tables require this property to be set).  The column mappings are in
the output of the `DESCRIBE` statement.

```sql
CREATE TABLE foo.bar (
  a VARCHAR,
  b BIGINT,
  c DATE
)
WITH (
  column_mapping = 'a:a:a,b:b:b,c:c:c',
  index_columns = 'b,c',
  external = true
);
```

(accumulo-type-mapping)=

## Type mapping

Because Trino and Accumulo each support types that the other does not, this
connector modifies some types when reading or writing data. Data types may not
map the same way in both directions between Trino and the data source. Refer to
the following sections for type mapping in each direction.

### Accumulo type to Trino type mapping

The connector maps Accumulo types to the corresponding Trino types following
this table:

```{eval-rst}
.. list-table:: Accumulo type to Trino type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - Accumulo type
    - Trino type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``TINYINT``
    - ``TINYINT``
    -
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``REAL``
    - ``REAL``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``VARBINARY``
    - ``VARBINARY``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP(n)``
    -
```

No other types are supported

### Trino type to Accumulo type mapping

The connector maps Trino types to the corresponding Trino type to Accumulo type
mapping types following this table:

```{eval-rst}
.. list-table:: Trino type to Accumulo type mapping
  :widths: 30, 20, 50
  :header-rows: 1

  * - Trino type
    - Accumulo type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``TINYINT``
    - ``TINYINT``
    - Trino only supports writing values belonging to ``[0, 127]``
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``REAL``
    - ``REAL``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``VARBINARY``
    - ``VARBINARY``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP(n)``
    -
```

No other types are supported

(accumulo-sql-support)=

## SQL support

The connector provides read and write access to data and metadata in
the Accumulo database. In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`
