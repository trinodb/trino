# Paimon connector

```{raw} html
<img src="../_static/img/paimon.png" class="connector-logo">
```

The Paimon connector enables querying [Paimon](https://paimon.apache.org/docs/master/) tables.

Apache Paimon is an open table format for huge analytic datasets, and it is 
also very friendly to streaming read/write.
The Paimon connector allows querying data stored in files written 
in [Paimon format](https://paimon.apache.org/docs/master/).

## Requirements

To use the Paimon connector, you need:

- Network access from the Trino coordinator and workers to the distributed
  object storage.

- Data files stored in the file formats
  [Parquet](parquet-format-configuration)(default),
  [ORC](orc-format-configuration), or Avro on a filesystem that is supported by trino.

## General configuration

To configure the Paimon connector, create a catalog properties file
`etc/catalog/paimon.properties` that references the `paimon` connector.

You must select and configure one of the [supported file
systems](paimon-file-system-configuration).

```properties
connector.name=paimon
paimon.catalog.type=filesystem
paimon.warehouse=s3://your-bucket/path/to/warehouse
fs.x.enabled=true
```

Replace the `fs.x.enabled` configuration property with the desired file system.

Additionally, following configuration properties can be set depending on the use-case:

:::{list-table} Paimon configuration properties
:widths: 30, 58, 12
:header-rows: 1

* - Property name
  - Description
  - Default
* - `paimon.catalog.type`
  - Possible values are:
    * `filesystem`
    * `hive`
  - `filesystem`
* - `paimon.warehouse`
  - The root path of paimon tables.
  - `No default`
* - `paimon.projection-pushdown-enabled`
  - Enable [projection pushdown](/optimizer/pushdown)
  - `true`
* - `paimon.metadata-cache.enabled`
  - Set to `false` to disable in-memory caching of metadata files on the
    coordinator. This cache is not used when `fs.cache.enabled` is set to true.
  - `true`
:::

(paimon-file-system-configuration)=
## File system access configuration

The connector supports accessing the following file systems:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-hdfs)

You must enable and configure the specific file system access. [Legacy
support](file-system-legacy) is not recommended and will be removed.


## Paimon to Trino type mapping

The connector maps Paimon types to the corresponding Trino types according to
the following table:

:::{list-table} Paimon type to Trino type mapping
:widths: 40, 60
:header-rows: 1

* - Paimon type
  - Trino type
* - `BOOLEAN`
  - `BOOLEAN`
* - `TINYINT`
  - `TINYINT`
* - `SMALLINT`
  - `SMALLINT`
* - `INT`
  - `INTEGER`
* - `BIGINT`
  - `BIGINT`
* - `FLOAT`
  - `REAL`
* - `DOUBLE`
  - `DOUBLE`
* - `DECIMAL(p,s)`
  - `DECIMAL(p,s)`
* - `DATE`
  - `DATE`
* - `TIME(n)`
  - `TIME(n)`
* - `TIMESTAMP(n)`
  - `TIMESTAMP(n)`
* - `TIMESTAMP_WITH_LOCAL_TIME_ZONE(n)`
  - `TIMESTAMP(n) WITH TIME ZONE`
* - `CHAR`
  - `CHAR`
* - `VARCHAR`
  - `VARCHAR`
* - `STRING`
  - `VARCHAR`
* - `BINARY`
  - `VARBINARY`
* - `VARBINARY`
  - `VARBINARY`
:::

No other types are supported. (MAP, LIST, STRUCT types are not supported yet.)

## SQL support

The connector provides read access to data in the Paimon table. 
The {ref}`globally available <sql-globally-available>`
and {ref}`read operation <sql-read-operations>` statements are supported.

### Basic usage examples

In the following example queries, `nation` is the Paimon append table loaded by tpch.
The target file system is s3, which mocked by minio in the local environment.

My paimon.properties in etc/catalog shows as below:
```properties
connector.name=paimon
paimon.catalog.type=filesystem
paimon.warehouse=s3://test-paimon-connector-ryo1qhtrl9/
fs.native-s3.enabled=true
s3.aws-access-key=accesskey
s3.aws-secret-key=secretkey
s3.region=us-east-1
s3.endpoint=http://localhost:32768
s3.path-style-access=true
```


```sql
USE use paimon.tests;

SELECT * FROM nation LIMIT 1;
```

```text
 nationkey |  name   | regionkey |                       comment
-----------+---------+-----------+-----------------------------------------------------
         0 | ALGERIA |         0 |  haggle. carefully final deposits detect slyly agai
(1 row)
```

```sql
SELECT nationkey, regionkey 
FROM nation 
WHERE regionkey > 3;
```

```text
 nationkey | regionkey
-----------+-----------
         4 |         4
        10 |         4
        11 |         4
        13 |         4
        20 |         4
(5 rows)
```

```sql
SELECT regionkey, count(nationkey) 
FROM nation 
GROUP BY regionkey;
```

```text
 regionkey | _col1
-----------+-------
         2 |     5
         0 |     5
         4 |     5
         3 |     5
         1 |     5
(5 rows)
```

(paimon-metadata-tables)=
### Metadata tables

The connector exposes a metadata table for each Paimon table.
The metadata table contains information about the internal structure
of the Paimon table. You can query each metadata table by appending the
metadata table name to the table name:

```
SELECT * FROM "nation$files"
```

```text
partition | bucket |                                                     file_path                                                      | file_format | schema_id | level | record_count | file_size_in_bytes | min_>
-----------+--------+--------------------------------------------------------------------------------------------------------------------+-------------+-----------+-------+--------------+--------------------+----->
[]        |      0 | s3://test-paimon-connector-ryo1qhtrl9/tests.db/nation/bucket-0/data-101a3045-cd04-40cd-8664-8355c7d642e9-0.parquet | parquet     |         0 |     0 |           25 |               2283 | NULL>
(1 row)
```

For all available metadata tables, see the [Paimon documentation](https://paimon.apache.org/docs/master/concepts/system-tables/).
