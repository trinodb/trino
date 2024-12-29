# Lakehouse connector

The Lakehouse connector provides a unified way to interact with data stored
in various table formats across different storage systems and metastore services.
This single connector allows you to query and write data seamlessly, regardless of
whether it's in Iceberg, Delta Lake, or Hudi table formats, or traditional Hive tables.

This connector offers flexible connectivity to popular metastore services including
AWS Glue and Hive Metastore. For data storage, it supports a wide range of options
including cloud storage services such as AWS S3, S3-compatible storage,
Google Cloud Storage (GCS), and Azure Blob Storage, as well as HDFS installations.

The connector combines the features of the
[Hive](/connector/hive), [Iceberg](/connector/iceberg),
[Delta Lake](/connector/delta-lake), and [Hudi](/connector/hudi)
connectors into a single connector. The configuration properties,
session properties, table properties, and beahvior come from the underlying
connectors. Please refer to the documentation for the underlying connectors
for the table formats that you are using.

## General configuration

To configure the Lakehouse connector, create a catalog properties file
`etc/catalog/example.properties` with the following content, replacing the
properties as appropriate:

```text
connector.name=lakehouse
```

You must configure a [AWS Glue or a Hive metastore](/object-storage/metastores).
The `hive.metastore` property will also configure the Iceberg catalog.
Do not specify `iceberg.catalog.type`.

You must select and configure one of the
[supported file systems](lakehouse-file-system-configuration).

## Configuration properties

The following configuration properties are available:

:::{list-table}
:widths: 30, 58, 12
:header-rows: 1

* - Property name
  - Description
  - Default
* - `lakehouse.table-type`
  - The default table type for newly created tables when the `format`
    table property is not specified. Possible values:
    * `HIVE`
    * `ICEBERG`
    * `DELTA`
  - `ICEBERG`
:::

(lakehouse-file-system-configuration)=
## File system access configuration

The connector supports accessing the following file systems:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-hdfs)

You must enable and configure the specific file system access. 

## Examples

Create an Iceberg table:

```sql
CREATE TABLE iceberg_table (
  c1 INTEGER,
  c2 DATE,
  c3 DOUBLE
)
WITH (
  type = 'ICEBERG'
  format = 'PARQUET',
  partitioning = ARRAY['c1', 'c2'],
  sorted_by = ARRAY['c3']
);
```

Create a Hive table:

```sql
CREATE TABLE hive_page_views (
  view_time TIMESTAMP,
  user_id BIGINT,
  page_url VARCHAR,
  ds DATE,
  country VARCHAR
)
WITH (
  type = 'HIVE',
  format = 'ORC',
  partitioned_by = ARRAY['ds', 'country'],
  bucketed_by = ARRAY['user_id'],
  bucket_count = 50
)
```
