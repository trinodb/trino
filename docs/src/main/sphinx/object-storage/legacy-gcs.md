# Legacy Google Cloud Storage support

Object storage connectors can access
[Google Cloud Storage](https://cloud.google.com/storage/) data using the
`gs://` URI prefix.

:::{warning}
Legacy support is not recommended and will be removed. Use [](file-system-gcs).
:::

## Requirements

To use Google Cloud Storage with non-anonymous access objects, you need:

- A [Google Cloud service account](https://console.cloud.google.com/projectselector2/iam-admin/serviceaccounts)
- The key for the service account in JSON format

(hive-google-cloud-storage-configuration)=
## Configuration

To use legacy support, the `fs.hadoop.enabled` property must be set to `true` in
your catalog configuration file.

The use of Google Cloud Storage as a storage location for an object storage
catalog requires setting a configuration property that defines the
[authentication method for any non-anonymous access object](https://cloud.google.com/storage/docs/authentication). Access methods cannot
be combined.

The default root path used by the `gs:\\` prefix is set in the catalog by the
contents of the specified key file, or the key file used to create the OAuth
token.

:::{list-table} Google Cloud Storage configuration properties
:widths: 35, 65
:header-rows: 1

* - Property Name
  - Description
* - `hive.gcs.json-key-file-path`
  - JSON key file used to authenticate your Google Cloud service account with
    Google Cloud Storage.
* - `hive.gcs.use-access-token`
  - Use client-provided OAuth token to access Google Cloud Storage.
:::

The following uses the Delta Lake connector in an example of a minimal
configuration file for an object storage catalog using a JSON key file:

```properties
connector.name=delta_lake
hive.metastore.uri=thrift://example.net:9083
hive.gcs.json-key-file-path=${ENV:GCP_CREDENTIALS_FILE_PATH}
```

## General usage

Create a schema to use if one does not already exist, as in the following
example:

```sql
CREATE SCHEMA storage_catalog.sales_data_in_gcs WITH (location = 'gs://example_location');
```

Once you have created a schema, you can create tables in the schema, as in the
following example:

```sql
CREATE TABLE storage_catalog.sales_data_in_gcs.orders (
    orderkey BIGINT,
    custkey BIGINT,
    orderstatus VARCHAR(1),
    totalprice DOUBLE,
    orderdate DATE,
    orderpriority VARCHAR(15),
    clerk VARCHAR(15),
    shippriority INTEGER,
    comment VARCHAR(79)
);
```

This statement creates the folder `gs://sales_data_in_gcs/orders` in the root
folder defined in the JSON key file.

Your table is now ready to populate with data using `INSERT` statements.
Alternatively, you can use `CREATE TABLE AS` statements to create and
populate the table in a single statement.

(fs-legacy-gcs-migration)=
## Migration to Google Cloud Storage file system

Trino includes a [native implementation to access Google Cloud
Storage](/object-storage/file-system-gcs) with a catalog using the Delta Lake,
Hive, Hudi, or Iceberg connectors. Upgrading existing deployments to the new
native implementation is recommended. Legacy support will be deprecated and
removed.

To migrate a catalog to use the native file system implementation for Google
Cloud Storage, make the following edits to your catalog configuration:

1. Add the `fs.native-gcs.enabled=true` catalog configuration property.
2. Refer to the following table to rename your existing legacy catalog
   configuration properties to the corresponding native configuration
   properties. Supported configuration values are identical unless otherwise
   noted.

  :::{list-table}
  :widths: 35, 35, 65
  :header-rows: 1
   * - Legacy property
     - Native property
     - Notes
   * - `hive.gcs.use-access-token`
     - `gcs.use-access-token`
     -
   * - `hive.gcs.json-key-file-path`
     - `gcs.json-key-file-path`
     - Also see `gcs.json-key` in [](/object-storage/file-system-gcs)
  :::

For more information, see the [](/object-storage/file-system-gcs).