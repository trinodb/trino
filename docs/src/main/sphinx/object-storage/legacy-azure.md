# Legacy Azure Storage support

The {doc}`/connector/hive` can be configured to use [Azure Data Lake Storage
(Gen2)](https://azure.microsoft.com/products/storage/data-lake-storage/). Trino
supports Azure Blob File System (ABFS) to access data in ADLS Gen2.

:::{warning}
Legacy support is not recommended and will be removed. Use
[](file-system-azure).
:::

## Hive connector configuration for Azure Storage credentials

To configure Trino to use the Azure Storage credentials, set the following
configuration properties in the catalog properties file. It is best to use this
type of configuration if the primary storage account is linked to the cluster.

The specific configuration depends on the type of storage and uses the
properties from the following sections in the catalog properties file.

For more complex use cases, such as configuring multiple secondary storage
accounts using Hadoop's `core-site.xml`, see the
{ref}`hive-azure-advanced-config` options.

To use legacy support, the `fs.hadoop.enabled` property must be set to `true` in
your catalog configuration file.

### ADLS Gen2 / ABFS storage

To connect to ABFS storage, you may either use the storage account's access
key, or a service principal. Do not use both sets of properties at the
same time.

:::{list-table} ABFS Access Key
:widths: 30, 70
:header-rows: 1

* - Property name
  - Description
* - `hive.azure.abfs-storage-account`
  - The name of the ADLS Gen2 storage account
* - `hive.azure.abfs-access-key`
  - The decrypted access key for the ADLS Gen2 storage account
:::

:::{list-table} ABFS Service Principal OAuth
:widths: 30, 70
:header-rows: 1

* - Property name
  - Description
* - `hive.azure.abfs.oauth.endpoint`
  - The service principal / application's OAuth 2.0 token endpoint (v1).
* - `hive.azure.abfs.oauth.client-id`
  - The service principal's client/application ID.
* - `hive.azure.abfs.oauth.secret`
  - A client secret for the service principal.
:::

When using a service principal, it must have the Storage Blob Data Owner,
Contributor, or Reader role on the storage account you are using, depending on
which operations you would like to use.


(hive-azure-advanced-config)=
### Advanced configuration

All of the configuration properties for the Azure storage driver are stored in
the Hadoop `core-site.xml` configuration file. When there are secondary
storage accounts involved, we recommend configuring Trino using a
`core-site.xml` containing the appropriate credentials for each account.

The path to the file must be configured in the catalog properties file:

```text
hive.config.resources=<path_to_hadoop_core-site.xml>
```

One way to find your account key is to ask for the connection string for the
storage account. The `abfsexample.dfs.core.windows.net` account refers to the
storage account. The connection string contains the account key:

```text
az storage account  show-connection-string --name abfswales1
{
  "connectionString": "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=abfsexample;AccountKey=examplekey..."
}
```

When you have the account access key, you can add it to your `core-site.xml`
or Java cryptography extension (JCEKS) file. Alternatively, you can have your
cluster management tool to set the option
`fs.azure.account.key.STORAGE-ACCOUNT` to the account key value:

```text
<property>
  <name>fs.azure.account.key.abfsexample.dfs.core.windows.net</name>
  <value>examplekey...</value>
</property>
```

For more information, see [Hadoop Azure Support: ABFS](https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html).

## Accessing Azure Storage data

### URI scheme to reference data

Consistent with other FileSystem implementations within Hadoop, the Azure
Standard Blob and Azure Data Lake Storage Gen2 (ABFS) drivers define their own
URI scheme so that resources (directories and files) may be distinctly
addressed. You can access both primary and secondary storage accounts linked to
the cluster with the same URI scheme. Following are example URIs for the
different systems.

ABFS URI:

```text
abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>/<path>/<file_name>
```

ADLS Gen1 URI:

```text
adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>
```

Azure Standard Blob URI:

```text
wasb[s]://<container>@<account_name>.blob.core.windows.net/<path>/<path>/<file_name>
```

### Querying Azure Storage

You can query tables already configured in your Hive metastore used in your Hive
catalog. To access Azure Storage data that is not yet mapped in the Hive
metastore, you need to provide the schema of the data, the file format, and the
data location.

For example, if you have ORC or Parquet files in an ABFS `file_system`, you
need to execute a query:

```
-- select schema in which the table is to be defined, must already exist
USE hive.default;

-- create table
CREATE TABLE orders (
     orderkey BIGINT,
     custkey BIGINT,
     orderstatus VARCHAR(1),
     totalprice DOUBLE,
     orderdate DATE,
     orderpriority VARCHAR(15),
     clerk VARCHAR(15),
     shippriority INTEGER,
     comment VARCHAR(79)
) WITH (
     external_location = 'abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>/<path>/',
     format = 'ORC' -- or 'PARQUET'
);
```

Now you can query the newly mapped table:

```
SELECT * FROM orders;
```

## Writing data

### Prerequisites

Before you attempt to write data to Azure Storage, make sure you have configured
everything necessary to read data from the storage.

### Create a write schema

If the Hive metastore contains schema(s) mapped to Azure storage filesystems,
you can use them to write data to Azure storage.

If you don't want to use existing schemas, or there are no appropriate schemas
in the Hive metastore, you need to create a new one:

```
CREATE SCHEMA hive.abfs_export
WITH (location = 'abfs[s]://file_system@account_name.dfs.core.windows.net/<path>');
```

### Write data to Azure Storage

Once you have a schema pointing to a location where you want to write the data,
you can issue a `CREATE TABLE AS` statement and select your desired file
format. The data will be written to one or more files within the
`abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/my_table`
namespace. Example:

```
CREATE TABLE hive.abfs_export.orders_abfs
WITH (format = 'ORC')
AS SELECT * FROM tpch.sf1.orders;
```

(fs-legacy-azure-migration)=
## Migration to Azure Storage file system

Trino includes a [native implementation to access Azure
Storage](/object-storage/file-system-azure) with a catalog using the Delta Lake,
Hive, Hudi, or Iceberg connectors. Upgrading existing deployments to the new
native implementation is recommended. Legacy support will be deprecated and
removed.

To migrate a catalog to use the native file system implementation for Azure,
make the following edits to your catalog configuration:

1. Add the `fs.native-azure.enabled=true` catalog configuration property.
2. Configure the `azure.auth-type` catalog configuration property.
3. Refer to the following table to rename your existing legacy catalog
   configuration properties to the corresponding native configuration
   properties. Supported configuration values are identical unless otherwise
   noted.

  :::{list-table}
  :widths: 35, 35, 65
  :header-rows: 1
   * - Legacy property
     - Native property
     - Notes
   * - `hive.azure.abfs-access-key`
     - `azure.access-key`
     -
   * - `hive.azure.abfs.oauth.endpoint`
     - `azure.oauth.endpoint`
     - Also see `azure.oauth.tenant-id` in [](azure-oauth-authentication).
   * - `hive.azure.abfs.oauth.client-id`
     - `azure.oauth.client-id`
     -
   * - `hive.azure.abfs.oauth.secret`
     - `azure.oauth.secret`
     -
   * - `hive.azure.abfs.oauth2.passthrough`
     - `azure.use-oauth-passthrough-token`
     -
  :::

4. Remove the following legacy configuration properties if they exist in your
   catalog configuration:

      * `hive.azure.abfs-storage-account`
      * `hive.azure.wasb-access-key`
      * `hive.azure.wasb-storage-account`

For more information, see the [](/object-storage/file-system-azure).