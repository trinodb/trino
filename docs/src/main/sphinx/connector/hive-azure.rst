=================================
Hive connector with Azure Storage
=================================

The :doc:`hive` can be configured to query
Azure Standard Blob Storage and Azure Data Lake Storage Gen2 (ABFS). Azure Blobs
are accessed via the Windows Azure Storage Blob (WASB). This layer is built on
top of the HDFS APIs and is what allows for the separation of storage from the
cluster.

Trino supports both ADLS Gen1 and Gen2. With ADLS Gen2 now generally available,
we recommend using ADLS Gen2. Learn more from `the official documentation
<https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-overview>`_.

Hive connector configuration
----------------------------

All configuration for the Azure storage driver is stored in the Hadoop
``core-site.xml`` configuration file. The path to the file needs to be
configured in the catalog properties file:

.. code-block:: text

    hive.config.resources=<path_to_hadoop_core-site.xml>

Configuration for Azure Storage credentials
-------------------------------------------

If you do not want to rely on Hadoop's ``core-site.xml`` and want to have Trino
configured independently with the storage credentials, you can use the following
properties in the catalog configuration.

We suggest to use this kind of configuration when you only have the Primary
Storage account linked to the cluster. When there are secondary storage
accounts involved, we recommend configuring Trino using a ``core-site.xml``
containing the appropriate credentials for each account, as described in the
preceding section.

WASB storage
^^^^^^^^^^^^

.. list-table:: WASB properties
  :widths: 30, 70
  :header-rows: 1

  * - Property name
    - Description
  * - ``hive.azure.wasb-storage-account``
    - Storage account name of Azure Blob Storage
  * - ``hive.azure.wasb-access-key``
    - The decrypted access key for the Azure Blob Storage

ADLS Gen2 / ABFS storage
^^^^^^^^^^^^^^^^^^^^^^^^

To connect to ABFS storage, you may either use the storage account's access
key, or a service principal. Do not use both sets of properties at the
same time.

.. list-table:: ABFS Access Key
  :widths: 30, 70
  :header-rows: 1

  * - Property name
    - Description
  * - ``hive.azure.abfs-storage-account``
    - The name of the ADLS Gen2 storage account
  * - ``hive.azure.abfs-access-key``
    - The decrypted access key for the ADLS Gen2 storage account

.. list-table:: ABFS Service Principal OAuth
  :widths: 30, 70
  :header-rows: 1

  * - Property name
    - Description
  * - ``hive.azure.abfs.oauth.endpoint``
    - The service principal / application's OAuth 2.0 token endpoint (v1).
  * - ``hive.azure.abfs.oauth.client-id``
    - The service principal's client/application ID.
  * - ``hive.azure.abfs.oauth.secret``
    - A client secret for the service principal.

When using a service principal, it must have the Storage Blob Data Owner,
Contributor, or Reader role on the storage account you are using, depending on
which operations you would like to use.

ADLS Gen1
^^^^^^^^^

While it is advised to migrate to ADLS Gen2 whenever possible, if you still
choose to use ADLS Gen1 you need to include the following properties in your
catalog configuration.

.. note::

    Credentials for the filesystem can be configured using ``ClientCredential``
    type. To authenticate with ADLS Gen1 you must create a new application
    secret for your ADLS Gen1 account's App Registration, and save this value
    because you won't able to retrieve the key later. Refer to the Azure
    `documentation
    <https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory>`_
    for details.

.. list-table:: ADLS properties
  :widths: 30, 70
  :header-rows: 1

  * - Property name
    - Description
  * - ``hive.azure.adl-client-id``
    - Client (Application) ID from the App Registrations for your storage
      account
  * - ``hive.azure.adl-credential``
    - Value of the new client (application) secret created
  * - ``hive.azure.adl-refresh-url``
    - OAuth 2.0 token endpoint url
  * - ``hive.azure.adl-proxy-host``
    - Proxy host and port in ``host:port`` format. Use this property to connect
      to an ADLS endpoint via a SOCKS proxy.

Accessing Azure Storage data
----------------------------

URI scheme to reference data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Consistent with other FileSystem implementations within Hadoop, the Azure
Standard Blob and Azure Data Lake Storage Gen2 (ABFS) drivers define their own
URI scheme so that resources (directories and files) may be distinctly
addressed. You can access both primary and secondary storage accounts linked to
the cluster with the same URI scheme. Following are example URIs for the
different systems.

ABFS URI:

.. code-block:: text

    abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/<path>/<file_name>

ADLS Gen1 URI:

.. code-block:: text

    adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>

Azure Standard Blob URI:

.. code-block:: text

    wasb[s]://container@account_name.blob.core.windows.net/<path>/<path>/<file_name>

Querying Azure Storage
^^^^^^^^^^^^^^^^^^^^^^

You can query tables already configured in your Hive metastore used in your Hive
catalog. To access Azure Storage data that is not yet mapped in the Hive
metastore, you need to provide the schema of the data, the file format, and the
data location.

For example, if you have ORC or Parquet files in an ABFS ``file_system``, you
need to execute a query::

    -- select schema in which the table is to be defined, must already exist
    USE hive.default;

    -- create table
    CREATE TABLE orders (
         orderkey bigint,
         custkey bigint,
         orderstatus varchar(1),
         totalprice double,
         orderdate date,
         orderpriority varchar(15),
         clerk varchar(15),
         shippriority integer,
         comment varchar(79)
    ) WITH (
         external_location = 'abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/<path>/<file_name>`',
         format = 'ORC' -- or 'PARQUET'
    );

Now you can query the newly mapped table::

    SELECT * FROM orders;

Writing data
------------

Prerequisites
^^^^^^^^^^^^^

Before you attempt to write data to Azure Storage, make sure you have configured
everything necessary to read data from the storage.

Create a write schema
^^^^^^^^^^^^^^^^^^^^^

If the Hive metastore contains schema(s) mapped to Azure storage filesystems,
you can use them to write data to Azure storage.

If you don't want to use existing schemas, or there are no appropriate schemas
in the Hive metastore, you need to create a new one::

    CREATE SCHEMA hive.abfs_export
    WITH (location = 'abfs[s]://file_system@account_name.dfs.core.windows.net/<path>');

Write data to Azure Storage
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have a schema pointing to a location where you want to write the data,
you can issue a ``CREATE TABLE AS`` statement and select your desired file
format. The data will be written to one or more files within the
``abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/my_table``
namespace. Example::

    CREATE TABLE hive.abfs_export.orders_abfs
    WITH (format = 'ORC')
    AS SELECT * FROM tpch.sf1.orders;
