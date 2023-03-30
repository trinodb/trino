=================================
Hive connector with Azure Storage
=================================

The :doc:`hive` can be configured to use `Azure Data Lake Storage (Gen2)
<https://azure.microsoft.com/products/storage/data-lake-storage/>`_. Trino
supports Azure Blob File System (ABFS) to access data in ADLS Gen2.

Trino also supports `ADLS Gen1
<https://learn.microsoft.com/azure/data-lake-store/data-lake-store-overview>`_
and Windows Azure Storage Blob driver (WASB), but we recommend `migrating to
ADLS Gen2
<https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-migrate-gen1-to-gen2-azure-portal>`_,
as ADLS Gen1 and WASB are legacy options that will be removed in the future.
Learn more from `the official documentation
<https://docs.microsoft.com/azure/data-lake-store/data-lake-store-overview>`_.

Hive connector configuration for Azure Storage credentials
----------------------------------------------------------

To configure Trino to use the Azure Storage credentials, set the following
configuration properties in the catalog properties file. It is best to use this
type of configuration if the primary storage account is linked to the cluster.

The specific configuration depends on the type of storage and uses the
properties from the following sections in the catalog properties file.

For more complex use cases, such as configuring multiple secondary storage
accounts using Hadoop's ``core-site.xml``, see the
:ref:`hive-azure-advanced-config` options.

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

ADLS Gen1 (legacy)
^^^^^^^^^^^^^^^^^^

While it is advised to migrate to ADLS Gen2 whenever possible, if you still
choose to use ADLS Gen1 you need to include the following properties in your
catalog configuration.

.. note::

    Credentials for the filesystem can be configured using ``ClientCredential``
    type. To authenticate with ADLS Gen1 you must create a new application
    secret for your ADLS Gen1 account's App Registration, and save this value
    because you won't able to retrieve the key later. Refer to the Azure
    `documentation
    <https://docs.microsoft.com/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory>`_
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

WASB storage (legacy)
^^^^^^^^^^^^^^^^^^^^^

.. list-table:: WASB properties
  :widths: 30, 70
  :header-rows: 1

  * - Property name
    - Description
  * - ``hive.azure.wasb-storage-account``
    - Storage account name of Azure Blob Storage
  * - ``hive.azure.wasb-access-key``
    - The decrypted access key for the Azure Blob Storage

.. _hive-azure-advanced-config:

Advanced configuration
^^^^^^^^^^^^^^^^^^^^^^

All of the configuration properties for the Azure storage driver are stored in
the Hadoop ``core-site.xml`` configuration file. When there are secondary
storage accounts involved, we recommend configuring Trino using a
``core-site.xml`` containing the appropriate credentials for each account.

The path to the file must be configured in the catalog properties file:

.. code-block:: text

    hive.config.resources=<path_to_hadoop_core-site.xml>

One way to find your account key is to ask for the connection string for the
storage account. The ``abfsexample.dfs.core.windows.net`` account refers to the
storage account. The connection string contains the account key:

.. code-block:: text

    az storage account  show-connection-string --name abfswales1
    {
      "connectionString": "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=abfsexample;AccountKey=examplekey..."
    }

When you have the account access key, you can add it to your ``core-site.xml``
or Java cryptography extension (JCEKS) file. Alternatively, you can have your
cluster management tool to set the option
``fs.azure.account.key.STORAGE-ACCOUNT`` to the account key value:

.. code-block:: text

    <property>
      <name>fs.azure.account.key.abfsexample.dfs.core.windows.net</name>
      <value>examplekey...</value>
    </property>

For more information, see `Hadoop Azure Support: ABFS
<https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html>`_.

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

    abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>/<path>/<file_name>

ADLS Gen1 URI:

.. code-block:: text

    adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>

Azure Standard Blob URI:

.. code-block:: text

    wasb[s]://<container>@<account_name>.blob.core.windows.net/<path>/<path>/<file_name>

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
         external_location = 'abfs[s]://<file_system>@<account_name>.dfs.core.windows.net/<path>/<path>/',
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
