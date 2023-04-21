Google Cloud Storage
====================

Object storage connectors can access
`Google Cloud Storage <https://cloud.google.com/storage/>`_ data using the 
``gs://`` URI prefix.

Requirements
-------------

To use Google Cloud Storage with non-anonymous access objects, you need:

* A `Google Cloud service account <https://console.cloud.google.com/projectselector2/iam-admin/serviceaccounts>`_
* The key for the service account in JSON format

.. _hive-google-cloud-storage-configuration:

Configuration
-------------

The use of Google Cloud Storage as a storage location for an object storage
catalog requires setting a configuration property that defines the
`authentication method for any non-anonymous access object
<https://cloud.google.com/storage/docs/authentication>`_. Access methods cannot
be combined.

The default root path used by the ``gs:\\`` prefix is set in the catalog by the
contents of the specified key file, or the key file used to create the OAuth
token.

.. list-table:: Google Cloud Storage configuration properties
    :widths: 35, 65
    :header-rows: 1

    * - Property Name
      - Description
    * - ``hive.gcs.json-key-file-path``
      - JSON key file used to authenticate your Google Cloud service account
        with Google Cloud Storage.  
    * - ``hive.gcs.use-access-token``
      - Use client-provided OAuth token to access Google Cloud Storage.

The following uses the Delta Lake connector in an example of a minimal
configuration file for an object storage catalog using a JSON key file:

.. code-block:: properties

  connector.name=delta_lake
  hive.metastore.uri=thrift://example.net:9083
  hive.gcs.json-key-file-path=${ENV:GCP_CREDENTIALS_FILE_PATH}

General usage
-------------

Create a schema to use if one does not already exist, as in the following
example:

.. code-block:: sql

  CREATE SCHEMA storage_catalog.sales_data_in_gcs WITH (location = 'gs://example_location');

Once you have created a schema, you can create tables in the schema, as in the
following example:

.. code-block:: sql

  CREATE TABLE storage_catalog.sales_data_in_gcs.orders (
      orderkey bigint,
      custkey bigint,
      orderstatus varchar(1),
      totalprice double,
      orderdate date,
      orderpriority varchar(15),
      clerk varchar(15),
      shippriority integer,
      comment varchar(79)
  );

This statement creates the folder ``gs://sales_data_in_gcs/orders`` in the root
folder defined in the JSON key file.

Your table is now ready to populate with data using ``INSERT`` statements.
Alternatively, you can use ``CREATE TABLE AS`` statements to create and
populate the table in a single statement.