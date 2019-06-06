Hive Connector GCS Tutorial
===========================

Preliminary Steps
-----------------

Ensure Access to GCS
^^^^^^^^^^^^^^^^^^^^

Access to Cloud Storage data is possible thanks to
`Hadoop Cloud Storage connector <https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage>`_.

If your data is publicly available, you do not need to do anything here.
However, in most cases data is not publicly available, and the Presto cluster needs to have access to it.
This is typically achieved by creating a service account which has permissions to access your data.
You can do this on the
`service accounts page in GCP <https://console.cloud.google.com/projectselector2/iam-admin/serviceaccounts>`_.
Once you create a service account, create a key for it and download the key in JSON format.

Hive Connector configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Another requirement is that you have enabled and configured a Hive connector in Presto.
The connector uses Hive metastore for data discovery and is not limited to data residing on HDFS.

**Configuring Hive Connector**

* URL to Hive metastore:

    * New Hive metastore on GCP:

        If your Presto nodes are provisioned by GCP, your Hive metastore should also be on GCP
        to minimize latency and costs. The simplest way to create a new Hive metastore on GCP
        is to create a small Cloud DataProc cluster (1 master, 0 workers), accessible from
        your Presto cluster. Follow the steps for existing Hive metastore after finishing this step.

    * Existing Hive metastore:

        To use an existing Hive metastore with a Presto cluster, you need to set the
        ``hive.metastore.uri`` property in your Hive catalog properties file to
        ``thrift://${METASTORE_ADDRESS}:${METASTORE_THRIFT_PORT}``.
        If the metastore uses authentication, please refer to :doc:`hive-security`.

* GCS access:

    Here are example values for all GCS configuration properties which can be set in Hive
    catalog properties file:

    .. code-block:: properties

        # JSON key file used to access Google Cloud Storage
        hive.gcs.json-key-file-path=/path/to/gcs_keyfile.json

        # Use client-provided OAuth token to access Google Cloud Storage
        hive.gcs.use-access-token=false

Hive Metastore configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If your Hive metastore uses StorageBasedAuthorization it will also need to access GCS
to perform POSIX permission checks.
Configuring GCS access for Hive is outside the scope of this tutorial, but there
are some excellent guides online:

* `Google: Installing the Cloud Storage connector <https://cloud.google.com/dataproc/docs/concepts/connectors/install-storage-connector>`_
* `HortonWorks: Working with Google Cloud Storage <https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/bk_cloud-data-access/content/gcp-get-started.html>`_
* `Cloudera: Configuring Google Cloud Storage Connectivity <https://www.cloudera.com/documentation/enterprise/latest/topics/admin_gcs_config.html>`_

GCS access is typically configured in ``core-site.xml``, to be used by all components using Apache Hadoop.

GCS connector for Hadoop provides an implementation of a Hadoop FileSystem.
Unfortunately GCS IAM permissions don't map to POSIX permissions required by Hadoop FileSystem,
so the GCS connector presents fake POSIX file permissions.

When Hive metastore accesses GCS, it will by default see fake POSIX permissions equal to ``0700``.
If Presto and Hive metastore are running as different user accounts, this will cause Hive metastore
to deny Presto data access.
There are two possible solutions to this problem:

* Run Presto service and Hive service as the same user.
* Make sure Hive GCS configuration includes a ``fs.gs.reported.permissions`` property
  with a value of ``777``.

Accessing GCS Data From Presto for the First Time
-------------------------------------------------

Accessing Data Already Mapped in the Hive metastore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you migrate to Presto from Hive, chances are that your GCS data is already mapped to
SQL tables in the metastore.
In that case, you should be able to query it.

Accessing Data Not Yet Mapped in the Hive metastore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To access GCS data that is not yet mapped in the Hive metastore you need to provide the
schema of the data, the file format, and the data location.
For example, if you have ORC or Parquet files in an GCS bucket ``my_bucket``, you will
need to execute a query::

    -- select schema in which the table will be defined, must already exist
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
         external_location = 'gs://my_bucket/path/to/folder',
         format = 'ORC' -- or 'PARQUET'
    );

Now you should be able to query the newly mapped table::

    SELECT * FROM orders;

Writing GCS Data with Presto
----------------------------

Prerequisites
^^^^^^^^^^^^^

Before you attempt to write data to GCS, make sure you have configured everything
necessary to read data from GCS.

Create Export Schema
^^^^^^^^^^^^^^^^^^^^

If Hive metastore contains schema(s) mapped to GCS locations, you can use them to
export data to GCS.
If you don't want to use existing schemas (or there are no appropriate schemas in
the Hive metastore), you need to create a new one::

    CREATE SCHEMA hive.gcs_export WITH (location = 'gs://my_bucket/some/path');

Export Data to GCS
^^^^^^^^^^^^^^^^^^

Once you have a schema pointing to a location where you want to export the data, you can issue
the export using a ``CREATE TABLE AS`` statement and select your desired file format. The data
will be written to one or more files within the ``gs://my_bucket/some/path/my_table`` namespace.
Example::

    CREATE TABLE hive.gcs_export.orders_export
    WITH (format = 'ORC')
    AS SELECT * FROM tpch.sf1.orders;
