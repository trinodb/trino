==================
BigQuery connector
==================

The BigQuery connector allows querying the data stored in `BigQuery
<https://cloud.google.com/bigquery/>`_. This can be used to join data between
different systems like BigQuery and Hive. The connector uses the `BigQuery
Storage API <https://cloud.google.com/bigquery/docs/reference/storage/>`_ to
read the data from the tables.

BigQuery Storage API
--------------------

The Storage API streams data in parallel directly from BigQuery via gRPC without
using Google Cloud Storage as an intermediary.
It has a number of advantages over using the previous export-based read flow
that should generally lead to better read performance:

**Direct Streaming**
    It does not leave any temporary files in Google Cloud Storage. Rows are read
    directly from BigQuery servers using an Avro wire format.

**Column Filtering**
    The new API allows column filtering to only read the data you are interested in.
    `Backed by a columnar datastore <https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format>`_,
    it can efficiently stream data without reading all columns.

**Dynamic Sharding**
    The API rebalances records between readers until they all complete. This means
    that all Map phases will finish nearly concurrently. See this blog article on
    `how dynamic sharding is similarly used in Google Cloud Dataflow
    <https://cloud.google.com/blog/big-data/2016/05/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow>`_.

Requirements
------------

To connect to BigQuery, you need:

* To enable the `BigQuery Storage Read API
  <https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api>`_.
* Network access from your Trino coordinator and workers to the
  Google Cloud API service endpoint. This endpoint uses HTTPS, or port 443.
* To configure BigQuery so that the Trino coordinator and workers have `permissions
  in BigQuery <https://cloud.google.com/bigquery/docs/reference/storage#permissions>`_.
* To set up authentication. Your authentiation options differ depending on whether
  you are using Dataproc/Google Compute Engine (GCE) or not.

  **On Dataproc/GCE** the authentication is done from the machine's role.

  **Outside Dataproc/GCE** you have 3 options:

  * Use a service account JSON key and ``GOOGLE_APPLICATION_CREDENTIALS`` as
    described in the Google Cloud authentication `getting started guide
    <https://cloud.google.com/docs/authentication/getting-started>`_.
  * Set ``bigquery.credentials-key`` in the catalog properties file. It should
    contain the contents of the JSON file, encoded using base64.
  * Set ``bigquery.credentials-file`` in the catalog properties file. It should
    point to the location of the JSON file.

Configuration
-------------

To configure the BigQuery connector, create a catalog properties file in
``etc/catalog`` named, for example, ``bigquery.properties``, to mount the
BigQuery connector as the ``bigquery`` catalog. Create the file with the
following contents, replacing the connection properties as appropriate for
your setup:

.. code-block:: text

    connector.name=bigquery
    bigquery.project-id=<your Google Cloud Platform project id>

Multiple GCP projects
^^^^^^^^^^^^^^^^^^^^^

The BigQuery connector can only access a single GCP project.Thus, if you have
data in multiple GCP projects, You need to create several catalogs, each
pointing to a different GCP project. For example, if you have two GCP projects,
one for the sales and one for analytics, you can create two properties files in
``etc/catalog`` named ``sales.properties`` and ``analytics.properties``, both
having ``connector.name=bigquery`` but with different ``project-id``. This will
create the two catalogs, ``sales`` and ``analytics`` respectively.

Configuring partitioning
^^^^^^^^^^^^^^^^^^^^^^^^

By default the connector creates one partition per 400MB in the table being
read (before filtering). This should roughly correspond to the maximum number
of readers supported by the BigQuery Storage API. This can be configured
explicitly with the ``bigquery.parallelism`` property. BigQuery may limit the
number of partitions based on server constraints.

Reading from views
^^^^^^^^^^^^^^^^^^

The connector has a preliminary support for reading from `BigQuery views
<https://cloud.google.com/bigquery/docs/views-intro>`_. Please note there are
a few caveats:

* Reading from views is disabled by default. In order to enable it, set the
  ``bigquery.views-enabled`` configuration property to ``true``.
* BigQuery views are not materialized by default, which means that the
  connector needs to materialize them before it can read them. This process
  affects the read performance.
* The materialization process can also incur additional costs to your BigQuery bill.
* By default, the materialized views are created in the same project and
  dataset. Those can be configured by the optional ``bigquery.view-materialization-project``
  and ``bigquery.view-materialization-dataset`` properties, respectively. The
  service account must have write permission to the project and the dataset in
  order to materialize the view.

Configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^

===================================================== ============================================================== ======================================================
Property                                              Description                                                    Default
===================================================== ============================================================== ======================================================
``bigquery.project-id``                               The Google Cloud Project ID where the data reside              Taken from the service account
``bigquery.parent-project-id``                        The project ID Google Cloud Project to bill for the export     Taken from the service account
``bigquery.parallelism``                              The number of partitions to split the data into                The number of executors
``bigquery.views-enabled``                            Enables the connector to read from views and not only tables.  ``false``
                                                      Please read `this section <#reading-from-views>`_ before
                                                      enabling this feature.
``bigquery.view-materialization-project``             The project where the materialized view is going to be created The view's project
``bigquery.view-materialization-dataset``             The dataset where the materialized view is going to be created The view's dataset
``bigquery.views-cache-ttl``                          Duration for which the materialization of a view will be       ``15m``
                                                      cached and reused. Set to ``0ms`` to disable the cache.
``bigquery.max-read-rows-retries``                    The number of retries in case of retryable server issues       ``3``
``bigquery.credentials-key``                          The base64 encoded credentials key                             None. See the `requirements <#requirements>`_ section.
``bigquery.credentials-file``                         The path to the JSON credentials file                          None. See the `requirements <#requirements>`_ section.
``bigquery.case-insensitive-name-matching``           Match dataset and table names case-insensitively               ``false``
``bigquery.case-insensitive-name-matching.cache-ttl`` Duration for which remote dataset and table names will be      ``1m``
                                                      cached. Higher values reduce the number of API calls to
                                                      BigQuery but can cause newly created dataset or tables to not
                                                      be visible until the configured duration. Set to ``0ms`` to
                                                      disable the cache.
===================================================== ============================================================== ======================================================

Data types
----------

With a few exceptions, all BigQuery types are mapped directly to their Trino
counterparts. Here are all the mappings:

============== =============================== =============================================================================================================
BigQuery       Trino                           Notes
============== =============================== =============================================================================================================
``ARRAY``      ``ARRAY``
``BOOLEAN``    ``BOOLEAN``
``BYTES``      ``VARBINARY``
``DATE``       ``DATE``
``DATETIME``   ``TIMESTAMP(6)``
``FLOAT``      ``DOUBLE``
``GEOGRAPHY``  ``VARCHAR``                     In `Well-known text (WKT) <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>`_ format
``INTEGER``    ``BIGINT``
``NUMERIC``    ``DECIMAL(P,S)``                Defaults to ``38`` as precision and ``9`` as scale
``BIGNUMERIC`` ``DECIMAL(P,S)``                Precision > 38 is not supported. Note that the default precision and scale of BIGNUMERIC is ``(77, 38)``.
``RECORD``     ``ROW``
``STRING``     ``VARCHAR``
``TIME``       ``TIME(6)``
``TIMESTAMP``  ``TIMESTAMP(6) WITH TIME ZONE`` Time zone is UTC
============== =============================== =============================================================================================================

System tables
-------------

For each Trino table which maps to BigQuery view there exists a system table which exposes BigQuery view definition.
Given a BigQuery view ``customer_view`` you can send query
``SELECT * customer_view$view_definition`` to see the SQL which defines view in BigQuery.

.. _bigquery-sql-support:

SQL support
-----------

The connector provides read and write access to data and metadata in
the BigQuery database. In addition to the :ref:`globally available
<sql-globally-available>` and :ref:`read operation <sql-read-operations>`
statements, the connector supports the following features:

* :doc:`/sql/create-table`
* :doc:`/sql/drop-table`
* :doc:`/sql/create-schema`
* :doc:`/sql/drop-schema`

FAQ
---

What is the Pricing for the Storage API?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See the `BigQuery pricing documentation
<https://cloud.google.com/bigquery/pricing#storage-api>`_.
