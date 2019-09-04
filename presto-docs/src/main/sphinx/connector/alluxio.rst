=========================
Alluxio Data Orchestrator
=========================


.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

`Alluxio <www.alluxio.io>`_ is a data orchestration open source distributed data orchestration layer for the cloud which provides a block-level read/write caching engine for Presto connecting to a variety of disparate storage systems including S3 and HDFS. Presto can query files stored in Alluxio through the Hive connector. 

Basic Configuration
-------------------

Presto can use the Hive Metastore for the database and table metadata information, as well as the file system location of table data. In order to enable this, create and edit the Presto configuration ``etc/catalog/hive.properties``:

``connector.name=hive-hadoop2``
``hive.metastore.uri=thrift://localhost:9083``

Additional Alluxio Configuration
--------------------------------

To configure Alluxio client-side properties on Presto, append the conf path (i.e. ``${ALLUXIO_HOME}/conf``) containing ``alluxio-site.properties`` to Presto’s JVM config at ``etc/jvm.config`` under Presto folder. The advantage of this approach is that all the Alluxio properties are set in the single ``alluxio-site.properties`` file. Additional Alluxio Configs can be found `here <https://docs.alluxio.io/os/user/2.0/en/compute/Presto.html#customize-alluxio-user-properties>`_


``...``

``-Xbootclasspath/p:<path-to-alluxio-conf>``



Alternatively, add Alluxio Configuration Properties to the Hadoop conf files (``core-site.xml``, ``hdfs-site.xml``), and use Presto property ``hive.config.resources`` in file ``etc/catalog/hive.properties`` to point to the file’s location for every Presto worker.

``hive.config.resources=core-site.xml,hdfs-site.xml``

Examples
---------

The Hive connector supports querying and manipulating Hive tables and schemas (databases). 

Create a new Hive schema named web that will store tables in Alluxio directory named ``my-table``:


``CREATE SCHEMA hive.web WITH (location = 'alluxio://master:port/my-table/')``


Create a new Hive table named ``page_views`` in the web schema that is stored using the ORC file format, partitioned by date and country, and bucketed by the user into 50 buckets (note that Hive requires the partition columns to be the last columns in the table):

.. code-block:: sql

  CREATE TABLE hive.web.page_views (
    view_time timestamp,
    user_id bigint,
    page_url varchar,
    ds date,
    country varchar
  )
  WITH (
    format = 'ORC',
    partitioned_by = ARRAY['ds', 'country'],
    bucketed_by = ARRAY['user_id'],
    bucket_count = 50
  )


Drop a partition from the ``page_views`` table.

.. code-block:: sql

  DELETE FROM hive.web.page_views
  WHERE ds = DATE '2016-08-09'
    AND country = 'US'


Query the ``page_views`` table:

.. code-block:: sql

  SELECT * FROM hive.web.page_views


List the partitions of the ``page_views`` table:

.. code-block:: sql

  SELECT * FROM hive.web."page_views$partitions"


Create an external Hive table named ``request_logs`` that points at existing data in Alluxio:

.. code-block:: sql

  CREATE TABLE hive.web.request_logs (
    request_time timestamp,
    url varchar,
    ip varchar,
    user_agent varchar
  )
  WITH (
    format = 'TEXTFILE',
    external_location = 'alluxio://master:port/my-table/data/logs/'
  )


Drop the external table ``request_logs``. This only drops the metadata for the table. The referenced data directory is not deleted.  Note that, this requires ``hive.allow-drop-table`` is set to ``true`` in ``etc/catalog/hive.properties``:

.. code-block:: sql

  DROP TABLE hive.web.request_logs
  
Additional Resources
--------------------
- `What is Alluxio? <https://www.alluxio.io/?utm_source=starburst&utm_medium=prestodocs>`_
- `Presto with Alluxio <https://www.alluxio.io/presto/?utm_source=starburst&utm_medium=prestodocs>`_
- `Performance tuning tips for Presto with Alluxio <https://www.alluxio.io/blog/top-5-performance-tuning-tips-for-running-presto-on-alluxio-1/?utm_source=starburst&utm_medium=prestodocs>`_
