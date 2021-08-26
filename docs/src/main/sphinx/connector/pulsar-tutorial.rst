=========================
Pulsar connector tutorial
=========================

Overview
========

The :doc:`pulsar` for Trino allows access to live topic data from Apache Pulsar
using Trino. 

This tutorial walks you through every step of using the Pulsar connector to
query data in Pulsar topics on a single node, including how to start Pulsar,
load data to Pulsar topics, configure the Pulsar connector, start Trino, query
data, and more.


Prerequisites
=============

* Pulsar 2.8.0 or later versions

Step 1: start Pulsar
--------------------

For how to install and start Pulsar in standalone mode, see `install Pulsar
standalone
<https://pulsar.apache.org/docs/en/next/standalone/#install-pulsar-standalone>`_.

Step 2: load data
-----------------

1. Download the ``examples`` repository.

.. code-block:: text

    git clone https://github.com/streamnative/examples

2. Build the `taxidata CLI tool
<https://github.com/streamnative/examples/tree/master/nyctaxi/taxidata>`_.

.. code-block:: text

    cd examples/nyctaxi/taxidata
    go build

3. Ingest data to Pulsar topics using the taxidata CLI tool. 

.. code-block:: text

    ./taxidata \                                                                 
    --topicNameGreen public/default/greenTaxi \
    --topicNameYellow public/default/yellowTaxi \
    --pulsarUrl pulsar://localhost:6650 \
    --speed 1

You can get two tables (greentaxi and yellowtaxi). For the data schema of these
tables, see `here
<https://github.com/streamnative/examples/tree/master/nyctaxi/taxidata/pkg/types>`_.

The taxidata CLI tool keeps ingesting data into Pulsar topics until it ingests
all data successfully (which may take several hours) or it is stopped. You can
proceed with the next steps once you execute the above command successfully and
do not need to wait until it ingests all data. 

.. note::

    After finishing loading data, you might not get any table when you query
    Pulsar data, that is because the topic is garbage collected by Pulsar as
    there is no active producer or consumer on that topic for a while. This can
    be configured but for this quick start the simplest solution is to
    re-execute the above command, then you can get the tables.

Step 3: configure Pulsar connector
----------------------------------

In the `Trino installation directory
<https://trino.io/docs/current/installation/deployment.html#installing-trino>`_,
create `etc/catalog
<https://trino.io/docs/current/installation/deployment.html#catalog-properties>`_
directory (if it does not exist) and then create ``apachepulsar.properties``
configuration file under that directory with the minimal configurations as
below. For how to configure the Pulsar connector, see :doc:`pulsar`. 

.. code-block:: text

    connector.name=pulsar
    pulsar.web-service-url=http://localhost:8080
    pulsar.zookeeper-uri=localhost:2181

Step 4: start Trino 
-------------------

1. Configure Trino.

Pulsar admin uses port 8080, so you need to change the Trino HTTP port to
another available port by updating ``http-server.http.port`` and
``discovery.uri`` properties in the Trino configuration file
(``etc/config.properties``) as below. For more information, see `Configuring
Trino
<https://trino.io/docs/current/installation/deployment.html#config-properties>`_.

.. code-block:: text

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8181
    query.max-memory=5GB

    query.max-memory-per-node=1GB
    query.max-total-memory-per-node=2GB
    discovery-server.enabled=true
    discovery.uri=http://localhost:8181

2. Start Trino in standalone mode.

For more information, see `Running Trino
<https://trino.io/docs/current/installation/deployment.html#running-trino>`_.

Step 5: query data 
------------------

1. `Install and start Trino CLI
<https://trino.io/docs/current/installation/cli.html>`_.

2. In the Trino installation directory, invoke the Trino CLI. 

Replace ``:{trino-port}`` with your actual Trino HTTP port

.. code-block:: text

    ./trino --server localhost:{trino-port} --catalog apachepulsar

3. Query data from Pulsar topics through Trino CLI.

3.1 Get information about tables.

.. code-block:: text

    trino> use apachepulsar."public/default";
    USE
        
    trino:public/default> SHOW TABLES;
    Table
    ------------
    greentaxi
    yellowtaxi
    (2 rows)

    Query 20210615_134032_00006_mt6v6, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    3.21 [2 rows, 67B] [0 rows/s, 21B/s]

3.2 Get data of the greentaxi table.

.. code-block:: text

    trino:public/default> describe greentaxi;
            Column        |  Type   | Extra |                                   Comment
    ----------------------+---------+-------+-----------------------------------------------------------------------------
    vendorid             | integer |       | "int"
    pickupdatetime       | bigint  |       | "long"
    dropoffdatetime      | bigint  |       | "long"
    storeandfwdflag      | boolean |       | "boolean"
    ratecodeid           | integer |       | "int"
    pickuplocationid     | integer |       | "int"
    dropofflocationid    | integer |       | "int"
    passengercount       | integer |       | "int"
    tripdistance         | double  |       | "double"
    fareamount           | double  |       | "double"
    extra                | double  |       | "double"
    mtatax               | double  |       | "double"
    tipamount            | double  |       | "double"
    tollsamount          | double  |       | "double"
    ehailfee             | double  |       | "double"
    improvementsurcharge | double  |       | "double"
    totalamount          | double  |       | "double"
    paymenttype          | integer |       | "int"
    triptype             | integer |       | "int"
    congestionsurcharge  | double  |       | "double"
    __partition__        | integer |       | The partition number which the message belongs to
    __message_id__       | varchar |       | The message ID of the message used to generate this row
    __sequence_id__      | bigint  |       | The sequence ID of the message used to generate this row
    __producer_name__    | varchar |       | The name of the producer that publish the message used to generate this row
    __key__              | varchar |       | The partition key for the topic
    __properties__       | varchar |       | User defined properties
    (26 rows)

    Query 20210615_134959_00014_mt6v6, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    5.87 [26 rows, 2.47KB] [4 rows/s, 430B/s]

The taxidata tool keeps ingesting data to Pulsar topic when it is running, so
you can get different results if performing the same query multiple times since
you are getting more data.

.. code-block:: text

    trino:public/default> select count(*) from greentaxi;
    _col0
    -------
    1350
    (1 row)

    Query 20210615_135021_00015_mt6v6, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    5.68 [6.01K rows, 2.63MB] [1.06K rows/s, 475KB/s]

    trino:public/default> select count(*) from greentaxi;
    _col0
    -------
    1410
    (1 row)

    Query 20210615_135319_00000_7ttpw, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    6.47 [7.8K rows, 2.98MB] [1.21K rows/s, 471KB/s]

    trino:public/default> select count(*) from greentaxi;
    _col0
    -------
    1440
    (1 row)

    Query 20210615_135923_00000_7ttpw, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    6.25 [6.3K rows, 2.74MB] [1.25K rows/s, 462KB/s]

    trino:public/default> select * from greentaxi limit 5;
    vendorid | pickupdatetime | dropoffdatetime | storeandfwdflag | ratecodeid | pickuplocationid | dropofflocationid | passengercount | tripdistanc
    ----------+----------------+-----------------+-----------------+------------+------------------+-------------------+----------------+------------
            2 |     1545423449 |      1545423537 | false           |          1 |              264 |               264 |              5 |          0.
            2 |     1546319416 |      1546319792 | false           |          1 |               97 |                49 |              2 |         0.8
            2 |     1546320431 |      1546320698 | false           |          1 |               49 |               189 |              2 |         0.6
            2 |     1546321580 |      1546322694 | false           |          1 |              189 |                17 |              2 |         2.6
            2 |     1546319946 |      1546321183 | false           |          1 |               82 |               258 |              1 |         4.5
    (5 rows)

    Query 20210615_135415_00001_7ttpw, FINISHED, 1 node
    Splits: 19 total, 19 done (100.00%)
    4.81 [8.34K rows, 3.46MB] [1.74K rows/s, 737KB/s]
    
3.3 Perform a query joining two tables.

.. code-block:: text

    trino:public/default> select greentaxi.pickuplocationid, date_format(from_unixtime(greentaxi.pickupdatetime), '%Y-%m-%d %h:%i:%s') from greentaxi join yellowtaxi on greentaxi.pickuplocationid = yellowtaxi.pickuplocationid;

    pickuplocationid |          _col1
    ------------------+-------------------------
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                    7 | 2019-01-01 04:08:05.000
                225 | 2019-01-01 04:48:22.000
                225 | 2019-01-01 04:48:22.000
                225 | 2019-01-01 04:48:22.000
                225 | 2019-01-01 04:48:22.000
                82 | 2019-01-01 04:02:43.000
                82 | 2019-01-01 04:02:43.000
                82 | 2019-01-01 04:02:43.000

    ……

Until now you have played with the Pulsar connector successfully.