===============
Pinot connector
===============

The Pinot connector allows Trino to query data stored in
`Apache Pinot™ <https://pinot.apache.org/>`_.

Requirements
------------

To connect to Pinot, you need:

* Pinot 0.1.0 or higher.
* Network access from the Trino coordinator and workers to the Pinot controller
  nodes. Port 8098 is the default port.

Configuration
-------------

To configure the Pinot connector, create a catalog properties file
e.g. ``etc/catalog/pinot.properties`` with at least the following contents:

.. code-block:: text

    connector.name=pinot
    pinot.controller-urls=host1:8098,host2:8098

Replace ``host1:8098,host2:8098`` with a comma-separated list of Pinot controller nodes.
This can be the ip or the FDQN, the url scheme (``http://``) is optional.

Configuration properties
------------------------

The following configuration properties are available:

============================== ========== ==============================================================================
Property Name                  Required   Description
============================== ========== ==============================================================================
``pinot.controller-urls``      Yes        A comma separated list of controller hosts. If Pinot is deployed via
                                          `Kubernetes <https://kubernetes.io/>`_ this needs to point to the controller
                                          service endpoint. The Pinot broker and server must be accessible via DNS as
                                          Pinot returns hostnames and not IP addresses.
``pinot.segments-per-split``   No         The number of segments processed in a split. Setting this higher reduces the
                                          number of requests made to Pinot. This is useful for smaller Pinot clusters.
``pinot.request-timeout``      No         The timeout for Pinot requests. Increasing this can reduce timeouts if DNS
                                          resolution is slow.
============================== ========== ==============================================================================

Querying Pinot tables
---------------------

The Pinot connector automatically exposes all tables in the default schema of the catalog.
You can list all tables in the pinot catalog with the following query::

    SHOW TABLES FROM pinot.default;

You can list columns in the flight_status table::

    DESCRIBE pinot.default.flight_status;
    SHOW COLUMNS FROM pinot.default.flight_status;

Queries written with SQL are fully supported and can include filters and limits::

    SELECT foo
    FROM pinot_table
    WHERE bar = 3 AND baz IN ('ONE', 'TWO', 'THREE')
    LIMIT 25000;

Dynamic tables
--------------

To leverage Pinot's fast aggregation, a Pinot query written in PQL can be used as the table name.
Filters and limits in the outer query are pushed down to Pinot.
Let's look at an example query::

    SELECT *
    FROM pinot.default."SELECT MAX(col1), COUNT(col2) FROM pinot_table GROUP BY col3, col4"
    WHERE col3 IN ('FOO', 'BAR') AND col4 > 50
    LIMIT 30000

Filtering and limit processing is pushed down to Pinot.

The queries are routed to the broker and are more suitable to aggregate queries.

For ``SELECT`` queries without aggregates it is more performant to issue a regular SQL query.
Processing is routed directly to the servers that store the data.

The above query is translated to the following Pinot PQL query::

    SELECT MAX(col1), COUNT(col2)
    FROM pinot_table
    WHERE col3 IN('FOO', 'BAR') and col4 > 50
    TOP 30000



Data types
----------

Pinot does not allow null values in any data type and supports the following primitive types:

==========================   ============
Pinot                        Trino
==========================   ============
``INT``                      ``INTEGER``
``LONG``                     ``BIGINT``
``FLOAT``                    ``REAL``
``DOUBLE``                   ``DOUBLE``
``STRING``                   ``VARCHAR``
``INT_ARRAY``                ``VARCHAR``
``LONG_ARRAY``               ``VARCHAR``
``FLOAT_ARRAY``              ``VARCHAR``
``DOUBLE_ARRAY``             ``VARCHAR``
``STRING_ARRAY``             ``VARCHAR``
==========================   ============

.. _pinot-sql-support:

SQL support
-----------

The connector provides :ref:`globally available <sql-globally-available>` and
:ref:`read operation <sql-read-operations>` statements to access data and
metadata in Pinot.

.. _pinot-pushdown:

Pushdown
--------

The connector supports pushdown for a number of operations:

* :ref:`limit-pushdown`

:ref:`Aggregate pushdown <aggregation-pushdown>` for the following functions:

* :func:`avg`
* :func:`approx_distinct`
* ``count(*)`` and ``count(distinct)`` variations of :func:`count`
* :func:`max`
* :func:`min`
* :func:`sum`

Aggregate function pushdown is enabled by default, but can be disabled with the
catalog property ``pinot.aggregation-pushdown.enabled`` or the catalog session
property ``aggregation_pushdown_enabled``.

A ``count(distint)`` pushdown may cause Pinot to run a full table scan with
significant performance impact. If you encounter this problem, you can disable
it with the catalog property ``pinot.count-distinct-pushdown.enabled`` or the
catalog session property ``count_distinct_pushdown_enabled``.
