===============
Pinot Connector
===============

The Pinot connector allows Presto to query data stored in
`Apache Pinot™ <https://pinot.apache.org/>`_.

Compatibility
-------------

The Pinot connector is compatible with all Pinot versions starting from 0.1.0.

Configuration
-------------

To configure the Pinot connector, create a catalog properties file
e.g. ``etc/catalog/pinot.properties`` with at least the following contents:

.. code-block:: none

    connector.name=pinot
    pinot.controller-urls=host1:9000,host2:9000

Replace ``host1:9000,host2:9000`` with a comma-separated list of Pinot Controller nodes.
This can be the ip or the FDQN, the url scheme (``http://``) is optional.

Configuration Properties
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

Querying Pinot Tables
-------------------------

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

Dynamic Tables
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
Pinot                        Presto
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

