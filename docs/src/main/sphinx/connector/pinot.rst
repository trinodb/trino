===============
Pinot connector
===============

.. raw:: html

  <img src="../_static/img/pinot.png" class="connector-logo">

The Pinot connector allows Trino to query data stored in
`Apache Pinot™ <https://pinot.apache.org/>`_.

Requirements
------------

To connect to Pinot, you need:

* Pinot 0.11.0 or higher.
* Network access from the Trino coordinator and workers to the Pinot controller
  nodes. Port 8098 is the default port.

Configuration
-------------

To configure the Pinot connector, create a catalog properties file
e.g. ``etc/catalog/example.properties`` with at least the following contents:

.. code-block:: text

    connector.name=pinot
    pinot.controller-urls=host1:8098,host2:8098

Replace ``host1:8098,host2:8098`` with a comma-separated list of Pinot controller nodes.
This can be the ip or the FDQN, the url scheme (``http://``) is optional.

Configuration properties
------------------------

General configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

========================================================= ========== ==============================================================================
Property name                                             Required   Description
========================================================= ========== ==============================================================================
``pinot.controller-urls``                                 Yes        A comma separated list of controller hosts. If Pinot is deployed via
                                                                     `Kubernetes <https://kubernetes.io/>`_ this needs to point to the controller
                                                                     service endpoint. The Pinot broker and server must be accessible via DNS as
                                                                     Pinot returns hostnames and not IP addresses.
``pinot.connection-timeout``                              No         Pinot connection timeout, default is ``15s``.
``pinot.metadata-expiry``                                 No         Pinot metadata expiration time, default is ``2m``.
``pinot.request-timeout``                                 No         The timeout for Pinot requests. Increasing this can reduce timeouts if DNS
                                                                     resolution is slow.
``pinot.controller.authentication.type``                  No         Pinot authentication method for controller requests. Allowed values are
                                                                     ``NONE`` and ``PASSWORD`` - defaults to ``NONE`` which is no authentication.
``pinot.controller.authentication.user``                  No         Controller username for basic authentication method.
``pinot.controller.authentication.password``              No         Controller password for basic authentication method.
``pinot.broker.authentication.type``                      No         Pinot authentication method for broker requests. Allowed values are
                                                                     ``NONE`` and ``PASSWORD`` - defaults to ``NONE`` which is no
                                                                     authentication.
``pinot.broker.authentication.user``                      No         Broker username for basic authentication method.
``pinot.broker.authentication.password``                  No         Broker password for basic authentication method.
``pinot.max-rows-per-split-for-segment-queries``          No         Fail query if Pinot server split returns more rows than configured, default to
                                                                     ``50,000`` for non-gRPC connection, ``2,147,483,647`` for gRPC connection.
``pinot.estimated-size-in-bytes-for-non-numeric-column``  No         Estimated byte size for non-numeric column for page pre-allocation in non-gRPC
                                                                     connection, default is ``20``.
``pinot.prefer-broker-queries``                           No         Pinot query plan prefers to query Pinot broker, default is ``true``.
``pinot.forbid-segment-queries``                          No         Forbid parallel querying and force all querying to happen via the broker,
                                                                     default is ``false``.
``pinot.segments-per-split``                              No         The number of segments processed in a split. Setting this higher reduces the
                                                                     number of requests made to Pinot. This is useful for smaller Pinot clusters,
                                                                     default is ``1``.
``pinot.fetch-retry-count``                               No         Retry count for retriable Pinot data fetch calls, default is ``2``.
``pinot.non-aggregate-limit-for-broker-queries``          No         Max limit for non aggregate queries to the Pinot broker, default is ``25,000``.
``pinot.max-rows-for-broker-queries``                     No         Max rows for a broker query can return, default is ``50,000``.
``pinot.aggregation-pushdown.enabled``                    No         Push down aggregation queries, default is ``true``.
``pinot.count-distinct-pushdown.enabled``                 No         Push down count distinct queries to Pinot, default is ``true``.
``pinot.target-segment-page-size``                        No         Max allowed page size for segment query, default is ``1MB``.
``pinot.proxy.enabled``                                   No         Use Pinot Proxy for controller and broker requests, default is ``false``.
========================================================= ========== ==============================================================================

If ``pinot.controller.authentication.type`` is set to ``PASSWORD`` then both ``pinot.controller.authentication.user`` and
``pinot.controller.authentication.password`` are required.

If ``pinot.broker.authentication.type`` is set to ``PASSWORD`` then both ``pinot.broker.authentication.user`` and
``pinot.broker.authentication.password`` are required.

If ``pinot.controller-urls`` uses ``https`` scheme then TLS is enabled for all connections including brokers.

gRPC configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

========================================================= ========== ==============================================================================
Property name                                             Required   Description
========================================================= ========== ==============================================================================
``pinot.grpc.enabled``                                    No         Use gRPC endpoint for Pinot server queries, default is ``true``.
``pinot.grpc.port``                                       No         Pinot gRPC port, default to ``8090``.
``pinot.grpc.max-inbound-message-size``                   No         Max inbound message bytes when init gRPC client, default is ``128MB``.
``pinot.grpc.use-plain-text``                             No         Use plain text for gRPC communication, default to ``true``.
``pinot.grpc.tls.keystore-type``                          No         TLS keystore type for gRPC connection, default is ``JKS``.
``pinot.grpc.tls.keystore-path``                          No         TLS keystore file location for gRPC connection, default is empty.
``pinot.grpc.tls.keystore-password``                      No         TLS keystore password, default is empty.
``pinot.grpc.tls.truststore-type``                        No         TLS truststore type for gRPC connection, default is ``JKS``.
``pinot.grpc.tls.truststore-path``                        No         TLS truststore file location for gRPC connection, default is empty.
``pinot.grpc.tls.truststore-password``                    No         TLS truststore password, default is empty.
``pinot.grpc.tls.ssl-provider``                           No         SSL provider, default is ``JDK``.
``pinot.grpc.proxy-uri``                                  No         Pinot Rest Proxy gRPC endpoint URI, default is null.
========================================================= ========== ==============================================================================

For more Apache Pinot TLS configurations, please also refer to `Configuring TLS/SSL <https://docs.pinot.apache.org/operators/tutorials/configuring-tls-ssl>`_.

You can use :doc:`secrets </security/secrets>` to avoid actual values in the catalog properties files.

Querying Pinot tables
---------------------

The Pinot connector automatically exposes all tables in the default schema of the catalog.
You can list all tables in the pinot catalog with the following query::

    SHOW TABLES FROM example.default;

You can list columns in the flight_status table::

    DESCRIBE example.default.flight_status;
    SHOW COLUMNS FROM example.default.flight_status;

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
    FROM example.default."SELECT MAX(col1), COUNT(col2) FROM pinot_table GROUP BY col3, col4"
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

.. _pinot-type-mapping:

Type mapping
------------

Because Trino and Pinot each support types that the other does not, this
connector :ref:`maps some types <type-mapping-overview>` when reading data.

Pinot type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Pinot types to the corresponding Trino types
according to the following table:

.. list-table:: Pinot type to Trino type mapping
  :widths: 75,60
  :header-rows: 1

  * - Pinot type
    - Trino type
  * - ``INT``
    - ``INTEGER``
  * - ``LONG``
    - ``BIGINT``
  * - ``FLOAT``
    - ``REAL``
  * - ``DOUBLE``
    - ``DOUBLE``
  * - ``STRING``
    - ``VARCHAR``
  * - ``BYTES``
    - ``VARBINARY``
  * - ``JSON``
    - ``JSON``
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
  * - ``INT_ARRAY``
    - ``VARCHAR``
  * - ``LONG_ARRAY``
    - ``VARCHAR``
  * - ``FLOAT_ARRAY``
    - ``VARCHAR``
  * - ``DOUBLE_ARRAY``
    - ``VARCHAR``
  * - ``STRING_ARRAY``
    - ``VARCHAR``

Pinot does not allow null values in any data type.

No other types are supported.

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

.. include:: pushdown-correctness-behavior.fragment
