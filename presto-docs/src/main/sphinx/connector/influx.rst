================
Influx Connector
================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The Influx connector allows querying data-points stored in an
`InfluxDB <https://www.influxdata.com/products/influxdb-overview/>`_
Time Series Database.

Configuration
-------------

The following configuration properties are available:

=================================== =============== ======================================================================
Property Name                       Default         Comment
=================================== =============== ======================================================================
``connector.name=``                                 e.g. influx
``influx.host=``                    localhost
``influx.port=``                    8086
``influx.connection-timeout=``      10              Seconds
``influx.write-timeout=``           10
``influx.read-timeout=``            180
``influx.use-https=``               false
``influx.database=``                                The database name must be specified.  Each instance of the connector
                                                    can only connect to a single database on a server
``influx.username=``                                Optional
``influx.password=``                                Optional
``influx.cache-meta-data-millis=``  10000           How long to cache schema info e.g. measurement names before refreshing
=================================== =============== ======================================================================

Limitations
-----------

* Only SELECT queries are supported
* Performance beware: complicated filter expressions and aggregation are not pushed down; it is likely you are retrieving
  all the information from the InfluxDB server and then filtering and aggregating that large data-set inside Presto
* InfluxDB has case-sensitive identifiers, whereas prestosql is case-insenstive.  The influx connector will report an error
  if two identifiers differ only in case, and therefore are ambiguous
* LDAP on InfluxDB Enterprise editions is not supported
