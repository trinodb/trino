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

================================================== ======================================================================
Property Name                                      Description
================================================== ======================================================================
``connector.name=influx``
``influx.host=``                                   Default localhost
``influx.port=``                                   Default 8086
``influx.use-https=``                              Default false
``influx.database=``                               The database name must be specified.  Each instance of the connector
                                                   can only connect to a single database on a server
``influx.username=``
``influx.password=``
``influx.cache-meta-data-millis=``                 How long to cache schema info e.g. measurement names before refreshing
================================================== ======================================================================

Limitations
-----------

* Only SELECT queries are supported
* InfluxDB has case-sensitive identifiers, whereas prestosql is case-insenstive.  The influx connector will report an error
  if two identifiers differ only in case, and therefore are ambiguous
* authentication and https support is untested
* LDAP on InfluxDB Enterprise editions is not supported