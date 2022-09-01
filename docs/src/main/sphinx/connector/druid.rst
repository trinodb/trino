===============
Druid connector
===============

.. raw:: html

  <img src="../_static/img/druid.png" class="connector-logo">

The Druid connector allows querying an `Apache Druid <https://druid.apache.org/>`_
database from Trino.

Requirements
------------

To connect to Druid, you need:

* Druid version 0.18.0 or higher.
* Network access from the Trino coordinator and workers to your Druid broker.
  Port 8082 is the default port.

Configuration
-------------

Create a catalog properties file that specifies the Druid connector by setting
the ``connector.name`` to ``druid`` and configuring the ``connection-url`` with
the JDBC string to connect to Druid.

For example, to access a database as ``example``, create the file
``etc/catalog/example.properties``. Replace ``BROKER:8082`` with the correct
host and port of your Druid broker.

.. code-block:: properties

    connector.name=druid
    connection-url=jdbc:avatica:remote:url=http://BROKER:8082/druid/v2/sql/avatica/

You can add authentication details to connect to a Druid deployment that is
secured by basic authentication by updating the URL and adding credentials:

.. code-block:: properties

    connection-url=jdbc:avatica:remote:url=http://BROKER:port/druid/v2/sql/avatica/;authentication=BASIC
    connection-user=root
    connection-password=secret

Now you can access your Druid database in Trino with the ``druiddb`` catalog
name from the properties file.

.. include:: jdbc-common-configurations.fragment

.. include:: jdbc-procedures.fragment

.. include:: jdbc-case-insensitive-matching.fragment

.. _druid-type-mapping:

Type mapping
------------

.. include:: jdbc-type-mapping.fragment

.. _druid-sql-support:

SQL support
-----------

The connector provides :ref:`globally available <sql-globally-available>` and
:ref:`read operation <sql-read-operations>` statements to access data and
metadata in the Druid database.

Table functions
---------------

The connector provides specific :doc:`table functions </functions/table>` to
access Druid.

.. _druid-query-function:

``query(varchar) -> table``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``query`` function allows you to query the underlying database directly. It
requires syntax native to Druid, because the full query is pushed down and
processed in Druid. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

As an example, use ``STRING_TO_MV`` and ``MV_LENGTH`` from
`Druid SQL's multi-value string functions <https://druid.apache.org/docs/latest/querying/sql-multivalue-string-functions.html>`_
to split and then count the number of comma-separated values in a column::

    SELECT
      num_reports
    FROM
      TABLE(
        druid.system.query(
          query => 'SELECT
            MV_LENGTH(
              STRING_TO_MV(direct_reports, ",")
            ) AS num_reports
          FROM company.managers'
        )
      );

