========
SET PATH
========

Synopsis
--------

.. code-block:: text

    SET PATH path-element[, ...]

Description
-----------

Define a collection of paths to functions or table functions in specific
catalogs and schemas for the current session.

Each path-element uses a period-separated syntax to specify the catalog name and
schema location ``<catalog>.<schema>`` of the function, or only the schema
location ``<schema>`` in the current catalog. The current catalog is set with
:doc:`use`, or as part of a client tool connection. Catalog and schema must
exist.

Examples
--------

The following example sets a path to access functions in the ``system`` schema
of the ``example`` catalog::

    SET PATH example.system;

The catalog uses the PostgreSQL connector, and you can therefore use the
:ref:`query table function <postgresql-query-function>` directly, without the
full catalog and schema qualifiers::

    SELECT
      *
    FROM
      TABLE(
        query(
          query => 'SELECT
            *
          FROM
            tpch.nation'
        )
      );

See also
--------

:doc:`use`

