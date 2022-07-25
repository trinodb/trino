************
SQL language
************

Trino is an ANSI SQL compliant query engine. This standard compliance allows
Trino users to integrate their favorite data tools, including BI and ETL tools
with any underlying data source.

Trino validates and translates the received SQL statements into the necessary
operations on the connected data source.

This chapter provides a reference to the supported SQL data types and other
general characteristics of the SQL support of Trino.

A :doc:`full SQL statement and syntax reference<sql>` is
available in a separate chapter.

Trino also provides :doc:`numerous SQL functions and operators<functions>`.

.. toctree::
    :maxdepth: 2

    language/sql-support
    language/types
    language/reserved
