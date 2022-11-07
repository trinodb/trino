===============
Table functions
===============

A table function is a function returning a table. It can be invoked inside the
``FROM`` clause of a query::

    SELECT * FROM TABLE(my_function(1, 100))

The row type of the returned table can depend on the arguments passed with
invocation of the function. If different row types can be returned, the
function is a **polymorphic table function**.

Polymorphic table functions allow you to dynamically invoke custom logic from
within the SQL query. They can be used for working with external systems as
well as for enhancing Trino with capabilities going beyond the SQL standard.

Trino supports adding custom table functions. They are declared by connectors
through implementing dedicated interfaces. For guidance on adding new table
functions, see the :doc:`developer guide</develop/table-functions>`.

Connectors offer support for different functions on a per-connector basis. For
more information about supported table functions, refer to the :doc:`connector
documentation <../../connector>`.

Table function invocation
-------------------------

You invoke a table function in the ``FROM`` clause of a query. Table function
invocation syntax is similar to a scalar function call.

Function resolution
^^^^^^^^^^^^^^^^^^^

Every table function is provided by a catalog, and it belongs to a schema in
the catalog. You can qualify the function name with a schema name, or with
catalog and schema names::

    SELECT * FROM TABLE(schema_name.my_function(1, 100))
    SELECT * FROM TABLE(catalog_name.schema_name.my_function(1, 100))

Otherwise, the standard Trino name resolution is applied. The connection
between the function and the catalog must be identified, because the function
is executed by the corresponding connector. If the function is not registered
by the specified catalog, the query fails.

The table function name is resolved case-insensitive, analogically to scalar
function and table resolution in Trino.

Argument passing conventions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are two conventions of passing arguments to a table function:

- **Arguments passed by name**::

    SELECT * FROM TABLE(my_function(row_count => 100, column_count => 1))

In this convention, you can pass the arguments in arbitrary order. Arguments
declared with default values can be skipped. Argument names are resolved
case-sensitive, and with automatic uppercasing of unquoted names.

- **Arguments passed positionally**::

    SELECT * FROM TABLE(my_function(1, 100))

In this convention, you must follow the order in which the arguments are
declared. You can skip a suffix of the argument list, provided that all the
skipped arguments are declared with default values.

You cannot mix the argument conventions in one invocation.

All arguments must be constant expressions, and they can be of any SQL type,
which is compatible with the declared argument type. You can also use
parameters in arguments::

    PREPARE stmt FROM
    SELECT * FROM TABLE(my_function(row_count => ? + 1, column_count => ?));

    EXECUTE stmt USING 100, 1;
