
===============
Table functions
===============

Table functions return tables. They allow users to dynamically invoke custom
logic from within the SQL query. They are invoked in the ``FROM`` clause of a
query, and the calling convention is similar to a scalar function call. For
description of table functions usage, see
:doc:`table functions</functions/table>`.

Trino supports adding custom table functions. They are declared by connectors
through implementing dedicated interfaces.

Table function declaration
--------------------------

To declare a table function, you need to implement ``ConnectorTableFunction``.
Subclassing ``AbstractConnectorTableFunction`` is a convenient way to do it.
The connector's ``getTableFunctions()`` method must return a set of your
implementations.

The constructor
^^^^^^^^^^^^^^^

.. code-block:: java

    public class MyFunction
            extends AbstractConnectorTableFunction
    {
        public MyFunction()
        {
            super(
                    "system",
                    "my_function",
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name("COLUMN_COUNT")
                                    .type(INTEGER)
                                    .defaultValue(2)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("ROW_COUNT")
                                    .type(INTEGER)
                                    .build()),
                    GENERIC_TABLE);
        }
    }

The constructor takes the following arguments:

- **schema name**

The schema name helps you organize functions, and it is used for function
resolution. When a table function is invoked, the right implementation is
identified by the catalog name, the schema name, and the function name.

The function can use the schema name, for example to use data from the
indicated schema, or ignore it.

- **function name**
- **list of expected arguments**

You can specify default values for some arguments, and those arguments can be
skipped during invocation:

.. code-block:: java

    ScalarArgumentSpecification.builder()
            .name("COLUMN_COUNT")
            .type(INTEGER)
            .defaultValue(2)
            .build()

If you do not specify the default value, the argument is required during
invocation:

.. code-block:: java

    ScalarArgumentSpecification.builder()
            .name("ROW_COUNT")
            .type(INTEGER)
            .build()

- **returned row type**

In the example, the returned row type is ``GENERIC_TABLE``, which means that
the row type is not known statically, and it is determined dynamically based on
the passed arguments. When the returned row type is known statically, you can
declare it using:

.. code-block:: java

    new DescribedTable(descriptor)

The ``analyze()`` method
^^^^^^^^^^^^^^^^^^^^^^^^

In order to provide all the necessary information to the Trino engine, the
class must implement the ``analyze()`` method. This method is called by the
engine during the analysis phase of query processing. The ``analyze()`` method
is also the place to perform custom checks on the arguments:

.. code-block:: java

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        long columnCount = (long) ((ScalarArgument) arguments.get("COLUMN_COUNT")).getValue();
        long rowCount = (long) ((ScalarArgument) arguments.get("ROW_COUNT")).getValue();

        // custom validation of arguments
        if (columnCount < 1 || columnCount > 3) {
             throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "column_count must be in range [1, 3]");
        }

        if (rowCount < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "row_count must be positive");
        }

        // determine the returned row type
        List<Descriptor.Field> fields = List.of("col_a", "col_b", "col_c").subList(0, (int) columnCount).stream()
                .map(name -> new Descriptor.Field(name, Optional.of(BIGINT)))
                .collect(toList());

        Descriptor returnedType = new Descriptor(fields);

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .build();
    }

The ``analyze()`` method returns a ``TableFunctionAnalysis`` object, which
comprises all the information required by the engine to analyze, plan, and
execute the table function invocation:

- The returned row type, specified as an optional ``Descriptor``. It should be
  passed if and only if the table function is declared with the
  ``GENERIC_TABLE`` returned type.
- Required columns from the table arguments, specified as a map of table
  argument names to lists of column indexes.
- Any information gathered during analysis that is useful during planning or
  execution, in the form of a ``ConnectorTableFunctionHandle``.
  ``ConnectorTableFunctionHandle`` is a marker interface intended to carry
  information throughout subsequent phases of query processing in a manner that
  is opaque to the engine.

Table function execution
------------------------

Table functions are executed as pushdown to the connector. The connector that
provides a table function should implement the ``applyTableFunction()`` method.
This method is called during the optimization phase of query processing. It
returns a ``ConnectorTableHandle`` and a list of ``ColumnHandle`` s
representing the table function result. The table function invocation is then
replaced with a ``TableScanNode``.

Access control
--------------

The access control for table functions can be provided both on system and
connector level. It is based on the fully qualified table function name,
which consists of the catalog name, the schema name, and the function name,
in the syntax of ``catalog.schema.function``.
