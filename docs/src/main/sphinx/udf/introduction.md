# Introduction to user-defined functions

A user-defined function (UDF) is a custom function authored by a user of Trino
in a client application. UDFs are scalar functions that return a single output
value, similar to [built-in functions](/functions).

[Declare the UDF](udf-declaration) with a `FUNCTION` definition using the
supported statements. A UDF can be declared and used as an [inline
UDF](udf-inline) or declared as a [catalog UDF](udf-catalog) and used
repeatedly.

UDFs are defined and written using the [SQL routine language](/udf/sql). 

(udf-inline)=
## Inline user-defined functions

An inline user-defined function (inline UDF) declares and uses the UDF within a
query processing context. The UDF is declared in a `WITH` block before the
query:

```sql
WITH
  FUNCTION abc(x integer)
    RETURNS integer
    RETURN x * 2
SELECT abc(21);
```

Inline UDF names must follow SQL identifier naming conventions, and cannot
contain `.` characters.

The UDF declaration is only valid within the context of the query. A separate
later invocation of the UDF is not possible. If this is desired, use a [catalog
UDF](udf-catalog).

Multiple inline UDF declarations are comma-separated, and can include UDFs
calling each other, as long as a called UDF is declared before the first
invocation.

```sql
WITH
  FUNCTION abc(x integer)
    RETURNS integer
    RETURN x * 2,
  FUNCTION xyz(x integer)
    RETURNS integer
    RETURN abc(x) + 1
SELECT xyz(21);
```

Note that inline UDFs can mask and override the meaning of a built-in function:

```sql
WITH
  FUNCTION abs(x integer)
    RETURNS integer
    RETURN x * 2
SELECT abs(-10); -- -20, not 10!
```

(udf-catalog)=
## Catalog user-defined functions

You can store a UDF in the context of a catalog, if the connector used in the
catalog supports UDF storage. The following connectors support catalog UDF
storage:

* [](/connector/hive)
* [](/connector/memory)

In this scenario, the following commands can be used:

* [](/sql/create-function) to create and store a UDF.
* [](/sql/drop-function) to remove a UDF.
* [](/sql/show-functions) to display a list of UDFs in a catalog.

Catalog UDFs must use a name that combines the catalog name and schema name with
the UDF name, such as `example.default.power` for the `power` UDF in the
`default` schema of the `example` catalog.

Invocation must use the fully qualified name, such as `example.default.power`.

(udf-sql-environment)=
## SQL environment configuration for UDFs

Configuration of the `sql.default-function-catalog` and
`sql.default-function-schema` [](/admin/properties-sql-environment) allows you
to set the default storage for UDFs. The catalog and schema must be added to the
`sql.path` as well. This enables users to call UDFs and perform all
[](udf-management) without specifying the full path to the UDF.

:::{note}
Use the [](/connector/memory) in a catalog for simple storing and
testing of your UDFs.
:::

(udf-declaration)=
## UDF declaration

Refer to the documentation for the [](/udf/function) keyword for more
details about declaring the UDF overall. The UDF body is composed with
statements from the following list:

* [](/udf/sql/begin)
* [](/udf/sql/case)
* [](/udf/sql/declare)
* [](/udf/sql/if)
* [](/udf/sql/iterate)
* [](/udf/sql/leave)
* [](/udf/sql/loop)
* [](/udf/sql/repeat)
* [](/udf/sql/return)
* [](/udf/sql/set)
* [](/udf/sql/while)

Statements can also use [built-in functions and operators](/functions) as well
as other UDFs, although recursion is not supported for UDFs.

Find simple examples in each statement documentation, and refer to the
[](/udf/sql/examples) for more complex use cases that combine multiple
statements.

:::{note}
User-defined functions can alternatively be written in Java and deployed as a
plugin. Details are available in the [developer guide](/develop/functions).
:::

(udf-sql-label)=
## Labels

SQL UDFs can contain labels as markers for a specific block in the declaration
before the following keywords:

* `CASE`
* `IF`
* `LOOP`
* `REPEAT`
* `WHILE`

The label is used to name the block to continue processing with the `ITERATE`
statement or exit the block with the `LEAVE` statement. This flow control is
supported for nested blocks, allowing to continue or exit an outer block, not
just the innermost block. For example, the following snippet uses the label
`top` to name the complete block from `REPEAT` to `END REPEAT`:

```sql
top: REPEAT
  SET a = a + 1;
  IF a <= 3 THEN
    ITERATE top;
  END IF;
  SET b = b + 1;
  UNTIL a >= 10
END REPEAT;
```

Labels can be used with the `ITERATE` and `LEAVE` statements to continue
processing the block or leave the block. This flow control is also supported for
nested blocks and labels.

## Recommendations

Processing UDFs can potentially be resource intensive on the cluster in
terms of memory and processing. Take the following considerations into account
when writing and running SQL UDFs:

* Some checks for the runtime behavior of UDF are in place. For example,
  UDFs that take longer to process than a hardcoded threshold are
  automatically terminated.
* Avoid creation of arrays in a looping construct. Each iteration creates a
  separate new array with all items and copies the data for each modification,
  leaving the prior array in memory for automated clean up later. Use a [lambda
  expression](/functions/lambda) instead of the loop.
* Avoid concatenating strings in a looping construct. Each iteration creates a
  separate new string and copying the old string for each modification, leaving
  the prior string in memory for automated clean up later. Use a [lambda
  expression](/functions/lambda) instead of the loop.
* Most UDFs should declare the `RETURNS NULL ON NULL INPUT` characteristics
  unless the code has some special handling for null values. You must declare
  this explicitly since `CALLED ON NULL INPUT` is the default characteristic.

## Limitations

The following limitations apply to SQL UDFs.

* SQL UDFs must be declared before they are referenced.
* Recursion cannot be declared or processed.
* Mutual recursion can not be declared or processed.
* Queries cannot be processed in a SQL UDF.

Specifically this means that SQL UDFs can not use `SELECT` queries to retrieve
data or any other queries to process data within the UDF. Instead queries can
use UDFs to process data. UDFs only work on data provided as input values and
only provide output data from the `RETURN` statement.
