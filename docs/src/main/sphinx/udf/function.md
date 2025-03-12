# FUNCTION

## Synopsis

```text
FUNCTION name ( [ parameter_name data_type [, ...] ] )
  RETURNS type
  [ LANGUAGE language]
  [ NOT? DETERMINISTIC ]
  [ RETURNS NULL ON NULL INPUT ]
  [ CALLED ON NULL INPUT ]
  [ SECURITY { DEFINER | INVOKER } ]
  [ COMMENT description]
  [ WITH ( property_name = expression [, ...] ) ]
  { statements | AS definition }
```

## Description

Declare a [user-defined function](/udf). 

The `name` of the UDF. [](udf-inline) can use a simple string. [](udf-catalog)
must qualify the name of the catalog and schema, delimited by `.`, to store the
UDF or rely on the [default catalog and schema for UDF
storage](/admin/properties-sql-environment).

The list of parameters is a comma-separated list of names `parameter_name` and
data types `data_type`, see [data type](/language/types). An empty list, specified as
`()` is also valid.

The `type` value after the `RETURNS` keyword identifies the [data
type](/language/types) of the UDF output.

The optional `LANGUAGE` characteristic identifies the language used for the UDF
definition with `language`. The `SQL` and `PYTHON` languages are supported by
default. Additional languages may be supported via a language engine plugin.
If not specified, the default language is `SQL`.

The optional `DETERMINISTIC` or `NOT DETERMINISTIC` characteristic declares that
the UDF is deterministic. This means that repeated UDF calls with identical
input parameters yield the same result. A UDF is non-deterministic if it calls
any non-deterministic UDFs and [functions](/functions). By default, UDFs are
assumed to have a deterministic behavior.

The optional `RETURNS NULL ON NULL INPUT` characteristic declares that the UDF
returns a `NULL` value when any of the input parameters are `NULL`. The UDF is
not invoked with a `NULL` input value.

The `CALLED ON NULL INPUT` characteristic declares that the UDF is invoked with
`NULL` input parameter values.

The `RETURNS NULL ON NULL INPUT` and `CALLED ON NULL INPUT` characteristics are
mutually exclusive, with `CALLED ON NULL INPUT` as the default.

The security declaration of `SECURITY INVOKER` or `SECURITY DEFINER` is only
valid for catalog UDFs. It sets the mode for processing the UDF with the
permissions of the user who calls the UDF (`INVOKER`) or the user who created
the UDF (`DEFINER`).

The `COMMENT` characteristic can be used to provide information about the
function to other users as `description`. The information is accessible with
[](/sql/show-functions).

The optional `WITH` clause can be used to specify properties for the function.
The available properties vary based on the function language. For
[](/udf/python), the `handler` property specifies the name of the Python
function to invoke.

For SQL UDFs the body of the UDF can either be a simple single `RETURN`
statement with an expression, or compound list of `statements` in a `BEGIN`
block. UDF must contain a `RETURN` statement at the end of the top-level block,
even if it's unreachable.

For UDFs in other languages, the `definition` is enclosed in a `$$`-quoted
string.

## Examples

A simple catalog function:

```sql
CREATE FUNCTION example.default.meaning_of_life()
  RETURNS BIGINT
  RETURN 42;
```

And used:

```sql
SELECT example.default.meaning_of_life(); -- returns 42
```

Equivalent usage with an inline function:

```sql
WITH FUNCTION meaning_of_life()
  RETURNS BIGINT
  RETURN 42
SELECT meaning_of_life();
```

Further examples of varying complexity that cover usage of the `FUNCTION`
statement in combination with other statements are available in the [SQL UDF
documentation](/udf/sql/examples) and the [Python UDF
documentation](/udf/python).

## See also

* [](/udf)
* [](/udf/sql)
* [](/udf/python)
* [](/sql/create-function)

