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
  statements
```

## Description

Declare a SQL routine.

The `name` of the routine. [Inline routines](routine-inline) can use a simple
string. [Catalog routines](routine-catalog) must qualify the name of the catalog
and schema, delimited by `.`, to store the routine or rely on the [default
catalog and schema for routine storage](/admin/properties-sql-environment).

The list of parameters is a comma-separated list of names `parameter_name` and
data types `data_type`, see [data type](/language/types). An empty list, specified as
`()` is also valid.

The `type` value after the `RETURNS` keyword identifies the [data
type](/language/types) of the routine output.

The optional `LANGUAGE` characteristic identifies the language used for the
routine definition with `language`. Only `SQL` is supported.

The optional `DETERMINISTIC` or `NOT DETERMINISTIC` characteristic declares that
the routine is deterministic. This means that repeated routine calls with
identical input parameters yield the same result. For SQL language routines, a
routine is non-deterministic if it calls any non-deterministic routines and
[functions](/functions). By default, routines are assume to have a deterministic
behavior.

The optional `RETURNS NULL ON NULL INPUT` characteristic declares that the
routine returns a `NULL` value when any of the input parameters are `NULL`.
The routine is not invoked with a `NULL` input value.

The `CALLED ON NULL INPUT` characteristic declares that the routine is invoked
with `NULL` input parameter values.

The `RETURNS NULL ON NULL INPUT` and `CALLED ON NULL INPUT` characteristics are
mutually exclusive, with `CALLED ON NULL INPUT` as the default.

The security declaration of `SECURITY INVOKER` or `SECURITY DEFINER` is only
valid for catalog routines. It sets the mode for processing the routine with the
permissions of the user who calls the routine (`INVOKER`) or the user who
created the routine (`DEFINER`).

The `COMMENT` characteristic can be used to provide information about the
function to other users as `description`. The information is accessible with
[](/sql/show-functions).

The body of the routine can either be a simple single `RETURN` statement with an
expression, or compound list of `statements` in a `BEGIN` block. Routines must
contain a `RETURN` statement at the end of the top-level block, even if it's
unreachable.

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
statement in combination with other statements are available in the [SQL
routines examples documentation](/routines/examples).

## See also

* [](/routines/introduction)
* [](/routines/begin)
* [](/routines/return)
* [](/sql/create-function)

