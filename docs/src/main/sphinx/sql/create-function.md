# CREATE FUNCTION

## Synopsis

```text
CREATE [OR REPLACE] FUNCTION
  routine_definition
```

## Description

Create or replace a [](routine-catalog). The `routine_definition` is composed of
the usage of [](/routines/function) and nested statements. The name of the
routine must be fully qualified with catalog and schema location, unless the
[default SQL routine storage catalog and
schema](/admin/properties-sql-environment) are configured. The connector used in
the catalog must support routine storage.

The optional `OR REPLACE` clause causes the routine to be replaced if it already
exists rather than raising an error.

## Examples

The following example creates the `meaning_of_life` routine in the `default`
schema of the `example` catalog:

```sql
CREATE FUNCTION example.default.meaning_of_life()
  RETURNS bigint
  BEGIN
    RETURN 42;
  END;
```

If the [default catalog and schema for routine
storage](/admin/properties-sql-environment) is configured, you can use the
following more compact syntax:

```sql
CREATE FUNCTION meaning_of_life() RETURNS bigint RETURN 42;
```

Further examples of varying complexity that cover usage of the `FUNCTION`
statement in combination with other statements are available in the [SQL
routines examples documentation](/routines/examples).

## See also

* [](/sql/drop-function)
* [](/sql/show-functions)
* [](/routines/introduction)
* [](/admin/properties-sql-environment)
