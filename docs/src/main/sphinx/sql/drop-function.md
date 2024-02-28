# DROP FUNCTION

## Synopsis

```text
DROP FUNCTION [ IF EXISTS ] routine_name ( [ [ parameter_name ] data_type [, ...] ] )
```

## Description

Removes a [](routine-catalog). The value of `routine_name`
must be fully qualified with catalog and schema location of the routine, unless
the [default SQL routine storage catalog and
schema](/admin/properties-sql-environment) are configured.

The `data_type`s must be included for routines that use parameters to ensure the
routine with the correct name and parameter signature is removed.

The optional `IF EXISTS` clause causes the error to be suppressed if
the function does not exist.

## Examples

The following example removes the `meaning_of_life` routine in the `default`
schema of the `example` catalog:

```sql
DROP FUNCTION example.default.meaning_of_life();
```

If the routine uses a input parameter, the type must be added:

```sql
DROP FUNCTION multiply_by_two(bigint);
```

If the [default catalog and schema for routine
storage](/admin/properties-sql-environment) is configured, you can use the
following more compact syntax:

```sql
DROP FUNCTION meaning_of_life();
```

## See also

* [](/sql/create-function)
* [](/sql/show-functions)
* [](/routines/introduction)
* [](/admin/properties-sql-environment)

