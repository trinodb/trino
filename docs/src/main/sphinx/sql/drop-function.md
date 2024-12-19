# DROP FUNCTION

## Synopsis

```text
DROP FUNCTION [ IF EXISTS ] udf_name ( [ [ parameter_name ] data_type [, ...] ] )
```

## Description

Removes a [catalog UDF](udf-catalog). The value of `udf_name` must be fully
qualified with catalog and schema location of the UDF, unless the [default UDF storage catalog and schema](/admin/properties-sql-environment) are
configured.

The `data_type`s must be included for UDFs that use parameters to ensure the UDF
with the correct name and parameter signature is removed.

The optional `IF EXISTS` clause causes the error to be suppressed if
the function does not exist.

## Examples

The following example removes the `meaning_of_life` UDF in the `default` schema
of the `example` catalog:

```sql
DROP FUNCTION example.default.meaning_of_life();
```

If the UDF uses a input parameter, the type must be added:

```sql
DROP FUNCTION multiply_by_two(bigint);
```

If the [default catalog and schema for UDF
storage](/admin/properties-sql-environment) is configured, you can use the
following more compact syntax:

```sql
DROP FUNCTION meaning_of_life();
```

## See also

* [](/sql/create-function)
* [](/sql/show-create-function)
* [](/sql/show-functions)
* [](/udf)
* [](/admin/properties-sql-environment)
