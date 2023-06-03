# Migrating from Hive

Trino uses ANSI SQL syntax and semantics, whereas Hive uses a language similar
to SQL called HiveQL which is loosely modeled after MySQL (which itself has many
differences from ANSI SQL).

## Use subscript for accessing a dynamic index of an array instead of a udf

The subscript operator in SQL supports full expressions, unlike Hive (which only supports constants). Therefore you can write queries like:

```
SELECT my_array[CARDINALITY(my_array)] as last_element
FROM ...
```

## Avoid out of bounds access of arrays

Accessing out of bounds elements of an array will result in an exception. You can avoid this with an `if` as follows:

```
SELECT IF(CARDINALITY(my_array) >= 3, my_array[3], NULL)
FROM ...
```

## Use ANSI SQL syntax for arrays

Arrays are indexed starting from 1, not from 0:

```
SELECT my_array[1] AS first_element
FROM ...
```

Construct arrays with ANSI syntax:

```
SELECT ARRAY[1, 2, 3] AS my_array
```

## Use ANSI SQL syntax for identifiers and strings

Strings are delimited with single quotes and identifiers are quoted with double quotes, not backquotes:

```
SELECT name AS "User Name"
FROM "7day_active"
WHERE name = 'foo'
```

## Quote identifiers that start with numbers

Identifiers that start with numbers are not legal in ANSI SQL and must be quoted using double quotes:

```
SELECT *
FROM "7day_active"
```

## Use the standard string concatenation operator

Use the ANSI SQL string concatenation operator:

```
SELECT a || b || c
FROM ...
```

## Use standard types for CAST targets

The following standard types are supported for `CAST` targets:

```
SELECT
  CAST(x AS varchar)
, CAST(x AS bigint)
, CAST(x AS double)
, CAST(x AS boolean)
FROM ...
```

In particular, use `VARCHAR` instead of `STRING`.

## Use CAST when dividing integers

Trino follows the standard behavior of performing integer division when dividing two integers. For example, dividing `7` by `2` will result in `3`, not `3.5`.
To perform floating point division on two integers, cast one of them to a double:

```
SELECT CAST(5 AS DOUBLE) / 2
```

## Use WITH for complex expressions or queries

When you want to re-use a complex output expression as a filter, use either an inline subquery or factor it out using the `WITH` clause:

```
WITH a AS (
  SELECT substr(name, 1, 3) x
  FROM ...
)
SELECT *
FROM a
WHERE x = 'foo'
```

## Use UNNEST to expand arrays and maps

Trino supports {ref}`unnest` for expanding arrays and maps.
Use `UNNEST` instead of `LATERAL VIEW explode()`.

Hive query:

```
SELECT student, score
FROM tests
LATERAL VIEW explode(scores) t AS score;
```

Trino query:

```
SELECT student, score
FROM tests
CROSS JOIN UNNEST(scores) AS t (score);
```

## Use ANSI SQL syntax for date and time INTERVAL expressions

Trino supports the ANSI SQL style `INTERVAL` expressions that differs from the implementation used in Hive.

- The `INTERVAL` keyword is required and is not optional.
- Date and time units must be singular. For example `day` and not `days`.
- Values must be quoted.

Hive query:

```
SELECT cast('2000-08-19' as date) + 14 days;
```

Equivalent Trino query:

```
SELECT cast('2000-08-19' as date) + INTERVAL '14' day;
```

## Caution with datediff

The Hive `datediff` function returns the difference between the two dates in
days and is declared as:

```text
datediff(string enddate, string startdate)  -> integer
```

The equivalent Trino function {ref}`date_diff<datetime-interval-functions>`
uses a reverse order for the two date parameters and requires a unit. This has
to be taken into account when migrating:

Hive query:

```
datediff(enddate, startdate)
```

Trino query:

```
date_diff('day', startdate, enddate)
```

## Overwriting data on insert

By default, `INSERT` queries are not allowed to overwrite existing data. You
can use the catalog session property `insert_existing_partitions_behavior` to
allow overwrites. Prepend the name of the catalog using the Hive connector, for
example `hdfs`, and set the property in the session before you run the insert
query:

```
SET SESSION hdfs.insert_existing_partitions_behavior = 'OVERWRITE';
INSERT INTO hdfs.schema.table ...
```

The resulting behavior is equivalent to using [INSERT OVERWRITE](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML) in Hive.

Insert overwrite operation is not supported by Trino when the table is stored on
encrypted HDFS, when the table is unpartitioned or table is transactional.
