# SHOW CREATE FUNCTION

## Synopsis

```sql
SHOW CREATE FUNCTION function_name
```

## Description

Show the definition of the SQL routine. The `function_name` parameter is the
name of the routine without `()` and any parameters.

## Examples

The following example uses `SHOW CREATE FUNCTION` to display the definition of
the routine that created the `answer` function.

```sql
SHOW CREATE FUNCTION answer;
```

Example output:

```text
     Create Function
--------------------------
 CREATE FUNCTION answer()
 RETURNS BIGINT
 RETURN 42
```

The following example uses `SHOW CREATE FUNCTION` to display the definition of
the `answer` SQL routine found on the current catalog and schema:

```sql
SHOW create function example.default.answer;
```

Example output:

```text

      Create Function
---------------------------
 CREATE FUNCTION answer()
 RETURNS BIGINT
 COMMENT 'meaning of life'
 RETURN 42
```

The following example specifies the default SQL path, then uses `SHOW CREATE
FUNCTION` to display the definition of the `answer` SQL routine found on the
default SQL path:

```sql
USE example.default;
SHOW create function answer;
```

Example output:

```text

      Create Function
---------------------------
 CREATE FUNCTION answer()
 RETURNS BIGINT
 COMMENT 'meaning of life'
 RETURN 42
```

Alternatively, you can specify the full path to the routine in a catalog and
schema:

```text
sql.default-function-catalog=example
sql.default-function-schema=functions
sql.path=example.functions
```

## See also

* [](/sql/create-function)
* [](/sql/drop-function)
* [](/sql/show-functions)
* [](/routines/introduction)
* [](/admin/properties-sql-environment)
