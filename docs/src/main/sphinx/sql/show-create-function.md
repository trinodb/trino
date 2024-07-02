# SHOW CREATE FUNCTION

## Synopsis

```sql
SHOW CREATE FUNCTION function_name
```

## Description

Shows the SQL statement that created the specified function.

## Examples

List all SQL routines and plugin functions in the `default` schema of the
`brain_catalog`:

```sql
SHOW FUNCTIONS FROM brain_catalog.default
```

Example output:

```text
Function | Return Type | Argument Types | Function Type | Deterministic | Description
----------+-------------+----------------+---------------+---------------+-------------
 answer   | bigint      |                | scalar        | true          |
```

List all functions with a name beginning with `ans`:

```sql
SHOW FUNCTIONS like 'ans%';
```

Example output:

```text
 Function | Return Type | Argument Types | Function Type | Deterministic | Description
----------+-------------+----------------+---------------+---------------+-------------
 answer   | bigint      |                | scalar        | true          |
 ```

The following example uses `SHOW CREATE FUNCTION` to reveal the SQL statement
that created the `answer` function:

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

## See also

* [](/sql/create-function)
* [](/sql/drop-function)
* [](/sql/show-functions)
* [](/routines/introduction)
* [](/admin/properties-sql-environment)
