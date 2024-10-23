# SHOW FUNCTIONS

## Synopsis

```text
SHOW FUNCTIONS [ FROM schema ] [ LIKE pattern ]
```

## Description

List functions in `schema` or all functions in the current session path. This
can include built-in functions, [functions from a custom
plugin](/develop/functions), and [SQL routines](/routines).

For each function returned, the following information is displayed:

- Function name
- Return type
- Argument types
- Function type
- Deterministic
- Description

Use the optional `FROM` keyword to only list functions in a specific catalog and
schema. The location in `schema` must be specified as
`cataglog_name.schema_name`.

{ref}`Specify a pattern <like-operator>` in the optional `LIKE` clause to
filter the results to the desired subset.

## Examples

List all SQL routines and plugin functions in the `default` schema of the
`example` catalog:

```sql
SHOW FUNCTIONS FROM example.default;
```

Example output:

```text
 Function | Return Type | Argument Types | Function Type | Deterministic |   Description
----------+-------------+----------------+---------------+---------------+-----------------
 answer   | bigint      |                | scalar        | true          | meaning of life
```

List all functions with a name beginning with `ans`:

```sql
SHOW FUNCTIONS like 'ans%';
```

Example output:

 Function | Return Type | Argument Types | Function Type | Deterministic |   Description
----------+-------------+----------------+---------------+---------------+-----------------
 answer   | bigint      |                | scalar        | true          | meaning of life

## See also

* [](/functions)
* [](/routines)
* [](/develop/functions)
* [](/sql/create-function)
* [](/sql/drop-function)
* [](/sql/show-create-function)
