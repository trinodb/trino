# SHOW FUNCTIONS

## Synopsis

```text
SHOW FUNCTIONS [ FROM schema ] [ LIKE pattern ]
```

## Description

List functions in `schema` or all functions in the current session path.
For each function returned, the following information is displayed:

- Function name
- Return type
- Argument types
- Function type
- Deterministic
- Description

{ref}`Specify a pattern <like-operator>` in the optional `LIKE` clause to
filter the results to the desired subset. For example, the following query
allows you to find functions beginning with `array`:

```
SHOW FUNCTIONS LIKE 'array%';
```

`SHOW FUNCTIONS` works with built-in functions as well as with {doc}`custom
functions </develop/functions>`. In the following example, three custom
functions beginning with `cf` are available:

```text
SHOW FUNCTIONS LIKE 'cf%';

     Function      | Return Type | Argument Types | Function Type | Deterministic |               Description
 ------------------+-------------+----------------+---------------+---------------+-----------------------------------------
 cf_getgroups      | varchar     |                | scalar        | true          | Returns the current session's groups
 cf_getprincipal   | varchar     |                | scalar        | true          | Returns the current session's principal
 cf_getuser        | varchar     |                | scalar        | true          | Returns the current session's user
```
