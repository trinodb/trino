# Conditional expressions

(case-expression)=
## CASE

The standard SQL `CASE` expression has two forms.
The "simple" form searches each `value` expression from left to right
until it finds one that equals `expression`:

```text
CASE expression
    WHEN value THEN result
    [ WHEN ... ]
    [ ELSE result ]
END
```

The `result` for the matching `value` is returned.
If no match is found, the `result` from the `ELSE` clause is
returned if it exists, otherwise null is returned. Example:

```
SELECT a,
       CASE a
           WHEN 1 THEN 'one'
           WHEN 2 THEN 'two'
           ELSE 'many'
       END
```

The "searched" form evaluates each boolean `condition` from left
to right until one is true and returns the matching `result`:

```text
CASE
    WHEN condition THEN result
    [ WHEN ... ]
    [ ELSE result ]
END
```

If no conditions are true, the `result` from the `ELSE` clause is
returned if it exists, otherwise null is returned. Example:

```
SELECT a, b,
       CASE
           WHEN a = 1 THEN 'aaa'
           WHEN b = 2 THEN 'bbb'
           ELSE 'ccc'
       END
```

SQL UDFs can use [`CASE` statements](/udf/sql/case) that use a slightly
different syntax from the CASE expressions. Specifically note the requirements
for terminating each clause with a semicolon `;` and the usage of `END CASE`.

(if-expression)=
## IF

The `IF` expression has two forms, one supplying only a
`true_value` and the other supplying both a `true_value` and a
`false_value`:

:::{function} if(condition, true_value)
Evaluates and returns `true_value` if `condition` is true,
otherwise null is returned and `true_value` is not evaluated.
:::

:::{function} if(condition, true_value, false_value)
:noindex: true

Evaluates and returns `true_value` if `condition` is true,
otherwise evaluates and returns `false_value`.
:::

The following `IF` and `CASE` expressions are equivalent:

```sql
SELECT
  orderkey,
  totalprice,
  IF(totalprice >= 150000, 'High Value', 'Low Value')
FROM tpch.sf1.orders;
```

```sql
SELECT
  orderkey,
  totalprice,
  CASE
    WHEN totalprice >= 150000 THEN 'High Value'
    ELSE 'Low Value'
  END
FROM tpch.sf1.orders;
```

SQL UDFs can use [`IF` statements](/udf/sql/if) that use a slightly different
syntax from `IF` expressions. Specifically note the requirement for terminating
each clause with a semicolon `;` and the usage of `END IF`.

(coalesce-function)=
## COALESCE

:::{function} coalesce(value1, value2[, ...])
Returns the first non-null `value` in the argument list.
Like a `CASE` expression, arguments are only evaluated if necessary.
:::

(nullif-function)=
## NULLIF

:::{function} nullif(value1, value2)
Returns null if `value1` equals `value2`, otherwise returns `value1`.
:::

(try-function)=
## TRY

:::{function} try(expression)
Evaluate an expression and handle certain types of errors by returning
`NULL`.
:::

In cases where it is preferable that queries produce `NULL` or default values
instead of failing when corrupt or invalid data is encountered, the `TRY`
function may be useful. To specify default values, the `TRY` function can be
used in conjunction with the `COALESCE` function.

The following errors are handled by `TRY`:

- Division by zero
- Invalid cast or function argument
- Numeric value out of range

### Examples

Source table with some invalid data:

```sql
SELECT * FROM shipping;
```

```text
 origin_state | origin_zip | packages | total_cost
--------------+------------+----------+------------
 California   |      94131 |       25 |        100
 California   |      P332a |        5 |         72
 California   |      94025 |        0 |        155
 New Jersey   |      08544 |      225 |        490
(4 rows)
```

Query failure without `TRY`:

```sql
SELECT CAST(origin_zip AS BIGINT) FROM shipping;
```

```text
Query failed: Cannot cast 'P332a' to BIGINT
```

`NULL` values with `TRY`:

```sql
SELECT TRY(CAST(origin_zip AS BIGINT)) FROM shipping;
```

```text
 origin_zip
------------
      94131
 NULL
      94025
      08544
(4 rows)
```

Query failure without `TRY`:

```sql
SELECT total_cost / packages AS per_package FROM shipping;
```

```text
Query failed: Division by zero
```

Default values with `TRY` and `COALESCE`:

```sql
SELECT COALESCE(TRY(total_cost / packages), 0) AS per_package FROM shipping;
```

```text
 per_package
-------------
          4
         14
          0
         19
(4 rows)
```
