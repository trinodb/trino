# Comparison functions and operators

(comparison-operators)=
## Comparison operators

:::{list-table}
:widths: 30, 70
:header-rows: 1

* - Operator
  - Description
* - `<`
  - Less than
* - `>`
  - Greater than
* - `<=`
  - Less than or equal to
* - `>=`
  - Greater than or equal to
* - `=`
  - Equal
* - `<>`
  - Not equal
* - `!=`
  - Not equal (non-standard but popular syntax)
:::

(range-operator)=
## Range operator: BETWEEN

The `BETWEEN` operator tests if a value is within a specified range. It uses the
syntax `value BETWEEN min AND max`:

```sql
SELECT 3 BETWEEN 2 AND 6;
```

The preceding statement is equivalent to the following statement:

```sql
SELECT 3 >= 2 AND 3 <= 6;
```

To test if a value does not fall within the specified range use `NOT BETWEEN`:

```sql
SELECT 3 NOT BETWEEN 2 AND 6;
```

The statement shown above is equivalent to the following statement:

```sql
SELECT 3 < 2 OR 3 > 6;
```

A `NULL` in a `BETWEEN` or `NOT BETWEEN` statement is evaluated using the
standard `NULL` evaluation rules applied to the equivalent expression above:

```sql
SELECT NULL BETWEEN 2 AND 4; -- null

SELECT 2 BETWEEN NULL AND 6; -- null

SELECT 2 BETWEEN 3 AND NULL; -- false

SELECT 8 BETWEEN NULL AND 6; -- false
```

The `BETWEEN` and `NOT BETWEEN` operators can also be used to evaluate any
orderable type. For example, a `VARCHAR`:

```sql
SELECT 'Paul' BETWEEN 'John' AND 'Ringo'; -- true
```

Note that the value, min, and max parameters to `BETWEEN` and `NOT BETWEEN` must
be the same type. For example, Trino produces an error if you ask it if `John`
is between `2.3` and `35.2`.

(is-null-operator)=
## IS NULL and IS NOT NULL

The `IS NULL` and `IS NOT NULL` operators test whether a value is null
(undefined).  Both operators work for all data types.

Using `NULL` with `IS NULL` evaluates to `true`:

```sql
SELECT NULL IS NULL; -- true
```

But any other constant does not:

```sql
SELECT 3.0 IS NULL; -- false
```

(is-distinct-operator)=
## IS DISTINCT FROM and IS NOT DISTINCT FROM

In SQL a `NULL` value signifies an unknown value, so any comparison involving a
`NULL` produces `NULL`.  The  `IS DISTINCT FROM` and `IS NOT DISTINCT FROM`
operators treat `NULL` as a known value and both operators guarantee either a
true or false outcome even in the presence of `NULL` input:

```sql
SELECT NULL IS DISTINCT FROM NULL; -- false

SELECT NULL IS NOT DISTINCT FROM NULL; -- true
```

In the preceding example a `NULL` value is not considered distinct from `NULL`.
When you are comparing values which may include `NULL` use these operators to
guarantee either a `TRUE` or `FALSE` result.

The following truth table demonstrate the handling of `NULL` in
`IS DISTINCT FROM` and `IS NOT DISTINCT FROM`:

| a      | b      | a = b   | a \<> b | a DISTINCT b | a NOT DISTINCT b |
| ------ | ------ | ------- | ------- | ------------ | ---------------- |
| `1`    | `1`    | `TRUE`  | `FALSE` | `FALSE`      | `TRUE`           |
| `1`    | `2`    | `FALSE` | `TRUE`  | `TRUE`       | `FALSE`          |
| `1`    | `NULL` | `NULL`  | `NULL`  | `TRUE`       | `FALSE`          |
| `NULL` | `NULL` | `NULL`  | `NULL`  | `FALSE`      | `TRUE`           |

## GREATEST and LEAST

These functions are not in the SQL standard, but are a common extension.
Like most other functions in Trino, they return null if any argument is
null. Note that in some other databases, such as PostgreSQL, they only
return null if all arguments are null.

The following types are supported:

* `DOUBLE`
* `BIGINT`
* `VARCHAR`
* `TIMESTAMP`
* `TIMESTAMP WITH TIME ZONE`
* `DATE`

:::{function} greatest(value1, value2, ..., valueN) -> [same as input]
Returns the largest of the provided values.
:::

:::{function} least(value1, value2, ..., valueN) -> [same as input]
Returns the smallest of the provided values.
:::

(quantified-comparison-predicates)=
## Quantified comparison predicates: ALL, ANY and SOME

The `ALL`, `ANY` and `SOME` quantifiers can be used together with comparison
operators in the following way:

```text
expression operator quantifier ( subquery )
```

For example:

```sql
SELECT 'hello' = ANY (VALUES 'hello', 'world'); -- true

SELECT 21 < ALL (VALUES 19, 20, 21); -- false

SELECT 42 >= SOME (SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43); -- true
```

Following are the meanings of some quantifier and comparison operator
combinations:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Expression
  - Meaning
* - `A = ALL (...)`
  - Evaluates to `true` when `A` is equal to all values.
* - `A <> ALL (...)`
  - Evaluates to `true` when `A` doesn't match any value.
* - `A < ALL (...)`
  - Evaluates to `true` when `A` is smaller than the smallest value.
* - `A = ANY (...)`
  - Evaluates to `true` when `A` is equal to any of the values. This form
    is equivalent to `A IN (...)`.
* - `A <> ANY (...)`
  - Evaluates to `true` when `A` doesn't match one or more values.
* - `A < ANY (...)`
  - Evaluates to `true` when `A` is smaller than the biggest value.
:::

`ANY` and `SOME` have the same meaning and can be used interchangeably.

(like-operator)=
## Pattern comparison: LIKE

The `LIKE` operator can be used to compare values with a pattern:

```
... column [NOT] LIKE 'pattern' ESCAPE 'character';
```

Matching characters is case sensitive, and the pattern supports two symbols for
matching:

- `_` matches any single character
- `%` matches zero or more characters

Typically it is often used as a condition in `WHERE` statements. An example is
a query to find all continents starting with `E`, which returns `Europe`:

```sql
SELECT * FROM (VALUES 'America', 'Asia', 'Africa', 'Europe', 'Australia', 'Antarctica') AS t (continent)
WHERE continent LIKE 'E%';
```

You can negate the result by adding `NOT`, and get all other continents, all
not starting with `E`:

```sql
SELECT * FROM (VALUES 'America', 'Asia', 'Africa', 'Europe', 'Australia', 'Antarctica') AS t (continent)
WHERE continent NOT LIKE 'E%';
```

If you only have one specific character to match, you can use the `_` symbol
for each character. The following query uses two underscores and produces only
`Asia` as result:

```sql
SELECT * FROM (VALUES 'America', 'Asia', 'Africa', 'Europe', 'Australia', 'Antarctica') AS t (continent)
WHERE continent LIKE 'A__A';
```

The wildcard characters `_` and `%` must be escaped to allow you to match
them as literals. This can be achieved by specifying the `ESCAPE` character to
use:

```sql
SELECT 'South_America' LIKE 'South\_America' ESCAPE '\';
```

The above query returns `true` since the escaped underscore symbol matches. If
you need to match the used escape character as well, you can escape it.

If you want to match for the chosen escape character, you simply escape itself.
For example, you can use `\\` to match for `\`.

(in-operator)=
## Row comparison: IN

The `IN` operator can be used in a `WHERE` clause to compare column values with 
a list of values. The list of values can be supplied by a subquery or directly 
as static values in an array:

```sql
... WHERE column [NOT] IN ('value1','value2');
... WHERE column [NOT] IN ( subquery );
```

Use the optional `NOT` keyword to negate the condition.

The following example shows a simple usage with a static array:

```sql
SELECT * FROM region WHERE name IN ('AMERICA', 'EUROPE');
```

The values in the clause are used for multiple comparisons that are combined as
a logical `OR`. The preceding query is equivalent to the following query:

```sql
SELECT * FROM region WHERE name = 'AMERICA' OR name = 'EUROPE';
```

You can negate the comparisons by adding `NOT`, and get all other regions
except the values in list:

```sql
SELECT * FROM region WHERE name NOT IN ('AMERICA', 'EUROPE');
```

When using a subquery to determine the values to use in the comparison, the
subquery must return a single column and one or more rows. For example, the
following query returns nation name of countries in regions starting with the
letter `A`, specifically Africa, America, and Asia:

```sql
SELECT nation.name
FROM nation
WHERE regionkey IN (
  SELECT regionkey
  FROM region
  WHERE starts_with(name, 'A')
)
ORDER by nation.name;
```

## Examples

The following example queries showcase aspects of using comparison functions and
operators related to implied ordering of values, implicit casting, and different
types.

Ordering:

```sql
SELECT 'M' BETWEEN 'A' AND 'Z'; -- true
SELECT 'A' < 'B'; -- true
SELECT 'A' < 'a'; -- true
SELECT TRUE > FALSE; -- true
SELECT 'M' BETWEEN 'A' AND 'Z'; -- true
SELECT 'm' BETWEEN 'A' AND 'Z'; -- false
```

The following queries show a subtle difference between `char` and `varchar`
types. The length parameter for `varchar` is an optional maximum length
parameter and comparison is based on the data only, ignoring the length:

```sql
SELECT cast('Test' as varchar(20)) = cast('Test' as varchar(25)); --true
SELECT cast('Test' as varchar(20)) = cast('Test   ' as varchar(25)); --false
```

The length parameter for `char` defines a fixed length character array.
Comparison with different length automatically includes a cast to the same
larger length. The cast is performed as automatic padding with spaces, and
therefore both queries in the following return `true`:

```sql
SELECT cast('Test' as char(20)) = cast('Test' as char(25)); -- true
SELECT cast('Test' as char(20)) = cast('Test   ' as char(25)); -- true
```

The following queries show how date types are ordered, and how date is
implicitly cast to timestamp with zero time values:

```sql
SELECT DATE '2024-08-22' < DATE '2024-08-31';
SELECT DATE '2024-08-22' < TIMESTAMP '2024-08-22 8:00:00';
```
