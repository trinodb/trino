# Collations

Strings compare under a collation, which determines whether two strings are equal and
how they sort. See {ref}`varchar-collations` for the collation names.

(collate-operator)=
## COLLATE

```text
expression COLLATE collation [ PAD SPACE | NO PAD ]
```

Gives a string expression an explicit collation, which applies to every comparison,
sort, or grouping of the value:

```sql
SELECT name FROM (VALUES 'Alice', 'ALICE', 'Bob') t(name) WHERE name COLLATE unicode_ci = 'alice';
SELECT name FROM (VALUES 'z', 'ä', 'a') t(name) ORDER BY name COLLATE unicode;
```

`COLLATE` binds tighter than `||`: `a || b COLLATE unicode` collates `b`, and
`(a || b) COLLATE unicode` collates the result.

## How an operation chooses a collation

Every string expression has a collation and a *derivation*, how the collation was
established:

- **explicit**: named by `COLLATE`;
- **implicit**: from the expression's type, such as a column's collation;
- **coercible**: a literal, which yields to any other collation;
- **none**: the combination of strings with different implicit collations, which has
  no collation.

A comparison, sort, or grouping uses the explicit collation if there is one, else the
implicit collation of its operands. A literal takes the collation it is compared with,
so `ci_column = 'abc'` compares under the column's collation. Different explicit
collations, or different implicit collations without an explicit one, are an error:

```sql
SELECT CAST(x AS varchar COLLATE unicode) = CAST(y AS varchar COLLATE unicode_ci) FROM t;              -- error
SELECT CAST(x AS varchar COLLATE unicode) = CAST(y AS varchar COLLATE unicode_ci) COLLATE unicode_ci FROM t;   -- compares under unicode_ci
```

Combining values, as in `CASE`, `COALESCE`, `UNION`, or `ARRAY[...]`, combines their
collations the same way, and strings of different implicit collations combine into a
value with derivation none. Such a value can be returned or stored, but comparing,
sorting, or grouping it is an error until `COLLATE` chooses a collation.

The strings inside arrays, maps, and rows keep their own collations, so
`ARRAY[ci_value] = ARRAY['a']`, where `ci_value` is a `unicode_ci` string, compares
under `unicode_ci`, and a subscript returns the element's collation. Functions that
compare values, such as `contains`, `greatest`, and `array_sort`, use the collation
of the strings they compare. String functions that transform a string, such as `upper`
and `substr`, return the collation of their input, and aggregate functions such as
`max` and `listagg` return their argument's collation.

Trino departs from the SQL standard in one respect: the standard makes literals
implicit in the default collation, which would make `ci_value = 'abc'` an error.

(collation-for)=
## COLLATION FOR

```text
COLLATION FOR (expression)
```

Returns the name of the collation of a string expression as `varchar`, such as `unicode_ci`,
`unicode_ci PAD SPACE`, or `ucs_basic`. It is an error for an expression with derivation none.

## Limitations

A `CHAR` value has no collation. Comparing a collated string with a `CHAR` value is an
error, rather than a comparison of code points. Cast the `CHAR` value to `varchar` to
compare it under the collation:

```sql
SELECT 'a' COLLATE unicode_ci = CAST(CAST('A' AS char(1)) AS varchar);  -- true
```

`LIKE` supports only the default collation. String functions that search or compare
their arguments' contents, such as `strpos`, `starts_with`, `replace`, `split`,
`regexp_like`, and `approx_set`, fail for collated strings. Cast to `varchar` to compare
code points instead:

```sql
SELECT strpos(CAST(name COLLATE unicode_ci AS varchar), 'b') FROM (VALUES 'abc') t(name);
```
