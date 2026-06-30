# Multiset functions and operators

Multiset functions and operators use the [MULTISET type](multiset-type). A
multiset is a collection that, unlike an [array](array-type), has no element
order, and that, unlike a set, retains duplicate elements. Two multisets are
equal when they contain the same elements with the same multiplicities,
regardless of order.

## Multiset constructor

Create a multiset with the `MULTISET` constructor. Duplicates are retained:

```sql
SELECT MULTISET[1, 2, 2];
-- {1, 2, 2}
```

The element type is the common supertype of the listed expressions, and null
elements are allowed:

```sql
SELECT MULTISET[1, NULL, 2];
```

An empty multiset is written `MULTISET[]`.

## Multiset type syntax

The `<type> MULTISET` syntax names a multiset type, for example in a cast:

```sql
SELECT CAST(MULTISET[1, 2, 2] AS integer MULTISET);
```

## Multiset from a subquery

`MULTISET(subquery)` builds a multiset from the rows of a single-column
subquery, retaining duplicates. The subquery may be correlated; an outer row
with no matching subquery rows yields an empty multiset, not null.

```sql
SELECT CARDINALITY(MULTISET(SELECT n FROM (VALUES 1, 1, 2) AS t(n)));
-- 3
```

## Cardinality

Use {func}`cardinality` to count the elements of a multiset, including
duplicates:

```sql
SELECT CARDINALITY(MULTISET[1, 2, 2]);
-- 3
```

## UNNEST

Expand a multiset into a relation with {ref}`unnest`. Each element
becomes one row, and duplicates produce one row each:

```sql
SELECT x FROM UNNEST(MULTISET[1, 2, 2]) AS t(x);
-- 1
-- 2
-- 2
```

## Casts

There is no implicit coercion between arrays and multisets; convert between them
with an explicit cast. Casting an array to a multiset discards element order,
and casting a multiset to an array materializes its elements in an unspecified
order:

```sql
SELECT CAST(ARRAY[2, 1, 2] AS multiset(integer));
-- {1, 2, 2}

SELECT CAST(MULTISET[1, 2, 2] AS array(integer));
-- [1, 2, 2]
```

The element type is coerced as part of the cast:

```sql
SELECT CAST(ARRAY[1, 2] AS multiset(bigint));
```

## Scalar functions

:::{function} element(multiset(E)) -> E
Returns the sole element of `multiset`. Returns null if `multiset` is empty or
its single element is null. Raises an error if `multiset` has more than one
element.

```sql
SELECT ELEMENT(MULTISET[42]);
-- 42
```
:::

:::{function} set(multiset(E)) -> multiset(E)
Returns the multiset with duplicate elements removed, so that every element
occurs exactly once. Null is treated as not distinct from null, so duplicate
nulls collapse to a single null element.

```sql
SELECT SET(MULTISET[1, 1, 2, 2, 3]);
-- {1, 2, 3}
```
:::

## Set operators

The `MULTISET UNION`, `MULTISET INTERSECT`, and `MULTISET EXCEPT` operators
combine two multisets. Each accepts an optional `ALL` or `DISTINCT` quantifier;
`ALL` is the default.

With `ALL`, the operators combine multiplicities per element value: `UNION`
adds them, `INTERSECT` takes the minimum, and `EXCEPT` subtracts the right
multiplicity from the left (not below zero). With `DISTINCT`, the result is
first reduced so each element occurs at most once.

```sql
SELECT MULTISET[1, 2] MULTISET UNION ALL MULTISET[2, 3];
-- {1, 2, 2, 3}

SELECT MULTISET[1, 1, 2, 3] MULTISET INTERSECT ALL MULTISET[1, 2, 2];
-- {1, 2}

SELECT MULTISET[1, 1, 2, 3] MULTISET EXCEPT ALL MULTISET[1, 3];
-- {1, 2}
```

`MULTISET INTERSECT` binds more tightly than `MULTISET UNION` and
`MULTISET EXCEPT`, so `a MULTISET UNION b MULTISET INTERSECT c` is evaluated as
`a MULTISET UNION (b MULTISET INTERSECT c)`.

## Predicates

### `SUBMULTISET`

`x SUBMULTISET OF y` returns true when every element of `x` occurs in `y` with
at least the same multiplicity. The `OF` keyword is optional. Use `NOT` to
negate the predicate. Null is treated as not distinct from null.

```sql
SELECT MULTISET[1, 1, 2] SUBMULTISET OF MULTISET[1, 1, 1, 2];
-- true

SELECT MULTISET[1, 1, 2] SUBMULTISET OF MULTISET[1, 2];
-- false
```

### `MEMBER OF`

`x MEMBER OF y` returns true when the value `x` is equal to at least one element
of the multiset `y`. The `OF` keyword is optional, and `NOT` negates the
predicate. Like `IN`, it is three-valued: the result is unknown (null) when `x`
is null, or when `x` matches no element but `y` contains a null.

```sql
SELECT 2 MEMBER OF MULTISET[1, 2, 2];
-- true

SELECT 3 MEMBER OF MULTISET[1, 2];
-- false
```

### `IS A SET`

`x IS A SET` returns true when no element of `x` occurs more than once, and
false otherwise. Use `IS NOT A SET` to negate it. Because null is not distinct
from null, two null elements count as duplicates.

```sql
SELECT MULTISET[1, 2, 3] IS A SET;
-- true

SELECT MULTISET[1, 2, 2] IS A SET;
-- false
```
