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
