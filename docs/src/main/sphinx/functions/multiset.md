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
