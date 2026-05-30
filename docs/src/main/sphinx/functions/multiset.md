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
