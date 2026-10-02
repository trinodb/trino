# Value domains

A `Domain` describes the possible values of one variable. It contains a `ValueSet`
of non-null values and a separate null allowance. `TupleDomain` combines per-column
domains as a conjunction. Combining domains for each column independently can lose
correlations between columns, so a column-wise union can be a superset even though
each individual column's value-set operations preserve exact membership.

Use the `ValueSet` factories to construct sets appropriate to a type. Orderable
types normally use sorted ranges, comparable types without ordering use discrete
sets, and other types support all-or-none sets. `REAL`, `DOUBLE`, and `NUMBER` use
`FloatingPointValueSet`, which stores ordered values and independent NaN membership.
Constructing a `Domain` from an older floating-point `SortedRangeSet` normalizes it
to this representation without changing its membership.

Legacy range sets and floating-point value sets interoperate through `ValueSet`
set operations in either operand order. Mixed operations preserve NaN membership,
including when subtracting a legacy range set from a floating-point set.
`SortedRangeSet.intersect(ValueSet)`, `union(ValueSet)`, and `union(Collection<ValueSet>)`
return `ValueSet`, since a mixed result may require the floating-point representation.
The overloads taking a `SortedRangeSet` retain their concrete range result. Code
using the changed concrete SPI signatures must be recompiled for this Trino version.

## NaN and set operations

NaN is a non-null value. It is not an ordered range endpoint, but it can belong to a
set independently of the ordered values. A NaN singleton, a finite interval plus
NaN, and all ordered values excluding NaN are distinct representable sets.
`containsValue`, union, intersection, subtraction, and complement account for NaN.
Joining ranges that cover all ordered values does not implicitly add NaN.

Set membership is distinct from SQL comparison. SQL equality with NaN is false,
while `IS NOT DISTINCT FROM` can match it. A consumer rendering a domain must
express NaN membership explicitly rather than treating it as an equality or `IN`
value. Null allowance also remains separate from the unknown Boolean result of a
SQL comparison.

## Consuming a value set

The four-callback `ValuesProcessor` methods visit ranges, discrete values,
all-or-none sets, or a complete `FloatingPointValueSet`. Its `getOrderedValues()`
returns only the ordered members, with explicit inclusive infinity bounds; the
consumer must also handle `isNaNAllowed()`. Ordered comparisons that need an
extreme value use these bounds, since a compatibility range view may make an outer
endpoint unbounded. Infinity singletons remain bounded in either view.

The older range interface can represent ordered subsets excluding NaN, and the
all-values set including NaN. `FloatingPointValueSet.asRanges()` returns an exact
range representation only in those cases. `getRanges()` and the three-callback
processor reject other NaN-containing sets rather than silently omitting members.
The immutable compatibility representation is computed once and reused for range
reads. Connector support checks use membership flags without rebuilding ranges;
retained-size accounting includes a separate compatibility representation when needed.

Boolean containment and overlap checks compare NaN membership separately and use
the ordered-range predicates directly, preserving their early termination.

Domain simplification preserves NaN membership while widening ordered ranges.
Serialization records the floating-point representation explicitly. Existing
serialized range sets remain readable; their all-values form includes NaN and
their proper subsets exclude it.

## Connector boundary

Connectors receive the complete domain, including independent NaN membership. A
constraint summary may be a conservative approximation of the query predicate;
its membership is nevertheless well defined. The engine retains the query
conditions omitted from that summary.

A connector may enforce the supplied domain, enforce a safe superset and return
the required `remainingFilter`, or decline pushdown. If it widens a domain, it must
not claim to enforce the original restriction. Approximation belongs to the
connector and its column mapping, including after domain compaction. SQL rendering
must implement the chosen predicate without introducing further unreported widening.

The generic JDBC renderer does not support NaN predicates or infinity parameters.
Its pushdown controller retains unsupported restrictions as residuals. A connector
with native support can supply its own column-mapping controller and renderer.
Finite-only column mappings can enforce ordinary ranges without a NaN residual.
Mappings with infinity support can use bounded ordered ranges to exclude NaN even
when the remote database orders it above or below numbers. For example, the JDBC
`FloatingPointQueryBuilder` renders explicit infinity bounds and pairs with
`FLOATING_POINT_PUSHDOWN`; Oracle selects this representation only for its binary
floating-point types. NaN-containing domains still require a residual when the
renderer has no native NaN predicate.
Generic unbounded ordered comparisons also retain a residual because a backend may order
NaN above or below numbers. Dynamic filters are approximated before intersection
with constraints the connector has already agreed to enforce.
An ordered universe bounded by infinities can be rendered as `IS NOT NULL` when
that column cannot contain NaN, or by a native NaN exclusion when supported; the
engine representation does not require binding those endpoints as SQL parameters.

Split enumeration and page sources can use conservative approximations of dynamic
filters, or ignore unsupported restrictions. The join remains authoritative.
Index lookup restrictions require the same exact lookup semantics as before;
unsupported restrictions must not be silently dropped.

File statistics are conservative domains of values that may occur in a section.
Finite floating-point min/max bounds do not establish that NaN is absent. Where
statistics cannot rule it out, their domain also allows NaN. Dictionary and Bloom
filter pruning must likewise avoid false negatives for NaN-containing predicates.

This SPI change requires consumers to handle or safely decline the new domains;
recompilation alone is insufficient. There is no universal connector capability
switch or engine-wide conversion to legacy ranges.

Native floating-point predicates must preserve Trino's signed-zero equivalence.
Iceberg equality sets include both signs of zero; inclusive bounds admit both
signs and exclusive bounds exclude both. Bloom filters are skipped for a column
whose domain allows NaN, before expanding discrete values or loading its filter.
Other columns remain eligible for pruning.
