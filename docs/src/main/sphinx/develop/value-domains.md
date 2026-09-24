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
to this representation without changing its membership: its all-values form
includes NaN, and its proper subsets exclude it.

## NaN and set operations

NaN is a non-null value. It is not an ordered range endpoint, but it can belong to a
set independently of the ordered values. A NaN singleton, a finite interval plus
NaN, and all ordered values excluding NaN are distinct representable sets.
`containsValue`, union, intersection, subtraction, containment, and overlap account
for NaN, for both representations and in either operand order. `complement()` does
too, except on an older `SortedRangeSet`, which returns only the range complement.
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
consumer must also handle `isNaNAllowed()`.

The older range interface can represent ordered subsets excluding NaN, and the
all-values set including NaN. `FloatingPointValueSet.asRanges()` returns an exact
range representation only in those cases. `getRanges()` and the three-callback
processor reject other NaN-containing sets rather than silently omitting members.
Connectors that render predicates can use the static helpers on
`FloatingPointValueSet`, which also accept other value sets. `orderedRanges()`
returns ordered ranges with unbounded outer endpoints, `boundedRanges()` keeps the
inclusive infinity bounds of a set excluding NaN, and `requiresNaNPredicate()`
identifies sets whose enforcement needs a NaN test.

## Connector boundary

Connectors receive the complete domain, including independent NaN membership. A
constraint summary may be a conservative approximation of the query predicate;
its membership is nevertheless well defined. The engine retains the query
conditions omitted from that summary.

A connector must handle floating-point domains or decline them. It may enforce
the supplied domain, enforce a safe superset and return the required
`remainingFilter`, or decline pushdown. If it widens a domain, it must not claim
to enforce the original restriction.

A renderer without a native NaN predicate keeps a residual for domains that
contain NaN. It also keeps a residual for unbounded ordered comparisons, because
the remote system may order NaN above or below numbers. Bounding a range by the
infinities excludes NaN where the remote system supports infinity values.

Split enumeration and page sources can use conservative approximations of dynamic
filters, or ignore unsupported restrictions. The join remains authoritative.

File statistics and other conservative summaries must not rule NaN out without
proof. Finite minimum and maximum values do not establish that NaN is absent, and
dictionary or Bloom filter pruning must not discard values for a predicate that
admits NaN.

Native floating-point predicates must keep positive and negative zero equal.
