# Function preimages

Trino rewrites comparisons over selected scalar functions into predicates over the
functions' inputs. For example, `year(order_date) = 2025` becomes a range over
`order_date`, and `CAST(small_value AS bigint) < 10` becomes a comparison directly
over `small_value`. The resulting predicates expose columns and simple ranges to
predicate inference, domain extraction, and connector pushdown, and they avoid
evaluating the function for every input row.

Scalar functions and coercions have an optional **domain preimage** contract. The
comparison rewrite uses exact results from that contract to move a comparison from a
function result to its distinguished input. Domain extraction and dynamic filtering
use the same contract and may additionally consume a conservative superset while
retaining the original predicate. When a consumer cannot obtain a result with the
exactness it requires, it leaves the expression unchanged.

The provider contract is defined in the developer documentation under
[Domain preimages](../src/main/sphinx/develop/functions.md#domain-preimages), and the
value-set, NaN, and connector rules in
[Value domains](../src/main/sphinx/develop/value-domains.md). This document describes
how the engine consumes that contract.

## Scope

The design covers scalar comparisons in expression-bearing plan nodes. It supports
ordinary equality and ordering comparisons, `IDENTICAL`, `BETWEEN`, bounded
`IN` lists, and their negations when a function supplies the required metadata.
It covers explicit casts and implicit coercions because both resolve to registered
cast functions.

The design does not attempt algebraic inversion of arbitrary expressions. It does
not infer monotonicity from Java code, sample function results, or function names.
It does not cover structurally unrelated rules such as unwrapping row subscripts.

## Vocabulary

For a function `f` and a set of output values `Y`, the **preimage** of `Y` is the set
of inputs whose output belongs to `Y`:

```text
preimage(f, Y) = { x | f(x) is in Y }
```

The preimage is a set even when `f` has no inverse function. For example, the
preimage of `{2025}` through `year(date)` is the entire interval from the beginning
to the end of 2025. It may be a point, an interval, a union of ranges, all values, or
no values.

Preimages are represented as a `Domain`: a set of non-null values plus a separate
null allowance. Providers and consumers share this model with ordinary predicate
extraction and runtime filtering; there is no separate preimage representation.

A **result constraint** is a `Domain` of function results selected by a consumer.
For the true result of `f(x) < 10`, its non-null values are those below 10. For the
true result of `f(x) = 10`, they contain only 10. The domain's null allowance records
whether the null function result belongs to that set; it does not describe an
unknown Boolean result of the predicate.

**Truth domains** describe a predicate over a function result using two disjoint
result constraints: the values for which the predicate is true and the values for
which it is false. Values in neither domain produce unknown. An expression rewrite
needs both classifications, while a filtering consumer needs only the true domain
of the complete predicate.

The **projected argument** is the function argument exposed by the rewrite. Other
arguments are **parameters** and must be plan-time constants. In
`date_trunc('month', timestamp_value)`, `timestamp_value` is the projected argument
and `'month'` is a parameter.

A **projection provider** is the implementation registered with function metadata
that computes a preimage for a resolved function or coercion signature. An exact
projection equals the true preimage; a conservative projection is a superset, useful
only to consumers that preserve a residual check.

## Architecture

The function registry owns whether a function supports projection and how its
preimage is calculated. The optimizer owns comparison syntax, evaluation rules, and
generation of replacement expressions. Types own ordering, ranges, and native
representations. Providers return domains or typed constants, never planner
expressions, so they do not reimplement null handling, operand binding, or Boolean
expression construction.

## Function metadata contract

A function declares a projection through its `FunctionMetadata`, either with the
`@FunctionPreimage` annotation or programmatically through the metadata builder.
The metadata is the canonical contract. Casts use the same contract; the projection
is associated with the resolved coercion's function identity, so cast support does
not depend on recognizing an implementation class or treating every cast as safe.

The provider interface is `DomainPreimage`; its javadoc and the
[developer documentation](../src/main/sphinx/develop/functions.md#domain-preimages)
define the call context, `PreimageResult`, and the meaning of each exactness. In
summary:

- An `EXACT` result contains precisely the inputs whose result belongs to the result
  constraint, among inputs on which the original function succeeds.
- A `CONSERVATIVE` result is a proven superset of those inputs, never a subset.
- An empty result means unsupported; `Domain.none` means a proven empty preimage.

Consumers that replace expressions require `EXACT`. Domain extraction and dynamic
filtering accept `CONSERVATIVE`; extraction keeps the original predicate as a
residual, and dynamic filtering relies on the join to verify matches.

Each invocation projects one set; the consumer owns its meaning as a true or false
domain. Projected true and false domains may overlap on inputs where the original
function fails; the comparison rewrite declines such a pair.

### Candidate inference and provider admission

Registration does not specify an argument position. After constant folding, the
consumer nominates the call's sole nonconstant SQL argument, which may be any
expression; the others must be non-null constants. Calls with multiple nonconstant
arguments decline.

Every provider entry point admits or declines the nominated candidate before reading
parameters. For example, the date-truncation provider supports argument 1 of
`date_trunc('month', ts)` and declines argument 0 of `date_trunc(unit, constant_ts)`.

### Native comparison constant mapping

Array, row, and map comparisons can return unknown for non-null operands, so their
truth sets cannot in general be represented as domains. A provider can instead map
a comparison constant to an input constant such that every supported native
comparison against the mapped constant has the same result, including unknown, on
every successful original evaluation. The rewrite then keeps the native comparison
over the projected argument. For `IN`, every item must map. Mapping declines when
any nested value cannot round-trip exactly.

A provider can also declare that a validated call is a comparison identity: input
and result compare identically, so the call can be removed even when the other
comparison operand varies. `at_timezone` with a valid constant zone is an example.

### Eligibility requirements

For the bound call and accepted parameters, the provider must establish:

1. Rewriting a comparison through its truth domains requires argument and result
   types whose comparison truth sets can be represented as a `Domain`. Exact constant
   mapping covers native comparisons that return unknown for non-null values.
2. With all parameters fixed, the function returns null if and only if the projected
   argument is null.
3. For every input on which the original expression succeeds, the replacement
   succeeds with the same result. A rewrite may eliminate a failure of the original
   expression; it must never introduce a failure into a successful query.
4. Every exact result is exact under Trino's comparison operators, including
   type-specific ordering and representation rules. Every conservative result is a
   proven superset under those same rules.
5. Projection depends on nothing beyond the values in the call context.

Known value-conversion failures while calculating boundaries make the provider
decline, leaving the original expression for runtime evaluation. Providers do not
reinterpret arbitrary exceptions as unsupported input; cancellation, resource
exhaustion, and internal errors propagate.

## SQL null and evaluation semantics

The comparison rewrite owns three-valued expression behavior. A single `Domain` can
identify inputs that make a condition true, but cannot distinguish false from unknown
among the others. For example, a null item in an `IN` list makes nonmatching non-null
inputs produce unknown, which a null allowance cannot represent. The rewrite
therefore projects both truth domains and produces an expression equivalent to:

```text
CASE
    WHEN belongs(input, trueDomain) THEN TRUE
    WHEN belongs(input, falseDomain) THEN FALSE
    ELSE NULL
END
```

where `belongs` is two-valued set membership honoring the domain's null allowance.
The rewrite emits a simpler form only when it is equivalent. It never treats the
complement of a true domain as the false domain without accounting for unknown
values.

Truth domains compose according to SQL's truth tables: `NOT` swaps them, `AND`
intersects true domains and unions false domains, and `OR` does the reverse.
`BETWEEN` composes as `AND` and `IN` as `OR`, so `y BETWEEN NULL AND 6` is false for
values above 6 and unknown otherwise, and `y IN (1, 2, NULL)` is never false.

For example, in `date_trunc('month', ts) = TIMESTAMP '2025-03-01'` the true domain
`{2025-03-01 00:00:00}` maps to `[2025-03-01 00:00:00, 2025-04-01 00:00:00)` over `ts`.
For `date_trunc('month', ts) = TIMESTAMP '2025-03-15'` no input truncates to March 15,
so the preimage is empty and the rewrite yields a null-preserving false.

Input expressions can be nondeterministic or fail. The rewrite evaluates the
projected argument at most once, binding it when the replacement refers to it more
than once. It may suppress an evaluation that would fail, but never moves evaluation
into a branch that originally succeeded without it.

## Rule ownership and optimizer integration

The comparison rewrite runs wherever comparison unwrapping operates, after
expression canonicalization and constant folding expose parameters and comparison
constants. Its output is ordinary IR, so Boolean simplification, domain extraction,
predicate inference, and connector pushdown need no knowledge of projection
metadata. Nested eligible calls are projected one layer at a time, and each layer
must independently produce an exact preimage.

Metadata lookup uses resolved function identity, never a display or canonical name.
An unrelated function named `year`, including one provided by a connector, does not
acquire built-in semantics, and overloads of one name can make different eligibility
decisions. Connector-provided functions can declare a projection in the
`FunctionMetadata` their catalog returns. SQL language functions decline projection.

Within one unit of work, such as an optimizer pass or one dynamic-filter
translation, projection lookups, implementations, casts, and comparators are
resolved once per shape and reused.

## Function-registration validation

`FunctionMetadata` rejects a malformed declaration at registration:

- a projection may be attached only to a deterministic scalar function or cast;
- the signature must have a nonempty, fixed-arity argument list;
- the function must have a non-nullable return and null-propagating arguments;
- a function can declare at most one projection.

Type agreement is validated on each call: the result constraint must use the bound
result type, and a provider result must use the projected argument's type and honor
the null contract.

Providers are trusted semantic implementations, like cast and comparison operators:
an incorrect provider can produce wrong query results. A catalog cannot replace the
metadata of a different function identity.

## Relationship with domain translation

Domain extraction consumes a Boolean expression in a filtering context and returns a
tuple domain together with a remaining expression. It requests the true result
domain of the complete predicate, including negation, and accepts conservative
results. The remaining expression is `TRUE` only when every projection in a nested
chain is exact; otherwise the original predicate is retained. A conservative
preimage is never complemented, because the complement of a superset is a subset.

The comparison rewrite instead produces an equivalent expression usable in any plan
context, has no residual, and accepts only exact preimages. In a filter, its output
is then extracted as an ordinary range over the input.

| Concern | Comparison rewrite | Domain extraction |
|---|---|---|
| Input | Predicate over an eligible function or cast | Any filtering predicate |
| Output | Equivalent expression | Tuple domain plus residual expression |
| Exactness | Exact only | Exact, or conservative with residual |
| SQL null result | Preserved | Only true admits a row |

Domain extraction uses providers as the authority for cast extraction. The
`CAST(varchar AS date)`, `CAST(char AS varchar)` ordering under the standard unpadded
policy, and truncating `CAST(varchar AS char)` cases are conservative. The
varchar-to-date provider declines ranges unbounded on either side, because string
order does not follow year order across year lengths.

## Runtime domains and cast preimages

A dynamic filter collected in a join comparison type is translated to the probe
column's type through the preimage of the probe-side cast. Comparison semantics are
applied to the collected domain first, then a conservative cast preimage is
requested. Because the join verifies matches, a superset is sufficient, and a
declined projection performs no pruning. With function preimages enabled,
dynamic-filter planning admits a cast only when it has a registered preimage.
Runtime projection does not render expressions and remains subject to dynamic
filtering's existing size limits.

## Compatibility and observability

The boolean configuration option `optimizer.function-preimages-enabled`, default
`true`, and the session property `function_preimages_enabled`, which overrides it,
select between function preimages and the legacy implementations: the per-function
unwrap rules, the legacy domain extraction for casts, and the saturated-floor
dynamic-filter coercion. The legacy implementations are retained as an operational
fallback and for same-revision performance comparison.

Only the selected implementation runs for a session; old and new comparison rules
never both act on one session, including predicate pushdown through projections. The
selector is an implementation choice, not a fallback after a provider declines.
Unsupported preimage cases keep their original expression or conservative residual
and do not fall through to legacy rewriting. Session properties that change function
or coercion semantics, such as the char/varchar coercion policy, remain inputs to
eligibility in either implementation.

With function preimages disabled, domain extraction bounds `CAST(varchar AS date)`
ordering comparisons by year prefix, which is unsound for years of other lengths
([#31809](https://github.com/trinodb/trino/issues/31809)); the providers do not
reproduce that behavior.

A provider decline is normal and produces no warning. Connectors remain responsible
for matching remote comparison semantics or retaining the engine predicate, as
described in [Value domains](../src/main/sphinx/develop/value-domains.md).

## Limitations

The contract supports one projected argument with constant parameters. A function
such as addition with two varying operands needs a relational constraint, not a
one-dimensional domain, and is unsupported.

Domain-based rewrites require preimages representable as bounded-size value sets. A
preimage with many disconnected regions may be rejected to protect planning time and
plan size.
