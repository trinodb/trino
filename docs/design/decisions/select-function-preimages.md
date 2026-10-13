# Select function preimages per session

Status: accepted.

## Context

The initial preimage design replaces the legacy implementation unconditionally.
That prevents a same-revision performance comparison and removes the operational
fallback while the new implementation is being evaluated. Wide expressions and
large unions can amplify differences that small correctness tests do not expose.

## Decision

Retain the legacy implementations and select function preimages with the boolean
configuration option `optimizer.function-preimages-enabled` and session property
`function_preimages_enabled`. The default is `true`. A session override wins over
the configured default.

Only the selected comparison implementation is eligible to run. Apply the same
choice when predicate pushdown exposes expressions through projection inlining.
Keep the existing rule identities and disable rules before traversing expressions.
Direct provider and rewrite helpers remain independently testable.

The selector also governs consumers of preimages in domain extraction and dynamic
filtering. Those integrations must preserve the legacy extraction and saturated-floor
coercion paths when introducing the selectable preimage path. Retain the operators,
registration, and tests needed by the fallback. The selector is an implementation
choice, not an additional fallback after a provider declines a projection.

This amends the proposal's unconditional replacement and no-configuration policy.
It preserves the single-owner principle: the old and new comparison rules must
never both act on the same session. Unsupported preimage cases remain unchanged;
they do not fall through to legacy rewriting.

## Validation

Test the configured default and explicit mapping, and both session settings across
all expression-bearing rule types. Compare identical benchmark workloads with the
session selector, retaining the selected mode with every result.
