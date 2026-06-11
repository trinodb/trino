# Solver-based expression type checking: the seam

How the prototype in this module (`SolverExpressionTypeChecker`) maps onto Trino's real
expression analysis, what it replaces, and what stands between the prototype and an
integration. The prototype's differential tests (`TestSolverExpressionDifferential`) are the
evidence backing every claim here: the checker and `ExpressionAnalyzer` agree on root types,
per-node types, per-node coercion decisions, and rejection categories over the corpus, with
every function and operator definition bridged from the live catalog.

## The problem being solved

`ExpressionAnalyzer` resolves each function call in isolation and requires every argument
type to be known *before* resolution. Lambdas violate that assumption — a lambda's parameter
types are determined *by* resolution — and the workaround is threaded through three layers:

1. **`TypeDescriptorProvider`** is an Either-in-a-boolean: a ground `TypeDescriptor`, or
   (`hasDependency = true`) a `Function<List<Type>, TypeDescriptor>` callback.
2. **`ExpressionAnalyzer.getCallArgumentTypes`** wraps each lambda argument in a deferred
   provider whose callback constructs a whole **inner `ExpressionAnalyzer`** and re-analyzes
   the lambda against candidate parameter types — once per candidate, per solver iteration.
3. **`SignatureBinder.FunctionSolver`** invokes that callback from inside the binder's
   bounded fixed point (`SOLVE_ITERATION_LIMIT = 4`), making analysis and binding mutually
   recursive through a type-descriptor-valued function.

Independently of lambdas, the analyzer carries a bespoke pairwise
`TypeCoercion.getCommonSuperType` walk for CASE/COALESCE/IF/IN/BETWEEN, and the binder
computes variable bindings twice (forward constraint solving, then backward extraction in
`FunctionBinder`).

## What the prototype demonstrates

Each call is one constraint problem. The protocol, end to end:

1. **No eager/deferred split.** Ground arguments enter the solve as ground type expressions;
   a lambda argument enters as a *function type over fresh variables*. There is no callback:
   the lambda's parameter types fall out of unifying the other arguments against the
   candidate's scheme.
2. **Probe.** Each candidate is matched against the probing arguments. A satisfied candidate
   materializes the lambda's formal parameter type (`function(bigint, @u)`) — the parameter
   types are read off the materialized formal, not requested through a provider.
3. **Discharge.** The one irreducible analyzer obligation — typing the lambda *body* once its
   parameter types are known — runs between the probe and the final solve. This is a plain
   recursive analysis with the parameters in scope, not a re-entrant resolution.
4. **Final solve.** With every argument ground, resolution applies full overload dominance.
   Distinct parameter-type assignments from the probe each get their own discharge and final
   solve; if they disagree on the result type, the call is ambiguous, not resolved by
   declaration order.
5. **The full analyzer product.** The resolution carries per-argument coercion plans
   (`Resolution.argumentCoercions`), so the checker emits exactly what the planner consumes:
   a type for every node and a coercion map — verified node-for-node against
   `ExpressionAnalyzer`'s own `expressionTypes`/`expressionCoercions`.

The analyzer's special-cased syntax needs no special machinery: CASE, COALESCE, IF, NULLIF,
IN, and BETWEEN all reduce to "branches flow into one type variable" — the constraint shape
of a generic `(T, ..., T) -> T` signature — so the least common supertype falls out of the
same coercion search resolution already uses. NULL is the ground `unknown` type plus the
existing `unknown -> X` coercion rule; subscript is the bridged `SUBSCRIPT` operator; `||`
is already `concat` after parsing.

## Replacement map

| Today | Replaced by |
|---|---|
| `TypeDescriptorProvider` (ground \| deferred callback) | Solver argument expressions: ground types, or `FunctionType` over fresh variables for lambdas |
| Inner-`ExpressionAnalyzer` re-analysis per candidate/iteration | One body analysis per distinct parameter-type assignment (usually one) |
| `SignatureBinder.iterativeSolve` 4-iteration fixed point | The solver's constraint worklist (no iteration cap, no `FunctionSolver`) |
| `applyBoundVariables` + `GroundSignature` | `MatchResult.returnType` / `parameterTypes` (materialized formals) |
| Backward binding extraction in `FunctionBinder` | `Resolution.typeBindings` / `numericBindings` (computed once, forward) |
| Per-argument coercion decisions via `TypeCoercion` after binding | `Resolution.argumentCoercions` with `CoercionPlan`s, decided *during* resolution |
| `coerceToSingleType` / pairwise `getCommonSuperType` | A synthetic `(T, ..., T) -> T` scheme per construct |
| `selectMostSpecific` | `Specificity.BY_COERCION_COUNT.then(TrinoSpecificity)` |
| Candidate reconstruction for error messages | `Incomplete.candidates()` — near-misses fall out of the failed solve |

## Integration shape

The visitor architecture survives; the seam is narrow. `visitFunctionCall` (and the operator
visitors) stop building `TypeDescriptorProvider`s and instead:

1. analyze non-lambda arguments as today (they are already typed bottom-up);
2. construct the call's argument expressions — lambdas as function types over fresh
   variables;
3. probe / discharge / final-solve, exactly the loop in
   `SolverExpressionTypeChecker.resolveCall`;
4. transcribe the resolution: return type, argument coercions, and (for the planner)
   the bound signature derived from the materialized formals.

Function schemes come from the catalog at **registration time**: `GlobalFunctionCatalog`
keeps a `TypeScheme` alongside each `Signature`, produced by the equivalent of
`SignatureBridge`. This is why the `type-template` stack is a prerequisite: the bridge
consumes `TypeTemplate` argument/return shapes and structurally distinguished variables
natively — against the pre-cleanup representation it would have to re-derive variable kinds
from out-of-band constraint lists.

Nothing changes at the coordinator/worker boundary. `ResolvedFunction`/`BoundSignature`
(ground types) remain the wire artifact; resolution remains coordinator-only. The
`CachingResolver` seam is untouched — the solver replaces what happens on a cache miss.

Remaining analyzer-owned concerns (correctly so): name and scope resolution, the
aggregate/window/scalar distinction, subqueries, pattern-recognition contexts, and error
message *rendering* (now fed by near-miss candidates instead of re-enumerated ones).

## Gaps between prototype and integration

- **Solver productization.** `org.weakref.solver` is a library under `lib/trino-type-solver`;
  integration means moving it into Trino proper and converging names with the `TypeTemplate`
  model (the solver renames to match Trino, not the reverse).
- **Coverage.** The prototype is scalar-only: no row-field dereference (collides with scoped
  identifier resolution — a name-resolution question), no `IN (subquery)`, `TRY`, quantified
  comparisons, aggregates, or window functions.
- **Type-only coercions.** The analyzer distinguishes type-only coercions; `CoercionPlan`
  carries kinds and rule ids from which the distinction is derivable, but the mapping is
  unverified.
- **Error messages.** `Incomplete` rejection carries near-miss candidates; rendering them
  into today's message shapes (`Unexpected parameters (...) for function ...`) is unwritten.
- **Performance.** `BenchmarkFunctionResolution` exists for the resolution path; the
  expression-checking loop itself is unbenchmarked. The probe adds one extra match per
  lambda-bearing call — bounded by candidate count, amortized by the resolution cache.
- **Numeric conditionals.** Range propagation bounds an undecided conditional by the union of
  its branch ranges; linear-term difference constraints do not see through conditionals.
  Sufficient for the catalog as it stands (validation constraints discharge), but a denser
  use of conditional precision rules could leave matches `Incomplete`.
