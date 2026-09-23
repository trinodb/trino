# Functions

## Plugin implementation

The function framework is used to implement SQL functions. Trino includes a
number of built-in functions. In order to implement new functions, you can
write a plugin that returns one or more functions from `getFunctions()`:

```java
public class ExampleFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(ExampleNullFunction.class)
                .add(IsNullFunction.class)
                .add(IsEqualOrNullFunction.class)
                .add(ExampleStringFunction.class)
                .add(ExampleAverageFunction.class)
                .build();
    }
}
```

Note that the `ImmutableSet` class is a utility class from Guava.
The `getFunctions()` method contains all of the classes for the functions
that we will implement below in this tutorial.

For a full example in the codebase, see either the `trino-ml` module for
machine learning functions or the `trino-teradata-functions` module for
Teradata-compatible functions, both in the `plugin` directory of the Trino
source.

## Scalar function implementation

The function framework uses annotations to indicate relevant information
about functions, including name, description, return type and parameter
types. Below is a sample function which implements `is_null`:

```java
public class ExampleNullFunction
{
    @ScalarFunction("is_null", deterministic = true)
    @Description("Returns TRUE if the argument is NULL")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNull(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        return (string == null);
    }
}
```

The function `is_null` takes a single `VARCHAR` argument and returns a
`BOOLEAN` indicating if the argument was `NULL`. Note that the argument to
the function is of type `Slice`. `VARCHAR` uses `Slice`, which is essentially
a wrapper around `byte[]`, rather than `String` for its native container type.

The `deterministic` argument indicates that a function has no side effects and,
for subsequent calls with the same argument(s), the function returns the exact
same value(s).

In Trino, deterministic functions don't rely on any changing state
and don't modify any state. The `deterministic` flag is optional and defaults
to `true`.

For example, the function {func}`shuffle` is non-deterministic, since it uses random
values. On the other hand, {func}`now` is deterministic, because subsequent calls in a
single query return the same timestamp.

Any function with non-deterministic behavior is required to set `deterministic = false`
to avoid unexpected results.

- `@SqlType`:

  The `@SqlType` annotation is used to declare the return type and the argument
  types. Note that the return type and arguments of the Java code must match
  the native container types of the corresponding annotations.

- `@SqlNullable`:

  The `@SqlNullable` annotation indicates that the argument may be `NULL`. Without
  this annotation the framework assumes that all functions return `NULL` if
  any of their arguments are `NULL`. When working with a `Type` that has a
  primitive native container type, such as `BigintType`, use the object wrapper for the
  native container type when using `@SqlNullable`. The method must be annotated with
  `@SqlNullable` if it can return `NULL` when the arguments are non-null.

- `@Name`:

  The `@Name` annotation declares the SQL-visible parameter name for an
  argument. With a declared name the function is invocable using the
  named-argument form `f(name => value)` in addition to the positional form.
  Functions without `@Name` annotations on their parameters can only be called
  positionally. For example:

  ```java
  @ScalarFunction("clamp")
  @SqlType(StandardTypes.BIGINT)
  public static long clamp(
          @Name("value") @SqlType(StandardTypes.BIGINT) long value,
          @Name("lo") @SqlType(StandardTypes.BIGINT) long lo,
          @Name("hi") @SqlType(StandardTypes.BIGINT) long hi)
  {
      return Math.max(lo, Math.min(hi, value));
  }
  ```

  After registration the function is callable both ways:

  ```sql
  SELECT clamp(7, 0, 5);
  SELECT clamp(value => 7, hi => 5, lo => 0);
  ```

## Parametric scalar functions

Scalar functions that have type parameters have some additional complexity.
To make our previous example work with any type we need the following:

```java
@ScalarFunction(name = "is_null")
@Description("Returns TRUE if the argument is NULL")
public final class IsNullFunction
{
    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullSlice(@SqlNullable @SqlType("T") Slice value)
    {
        return (value == null);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullLong(@SqlNullable @SqlType("T") Long value)
    {
        return (value == null);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNullDouble(@SqlNullable @SqlType("T") Double value)
    {
        return (value == null);
    }

    // ...and so on for each native container type
}
```

- `@TypeParameter`:

  The `@TypeParameter` annotation is used to declare a type parameter which can
  be used in the argument types `@SqlType` annotation, or return type of the function.
  It can also be used to annotate a parameter of type `Type`. At runtime, the engine
  will bind the concrete type to this parameter. `@OperatorDependency` may be used
  to declare that an additional function for operating on the given type parameter is needed.
  For example, the following function will only bind to types which have an equals function
  defined:

```java
@ScalarFunction(name = "is_equal_or_null")
@Description("Returns TRUE if arguments are equal or both NULL")
public final class IsEqualOrNullFunction
{
    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isEqualOrNullSlice(
            @OperatorDependency(
                    operator = OperatorType.EQUAL,
                    returnType = StandardTypes.BOOLEAN,
                    argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlNullable @SqlType("T") Slice value1,
            @SqlNullable @SqlType("T") Slice value2)
    {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }
        return (boolean) equals.invokeExact(value1, value2);
    }

    // ...and so on for each native container type
}
```

## Another scalar function example

The `lowercaser` function takes a single `VARCHAR` argument and returns a
`VARCHAR`, which is the argument converted to lower case:

```java
public class ExampleStringFunction
{
    @ScalarFunction("lowercaser")
    @Description("Converts the string to alternating case")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice lowercaser(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        String argument = slice.toStringUtf8();
        return Slices.utf8Slice(argument.toLowerCase());
    }
}
```

Note that for most common string functions, including converting a string to
lower case, the Slice library also provides implementations that work directly
on the underlying `byte[]`, which have much better performance. This function
has no `@SqlNullable` annotations, meaning that if the argument is `NULL`,
the result will automatically be `NULL` (the function will not be called).

## Domain preimages

A scalar function can declare how a set of its results maps back to a set of its
inputs. The optimizer uses this *preimage* to expose columns and ranges to predicate
pushdown. For example, the preimage of `2025` through `year(date)` is the range from
the beginning of 2025 to the beginning of 2026.

Attach `@FunctionPreimage(Provider.class)` to the same
method or class as the scalar function declaration. Cast operators use the same
annotation. The provider implements `DomainPreimage` and has a public
no-argument constructor. Programmatic declarations use
`FunctionMetadata.Builder.domainProjection(new DomainProjection(provider))`.
The registry associates this metadata with the function identity, including its
aliases; unrelated functions with the same name do not acquire the projection.

The contract requires a deterministic scalar function of fixed arity, a
non-nullable return, and null-propagating arguments. The resolved signature must
preserve the result of every successful original evaluation. A rewrite may
eliminate an original failure, but must never introduce one. Projection does not
require a `neverFails` declaration. Known failures of planning-time boundary
conversions cause the provider to decline the rewrite. All arguments other than the projected argument must be
non-null constants. For such a call the function result is null if and only if the
projected argument is null. Unsupported signatures and parameters retain the
original expression.

Registration does not specify an argument position. After constant folding, the
consumer nominates the call's sole nonconstant SQL argument. It declines calls with
multiple nonconstant arguments or null parameters; all-constant calls use ordinary
constant folding. The candidate can be an arbitrary expression and retains the
usual single-evaluation guarantees.

Every provider entry point must validate `context.inputArgument()` before
reading constant parameters or computing results. For example, the date-truncation
provider accepts argument 1 and declines a varying unit at argument 0. Unsupported
candidates return an empty optional, or false from `isComparisonIdentity`. A provider
may support different argument positions depending on the bound call. The inferred
index belongs to the per-call context; no argument annotation is required.

The provider receives a `DomainPreimage.Context` describing the function call and a
`Domain` describing the requested result values. The context supplies the
session, concrete SQL argument and result types, fixed argument values, the
`inputArgument` index, and the required exactness. Its function helpers evaluate
the same function on constants, supply casts and result ordering, and check implicit
coercions. Only the selected input has no constant value. Providers are registered
against function metadata, so they do not need a separate function identifier. The provider returns an optional `PreimageResult`:

- `EXACT` means precisely the inputs whose function result belongs to the result
  domain, among inputs on which the original function succeeds.
- `CONSERVATIVE` means a proven superset of those inputs. Subsets are never valid.
- An empty optional means unsupported. `Domain.none(inputType)` means a proven
  empty preimage.

The returned domain must use the projected argument type. Null allowance describes
membership of the null value, independently of SQL's unknown Boolean result.
The shared [value-domain model](value-domains.md) represents NaN membership explicitly
for `REAL`, `DOUBLE`, and `NUMBER`; providers use the same `Domain` set operations
as predicate extraction and runtime filtering. Providers processing ordered ranges
must handle NaN separately through `FloatingPointValueSet`.
Providers are trusted semantic implementations; incorrect results can produce
incorrect query answers. They must not reinterpret arbitrary exceptions as
unsupported input.

Failing inputs do not constrain an exact projection. If independently projected
true and false domains overlap, expression rewriting declines the pair; domain
extraction can still consume a single projection. The large-`IN` shortcut also
declines projected NaN singletons, which SQL equality cannot match.

The dependency helper supplies a lazily resolved native comparator for non-null
function results, with unordered values sorted last. Providers reuse it for the
bound call when comparing constants and calculating boundaries.

Timestamp-to-timestamp-with-time-zone projections decline boundaries in either
occurrence of a repeated local time in the session zone. Reversing a boundary in
the occurrence that the ordinary cast cannot produce can otherwise exclude matching
inputs. Runtime domain consumers perform no pruning when this projection declines.
Admission uses `java.time` zone rules to detect gaps and overlaps. Regional-zone
bounds before 1970 decline projection because their historical rules can differ
from those used by the legacy casts. Fixed-offset zones have no such restriction. DATE-to-timestamp-with-time-zone projection checks adjacent
dates through the ordinary forward cast: midnight offset changes can make a reverse
cast return the wrong date, or make multiple dates produce the same result. If the
adjacent results do not strictly bracket the boundary, projection declines.

Expression replacement requires exact preimages for both the true and false result
domains. Inputs in neither domain produce unknown. `NOT` swaps the domains; `AND`
intersects true domains and unions false domains; `OR` unions true domains and
intersects false domains. Thus `year(d) IN (2025, NULL)` is true in the 2025 range
and unknown elsewhere. Its false domain is empty, and negating it does not admit
any rows. `BETWEEN` applies the same rules to its two comparisons, including null
bounds. Nontrivial projected arguments are bound when needed to avoid repeated evaluation.

`DomainTranslator` consumes only the true domain of the complete filtering
predicate. It permits conservative preimages while retaining the original predicate
as a residual. In nested projections, any conservative step requires that residual,
even if later steps are exact. Negation selects the false result domain before
projection; neither the complement of a true domain nor the complement of a
conservative preimage is a valid general substitute.

Built-in providers cover `year(date)`, `year(timestamp)`, eligible `date_trunc`
units, instant-preserving `at_timezone` calls, and eligible numeric, character,
temporal casts, and exact array, row, and map constant mappings. All comparison unwrapping uses `UnwrapFunctionInComparison`,
including predicates exposed by inlining project assignments during predicate
pushdown. Projection lookup currently supports global function bundles.

`Domain` represents NaN independently of ordered values. True and false sets
account for ordinary comparisons, `IDENTICAL`, and nulls independently. Domain
extraction preserves these sets exactly; the shared renderer uses `IDENTICAL` to
express NaN membership. Connectors receive complete domains and choose whether to
enforce them, push a conservative superset with the required residual, or decline
pushdown. See [](value-domains) for the connector contract.
Providers return `isComparisonIdentity(context) == true` only
when a validated call preserves comparisons with arbitrary expressions of the same
type; this permits removing `at_timezone` even when neither comparison operand is
constant.

For comparisons that can return null for non-null operands, a provider may implement
`comparisonConstant(context, constant)`. The mapped input constant must preserve
every supported native comparison operator, including unknown results, for every successful
original evaluation. The common rule retains the native operator. Array, row, and map
casts use recursive exact mappings that preserve nested nulls; lossy constants and
unsupported element coercions decline. `IN` maps every item without repeating the
input. This contract does not imply that domain extraction can represent the
comparison's truth sets.

The common consumer declines unsupported comparisons, nonconstant parameters,
expansions of lists longer than ten items, and truth domains exceeding a combined
32 ranges. Point-to-point `IN` rewrites that preserve the list size do not require
expansion.
Providers return domains or typed constants, never planner expressions; the comparison rule and domain
translator share value-set rendering while owning their respective null semantics.

Integer-to-floating providers account for all source values that round to a boundary,
using at most the source bit width in bisection steps where direct conversion is not
exact. Runtime domains keep dynamic filtering's size limits rather than expression
expansion budgets. NaN representation and rendering support REAL, DOUBLE, and NUMBER.
Providers collect projected ranges and normalize them together when constructing the
value set, keeping large collected domains from repeatedly rebuilding prior ranges.

## Aggregation function implementation

Aggregation functions use a similar framework to scalar functions, but are
a bit more complex.

- `AccumulatorState`:

  All aggregation functions accumulate input rows into a state object; this
  object must implement `AccumulatorState`. For simple aggregations, just
  extend `AccumulatorState` into a new interface with the getters and setters
  you want, and the framework will generate all the implementations and
  serializers for you. If you need a more complex state object, you will need
  to implement `AccumulatorStateFactory` and `AccumulatorStateSerializer`
  and provide these via the `AccumulatorStateMetadata` annotation.

The following code implements the aggregation function `avg_double` which computes the
average of a `DOUBLE` column:

```java
@AggregationFunction("avg_double")
public class AverageAggregation
{
    @InputFunction
    public static void input(
            LongAndDoubleState state,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    @CombineFunction
    public static void combine(
            LongAndDoubleState state,
            LongAndDoubleState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble();
            DOUBLE.writeDouble(out, value / count);
        }
    }
}
```

The average has two parts: the sum of the `DOUBLE` in each row of the column
and the `LONG` count of the number of rows seen. `LongAndDoubleState` is an interface
which extends `AccumulatorState`:

```java
public interface LongAndDoubleState
        extends AccumulatorState
{
    long getLong();

    void setLong(long value);

    double getDouble();

    void setDouble(double value);
}
```

As stated above, for simple `AccumulatorState` objects, it is sufficient to
just define the interface with the getters and setters, and the framework
will generate the implementation for you.

An in-depth look at the various annotations relevant to writing an aggregation
function follows:

- `@InputFunction`:

  The `@InputFunction` annotation declares the function which accepts input
  rows and stores them in the `AccumulatorState`. Similar to scalar functions
  you must annotate the arguments with `@SqlType`.  Note that, unlike in the above
  scalar example where `Slice` is used to hold `VARCHAR`, the primitive
  `double` type is used for the argument to input. In this example, the input
  function simply keeps track of the running count of rows (via `setLong()`)
  and the running sum (via `setDouble()`).

- `@CombineFunction`:

  The `@CombineFunction` annotation declares the function used to combine two
  state objects. This function is used to merge all the partial aggregation states.
  It takes two state objects, and merges the results into the first one (in the
  above example, just by adding them together).

- `@OutputFunction`:

  The `@OutputFunction` is the last function called when computing an
  aggregation. It takes the final state object (the result of merging all
  partial states) and writes the result to a `BlockBuilder`.

- Where does serialization happen, and what is `GroupedAccumulatorState`?

  The `@InputFunction` is usually run on a different worker from the
  `@CombineFunction`, so the state objects are serialized and transported
  between these workers by the aggregation framework. `GroupedAccumulatorState`
  is used when performing a `GROUP BY` aggregation, and an implementation
  will be automatically generated for you, if you don't specify a
  `AccumulatorStateFactory`

## Deprecated function

The `@Deprecated` annotation has to be used on any function that should no longer be
used. The annotation causes Trino to generate a warning whenever SQL statements
use a deprecated function. When a function is deprecated, the `@Description`
needs to be replaced with a note about the deprecation and the replacement function:

```java
public class ExampleDeprecatedFunction
{
    @Deprecated
    @ScalarFunction("bad_function")
    @Description("(DEPRECATED) Use good_function() instead")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean bad_function()
    {
        return false;
    }
}
```
