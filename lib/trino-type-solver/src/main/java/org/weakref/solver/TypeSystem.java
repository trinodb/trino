/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.solver;

import org.weakref.solver.Expression.FunctionType;
import org.weakref.solver.Expression.Variable;
import org.weakref.solver.type.TypeConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.variable;

/**
 * Registry of type constructors and rules.
 * <p>
 * Holds three rule lists:
 * <ul>
 *   <li><b>Coercions</b> — implicit conversions. Used by the {@link Solver} when resolving
 *       {@link Subtype} constraints.</li>
 *   <li><b>Cast rules</b> — explicit-only conversions that do <i>not</i> apply implicitly.
 *       Consulted by {@link #castPlan} as a secondary source after trying implicit coercions.</li>
 *   <li><b>Type constructors</b> — the catalog of named types (primitive + parametric).
 *       Each constructor carries validation constraints that are injected into the solver
 *       whenever a type expression using that constructor appears in a problem.</li>
 * </ul>
 * The instance is immutable and safe to share across solver runs.
 */
public class TypeSystem
{
    private final List<TypeConstructor> types;
    private final List<CoercionRule> coercions;
    private final List<CoercionRule> castRules;
    private final List<IndexedRule> indexedCoercions;
    private final Map<String, List<CoercionRule>> candidateCoercionCache = new ConcurrentHashMap<>();

    public TypeSystem(List<TypeConstructor> types, List<CoercionRule> coercions)
    {
        this(types, coercions, List.of());
    }

    public TypeSystem(List<TypeConstructor> types, List<CoercionRule> coercions, List<CoercionRule> castRules)
    {
        this.types = List.copyOf(types);
        this.coercions = List.copyOf(coercions);
        this.castRules = List.copyOf(castRules);
        this.indexedCoercions = this.coercions.stream()
                .map(rule -> new IndexedRule(rule, fromBaseOf(rule), toBaseOf(rule)))
                .toList();
    }

    /**
     * A coercion rule tagged with the base type names of its source and target patterns ({@code empty}
     * when a side is a variable or otherwise unconstrained — i.e. matches any base). Used to prune the
     * rule scan: a rule whose concrete source/target base differs from the query's cannot unify, so it
     * is skipped without the cost of instantiating and unifying its patterns.
     */
    private record IndexedRule(CoercionRule rule, Optional<String> fromBase, Optional<String> toBase) {}

    /**
     * The coercion rules that could conceivably match {@code from <: to}, in registration order: every
     * rule except those whose concrete source base differs from a concrete {@code from} base, or whose
     * concrete target base differs from a concrete {@code to} base. Filtering by base is sound because
     * unification of a concrete head can only succeed against the same head (or a variable).
     */
    List<CoercionRule> candidateCoercions(Expression from, Expression to)
    {
        Optional<String> fromBase = baseOf(from);
        Optional<String> toBase = baseOf(to);
        if (fromBase.isEmpty() && toBase.isEmpty()) {
            return coercions;
        }
        // The candidate set depends only on the source/target base names over the immutable rule set,
        // and the same (base, base) pairs recur constantly across resolutions, so memoize the scan.
        return candidateCoercionCache.computeIfAbsent(
                fromBase.orElse("") + ' ' + toBase.orElse(""),
                _ -> filterCoercions(fromBase, toBase));
    }

    private List<CoercionRule> filterCoercions(Optional<String> fromBase, Optional<String> toBase)
    {
        List<CoercionRule> candidates = new ArrayList<>();
        for (IndexedRule indexed : indexedCoercions) {
            boolean fromMatches = fromBase.isEmpty() || indexed.fromBase().isEmpty() || indexed.fromBase().equals(fromBase);
            boolean toMatches = toBase.isEmpty() || indexed.toBase().isEmpty() || indexed.toBase().equals(toBase);
            if (fromMatches && toMatches) {
                candidates.add(indexed.rule());
            }
        }
        return List.copyOf(candidates);
    }

    private static Optional<String> fromBaseOf(CoercionRule rule)
    {
        return switch (rule) {
            case PrimitiveTypeCoercion primitive -> Optional.of(primitive.fromType());
            case ParametricTypeCovariantCoercion covariant -> Optional.of(covariant.type());
            case PatternCoercion pattern -> baseOf(pattern.fromPattern());
            default -> Optional.empty();
        };
    }

    private static Optional<String> toBaseOf(CoercionRule rule)
    {
        return switch (rule) {
            case PrimitiveTypeCoercion primitive -> Optional.of(primitive.toType());
            case ParametricTypeCovariantCoercion covariant -> Optional.of(covariant.type());
            case PatternCoercion pattern -> baseOf(pattern.toPattern());
            default -> Optional.empty();
        };
    }

    private static Optional<String> baseOf(Expression expression)
    {
        return switch (expression) {
            case Expression.Symbol(String name) -> Optional.of(name);
            case Expression.Application(Expression head, List<Expression> _) when head instanceof Expression.Symbol(String name) -> Optional.of(name);
            default -> Optional.empty();
        };
    }

    public List<CoercionRule> castRules()
    {
        return castRules;
    }

    public List<TypeConstructor> types()
    {
        return types;
    }

    /**
     * Look up a type constructor by name and arity.
     * <p>
     * Returns the unique constructor matching (name, arity), or empty if none. Multiple
     * constructors may share a name but must have different arities — for example, unbounded
     * {@code varchar} (0 args) and bounded {@code varchar(n)} (1 arg) are two distinct
     * constructors.
     */
    public Optional<TypeConstructor> findConstructor(String name, int arity)
    {
        return types.stream()
                .filter(type -> type.name().equals(name))
                .filter(type -> type.variadic() ? arity >= Math.max(0, type.parameters().size() - 1) : type.parameters().size() == arity)
                .findFirst();
    }

    public List<CoercionRule> coercions()
    {
        return coercions;
    }

    /**
     * Whether {@code type} supports equality comparison — the obligation imposed by
     * {@link RequireComparable}. Container types are comparable iff their type arguments are
     * (see {@link TypeConstructor.Trait#STRUCTURAL}); unresolved variables and unknown
     * constructors are treated permissively (not refuted).
     */
    public boolean isComparable(Expression type)
    {
        return hasTrait(type, TypeConstructor::comparable);
    }

    /**
     * Whether {@code type} supports ordering — the obligation imposed by
     * {@link RequireOrderable}. Follows the same structural and permissive rules as
     * {@link #isComparable}.
     */
    public boolean isOrderable(Expression type)
    {
        return hasTrait(type, TypeConstructor::orderable);
    }

    private boolean hasTrait(Expression type, Function<TypeConstructor, TypeConstructor.Trait> selector)
    {
        return switch (type) {
            // An unresolved variable hasn't been forced to a concrete type, so there's nothing to refute yet.
            case Variable _ -> true;
            case Expression.Symbol(String name) -> constructorHasTrait(name, List.of(), selector);
            case Expression.Application(Expression.Symbol(String name), List<Expression> arguments) -> constructorHasTrait(name, arguments, selector);
            // Rows carry their fields inline rather than as a named constructor application; treat them structurally.
            case Expression.Row(List<Expression.RowField> fields) -> fields.stream().allMatch(field -> hasTrait(field.type(), selector));
            // A row with unknown shape can't be refuted.
            case Expression.AnyRow _ -> true;
            // Function types support neither equality nor ordering.
            case FunctionType _ -> false;
            // Non-symbol application head, or a numeric expression in type position: nothing to refute.
            case Expression.Application _, Expression.Literal _, Expression.BinaryOperation _, Expression.Conditional _ -> true;
        };
    }

    private boolean constructorHasTrait(String name, List<Expression> arguments, Function<TypeConstructor, TypeConstructor.Trait> selector)
    {
        Optional<TypeConstructor> constructor = findConstructor(name, arguments.size());
        if (constructor.isEmpty()) {
            // Unknown constructor — be permissive rather than reject a type we can't classify.
            return true;
        }
        return switch (selector.apply(constructor.orElseThrow())) {
            case ABSENT -> false;
            case PRESENT -> true;
            // Structural: comparable/orderable iff every type argument is. Numeric arguments
            // (e.g. the precision of decimal(p, s)) never reach here because such constructors
            // are PRESENT, but guard against them anyway.
            case STRUCTURAL -> arguments.stream()
                    .filter(argument -> !(argument instanceof Expression.Literal) && !(argument instanceof Expression.BinaryOperation))
                    .allMatch(argument -> hasTrait(argument, selector));
        };
    }

    /**
     * Return a plan describing an explicit cast from {@code from} to {@code to}.
     * <p>
     * Three-tier lookup: first tries implicit coercion rules (any implicit conversion is
     * also a valid cast); if none match, tries the cast-only rule set. Guards that depend
     * on ground types are pre-checked so a cast like {@code array(date) → json} is
     * rejected because the element cast {@code date → json} does not exist.
     */
    public Optional<CoercionPlan> castPlan(Expression from, Expression to)
    {
        // Any valid implicit coercion is a valid cast.
        Optional<CoercionPlan> implicit = coercionPlan(from, to);
        if (implicit.isPresent()) {
            return implicit;
        }
        // Covariant containers and rows cast element-wise when each element casts.
        Optional<CoercionPlan> structural = structuralCastPlan(from, to);
        if (structural.isPresent()) {
            return structural;
        }
        // Otherwise consult cast-specific rules — filter out matches whose ground guards fail.
        List<CoercionPlan> directPlans = castRules.stream()
                .map(rule -> rule.matches(new VariableAllocator(), from, to))
                .flatMap(Optional::stream)
                .filter(match -> match.constraints().stream().allMatch(this::isGuardSatisfiable))
                .map(CoercionRule.Match::plan)
                .flatMap(Optional::stream)
                .filter(plan -> plan.kind() == CoercionPlan.Kind.DIRECT)
                .distinct()
                .toList();
        if (directPlans.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(directPlans.getFirst());
    }

    private Optional<CoercionPlan> structuralCastPlan(Expression from, Expression to)
    {
        if (from instanceof Expression.Row(List<Expression.RowField> leftFields) &&
                to instanceof Expression.Row(List<Expression.RowField> rightFields) &&
                leftFields.size() == rightFields.size()) {
            return structuralCast(
                    from,
                    to,
                    "row",
                    leftFields.stream().map(Expression.RowField::type).toList(),
                    rightFields.stream().map(Expression.RowField::type).toList());
        }
        if (from instanceof Expression.Application(Expression.Symbol(String leftName), List<Expression> leftArguments) &&
                to instanceof Expression.Application(Expression.Symbol(String rightName), List<Expression> rightArguments) &&
                leftName.equals(rightName) &&
                leftArguments.size() == rightArguments.size() &&
                isCovariantType(leftName)) {
            return structuralCast(from, to, leftName, leftArguments, rightArguments);
        }
        return Optional.empty();
    }

    private Optional<CoercionPlan> structuralCast(Expression from, Expression to, String constructor, List<Expression> fromTypes, List<Expression> toTypes)
    {
        if (fromTypes.size() != toTypes.size()) {
            return Optional.empty();
        }
        List<CoercionPlan> children = new ArrayList<>();
        for (int index = 0; index < fromTypes.size(); index++) {
            Optional<CoercionPlan> child = castPlan(fromTypes.get(index), toTypes.get(index));
            if (child.isEmpty()) {
                return Optional.empty();
            }
            if (!child.orElseThrow().isExact()) {
                children.add(child.orElseThrow());
            }
        }
        if (children.isEmpty()) {
            return Optional.of(CoercionPlan.exact(from, to));
        }
        return Optional.of(CoercionPlan.derived(
                from,
                to,
                List.of(new CoercionPlan.Structural(constructor, List.copyOf(children)))));
    }

    private boolean isGuardSatisfiable(Constraint constraint)
    {
        return switch (constraint) {
            case RequireCastableTo(Expression source, Expression target) -> !Expression.isGround(source) || !Expression.isGround(target) || castPlan(source, target).isPresent();
            case RequireCastableFrom(Expression target, Expression source) -> !Expression.isGround(source) || !Expression.isGround(target) || castPlan(source, target).isPresent();
            // A numeric guard over ground operands (e.g. 50 <= 5 from a varchar narrowing) can be
            // decided here. Non-ground guards are left for the full solver and treated permissively.
            case NumericRelation relation -> evaluateGroundComparison(relation.operation()).orElse(true);
            default -> true;
        };
    }

    private static Optional<Boolean> evaluateGroundComparison(Expression.BinaryOperation operation)
    {
        Expression left = Expression.evaluate(operation.left());
        Expression right = Expression.evaluate(operation.right());
        if (!(left instanceof Expression.Literal(int leftValue)) || !(right instanceof Expression.Literal(int rightValue))) {
            return Optional.empty();
        }
        return switch (operation.operator()) {
            case LESS_THAN -> Optional.of(leftValue < rightValue);
            case LESS_THAN_OR_EQUAL -> Optional.of(leftValue <= rightValue);
            case GREATER_THAN -> Optional.of(leftValue > rightValue);
            case GREATER_THAN_OR_EQUAL -> Optional.of(leftValue >= rightValue);
            case EQUAL -> Optional.of(leftValue == rightValue);
            case NOT_EQUAL -> Optional.of(leftValue != rightValue);
            case ADD, SUBTRACT, MULTIPLY, DIVIDE, MIN, MAX -> Optional.empty();
        };
    }

    public Optional<CoercionPlan> coercionPlan(Expression from, Expression to)
    {
        if (from.equals(to)) {
            return Optional.of(CoercionPlan.exact(from, to));
        }

        if (from instanceof Expression.Row(List<Expression.RowField> leftFields) &&
                to instanceof Expression.Row(List<Expression.RowField> rightFields) &&
                leftFields.size() == rightFields.size()) {
            return structuralPlan(
                    from,
                    to,
                    "row",
                    leftFields.stream().map(Expression.RowField::type).toList(),
                    rightFields.stream().map(Expression.RowField::type).toList());
        }

        if (from instanceof Expression.Row && to instanceof Expression.AnyRow) {
            return Optional.of(CoercionPlan.exact(from, to));
        }

        if (from instanceof Expression.Application(Expression.Symbol(String leftName), List<Expression> leftArguments) &&
                to instanceof Expression.Application(Expression.Symbol(String rightName), List<Expression> rightArguments) &&
                leftName.equals(rightName) &&
                leftArguments.size() == rightArguments.size() &&
                isCovariantType(leftName)) {
            return structuralPlan(from, to, leftName, leftArguments, rightArguments);
        }

        // Function types are invariant in their parameters and covariant in the return — a lambda
        // whose body produces a subtype of the declared result satisfies the formal (the engine
        // coerces the lambda's return expression, never its parameters)
        if (from instanceof Expression.FunctionType fromFunction &&
                to instanceof Expression.FunctionType toFunction &&
                fromFunction.parameterTypes().equals(toFunction.parameterTypes()) &&
                !fromFunction.isVariadic() &&
                !toFunction.isVariadic()) {
            return structuralPlan(from, to, "function", List.of(fromFunction.returnType()), List.of(toFunction.returnType()));
        }

        List<CoercionPlan> matchedPlans = coercions.stream()
                .map(coercion -> coercion.matches(new VariableAllocator(), from, to))
                .flatMap(Optional::stream)
                .filter(match -> match.constraints().stream().allMatch(this::isGuardSatisfiable))
                .map(CoercionRule.Match::plan)
                .flatMap(Optional::stream)
                .distinct()
                .toList();

        // A rule declaring the conversion exact (a re-encoding, not a coercion) settles the plan:
        // there is nothing to convert, whatever other rules might offer
        Optional<CoercionPlan> exactPlan = matchedPlans.stream()
                .filter(CoercionPlan::isExact)
                .findFirst();
        if (exactPlan.isPresent()) {
            return exactPlan;
        }

        List<CoercionPlan> directPlans = matchedPlans.stream()
                .filter(plan -> plan.kind() == CoercionPlan.Kind.DIRECT)
                .toList();
        if (!directPlans.isEmpty()) {
            if (directPlans.size() == 1) {
                return Optional.of(directPlans.getFirst());
            }
            List<CoercionPlan.DirectRule> directRules = directPlans.stream()
                    .flatMap(plan -> plan.steps().stream())
                    .filter(CoercionPlan.DirectRule.class::isInstance)
                    .map(CoercionPlan.DirectRule.class::cast)
                    .distinct()
                    .toList();
            return Optional.of(CoercionPlan.directSteps(from, to, directRules));
        }

        return Optional.empty();
    }

    private Optional<CoercionPlan> structuralPlan(
            Expression from,
            Expression to,
            String constructor,
            List<Expression> fromTypes,
            List<Expression> toTypes)
    {
        if (fromTypes.size() != toTypes.size()) {
            return Optional.empty();
        }

        List<CoercionPlan> children = new ArrayList<>();
        for (int index = 0; index < fromTypes.size(); index++) {
            Optional<CoercionPlan> child = coercionPlan(fromTypes.get(index), toTypes.get(index));
            if (child.isEmpty()) {
                return Optional.empty();
            }
            if (!child.orElseThrow().isExact()) {
                children.add(child.orElseThrow());
            }
        }

        if (children.isEmpty()) {
            return Optional.of(CoercionPlan.exact(from, to));
        }
        return Optional.of(CoercionPlan.derived(
                from,
                to,
                List.of(new CoercionPlan.Structural(constructor, List.copyOf(children)))));
    }

    public record CoercionResult(Expression type, Set<Constraint> guards, CoercionPlan plan) {}

    public List<CoercionResult> coercionsTo(Expression type, VariableAllocator allocator)
    {
        LinkedHashSet<CoercionResult> results = new LinkedHashSet<>(collectCoercionResults(variable("@x"), type, allocator));
        results.addAll(liftCovariantCoercions(type, allocator, false));
        return List.copyOf(results);
    }

    public List<CoercionResult> coercionsFrom(Expression type, VariableAllocator allocator)
    {
        LinkedHashSet<CoercionResult> results = new LinkedHashSet<>(collectCoercionResults(type, variable("@x"), allocator));
        results.addAll(liftCovariantCoercions(type, allocator, true));
        return List.copyOf(results);
    }

    public Optional<Expression> getCommonSupertype(Expression left, Expression right)
    {
        if (left.equals(right)) {
            return Optional.of(left);
        }
        String witness = new VariableAllocator().newVariable();
        try {
            Solver.Result result = new Solver(this).solve(List.of(
                    new Subtype(left, new Variable(witness)),
                    new Subtype(right, new Variable(witness))));
            return Optional.ofNullable(result.materializedTypeVariables().get(witness));
        }
        catch (UnsatisfiableException _) {
            return Optional.empty();
        }
    }

    private List<CoercionResult> collectCoercionResults(Expression from, Expression to, VariableAllocator allocator)
    {
        Variable variable = new Variable("@x");
        List<CoercionRule.Match> matches = candidateCoercions(from, to).stream()
                .map(coercion -> coercion.matches(allocator, from, to))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();

        List<CoercionResult> result = new ArrayList<>();
        for (CoercionRule.Match match : matches) {
            boolean emitted = false;
            for (Constraint constraint : match.constraints()) {
                if (constraint instanceof ExactType exact && exact.variable().equals(variable.name())) {
                    Set<Constraint> constraints = new HashSet<>(match.constraints());
                    constraints.remove(constraint);
                    CoercionPlan plan = match.plan()
                            .map(value -> value.apply(Map.of(exact.variable(), exact.type())))
                            .orElseGet(() -> coercionPlan(from, exact.type())
                                    .orElse(CoercionPlan.exact(from, exact.type())));
                    result.add(new CoercionResult(exact.type(), constraints, plan));
                    emitted = true;
                }
            }
            if (!emitted && match.plan().isPresent() && match.plan().orElseThrow().targetType().equals(variable)) {
                result.add(new CoercionResult(variable, Set.copyOf(match.constraints()), match.plan().orElseThrow()));
            }
        }

        return result;
    }

    /// Lift component-wise coercions to a covariant type (a row, or a parametric type whose
    /// arguments coerce covariantly). The type's coercion targets (or sources) form the cartesian
    /// product of its components' candidates, which grows exponentially with width — a six-field
    /// row already yields millions of combinations. Instead of enumerating them, return a single
    /// symbolic witness: the same shape over fresh variables, guarded by one [Subtype] per
    /// component. Discharging a guard runs the component through the same machinery that built
    /// this domain, so the solver tracks every component independently and the product becomes a
    /// sum; the witness's coercion plan is recovered pairwise from the materialized types.
    private List<CoercionResult> liftCovariantCoercions(Expression type, VariableAllocator allocator, boolean fromDirection)
    {
        if (type instanceof Expression.Row(List<Expression.RowField> fields)) {
            return symbolicCovariantCoercion(
                    fields.stream().map(Expression.RowField::type).toList(),
                    variables -> new Expression.Row(namedFields(fields, variables)),
                    allocator,
                    fromDirection);
        }

        if (!(type instanceof Expression.Application(Expression.Symbol(String name), List<Expression> arguments))) {
            return List.of();
        }
        if (!isCovariantType(name)) {
            return List.of();
        }
        return symbolicCovariantCoercion(
                arguments,
                variables -> apply(name, variables.toArray(Expression[]::new)),
                allocator,
                fromDirection);
    }

    private List<CoercionResult> symbolicCovariantCoercion(
            List<Expression> components,
            Function<List<Expression>, Expression> witnessBuilder,
            VariableAllocator allocator,
            boolean fromDirection)
    {
        // A component with no coercion candidates of its own admits no lifted witnesses
        for (Expression component : components) {
            List<CoercionResult> candidates = fromDirection ? coercionsFrom(component, allocator) : coercionsTo(component, allocator);
            if (candidates.isEmpty()) {
                return List.of();
            }
        }

        List<Expression> variables = new ArrayList<>(components.size());
        LinkedHashSet<Constraint> guards = new LinkedHashSet<>();
        for (Expression component : components) {
            Variable variable = new Variable(allocator.newVariable());
            variables.add(variable);
            guards.add(fromDirection ? new Subtype(component, variable) : new Subtype(variable, component));
        }
        Expression witness = witnessBuilder.apply(variables);
        return List.of(new CoercionResult(witness, Set.copyOf(guards), CoercionPlan.exact(witness, witness)));
    }

    private static List<Expression.RowField> namedFields(List<Expression.RowField> fields, List<Expression> types)
    {
        List<Expression.RowField> result = new ArrayList<>(fields.size());
        for (int index = 0; index < fields.size(); index++) {
            result.add(new Expression.RowField(fields.get(index).name(), types.get(index)));
        }
        return List.copyOf(result);
    }

    private boolean isCovariantType(String name)
    {
        return coercions.stream()
                .filter(ParametricTypeCovariantCoercion.class::isInstance)
                .map(ParametricTypeCovariantCoercion.class::cast)
                .anyMatch(coercion -> coercion.type().equals(name));
    }

    /// Whether the type coerces to anything: some rule's target pattern is a bare variable, the
    /// way the unknown rule (`unknown <: @X`) declares. Such a type is the bottom of the coercion
    /// lattice, and binding extraction assumes its nested types are bottom as well — a parametric
    /// formal facing it binds every type parameter to it, the way the engine treats nulls.
    public boolean isBottom(Expression type)
    {
        return coercions.stream().anyMatch(rule -> rule instanceof PatternCoercion pattern
                && pattern.toPattern() instanceof Expression.Variable
                && Unifier.unify(Expression.instantiate(pattern.fromPattern(), new VariableAllocator()).expression(), type) instanceof Unifier.Success);
    }

    public List<Constraint> instantiateValidationConstraints(Expression expression)
    {
        if (!(expression instanceof Expression.Application(Expression.Symbol(String name), List<Expression> arguments))) {
            return List.of();
        }

        // The lookup must respect arity: a name can have constructors at several arities
        // (unbounded varchar is parameterless, bounded varchar takes a length), and matching by
        // name alone finds whichever registered first and silently drops the constraints.
        Optional<TypeConstructor> constructor = findConstructor(name, arguments.size());

        if (constructor.isEmpty()) {
            return List.of();
        }

        TypeConstructor ctor = constructor.orElseThrow();
        List<String> parameters = ctor.parameters();

        if (ctor.variadic()) {
            if (parameters.isEmpty()) {
                return List.of();
            }
            int fixedCount = parameters.size() - 1;
            if (arguments.size() < fixedCount) {
                return List.of();
            }
            String template = parameters.getLast();

            List<Constraint> instantiated = new ArrayList<>();
            for (int index = fixedCount; index < arguments.size(); index++) {
                Map<String, Expression> bindings = new HashMap<>();
                for (int fixedIndex = 0; fixedIndex < fixedCount; fixedIndex++) {
                    bindings.put(parameters.get(fixedIndex), arguments.get(fixedIndex));
                }
                bindings.put(template, arguments.get(index));
                for (Constraint constraint : ctor.constraints()) {
                    instantiated.addAll(instantiateValidationConstraint(constraint, bindings));
                }
            }
            return instantiated;
        }

        if (parameters.size() != arguments.size()) {
            return List.of();
        }

        Map<String, Expression> bindings = new HashMap<>();
        for (int index = 0; index < arguments.size(); index++) {
            bindings.put(parameters.get(index), arguments.get(index));
        }

        List<Constraint> instantiated = new ArrayList<>();
        for (Constraint constraint : ctor.constraints()) {
            instantiated.addAll(instantiateValidationConstraint(constraint, bindings));
        }
        return instantiated;
    }

    private List<Constraint> instantiateValidationConstraint(Constraint constraint, Map<String, Expression> bindings)
    {
        return switch (constraint) {
            case NumericRelation relation -> List.of(relation.apply(bindings));
            case Subtype subtype -> List.of(subtype.apply(bindings));
            case ExactType(String variable, Expression type) -> {
                Expression target = bindings.get(variable);
                if (target instanceof Variable(String name)) {
                    yield List.of(new ExactType(name, Expression.substitute(type, bindings)));
                }
                yield List.of();
            }
            case RequireKind(String variable, Kind kind) -> instantiateKindConstraint(bindings.get(variable), kind);
            case RequireComparable(String variable) -> instantiateUnaryTraitConstraint(bindings.get(variable), true);
            case RequireOrderable(String variable) -> instantiateUnaryTraitConstraint(bindings.get(variable), false);
            case RequireCastableTo castable -> List.of(castable.apply(bindings));
            case RequireCastableFrom castable -> List.of(castable.apply(bindings));
            case Choice choice -> List.of(choice);
        };
    }

    private static List<Constraint> instantiateKindConstraint(Expression expression, Kind kind)
    {
        if (expression == null) {
            return List.of();
        }
        return switch (expression) {
            case Variable(String name) -> List.of(new RequireKind(name, kind));
            case Expression.Literal _, Expression.BinaryOperation _, Expression.Conditional _ -> kind == Kind.NUMBER ? List.of() : List.of();
            case Expression.Symbol _, Expression.Application _, Expression.Row _, Expression.AnyRow _, FunctionType _ -> kind == Kind.TYPE ? List.of() : List.of();
        };
    }

    private static List<Constraint> instantiateUnaryTraitConstraint(Expression expression, boolean comparable)
    {
        if (expression instanceof Variable(String name)) {
            if (comparable) {
                return List.of(new RequireComparable(name));
            }
            return List.of(new RequireOrderable(name));
        }
        return List.of();
    }
}
