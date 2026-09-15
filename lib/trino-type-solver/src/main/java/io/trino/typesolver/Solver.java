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
package io.trino.typesolver;

import io.trino.typesolver.Expression.BinaryOperation;
import io.trino.typesolver.Expression.Variable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/// Work-list constraint solver.
///
/// Operates in two phases:
///
/// 1. **Worklist** — pop constraints one at a time; each case arm in
///    [#solve] either discharges the constraint, adds more constraints to the
///    worklist, parks it in pending, or throws [UnsatisfiableException]. Numeric
///    relations delegate to [NumericPropagator]; numeric-related worklist items
///    can enter from [RequireKind] handling or from parametric-type validation
///    constraints injected by the type system.
/// 2. **Reconcile** — revisit pending constraints when their dependencies change.
///    Once the worklist drains, refine the affected [Domain]s. Each pass may reintroduce
///    newly-activated constraints to the worklist (e.g. a choice that collapsed to a
///    single alternative), which restarts the outer loop.
///
/// [#solve] returns a raw [Result]; [#solveOutcome] is a higher-level
/// wrapper that distinguishes satisfied vs incomplete vs unsatisfied (for callers that
/// want success/failure rather than exceptions).
public class Solver
{
    private final TypeSystem typeSystem;
    private final SubtypeOracle subtypeOracle;
    private final DomainRefiner domainRefiner;
    private final ChoiceSimplifier choiceSimplifier;

    public Solver(TypeSystem typeSystem)
    {
        this(typeSystem, new SubtypeOracle(typeSystem));
    }

    Solver(TypeSystem typeSystem, SubtypeOracle subtypeOracle)
    {
        this.typeSystem = typeSystem;
        this.subtypeOracle = subtypeOracle;
        this.domainRefiner = new DomainRefiner(subtypeOracle);
        this.choiceSimplifier = new ChoiceSimplifier(subtypeOracle);
    }

    /// Solve the constraint system, returning a raw [Result]. Throws
    /// [UnsatisfiableException] on contradiction.
    public Result solve(List<Constraint> constraints)
    {
        return ResolutionBudget.nested(() -> solveInternal(constraints));
    }

    private Result solveInternal(List<Constraint> constraints)
    {
        GroundBoundCheck.checkRows(typeSystem, constraints);
        VariableAllocator allocator = new VariableAllocator();
        // The incoming constraints carry variables minted by the caller's allocator, which
        // also started at $v1. Seed ours past them so the fresh names we mint while solving
        // (e.g. instantiating coercion rules) can't collide with an input variable and get
        // conflated with it during unification.
        allocator.reserveThrough(maxVariableId(constraints));
        SolverState state = new SolverState();
        CoercionCache coercions = new CoercionCache(typeSystem);

        state.workList().addAll(constraints);
        while (!state.workList().isEmpty()) {
            ResolutionBudget.consume();
            // Substitute the freshest variable bindings before dispatching, so each arm
            // sees the most-resolved form (e.g. after a sibling Subtype bound a var).
            Constraint queued = state.workList().poll();
            if (queued instanceof Subtype(Expression lower, Variable(String name)) &&
                    state.variableStates().get(name) instanceof TypeVariableState typeState && typeState.binding().isPresent()) {
                // Keep the source's field names even after a symbolic structural binding
                // has decomposed this obligation into its independent components.
                typeState.recordLowerBound(lower);
            }
            Constraint candidate = substitute(queued, state.substitutions());
            enqueueValidationConstraints(candidate, state);

            switch (candidate) {
                case ExactType(String variable, Expression type) -> bindTypeVariable(variable, type, state);
                case RequireKind(String variable, Kind kind) -> requireVariableKind(variable, kind, state);
                case RequireComparable(String variable) -> {
                    requireTypeVariable(variable, state);
                    // Keep the trait obligation pending so it is re-checked once the variable resolves.
                    state.pendingConstraints().add(candidate);
                }
                case RequireOrderable(String variable) -> {
                    requireTypeVariable(variable, state);
                    state.pendingConstraints().add(candidate);
                }
                case RequireCastableTo castable -> {
                    if (castable.source() instanceof Variable(String name)) {
                        requireTypeVariable(name, state);
                    }
                    if (castable.target() instanceof Variable(String name)) {
                        requireTypeVariable(name, state);
                    }
                    state.pendingConstraints().add(castable);
                }
                case RequireCastableFrom castable -> {
                    if (castable.source() instanceof Variable(String name)) {
                        requireTypeVariable(name, state);
                    }
                    if (castable.target() instanceof Variable(String name)) {
                        requireTypeVariable(name, state);
                    }
                    state.pendingConstraints().add(castable);
                }
                case Subtype(Variable subtype, Variable supertype) -> {
                    requireTypeVariable(subtype.name(), state).addUpperBound(supertype);
                    requireTypeVariable(supertype.name(), state).addLowerBound(subtype);
                    state.variableChanged(subtype.name());
                    state.variableChanged(supertype.name());
                }
                case Subtype(Variable(String variable), Expression type) -> {
                    TypeVariableState variableState = requireTypeVariable(variable, state);
                    if (type instanceof Expression.AnyRow) {
                        restrictToRow(variable, variableState, state);
                    }
                    else {
                        variableState.addUpperBound(type);
                        state.variableChanged(variable);
                        constrainEnumerableDomain(variable, variableState, type, coercions.to(type, allocator), state);
                    }
                }
                case Subtype(Expression type, Variable(String variable)) -> {
                    TypeVariableState variableState = requireTypeVariable(variable, state);
                    if (type instanceof Expression.AnyRow) {
                        restrictToRow(variable, variableState, state);
                    }
                    else {
                        variableState.addLowerBound(type);
                        state.variableChanged(variable);
                        constrainEnumerableDomain(variable, variableState, type, coercions.from(type, allocator), state);
                    }
                }
                case Subtype(Expression subtype, Expression supertype) -> processConcreteSubtype(subtype, supertype, allocator, state);
                case NumericRelation(BinaryOperation operation) -> NumericPropagator.process(operation, state);
                case Choice choice -> processChoice(choice, state);
            }
            reconcileState(state);
        }
        reconcileState(state);
        return new Result(typeSystem, state.variableStates(), state.pendingConstraints().stream().toList(), subtypeOracle);
    }

    public SolveOutcome solveOutcome(List<Constraint> constraints)
    {
        try {
            Result result = solve(constraints);
            if (result.nextBatch().isEmpty() || result.pendingConstraintsSatisfied()) {
                return new Satisfied(result);
            }
            return new Incomplete(result);
        }
        catch (UnsatisfiableException e) {
            return new Unsatisfied(e.getMessage());
        }
    }

    private static TypeVariableState requireTypeVariable(String variable, SolverState state)
    {
        VariableState variableState = requireVariableKind(variable, Kind.TYPE, state);
        if (!(variableState instanceof TypeVariableState typeVariableState)) {
            throw new IllegalStateException("Expected " + variable + " to be of TYPE kind");
        }
        return typeVariableState;
    }

    private static VariableState requireVariableKind(String variable, Kind kind, SolverState state)
    {
        VariableState variableState = state.variableStates().get(variable);
        if (variableState == null) {
            variableState = switch (kind) {
                case TYPE -> new TypeVariableState();
                case NUMBER -> new NumericVariableState();
            };
            state.putVariableState(variable, variableState);
            return variableState;
        }
        return switch (kind) {
            case TYPE -> {
                if (variableState instanceof NumericVariableState) {
                    throw new IllegalStateException("Expected " + variable + " to be of TYPE kind");
                }
                yield variableState;
            }
            case NUMBER -> {
                if (variableState instanceof TypeVariableState) {
                    throw new IllegalStateException("Expected " + variable + " to be of NUMBER kind");
                }
                yield variableState;
            }
        };
    }

    private static Constraint substitute(Constraint constraint, Map<String, Expression> substitutions)
    {
        if (substitutions.isEmpty()) {
            return constraint;
        }
        return constraint.apply(substitutions);
    }

    public static final class Result
    {
        private final TypeSystem typeSystem;
        private final Map<String, VariableState> variableBounds;
        private final List<Constraint> nextBatch;
        private final ResolutionBudget budget;
        private final SubtypeOracle subtypeOracle;
        private Materialization materialization;

        public Result(TypeSystem typeSystem, Map<String, VariableState> variableBounds, List<Constraint> nextBatch)
        {
            this(typeSystem, variableBounds, nextBatch, new SubtypeOracle(typeSystem));
        }

        private Result(TypeSystem typeSystem, Map<String, VariableState> variableBounds, List<Constraint> nextBatch, SubtypeOracle subtypeOracle)
        {
            this.typeSystem = typeSystem;
            this.variableBounds = copyVariableBounds(variableBounds);
            this.nextBatch = List.copyOf(nextBatch);
            this.budget = ResolutionBudget.current();
            this.subtypeOracle = subtypeOracle;
        }

        public TypeSystem typeSystem()
        {
            return typeSystem;
        }

        public Map<String, VariableState> variableBounds()
        {
            return copyVariableBounds(variableBounds);
        }

        public List<Constraint> nextBatch()
        {
            return nextBatch;
        }

        private static Map<String, VariableState> copyVariableBounds(Map<String, VariableState> bounds)
        {
            Map<String, VariableState> copy = new HashMap<>();
            bounds.forEach((name, state) -> copy.put(name, switch (state) {
                case TypeVariableState typeState -> typeState.copy();
                case NumericVariableState numericState -> numericState;
            }));
            return Map.copyOf(copy);
        }

        public Map<String, Integer> materializedNumericValues()
        {
            return materialize().numericValues();
        }

        public Map<String, Expression> materializedTypeVariables()
        {
            return materialize().typeValues();
        }

        public Map<String, Alternative> selectedAlternatives()
        {
            return materialize().selectedAlternatives();
        }

        public boolean pendingConstraintsSatisfied()
        {
            return budget.run(this::checkPendingConstraints);
        }

        private boolean checkPendingConstraints()
        {
            Map<String, Expression> substitutions = new HashMap<>(materializedTypeVariables());
            materializedNumericValues().forEach((name, value) -> substitutions.put(name, Expression.literal(value)));
            for (Constraint constraint : nextBatch) {
                ResolutionBudget.consume();
                Constraint substituted = constraint.apply(substitutions);
                boolean satisfied = switch (substituted) {
                    case NumericRelation(Expression.BinaryOperation operation) -> evaluateResolvedNumericRelation(operation);
                    case ExactType(String variable, Expression type) -> {
                        Expression resolved = substitutions.get(variable);
                        yield resolved != null && resolved.equals(type);
                    }
                    case Subtype(Expression subtype, Expression supertype) -> isResolvedSubtype(subtype, supertype, typeSystem);
                    case RequireKind _ -> true;
                    case RequireComparable(String variable) -> {
                        Expression resolved = substitutions.get(variable);
                        yield resolved == null || typeSystem.isComparable(Expression.substitute(resolved, substitutions));
                    }
                    case RequireOrderable(String variable) -> {
                        Expression resolved = substitutions.get(variable);
                        yield resolved == null || typeSystem.isOrderable(Expression.substitute(resolved, substitutions));
                    }
                    case RequireCastableTo(Expression source, Expression target) -> typeSystem.castPlan(source, target).isPresent();
                    case RequireCastableFrom(Expression target, Expression source) -> typeSystem.castPlan(source, target).isPresent();
                    case Choice _ -> false;
                };
                if (!satisfied) {
                    return false;
                }
            }
            return true;
        }

        private synchronized Materialization materialize()
        {
            if (materialization == null) {
                SolverMaterializer.Materialization result = budget.run(() -> new SolverMaterializer(typeSystem, variableBounds, nextBatch, subtypeOracle).materialize());
                materialization = new Materialization(result.numericValues(), result.typeValues(), result.selectedAlternatives());
            }
            return materialization;
        }

        private record Materialization(Map<String, Integer> numericValues, Map<String, Expression> typeValues, Map<String, Alternative> selectedAlternatives) {}

        private static boolean evaluateResolvedNumericRelation(Expression.BinaryOperation operation)
        {
            Optional<Boolean> satisfied = evaluateBoolean(operation);
            return satisfied.isPresent() && satisfied.orElseThrow();
        }

        private static boolean isResolvedSubtype(Expression subtype, Expression supertype, TypeSystem typeSystem)
        {
            if (subtype instanceof Expression.Variable || supertype instanceof Expression.Variable) {
                return false;
            }
            if (subtype.equals(supertype)) {
                return true;
            }
            if (subtype instanceof Expression.Row && supertype instanceof Expression.AnyRow) {
                return true;
            }
            return typeSystem.coercionPlan(subtype, supertype).isPresent();
        }

        private static Optional<Boolean> evaluateBoolean(Expression.BinaryOperation operation)
        {
            OptionalInt left = evaluateNumericExpression(operation.left());
            OptionalInt right = evaluateNumericExpression(operation.right());
            if (left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(switch (operation.operator()) {
                case LESS_THAN -> left.orElseThrow() < right.orElseThrow();
                case LESS_THAN_OR_EQUAL -> left.orElseThrow() <= right.orElseThrow();
                case GREATER_THAN -> left.orElseThrow() > right.orElseThrow();
                case GREATER_THAN_OR_EQUAL -> left.orElseThrow() >= right.orElseThrow();
                case EQUAL -> left.orElseThrow() == right.orElseThrow();
                case NOT_EQUAL -> left.orElseThrow() != right.orElseThrow();
                default -> throw new UnsupportedOperationException("Expected comparison operator");
            });
        }

        private static OptionalInt evaluateNumericExpression(Expression expression)
        {
            return switch (expression) {
                case Expression.Literal(int value) -> OptionalInt.of(value);
                case Expression.BinaryOperation(Expression.BinaryOperator operator, Expression left, Expression right) -> {
                    OptionalInt leftValue = evaluateNumericExpression(left);
                    OptionalInt rightValue = evaluateNumericExpression(right);
                    if (leftValue.isEmpty() || rightValue.isEmpty()) {
                        yield OptionalInt.empty();
                    }
                    // Saturating like Expression.evaluate, so wrapped arithmetic cannot fail
                    // a calculated varchar length's validation
                    yield switch (operator) {
                        case ADD -> OptionalInt.of(Expression.saturateToInt((long) leftValue.orElseThrow() + rightValue.orElseThrow()));
                        case SUBTRACT -> OptionalInt.of(Expression.saturateToInt((long) leftValue.orElseThrow() - rightValue.orElseThrow()));
                        case MULTIPLY -> OptionalInt.of(Expression.saturateToInt((long) leftValue.orElseThrow() * rightValue.orElseThrow()));
                        case DIVIDE -> rightValue.orElseThrow() == 0 ? OptionalInt.empty() : OptionalInt.of(leftValue.orElseThrow() / rightValue.orElseThrow());
                        case MIN -> OptionalInt.of(Math.min(leftValue.orElseThrow(), rightValue.orElseThrow()));
                        case MAX -> OptionalInt.of(Math.max(leftValue.orElseThrow(), rightValue.orElseThrow()));
                        default -> OptionalInt.empty();
                    };
                }
                case Expression.Conditional(Expression.BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> {
                    Optional<Boolean> holds = evaluateBoolean(condition);
                    if (holds.isEmpty()) {
                        yield OptionalInt.empty();
                    }
                    yield evaluateNumericExpression(holds.orElseThrow() ? ifTrue : ifFalse);
                }
                default -> OptionalInt.empty();
            };
        }
    }

    /// Constrains the variable's domain to the given coercion results — unless one of them is a
    /// wildcard: a witness with a variable that no guard constrains, as produced by the unknown
    /// rule (`unknown <: X`) directly or lifted covariantly (`array(unknown) <: array(x)`). A
    /// wildcard means the bound type coerces to ANYTHING of that shape — the domain cannot be
    /// enumerated, and treating the wildcard as a concrete alternative poisons dominance pruning
    /// (it and any concrete witness dominate each other, emptying the domain). Shaped witnesses
    /// whose variables ARE guarded (`varchar(n2)` under `n1 <= n2`) remain enumerable. The bound
    /// itself still holds either way, and materialization defaults an otherwise-unconstrained
    /// variable from its ground bounds.
    private void constrainEnumerableDomain(String variable, TypeVariableState variableState, Expression boundType, List<TypeSystem.CoercionResult> coercionResults, SolverState state)
    {
        Set<String> boundVariables = new HashSet<>();
        walkExpression(boundType, expression -> {
            if (expression instanceof Variable(String name)) {
                boundVariables.add(name);
            }
        });
        List<Alternative> alternatives = toAlternatives(coercionResults);
        if (alternatives.stream().anyMatch(alternative -> hasWildcardVariable(alternative, boundVariables))) {
            return;
        }
        constrainDomain(variable, variableState, alternatives, state);
    }

    /// A wildcard variable in a witness is one that nothing pins down: it is neither a variable of
    /// the bound type itself (an exact witness of a symbolic bound mirrors its variables) nor
    /// mentioned by any guard (a widening witness like `varchar(n2)` is pinned by `n1 <= n2`).
    /// The unknown rule produces them (`unknown <: X`, lifted covariantly to `array(x)`).
    private static boolean hasWildcardVariable(Alternative alternative, Set<String> boundVariables)
    {
        if (Expression.isGround(alternative.witness())) {
            return false;
        }
        Set<String> witnessVariables = new HashSet<>();
        walkExpression(alternative.witness(), expression -> {
            if (expression instanceof Variable(String name)) {
                witnessVariables.add(name);
            }
        });
        witnessVariables.removeAll(boundVariables);
        if (witnessVariables.isEmpty()) {
            return false;
        }
        Set<String> guardedVariables = new HashSet<>();
        for (Constraint guard : alternative.guards()) {
            walkConstraint(guard, expression -> {
                if (expression instanceof Variable(String name)) {
                    guardedVariables.add(name);
                }
            });
        }
        return !guardedVariables.containsAll(witnessVariables);
    }

    private static List<Alternative> toAlternatives(List<TypeSystem.CoercionResult> coercionResults)
    {
        return coercionResults.stream()
                .map(result -> new Alternative(result.type(), result.guards(), List.of(result.plan())))
                .toList();
    }

    private void bindTypeVariable(String variable, Expression expression, SolverState state)
    {
        // A numeric-kind variable (declared by a constructor's RequireKind before any binding
        // arrives, e.g. the length in varchar(n)) binds through its bounds, not as a type:
        // an exact literal pins min and max to the value
        if (state.variableStates().get(variable) instanceof NumericVariableState && expression instanceof Expression.Literal(int value)) {
            NumericPropagator.process(new Expression.BinaryOperation(Expression.BinaryOperator.EQUAL, new Variable(variable), Expression.literal(value)), state);
            return;
        }
        TypeVariableState variableState = requireTypeVariable(variable, state);
        if (variableState.binding().isPresent()) {
            // Another exact argument may bind the same variable. Check equivalence after
            // substitution, including any still-symbolic components, instead of rebinding it.
            Expression bound = Expression.substitute(variableState.binding().orElseThrow(), state.substitutions());
            state.workList().add(new Subtype(bound, expression));
            state.workList().add(new Subtype(expression, bound));
            return;
        }
        variableState.bind(expression);
        state.variableChanged(variable);
        state.forgetDomain(variable);
        enqueueValidationConstraints(expression, state);
    }

    private void enqueueValidationConstraints(Constraint constraint, SolverState state)
    {
        walkConstraint(constraint, expression -> enqueueValidationConstraints(expression, state));
    }

    private void enqueueValidationConstraints(Expression expression, SolverState state)
    {
        if (expression instanceof Expression.Application application) {
            // Dedup on the expression itself (records compare by value) rather than its rendered
            // string, which avoids building a String for every application on the hot path.
            if (state.validatedExpressions().add(application)) {
                state.workList().addAll(typeSystem.instantiateValidationConstraints(application));
            }
        }
    }

    private static int maxVariableId(List<Constraint> constraints)
    {
        int max = 0;
        for (Constraint constraint : constraints) {
            max = Math.max(max, maxVariableId(constraint));
        }
        return max;
    }

    private static int maxVariableId(Constraint constraint)
    {
        return switch (constraint) {
            case ExactType(String variable, Expression type) -> Math.max(VariableAllocator.variableId(variable), maxVariableId(type));
            case NumericRelation(BinaryOperation operation) -> maxVariableId(operation);
            case Subtype(Expression subtype, Expression supertype) -> Math.max(maxVariableId(subtype), maxVariableId(supertype));
            case RequireKind(String variable, Kind _) -> VariableAllocator.variableId(variable);
            case RequireComparable(String variable) -> VariableAllocator.variableId(variable);
            case RequireOrderable(String variable) -> VariableAllocator.variableId(variable);
            case RequireCastableTo(Expression source, Expression target) -> Math.max(maxVariableId(source), maxVariableId(target));
            case RequireCastableFrom(Expression target, Expression source) -> Math.max(maxVariableId(target), maxVariableId(source));
            case Choice(List<Alternative> alternatives) -> {
                int max = 0;
                for (Alternative alternative : alternatives) {
                    max = Math.max(max, maxVariableId(alternative.witness()));
                    for (Constraint guard : alternative.guards()) {
                        max = Math.max(max, maxVariableId(guard));
                    }
                }
                yield max;
            }
        };
    }

    private static int maxVariableId(Expression expression)
    {
        int[] max = {0};
        walkExpression(expression, visited -> {
            if (visited instanceof Variable(String name)) {
                max[0] = Math.max(max[0], VariableAllocator.variableId(name));
            }
        });
        return max[0];
    }

    private static void walkConstraint(Constraint constraint, Consumer<Expression> visitor)
    {
        switch (constraint) {
            case ExactType(String variable, Expression type) -> {
                visitor.accept(new Variable(variable));
                walkExpression(type, visitor);
            }
            case NumericRelation(BinaryOperation operation) -> walkExpression(operation, visitor);
            case Subtype(Expression subtype, Expression supertype) -> {
                walkExpression(subtype, visitor);
                walkExpression(supertype, visitor);
            }
            case RequireKind(String variable, _) -> visitor.accept(new Variable(variable));
            case RequireComparable(String variable) -> visitor.accept(new Variable(variable));
            case RequireOrderable(String variable) -> visitor.accept(new Variable(variable));
            case RequireCastableTo(Expression source, Expression target) -> {
                walkExpression(source, visitor);
                walkExpression(target, visitor);
            }
            case RequireCastableFrom(Expression target, Expression source) -> {
                walkExpression(source, visitor);
                walkExpression(target, visitor);
            }
            case Choice(List<Alternative> alternatives) -> {
                for (Alternative alternative : alternatives) {
                    walkExpression(alternative.witness(), visitor);
                    alternative.guards().forEach(guard -> walkConstraint(guard, visitor));
                }
            }
        }
    }

    private static void walkExpression(Expression expression, Consumer<Expression> visitor)
    {
        if (expression instanceof Variable || expression instanceof Expression.Symbol || expression instanceof Expression.Literal || expression instanceof Expression.AnyRow) {
            ResolutionBudget.checkDepth(0);
            visitor.accept(expression);
            return;
        }
        Deque<ExpressionVisit> stack = new ArrayDeque<>();
        stack.push(new ExpressionVisit(expression, 0));
        while (!stack.isEmpty()) {
            ExpressionVisit visit = stack.pop();
            ResolutionBudget.checkDepth(visit.depth());
            visitor.accept(visit.expression());
            int depth = visit.depth() + 1;
            Consumer<Expression> push = child -> stack.push(new ExpressionVisit(child, depth));
            switch (visit.expression()) {
                case Expression.Application(Expression head, List<Expression> arguments) -> {
                    arguments.reversed().forEach(push);
                    push.accept(head);
                }
                case Expression.Row(List<Expression.RowField> fields) -> fields.reversed().forEach(field -> push.accept(field.type()));
                case BinaryOperation(_, Expression left, Expression right) -> {
                    push.accept(right);
                    push.accept(left);
                }
                case Expression.Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> {
                    push.accept(ifFalse);
                    push.accept(ifTrue);
                    push.accept(condition);
                }
                case Expression.FunctionType functionType -> {
                    push.accept(functionType.returnType());
                    functionType.variadicParameterType().ifPresent(push);
                    functionType.parameterTypes().reversed().forEach(push);
                }
                default -> {}
            }
        }
    }

    private record ExpressionVisit(Expression expression, int depth) {}

    static void validateExpression(Expression expression)
    {
        walkExpression(expression, _ -> {});
    }

    /// Handle a `Subtype(left, right)` where neither side is a lone variable.
    ///
    /// Four cases in order:
    ///
    /// 1. Identical types — discharged trivially.
    /// 2. A concrete row flowing into [Expression.AnyRow] — satisfied by family check.
    /// 3. Two rows — structurally decomposed into field-wise [Subtype] constraints.
    /// 4. Otherwise scan the registered coercion rules. Zero matches on ground types is a
    ///    contradiction; zero matches on non-ground types is parked pending; one match is
    ///    discharged; multiple matches become a [Choice].
    private void processConcreteSubtype(Expression subtype, Expression supertype, VariableAllocator allocator, SolverState state)
    {
        if (subtype.equals(supertype)) {
            return;
        }

        if (subtype instanceof Expression.Row && supertype instanceof Expression.AnyRow) {
            return;
        }

        if (subtype instanceof Expression.Row(List<Expression.RowField> leftFields) &&
                supertype instanceof Expression.Row(List<Expression.RowField> rightFields)) {
            if (leftFields.size() != rightFields.size()) {
                throw new UnsatisfiableException("Unsatisfiable: row arity mismatch");
            }
            // Row is invariant on field types in this model (positional, no width subtyping).
            for (int index = 0; index < leftFields.size(); index++) {
                state.workList().add(new Subtype(leftFields.get(index).type(), rightFields.get(index).type()));
            }
            return;
        }

        List<CoercionRule.Match> matches = typeSystem.candidateCoercions(subtype, supertype).stream()
                .map(coercion -> coercion.matches(allocator, subtype, supertype))
                .flatMap(Optional::stream)
                .toList();

        if (matches.isEmpty()) {
            // A ground subtype query that matches no rule is definitively false.
            // An under-bound one is parked — resolving the other side later might enable a rule.
            if (Expression.isGround(subtype) && Expression.isGround(supertype)) {
                throw new UnsatisfiableException("Unsatisfiable: no coercion from " + subtype + " to " + supertype);
            }
            state.pendingConstraints().add(new Subtype(subtype, supertype));
            return;
        }

        if (matches.size() == 1) {
            state.workList().addAll(matches.getFirst().constraints());
            return;
        }

        state.pendingConstraints().add(new Choice(matches.stream()
                .map(match -> new Alternative(
                        supertype,
                        match.constraints(),
                        match.plan()
                                .map(List::of)
                                .orElseGet(() -> typeSystem.coercionPlan(subtype, supertype)
                                        .map(List::of)
                                        .orElse(List.of()))))
                .toList()));
    }

    private void processChoice(Choice choice, SolverState state)
    {
        List<Alternative> feasible = simplifyGuards(choice.alternatives(), state);
        feasible = choiceSimplifier.prune(feasible);

        if (feasible.isEmpty()) {
            throw new UnsatisfiableException("Unsatisfiable choice: " + choice);
        }
        if (feasible.size() == 1) {
            commitChoiceAlternative(feasible.getFirst(), state);
            return;
        }
        state.pendingConstraints().add(new Choice(feasible));
    }

    private void restrictToRow(String variable, TypeVariableState variableState, SolverState state)
    {
        Optional<Alternative> previousForced = variableState.domain().forced();
        variableState.restrictToRow();
        state.variableChanged(variable);
        if (variableState.domain().isRestricted()) {
            List<Alternative> filtered = domainRefiner.applyRowRestriction(variableState, variableState.domain().alternatives());
            if (filtered.isEmpty()) {
                throw new UnsatisfiableException("Unsatisfiable domain for variable " + variable);
            }
            variableState.domain().replace(filtered);
            state.watchDomain(variable, filtered);
        }

        Optional<Alternative> forced = variableState.domain().forced();
        if (forced.isPresent() && !forced.equals(previousForced)) {
            bindForcedAlternative(variable, variableState, forced.orElseThrow(), state);
        }
    }

    private void constrainDomain(String variable, TypeVariableState variableState, List<Alternative> candidates, SolverState state)
    {
        if (candidates.isEmpty()) {
            throw new UnsatisfiableException("Unsatisfiable domain for variable " + variable);
        }

        candidates = domainRefiner.applyRowRestriction(variableState, candidates);
        if (candidates.isEmpty()) {
            throw new UnsatisfiableException("Unsatisfiable domain for variable " + variable);
        }

        Optional<Alternative> previousForced = variableState.domain().forced();
        variableState.domain().constrain(candidates);
        state.watchDomain(variable, variableState.domain().alternatives());

        if (variableState.domain().isEmpty()) {
            throw new UnsatisfiableException("Unsatisfiable domain for variable " + variable);
        }

        Optional<Alternative> forced = variableState.domain().forced();
        if (forced.isPresent() && !forced.equals(previousForced)) {
            bindForcedAlternative(variable, variableState, forced.orElseThrow(), state);
        }
    }

    private void reconcileState(SolverState state)
    {
        do {
            if (state.workList().isEmpty()) {
                refineRestrictedDomains(state);
            }
            revisitPendingConstraints(state);
        }
        while (state.hasReconciliationWork());
    }

    private void refineRestrictedDomains(SolverState state)
    {
        while (state.workList().isEmpty()) {
            String variable = state.pollDirtyDomain();
            if (variable == null) {
                return;
            }
            ResolutionBudget.consume();
            if (!(state.variableStates().get(variable) instanceof TypeVariableState typeVariableState) || !typeVariableState.domain().isRestricted()) {
                continue;
            }

            List<Alternative> feasible = simplifyGuards(typeVariableState.domain().alternatives(), state);
            feasible = domainRefiner.applyRowRestriction(typeVariableState, feasible);
            feasible = domainRefiner.pruneDominatedAlternatives(typeVariableState, feasible);
            if (feasible.isEmpty()) {
                throw new UnsatisfiableException("Unsatisfiable domain for variable " + variable);
            }
            boolean changed = typeVariableState.domain().replace(feasible);
            Optional<Alternative> forced = typeVariableState.domain().forced();
            if (forced.isPresent() && typeVariableState.binding().isEmpty()) {
                bindForcedAlternative(variable, typeVariableState, forced.orElseThrow(), state);
            }
            else if (changed) {
                state.watchDomain(variable, feasible);
            }
        }
    }

    private void revisitPendingConstraints(SolverState state)
    {
        for (Constraint pendingConstraint : state.pendingConstraints().takeReady()) {
            ResolutionBudget.consume();
            Constraint substituted = substitute(pendingConstraint, state.substitutions());
            switch (substituted) {
                case Choice choice -> {
                    List<Alternative> feasible = simplifyGuards(choice.alternatives(), state);
                    feasible = choiceSimplifier.prune(feasible);
                    if (feasible.isEmpty()) {
                        throw new UnsatisfiableException("Unsatisfiable choice: " + choice);
                    }
                    if (feasible.size() == 1) {
                        commitChoiceAlternative(feasible.getFirst(), state);
                    }
                    else {
                        state.pendingConstraints().park(new Choice(feasible));
                    }
                }
                case NumericRelation(BinaryOperation operation) -> {
                    // Substitution may turn a relation between variables into a direct
                    // literal bound. Run the full propagator so it can bind that variable
                    // and wake its dependents, rather than leaving the chain to materialization.
                    NumericPropagator.process(operation, state);
                }
                case RequireComparable(String variable) -> {
                    // Once the variable resolves to a ground type, the trait is decidable: discharge
                    // it if satisfied, or fail the whole system if the concrete type isn't comparable.
                    if (!dischargeTrait(variable, "comparable", state, typeSystem::isComparable)) {
                        state.pendingConstraints().park(substituted);
                    }
                }
                case RequireOrderable(String variable) -> {
                    if (!dischargeTrait(variable, "orderable", state, typeSystem::isOrderable)) {
                        state.pendingConstraints().park(substituted);
                    }
                }
                case Subtype _, ExactType _, RequireKind _, RequireCastableTo _, RequireCastableFrom _ -> {
                    if (!substituted.equals(pendingConstraint)) {
                        state.workList().add(substituted);
                    }
                    else {
                        state.pendingConstraints().park(substituted);
                    }
                }
            }
        }
    }

    private void bindForcedAlternative(String variable, TypeVariableState typeVariableState, Alternative alternative, SolverState state)
    {
        if (typeVariableState.binding().isPresent()) {
            return;
        }
        bindTypeVariable(variable, alternative.witness(), state);
        state.workList().addAll(alternative.guards());
    }

    private void commitChoiceAlternative(Alternative alternative, SolverState state)
    {
        state.workList().addAll(alternative.guards());
        enqueueValidationConstraints(alternative.witness(), state);
    }

    /// Remove contradictory alternatives and discharge ground guards proved true. Keeping
    /// tautologies in a guard set prevents otherwise-equivalent alternatives from being
    /// compared by dominance. Guards involving open parameters remain attached to their
    /// alternative, even when current numeric bounds happen to prove them true.
    private List<Alternative> simplifyGuards(List<Alternative> alternatives, SolverState state)
    {
        List<Alternative> feasible = new ArrayList<>();
        boolean simplified = false;
        for (Alternative alternative : alternatives) {
            if (alternative.guards().isEmpty()) {
                feasible.add(alternative);
                continue;
            }
            Set<Constraint> remaining = new LinkedHashSet<>();
            boolean impossible = false;
            for (Constraint guard : alternative.guards()) {
                ResolutionBudget.consume();
                Constraint substituted = guard.apply(state.substitutions());
                if (substituted instanceof NumericRelation(BinaryOperation operation)) {
                    Optional<Boolean> satisfied = NumericPropagator.evaluate(operation, state);
                    if (satisfied.equals(Optional.of(false))) {
                        impossible = true;
                        break;
                    }
                    if (satisfied.equals(Optional.of(true)) && Expression.isGround(operation)) {
                        continue;
                    }
                }
                if (substituted instanceof Subtype(Expression left, Expression right) &&
                        Expression.isGround(left) && Expression.isGround(right)) {
                    SubtypeOracle.Relation relation = subtypeOracle.classify(left, right);
                    if (relation == SubtypeOracle.Relation.UNSATISFIED) {
                        impossible = true;
                        break;
                    }
                    if (relation == SubtypeOracle.Relation.SATISFIED) {
                        continue;
                    }
                }
                remaining.add(guard);
            }
            if (!impossible) {
                if (remaining.size() == alternative.guards().size()) {
                    feasible.add(alternative);
                }
                else {
                    simplified = true;
                    feasible.add(new Alternative(alternative.witness(), remaining, alternative.coercionPlans()));
                }
            }
        }
        // Discharging different tautologies can make two alternatives identical. Merge
        // their proof plans before dominance so neither eliminates the other.
        return simplified ? Alternative.normalize(feasible) : List.copyOf(feasible);
    }

    /// Decide a pending trait obligation on `variable`. Returns `true` if the
    /// variable is bound to a ground type that satisfies `trait` (the obligation is
    /// discharged); `false` if the binding is still unresolved (keep it pending); and
    /// throws [UnsatisfiableException] if the ground type definitively violates the trait.
    private static boolean dischargeTrait(String variable, String traitName, SolverState state, Predicate<Expression> trait)
    {
        Map<String, Expression> substitutions = state.substitutions();
        Expression resolved = substitutions.get(variable);
        if (resolved == null) {
            return false;
        }
        resolved = Expression.substitute(resolved, substitutions);
        if (!Expression.isGround(resolved)) {
            return false;
        }
        if (!trait.test(resolved)) {
            throw new UnsatisfiableException(resolved + " (bound to " + variable + ") is not " + traitName);
        }
        return true;
    }

    public sealed interface SolveOutcome
            permits Incomplete,
                    Satisfied,
                    Unsatisfied {}

    public record Satisfied(Result result)
            implements SolveOutcome {}

    public record Unsatisfied(String message)
            implements SolveOutcome {}

    public record Incomplete(Result result)
            implements SolveOutcome {}

    static class SolverState
    {
        private final Queue<Constraint> workList = new WorkList();
        private final Map<String, VariableState> variableStates = new HashMap<>();
        private final PendingConstraints pendingConstraints = new PendingConstraints();
        private final Set<Expression> validatedExpressions = new HashSet<>();
        private final Map<String, Expression> substitutions = new HashMap<>();
        private final Map<String, Set<String>> domainDependencies = new HashMap<>();
        private final Map<String, Set<String>> domainDependents = new HashMap<>();
        private final Set<String> dirtyDomains = new LinkedHashSet<>();
        private final Set<String> registeredBindings = new HashSet<>();
        private final Map<String, Set<String>> bindingDependents = new HashMap<>();

        public Map<String, Expression> substitutions()
        {
            return substitutions;
        }

        public void putVariableState(String variable, VariableState value)
        {
            if (!value.equals(variableStates.put(variable, value))) {
                variableChanged(variable);
            }
        }

        public void variableChanged(String variable)
        {
            switch (variableStates.get(variable)) {
                case TypeVariableState typeState -> typeState.binding().ifPresent(type -> {
                    substitutions.put(variable, type);
                    if (registeredBindings.add(variable)) {
                        Set<String> dependencies = new HashSet<>();
                        walkExpression(type, expression -> addVariable(expression, dependencies));
                        for (String dependency : dependencies) {
                            bindingDependents.computeIfAbsent(dependency, _ -> new LinkedHashSet<>()).add(variable);
                        }
                    }
                });
                case NumericVariableState(OptionalInt min, OptionalInt max) -> {
                    if (min.isPresent() && min.equals(max)) {
                        substitutions.put(variable, Expression.literal(min.orElseThrow()));
                    }
                }
            }
            Deque<String> changed = new ArrayDeque<>();
            Set<String> visited = new HashSet<>();
            changed.add(variable);
            while (!changed.isEmpty()) {
                ResolutionBudget.consume();
                String name = changed.removeFirst();
                if (!visited.add(name)) {
                    continue;
                }
                pendingConstraints.variableChanged(name);
                Set<String> dependents = domainDependents.getOrDefault(name, Set.of());
                dirtyDomains.addAll(dependents);
                changed.addAll(bindingDependents.getOrDefault(name, Set.of()));
            }
        }

        public void watchDomain(String variable, List<Alternative> alternatives)
        {
            forgetDomain(variable);
            if (variableStates.get(variable) instanceof TypeVariableState state && state.binding().isPresent()) {
                return;
            }
            Set<String> dependencies = new HashSet<>();
            dependencies.add(variable);
            for (Alternative alternative : alternatives) {
                walkExpression(alternative.witness(), expression -> addVariable(expression, dependencies));
                for (Constraint guard : alternative.guards()) {
                    walkConstraint(guard, expression -> addVariable(expression, dependencies));
                }
            }
            domainDependencies.put(variable, dependencies);
            for (String dependency : dependencies) {
                domainDependents.computeIfAbsent(dependency, _ -> new LinkedHashSet<>()).add(variable);
            }
            dirtyDomains.add(variable);
        }

        public void forgetDomain(String variable)
        {
            for (String dependency : domainDependencies.getOrDefault(variable, Set.of())) {
                domainDependents.get(dependency).remove(variable);
            }
            domainDependencies.remove(variable);
            dirtyDomains.remove(variable);
        }

        public String pollDirtyDomain()
        {
            var iterator = dirtyDomains.iterator();
            if (!iterator.hasNext()) {
                return null;
            }
            String result = iterator.next();
            iterator.remove();
            return result;
        }

        public boolean hasReconciliationWork()
        {
            return !pendingConstraints.ready.isEmpty() || (workList.isEmpty() && !dirtyDomains.isEmpty());
        }

        public Queue<Constraint> workList()
        {
            return workList;
        }

        public Map<String, VariableState> variableStates()
        {
            return variableStates;
        }

        public PendingConstraints pendingConstraints()
        {
            return pendingConstraints;
        }

        public Set<Expression> validatedExpressions()
        {
            return validatedExpressions;
        }
    }

    private static final class WorkList
            extends java.util.AbstractQueue<Constraint>
    {
        private final Queue<Constraint> queue = new ArrayDeque<>();
        private final Set<Constraint> queued = new HashSet<>();

        @Override
        public boolean offer(Constraint constraint)
        {
            if (queued.add(constraint)) {
                ResolutionBudget.consume();
                queue.add(constraint);
            }
            return true;
        }

        @Override
        public Constraint poll()
        {
            Constraint constraint = queue.poll();
            queued.remove(constraint);
            return constraint;
        }

        @Override
        public Constraint peek()
        {
            return queue.peek();
        }

        @Override
        public java.util.Iterator<Constraint> iterator()
        {
            return queue.iterator();
        }

        @Override
        public int size()
        {
            return queue.size();
        }
    }

    private static void addVariable(Expression expression, Set<String> variables)
    {
        if (expression instanceof Variable(String name)) {
            variables.add(name);
        }
    }

    static Set<String> variables(Constraint constraint)
    {
        Set<String> variables = new LinkedHashSet<>();
        walkConstraint(constraint, expression -> addVariable(expression, variables));
        return variables;
    }

    static Set<String> variables(Expression expression)
    {
        Set<String> variables = new LinkedHashSet<>();
        walkExpression(expression, node -> addVariable(node, variables));
        return variables;
    }

    /// Pending constraints subscribe to the variables they read. A variable update wakes
    /// only those constraints; unchanged obligations remain parked without being rescanned.
    static final class PendingConstraints
            extends java.util.AbstractCollection<Constraint>
    {
        private final Map<Constraint, Set<String>> dependencies = new HashMap<>();
        private final Map<String, Set<Constraint>> dependents = new HashMap<>();
        private final Set<Constraint> ready = new LinkedHashSet<>();

        @Override
        public boolean add(Constraint constraint)
        {
            boolean added = park(constraint);
            ready.add(constraint);
            return added;
        }

        public boolean park(Constraint constraint)
        {
            if (dependencies.containsKey(constraint)) {
                return false;
            }
            Set<String> variables = new HashSet<>();
            walkConstraint(constraint, expression -> addVariable(expression, variables));
            dependencies.put(constraint, variables);
            for (String variable : variables) {
                dependents.computeIfAbsent(variable, _ -> new LinkedHashSet<>()).add(constraint);
            }
            return true;
        }

        public void variableChanged(String variable)
        {
            ready.addAll(dependents.getOrDefault(variable, Set.of()));
        }

        public List<Constraint> takeReady()
        {
            List<Constraint> result = List.copyOf(ready);
            ready.clear();
            for (Constraint constraint : result) {
                for (String variable : dependencies.remove(constraint)) {
                    dependents.get(variable).remove(constraint);
                }
            }
            return result;
        }

        @Override
        public java.util.Iterator<Constraint> iterator()
        {
            return dependencies.keySet().iterator();
        }

        @Override
        public int size()
        {
            return dependencies.size();
        }
    }
}
