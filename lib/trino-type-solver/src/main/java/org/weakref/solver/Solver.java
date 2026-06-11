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

import org.weakref.solver.Expression.BinaryOperation;
import org.weakref.solver.Expression.Variable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Work-list constraint solver.
 * <p>
 * Operates in two phases:
 * <ol>
 *   <li><b>Worklist</b> — pop constraints one at a time; each case arm in
 *       {@link #solve} either discharges the constraint, adds more constraints to the
 *       worklist, parks it in pending, or throws {@link UnsatisfiableException}. Numeric
 *       relations delegate to {@link NumericPropagator}; numeric-related worklist items
 *       can enter from {@link RequireKind} handling or from parametric-type validation
 *       constraints injected by the type system.</li>
 *   <li><b>Reconcile</b> — once the worklist is empty, iteratively refine restricted
 *       {@link Domain}s and revisit pending constraints. Each pass may reintroduce
 *       newly-activated constraints to the worklist (e.g. a choice that collapsed to a
 *       single alternative), which restarts the outer loop.</li>
 * </ol>
 * {@link #solve} returns a raw {@link Result}; {@link #solveOutcome} is a higher-level
 * wrapper that distinguishes satisfied vs incomplete vs unsatisfied (for callers that
 * want success/failure rather than exceptions).
 */
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

    /**
     * Solve the constraint system, returning a raw {@link Result}. Throws
     * {@link UnsatisfiableException} on contradiction.
     */
    public Result solve(List<Constraint> constraints)
    {
        VariableAllocator allocator = new VariableAllocator();
        // The incoming constraints carry variables minted by the caller's allocator, which
        // also started at @v1. Seed ours past them so the fresh names we mint while solving
        // (e.g. instantiating coercion rules) can't collide with an input variable and get
        // conflated with it during unification.
        allocator.reserveThrough(maxVariableId(constraints));
        SolverState state = new SolverState();

        state.workList().addAll(constraints);
        while (!state.workList().isEmpty()) {
            // Substitute the freshest variable bindings before dispatching, so each arm
            // sees the most-resolved form (e.g. after a sibling Subtype bound a var).
            Constraint candidate = substitute(state.workList().poll(), state.variableStates());
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
                }
                case Subtype(Variable(String variable), Expression type) -> {
                    TypeVariableState variableState = requireTypeVariable(variable, state);
                    if (type instanceof Expression.AnyRow) {
                        restrictToRow(variable, variableState, state);
                    }
                    else {
                        variableState.addUpperBound(type);
                        constrainDomain(variable, variableState, toAlternatives(typeSystem.coercionsTo(type, allocator)), state);
                    }
                }
                case Subtype(Expression type, Variable(String variable)) -> {
                    TypeVariableState variableState = requireTypeVariable(variable, state);
                    if (type instanceof Expression.AnyRow) {
                        restrictToRow(variable, variableState, state);
                    }
                    else {
                        variableState.addLowerBound(type);
                        constrainDomain(variable, variableState, toAlternatives(typeSystem.coercionsFrom(type, allocator)), state);
                    }
                }
                case Subtype(Expression subtype, Expression supertype) -> processConcreteSubtype(subtype, supertype, allocator, state);
                case NumericRelation(BinaryOperation operation) -> NumericPropagator.process(operation, state);
                case Choice choice -> processChoice(choice, state);
            }
            reconcileState(state);
        }
        reconcileState(state);
        return new Result(typeSystem, state.variableStates(), state.pendingConstraints().stream().toList());
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
            state.variableStates().put(variable, variableState);
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

    private Constraint substitute(Constraint constraint, Map<String, VariableState> variableStates)
    {
        Map<String, Expression> substitutions = currentSubstitutions(variableStates);
        if (substitutions.isEmpty()) {
            return constraint;
        }
        return constraint.apply(substitutions);
    }

    public record Result(TypeSystem typeSystem, Map<String, VariableState> variableBounds, List<Constraint> nextBatch)
    {
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
            Map<String, Expression> substitutions = new HashMap<>(materializedTypeVariables());
            materializedNumericValues().forEach((name, value) -> substitutions.put(name, Expression.literal(value)));
            for (Constraint constraint : nextBatch) {
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

        private Materialization materialize()
        {
            SolverMaterializer.Materialization materialization = new SolverMaterializer(typeSystem, variableBounds, nextBatch)
                    .materialize();
            return new Materialization(materialization.numericValues(), materialization.typeValues(), materialization.selectedAlternatives());
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
                    yield switch (operator) {
                        case ADD -> OptionalInt.of(leftValue.orElseThrow() + rightValue.orElseThrow());
                        case SUBTRACT -> OptionalInt.of(leftValue.orElseThrow() - rightValue.orElseThrow());
                        case MULTIPLY -> OptionalInt.of(leftValue.orElseThrow() * rightValue.orElseThrow());
                        case DIVIDE -> rightValue.orElseThrow() == 0 ? OptionalInt.empty() : OptionalInt.of(leftValue.orElseThrow() / rightValue.orElseThrow());
                        case MIN -> OptionalInt.of(Math.min(leftValue.orElseThrow(), rightValue.orElseThrow()));
                        case MAX -> OptionalInt.of(Math.max(leftValue.orElseThrow(), rightValue.orElseThrow()));
                        default -> OptionalInt.empty();
                    };
                }
                default -> OptionalInt.empty();
            };
        }
    }

    private static List<Alternative> toAlternatives(List<TypeSystem.CoercionResult> coercionResults)
    {
        return coercionResults.stream()
                .map(result -> new Alternative(result.type(), result.guards(), List.of(result.plan())))
                .toList();
    }

    private void bindTypeVariable(String variable, Expression expression, SolverState state)
    {
        requireTypeVariable(variable, state).bind(expression);
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

    private static void walkConstraint(Constraint constraint, java.util.function.Consumer<Expression> visitor)
    {
        switch (constraint) {
            case ExactType(_, Expression type) -> walkExpression(type, visitor);
            case NumericRelation(BinaryOperation operation) -> walkExpression(operation, visitor);
            case Subtype(Expression subtype, Expression supertype) -> {
                walkExpression(subtype, visitor);
                walkExpression(supertype, visitor);
            }
            default -> {}
        }
    }

    private static void walkExpression(Expression expression, java.util.function.Consumer<Expression> visitor)
    {
        visitor.accept(expression);
        switch (expression) {
            case Expression.Application(Expression head, List<Expression> arguments) -> {
                walkExpression(head, visitor);
                arguments.forEach(argument -> walkExpression(argument, visitor));
            }
            case Expression.Row(List<Expression.RowField> fields) -> fields.forEach(field -> walkExpression(field.type(), visitor));
            case Expression.AnyRow _ -> {}
            case BinaryOperation(_, Expression left, Expression right) -> {
                walkExpression(left, visitor);
                walkExpression(right, visitor);
            }
            case Expression.Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> {
                walkExpression(condition, visitor);
                walkExpression(ifTrue, visitor);
                walkExpression(ifFalse, visitor);
            }
            case Expression.FunctionType functionType -> {
                functionType.parameterTypes().forEach(parameter -> walkExpression(parameter, visitor));
                functionType.variadicParameterType().ifPresent(parameter -> walkExpression(parameter, visitor));
                walkExpression(functionType.returnType(), visitor);
            }
            default -> {}
        }
    }

    /**
     * Handle a {@code Subtype(left, right)} where neither side is a lone variable.
     * <p>
     * Four cases in order:
     * <ol>
     *   <li>Identical types — discharged trivially.</li>
     *   <li>A concrete row flowing into {@link Expression.AnyRow} — satisfied by family check.</li>
     *   <li>Two rows — structurally decomposed into field-wise {@link Subtype} constraints.</li>
     *   <li>Otherwise scan the registered coercion rules. Zero matches on ground types is a
     *       contradiction; zero matches on non-ground types is parked pending; one match is
     *       discharged; multiple matches become a {@link Choice}.</li>
     * </ol>
     */
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
        List<Alternative> feasible = choice.alternatives().stream()
                .filter(alternative -> guardsStatus(alternative.guards(), state) != Feasibility.INFEASIBLE)
                .toList();
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
        if (variableState.domain().isRestricted()) {
            List<Alternative> filtered = domainRefiner.applyRowRestriction(variableState, variableState.domain().alternatives());
            if (filtered.isEmpty()) {
                throw new UnsatisfiableException("Unsatisfiable domain for variable " + variable);
            }
            variableState.domain().replace(filtered);
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
        boolean changed;
        do {
            changed = refineRestrictedDomains(state);
            changed |= revisitPendingConstraints(state);
        }
        while (changed);
    }

    private boolean refineRestrictedDomains(SolverState state)
    {
        boolean changed = false;
        for (Map.Entry<String, VariableState> entry : state.variableStates().entrySet()) {
            if (!(entry.getValue() instanceof TypeVariableState typeVariableState) || !typeVariableState.domain().isRestricted()) {
                continue;
            }

            List<Alternative> feasible = typeVariableState.domain().alternatives().stream()
                    .filter(alternative -> guardsStatus(alternative.guards(), state) != Feasibility.INFEASIBLE)
                    .toList();
            feasible = domainRefiner.applyRowRestriction(typeVariableState, feasible);
            if (state.workList().isEmpty()) {
                feasible = domainRefiner.pruneDominatedAlternatives(typeVariableState, feasible);
            }
            if (feasible.isEmpty()) {
                throw new UnsatisfiableException("Unsatisfiable domain for variable " + entry.getKey());
            }
            if (typeVariableState.domain().replace(feasible)) {
                changed = true;
            }

            Optional<Alternative> forced = typeVariableState.domain().forced();
            if (forced.isPresent() && typeVariableState.binding().isEmpty()) {
                bindForcedAlternative(entry.getKey(), typeVariableState, forced.orElseThrow(), state);
                changed = true;
            }
        }
        return changed;
    }

    private boolean revisitPendingConstraints(SolverState state)
    {
        boolean changed = false;
        List<Constraint> pending = new ArrayList<>();
        for (Constraint pendingConstraint : state.pendingConstraints()) {
            Constraint substituted = substitute(pendingConstraint, state.variableStates());
            switch (substituted) {
                case Choice choice -> {
                    List<Alternative> feasible = choice.alternatives().stream()
                            .filter(alternative -> guardsStatus(alternative.guards(), state) != Feasibility.INFEASIBLE)
                            .toList();
                    feasible = choiceSimplifier.prune(feasible);
                    if (feasible.isEmpty()) {
                        throw new UnsatisfiableException("Unsatisfiable choice: " + choice);
                    }
                    if (feasible.size() == 1) {
                        commitChoiceAlternative(feasible.getFirst(), state);
                        changed = true;
                    }
                    else {
                        if (!feasible.equals(choice.alternatives())) {
                            changed = true;
                        }
                        pending.add(new Choice(feasible));
                    }
                }
                case NumericRelation(BinaryOperation operation) -> {
                    boolean propagated = NumericPropagator.propagate(operation, state);
                    Optional<Boolean> satisfied = NumericPropagator.evaluate(operation, state);
                    if (satisfied.isPresent()) {
                        if (!satisfied.orElseThrow()) {
                            throw new UnsatisfiableException("Unsatisfiable: " + operation);
                        }
                        changed = true;
                    }
                    else {
                        changed |= propagated;
                        pending.add(substituted);
                    }
                }
                case RequireComparable(String variable) -> {
                    // Once the variable resolves to a ground type, the trait is decidable: discharge
                    // it if satisfied, or fail the whole system if the concrete type isn't comparable.
                    if (dischargeTrait(variable, "comparable", state, typeSystem::isComparable)) {
                        changed = true;
                    }
                    else {
                        pending.add(substituted);
                        changed |= !substituted.equals(pendingConstraint);
                    }
                }
                case RequireOrderable(String variable) -> {
                    if (dischargeTrait(variable, "orderable", state, typeSystem::isOrderable)) {
                        changed = true;
                    }
                    else {
                        pending.add(substituted);
                        changed |= !substituted.equals(pendingConstraint);
                    }
                }
                case Subtype _, ExactType _, RequireKind _, RequireCastableTo _, RequireCastableFrom _ -> {
                    if (!substituted.equals(pendingConstraint)) {
                        state.workList().add(substituted);
                        changed = true;
                    }
                    else {
                        pending.add(substituted);
                    }
                }
            }
        }
        state.pendingConstraints().clear();
        state.pendingConstraints().addAll(pending);
        return changed;
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

    private static Feasibility guardsStatus(Set<Constraint> guards, SolverState state)
    {
        boolean unknown = false;
        for (Constraint guard : guards) {
            Constraint substituted = guard.apply(currentSubstitutions(state.variableStates()));
            if (substituted instanceof NumericRelation(BinaryOperation operation)) {
                Optional<Boolean> satisfied = NumericPropagator.evaluate(operation, state);
                if (satisfied.isPresent() && !satisfied.orElseThrow()) {
                    return Feasibility.INFEASIBLE;
                }
                if (satisfied.isEmpty()) {
                    unknown = true;
                }
            }
        }
        return unknown ? Feasibility.UNKNOWN : Feasibility.FEASIBLE;
    }

    /**
     * Decide a pending trait obligation on {@code variable}. Returns {@code true} if the
     * variable is bound to a ground type that satisfies {@code trait} (the obligation is
     * discharged); {@code false} if the binding is still unresolved (keep it pending); and
     * throws {@link UnsatisfiableException} if the ground type definitively violates the trait.
     */
    private static boolean dischargeTrait(String variable, String traitName, SolverState state, Predicate<Expression> trait)
    {
        Map<String, Expression> substitutions = currentSubstitutions(state.variableStates());
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

    private static Map<String, Expression> currentSubstitutions(Map<String, VariableState> variableStates)
    {
        Map<String, Expression> substitutions = new HashMap<>();
        variableStates.forEach((name, state) -> {
            if (state instanceof TypeVariableState typeState && typeState.binding().isPresent()) {
                substitutions.put(name, typeState.binding().orElseThrow());
            }
            if (state instanceof NumericVariableState(OptionalInt min, OptionalInt max) &&
                    min.isPresent() &&
                    max.isPresent() &&
                    min.orElseThrow() == max.orElseThrow()) {
                substitutions.put(name, Expression.literal(min.orElseThrow()));
            }
        });
        return substitutions;
    }

    private enum Feasibility
    {
        FEASIBLE,
        UNKNOWN,
        INFEASIBLE,
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
        private final Deque<Constraint> workList = new ArrayDeque<>();
        private final Map<String, VariableState> variableStates = new HashMap<>();
        private final List<Constraint> pendingConstraints = new ArrayList<>();
        private final Set<Expression> validatedExpressions = new HashSet<>();

        public Deque<Constraint> workList()
        {
            return workList;
        }

        public Map<String, VariableState> variableStates()
        {
            return variableStates;
        }

        public List<Constraint> pendingConstraints()
        {
            return pendingConstraints;
        }

        public Set<Expression> validatedExpressions()
        {
            return validatedExpressions;
        }
    }
}
