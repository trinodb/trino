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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.variable;

/**
 * A bounded polymorphic function signature: quantified variables + side constraints + a type.
 * <p>
 * Example — {@code concat} over any orderable {@code T}:
 * <pre>
 *   parameters  = [@T]
 *   constraints = [RequireOrderable(@T)]
 *   type        = function(@T, @T) -> @T
 * </pre>
 * Matching a call against a scheme ({@link #matchFunctionCallOutcome}) is:
 * <ol>
 *   <li>Allocate fresh variables and {@linkplain #instantiate substitute}
 *       them for the scheme's parameters.</li>
 *   <li>Add one {@link Subtype} constraint per argument position (plus repeated
 *       variadic positions).</li>
 *   <li>Also add the constraints from parameter-type {@linkplain TypeConstructor
 *       constructor validation} (e.g. {@code decimal(p, s)} implies {@code p ≥ s}).</li>
 *   <li>Solve.</li>
 *   <li>If satisfied, materialize the return type and per-argument coercion plans.</li>
 * </ol>
 * The outcome is one of {@link Satisfied} (complete binding + return type),
 * {@link Incomplete} (no contradiction, but unresolved variables remain), or
 * {@link Unsatisfied} (this overload cannot match).
 */
public record TypeScheme(List<Variable> parameters, List<Constraint> constraints, Expression type)
{
    public TypeScheme
    {
        parameters = List.copyOf(parameters);
        constraints = List.copyOf(constraints);
    }

    public Instantiation instantiate(VariableAllocator allocator)
    {
        Map<String, Expression> substitutions = new LinkedHashMap<>();
        for (Variable parameter : parameters) {
            substitutions.put(parameter.name(), variable(allocator.newVariable()));
        }

        return new Instantiation(
                Expression.substitute(type, substitutions),
                constraints.stream()
                        .map(constraint -> constraint.apply(substitutions))
                        .toList(),
                Map.copyOf(substitutions));
    }

    /**
     * Match a call against this scheme. See class-level doc for the five-step flow.
     */
    public MatchOutcome matchFunctionCallOutcome(List<Expression> arguments, TypeSystem typeSystem)
    {
        // Step 1: freshen all quantified variables so different simultaneous matches
        // against the same scheme don't alias each other.
        Instantiation instantiation = instantiate(new VariableAllocator());
        if (!(instantiation.type() instanceof FunctionType functionType)) {
            throw new IllegalStateException("Expected function type but got: " + instantiation.type());
        }

        List<Constraint> callConstraints = new ArrayList<>(instantiation.constraints());
        if (!matchesArity(functionType, arguments.size())) {
            return new Unsatisfied("Arity mismatch");
        }

        // Step 2: add Subtype constraints for each argument position. This is the only
        // place call-level binding obligations enter the system.
        int fixedParameterCount = functionType.parameterTypes().size();
        for (int index = 0; index < fixedParameterCount; index++) {
            addBindingConstraints(arguments.get(index), functionType.parameterTypes().get(index), typeSystem, callConstraints);
        }
        if (functionType.isVariadic()) {
            Expression variadicType = functionType.variadicParameterType().orElseThrow();
            for (int index = fixedParameterCount; index < arguments.size(); index++) {
                addBindingConstraints(arguments.get(index), variadicType, typeSystem, callConstraints);
            }
        }
        // Step 3: kick constructor validation constraints on the return type as well, so
        // that e.g. decimal(p, s) in the return type still enforces its own invariants.
        collectValidationConstraints(functionType.returnType(), typeSystem, callConstraints);

        // Step 4: solve.
        Solver.SolveOutcome outcome = new Solver(typeSystem).solveOutcome(callConstraints);
        if (outcome instanceof Solver.Unsatisfied unsatisfied) {
            return new Unsatisfied(unsatisfied.message());
        }
        if (outcome instanceof Solver.Incomplete incomplete) {
            return new Incomplete(incomplete.result());
        }
        Solver.Result result = ((Solver.Satisfied) outcome).result();

        // Step 5: materialize — build a substitution from the solver's bindings (both type
        // variables and numeric values) so we can evaluate the scheme's return type, each
        // parameter's resolved type, and compute per-argument coercion plans.
        Map<String, Expression> substitutions = new LinkedHashMap<>(result.materializedTypeVariables());
        result.materializedNumericValues().forEach((name, value) -> substitutions.put(name, literal(value)));

        List<Expression> resolvedParameterTypes = new ArrayList<>();
        List<Expression> parameterTemplates = new ArrayList<>(functionType.parameterTypes());
        for (Expression parameterType : functionType.parameterTypes()) {
            resolvedParameterTypes.add(Expression.evaluate(Expression.substitute(parameterType, substitutions)));
        }
        if (functionType.isVariadic()) {
            Expression variadicType = Expression.evaluate(Expression.substitute(functionType.variadicParameterType().orElseThrow(), substitutions));
            Expression variadicTemplate = functionType.variadicParameterType().orElseThrow();
            for (int index = functionType.parameterTypes().size(); index < arguments.size(); index++) {
                resolvedParameterTypes.add(variadicType);
                parameterTemplates.add(variadicTemplate);
            }
        }

        // Per-argument coercion plans are NOT built here: they are only needed for the candidate that
        // ultimately wins overload resolution, and building them for every satisfied candidate (then
        // discarding the losers') was a large fraction of resolution cost. The parameter templates and
        // resolved types are carried so the resolver can build plans for the winner via
        // {@link #coercionPlanFor}.
        return new Satisfied(new MatchResult(
                Expression.evaluate(Expression.substitute(functionType.returnType(), substitutions)),
                List.copyOf(resolvedParameterTypes),
                List.copyOf(parameterTemplates),
                translateTypeBindings(parameters, instantiation.substitutions(), result.materializedTypeVariables()),
                translateNumericBindings(parameters, instantiation.substitutions(), result.materializedNumericValues()),
                result));
    }

    /**
     * Build the coercion plan converting {@code actual} into {@code formal} at one argument position,
     * reusing the solver result where the parameter {@code template} pins a structural shape and
     * falling back to the type system's plan otherwise. Static so the resolver can defer this to the
     * winning candidate.
     */
    public static CoercionPlan coercionPlanFor(Expression actual, Expression template, Expression formal, Solver.Result result, TypeSystem typeSystem)
    {
        return coercionPlan(actual, template, formal, result)
                .or(() -> typeSystem.coercionPlan(actual, formal))
                .orElseThrow(() -> new IllegalStateException("Expected coercion plan for " + actual + " <: " + formal));
    }

    /**
     * Whether an actual argument needs a non-trivial conversion to match the formal parameter —
     * i.e. whether {@link #coercionPlanFor} would yield a non-exact plan — computed without building
     * the (potentially expensive) plan, so overload specificity can rank candidates before plans are
     * built. The structural/selection-aware result plan settles it when present; otherwise an
     * unstructured coercion is needed exactly when the types differ (a same-type match is exact).
     */
    public static boolean coercionNeeded(Expression actual, Expression template, Expression formal, Solver.Result result)
    {
        return coercionPlan(actual, template, formal, result)
                .map(plan -> !plan.isExact())
                .orElseGet(() -> !actual.equals(formal));
    }

    private static void addBindingConstraints(Expression actual, Expression formal, TypeSystem typeSystem, List<Constraint> constraints)
    {
        if (actual instanceof Expression.Application(Expression.Symbol(String actualName), List<Expression> actualArgs) &&
                formal instanceof Expression.Application(Expression.Symbol(String formalName), List<Expression> formalArgs) &&
                actualName.equals(formalName) &&
                actualArgs.size() == formalArgs.size()) {
            Optional<TypeConstructor> constructor = typeSystem.findConstructor(formalName, formalArgs.size());
            // Decomposing into per-parameter constraints is what binds the formal's variables, but
            // parameter positions are not independently coercible: varchar(0) flows into a ground
            // varchar(1) formal by whole-type widening, not by a 0 <: 1 relation at the length
            // position. A fully ground formal binds nothing, so leave it to the coercion rules.
            if (constructor.isPresent() && !formalArgs.stream().allMatch(Expression::isGround)) {
                collectValidationConstraints(actual, typeSystem, constraints);
                collectValidationConstraints(formal, typeSystem, constraints);
                Map<String, Kind> kinds = parameterKinds(constructor.orElseThrow());
                List<String> params = constructor.orElseThrow().parameters();
                for (int i = 0; i < actualArgs.size(); i++) {
                    Kind paramKind = i < params.size() ? kinds.getOrDefault(params.get(i), Kind.TYPE) : Kind.TYPE;
                    bindAt(actualArgs.get(i), formalArgs.get(i), paramKind, typeSystem, constraints);
                }
                return;
            }
        }
        if (actual instanceof Expression.Row(List<Expression.RowField> actualFields) &&
                formal instanceof Expression.Row(List<Expression.RowField> formalFields) &&
                actualFields.size() == formalFields.size()) {
            for (int i = 0; i < actualFields.size(); i++) {
                bindAt(actualFields.get(i).type(), formalFields.get(i).type(), Kind.TYPE, typeSystem, constraints);
            }
            return;
        }
        if (actual instanceof Expression.FunctionType actualFunction &&
                formal instanceof FunctionType formalFunction &&
                actualFunction.parameterTypes().size() == formalFunction.parameterTypes().size() &&
                actualFunction.isVariadic() == formalFunction.isVariadic()) {
            for (int i = 0; i < actualFunction.parameterTypes().size(); i++) {
                bindAt(actualFunction.parameterTypes().get(i), formalFunction.parameterTypes().get(i), Kind.TYPE, typeSystem, constraints);
            }
            if (actualFunction.isVariadic() && formalFunction.isVariadic()) {
                bindAt(actualFunction.variadicParameterType().orElseThrow(), formalFunction.variadicParameterType().orElseThrow(), Kind.TYPE, typeSystem, constraints);
            }
            bindAt(actualFunction.returnType(), formalFunction.returnType(), Kind.TYPE, typeSystem, constraints);
            return;
        }
        constraints.add(new Subtype(actual, formal));
    }

    private static void collectValidationConstraints(Expression expression, TypeSystem typeSystem, List<Constraint> constraints)
    {
        switch (expression) {
            case Expression.Application application -> {
                constraints.addAll(typeSystem.instantiateValidationConstraints(application));
                for (Expression argument : application.arguments()) {
                    collectValidationConstraints(argument, typeSystem, constraints);
                }
            }
            case Expression.Row row -> {
                for (Expression.RowField field : row.fields()) {
                    collectValidationConstraints(field.type(), typeSystem, constraints);
                }
            }
            case FunctionType functionType -> {
                for (Expression parameter : functionType.parameterTypes()) {
                    collectValidationConstraints(parameter, typeSystem, constraints);
                }
                functionType.variadicParameterType().ifPresent(parameter -> collectValidationConstraints(parameter, typeSystem, constraints));
                collectValidationConstraints(functionType.returnType(), typeSystem, constraints);
            }
            default -> {}
        }
    }

    private static void bindAt(Expression actual, Expression formal, Kind parentKind, TypeSystem typeSystem, List<Constraint> constraints)
    {
        if (formal instanceof Variable(String _) && parentKind == Kind.NUMBER) {
            constraints.add(new NumericRelation(new Expression.BinaryOperation(Expression.BinaryOperator.EQUAL, formal, actual)));
            return;
        }
        addBindingConstraints(actual, formal, typeSystem, constraints);
    }

    private static Map<String, Kind> parameterKinds(TypeConstructor constructor)
    {
        Map<String, Kind> kinds = new HashMap<>();
        for (String parameter : constructor.parameters()) {
            kinds.put(parameter, Kind.TYPE);
        }
        for (Constraint constraint : constructor.constraints()) {
            if (constraint instanceof RequireKind(String variable, Kind kind)) {
                kinds.put(variable, kind);
            }
        }
        return kinds;
    }

    private static boolean matchesArity(FunctionType functionType, int argumentCount)
    {
        if (!functionType.isVariadic()) {
            return functionType.parameterTypes().size() == argumentCount;
        }
        return argumentCount >= functionType.parameterTypes().size();
    }

    private static Optional<CoercionPlan> coercionPlan(Expression actual, Expression template, Expression formal, Solver.Result result)
    {
        return switch (template) {
            case Variable(String name) -> planFromSelection(actual, formal, result.selectedAlternatives().get(name));
            case Expression.Application(Expression.Symbol(String constructor), List<Expression> templateArguments)
                    when actual instanceof Expression.Application(Expression.Symbol(String actualName), List<Expression> actualChildren) &&
                    formal instanceof Expression.Application(Expression.Symbol(String formalName), List<Expression> formalArguments) &&
                    constructor.equals(actualName) &&
                    constructor.equals(formalName) &&
                    templateArguments.size() == actualChildren.size() &&
                    templateArguments.size() == formalArguments.size() -> structuralPlan(
                    actual,
                    formal,
                    constructor,
                    actualChildren,
                    templateArguments,
                    formalArguments,
                    result);
            case Expression.Row(List<Expression.RowField> templateFields)
                    when actual instanceof Expression.Row(List<Expression.RowField> actualFields) &&
                    formal instanceof Expression.Row(List<Expression.RowField> formalFields) &&
                    templateFields.size() == actualFields.size() &&
                    templateFields.size() == formalFields.size() -> structuralPlan(
                    actual,
                    formal,
                    "row",
                    actualFields.stream().map(Expression.RowField::type).toList(),
                    templateFields.stream().map(Expression.RowField::type).toList(),
                    formalFields.stream().map(Expression.RowField::type).toList(),
                    result);
            case Expression.AnyRow _ when actual instanceof Expression.Row -> Optional.of(CoercionPlan.exact(actual, formal));
            default -> Optional.empty();
        };
    }

    /**
     * Build a coercion plan for a structural type ({@code constructor} applied to children),
     * recursing into each child position. The {@code constructor} name is supplied by the
     * caller from the already-matched head — either a named type application or {@code "row"} —
     * so this method handles every shape {@link #coercionPlan} routes to it.
     */
    private static Optional<CoercionPlan> structuralPlan(
            Expression actual,
            Expression formal,
            String constructor,
            List<Expression> actualChildren,
            List<Expression> templateChildren,
            List<Expression> formalChildren,
            Solver.Result result)
    {
        List<CoercionPlan> children = new ArrayList<>();
        for (int index = 0; index < templateChildren.size(); index++) {
            Optional<CoercionPlan> child = coercionPlan(actualChildren.get(index), templateChildren.get(index), formalChildren.get(index), result);
            if (child.isEmpty()) {
                return Optional.empty();
            }
            if (!child.orElseThrow().isExact()) {
                children.add(child.orElseThrow());
            }
        }
        if (children.isEmpty()) {
            return Optional.of(CoercionPlan.exact(actual, formal));
        }
        return Optional.of(CoercionPlan.derived(actual, formal, List.of(new CoercionPlan.Structural(constructor, children))));
    }

    private static Optional<CoercionPlan> planFromSelection(Expression actual, Expression formal, Alternative selection)
    {
        if (selection == null) {
            return Optional.empty();
        }
        List<CoercionPlan> matchingPlans = selection.coercionPlans().stream()
                .filter(plan -> plan.sourceType().equals(actual) && plan.targetType().equals(formal))
                .distinct()
                .toList();
        if (matchingPlans.size() == 1) {
            return Optional.of(matchingPlans.getFirst());
        }
        return Optional.empty();
    }

    private static Map<String, Expression> translateTypeBindings(
            List<Variable> parameters,
            Map<String, Expression> instantiationSubstitutions,
            Map<String, Expression> materializedTypeVariables)
    {
        Map<String, Expression> translated = new LinkedHashMap<>();
        for (Variable parameter : parameters) {
            Expression instantiated = instantiationSubstitutions.get(parameter.name());
            if (instantiated instanceof Variable(String freshName)) {
                Expression binding = materializedTypeVariables.get(freshName);
                if (binding != null) {
                    translated.put(parameter.name(), binding);
                }
            }
        }
        return Map.copyOf(translated);
    }

    private static Map<String, Integer> translateNumericBindings(
            List<Variable> parameters,
            Map<String, Expression> instantiationSubstitutions,
            Map<String, Integer> materializedNumericValues)
    {
        Map<String, Integer> translated = new LinkedHashMap<>();
        for (Variable parameter : parameters) {
            Expression instantiated = instantiationSubstitutions.get(parameter.name());
            if (instantiated instanceof Variable(String freshName)) {
                Integer binding = materializedNumericValues.get(freshName);
                if (binding != null) {
                    translated.put(parameter.name(), binding);
                }
            }
        }
        return Map.copyOf(translated);
    }

    public record Instantiation(Expression type, List<Constraint> constraints, Map<String, Expression> substitutions) {}

    public record MatchResult(
            Expression returnType,
            List<Expression> parameterTypes,
            List<Expression> parameterTemplates,
            Map<String, Expression> typeBindings,
            Map<String, Integer> numericBindings,
            Solver.Result solverResult) {}

    public sealed interface MatchOutcome
            permits Incomplete,
                    Satisfied,
                    Unsatisfied {}

    public record Satisfied(MatchResult result)
            implements MatchOutcome {}

    public record Unsatisfied(String message)
            implements MatchOutcome {}

    public record Incomplete(Solver.Result partialResult)
            implements MatchOutcome {}
}
