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

import io.trino.typesolver.Expression.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// Declarative rule of the form `fromPattern ≤ toPattern iff constraints hold`.
///
/// Example: `decimal(p1,s1) ≤ decimal(p2,s2) iff p1-s1 ≤ p2-s2 AND s1 ≤ s2`.
///
/// Matching is non-trivial because the same logical variable (like `p1`) can
/// occur multiple times in a single pattern, and the "same" logical variable is a fresh
/// allocated variable each time the rule fires so invocations don't collide:
///
/// 1. Both patterns are instantiated with fresh variables (distinct freshening of
///    `fromPattern` and `toPattern`).
/// 2. Each fresh pattern is unified against the actual expression.
/// 3. Multiple fresh variables that descend from the same logical name are then
///    unified pairwise so repeated occurrences align.
/// 4. The surviving bindings, plus rule constraints rewritten through them, form
///    the [Match] result.
///
/// Scalar ground inputs bind patterns directly. When only the bound is ground and
/// the other input is a type variable, only the witness's remaining parameters need
/// fresh names. Structural and open inputs use the general procedure above.
public record PatternCoercion(Expression fromPattern, Expression toPattern, List<Constraint> constraints)
        implements CoercionRule
{
    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
        return match(allocator, from, to, true);
    }

    /// Subtype classification needs the same obligations, without a runtime conversion plan.
    Optional<Set<Constraint>> matchConstraints(VariableAllocator allocator, Expression from, Expression to)
    {
        return match(allocator, from, to, false).map(Match::constraints);
    }

    private Optional<Match> match(VariableAllocator allocator, Expression from, Expression to, boolean includePlan)
    {
        if (isGroundScalar(from) && isGroundScalar(to)) {
            return matchGround(from, to, includePlan);
        }
        if (isGroundScalar(from) && to instanceof Variable variable && isScalarPattern(fromPattern) && isScalarPattern(toPattern)) {
            return matchGroundBound(allocator, from, to, fromPattern, toPattern, from, variable, includePlan);
        }
        if (from instanceof Variable variable && isGroundScalar(to) && isScalarPattern(fromPattern) && isScalarPattern(toPattern)) {
            return matchGroundBound(allocator, from, to, toPattern, fromPattern, to, variable, includePlan);
        }
        if (isScalarPattern(from) && isScalarPattern(to)) {
            Match match = matchOpenScalar(from, to, includePlan);
            if (match != null) {
                return Optional.of(match);
            }
        }
        // Instantiate each side independently with fresh variables so a single rule firing
        // against (from, to) doesn't alias logical names across the two positions.
        Expression.Instantiation fromInstantiation = Expression.instantiate(fromPattern, allocator);
        Expression.Instantiation toInstantiation = Expression.instantiate(toPattern, allocator);

        Unifier.Result fromUnification = Unifier.unify(fromInstantiation.expression(), from);
        Unifier.Result toUnification = Unifier.unify(toInstantiation.expression(), to);

        if (fromUnification instanceof Unifier.Failure || toUnification instanceof Unifier.Failure) {
            return Optional.empty();
        }

        if (fromUnification instanceof Unifier.Success(Map<String, Expression> fromBindings) &&
                toUnification instanceof Unifier.Success(Map<String, Expression> toBindings)) {
            Set<String> freshVariables = new HashSet<>();
            freshVariables.addAll(fromInstantiation.mapping().values());
            freshVariables.addAll(toInstantiation.mapping().values());

            // Collect every substitution that each logical name expanded into (from both sides).
            // A logical variable like "p" may appear in multiple positions — each occurrence
            // received its own fresh id, and each id may have been unified against a different
            // expression. We need to reconcile them so the rule constraints are rewritten
            // against a single representative per logical name.
            Map<String, List<Expression>> logicalVariableOccurrences = new HashMap<>();
            addLogicalOccurrences(logicalVariableOccurrences, fromInstantiation.mapping(), fromBindings);
            addLogicalOccurrences(logicalVariableOccurrences, toInstantiation.mapping(), toBindings);

            Map<String, Expression> extraBindings = new HashMap<>();
            Map<String, Expression> patternBindings = new HashMap<>();
            for (Map.Entry<String, List<Expression>> entry : logicalVariableOccurrences.entrySet()) {
                // Repeated occurrences of the same logical variable must agree; unify them and
                // collect any cross-consequences (e.g. unifying p=integer with p=s forces s=integer).
                List<Expression> occurrences = entry.getValue();
                Expression representative = Expression.substitute(occurrences.getFirst(), extraBindings);
                for (int index = 1; index < occurrences.size(); index++) {
                    Expression next = Expression.substitute(occurrences.get(index), extraBindings);
                    Unifier.Result result = Unifier.unify(representative, next);
                    if (result instanceof Unifier.Failure) {
                        return Optional.empty();
                    }
                    combineBindings(extraBindings, ((Unifier.Success) result).bindings());
                    representative = Expression.substitute(representative, extraBindings);
                }
                Expression binding = Expression.substitute(representative, extraBindings);
                if (!(binding instanceof Variable(String name)) || !name.equals(entry.getKey())) {
                    patternBindings.put(entry.getKey(), binding);
                }
            }

            Map<String, Expression> freshSubstitutions = new HashMap<>();
            fromInstantiation.mapping().forEach((logical, fresh) ->
                    freshSubstitutions.put(fresh, patternBindings.getOrDefault(logical, new Variable(logical))));
            toInstantiation.mapping().forEach((logical, fresh) ->
                    freshSubstitutions.put(fresh, patternBindings.getOrDefault(logical, new Variable(logical))));

            addExternalBindings(extraBindings, fromBindings, freshVariables, freshSubstitutions);
            addExternalBindings(extraBindings, toBindings, freshVariables, freshSubstitutions);

            Set<Constraint> newConstraints = new HashSet<>();
            List<Constraint> instantiatedConstraints = constraints.stream()
                    .map(constraint -> constraint.apply(patternBindings))
                    .toList();
            newConstraints.addAll(instantiatedConstraints);

            extraBindings.entrySet().stream()
                    .filter(entry -> !freshVariables.contains(entry.getKey()))
                    .map(entry -> new ExactType(entry.getKey(), Expression.substitute(entry.getValue(), extraBindings)))
                    .forEach(newConstraints::add);

            return Optional.of(matchResult(from, to, newConstraints, instantiatedConstraints, includePlan));
        }
        else {
            return Optional.empty();
        }
    }

    private Optional<Match> matchGroundBound(VariableAllocator allocator, Expression from, Expression to, Expression boundPattern, Expression openPattern, Expression bound, Variable variable, boolean includePlan)
    {
        if (!(Unifier.unify(boundPattern, bound) instanceof Unifier.Success(Map<String, Expression> boundBindings))) {
            return Optional.empty();
        }
        // Only the unbound parameters need fresh names. Matching a scalar ground bound
        // cannot introduce aliases back into the caller's variable namespace.
        allocator.reserveThrough(VariableAllocator.variableId(variable.name()));
        Expression.Instantiation witness = Expression.instantiate(Expression.substitute(openPattern, boundBindings), allocator);
        Map<String, Expression> bindings = new HashMap<>(boundBindings);
        witness.mapping().forEach((name, fresh) -> bindings.put(name, Expression.variable(fresh)));
        List<Constraint> instantiated = constraints.stream().map(constraint -> constraint.apply(bindings)).toList();
        Set<Constraint> obligations = new HashSet<>(instantiated);
        obligations.add(new ExactType(variable.name(), witness.expression()));
        return Optional.of(matchResult(from, to, obligations, instantiated, includePlan));
    }

    private static boolean isScalarPattern(Expression expression)
    {
        return expression instanceof Expression.Symbol ||
                (expression instanceof Expression.Application(Expression.Symbol _, List<Expression> arguments) && arguments.stream().allMatch(argument -> argument instanceof Variable || argument instanceof Expression.Literal));
    }

    /// Flat numeric patterns need no fresh variables when every parameter binds directly
    /// to an input and no caller name can be captured by recursive substitution. Repeated
    /// parameters requiring aliases and guards with additional variables use general matching.
    private Match matchOpenScalar(Expression from, Expression to, boolean includePlan)
    {
        Map<String, Expression> bindings = new HashMap<>();
        if (!bindScalarPattern(fromPattern, from, bindings) || !bindScalarPattern(toPattern, to, bindings)) {
            return null;
        }
        for (Expression value : bindings.values()) {
            if (value instanceof Variable(String name) && bindings.containsKey(name)) {
                return null;
            }
        }
        for (Constraint constraint : constraints) {
            if (!(constraint instanceof NumericRelation) || !bindings.keySet().containsAll(Solver.variables(constraint))) {
                return null;
            }
        }
        List<Constraint> instantiated = constraints.stream().map(constraint -> constraint.apply(bindings)).toList();
        return matchResult(from, to, Set.copyOf(instantiated), instantiated, includePlan);
    }

    private static boolean bindScalarPattern(Expression pattern, Expression input, Map<String, Expression> bindings)
    {
        ResolutionBudget.consume();
        if (pattern instanceof Expression.Symbol && input instanceof Expression.Symbol) {
            return pattern.equals(input);
        }
        if (!(pattern instanceof Expression.Application(Expression.Symbol patternName, List<Expression> parameters)) ||
                !(input instanceof Expression.Application(Expression.Symbol inputName, List<Expression> arguments)) ||
                !patternName.equals(inputName) || parameters.size() != arguments.size()) {
            return false;
        }
        for (int index = 0; index < parameters.size(); index++) {
            ResolutionBudget.consume();
            Expression parameter = parameters.get(index);
            Expression argument = arguments.get(index);
            if (parameter instanceof Variable(String name)) {
                Expression previous = bindings.putIfAbsent(name, argument);
                if (previous != null && !previous.equals(argument)) {
                    return false;
                }
            }
            else if (!(parameter instanceof Expression.Literal) || !parameter.equals(argument)) {
                return false;
            }
        }
        return true;
    }

    private Optional<Match> matchGround(Expression from, Expression to, boolean includePlan)
    {
        // Ground scalar inputs cannot capture pattern variables. Bind the patterns directly,
        // preserving repeated logical names across both sides without freshening them.
        if (!(Unifier.unify(fromPattern, from) instanceof Unifier.Success(Map<String, Expression> fromBindings))) {
            return Optional.empty();
        }
        if (!(Unifier.unify(Expression.substitute(toPattern, fromBindings), to) instanceof Unifier.Success(Map<String, Expression> toBindings))) {
            return Optional.empty();
        }
        Map<String, Expression> bindings = new HashMap<>(fromBindings);
        bindings.putAll(toBindings);
        List<Constraint> instantiated = constraints.stream().map(constraint -> constraint.apply(bindings)).toList();
        return Optional.of(matchResult(from, to, Set.copyOf(instantiated), instantiated, includePlan));
    }

    private Match matchResult(Expression from, Expression to, Set<Constraint> obligations, List<Constraint> conditions, boolean includePlan)
    {
        if (!includePlan) {
            return new Match(obligations);
        }
        CoercionPlan plan = from.equals(to)
                ? CoercionPlan.exact(from, to)
                : CoercionPlan.directSteps(from, to, List.of(new CoercionPlan.DirectRule(ruleId(), conditions)));
        return new Match(obligations, plan);
    }

    private static boolean isGroundScalar(Expression expression)
    {
        return expression instanceof Expression.Symbol ||
                (expression instanceof Expression.Application(Expression.Symbol _, List<Expression> arguments) && arguments.stream().allMatch(Expression.Literal.class::isInstance));
    }

    @Override
    public String ruleId()
    {
        return "pattern:" + fromPattern + "->" + toPattern;
    }

    @Override
    public String toString()
    {
        return fromPattern + " <: " + toPattern + " <=> " + constraints;
    }

    private static void addLogicalOccurrences(Map<String, List<Expression>> logicalVariableOccurrences, Map<String, String> mapping, Map<String, Expression> bindings)
    {
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            logicalVariableOccurrences.computeIfAbsent(entry.getKey(), _ -> new ArrayList<>())
                    .add(resolve(entry.getValue(), bindings));
        }
    }

    private static Expression resolve(String variable, Map<String, Expression> bindings)
    {
        return Expression.substitute(new Variable(variable), bindings);
    }

    private static void combineBindings(Map<String, Expression> bindings, Map<String, Expression> newBindings)
    {
        for (Map.Entry<String, Expression> entry : bindings.entrySet()) {
            entry.setValue(Expression.substitute(entry.getValue(), newBindings));
        }
        newBindings.forEach((key, value) -> {
            if (!(value instanceof Variable(String name)) || !name.equals(key)) {
                bindings.put(key, value);
            }
        });
    }

    private static void addExternalBindings(
            Map<String, Expression> extraBindings,
            Map<String, Expression> bindings,
            Set<String> freshVariables,
            Map<String, Expression> freshSubstitutions)
    {
        for (Map.Entry<String, Expression> entry : bindings.entrySet()) {
            if (freshVariables.contains(entry.getKey())) {
                continue;
            }
            Expression translated = Expression.substitute(entry.getValue(), freshSubstitutions);
            combineBindings(extraBindings, Map.of(entry.getKey(), translated));
        }
    }
}
