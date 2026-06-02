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

import org.weakref.solver.Expression.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Declarative rule of the form {@code fromPattern ≤ toPattern iff constraints hold}.
 * <p>
 * Example: {@code decimal(@p1,@s1) ≤ decimal(@p2,@s2) iff @p1-@s1 ≤ @p2-@s2 AND @s1 ≤ @s2}.
 * <p>
 * Matching is non-trivial because the same logical variable (like {@code @p1}) can
 * occur multiple times in a single pattern, and the "same" logical variable is a fresh
 * allocated variable each time the rule fires so invocations don't collide:
 * <ol>
 *   <li>Both patterns are instantiated with fresh variables (distinct freshening of
 *       {@code fromPattern} and {@code toPattern}).</li>
 *   <li>Each fresh pattern is unified against the actual expression.</li>
 *   <li>Multiple fresh variables that descend from the same logical name are then
 *       unified pairwise so repeated occurrences align.</li>
 *   <li>The surviving bindings, plus rule constraints rewritten through them, form
 *       the {@link Match} result.</li>
 * </ol>
 */
public record PatternCoercion(Expression fromPattern, Expression toPattern, List<Constraint> constraints)
        implements CoercionRule
{
    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
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
            // A logical variable like "@p" may appear in multiple positions — each occurrence
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
                // collect any cross-consequences (e.g. unifying @p=integer with @p=@s forces @s=integer).
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

            CoercionPlan plan = from.equals(to)
                    ? CoercionPlan.exact(from, to)
                    : CoercionPlan.directSteps(
                    from,
                    to,
                    List.of(new CoercionPlan.DirectRule(ruleId(), instantiatedConstraints)));

            return Optional.of(new Match(newConstraints, plan));
        }
        else {
            return Optional.empty();
        }
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
