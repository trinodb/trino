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

import io.trino.typesolver.TypeSystem.CoercionResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/// Reuses rule expansion for repeated ground bounds within one solve. Cached results
/// are immutable templates: every reuse gives their variables fresh names so independent
/// fields never share numeric parameters or conditional guards.
final class CoercionCache
{
    private final TypeSystem typeSystem;
    private final Map<Key, Template> templates = new HashMap<>();

    CoercionCache(TypeSystem typeSystem)
    {
        this.typeSystem = typeSystem;
    }

    List<CoercionResult> from(Expression type, VariableAllocator allocator)
    {
        return get(type, allocator, true);
    }

    List<CoercionResult> to(Expression type, VariableAllocator allocator)
    {
        return get(type, allocator, false);
    }

    private List<CoercionResult> get(Expression type, VariableAllocator allocator, boolean from)
    {
        ResolutionBudget.consume();
        if (!Expression.isGround(type)) {
            return expand(type, allocator, from);
        }
        Key key = new Key(type, from);
        Template cached = templates.get(key);
        if (cached != null) {
            return cached.instantiate(allocator);
        }

        List<CoercionResult> results = expand(type, allocator, from);
        templates.put(key, new Template(results));
        return results;
    }

    private List<CoercionResult> expand(Expression type, VariableAllocator allocator, boolean from)
    {
        return from ? typeSystem.coercionsFrom(type, allocator) : typeSystem.coercionsTo(type, allocator);
    }

    private static void collectVariables(CoercionPlan plan, Set<String> variables)
    {
        variables.addAll(Solver.variables(plan.sourceType()));
        variables.addAll(Solver.variables(plan.targetType()));
        for (CoercionPlan.CoercionStep step : plan.steps()) {
            switch (step) {
                case CoercionPlan.DirectRule(_, List<Constraint> conditions) -> conditions.forEach(condition -> variables.addAll(Solver.variables(condition)));
                case CoercionPlan.Structural(_, List<CoercionPlan> children) -> children.forEach(child -> collectVariables(child, variables));
            }
        }
    }

    private record Key(Expression type, boolean from) {}

    private static final class Template
    {
        private final List<CoercionResult> results;
        private List<Boolean> symbolic;
        private List<String> variables;

        private Template(List<CoercionResult> results)
        {
            this.results = results;
        }

        private void initializeVariables()
        {
            List<Boolean> symbolic = new ArrayList<>();
            Set<String> variables = new LinkedHashSet<>();
            for (CoercionResult result : results) {
                Set<String> referenced = new LinkedHashSet<>(Solver.variables(result.type()));
                result.guards().forEach(guard -> referenced.addAll(Solver.variables(guard)));
                collectVariables(result.plan(), referenced);
                symbolic.add(!referenced.isEmpty());
                variables.addAll(referenced);
            }
            this.symbolic = List.copyOf(symbolic);
            this.variables = List.copyOf(variables);
        }

        List<CoercionResult> instantiate(VariableAllocator allocator)
        {
            if (variables == null) {
                initializeVariables();
            }
            if (variables.isEmpty()) {
                return results;
            }
            // Substitution is recursive. Allocate beyond every template name so a new
            // name can never also be a substitution key, even with caller-supplied names.
            variables.forEach(name -> allocator.reserveThrough(VariableAllocator.variableId(name)));
            Map<String, Expression> substitutions = new HashMap<>();
            variables.forEach(name -> substitutions.put(name, Expression.variable(allocator.newVariable())));
            List<CoercionResult> instantiated = new ArrayList<>(results.size());
            for (int index = 0; index < results.size(); index++) {
                CoercionResult result = results.get(index);
                if (!symbolic.get(index)) {
                    instantiated.add(result);
                    continue;
                }
                Set<Constraint> guards = new LinkedHashSet<>();
                for (Constraint guard : result.guards()) {
                    guards.add(guard.apply(substitutions));
                }
                instantiated.add(new CoercionResult(
                        Expression.substitute(result.type(), substitutions),
                        Set.copyOf(guards),
                        result.plan().apply(substitutions)));
            }
            return List.copyOf(instantiated);
        }
    }
}
