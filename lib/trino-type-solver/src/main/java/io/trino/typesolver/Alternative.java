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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/// One concrete option in a [Choice] or [Domain].
///
/// Represents: "if this choice commits, the variable is `witness`, conditional on
/// `guards`, and the planner should record `coercionPlans` on the argument."
/// Two alternatives with the same witness and guards are considered the same shape
/// ([#sameShape]); their plans can be merged if both were produced by different
/// match paths.
///
/// @param witness the concrete type (or type shape) this alternative commits to
/// @param guards additional constraints that must hold — usually numeric relations
///         from a pattern coercion, or type-class obligations
/// @param coercionPlans how the planner should realize the coercion at runtime; empty
///         for witnesses derived from structural/covariant decomposition
public record Alternative(Expression witness, Set<Constraint> guards, List<CoercionPlan> coercionPlans)
{
    public Alternative
    {
        guards = Set.copyOf(guards);
        coercionPlans = List.copyOf(coercionPlans);
    }

    public Alternative(Expression witness, Set<Constraint> guards)
    {
        this(witness, guards, List.of());
    }

    public Alternative apply(Map<String, Expression> substitutions)
    {
        if (substitutions.isEmpty()) {
            return this;
        }
        return new Alternative(
                Expression.substitute(witness, substitutions),
                guards.stream()
                        .map(constraint -> constraint.apply(substitutions))
                        .collect(Collectors.toSet()),
                coercionPlans.stream()
                        .map(plan -> plan.apply(substitutions))
                        .toList());
    }

    public Alternative rewrite(Map<String, Expression> mappings)
    {
        return new Alternative(
                Expression.rewrite(witness, mappings),
                guards.stream()
                        .map(constraint -> constraint.rewrite(mappings))
                        .collect(Collectors.toSet()),
                coercionPlans.stream()
                        .map(plan -> plan.apply(mappings))
                        .toList());
    }

    public boolean sameShape(Alternative other)
    {
        return witness.equals(other.witness()) && guards.equals(other.guards());
    }

    static List<Alternative> normalize(List<Alternative> alternatives)
    {
        if (alternatives.size() <= 1) {
            return List.copyOf(alternatives);
        }
        Map<Shape, Alternative> normalized = new LinkedHashMap<>();
        for (Alternative alternative : alternatives) {
            ResolutionBudget.consume();
            normalized.merge(new Shape(alternative.witness(), alternative.guards()), alternative, Alternative::mergePlans);
        }
        return List.copyOf(normalized.values());
    }

    private record Shape(Expression witness, Set<Constraint> guards) {}

    public Alternative mergePlans(Alternative other)
    {
        if (!sameShape(other)) {
            throw new IllegalArgumentException("Cannot merge alternatives with different witness/guard shapes");
        }
        return new Alternative(
                witness,
                guards,
                Stream.concat(coercionPlans.stream(), other.coercionPlans().stream())
                        .distinct()
                        .toList());
    }
}
