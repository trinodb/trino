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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// The set of alternatives a type variable may still take, narrowing as the solver progresses.
///
/// A fresh variable starts [unrestricted][#isRestricted()]. The first call to
/// [#constrain] seeds the domain; subsequent calls intersect new candidates with the
/// existing set via [structural unification][Unifier], so that e.g. a variable
/// upper-bounded by both `array(X)` and `array(integer)` is narrowed to
/// `array(integer)` with `X ↦ integer`.
///
/// When the domain reduces to exactly one alternative it is [forced][#forced()],
/// and the solver binds the variable. When it reduces to zero, the problem is unsatisfiable.
public final class Domain
{
    private List<Alternative> alternatives = List.of();
    private boolean restricted;

    /// True once [#constrain] or [#replace] has been called at least once.
    public boolean isRestricted()
    {
        return restricted;
    }

    public List<Alternative> alternatives()
    {
        return alternatives;
    }

    /// Intersect the current domain with a new list of candidates.
    ///
    /// On the first call, the domain takes on `candidates` directly. Afterwards each
    /// existing alternative is unified with compatible incoming candidates; only pairs that
    /// unify survive, and their witness/guards are rewritten through the resulting substitution
    /// so that later queries see the most-refined form.
    public void constrain(List<Alternative> candidates)
    {
        List<Alternative> normalizedCandidates = Alternative.normalize(candidates);
        if (!restricted) {
            alternatives = normalizedCandidates;
            restricted = true;
            return;
        }

        List<Alternative> intersection = new ArrayList<>();
        Map<Head, List<Alternative>> byHead = indexByHead(normalizedCandidates);
        for (Alternative existing : alternatives) {
            Head head = head(existing.witness());
            List<Alternative> compatible = byHead.isEmpty() || head == null ? normalizedCandidates : byHead.getOrDefault(head, List.of());
            for (Alternative candidate : compatible) {
                ResolutionBudget.consume();
                Unifier.Result result = Unifier.unify(existing.witness(), candidate.witness());
                if (result instanceof Unifier.Success success) {
                    Set<Constraint> guards;
                    if (success.bindings().isEmpty() && existing.guards().isEmpty()) {
                        guards = candidate.guards();
                    }
                    else if (success.bindings().isEmpty() && candidate.guards().isEmpty()) {
                        guards = existing.guards();
                    }
                    else {
                        guards = new LinkedHashSet<>();
                        for (Constraint guard : existing.guards()) {
                            guards.add(guard.apply(success.bindings()));
                        }
                        for (Constraint guard : candidate.guards()) {
                            guards.add(guard.apply(success.bindings()));
                        }
                    }

                    // The merged witness keeps a row field name only where both sides agree, the
                    // way the engine computes row supertypes — this catches names where the two
                    // witnesses meet as simultaneous domain alternatives; a witness that forces
                    // and binds before its competitor arrives is reconciled later against the
                    // variable's recorded bounds in the materializer
                    intersection.add(new Alternative(
                            Expression.mergeRowFieldNames(
                                    Expression.substitute(existing.witness(), success.bindings()),
                                    Expression.substitute(candidate.witness(), success.bindings())),
                            Set.copyOf(guards),
                            mergePlans(existing, candidate, success.bindings())));
                }
            }
        }
        alternatives = Alternative.normalize(intersection);
    }

    private static Map<Head, List<Alternative>> indexByHead(List<Alternative> alternatives)
    {
        if (alternatives.size() <= 1) {
            return Map.of();
        }
        Map<Head, List<Alternative>> indexed = new HashMap<>();
        for (Alternative alternative : alternatives) {
            ResolutionBudget.consume();
            Head head = head(alternative.witness());
            if (head == null) {
                // Variables and row-family wildcards may unify across groups.
                return Map.of();
            }
            indexed.computeIfAbsent(head, _ -> new ArrayList<>()).add(alternative);
        }
        return indexed;
    }

    private static Head head(Expression expression)
    {
        return switch (expression) {
            case Expression.Symbol(String name) -> new Head(name, -1);
            case Expression.Application(Expression.Symbol(String name), List<Expression> arguments) -> new Head(name, arguments.size());
            default -> null;
        };
    }

    private record Head(String name, int arity) {}

    /// True once the domain has been constrained to zero feasible alternatives.
    public boolean isEmpty()
    {
        return restricted && alternatives.isEmpty();
    }

    /// If the domain has been narrowed to exactly one alternative, return it. The solver uses
    /// this to decide when a variable can be committed to its witness.
    public Optional<Alternative> forced()
    {
        if (restricted && alternatives.size() == 1) {
            return Optional.of(alternatives.getFirst());
        }
        return Optional.empty();
    }

    /// Replace the current alternative set wholesale. Returns `true` if anything
    /// actually changed. Used by [DomainRefiner] after filtering/pruning.
    public boolean replace(List<Alternative> candidates)
    {
        List<Alternative> normalized = Alternative.normalize(candidates);
        if (alternatives.equals(normalized)) {
            return false;
        }
        alternatives = normalized;
        restricted = true;
        return true;
    }

    private static List<CoercionPlan> mergePlans(Alternative existing, Alternative candidate, Map<String, Expression> substitutions)
    {
        if (substitutions.isEmpty()) {
            if (existing.coercionPlans().isEmpty()) {
                return candidate.coercionPlans();
            }
            if (candidate.coercionPlans().isEmpty() || existing.coercionPlans().equals(candidate.coercionPlans())) {
                return existing.coercionPlans();
            }
        }
        Set<CoercionPlan> plans = new LinkedHashSet<>();
        for (CoercionPlan plan : existing.coercionPlans()) {
            plans.add(plan.apply(substitutions));
        }
        for (CoercionPlan plan : candidate.coercionPlans()) {
            plans.add(plan.apply(substitutions));
        }
        return List.copyOf(plans);
    }

    @Override
    public String toString()
    {
        if (!restricted) {
            return "unrestricted";
        }
        return alternatives.toString();
    }
}
