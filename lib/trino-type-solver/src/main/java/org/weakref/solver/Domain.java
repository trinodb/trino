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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The set of alternatives a type variable may still take, narrowing as the solver progresses.
 * <p>
 * A fresh variable starts {@linkplain #isRestricted() unrestricted}. The first call to
 * {@link #constrain} seeds the domain; subsequent calls intersect new candidates with the
 * existing set via {@link Unifier structural unification}, so that e.g. a variable
 * upper-bounded by both {@code array(@X)} and {@code array(integer)} is narrowed to
 * {@code array(integer)} with {@code @X ↦ integer}.
 * <p>
 * When the domain reduces to exactly one alternative it is {@linkplain #forced() forced},
 * and the solver binds the variable. When it reduces to zero, the problem is unsatisfiable.
 */
public final class Domain
{
    private List<Alternative> alternatives = List.of();
    private boolean restricted;

    /**
     * True once {@link #constrain} or {@link #replace} has been called at least once.
     */
    public boolean isRestricted()
    {
        return restricted;
    }

    public List<Alternative> alternatives()
    {
        return alternatives;
    }

    /**
     * Intersect the current domain with a new list of candidates.
     * <p>
     * On the first call, the domain takes on {@code candidates} directly. Afterwards each
     * existing alternative is unified pairwise with each incoming candidate; only pairs that
     * unify survive, and their witness/guards are rewritten through the resulting substitution
     * so that later queries see the most-refined form.
     */
    public void constrain(List<Alternative> candidates)
    {
        List<Alternative> normalizedCandidates = normalize(candidates);
        if (!restricted) {
            alternatives = normalizedCandidates;
            restricted = true;
            return;
        }

        List<Alternative> intersection = new ArrayList<>();
        for (Alternative existing : alternatives) {
            for (Alternative candidate : normalizedCandidates) {
                Unifier.Result result = Unifier.unify(existing.witness(), candidate.witness());
                if (result instanceof Unifier.Success success) {
                    Set<Constraint> guards = new LinkedHashSet<>();
                    existing.guards().stream()
                            .map(constraint -> constraint.apply(success.bindings()))
                            .forEach(guards::add);
                    candidate.guards().stream()
                            .map(constraint -> constraint.apply(success.bindings()))
                            .forEach(guards::add);

                    intersection.add(new Alternative(
                            Expression.substitute(existing.witness(), success.bindings()),
                            Set.copyOf(guards),
                            mergePlans(existing, candidate, success.bindings())));
                }
            }
        }
        alternatives = normalize(intersection);
    }

    /**
     * True once the domain has been constrained to zero feasible alternatives.
     */
    public boolean isEmpty()
    {
        return restricted && alternatives.isEmpty();
    }

    /**
     * If the domain has been narrowed to exactly one alternative, return it. The solver uses
     * this to decide when a variable can be committed to its witness.
     */
    public Optional<Alternative> forced()
    {
        if (restricted && alternatives.size() == 1) {
            return Optional.of(alternatives.getFirst());
        }
        return Optional.empty();
    }

    /**
     * Replace the current alternative set wholesale. Returns {@code true} if anything
     * actually changed. Used by {@link DomainRefiner} after filtering/pruning.
     */
    public boolean replace(List<Alternative> candidates)
    {
        List<Alternative> normalized = normalize(candidates);
        if (alternatives.equals(normalized)) {
            return false;
        }
        alternatives = normalized;
        restricted = true;
        return true;
    }

    private static List<Alternative> normalize(List<Alternative> candidates)
    {
        List<Alternative> normalized = new ArrayList<>();
        for (Alternative candidate : candidates) {
            boolean merged = false;
            for (int index = 0; index < normalized.size(); index++) {
                Alternative existing = normalized.get(index);
                if (existing.sameShape(candidate)) {
                    normalized.set(index, existing.mergePlans(candidate));
                    merged = true;
                    break;
                }
            }
            if (!merged) {
                normalized.add(candidate);
            }
        }
        return List.copyOf(normalized);
    }

    private static List<CoercionPlan> mergePlans(Alternative existing, Alternative candidate, java.util.Map<String, Expression> substitutions)
    {
        return java.util.stream.Stream.concat(
                        existing.coercionPlans().stream().map(plan -> plan.apply(substitutions)),
                        candidate.coercionPlans().stream().map(plan -> plan.apply(substitutions)))
                .distinct()
                .toList();
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
