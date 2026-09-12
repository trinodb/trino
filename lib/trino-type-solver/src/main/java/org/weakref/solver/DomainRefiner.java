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
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Secondary filtering layer for {@link Domain}s during the solver's reconcile phase.
 * <p>
 * Two responsibilities:
 * <ul>
 *   <li>{@link #applyRowRestriction} — drop alternatives whose witness is not a row type,
 *       when the variable has been asserted to belong to the row family
 *       ({@link TypeVariableState#mustBeRow()}).</li>
 *   <li>{@link #pruneDominatedAlternatives} — when a variable has only an upper bound,
 *       prefer the most specific witness (MAXIMAL dominance); when it has only a lower
 *       bound, prefer the most general (MINIMAL). An alternative is dominated and
 *       discarded when another alternative has strictly preferred witness and a subset of
 *       its guards, since the other is always feasible whenever this one is.</li>
 * </ul>
 * These transforms shrink the domain without committing to a specific alternative — that
 * happens later, once the domain has exactly one entry left.
 */
final class DomainRefiner
{
    private final SubtypeOracle subtypeOracle;

    DomainRefiner(SubtypeOracle subtypeOracle)
    {
        this.subtypeOracle = subtypeOracle;
    }

    List<Alternative> applyRowRestriction(TypeVariableState typeVariableState, List<Alternative> alternatives)
    {
        if (!typeVariableState.mustBeRow()) {
            return alternatives;
        }
        return alternatives.stream()
                .filter(alternative -> typeVariableState.isCompatible(alternative.witness()))
                .toList();
    }

    List<Alternative> pruneDominatedAlternatives(TypeVariableState typeVariableState, List<Alternative> alternatives)
    {
        Preference preference = preference(typeVariableState);
        if (preference == Preference.NONE || alternatives.size() <= 1) {
            return alternatives;
        }

        List<Alternative> pruned = new ArrayList<>();
        for (Alternative candidate : alternatives) {
            boolean dominated = false;
            for (Alternative other : alternatives) {
                if (candidate.equals(other)) {
                    continue;
                }
                if (dominates(other, candidate, preference)) {
                    dominated = true;
                    break;
                }
            }
            if (!dominated) {
                pruned.add(candidate);
            }
        }
        return List.copyOf(pruned);
    }

    private boolean dominates(Alternative preferred, Alternative discarded, Preference preference)
    {
        boolean preferredRelation = switch (preference) {
            case MINIMAL -> subtypeOracle.isSubtype(preferred.witness(), discarded.witness());
            case MAXIMAL -> subtypeOracle.isSubtype(discarded.witness(), preferred.witness());
            case NONE -> false;
        };

        if (!preferredRelation) {
            return false;
        }
        return discarded.guards().containsAll(preferred.guards());
    }

    private static Preference preference(TypeVariableState typeState)
    {
        boolean hasLowerBounds = hasMeaningfulBounds(typeState.lowerBounds());
        boolean hasUpperBounds = hasMeaningfulBounds(typeState.upperBounds());
        if (hasLowerBounds && !hasUpperBounds) {
            return Preference.MINIMAL;
        }
        if (!hasLowerBounds && hasUpperBounds) {
            return Preference.MAXIMAL;
        }
        return Preference.NONE;
    }

    private static boolean hasMeaningfulBounds(Optional<Set<Expression>> bounds)
    {
        return bounds.stream()
                .flatMap(Set::stream)
                .anyMatch(DomainRefiner::isMeaningfulBound);
    }

    private static boolean isMeaningfulBound(Expression expression)
    {
        return !(expression instanceof Expression.Variable) &&
                !(expression instanceof Expression.AnyRow);
    }

    private enum Preference
    {
        MINIMAL,
        MAXIMAL,
        NONE,
    }
}
