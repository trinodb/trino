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

/**
 * Prunes dominated alternatives inside a {@link Choice}.
 * <p>
 * Two forms of dominance:
 * <ul>
 *   <li><b>Guard subsumption</b> — if two alternatives share the same witness, the one
 *       with a larger guard set is weaker and drops out (whoever has fewer guards is
 *       satisfiable at least as often).</li>
 *   <li><b>Witness subtype comparison</b> — if two alternatives have the same guard set and
 *       one witness is a subtype of the other, the more specific one wins (the more
 *       general witness is dominated).</li>
 * </ul>
 * Normalization also merges alternatives with identical witness + guards (just adding
 * their coercion plans together), so duplicates don't confuse the dominance loop.
 */
final class ChoiceSimplifier
{
    private final SubtypeOracle subtypeOracle;

    ChoiceSimplifier(SubtypeOracle subtypeOracle)
    {
        this.subtypeOracle = subtypeOracle;
    }

    List<Alternative> prune(List<Alternative> alternatives)
    {
        alternatives = normalize(alternatives);
        if (alternatives.size() <= 1) {
            return alternatives;
        }

        List<Alternative> pruned = new ArrayList<>();
        for (Alternative candidate : alternatives) {
            boolean dominated = false;
            for (Alternative other : alternatives) {
                if (candidate.equals(other)) {
                    continue;
                }
                if (!candidate.witness().equals(other.witness())) {
                    if (candidate.guards().equals(other.guards()) &&
                            witnessesComparable(candidate.witness(), other.witness()) &&
                            compareWitnesses(other.witness(), candidate.witness()) < 0) {
                        dominated = true;
                        break;
                    }
                    continue;
                }
                if (candidate.guards().containsAll(other.guards())) {
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

    private static List<Alternative> normalize(List<Alternative> alternatives)
    {
        List<Alternative> normalized = new ArrayList<>();
        for (Alternative candidate : alternatives) {
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

    private boolean witnessesComparable(Expression left, Expression right)
    {
        return subtypeOracle.isSubtype(left, right) || subtypeOracle.isSubtype(right, left);
    }

    private int compareWitnesses(Expression left, Expression right)
    {
        boolean leftSubtypeRight = subtypeOracle.isSubtype(left, right);
        boolean rightSubtypeLeft = subtypeOracle.isSubtype(right, left);
        if (leftSubtypeRight != rightSubtypeLeft) {
            return leftSubtypeRight ? -1 : 1;
        }
        return left.toString().compareTo(right.toString());
    }
}
