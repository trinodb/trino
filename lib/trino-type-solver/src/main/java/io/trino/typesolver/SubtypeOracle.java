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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// Answers "is A a subtype of B?" by delegating to a [Solver] and caching the result.
///
/// The solver is invoked with a single `Subtype(left, right)` constraint; whatever
/// outcome it reaches ([Solver.Satisfied], [Solver.Unsatisfied],
/// [Solver.Incomplete]) maps to a [Relation] value. Because that call can itself
/// ask subtype questions recursively (e.g. for structural decomposition), an
/// in-progress set guards against infinite recursion by returning [Relation#INCOMPLETE]
/// for cycles.
///
/// Used by
/// [FunctionResolver] for argument-specificity comparisons, and by the [Solver]
/// internals for refining alternatives.
public final class SubtypeOracle
{
    private final TypeSystem typeSystem;
    private final Map<Key, Relation> cache = new HashMap<>();
    private final Set<Key> inProgress = new HashSet<>();

    public SubtypeOracle(TypeSystem typeSystem)
    {
        this.typeSystem = typeSystem;
    }

    /// @return [Relation#SATISFIED] iff the solver proves `left` is a subtype of
    ///         `right`, [Relation#UNSATISFIED] iff it proves the opposite, or
    ///         [Relation#INCOMPLETE] if the relation depends on unresolved variables.
    public Relation classify(Expression left, Expression right)
    {
        if (left.equals(right)) {
            return Relation.SATISFIED;
        }
        Key key = new Key(left, right);
        Relation cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        if (!inProgress.add(key)) {
            return Relation.INCOMPLETE;
        }
        try {
            Relation result = classifyUncached(left, right);
            cache.put(key, result);
            return result;
        }
        finally {
            inProgress.remove(key);
        }
    }

    public boolean isSubtype(Expression left, Expression right)
    {
        return classify(left, right) == Relation.SATISFIED;
    }

    private Relation classifyUncached(Expression left, Expression right)
    {
        if (!Expression.isGround(left) || !Expression.isGround(right)) {
            boolean matched = false;
            boolean onlyConditionalRepresentationBridges = true;
            for (CoercionRule rule : typeSystem.candidateCoercions(left, right)) {
                Optional<CoercionRule.Match> match = rule.matches(new VariableAllocator(), left, right);
                if (match.isEmpty()) {
                    continue;
                }
                matched = true;
                if (!rule.isRepresentationBridge() || match.orElseThrow().constraints().isEmpty()) {
                    onlyConditionalRepresentationBridges = false;
                    break;
                }
            }
            // A conditional match proves only that some assignment of the open parameters makes
            // the relation hold. It does not prove the symbolic subtype relation itself. This
            // distinction matters for witness ordering: an exact representation bridge such as
            // unbounded varchar -> varchar(n), guarded by n == MAX_VALUE, must not make unbounded
            // varchar look narrower than every bounded varchar(n).
            if (matched && onlyConditionalRepresentationBridges) {
                return Relation.INCOMPLETE;
            }
        }
        return switch (new Solver(typeSystem, this).solveOutcome(List.of(new Subtype(left, right)))) {
            case Solver.Satisfied _ -> Relation.SATISFIED;
            case Solver.Unsatisfied _ -> Relation.UNSATISFIED;
            case Solver.Incomplete _ -> Relation.INCOMPLETE;
        };
    }

    public enum Relation
    {
        SATISFIED,
        UNSATISFIED,
        INCOMPLETE,
    }

    private record Key(Expression left, Expression right) {}
}
