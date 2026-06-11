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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Answers "is A a subtype of B?" by delegating to a {@link Solver} and caching the result.
 * <p>
 * The solver is invoked with a single {@code Subtype(left, right)} constraint; whatever
 * outcome it reaches ({@link Solver.Satisfied}, {@link Solver.Unsatisfied},
 * {@link Solver.Incomplete}) maps to a {@link Relation} value. Because that call can itself
 * ask subtype questions recursively (e.g. for structural decomposition), an
 * in-progress set guards against infinite recursion by returning {@link Relation#INCOMPLETE}
 * for cycles.
 * <p>
 * Used by
 * {@link FunctionResolver} for argument-specificity comparisons, and by the {@link Solver}
 * internals for refining alternatives.
 */
public final class SubtypeOracle
{
    private final TypeSystem typeSystem;
    private final Map<Key, Relation> cache = new HashMap<>();
    private final Set<Key> inProgress = new HashSet<>();

    public SubtypeOracle(TypeSystem typeSystem)
    {
        this.typeSystem = typeSystem;
    }

    /**
     * @return {@link Relation#SATISFIED} iff the solver proves {@code left} is a subtype of
     *         {@code right}, {@link Relation#UNSATISFIED} iff it proves the opposite, or
     *         {@link Relation#INCOMPLETE} if the relation depends on unresolved variables.
     */
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
