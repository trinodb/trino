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

import org.weakref.solver.Expression.Application;
import org.weakref.solver.Expression.Symbol;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Covariant decomposition for a parametric type constructor.
 * <p>
 * Expresses: {@code C(T1, ..., Tn) ≤ C(U1, ..., Un) iff Ti ≤ Ui for all i}. Reduces a
 * structural subtyping question into element-wise subtyping, which re-enters the solver
 * and drives recursion into nested containers.
 * <p>
 * Used by the Trino preset for {@code array}, {@code map}, and {@code row}. All three are
 * treated identically — arity matches, then each argument pair is queued as a new subtype
 * question.
 * <p>
 * Covariance is an oversimplification for {@code map} keys (they are really invariant in
 * most SQL semantics), but for the purposes of analytical type matching it's what callers
 * expect.
 */
public record ParametricTypeCovariantCoercion(String type)
        implements CoercionRule
{
    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
        if (from instanceof Application(Expression fromHead, List<Expression> fromArguments) &&
                to instanceof Application(Expression toHead, List<Expression> toArguments) &&
                fromHead instanceof Symbol(String fromName) && fromName.equals(type) &&
                toHead instanceof Symbol(String toName) && toName.equals(type) &&
                fromArguments.size() == toArguments.size()) {
            Set<Constraint> constraints = new HashSet<>();
            for (int i = 0; i < fromArguments.size(); i++) {
                constraints.add(new Subtype(fromArguments.get(i), toArguments.get(i)));
            }

            return Optional.of(new Match(constraints));
        }
        return Optional.empty();
    }

    @Override
    public String ruleId()
    {
        return "covariant:" + type;
    }

    @Override
    public String toString()
    {
        return type + "(@T1, ..., @Tn) <: " + type + "(@U1, ..., @Un) <=> @T1 <: @U1, ..., @Tn <: @Un";
    }
}
