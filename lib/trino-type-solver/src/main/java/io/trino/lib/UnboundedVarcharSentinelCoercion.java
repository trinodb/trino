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
package io.trino.lib;

import org.weakref.solver.CoercionPlan;
import org.weakref.solver.CoercionRule;
import org.weakref.solver.Expression;
import org.weakref.solver.NumericRelation;
import org.weakref.solver.VariableAllocator;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.weakref.solver.Expression.operation;

/// Lets the preset's distinct unbounded varchar type flow into the parametric varchar formals of
/// catalog-bridged signatures, which encode unbounded as the `varchar(2147483647)` length
/// sentinel. The re-encoding is EXACT, not a coercion: in the engine's own model the sentinel IS
/// the unbounded type, so a parametric formal binds it directly and overload selection must not
/// count a conversion — otherwise a genuinely coercing candidate (`upper(char(x))` for a varchar
/// argument) ties with the varchar form the engine prefers.
public final class UnboundedVarcharSentinelCoercion
        implements CoercionRule
{
    @Override
    public String ruleId()
    {
        return "unbounded-varchar-sentinel";
    }

    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
        if (!from.equals(Expression.symbol("varchar"))) {
            return Optional.empty();
        }
        if (!(to instanceof Expression.Application(Expression.Symbol(String base), List<Expression> arguments)) ||
                !base.equals("varchar") ||
                arguments.size() != 1) {
            return Optional.empty();
        }
        return switch (arguments.getFirst()) {
            case Expression.Variable variable -> Optional.of(new Match(
                    Set.of(new NumericRelation(operation(Expression.BinaryOperator.EQUAL, variable, Expression.literal(Integer.MAX_VALUE)))),
                    CoercionPlan.exact(from, to)));
            case Expression.Literal(int value) when value == Integer.MAX_VALUE -> Optional.of(new Match(Set.of(), CoercionPlan.exact(from, to)));
            default -> Optional.empty();
        };
    }
}
