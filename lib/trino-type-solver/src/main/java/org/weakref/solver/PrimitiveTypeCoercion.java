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

import org.weakref.solver.Expression.Symbol;
import org.weakref.solver.Expression.Variable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.weakref.solver.Expression.symbol;

/**
 * Declares a single atomic widening such as {@code tinyint → smallint}.
 * <p>
 * Matches three shapes: concrete→concrete, concrete→variable (binding the variable to
 * {@code toType}), and variable→concrete (binding the variable to {@code fromType}).
 * These variable-side matches are what let the solver use this rule while computing
 * {@code coercionsTo}/{@code coercionsFrom} witnesses for a type variable with the
 * named type as a bound.
 * <p>
 * The {@link #ruleId()} encodes the specific pair so plans downstream can distinguish
 * "integer → bigint" from "smallint → integer" in reporting.
 */
public record PrimitiveTypeCoercion(String fromType, String toType)
        implements CoercionRule
{
    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
        if (from instanceof Symbol(String fromName) &&
                to instanceof Symbol(String toName) &&
                fromName.equals(fromType) && toName.equals(toType)) {
            return Optional.of(new Match(
                    Set.of(),
                    CoercionPlan.directSteps(from, to, List.of(new CoercionPlan.DirectRule(ruleId(), List.of())))));
        }
        else if (from instanceof Symbol(String fromName) &&
                to instanceof Variable(String variable) &&
                fromName.equals(fromType)) {
            return Optional.of(new Match(
                    Set.of(new ExactType(variable, symbol(toType))),
                    CoercionPlan.directSteps(from, to, List.of(new CoercionPlan.DirectRule(ruleId(), List.of())))));
        }
        else if (from instanceof Variable(String variable) &&
                to instanceof Symbol(String toName) &&
                toName.equals(toType)) {
            return Optional.of(new Match(
                    Set.of(new ExactType(variable, symbol(fromType))),
                    CoercionPlan.directSteps(from, to, List.of(new CoercionPlan.DirectRule(ruleId(), List.of())))));
        }

        return Optional.empty();
    }

    @Override
    public String ruleId()
    {
        return "primitive:" + fromType + "->" + toType;
    }

    @Override
    public String toString()
    {
        return fromType + " <: " + toType;
    }
}
