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
import org.weakref.solver.Constraint;
import org.weakref.solver.Expression;
import org.weakref.solver.RequireCastableFrom;
import org.weakref.solver.VariableAllocator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cast rule for {@code variant → row(...)}: permits the cast if every field of the target row
 * is individually castable from {@code variant}.
 */
public final class VariantToRowCastRule
        implements CoercionRule
{
    private static final Expression.Symbol VARIANT = new Expression.Symbol("variant");

    @Override
    public Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to)
    {
        if (!VARIANT.equals(from) || !(to instanceof Expression.Row(List<Expression.RowField> fields))) {
            return Optional.empty();
        }
        Set<Constraint> guards = fields.stream()
                .map(field -> (Constraint) new RequireCastableFrom(field.type(), VARIANT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Optional.of(new Match(
                guards,
                CoercionPlan.directSteps(from, to, List.of(new CoercionPlan.DirectRule(ruleId(), List.of())))));
    }

    @Override
    public String ruleId()
    {
        return "variantToRow";
    }
}
