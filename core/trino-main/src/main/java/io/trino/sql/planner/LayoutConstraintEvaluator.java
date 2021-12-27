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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NullLiteral;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;

public class LayoutConstraintEvaluator
{
    private final Map<Symbol, ColumnHandle> assignments;
    private final ExpressionInterpreter evaluator;
    private final Set<ColumnHandle> arguments;

    public LayoutConstraintEvaluator(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, Session session, TypeProvider types, Map<Symbol, ColumnHandle> assignments, Expression expression)
    {
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        evaluator = new ExpressionInterpreter(expression, plannerContext, session, typeAnalyzer.getTypes(session, types, expression));
        arguments = SymbolsExtractor.extractUnique(expression).stream()
                .map(assignments::get)
                .collect(toImmutableSet());
    }

    public Set<ColumnHandle> getArguments()
    {
        return arguments;
    }

    public boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
    {
        if (intersection(bindings.keySet(), arguments).isEmpty()) {
            return true;
        }
        LookupSymbolResolver inputs = new LookupSymbolResolver(assignments, bindings);

        // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
        // spurious query failures for partitions that would otherwise be filtered out.
        Object optimized = TryFunction.evaluate(() -> evaluator.optimize(inputs), true);

        // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
        return !(Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral);
    }
}
