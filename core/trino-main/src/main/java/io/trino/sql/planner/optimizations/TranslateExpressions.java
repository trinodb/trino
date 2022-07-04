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

package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.Map;
import java.util.Set;

import static io.trino.sql.planner.plan.Patterns.values;
import static io.trino.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.trino.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public class TranslateExpressions
{
    private final Metadata metadata;

    private final TypeAnalyzer typeAnalyzer;

    private final PlannerContext plannerContext;

    public TranslateExpressions(Metadata metadata, TypeAnalyzer typeAnalyzer, PlannerContext plannerContext)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public Set<Rule<?>> rules()
    {
        // TODO: finish all other PlanNodes that have Expression
        return ImmutableSet.of(
                new ValuesExpressionTranslation());
    }

    private final class ValuesExpressionTranslation
            implements Rule<ValuesNode>
    {
        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Result apply(ValuesNode valuesNode, Captures captures, Context context)
        {
            if (valuesNode.getRows().isEmpty()) {
                return Result.empty();
            }
            boolean anyRewritten = false;
            ImmutableList.Builder<RowExpression> rows = ImmutableList.builder();
            for (RowExpression row : valuesNode.getRows().get()) {
                if (isExpression(row)) {
                    Expression expression = castToExpression(row);
                    RowExpression rewritten = toRowExpression(expression, context, ImmutableMap.of());
                    anyRewritten = true;
                    rows.add(rewritten);
                }
                else {
                    rows.add(row);
                }
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getOutputSymbols(), rows.build()));
            }
            return Result.empty();
        }
    }

    private RowExpression toRowExpression(Expression expression, Context context, Map<Symbol, Integer> sourceLayout)
    {
        Map<NodeRef<Expression>, Type> types = typeAnalyzer.getTypes(context.getSession(), context.getSymbolAllocator().getTypes(), expression);
        return SqlToRowExpressionTranslator.translate(expression, types, sourceLayout, metadata, plannerContext.getFunctionManager(), context.getSession(), false);
    }
}
