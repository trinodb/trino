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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.FunctionManager;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.tree.FunctionCall;

import java.util.Map;

import static io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

public class PruneOrderByInAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final FunctionManager functionManager;

    public PruneOrderByInAggregation(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        if (!node.hasOrderings()) {
            return Result.empty();
        }

        boolean anyRewritten = false;
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            if (!aggregation.getCall().getOrderBy().isPresent()) {
                aggregations.put(entry);
            }
            // getAggregateFunctionImplementation can be expensive, so check it last.
            else if (functionManager.getAggregateFunctionImplementation(aggregation.getFunctionHandle()).isOrderSensitive()) {
                aggregations.put(entry);
            }
            else {
                anyRewritten = true;
                FunctionCall rewritten = new FunctionCall(
                        aggregation.getCall().getName(),
                        aggregation.getCall().isDistinct(),
                        aggregation.getCall().getArguments(),
                        aggregation.getCall().getFilter());

                aggregations.put(entry.getKey(), new Aggregation(rewritten, aggregation.getFunctionHandle(), aggregation.getMask()));
            }
        }

        if (!anyRewritten) {
            return Result.empty();
        }
        return Result.ofPlanNode(new AggregationNode(
                node.getId(),
                node.getSource(),
                aggregations.build(),
                node.getGroupingSets(),
                node.getPreGroupedSymbols(),
                node.getStep(),
                node.getHashSymbol(),
                node.getGroupIdSymbol()));
    }
}
