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
package io.trino.sql.planner.iterative.rule;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ValuesNode;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.aggregation;

public class RemoveEmptyGlobalAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN =
            aggregation()
                    .with(step().equalTo(AggregationNode.Step.SINGLE))
                    .matching(node ->
                            // no aggregate functions
                            node.getAggregations().isEmpty() &&
                                    node.hasSingleGlobalAggregation());

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        // There should be no hash symbol in a global aggregation
        checkArgument(node.getHashSymbol().isEmpty(), "Unexpected hash symbol: %s", node.getHashSymbol());
        // There should be no output symbols, since there is no information the aggregation could return
        checkArgument(node.getOutputSymbols().isEmpty(), "Unexpected output symbols: %s", node.getOutputSymbols());

        return Result.ofPlanNode(new ValuesNode(node.getId(), 1));
    }
}
