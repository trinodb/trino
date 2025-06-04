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

import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.plan.AggregationNode.Aggregation;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

public class PruneOrderByInAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final Metadata metadata;

    public PruneOrderByInAggregation(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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

            // getAggregateFunctionImplementation can be expensive, so check it last.
            if (aggregation.getOrderingScheme().isPresent() &&
                    !metadata.getAggregationFunctionMetadata(context.getSession(), aggregation.getResolvedFunction()).isOrderSensitive()) {
                aggregation = new Aggregation(
                        aggregation.getResolvedFunction(),
                        aggregation.getArguments(),
                        aggregation.isDistinct(),
                        aggregation.getFilter(),
                        Optional.empty(),
                        aggregation.getMask());
                anyRewritten = true;
            }
            aggregations.put(entry.getKey(), aggregation);
        }

        if (!anyRewritten) {
            return Result.empty();
        }
        return Result.ofPlanNode(AggregationNode.builderFrom(node)
                .setAggregations(aggregations.buildOrThrow())
                .build());
    }
}
