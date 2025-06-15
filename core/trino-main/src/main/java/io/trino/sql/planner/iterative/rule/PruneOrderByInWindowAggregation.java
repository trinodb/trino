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
import io.trino.spi.function.FunctionKind;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Function;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.window;
import static java.util.Objects.requireNonNull;

public class PruneOrderByInWindowAggregation
        implements Rule<WindowNode>
{
    private static final Pattern<WindowNode> PATTERN = window();
    private final Metadata metadata;

    public PruneOrderByInWindowAggregation(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<WindowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(WindowNode node, Captures captures, Context context)
    {
        boolean anyRewritten = false;
        ImmutableMap.Builder<Symbol, Function> rewritten = ImmutableMap.builder();
        for (Map.Entry<Symbol, Function> entry : node.getWindowFunctions().entrySet()) {
            Function function = entry.getValue();
            // getAggregateFunctionImplementation can be expensive, so check it last.
            if (function.getOrderingScheme().isPresent() &&
                    function.getResolvedFunction().functionKind() == FunctionKind.AGGREGATE &&
                    !metadata.getAggregationFunctionMetadata(context.getSession(), function.getResolvedFunction()).isOrderSensitive()) {
                function = new Function(
                        function.getResolvedFunction(),
                        function.getArguments(),
                        Optional.empty(), // prune
                        function.getFrame(),
                        function.isIgnoreNulls(),
                        function.isDistinct());
                anyRewritten = true;
            }
            rewritten.put(entry.getKey(), function);
        }

        if (!anyRewritten) {
            return Result.empty();
        }
        return Result.ofPlanNode(new WindowNode(
                node.getId(),
                node.getSource(),
                node.getSpecification(),
                rewritten.buildOrThrow(),
                node.getPrePartitionedInputs(),
                node.getPreSortedOrderPrefix()));
    }
}
