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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.TableWriterNode;

import static io.trino.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.trino.sql.planner.plan.Patterns.tableWriterNode;

public class PruneTableWriterSourceColumns
        implements Rule<TableWriterNode>
{
    private static final Pattern<TableWriterNode> PATTERN = tableWriterNode();

    @Override
    public Pattern<TableWriterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableWriterNode tableWriterNode, Captures captures, Context context)
    {
        ImmutableSet.Builder<Symbol> requiredInputs = ImmutableSet.<Symbol>builder()
                .addAll(tableWriterNode.getColumns());

        if (tableWriterNode.getPartitioningScheme().isPresent()) {
            PartitioningScheme partitioningScheme = tableWriterNode.getPartitioningScheme().get();
            partitioningScheme.getPartitioning().getColumns().forEach(requiredInputs::add);
            partitioningScheme.getHashColumn().ifPresent(requiredInputs::add);
        }

        if (tableWriterNode.getStatisticsAggregation().isPresent()) {
            StatisticAggregations aggregations = tableWriterNode.getStatisticsAggregation().get();
            requiredInputs.addAll(aggregations.getGroupingSymbols());
            aggregations.getAggregations().values().stream()
                    .map(SymbolsExtractor::extractUnique)
                    .forEach(requiredInputs::addAll);
        }

        return restrictChildOutputs(context.getIdAllocator(), tableWriterNode, requiredInputs.build())
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
