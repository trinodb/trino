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

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableExecuteNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.getPreferredWritePartitioningMinNumberOfPartitions;
import static io.trino.SystemSessionProperties.isUsePreferredWritePartitioning;
import static io.trino.cost.AggregationStatsRule.getRowsCount;
import static io.trino.sql.planner.plan.Patterns.tableExecute;
import static java.lang.Double.isNaN;

/**
 * Replaces {@link TableExecuteNode} with {@link TableExecuteNode#getPreferredPartitioningScheme()}
 * with a {@link TableExecuteNode} with {@link TableExecuteNode#getPartitioningScheme()} set.
 */
public class ApplyPreferredTableExecutePartitioning
        implements Rule<TableExecuteNode>
{
    public static final Pattern<TableExecuteNode> TABLE_EXECUTE_NODE_WITH_PREFERRED_PARTITIONING = tableExecute()
            .matching(node -> node.getPreferredPartitioningScheme().isPresent());

    @Override
    public Pattern<TableExecuteNode> getPattern()
    {
        return TABLE_EXECUTE_NODE_WITH_PREFERRED_PARTITIONING;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isUsePreferredWritePartitioning(session);
    }

    @Override
    public Result apply(TableExecuteNode node, Captures captures, Context context)
    {
        int minimumNumberOfPartitions = getPreferredWritePartitioningMinNumberOfPartitions(context.getSession());
        if (minimumNumberOfPartitions <= 1) {
            // Force 'preferred write partitioning' even if stats are missing or broken
            return enable(node);
        }

        double expectedNumberOfPartitions = getRowsCount(
                context.getStatsProvider().getStats(node.getSource()),
                node.getPreferredPartitioningScheme().get().getPartitioning().getColumns());

        if (isNaN(expectedNumberOfPartitions) || expectedNumberOfPartitions < minimumNumberOfPartitions) {
            return Result.empty();
        }

        return enable(node);
    }

    private static Result enable(TableExecuteNode node)
    {
        return Result.ofPlanNode(new TableExecuteNode(
                node.getId(),
                node.getSource(),
                node.getTarget(),
                node.getRowCountSymbol(),
                node.getFragmentSymbol(),
                node.getColumns(),
                node.getColumnNames(),
                node.getPreferredPartitioningScheme(),
                Optional.empty()));
    }
}
