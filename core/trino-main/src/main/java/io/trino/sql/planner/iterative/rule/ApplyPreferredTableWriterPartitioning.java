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
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableWriterNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.getPreferredWritePartitioningMinNumberOfPartitions;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionForcePreferredWritePartitioningEnabled;
import static io.trino.SystemSessionProperties.isUsePreferredWritePartitioning;
import static io.trino.cost.AggregationStatsRule.getRowsCount;
import static io.trino.sql.planner.plan.Patterns.tableWriterNode;
import static java.lang.Double.isNaN;

/**
 * Rule verifies if preconditions for using preferred write partitioning are met:
 *  - expected number of partitions to be written (based on table stat) is greater
 *    than or equal to preferred_write_partitioning_min_number_of_partitions session property,
 *  - use_preferred_write_partitioning is set to true.
 *
 * If precondition are met the {@link TableWriterNode} is modified to mark the intention to use preferred write partitioning:
 * value of {@link TableWriterNode#getPreferredPartitioningScheme()} is set as result of {@link TableWriterNode#getPartitioningScheme()}.
 */
public class ApplyPreferredTableWriterPartitioning
        implements Rule<TableWriterNode>
{
    public static final Pattern<TableWriterNode> WRITER_NODE_WITH_PREFERRED_PARTITIONING = tableWriterNode()
            .matching(node -> node.getPreferredPartitioningScheme().isPresent());

    @Override
    public Pattern<TableWriterNode> getPattern()
    {
        return WRITER_NODE_WITH_PREFERRED_PARTITIONING;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isUsePreferredWritePartitioning(session);
    }

    @Override
    public Result apply(TableWriterNode node, Captures captures, Context context)
    {
        if (getRetryPolicy(context.getSession()) == RetryPolicy.TASK && isFaultTolerantExecutionForcePreferredWritePartitioningEnabled(context.getSession())) {
            // Choosing preferred partitioning introduces a risk of running into a skew (for example when writing to only a single partition).
            // Fault tolerant execution can detect a potential skew automatically (based on runtime statistics) and mitigate it by splitting skewed partitions.
            return enable(node);
        }

        int minimumNumberOfPartitions = getPreferredWritePartitioningMinNumberOfPartitions(context.getSession());
        if (minimumNumberOfPartitions <= 1) {
            return enable(node);
        }

        double expectedNumberOfPartitions = getRowsCount(
                context.getStatsProvider().getStats(node.getSource()),
                node.getPreferredPartitioningScheme().get().getPartitioning().getColumns());
        // Disable preferred partitioning at remote exchange level if stats are absent or estimated number of partitions
        // are less than minimumNumberOfPartitions. This is because at remote exchange we don't have scaling to
        // mitigate skewness.
        // TODO - Remove this check after implementing skewness mitigation at remote exchange - https://github.com/trinodb/trino/issues/16178
        if (isNaN(expectedNumberOfPartitions) || (expectedNumberOfPartitions < minimumNumberOfPartitions)) {
            return Result.empty();
        }

        return enable(node);
    }

    private Result enable(TableWriterNode node)
    {
        return Result.ofPlanNode(new TableWriterNode(
                node.getId(),
                node.getSource(),
                node.getTarget(),
                node.getRowCountSymbol(),
                node.getFragmentSymbol(),
                node.getColumns(),
                node.getColumnNames(),
                node.getPreferredPartitioningScheme(),
                Optional.empty(),
                node.getStatisticsAggregation(),
                node.getStatisticsAggregationDescriptor()));
    }
}
