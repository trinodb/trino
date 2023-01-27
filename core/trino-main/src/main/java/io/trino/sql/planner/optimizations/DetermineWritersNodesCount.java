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

import io.trino.Session;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableWriterNode;

import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.MAX_WRITERS_NODES_COUNT;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

public class DetermineWritersNodesCount
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            TableStatsProvider tableStatsProvider)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");

        // Skip for plans where there is not writing stages Additionally, skip for FTE mode since we
        // are not using estimated partitionCount in FTE scheduler.

        if (!PlanNodeSearcher.searchFrom(plan).whereIsInstanceOfAny(TableWriterNode.class).matches() || getRetryPolicy(session) == RetryPolicy.TASK) {
            return plan;
        }

        return rewriteWith(new Rewriter(
                session.getSystemProperty(MAX_WRITERS_NODES_COUNT, Integer.class),
                session.getSystemProperty(MAX_HASH_PARTITION_COUNT, Integer.class)), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final int maxWriterNodesCount;
        private final int maxHashPartitionCount;

        private Rewriter(int maxWriterNodesCount, int maxHashPartitionCount)
        {
            this.maxWriterNodesCount = maxWriterNodesCount;
            this.maxHashPartitionCount = maxHashPartitionCount;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof ExchangeNode)) {
                return node;
            }

            ExchangeNode source = (ExchangeNode) node.getSource();
            PartitioningHandle handle = source.getPartitioningScheme().getPartitioning().getHandle();

            if (source.getScope() != REMOTE || !(handle.getConnectorHandle() instanceof SystemPartitioningHandle) || handle != SCALED_WRITER_HASH_DISTRIBUTION) {
                return node;
            }

            // For TableWriterNode's sources (exchanges) there is no adaptive hash partition count. Then max-partition-hash-count is used.
            // We limit that value with maxWriterNodesCount for writing stages, and only for them.

            return new TableWriterNode(
                node.getId(),
                new ExchangeNode(
                        source.getId(),
                        source.getType(),
                        source.getScope(),
                        source.getPartitioningScheme().withPartitionCount(Math.min(maxWriterNodesCount, maxHashPartitionCount)),
                        source.getSources(),
                        source.getInputs(),
                        source.getOrderingScheme()),
                node.getTarget(),
                node.getRowCountSymbol(),
                node.getFragmentSymbol(),
                node.getColumns(),
                node.getColumnNames(),
                node.getPartitioningScheme(),
                node.getPreferredPartitioningScheme(),
                node.getStatisticsAggregation(),
                node.getStatisticsAggregationDescriptor());
        }
    }
}
