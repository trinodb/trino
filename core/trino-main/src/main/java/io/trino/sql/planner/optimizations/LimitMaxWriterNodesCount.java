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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.MAX_WRITER_NODES_COUNT;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

public class LimitMaxWriterNodesCount
        implements PlanOptimizer
{
    private static final List<Class<? extends PlanNode>> SUPPORTED_NODES = ImmutableList.of(TableWriterNode.class);
    private static final List<PartitioningHandle> SUPPORTED_PARTITIONING_MODES = ImmutableList.of(
            FIXED_HASH_DISTRIBUTION, SCALED_WRITER_HASH_DISTRIBUTION, FIXED_ARBITRARY_DISTRIBUTION);

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

        // Skip for plans where there is not writing stages. Additionally, skip for FTE mode since we
        // are not using estimated partitionCount in FTE scheduler.

        if (!PlanNodeSearcher.searchFrom(plan).whereIsInstanceOfAny(SUPPORTED_NODES).matches() || getRetryPolicy(session) == RetryPolicy.TASK) {
            return plan;
        }

        // if TableWriter's source is not an exchange or partitioning is not supported does not fire that rule
        List<TableWriterNode> allTableWriters = PlanNodeSearcher
                .searchFrom(plan)
                .where(TableWriterNode.class::isInstance)
                .findAll();

        boolean isPartitioningWritingModeSupported = allTableWriters
                .stream()
                .allMatch(it -> it.getSource() instanceof ExchangeNode exchangeNode && SUPPORTED_PARTITIONING_MODES.contains(exchangeNode.getPartitioningScheme().getPartitioning().getHandle()));

        if (!isPartitioningWritingModeSupported) {
            return plan;
        }

        // if there is not-system partitioning does not file that rule
        boolean isAllPartitioningSystemPartitioning = PlanNodeSearcher
                .searchFrom(plan)
                .where(ExchangeNode.class::isInstance)
                .findAll()
                .stream()
                .map(ExchangeNode.class::cast)
                .map(node -> node.getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle())
                .allMatch(SystemPartitioningHandle.class::isInstance);

        if (!isAllPartitioningSystemPartitioning) {
            return plan;
        }

        int maxWritersNodesCount = Math.min(
                session.getSystemProperty(MAX_WRITER_NODES_COUNT, Integer.class),
                session.getSystemProperty(MAX_HASH_PARTITION_COUNT, Integer.class));
        return rewriteWith(new Rewriter(maxWritersNodesCount), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final int maxWriterNodesCount;

        private Rewriter(int maxWriterNodesCount)
        {
            this.maxWriterNodesCount = maxWriterNodesCount;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            if (node.getScope() != REMOTE) {
                return node;
            }

            List<PlanNode> sources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    node.getPartitioningScheme().withPartitionCount(maxWriterNodesCount),
                    sources,
                    node.getInputs(),
                    node.getOrderingScheme());
        }
    }
}
