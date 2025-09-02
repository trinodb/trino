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
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.cost.TaskCountEstimator;
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMinPartitionCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMinPartitionCountForWrite;
import static io.trino.SystemSessionProperties.getMaxHashPartitionCount;
import static io.trino.SystemSessionProperties.getMinHashPartitionCount;
import static io.trino.SystemSessionProperties.getMinHashPartitionCountForWrite;
import static io.trino.SystemSessionProperties.getMinInputRowsPerTask;
import static io.trino.SystemSessionProperties.getMinInputSizePerTask;
import static io.trino.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isDeterminePartitionCountForWriteEnabled;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * This rule looks at the amount of data read and processed by the query to determine the value of partition count
 * used for remote partitioned exchanges. It helps to increase the concurrency of the engine in the case of large cluster.
 * This rule is also cautious about lack of or incorrect statistics therefore it skips for input multiplying nodes like
 * CROSS JOIN or UNNEST.
 * <p>
 * E.g. 1:
 * Given query: SELECT count(column_a) FROM table_with_stats_a group by column_b
 * config:
 * MIN_INPUT_SIZE_PER_TASK: 500 MB
 * Input table data size: 1000 MB
 * Estimated partition count: Input table data size / MIN_INPUT_SIZE_PER_TASK => 2
 * <p>
 * E.g. 2:
 * Given query: SELECT * FROM table_with_stats_a as a JOIN table_with_stats_b as b ON a.column_b = b.column_b
 * config:
 * MIN_INPUT_SIZE_PER_TASK: 500 MB
 * Input tables data size: 1000 MB
 * Join output data size: 5000 MB
 * Estimated partition count: max((Input table data size / MIN_INPUT_SIZE_PER_TASK), (Join output data size / MIN_INPUT_SIZE_PER_TASK))  => 10
 */
public class DeterminePartitionCount
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(DeterminePartitionCount.class);
    private static final List<Class<? extends PlanNode>> INSERT_NODES = ImmutableList.of(TableExecuteNode.class, TableWriterNode.class, MergeWriterNode.class);

    private final StatsCalculator statsCalculator;
    private final TaskCountEstimator taskCountEstimator;

    public DeterminePartitionCount(StatsCalculator statsCalculator, TaskCountEstimator taskCountEstimator)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        requireNonNull(plan, "plan is null");

        // Skip partition count determination if no partitioned remote exchanges exist in the plan anyway
        boolean taskRetries = getRetryPolicy(context.session()).equals(RetryPolicy.TASK);
        if (!isEligibleRemoteExchangePresent(plan, taskRetries)) {
            return plan;
        }

        // Unless enabled, skip for write nodes since writing partitioned data with small amount of nodes could cause
        // memory related issues even when the amount of data is small.
        boolean isWriteQuery = PlanNodeSearcher.searchFrom(plan).whereIsInstanceOfAny(INSERT_NODES).matches();
        if (isWriteQuery && !isDeterminePartitionCountForWriteEnabled(context.session())) {
            return plan;
        }

        return determinePartitionCount(plan, context.session(), context.tableStatsProvider(), isWriteQuery)
                .map(partitionCount -> rewriteWith(new Rewriter(partitionCount, taskRetries), plan))
                .orElse(plan);
    }

    private Optional<Integer> determinePartitionCount(
            PlanNode plan,
            Session session,
            TableStatsProvider tableStatsProvider,
            boolean isWriteQuery)
    {
        long minInputSizePerTask = getMinInputSizePerTask(session).toBytes();
        long minInputRowsPerTask = getMinInputRowsPerTask(session);
        if (minInputSizePerTask == 0 || minInputRowsPerTask == 0) {
            return Optional.empty();
        }

        // Skip for expanding plan nodes like CROSS JOIN or UNNEST which can substantially increase the amount of data.
        if (isInputMultiplyingPlanNodePresent(plan)) {
            return Optional.empty();
        }

        int minPartitionCount;
        int maxPartitionCount;
        if (getRetryPolicy(session).equals(RetryPolicy.TASK)) {
            if (isWriteQuery) {
                minPartitionCount = getFaultTolerantExecutionMinPartitionCountForWrite(session);
            }
            else {
                minPartitionCount = getFaultTolerantExecutionMinPartitionCount(session);
            }
            maxPartitionCount = getFaultTolerantExecutionMaxPartitionCount(session);
        }
        else {
            if (isWriteQuery) {
                minPartitionCount = getMinHashPartitionCountForWrite(session);
            }
            else {
                minPartitionCount = getMinHashPartitionCount(session);
            }
            maxPartitionCount = getMaxHashPartitionCount(session);
        }
        verify(minPartitionCount <= maxPartitionCount, "minPartitionCount %s larger than maxPartitionCount %s",
                minPartitionCount, maxPartitionCount);
        int maxPossiblePartitionCount = taskCountEstimator.estimateHashedTaskCount(session);
        RetryPolicy retryPolicy = getRetryPolicy(session);
        if (maxPossiblePartitionCount <= 2 * minPartitionCount && !retryPolicy.equals(RetryPolicy.TASK)) {
            // Do not set partition count if the possible partition count is already close to the minimum partition count.
            // This avoids incurring cost of fetching table statistics for simple queries when the cluster is small.
            return Optional.empty();
        }

        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, tableStatsProvider);
        long queryMaxMemoryPerNode = getQueryMaxMemoryPerNode(session).toBytes();

        // Calculate partition count based on nodes output data size and rows
        Optional<Integer> partitionCountBasedOnOutputSize = getPartitionCountBasedOnOutputSize(
                plan,
                statsProvider,
                minInputSizePerTask,
                queryMaxMemoryPerNode);
        Optional<Integer> partitionCountBasedOnRows = getPartitionCountBasedOnRows(plan, statsProvider, minInputRowsPerTask);

        if (partitionCountBasedOnOutputSize.isEmpty() || partitionCountBasedOnRows.isEmpty()) {
            return Optional.empty();
        }

        int partitionCount = max(
                // Consider both output size and rows count to estimate the value of partition count. This is essential
                // because huge number of small size rows can be cpu intensive for some operators. On the other
                // hand, small number of rows with considerable size in bytes can be memory intensive.
                max(partitionCountBasedOnOutputSize.get(), partitionCountBasedOnRows.get()),
                minPartitionCount);

        if (partitionCount >= maxPartitionCount) {
            return Optional.empty();
        }

        if (partitionCount * 2 >= maxPossiblePartitionCount && !retryPolicy.equals(RetryPolicy.TASK)) {
            // Do not cap partition count if it's already close to the possible number of tasks.
            return Optional.empty();
        }

        log.debug("Estimated remote exchange partition count for query %s is %s", session.getQueryId(), partitionCount);
        return Optional.of(partitionCount);
    }

    private static Optional<Integer> getPartitionCountBasedOnOutputSize(
            PlanNode plan,
            StatsProvider statsProvider,
            long minInputSizePerTask,
            long queryMaxMemoryPerNode)
    {
        double sourceTablesOutputSize = getSourceNodesOutputStats(
                plan,
                node -> statsProvider.getStats(node).getOutputSizeInBytes(node.getOutputSymbols()));
        double expandingNodesMaxOutputSize = getExpandingNodesMaxOutputStats(
                plan,
                node -> statsProvider.getStats(node).getOutputSizeInBytes(node.getOutputSymbols()));
        if (isNaN(sourceTablesOutputSize) || isNaN(expandingNodesMaxOutputSize)) {
            return Optional.empty();
        }
        int partitionCountBasedOnOutputSize = getPartitionCount(
                max(sourceTablesOutputSize, expandingNodesMaxOutputSize), minInputSizePerTask);

        // Calculate partition count based on maximum memory usage. This is based on the assumption that
        // generally operators won't keep data in memory more than the size of input data.
        int partitionCountBasedOnMemory = (int) ((max(sourceTablesOutputSize, expandingNodesMaxOutputSize) * 2) / queryMaxMemoryPerNode);

        return Optional.of(max(partitionCountBasedOnOutputSize, partitionCountBasedOnMemory));
    }

    private static Optional<Integer> getPartitionCountBasedOnRows(PlanNode plan, StatsProvider statsProvider, long minInputRowsPerTask)
    {
        double sourceTablesRowCount = getSourceNodesOutputStats(plan, node -> statsProvider.getStats(node).getOutputRowCount());
        double expandingNodesMaxRowCount = getExpandingNodesMaxOutputStats(plan, node -> statsProvider.getStats(node).getOutputRowCount());
        if (isNaN(sourceTablesRowCount) || isNaN(expandingNodesMaxRowCount)) {
            return Optional.empty();
        }

        return Optional.of(getPartitionCount(
                max(sourceTablesRowCount, expandingNodesMaxRowCount), minInputRowsPerTask));
    }

    private static int getPartitionCount(double outputStats, long minInputStatsPerTask)
    {
        return max((int) (outputStats / minInputStatsPerTask), 1);
    }

    private static boolean isInputMultiplyingPlanNodePresent(PlanNode root)
    {
        return PlanNodeSearcher.searchFrom(root)
                .where(DeterminePartitionCount::isInputMultiplyingPlanNode)
                .matches();
    }

    private static boolean isInputMultiplyingPlanNode(PlanNode node)
    {
        if (node instanceof UnnestNode) {
            return true;
        }

        if (node instanceof JoinNode joinNode) {
            // Skip for cross join
            if (joinNode.isCrossJoin()) {
                // If any of the input node is scalar then there's no need to skip cross join
                return !isAtMostScalar(joinNode.getRight()) && !isAtMostScalar(joinNode.getLeft());
            }

            // Skip for joins with multi keys since output row count stats estimation can wrong due to
            // low correlation between multiple join keys.
            return joinNode.getCriteria().size() > 1;
        }

        return false;
    }

    private static double getExpandingNodesMaxOutputStats(PlanNode root, ToDoubleFunction<PlanNode> statsMapper)
    {
        List<PlanNode> expandingNodes = PlanNodeSearcher.searchFrom(root)
                .where(DeterminePartitionCount::isExpandingPlanNode)
                .findAll();

        return expandingNodes.stream()
                .mapToDouble(statsMapper)
                .max()
                .orElse(0);
    }

    private static boolean isExpandingPlanNode(PlanNode node)
    {
        return node instanceof JoinNode
                // consider union node and exchange node with multiple sources as expanding since it merge the rows
                // from two different sources, thus more data is transferred over the network.
                || node instanceof UnionNode
                || (node instanceof ExchangeNode && node.getSources().size() > 1);
    }

    private static double getSourceNodesOutputStats(PlanNode root, ToDoubleFunction<PlanNode> statsMapper)
    {
        List<PlanNode> sourceNodes = PlanNodeSearcher.searchFrom(root)
                .whereIsInstanceOfAny(TableScanNode.class, ValuesNode.class)
                .findAll();

        return sourceNodes.stream()
                .mapToDouble(statsMapper)
                .sum();
    }

    private static boolean isEligibleRemoteExchangePresent(PlanNode root, boolean taskRetries)
    {
        return PlanNodeSearcher.searchFrom(root)
                .where(node -> node instanceof ExchangeNode exchangeNode && isEligibleRemoteExchange(exchangeNode, taskRetries))
                .matches();
    }

    private static boolean isEligibleRemoteExchange(ExchangeNode exchangeNode, boolean taskRetries)
    {
        if (exchangeNode.getScope() != REMOTE || exchangeNode.getType() != REPARTITION) {
            return false;
        }

        PartitioningHandle partitioningHandle = exchangeNode.getPartitioningScheme().getPartitioning().getHandle();

        return !partitioningHandle.isScaleWriters()
                && !partitioningHandle.isSingleNode()
                && partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle
                && (!taskRetries || partitioningHandle == FIXED_HASH_DISTRIBUTION); // for FTE it only makes sense to set partition count fot hash partitioned fragments
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final int partitionCount;
        private final boolean taskRetries;

        private Rewriter(int partitionCount, boolean taskRetries)
        {
            this.partitionCount = partitionCount;
            this.taskRetries = taskRetries;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            List<PlanNode> sources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());

            PartitioningScheme partitioningScheme = node.getPartitioningScheme();
            if (isEligibleRemoteExchange(node, taskRetries)) {
                partitioningScheme = partitioningScheme.withPartitionCount(Optional.of(partitionCount));
            }

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    sources,
                    node.getInputs(),
                    node.getOrderingScheme());
        }
    }
}
