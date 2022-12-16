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
package io.trino.cost;

import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.assertj.core.util.VisibleForTesting;

import java.util.List;

import static io.trino.SystemSessionProperties.getFaultTolerantExecutionPartitionCount;
import static io.trino.SystemSessionProperties.getRemoteHashPartitionMinRowCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public final class HashPartitionCountProvider
{
    private HashPartitionCountProvider() {}

    @VisibleForTesting
    public static int getHashPartitionCount(Session session)
    {
        if (getRetryPolicy(session) == RetryPolicy.TASK) {
            return getFaultTolerantExecutionPartitionCount(session);
        }

        return SystemSessionProperties.getHashPartitionCount(session);
    }

    public static int getHashPartitionCount(Session session, PlanNode node, StatsProvider statsProvider)
    {
        requireNonNull(session, "session is null");
        requireNonNull(node, "node is null");
        requireNonNull(statsProvider, "statsProvider is null");

        if (getRetryPolicy(session) == RetryPolicy.TASK) {
            return getFaultTolerantExecutionPartitionCount(session);
        }

        return getHashPartitionCount(
                node,
                statsProvider,
                SystemSessionProperties.getHashPartitionCount(session),
                getRemoteHashPartitionMinRowCount(session));
    }

    private static int getHashPartitionCount(
            PlanNode root,
            StatsProvider statsProvider,
            int maxHashPartitionCount,
            long remoteHashPartitionMinRowCount)
    {
        if (remoteHashPartitionMinRowCount < 1) {
            return maxHashPartitionCount;
        }

        if (isExpandingNodesPresent(root)) {
            return maxHashPartitionCount;
        }

        double sourceTablesRowCount = getSourceTablesRowCount(root, statsProvider);
        double maxJoinRowCount = getMaxJoinRowCount(root, statsProvider);
        if (Double.isNaN(sourceTablesRowCount) || Double.isNaN(maxJoinRowCount)) {
            return maxHashPartitionCount;
        }

        int hashPartitionCount = max(
                getHashPartitionsFromRowCount(sourceTablesRowCount, remoteHashPartitionMinRowCount),
                getHashPartitionsFromRowCount(maxJoinRowCount, remoteHashPartitionMinRowCount));
        return min(hashPartitionCount, maxHashPartitionCount);
    }

    private static int getHashPartitionsFromRowCount(double rowCount, long remoteHashPartitionMinRowCount)
    {
        return max((int) (rowCount / remoteHashPartitionMinRowCount), 1);
    }

    private static boolean isExpandingNodesPresent(PlanNode root)
    {
        return PlanNodeSearcher.searchFrom(root, noLookup())
                .where(node -> ((node instanceof JoinNode) && ((JoinNode) node).isCrossJoin()) || (node instanceof UnnestNode))
                .matches();
    }

    private static double getMaxJoinRowCount(PlanNode root, StatsProvider statsProvider)
    {
        List<PlanNode> joinNodes = PlanNodeSearcher.searchFrom(root, noLookup())
                .whereIsInstanceOfAny(JoinNode.class)
                .findAll();

        return joinNodes.stream()
                .mapToDouble(node -> statsProvider.getStats(node).getOutputRowCount())
                .max()
                .orElse(0);
    }

    private static double getSourceTablesRowCount(PlanNode root, StatsProvider statsProvider)
    {
        List<PlanNode> sourceNodes = PlanNodeSearcher.searchFrom(root, noLookup())
                .whereIsInstanceOfAny(TableScanNode.class, ValuesNode.class)
                .findAll();

        return sourceNodes.stream()
                .mapToDouble(node -> statsProvider.getStats(node).getOutputRowCount())
                .sum();
    }
}
