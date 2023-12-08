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

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.operator.RetryPolicy;

import java.util.Set;
import java.util.function.IntSupplier;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.getCostEstimationWorkerCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionMaxPartitionCount;
import static io.trino.SystemSessionProperties.getMaxHashPartitionCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TaskCountEstimator
{
    private final IntSupplier numberOfNodes;

    @Inject
    public TaskCountEstimator(NodeSchedulerConfig nodeSchedulerConfig, InternalNodeManager nodeManager)
    {
        boolean schedulerIncludeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();
        requireNonNull(nodeManager, "nodeManager is null");
        this.numberOfNodes = () -> {
            Set<InternalNode> activeNodes = nodeManager.getAllNodes().getActiveNodes();
            int count;
            if (schedulerIncludeCoordinator) {
                count = activeNodes.size();
            }
            else {
                count = toIntExact(activeNodes.stream()
                        .filter(node -> !node.isCoordinator())
                        .count());
            }
            // At least 1 even if no worker nodes currently registered. This is to prevent underflow or other mis-estimations.
            return Math.max(count, 1);
        };
    }

    public TaskCountEstimator(IntSupplier numberOfNodes)
    {
        this.numberOfNodes = requireNonNull(numberOfNodes, "numberOfNodes is null");
    }

    public int estimateSourceDistributedTaskCount(Session session)
    {
        Integer costEstimationWorkerCount = getCostEstimationWorkerCount(session);
        if (costEstimationWorkerCount != null) {
            // validated to be at least 1
            return costEstimationWorkerCount;
        }
        int count = numberOfNodes.getAsInt();
        checkState(count > 0, "%s should return positive number of nodes: %s", numberOfNodes, count);
        return count;
    }

    public int estimateHashedTaskCount(Session session)
    {
        int partitionCount;
        if (getRetryPolicy(session) == RetryPolicy.TASK) {
            partitionCount = getFaultTolerantExecutionMaxPartitionCount(session);
        }
        else {
            partitionCount = getMaxHashPartitionCount(session);
        }
        return min(estimateSourceDistributedTaskCount(session), partitionCount);
    }
}
