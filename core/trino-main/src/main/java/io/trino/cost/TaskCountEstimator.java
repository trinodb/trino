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
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.operator.RetryPolicy;

import javax.inject.Inject;

import java.util.Set;
import java.util.function.IntSupplier;

import static io.trino.SystemSessionProperties.getCostEstimationWorkerCount;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionPartitionCount;
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
            if (schedulerIncludeCoordinator) {
                return activeNodes.size();
            }
            return toIntExact(activeNodes.stream()
                    .filter(node -> !node.isCoordinator())
                    .count());
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
            return costEstimationWorkerCount;
        }
        return numberOfNodes.getAsInt();
    }

    public int estimateHashedTaskCount(Session session)
    {
        int partitionCount;
        if (getRetryPolicy(session) == RetryPolicy.TASK) {
            partitionCount = getFaultTolerantExecutionPartitionCount(session);
        }
        else {
            partitionCount = getMaxHashPartitionCount(session);
        }
        return min(estimateSourceDistributedTaskCount(session), partitionCount);
    }
}
