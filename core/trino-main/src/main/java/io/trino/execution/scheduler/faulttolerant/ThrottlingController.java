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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.StageId;
import io.trino.metadata.InternalNode;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.execution.scheduler.faulttolerant.ExecutionThrottling.UNRESTRICTED;

public class ThrottlingController
{
    private static final int TASKS_PER_WORKER = 1;
    private final Map<StageId, Throttler<InternalNode, InternalNode>> nodeThrottler;
    private final Map<NodeStage, Throttler<Integer, InternalNode>> taskThrottler;
    private final int workers;

    public ThrottlingController(int maxWorkers)
    {
        this.nodeThrottler = new ConcurrentHashMap<>();
        this.taskThrottler = new ConcurrentHashMap<>();
        this.workers = maxWorkers;
    }

    public ExecutionThrottling createThrottling(StageId stageId, int partitionId)
    {
        if (workers == 0) {
            return UNRESTRICTED;
        }

        return new ExecutionThrottling() {
            private Optional<NodeStage> nodeStage = Optional.empty();

            @Override
            public ListenableFuture<InternalNode> throttle(InternalNode node)
            {
                Throttler<InternalNode, InternalNode> throttler = nodeThrottler.computeIfAbsent(stageId, _ -> new Throttler<>(workers));
                return Futures.transformAsync(throttler.registerTask(node, node), this::transform, directExecutor());
            }

            private ListenableFuture<InternalNode> transform(InternalNode node)
            {
                nodeStage = Optional.of(new NodeStage(node, stageId));
                return taskThrottler.computeIfAbsent(nodeStage.get(), _ -> new Throttler<>(TASKS_PER_WORKER))
                        .registerTask(partitionId, node);
            }

            @Override
            public void release()
            {
                nodeStage.ifPresent(ns -> {
                    taskThrottler.get(ns).done(partitionId);
                    nodeThrottler.get(stageId).done(ns.node());
                });
            }
        };
    }

    private record NodeStage(InternalNode node, StageId stageId)
    {}
}
