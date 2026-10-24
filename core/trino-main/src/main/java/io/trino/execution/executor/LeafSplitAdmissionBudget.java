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
package io.trino.execution.executor;

import io.trino.execution.TaskId;
import io.trino.spi.QueryId;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;

// Bounds one admission round to a quarter of pool free bytes and keeps a quarter of each query's node limit free.
public class LeafSplitAdmissionBudget
{
    private long poolBytes = -1;
    private final Map<QueryId, Long> queryBytes = new HashMap<>();
    private final Map<TaskId, Long> unreservedBytes = new HashMap<>();

    // Returns the estimated bytes of one more leaf split of the task when it fits, or zero when it does not or the task has no estimate yet.
    public long admissibleEstimate(TaskId taskId, LeafSplitMemoryEstimate memoryEstimate, int runningLeafSplits, long nowNanos)
    {
        TaskMemory taskMemory = memoryEstimate.getTaskMemory();
        // leaf driver memory is sampled where admission reads the estimate
        long estimate = memoryEstimate.sample(nowNanos);
        if (taskMemory == null || estimate == 0) {
            return 0;
        }
        if (!fits(taskId, taskMemory, estimate, runningLeafSplits)) {
            return 0;
        }
        return estimate;
    }

    boolean fits(TaskId taskId, TaskMemory taskMemory, long estimate, int runningLeafSplits)
    {
        if (poolBytes < 0) {
            poolBytes = taskMemory.getPoolFreeBytes() / 4;
        }
        // a quarter of the query's node limit stays free
        long queryRemaining = queryBytes.computeIfAbsent(taskId.queryId(), _ -> taskMemory.getQueryFreeBytes() - taskMemory.getQueryMaxBytes() / 4);
        // running leaf splits below the estimate are charged once per round for the bytes they have yet to reserve
        long unreserved = unreservedBytes.computeIfAbsent(taskId, _ -> max(0, estimate * runningLeafSplits - taskMemory.getLeafPipelineBytes()));
        long cost = estimate + unreserved;
        return cost <= poolBytes && cost <= queryRemaining;
    }

    // Charges a split admitted after fits returned true for its task.
    public void reserve(TaskId taskId, long estimate)
    {
        Long unreserved = unreservedBytes.put(taskId, 0L);
        checkState(unreserved != null, "No fits check for task %s", taskId);
        long cost = estimate + unreserved;
        poolBytes -= cost;
        queryBytes.merge(taskId.queryId(), -cost, Long::sum);
    }
}
