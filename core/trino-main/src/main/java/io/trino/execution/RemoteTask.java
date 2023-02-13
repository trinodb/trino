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
package io.trino.execution;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

public interface RemoteTask
{
    TaskId getTaskId();

    String getNodeId();

    TaskInfo getTaskInfo();

    TaskStatus getTaskStatus();

    void start();

    void addSplits(Multimap<PlanNodeId, Split> splits);

    void noMoreSplits(PlanNodeId sourceId);

    void setOutputBuffers(OutputBuffers outputBuffers);

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener);

    /**
     * Add a listener for the final task info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener);

    ListenableFuture<Void> whenSplitQueueHasSpace(long weightThreshold);

    void cancel();

    void abort();

    PartitionedSplitsInfo getPartitionedSplitsInfo();

    /**
     * Fails task from the coordinator perspective immediately, without waiting for acknowledgement from the remote task
     */
    void failLocallyImmediately(Throwable cause);

    /**
     * Fails task remotely; only transitions to failed state when we receive confirmation that remote operation is completed
     */
    void failRemotely(Throwable cause);

    PartitionedSplitsInfo getQueuedPartitionedSplitsInfo();

    int getUnacknowledgedPartitionedSplitCount();

    /**
     * Retrieves spooling output stats. Stats are available only for tasks that are in the {@link TaskState#FINISHED} state
     * and have received the final task info (see {@link #addFinalTaskInfoListener(StateChangeListener)}.
     * Stats can be retrieved only once and are discarded upon retrieval. Subsequent calls to the method after initial retrieval will fail.
     * <p>
     * The retrieve-and-drop semantics is necessary to avoid unnecessary memory overhead of keeping the {@link SpoolingOutputStats.Snapshot}
     * for every task as usually only the total output of the entire stage is of interest.
     *
     * @return spooling output statistics
     */
    SpoolingOutputStats.Snapshot retrieveAndDropSpoolingOutputStats();
}
