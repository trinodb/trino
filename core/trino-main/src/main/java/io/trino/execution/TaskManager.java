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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TaskManager
{
    /**
     * Gets all of the currently tracked tasks.  This will included
     * uninitialized, running, and completed tasks.
     */
    List<TaskInfo> getAllTaskInfo();

    /**
     * Gets the info for the specified task.  If the task has not been created
     * yet, an uninitialized task is created and the info is returned.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    TaskInfo getTaskInfo(TaskId taskId);

    /**
     * Gets the status for the specified task.
     */
    TaskStatus getTaskStatus(TaskId taskId);

    /**
     * Gets future info for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the info is returned immediately.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, long currentVersion);

    /**
     * Gets the unique instance id of a task.  This can be used to detect a task
     * that was destroyed and recreated.
     */
    String getTaskInstanceId(TaskId taskId);

    /**
     * Gets future status for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the status is returned immediately.
     * <p>
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, long currentVersion);

    VersionedDynamicFilterDomains acknowledgeAndGetNewDynamicFilterDomains(TaskId taskId, long currentDynamicFiltersVersion);

    /**
     * Updates the task plan, splitAssignments and output buffers.  If the task does not
     * already exist, it is created and then updated.
     */
    TaskInfo updateTask(
            Session session,
            TaskId taskId,
            Optional<PlanFragment> fragment,
            List<SplitAssignment> splitAssignments,
            OutputBuffers outputBuffers,
            Map<DynamicFilterId, Domain> dynamicFilterDomains);

    /**
     * Cancels a task.  If the task does not already exist, it is created and then
     * canceled.
     */
    TaskInfo cancelTask(TaskId taskId);

    /**
     * Aborts a task.  If the task does not already exist, it is created and then
     * aborted.
     */
    TaskInfo abortTask(TaskId taskId);

    /**
     * Fail a task.  If the task does not already exist, it is created and then
     * failed.
     */
    TaskInfo failTask(TaskId taskId, Throwable failure);

    /**
     * Gets results from a task either immediately or in the future.  If the
     * task or buffer has not been created yet, an uninitialized task is
     * created and a future is returned.
     * <p>
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBufferId bufferId, long startingSequenceId, DataSize maxSize);

    /**
     * Acknowledges previously received results.
     */
    void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long sequenceId);

    /**
     * Aborts a result buffer for a task.  If the task or buffer has not been
     * created yet, an uninitialized task is created and a the buffer is
     * aborted.
     * <p>
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    TaskInfo destroyTaskResults(TaskId taskId, OutputBufferId bufferId);

    /**
     * Adds a state change listener to the specified task.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener);

    /**
     * Add a listener that notifies about failures of any source tasks for a given task
     */
    void addSourceTaskFailureListener(TaskId taskId, TaskFailureListener listener);

    /**
     * Return trace token for a given task (see Session#traceToken)
     */
    Optional<String> getTraceToken(TaskId taskId);
}
