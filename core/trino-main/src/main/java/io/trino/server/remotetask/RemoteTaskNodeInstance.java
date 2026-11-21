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
package io.trino.server.remotetask;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.execution.TaskId;

import java.net.ConnectException;
import java.util.Optional;

import static com.google.common.base.Throwables.getCausalChain;
import static java.util.Objects.requireNonNull;

/**
 * Tracks the instance of the server process a remote task is addressed to.
 *
 * <p>The coordinator learns about restarts of a worker with a delay, so it may create a task on an instance which
 * is already gone. The worker rejects requests addressed to another instance. If no task update could have reached
 * the instance the task was addressed to, the task did not run anywhere, and it is redirected to the current instance.
 * Otherwise, the task is lost.
 */
@ThreadSafe
class RemoteTaskNodeInstance
{
    private static final Logger log = Logger.get(RemoteTaskNodeInstance.class);

    private final TaskId taskId;

    @GuardedBy("this")
    private String nodeInstanceId;
    @GuardedBy("this")
    private boolean taskUpdatePossiblyDelivered;

    public RemoteTaskNodeInstance(TaskId taskId, String nodeInstanceId)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.nodeInstanceId = requireNonNull(nodeInstanceId, "nodeInstanceId is null");
    }

    public synchronized String nodeInstanceId()
    {
        return nodeInstanceId;
    }

    public synchronized void taskUpdateSucceeded()
    {
        taskUpdatePossiblyDelivered = true;
    }

    public synchronized void taskUpdateFailed(Throwable cause)
    {
        // A request which could not connect or was rejected by another instance did not reach the instance the task is addressed to
        boolean notDelivered = cause instanceof NodeInstanceMismatchException ||
                getCausalChain(cause).stream().anyMatch(ConnectException.class::isInstance);
        if (!notDelivered) {
            taskUpdatePossiblyDelivered = true;
        }
    }

    /**
     * Handles a task update rejected because it was addressed to another instance.
     * There is at most one task update in flight, so the outcome of all previous task updates is known.
     *
     * @return whether the task was redirected to the current instance
     */
    public synchronized boolean redirectTaskUpdate(String rejectedNodeInstanceId, Optional<String> actualNodeInstanceId)
    {
        if (!rejectedNodeInstanceId.equals(nodeInstanceId)) {
            // should not happen, as a task update is not sent before the previous one completes
            return true;
        }
        if (taskUpdatePossiblyDelivered || actualNodeInstanceId.isEmpty()) {
            return false;
        }
        log.info("Task %s was not delivered to node instance %s, which is gone. Redirecting it to node instance %s", taskId, nodeInstanceId, actualNodeInstanceId.get());
        nodeInstanceId = actualNodeInstanceId.get();
        return true;
    }

    /**
     * Handles a request other than a task update rejected because it was addressed to another instance.
     *
     * @return whether the request should be retried, as the task is not known to be lost
     */
    public synchronized boolean canRetryRejectedRequest(String rejectedNodeInstanceId)
    {
        if (!rejectedNodeInstanceId.equals(nodeInstanceId)) {
            // the task was redirected since the request was sent
            return true;
        }
        // Whether the task can be redirected is decided when a task update is rejected. The task updates are retried until they succeed or the task fails.
        return !taskUpdatePossiblyDelivered;
    }
}
