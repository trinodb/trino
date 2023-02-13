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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.trino.execution.StateMachine.StateChangeListener;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.execution.TaskState.ABORTED;
import static io.trino.execution.TaskState.ABORTING;
import static io.trino.execution.TaskState.CANCELED;
import static io.trino.execution.TaskState.CANCELING;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.execution.TaskState.FAILING;
import static io.trino.execution.TaskState.FINISHED;
import static io.trino.execution.TaskState.FLUSHING;
import static io.trino.execution.TaskState.RUNNING;
import static io.trino.execution.TaskState.TERMINAL_TASK_STATES;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TaskStateMachine
{
    private static final Logger log = Logger.get(TaskStateMachine.class);

    private final DateTime createdTime = DateTime.now();

    private final TaskId taskId;
    private final Executor executor;
    private final StateMachine<TaskState> taskState;
    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    @GuardedBy("this")
    private final Map<TaskId, Throwable> sourceTaskFailures = new HashMap<>();
    @GuardedBy("this")
    private final List<TaskFailureListener> sourceTaskFailureListeners = new ArrayList<>();

    public TaskStateMachine(TaskId taskId, Executor executor)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.executor = requireNonNull(executor, "executor is null");
        taskState = new StateMachine<>("task " + taskId, executor, RUNNING, TERMINAL_TASK_STATES);
        taskState.addStateChangeListener(newState -> log.debug("Task %s is %s", taskId, newState));
    }

    public DateTime getCreatedTime()
    {
        return createdTime;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskState getState()
    {
        return taskState.get();
    }

    public ListenableFuture<TaskState> getStateChange(TaskState currentState)
    {
        requireNonNull(currentState, "currentState is null");
        checkArgument(!currentState.isDone(), "Current state is already done");

        ListenableFuture<TaskState> future = taskState.getStateChange(currentState);
        TaskState state = taskState.get();
        if (state.isDone()) {
            return immediateFuture(state);
        }
        return future;
    }

    public LinkedBlockingQueue<Throwable> getFailureCauses()
    {
        return failureCauses;
    }

    public void transitionToFlushing()
    {
        taskState.setIf(FLUSHING, currentState -> currentState == RUNNING);
    }

    public void finished()
    {
        taskState.setIf(FINISHED, currentState -> !currentState.isTerminatingOrDone());
    }

    public void cancel()
    {
        startTermination(CANCELING);
    }

    public void abort()
    {
        startTermination(ABORTING);
    }

    public void failed(Throwable cause)
    {
        failureCauses.add(cause);
        startTermination(FAILING);
    }

    public void terminationComplete()
    {
        TaskState currentState = taskState.get();
        if (currentState.isDone()) {
            return; // ignore redundant completion events
        }
        checkState(currentState.isTerminating(), "current state %s is not a terminating state", currentState);
        TaskState newState = switch (currentState) {
            case CANCELING -> CANCELED;
            case ABORTING -> ABORTED;
            case FAILING -> FAILED;
            default -> throw new IllegalStateException("Unhandled terminating state: " + currentState);
        };
        taskState.compareAndSet(currentState, newState);
    }

    private void startTermination(TaskState terminatingState)
    {
        requireNonNull(terminatingState, "terminatingState is null");
        checkArgument(terminatingState.isTerminating(), "terminatingState %s is not a terminating state", terminatingState);

        taskState.setIf(terminatingState, currentState -> !currentState.isTerminatingOrDone());
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskState.addStateChangeListener(stateChangeListener);
    }

    public void addSourceTaskFailureListener(TaskFailureListener listener)
    {
        Map<TaskId, Throwable> failures;
        synchronized (this) {
            sourceTaskFailureListeners.add(listener);
            failures = ImmutableMap.copyOf(sourceTaskFailures);
        }
        executor.execute(() -> {
            failures.forEach(listener::onTaskFailed);
        });
    }

    public void sourceTaskFailed(TaskId taskId, Throwable failure)
    {
        List<TaskFailureListener> listeners;
        synchronized (this) {
            sourceTaskFailures.putIfAbsent(taskId, failure);
            listeners = ImmutableList.copyOf(sourceTaskFailureListeners);
        }
        executor.execute(() -> {
            for (TaskFailureListener listener : listeners) {
                listener.onTaskFailed(taskId, failure);
            }
        });
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("taskState", taskState)
                .add("failureCauses", failureCauses)
                .toString();
    }
}
