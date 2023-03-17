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
package io.trino.execution.scheduler;

import com.google.common.collect.Multimap;
import io.opentelemetry.api.trace.Span;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public interface StageExecution
{
    StageId getStageId();

    int getAttemptId();

    Span getStageSpan();

    PlanFragment getFragment();

    boolean isAnyTaskBlocked();

    void beginScheduling();

    void transitionToSchedulingSplits();

    State getState();

    void addStateChangeListener(StateChangeListener<State> stateChangeListener);

    TaskLifecycleListener getTaskLifecycleListener();

    void schedulingComplete();

    void schedulingComplete(PlanNodeId partitionedSource);

    void cancel();

    void abort();

    void recordGetSplitTime(long start);

    Optional<RemoteTask> scheduleTask(
            InternalNode node,
            int partition,
            Multimap<PlanNodeId, Split> initialSplits);

    void failTask(TaskId taskId, Throwable failureCause);

    List<RemoteTask> getAllTasks();

    List<TaskStatus> getTaskStatuses();

    Optional<ExecutionFailureInfo> getFailureCause();

    enum State
    {
        /**
         * Stage is planned but has not been scheduled yet.  A stage will
         * be in the planned state until, the dependencies of the stage
         * have begun producing output.
         */
        PLANNED(false, false),
        /**
         * Stage tasks are being scheduled on nodes.
         */
        SCHEDULING(false, false),
        /**
         * All stage tasks have been scheduled, but splits are still being scheduled.
         */
        SCHEDULING_SPLITS(false, false),
        /**
         * Stage has been scheduled on nodes and ready to execute, but all tasks are still queued.
         */
        SCHEDULED(false, false),
        /**
         * Stage is running.
         */
        RUNNING(false, false),
        /**
         * Stage has finished executing and output being consumed.
         * In this state, at-least one of the tasks is flushing and the non-flushing tasks are finished
         */
        FLUSHING(false, false),
        /**
         * Stage has finished executing and all output has been consumed.
         */
        FINISHED(true, false),
        /**
         * Stage was canceled by a user.
         */
        CANCELED(true, false),
        /**
         * Stage was aborted due to a failure in the query.  The failure
         * was not in this stage.
         */
        ABORTED(true, true),
        /**
         * Stage execution failed.
         */
        FAILED(true, true);

        private final boolean doneState;
        private final boolean failureState;

        State(boolean doneState, boolean failureState)
        {
            checkArgument(!failureState || doneState, "%s is a non-done failure state", name());
            this.doneState = doneState;
            this.failureState = failureState;
        }

        /**
         * Is this a terminal state.
         */
        public boolean isDone()
        {
            return doneState;
        }

        /**
         * Is this a non-success terminal state.
         */
        public boolean isFailure()
        {
            return failureState;
        }

        public boolean canScheduleMoreTasks()
        {
            switch (this) {
                case PLANNED:
                case SCHEDULING:
                    // workers are still being added to the query
                    return true;
                case SCHEDULING_SPLITS:
                case SCHEDULED:
                case RUNNING:
                case FLUSHING:
                case FINISHED:
                case CANCELED:
                    // no more workers will be added to the query
                    return false;
                case ABORTED:
                case FAILED:
                    // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                    // stage above to finish normally, which will result in a query
                    // completing successfully when it should fail..
                    return true;
            }
            throw new IllegalStateException("Unhandled state: " + this);
        }
    }
}
