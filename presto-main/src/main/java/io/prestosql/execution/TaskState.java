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
package io.prestosql.execution;

import io.prestosql.spi.tracer.TracerEventType;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_ABORTED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_CANCELED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_FAILED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_FINISHED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_PLANNED;
import static io.prestosql.spi.tracer.TracerEventType.TASK_STATE_CHANGE_RUNNING;

public enum TaskState
{
    /**
     * Task is planned but has not been scheduled yet.  A task will
     * be in the planned state until, the dependencies of the task
     * have begun producing output.
     */
    PLANNED(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return TASK_STATE_CHANGE_PLANNED;
        }
    },
    /**
     * Task is running.
     */
    RUNNING(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return TASK_STATE_CHANGE_RUNNING;
        }
    },
    /**
     * Task has finished executing and all output has been consumed.
     */
    FINISHED(true)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return TASK_STATE_CHANGE_FINISHED;
        }
    },
    /**
     * Task was canceled by a user.
     */
    CANCELED(true)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return TASK_STATE_CHANGE_CANCELED;
        }
    },
    /**
     * Task was aborted due to a failure in the query.  The failure
     * was not in this task.
     */
    ABORTED(true)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return TASK_STATE_CHANGE_ABORTED;
        }
    },
    /**
     * Task execution failed.
     */
    FAILED(true)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return TASK_STATE_CHANGE_FAILED;
        }
    };

    public static final Set<TaskState> TERMINAL_TASK_STATES = Stream.of(TaskState.values()).filter(TaskState::isDone).collect(toImmutableSet());

    private final boolean doneState;

    TaskState(boolean doneState)
    {
        this.doneState = doneState;
    }

    public TracerEventType toTracerEventType()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }
}
