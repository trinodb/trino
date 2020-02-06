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
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_DISPATCHING;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_FAILED;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_FINISHED;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_FINISHING;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_PLANNING;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_QUEUED;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_RUNNING;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_STARTING;
import static io.prestosql.spi.tracer.TracerEventType.QUERY_STATE_CHANGE_WAITING_FOR_RESOURCES;

public enum QueryState
{
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_QUEUED;
        }
    },
    /**
     * Query is waiting for the required resources (beta).
     */
    WAITING_FOR_RESOURCES(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_WAITING_FOR_RESOURCES;
        }
    },
    /**
     * Query is being dispatched to a coordinator.
     */
    DISPATCHING(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_DISPATCHING;
        }
    },
    /**
     * Query is being planned.
     */
    PLANNING(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_PLANNING;
        }
    },
    /**
     * Query execution is being started.
     */
    STARTING(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_STARTING;
        }
    },
    /**
     * Query has at least one running task.
     */
    RUNNING(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_RUNNING;
        }
    },
    /**
     * Query is finishing (e.g. commit for autocommit queries)
     */
    FINISHING(false)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_FINISHING;
        }
    },
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED(true)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_FINISHED;
        }
    },
    /**
     * Query execution failed.
     */
    FAILED(true)
    {
        @Override
        public TracerEventType toTracerEventType()
        {
            return QUERY_STATE_CHANGE_FAILED;
        }
    };

    public static final Set<QueryState> TERMINAL_QUERY_STATES = Stream.of(QueryState.values()).filter(QueryState::isDone).collect(toImmutableSet());

    private final boolean doneState;

    QueryState(boolean doneState)
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
