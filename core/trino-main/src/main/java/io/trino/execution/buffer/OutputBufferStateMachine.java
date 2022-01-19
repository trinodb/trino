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
package io.trino.execution.buffer;

import io.trino.execution.StateMachine;
import io.trino.execution.TaskId;

import java.util.concurrent.Executor;

import static io.trino.execution.buffer.BufferState.ABORTED;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;

public class OutputBufferStateMachine
{
    private final StateMachine<BufferState> state;

    public OutputBufferStateMachine(TaskId taskId, Executor executor)
    {
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN, TERMINAL_BUFFER_STATES);
    }

    public BufferState getState()
    {
        return state.get();
    }

    public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    public boolean noMoreBuffers()
    {
        if (state.compareAndSet(OPEN, NO_MORE_BUFFERS)) {
            return true;
        }
        return state.compareAndSet(NO_MORE_PAGES, FLUSHING);
    }

    public boolean noMorePages()
    {
        if (state.compareAndSet(OPEN, NO_MORE_PAGES)) {
            return true;
        }
        return state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
    }

    public boolean finish()
    {
        return state.setIf(FINISHED, oldState -> !oldState.isTerminal());
    }

    public boolean abort()
    {
        return state.setIf(ABORTED, oldState -> !oldState.isTerminal());
    }
}
