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

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;

import static io.trino.execution.buffer.PipelinedOutputBuffers.BROADCAST_PARTITION_ID;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.BROADCAST;

@ThreadSafe
class BroadcastPipelinedOutputBufferManager
        implements PipelinedOutputBufferManager
{
    @GuardedBy("this")
    private PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(BROADCAST);

    @Override
    public synchronized void addOutputBuffer(OutputBufferId newBuffer)
    {
        if (outputBuffers.isNoMoreBufferIds()) {
            // a stage can move to a final state (e.g., failed) while scheduling, so ignore
            // the new buffers
            return;
        }

        // Note: it does not matter which partition id the task is using, in broadcast all tasks read from the same partition
        PipelinedOutputBuffers newOutputBuffers = outputBuffers.withBuffer(newBuffer, BROADCAST_PARTITION_ID);

        // don't update if nothing changed
        if (newOutputBuffers != outputBuffers) {
            this.outputBuffers = newOutputBuffers;
        }
    }

    @Override
    public synchronized void noMoreBuffers()
    {
        if (!outputBuffers.isNoMoreBufferIds()) {
            outputBuffers = outputBuffers.withNoMoreBufferIds();
        }
    }

    @Override
    public synchronized PipelinedOutputBuffers getOutputBuffers()
    {
        return outputBuffers;
    }
}
