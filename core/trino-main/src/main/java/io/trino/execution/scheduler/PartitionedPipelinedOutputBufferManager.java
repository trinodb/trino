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

import com.google.common.collect.ImmutableMap;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.sql.planner.PartitioningHandle;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PartitionedPipelinedOutputBufferManager
        implements PipelinedOutputBufferManager
{
    private final PipelinedOutputBuffers outputBuffers;

    public PartitionedPipelinedOutputBufferManager(PartitioningHandle partitioningHandle, int partitionCount)
    {
        checkArgument(partitionCount >= 1, "partitionCount must be at least 1");

        ImmutableMap.Builder<OutputBufferId, Integer> partitions = ImmutableMap.builder();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitions.put(new OutputBufferId(partition), partition);
        }

        outputBuffers = PipelinedOutputBuffers.createInitial(requireNonNull(partitioningHandle, "partitioningHandle is null"))
                .withBuffers(partitions.buildOrThrow())
                .withNoMoreBufferIds();
    }

    @Override
    public void addOutputBuffer(OutputBufferId newBuffer)
    {
        // All buffers are created in the constructor, so just validate that this isn't
        // a request to add a new buffer
        Integer existingBufferId = outputBuffers.getBuffers().get(newBuffer);
        if (existingBufferId == null) {
            throw new IllegalStateException("Unexpected new output buffer " + newBuffer);
        }
        if (newBuffer.getId() != existingBufferId) {
            throw new IllegalStateException("newOutputBuffers has changed the assignment for task " + newBuffer);
        }
    }

    @Override
    public void noMoreBuffers() {}

    @Override
    public PipelinedOutputBuffers getOutputBuffers()
    {
        return outputBuffers;
    }
}
