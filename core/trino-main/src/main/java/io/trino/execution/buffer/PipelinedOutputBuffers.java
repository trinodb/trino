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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.PartitioningHandle;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.ARBITRARY;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.BROADCAST;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public class PipelinedOutputBuffers
        extends OutputBuffers
{
    public static final int BROADCAST_PARTITION_ID = 0;

    private final BufferType type;
    private final Map<OutputBufferId, Integer> buffers;
    private final boolean noMoreBufferIds;

    public static PipelinedOutputBuffers createInitial(BufferType type)
    {
        return new PipelinedOutputBuffers(0, type, ImmutableMap.of(), false);
    }

    public static PipelinedOutputBuffers createInitial(PartitioningHandle partitioningHandle)
    {
        BufferType type;
        if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            type = BROADCAST;
        }
        else if (partitioningHandle.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            type = ARBITRARY;
        }
        else {
            type = PARTITIONED;
        }
        return new PipelinedOutputBuffers(0, type, ImmutableMap.of(), false);
    }

    // Visible only for Jackson... Use the "with" methods instead
    @JsonCreator
    public PipelinedOutputBuffers(
            @JsonProperty("version") long version,
            @JsonProperty("type") BufferType type,
            @JsonProperty("buffers") Map<OutputBufferId, Integer> buffers,
            @JsonProperty("noMoreBufferIds") boolean noMoreBufferIds)
    {
        super(version);
        this.type = requireNonNull(type, "type is null");
        this.buffers = ImmutableMap.copyOf(requireNonNull(buffers, "buffers is null"));
        this.noMoreBufferIds = noMoreBufferIds;
    }

    @JsonProperty
    public BufferType getType()
    {
        return type;
    }

    @JsonProperty
    public Map<OutputBufferId, Integer> getBuffers()
    {
        return buffers;
    }

    @JsonProperty
    public boolean isNoMoreBufferIds()
    {
        return noMoreBufferIds;
    }

    @Override
    public void checkValidTransition(OutputBuffers outputBuffers)
    {
        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers instanceof PipelinedOutputBuffers, "outputBuffers is expected to be an instance of PipelinedOutputBuffers");

        PipelinedOutputBuffers newOutputBuffers = (PipelinedOutputBuffers) outputBuffers;
        checkState(type == newOutputBuffers.getType(), "newOutputBuffers has a different type %s, expected %s", newOutputBuffers.getType(), type);

        if (noMoreBufferIds) {
            checkArgument(this.equals(newOutputBuffers), "Expected buffer to not change after no more buffers is set");
            return;
        }

        if (getVersion() > newOutputBuffers.getVersion()) {
            throw new IllegalArgumentException("newOutputBuffers version is older");
        }

        if (getVersion() == newOutputBuffers.getVersion()) {
            checkArgument(this.equals(newOutputBuffers), "newOutputBuffers is the same version but contains different information");
        }

        // assure we have not changed the buffer assignments
        for (Map.Entry<OutputBufferId, Integer> entry : buffers.entrySet()) {
            if (!entry.getValue().equals(newOutputBuffers.getBuffers().get(entry.getKey()))) {
                throw new IllegalArgumentException("newOutputBuffers has changed the assignment for task " + entry.getKey());
            }
        }
    }

    public PipelinedOutputBuffers withBuffer(OutputBufferId bufferId, int partition)
    {
        requireNonNull(bufferId, "bufferId is null");

        if (buffers.containsKey(bufferId)) {
            checkHasBuffer(bufferId, partition);
            return this;
        }

        // verify no new buffers is not set
        checkState(!noMoreBufferIds, "No more buffer ids already set");

        return new PipelinedOutputBuffers(
                getVersion() + 1,
                type,
                ImmutableMap.<OutputBufferId, Integer>builder()
                        .putAll(buffers)
                        .put(bufferId, partition)
                        .buildOrThrow(),
                false);
    }

    public PipelinedOutputBuffers withBuffers(Map<OutputBufferId, Integer> buffers)
    {
        requireNonNull(buffers, "buffers is null");

        Map<OutputBufferId, Integer> newBuffers = new HashMap<>();
        for (Map.Entry<OutputBufferId, Integer> entry : buffers.entrySet()) {
            OutputBufferId bufferId = entry.getKey();
            int partition = entry.getValue();

            // it is ok to have a duplicate buffer declaration but it must have the same page partition
            if (this.buffers.containsKey(bufferId)) {
                checkHasBuffer(bufferId, partition);
                continue;
            }

            newBuffers.put(bufferId, partition);
        }

        // if we don't have new buffers, don't update
        if (newBuffers.isEmpty()) {
            return this;
        }

        // verify no new buffers is not set
        checkState(!noMoreBufferIds, "No more buffer ids already set");

        // add the existing buffers
        newBuffers.putAll(this.buffers);

        return new PipelinedOutputBuffers(getVersion() + 1, type, newBuffers, false);
    }

    public PipelinedOutputBuffers withNoMoreBufferIds()
    {
        if (noMoreBufferIds) {
            return this;
        }

        return new PipelinedOutputBuffers(getVersion() + 1, type, buffers, true);
    }

    private void checkHasBuffer(OutputBufferId bufferId, int partition)
    {
        checkArgument(
                Objects.equals(buffers.get(bufferId), partition),
                "OutputBuffers already contains task %s, but partition is set to %s not %s",
                bufferId,
                buffers.get(bufferId),
                partition);
    }

    public enum BufferType
    {
        PARTITIONED,
        BROADCAST,
        ARBITRARY,
    }

    public static class OutputBufferId
    {
        // this is needed by JAX-RS
        public static OutputBufferId fromString(String id)
        {
            return new OutputBufferId(parseInt(id));
        }

        private final int id;

        @JsonCreator
        public OutputBufferId(int id)
        {
            checkArgument(id >= 0, "id is negative");
            this.id = id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OutputBufferId that = (OutputBufferId) o;
            return id == that.id;
        }

        @JsonValue
        public int getId()
        {
            return id;
        }

        @Override
        public int hashCode()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return String.valueOf(id);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipelinedOutputBuffers that = (PipelinedOutputBuffers) o;
        return getVersion() == that.getVersion() && noMoreBufferIds == that.noMoreBufferIds && type == that.type && buffers.equals(that.buffers);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getVersion(), type, buffers, noMoreBufferIds);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", getVersion())
                .add("type", type)
                .add("buffers", buffers)
                .add("noMoreBufferIds", noMoreBufferIds)
                .toString();
    }
}
