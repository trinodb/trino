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
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SpoolingOutputBuffers
        extends OutputBuffers
{
    private final ExchangeSinkInstanceHandle exchangeSinkInstanceHandle;
    private final int outputPartitionCount;

    public static SpoolingOutputBuffers createInitial(ExchangeSinkInstanceHandle exchangeSinkInstanceHandle, int outputPartitionCount)
    {
        return new SpoolingOutputBuffers(0, exchangeSinkInstanceHandle, outputPartitionCount);
    }

    // Visible only for Jackson... Use the "with" methods instead
    @JsonCreator
    public SpoolingOutputBuffers(
            @JsonProperty("version") long version,
            @JsonProperty("exchangeSinkInstanceHandle") ExchangeSinkInstanceHandle exchangeSinkInstanceHandle,
            @JsonProperty("outputPartitionCount") int outputPartitionCount)
    {
        super(version);
        this.exchangeSinkInstanceHandle = requireNonNull(exchangeSinkInstanceHandle, "exchangeSinkInstanceHandle is null");
        checkArgument(outputPartitionCount > 0, "outputPartitionCount must be greater than zero");
        this.outputPartitionCount = outputPartitionCount;
    }

    @JsonProperty
    public ExchangeSinkInstanceHandle getExchangeSinkInstanceHandle()
    {
        return exchangeSinkInstanceHandle;
    }

    @JsonProperty
    public int getOutputPartitionCount()
    {
        return outputPartitionCount;
    }

    @Override
    public void checkValidTransition(OutputBuffers outputBuffers)
    {
        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers instanceof SpoolingOutputBuffers, "outputBuffers is expected to be an instance of SpoolingOutputBuffers");
        if (getVersion() > outputBuffers.getVersion()) {
            throw new IllegalArgumentException("new outputBuffers version is older");
        }
        SpoolingOutputBuffers newOutputBuffers = (SpoolingOutputBuffers) outputBuffers;
        checkArgument(
                getOutputPartitionCount() == newOutputBuffers.getOutputPartitionCount(),
                "number of output partitions must be the same: %s != %s",
                getOutputPartitionCount(),
                newOutputBuffers.getOutputPartitionCount());
    }

    public SpoolingOutputBuffers withExchangeSinkInstanceHandle(ExchangeSinkInstanceHandle handle)
    {
        return new SpoolingOutputBuffers(getVersion() + 1, handle, outputPartitionCount);
    }
}
