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
package io.trino.plugin.exchange.filesystem;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSinkInstanceHandle
        implements ExchangeSinkInstanceHandle
{
    private final FileSystemExchangeSinkHandle sinkHandle;
    private final URI outputDirectory;
    private final int outputPartitionCount;
    private final boolean preserveOrderWithinPartition;

    @JsonCreator
    public FileSystemExchangeSinkInstanceHandle(
            @JsonProperty("sinkHandle") FileSystemExchangeSinkHandle sinkHandle,
            @JsonProperty("outputDirectory") URI outputDirectory,
            @JsonProperty("outputPartitionCount") int outputPartitionCount,
            @JsonProperty("preserveOrderWithinPartition") boolean preserveOrderWithinPartition)
    {
        this.sinkHandle = requireNonNull(sinkHandle, "sinkHandle is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
    }

    @JsonProperty
    public FileSystemExchangeSinkHandle getSinkHandle()
    {
        return sinkHandle;
    }

    @JsonProperty
    public URI getOutputDirectory()
    {
        return outputDirectory;
    }

    @JsonProperty
    public int getOutputPartitionCount()
    {
        return outputPartitionCount;
    }

    @JsonProperty
    public boolean isPreserveOrderWithinPartition()
    {
        return preserveOrderWithinPartition;
    }
}
