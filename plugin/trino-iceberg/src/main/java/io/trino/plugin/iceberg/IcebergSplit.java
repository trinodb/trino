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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.plugin.iceberg.serdes.IcebergFileScanTaskWrapper;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.iceberg.FileScanTask;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class IcebergSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IcebergSplit.class).instanceSize();

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final IcebergFileFormat fileFormat;
    private final List<HostAddress> addresses;
    private final Map<Integer, Optional<String>> partitionKeys;
    private final IcebergFileScanTaskWrapper taskWrapper;
    private final FileScanTask task;

    @JsonCreator
    public IcebergSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("partitionKeys") Map<Integer, Optional<String>> partitionKeys,
            @JsonProperty("taskWrapper") IcebergFileScanTaskWrapper taskWrapper)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.partitionKeys = ImmutableMap.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.taskWrapper = requireNonNull(taskWrapper, "taskWrapper is null");
        this.task = taskWrapper.getTask();
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public IcebergFileScanTaskWrapper getTaskWrapper()
    {
        return taskWrapper;
    }

    @JsonIgnore
    public FileScanTask getTask()
    {
        return task;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public IcebergFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Map<Integer, Optional<String>> getPartitionKeys()
    {
        return partitionKeys;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // TODO: Add size of FileScanTask
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + estimatedSizeOf(partitionKeys, SizeOf::sizeOf, valueOptional -> sizeOf(valueOptional, SizeOf::estimatedSizeOf));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .toString();
    }
}
