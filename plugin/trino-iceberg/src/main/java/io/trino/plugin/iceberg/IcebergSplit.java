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
import io.trino.plugin.iceberg.serdes.IcebergFileScanTaskWrapper;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.iceberg.IcebergUtil.getSerializedPartitionKeys;
import static java.util.Objects.requireNonNull;

public class IcebergSplit
        implements ConnectorSplit
{
    private final IcebergFileScanTaskWrapper taskWrapper;
    private final List<HostAddress> addresses;

    // cached fields
    private final FileScanTask task;
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final FileFormat fileFormat;
    private final Map<Integer, Optional<String>> partitionKeys;

    @JsonCreator
    public IcebergSplit(
            @JsonProperty("taskWrapper") IcebergFileScanTaskWrapper taskWrapper,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.taskWrapper = requireNonNull(taskWrapper, "taskWrapper is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));

        this.task = taskWrapper.getTask();
        this.path = task.file().path().toString();
        this.start = task.start();
        this.length = task.length();
        this.fileSize = task.file().fileSizeInBytes();
        this.fileFormat = task.file().format();
        this.partitionKeys = getSerializedPartitionKeys(task);
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

    @JsonIgnore
    public String getPath()
    {
        return path;
    }

    @JsonIgnore
    public long getStart()
    {
        return start;
    }

    @JsonIgnore
    public long getLength()
    {
        return length;
    }

    @JsonIgnore
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonIgnore
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonIgnore
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
                .build();
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
