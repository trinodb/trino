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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.SerializationUtil.deserializeFromBytes;

public class IcebergSplit
        implements ConnectorSplit
{
    private final byte[] serializedTask;
    private final List<HostAddress> addresses;
    private final Map<Integer, String> partitionKeys;

    private transient FileScanTask task;

    @JsonCreator
    public IcebergSplit(
            @JsonProperty("serializedTask") byte[] serializedTask,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("partitionKeys") Map<Integer, String> partitionKeys)
    {
        this.serializedTask = requireNonNull(serializedTask, "serializedTask is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.partitionKeys = Collections.unmodifiableMap(requireNonNull(partitionKeys, "partitionKeys is null"));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    public byte[] getSerializedTask()
    {
        return serializedTask;
    }

    public FileScanTask getTask()
    {
        if (task == null) {
            task = deserializeFromBytes(serializedTask);
        }
        return task;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    public String getPath()
    {
        return getTask().file().path().toString();
    }

    public long getStart()
    {
        return getTask().start();
    }

    public long getLength()
    {
        return getTask().length();
    }

    public long getFileSize()
    {
        return getTask().file().fileSizeInBytes();
    }

    public FileFormat getFileFormat()
    {
        return getTask().file().format();
    }

    @JsonProperty
    public Map<Integer, String> getPartitionKeys()
    {
        return partitionKeys;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", getPath())
                .put("start", getStart())
                .put("length", getLength())
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(getPath())
                .addValue(getStart())
                .addValue(getLength())
                .toString();
    }
}
