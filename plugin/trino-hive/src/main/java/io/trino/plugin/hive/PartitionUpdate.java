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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.spi.TrinoException;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionUpdate
{
    private final String name;
    private final UpdateMode updateMode;
    private final Location writePath;
    private final Location targetPath;
    private final List<String> fileNames;
    private final long rowCount;
    private final long inMemoryDataSizeInBytes;
    private final long onDiskDataSizeInBytes;

    @JsonCreator
    public PartitionUpdate(
            @JsonProperty("name") String name,
            @JsonProperty("updateMode") UpdateMode updateMode,
            @JsonProperty("writePath") String writePath,
            @JsonProperty("targetPath") String targetPath,
            @JsonProperty("fileNames") List<String> fileNames,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("inMemoryDataSizeInBytes") long inMemoryDataSizeInBytes,
            @JsonProperty("onDiskDataSizeInBytes") long onDiskDataSizeInBytes)
    {
        this(
                name,
                updateMode,
                Location.of(requireNonNull(writePath, "writePath is null")),
                Location.of(requireNonNull(targetPath, "targetPath is null")),
                fileNames,
                rowCount,
                inMemoryDataSizeInBytes,
                onDiskDataSizeInBytes);
    }

    public PartitionUpdate(
            String name,
            UpdateMode updateMode,
            Location writePath,
            Location targetPath,
            List<String> fileNames,
            long rowCount,
            long inMemoryDataSizeInBytes,
            long onDiskDataSizeInBytes)
    {
        this.name = requireNonNull(name, "name is null");
        this.updateMode = requireNonNull(updateMode, "updateMode is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.targetPath = requireNonNull(targetPath, "targetPath is null");
        this.fileNames = ImmutableList.copyOf(requireNonNull(fileNames, "fileNames is null"));
        this.rowCount = rowCount;
        checkArgument(inMemoryDataSizeInBytes >= 0, "inMemoryDataSizeInBytes is negative: %s", inMemoryDataSizeInBytes);
        this.inMemoryDataSizeInBytes = inMemoryDataSizeInBytes;
        checkArgument(onDiskDataSizeInBytes >= 0, "onDiskDataSizeInBytes is negative: %s", onDiskDataSizeInBytes);
        this.onDiskDataSizeInBytes = onDiskDataSizeInBytes;
    }

    public PartitionUpdate withRowCount(int rowCount)
    {
        return new PartitionUpdate(name, updateMode, writePath, targetPath, fileNames, rowCount, inMemoryDataSizeInBytes, onDiskDataSizeInBytes);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public UpdateMode getUpdateMode()
    {
        return updateMode;
    }

    public Location getWritePath()
    {
        return writePath;
    }

    public Location getTargetPath()
    {
        return targetPath;
    }

    @JsonProperty
    public List<String> getFileNames()
    {
        return fileNames;
    }

    @JsonProperty("targetPath")
    public String getJsonSerializableTargetPath()
    {
        return targetPath.toString();
    }

    @JsonProperty("writePath")
    public String getJsonSerializableWritePath()
    {
        return writePath.toString();
    }

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public long getInMemoryDataSizeInBytes()
    {
        return inMemoryDataSizeInBytes;
    }

    @JsonProperty
    public long getOnDiskDataSizeInBytes()
    {
        return onDiskDataSizeInBytes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("updateMode", updateMode)
                .add("writePath", writePath)
                .add("targetPath", targetPath)
                .add("fileNames", fileNames)
                .add("rowCount", rowCount)
                .add("inMemoryDataSizeInBytes", inMemoryDataSizeInBytes)
                .add("onDiskDataSizeInBytes", onDiskDataSizeInBytes)
                .toString();
    }

    public HiveBasicStatistics getStatistics()
    {
        return new HiveBasicStatistics(fileNames.size(), rowCount, inMemoryDataSizeInBytes, onDiskDataSizeInBytes);
    }

    public static List<PartitionUpdate> mergePartitionUpdates(Iterable<PartitionUpdate> unMergedUpdates)
    {
        ImmutableList.Builder<PartitionUpdate> partitionUpdates = ImmutableList.builder();
        for (Collection<PartitionUpdate> partitionGroup : Multimaps.index(unMergedUpdates, PartitionUpdate::getName).asMap().values()) {
            PartitionUpdate firstPartition = partitionGroup.iterator().next();

            ImmutableList.Builder<String> allFileNames = ImmutableList.builder();
            long totalRowCount = 0;
            long totalInMemoryDataSizeInBytes = 0;
            long totalOnDiskDataSizeInBytes = 0;
            for (PartitionUpdate partition : partitionGroup) {
                // verify partitions have the same new flag, write path and target path
                // this shouldn't happen but could if another user added a partition during the write
                if (partition.getUpdateMode() != firstPartition.getUpdateMode() ||
                        !partition.getWritePath().equals(firstPartition.getWritePath()) ||
                        !partition.getTargetPath().equals(firstPartition.getTargetPath())) {
                    throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, format("Partition %s was added or modified during INSERT", firstPartition.getName()));
                }
                allFileNames.addAll(partition.getFileNames());
                totalRowCount += partition.getRowCount();
                totalInMemoryDataSizeInBytes += partition.getInMemoryDataSizeInBytes();
                totalOnDiskDataSizeInBytes += partition.getOnDiskDataSizeInBytes();
            }

            partitionUpdates.add(new PartitionUpdate(firstPartition.getName(),
                    firstPartition.getUpdateMode(),
                    firstPartition.getWritePath(),
                    firstPartition.getTargetPath(),
                    allFileNames.build(),
                    totalRowCount,
                    totalInMemoryDataSizeInBytes,
                    totalOnDiskDataSizeInBytes));
        }
        return partitionUpdates.build();
    }

    public enum UpdateMode
    {
        NEW,
        APPEND,
        OVERWRITE,
    }
}
