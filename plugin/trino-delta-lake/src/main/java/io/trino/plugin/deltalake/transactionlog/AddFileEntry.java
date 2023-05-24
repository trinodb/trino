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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.log.Logger;
import io.airlift.slice.SizeOf;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeStatsAsJson;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.canonicalizePartitionValues;
import static java.lang.String.format;

public class AddFileEntry
{
    private static final Logger LOG = Logger.get(AddFileEntry.class);
    private static final long INSTANCE_SIZE = instanceSize(AddFileEntry.class);

    private final String path;
    private final Map<String, String> partitionValues;
    private final Map<String, Optional<String>> canonicalPartitionValues;
    private final long size;
    private final long modificationTime;
    private final boolean dataChange;
    private final Map<String, String> tags;
    private final Optional<? extends DeltaLakeFileStatistics> parsedStats;

    @JsonCreator
    public AddFileEntry(
            @JsonProperty("path") String path,
            @JsonProperty("partitionValues") Map<String, String> partitionValues,
            @JsonProperty("size") long size,
            @JsonProperty("modificationTime") long modificationTime,
            @JsonProperty("dataChange") boolean dataChange,
            @JsonProperty("stats") Optional<String> stats,
            @JsonProperty("parsedStats") Optional<DeltaLakeParquetFileStatistics> parsedStats,
            @JsonProperty("tags") @Nullable Map<String, String> tags)
    {
        this.path = path;
        this.partitionValues = partitionValues;
        this.canonicalPartitionValues = canonicalizePartitionValues(partitionValues);
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
        this.tags = tags;

        Optional<? extends DeltaLakeFileStatistics> resultParsedStats = Optional.empty();
        if (parsedStats.isPresent()) {
            resultParsedStats = parsedStats;
        }
        else if (stats.isPresent()) {
            try {
                resultParsedStats = Optional.ofNullable(DeltaLakeJsonFileStatistics.create(stats.get()));
            }
            catch (JsonProcessingException e) {
                LOG.debug(
                        e,
                        "File level stats could not be parsed and will be ignored. The JSON string was: %s",
                        stats.get());
            }
        }
        this.parsedStats = resultParsedStats;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    @Deprecated // required for JSON serialization; getCanonicalPartitionValues should be used in code instead.
    public Map<String, String> getPartitionValues()
    {
        return partitionValues;
    }

    @JsonIgnore
    public Map<String, Optional<String>> getCanonicalPartitionValues()
    {
        return canonicalPartitionValues;
    }

    @JsonProperty
    public long getSize()
    {
        return size;
    }

    @JsonProperty
    public long getModificationTime()
    {
        return modificationTime;
    }

    @JsonProperty("dataChange")
    public boolean isDataChange()
    {
        return dataChange;
    }

    @JsonProperty("stats")
    public Optional<String> getStatsString()
    {
        if (parsedStats.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(serializeStatsAsJson(parsedStats.get()));
        }
        catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    public Optional<? extends DeltaLakeFileStatistics> getStats()
    {
        return parsedStats;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getTags()
    {
        return tags;
    }

    @Override
    public String toString()
    {
        return format("AddFileEntry{path=%s, partitionValues=%s, size=%d, modificationTime=%d, dataChange=%b, parsedStats=%s, tags=%s}",
                path, partitionValues, size, modificationTime, dataChange, parsedStats, tags);
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
        AddFileEntry that = (AddFileEntry) o;
        return size == that.size &&
                modificationTime == that.modificationTime &&
                dataChange == that.dataChange &&
                Objects.equals(path, that.path) &&
                Objects.equals(partitionValues, that.partitionValues) &&
                Objects.equals(canonicalPartitionValues, that.canonicalPartitionValues) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(parsedStats, that.parsedStats);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                path,
                partitionValues,
                canonicalPartitionValues,
                size,
                modificationTime,
                dataChange,
                tags,
                parsedStats);
    }

    public long getRetainedSizeInBytes()
    {
        long totalSize = INSTANCE_SIZE;
        totalSize += estimatedSizeOf(path);
        if (parsedStats.isPresent()) {
            totalSize += parsedStats.get().getRetainedSizeInBytes();
        }
        totalSize += estimatedSizeOf(partitionValues, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
        totalSize += estimatedSizeOf(tags, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
        return totalSize;
    }
}
