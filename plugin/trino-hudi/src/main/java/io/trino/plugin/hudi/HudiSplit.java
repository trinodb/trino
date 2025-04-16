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
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.file.HudiLogFile;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HudiSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(HudiSplit.class));

    private final Optional<HudiBaseFile> baseFile;
    private final List<HudiLogFile> logFiles;
    private final String commitTime;
    private final TupleDomain<HiveColumnHandle> predicate;
    private final List<HivePartitionKey> partitionKeys;
    private final SplitWeight splitWeight;

    @JsonCreator
    public HudiSplit(
            @JsonProperty("baseFile") HudiBaseFile baseFile,
            @JsonProperty("logFiles") List<HudiLogFile> logFiles,
            @JsonProperty("commitTime") String commitTime,
            @JsonProperty("predicate") TupleDomain<HiveColumnHandle> predicate,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        this.baseFile = Optional.ofNullable(baseFile);
        this.logFiles = requireNonNull(logFiles, "logFiles is null");
        this.commitTime = requireNonNull(commitTime, "commitTime is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.partitionKeys = ImmutableList.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.<String, String>builder()
                .put("baseFile", baseFile.toString())
                .put("logFiles", logFiles.toString())
                .put("commitTime", commitTime)
                .buildOrThrow();
    }

    @JsonProperty
    public Optional<HudiBaseFile> getBaseFile()
    {
        return baseFile;
    }

    @JsonProperty
    public List<HudiLogFile> getLogFiles()
    {
        return logFiles;
    }

    @JsonProperty
    public String getCommitTime()
    {
        return commitTime;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + 10
                + 10
                + estimatedSizeOf(commitTime)
                + splitWeight.getRetainedSizeInBytes()
                + predicate.getRetainedSizeInBytes(HiveColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(partitionKeys, HivePartitionKey::estimatedSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(baseFile)
                .addValue(logFiles)
                .addValue(commitTime)
                .toString();
    }
}
