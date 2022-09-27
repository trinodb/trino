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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.files.HudiBaseFile;
import io.trino.plugin.hudi.files.HudiLogFile;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
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

    private final TupleDomain<HiveColumnHandle> predicate;
    private final List<HivePartitionKey> partitionKeys;
    private final SplitWeight splitWeight;
    private final Optional<HudiBaseFile> baseFile;
    private final List<HudiLogFile> logFiles;
    private final String commitTime;

    @JsonCreator
    public HudiSplit(
            @JsonProperty("predicate") TupleDomain<HiveColumnHandle> predicate,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("baseFile") Optional<HudiBaseFile> baseFile,
            @JsonProperty("logFiles") List<HudiLogFile> logFiles,
            @JsonProperty("commitTime") String commitTime)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.partitionKeys = ImmutableList.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.baseFile = requireNonNull(baseFile, "baseFile is null");
        this.logFiles = requireNonNull(logFiles, "logFiles is null");
        this.commitTime = requireNonNull(commitTime, "commitTime is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonIgnore
    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        ImmutableMap.Builder<Object, Object> infoMap = ImmutableMap.builder().put("commitTime", commitTime);
        baseFile.ifPresent(file -> infoMap
                .put("location", file.getLocation().toString())
                .put("start", file.getOffset())
                .put("length", file.getFileLen())
                .put("fileSize", file.getFileLen())
                .put("fileModifiedTime", file.getFileModifiedTime()));

        return infoMap.buildOrThrow();
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

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(baseFile.map(file -> file.getLocation().toString()).orElse(""))
                + splitWeight.getRetainedSizeInBytes()
                + predicate.getRetainedSizeInBytes(HiveColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(partitionKeys, HivePartitionKey::getEstimatedSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("baseFile", baseFile)
                .add("logFiles", logFiles)
                .addValue(commitTime)
                .toString();
    }
}
