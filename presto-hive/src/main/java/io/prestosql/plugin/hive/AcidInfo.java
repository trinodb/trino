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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Stores information about Acid properties of a partition.
 */
public class AcidInfo
{
    private final String partitionLocation;
    private final List<DeleteDeltaInfo> deleteDeltas;
    private final Optional<OriginalFileLocations> originalFileLocations;
    private final Optional<Integer> bucketId;

    @JsonCreator
    public AcidInfo(
            @JsonProperty("partitionLocation") String partitionLocation,
            @JsonProperty("deleteDeltas") List<DeleteDeltaInfo> deleteDeltas)
    {
        this(partitionLocation, deleteDeltas, Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public AcidInfo(@JsonProperty("partitionLocation") String partitionLocation,
            @JsonProperty("deleteDeltas") List<DeleteDeltaInfo> deleteDeltas,
            @JsonProperty("originalFiles") Optional<OriginalFileLocations> originalFileLocations,
            @JsonProperty("bucketId") Optional<Integer> bucketId)
    {
        this.partitionLocation = requireNonNull(partitionLocation, "partitionLocation is null");
        this.deleteDeltas = ImmutableList.copyOf(requireNonNull(deleteDeltas, "deleteDeltas is null"));
        checkArgument(!deleteDeltas.isEmpty(), "deleteDeltas is empty");
        //this.originalFileLocations = requireNonNull(originalFileLocations, "originalFileLocations is null");
        this.originalFileLocations = originalFileLocations;
        //this.bucketId = requireNonNull(bucketId, "bucketId is null");
        this.bucketId = bucketId;
    }

    @JsonProperty
    public Optional<OriginalFileLocations> getOriginalFileLocations()
    {
        return originalFileLocations;
    }

    @JsonProperty
    public Optional<Integer> getBucketId()
    {
        return bucketId;
    }

    @JsonProperty
    public String getPartitionLocation()
    {
        return partitionLocation;
    }

    @JsonProperty
    public List<DeleteDeltaInfo> getDeleteDeltas()
    {
        return deleteDeltas;
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

        AcidInfo that = (AcidInfo) o;
        return partitionLocation.equals(that.partitionLocation) &&
                deleteDeltas.equals(that.deleteDeltas);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionLocation, deleteDeltas);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionLocation", partitionLocation)
                .add("deleteDeltas", deleteDeltas)
                .toString();
    }

    public static class DeleteDeltaInfo
    {
        private final long minWriteId;
        private final long maxWriteId;
        private final int statementId;

        @JsonCreator
        public DeleteDeltaInfo(
                @JsonProperty("minWriteId") long minWriteId,
                @JsonProperty("maxWriteId") long maxWriteId,
                @JsonProperty("statementId") int statementId)
        {
            this.minWriteId = minWriteId;
            this.maxWriteId = maxWriteId;
            this.statementId = statementId;
        }

        @JsonProperty
        public long getMinWriteId()
        {
            return minWriteId;
        }

        @JsonProperty
        public long getMaxWriteId()
        {
            return maxWriteId;
        }

        @JsonProperty
        public int getStatementId()
        {
            return statementId;
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

            DeleteDeltaInfo that = (DeleteDeltaInfo) o;
            return minWriteId == that.minWriteId &&
                    maxWriteId == that.maxWriteId &&
                    statementId == that.statementId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(minWriteId, maxWriteId, statementId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("minWriteId", minWriteId)
                    .add("maxWriteId", maxWriteId)
                    .add("statementId", statementId)
                    .toString();
        }
    }

    public static Builder builder(Path partitionPath)
    {
        return new Builder(partitionPath);
    }

    public static Builder builder(AcidInfo acidInfo)
    {
        return new Builder(acidInfo);
    }

    public static class Builder
    {
        private final Path partitionLocation;
        private final ImmutableList.Builder<DeleteDeltaInfo> deleteDeltaInfoBuilder = ImmutableList.builder();
        private Optional<Integer> bucketId;
        private final Map<Integer, List<OriginalFileLocations.OriginalFileInfo>> bucketIdToOriginalFileInfoMap = new HashMap<>();
        private Function<FileStatus, Integer> bucketIdProvider;
        private Optional<OriginalFileLocations> originalFileLocations;

        private Builder(Path partitionPath)
        {
            partitionLocation = requireNonNull(partitionPath, "partitionPath is null");
        }

        private Builder(AcidInfo acidInfo)
        {
            partitionLocation = new Path(acidInfo.getPartitionLocation());
            deleteDeltaInfoBuilder.addAll(acidInfo.deleteDeltas);
        }

        public Builder addDeleteDelta(Path deleteDeltaPath, long minWriteId, long maxWriteId, int statementId)
        {
            requireNonNull(deleteDeltaPath, "deleteDeltaPath is null");
            Path partitionPathFromDeleteDelta = deleteDeltaPath.getParent();
            checkArgument(
                    partitionLocation.equals(partitionPathFromDeleteDelta),
                    "Partition location in DeleteDelta '%s' does not match stored location '%s'",
                    deleteDeltaPath.getParent().toString(),
                    partitionLocation);

            deleteDeltaInfoBuilder.add(new DeleteDeltaInfo(minWriteId, maxWriteId, statementId));
            return this;
        }

        public Builder addDeleteDelta(List<AcidUtils.ParsedDelta> currentDirectories)
        {
            if (currentDirectories == null || currentDirectories.isEmpty()) {
                return this;
            }
            for (AcidUtils.ParsedDelta delta : currentDirectories) {
                if (delta.isDeleteDelta()) {
                    deleteDeltaInfoBuilder.add(new DeleteDeltaInfo(delta.getMinWriteId(),
                            delta.getMaxWriteId(), delta.getStatementId()));
                }
            }
            return this;
        }

        public Builder addOriginalFiles(Configuration configuration, List<HadoopShims.HdfsFileStatusWithId> originalFileLocations)
        {
            bucketIdProvider = fileStatus -> {
                try {
                    return AcidUtils.parseBaseOrDeltaBucketFilename(fileStatus.getPath(), configuration).getBucketId();
                }
                catch (IOException e) {
                    throw new PrestoException(HIVE_UNKNOWN_ERROR, e);
                }
            };

            for (HadoopShims.HdfsFileStatusWithId hdfsFileStatusWithId : originalFileLocations) {
                Path originalFilePath = hdfsFileStatusWithId.getFileStatus().getPath();
                long originalFileLength = hdfsFileStatusWithId.getFileStatus().getLen();
                int bucketId = bucketIdProvider.apply(hdfsFileStatusWithId.getFileStatus());

                List<OriginalFileLocations.OriginalFileInfo> originalFileInfoList = bucketIdToOriginalFileInfoMap.getOrDefault(bucketId, new ArrayList<>());
                originalFileInfoList.add(new OriginalFileLocations.OriginalFileInfo(originalFilePath.getName(), originalFileLength));
                bucketIdToOriginalFileInfoMap.put(bucketId, originalFileInfoList);
            }
            return this;
        }

        public Builder addOriginalFiles(Optional<OriginalFileLocations> originalFileLocations)
        {
            this.originalFileLocations = originalFileLocations;
            return this;
        }

        public Optional<AcidInfo> buildWithRequiredOriginalFiles()
        {
            // 1. Fetch list of all the original files which have same bucket Id
            // 2. Build AcidInfo
            checkState(this.bucketId.isPresent() && bucketIdToOriginalFileInfoMap.containsKey(this.bucketId.get()), "Bucket Id to OriginalFileInfo map should have " +
                    "entry for requested bucket Id");
            OriginalFileLocations originalFileLocations = new OriginalFileLocations(bucketIdToOriginalFileInfoMap.get(bucketId.get()));
            List<DeleteDeltaInfo> deleteDeltas = deleteDeltaInfoBuilder.build();
            if (deleteDeltas.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new AcidInfo(partitionLocation.toString(), deleteDeltas, Optional.of(originalFileLocations), bucketId));
        }

        public Optional<OriginalFileLocations> getOriginalFileLocations()
        {
            return originalFileLocations;
        }

        public Optional<Integer> getBucketId()
        {
            return bucketId;
        }

        public Builder setBucketId(int bucketId)
        {
            this.bucketId = Optional.of(bucketId);
            return this;
        }

        public Builder setBucketId(FileStatus fileStatus)
        {
            requireNonNull(bucketIdProvider, "bucketIdProvider is null");
            this.bucketId = Optional.of(bucketIdProvider.apply(fileStatus));
            return this;
        }

        public Optional<AcidInfo> build()
        {
            List<DeleteDeltaInfo> deleteDeltas = deleteDeltaInfoBuilder.build();
            if (deleteDeltas.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new AcidInfo(partitionLocation.toString(), deleteDeltas, originalFileLocations, bucketId));
        }
    }
}
