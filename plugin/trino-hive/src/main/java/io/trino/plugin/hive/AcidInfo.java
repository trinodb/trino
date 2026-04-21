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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.SizeOf;
import io.trino.filesystem.Location;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Stores information about Acid properties of a partition.
 */
public record AcidInfo(
        String partitionLocation,
        List<String> deleteDeltaDirectories,
        List<OriginalFileInfo> originalFiles,
        int bucketId,
        boolean orcAcidVersionValidated)
{
    private static final int INSTANCE_SIZE = instanceSize(AcidInfo.class);

    public AcidInfo
    {
        requireNonNull(partitionLocation, "partitionLocation is null");
        deleteDeltaDirectories = ImmutableList.copyOf(deleteDeltaDirectories);
        originalFiles = ImmutableList.copyOf(originalFiles);
    }

    public long retainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(partitionLocation)
                + estimatedSizeOf(deleteDeltaDirectories, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(originalFiles, OriginalFileInfo::getRetainedSizeInBytes);
    }

    public record OriginalFileInfo(String name, long fileSize)
    {
        private static final int INSTANCE_SIZE = instanceSize(OriginalFileInfo.class);

        public OriginalFileInfo
        {
            requireNonNull(name, "name is null");
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(name);
        }
    }

    public static Builder builder(Location partitionPath)
    {
        return new Builder(partitionPath);
    }

    public static class Builder
    {
        private final Location partitionLocation;
        private final List<String> deleteDeltaDirectories = new ArrayList<>();
        private final ListMultimap<Integer, OriginalFileInfo> bucketIdToOriginalFileInfoMap = ArrayListMultimap.create();
        private boolean orcAcidVersionValidated;

        private Builder(Location partitionPath)
        {
            partitionLocation = requireNonNull(partitionPath, "partitionPath is null");
        }

        public Builder addDeleteDelta(Location deleteDeltaPath)
        {
            requireNonNull(deleteDeltaPath, "deleteDeltaPath is null");
            Location partitionPathFromDeleteDelta = deleteDeltaPath.parentDirectory();
            checkArgument(
                    partitionLocation.equals(partitionPathFromDeleteDelta),
                    "Partition location in DeleteDelta '%s' does not match stored location '%s'",
                    partitionPathFromDeleteDelta,
                    partitionLocation);

            deleteDeltaDirectories.add(deleteDeltaPath.fileName());
            return this;
        }

        public Builder addOriginalFile(Location originalFilePath, long originalFileSize, int bucketId)
        {
            requireNonNull(originalFilePath, "originalFilePath is null");
            Location partitionPathFromOriginalPath = originalFilePath.parentDirectory();
            // originalFilePath has scheme in the prefix (i.e. scheme://<path>), extract path from uri and compare.
            checkArgument(
                    partitionLocation.equals(partitionPathFromOriginalPath),
                    "Partition location in OriginalFile '%s' does not match stored location '%s'",
                    partitionPathFromOriginalPath,
                    partitionLocation);
            bucketIdToOriginalFileInfoMap.put(bucketId, new OriginalFileInfo(originalFilePath.fileName(), originalFileSize));
            return this;
        }

        public Builder setOrcAcidVersionValidated(boolean orcAcidVersionValidated)
        {
            this.orcAcidVersionValidated = orcAcidVersionValidated;
            return this;
        }

        public AcidInfo buildWithRequiredOriginalFiles(int bucketId)
        {
            checkState(
                    bucketId > -1 && bucketIdToOriginalFileInfoMap.containsKey(bucketId),
                    "Bucket Id to OriginalFileInfo map should have entry for requested bucket id: %s",
                    bucketId);
            return new AcidInfo(partitionLocation.toString(), deleteDeltaDirectories, bucketIdToOriginalFileInfoMap.get(bucketId), bucketId, orcAcidVersionValidated);
        }

        public Optional<AcidInfo> build()
        {
            if (deleteDeltaDirectories.isEmpty() && orcAcidVersionValidated) {
                // We do not want to bail out with `Optional.empty()` if ORC ACID version was not validated based on _orc_acid_version file.
                // If we did so extra validation in OrcPageSourceFactory (based on file metadata) would not be performed.
                return Optional.empty();
            }
            return Optional.of(new AcidInfo(partitionLocation.toString(), deleteDeltaDirectories, ImmutableList.of(), -1, orcAcidVersionValidated));
        }
    }
}
