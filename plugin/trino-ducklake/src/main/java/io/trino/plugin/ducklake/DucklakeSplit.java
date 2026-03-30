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
package io.trino.plugin.ducklake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Represents a split for reading Ducklake data.
 * Each split corresponds to a Parquet data file.
 */
public record DucklakeSplit(
        @JsonProperty("dataFilePath") String dataFilePath,
        @JsonProperty("deleteFilePath") Optional<String> deleteFilePath,
        @JsonProperty("rowIdStart") long rowIdStart,
        @JsonProperty("recordCount") long recordCount,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("fileFormat") String fileFormat,
        @JsonProperty("fileStatisticsDomain") TupleDomain<DucklakeColumnHandle> fileStatisticsDomain)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DucklakeSplit.class);

    @JsonCreator
    public DucklakeSplit
    {
        requireNonNull(dataFilePath, "dataFilePath is null");
        requireNonNull(deleteFilePath, "deleteFilePath is null");
        requireNonNull(fileFormat, "fileFormat is null");
        requireNonNull(fileStatisticsDomain, "fileStatisticsDomain is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // Ducklake files can be on object storage
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        // No specific host affinity for object storage
        return List.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(dataFilePath)
                + deleteFilePath.map(SizeOf::estimatedSizeOf).orElse(0L)
                + (SIZE_OF_LONG * 3) // rowIdStart, recordCount, fileSizeBytes
                + estimatedSizeOf(fileFormat)
                + fileStatisticsDomain.getRetainedSizeInBytes(DucklakeColumnHandle::getRetainedSizeInBytes);
    }
}
