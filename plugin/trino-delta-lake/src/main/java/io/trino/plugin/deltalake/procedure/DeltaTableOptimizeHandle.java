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
package io.trino.plugin.deltalake.procedure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DeltaTableOptimizeHandle
        extends DeltaTableProcedureHandle
{
    private final MetadataEntry metadataEntry;
    private final ProtocolEntry protocolEntry;
    private final List<DeltaLakeColumnHandle> tableColumns;
    private final List<String> originalPartitionColumns;
    private final DataSize maxScannedFileSize;
    private final Optional<Long> currentVersion;
    private final boolean retriesEnabled;
    private final TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint;

    @JsonCreator
    public DeltaTableOptimizeHandle(
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            List<DeltaLakeColumnHandle> tableColumns,
            List<String> originalPartitionColumns,
            DataSize maxScannedFileSize,
            Optional<Long> currentVersion,
            boolean retriesEnabled,
            TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint)
    {
        this.metadataEntry = requireNonNull(metadataEntry, "metadataEntry is null");
        this.protocolEntry = requireNonNull(protocolEntry, "protocolEntry is null");
        this.tableColumns = ImmutableList.copyOf(requireNonNull(tableColumns, "tableColumns is null"));
        this.originalPartitionColumns = ImmutableList.copyOf(requireNonNull(originalPartitionColumns, "originalPartitionColumns is null"));
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
        this.currentVersion = requireNonNull(currentVersion, "currentVersion is null");
        this.retriesEnabled = retriesEnabled;
        this.enforcedPartitionConstraint = requireNonNull(enforcedPartitionConstraint, "enforcedPartitionConstraint is null");
    }

    public DeltaTableOptimizeHandle withCurrentVersion(long currentVersion)
    {
        checkState(this.currentVersion.isEmpty(), "currentVersion already set");
        return new DeltaTableOptimizeHandle(
                metadataEntry,
                protocolEntry,
                tableColumns,
                originalPartitionColumns,
                maxScannedFileSize,
                Optional.of(currentVersion),
                retriesEnabled,
                enforcedPartitionConstraint);
    }

    public DeltaTableOptimizeHandle withEnforcedPartitionConstraint(TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint)
    {
        return new DeltaTableOptimizeHandle(
                metadataEntry,
                protocolEntry,
                tableColumns,
                originalPartitionColumns,
                maxScannedFileSize,
                currentVersion,
                retriesEnabled,
                requireNonNull(enforcedPartitionConstraint, "enforcedPartitionConstraint is null"));
    }

    @JsonProperty
    public MetadataEntry getMetadataEntry()
    {
        return metadataEntry;
    }

    @JsonProperty
    public ProtocolEntry getProtocolEntry()
    {
        return protocolEntry;
    }

    @JsonProperty
    public List<DeltaLakeColumnHandle> getTableColumns()
    {
        return tableColumns;
    }

    /**
     * Returns partition column names with case preserved.
     */
    @JsonProperty
    public List<String> getOriginalPartitionColumns()
    {
        return originalPartitionColumns;
    }

    @JsonProperty
    public Optional<Long> getCurrentVersion()
    {
        return currentVersion;
    }

    @JsonProperty
    public DataSize getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    @JsonProperty
    public boolean isRetriesEnabled()
    {
        return retriesEnabled;
    }

    @JsonProperty
    public TupleDomain<DeltaLakeColumnHandle> getEnforcedPartitionConstraint()
    {
        return enforcedPartitionConstraint;
    }
}
