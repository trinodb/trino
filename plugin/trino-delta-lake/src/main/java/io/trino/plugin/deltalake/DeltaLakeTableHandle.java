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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.DeltaLakeTableHandle.WriteType.UPDATE;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTableHandle
        implements LocatedTableHandle
{
    // Insert is not included here because it uses a separate TableHandle type
    public enum WriteType
    {
        UPDATE,
        DELETE
    }

    private final String schemaName;
    private final String tableName;
    private final boolean managed;
    private final String location;
    private final MetadataEntry metadataEntry;
    private final ProtocolEntry protocolEntry;
    private final TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint;
    private final TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint;
    private final Optional<WriteType> writeType;
    private final long readVersion;
    private final boolean timeTravel;

    private final Optional<Set<DeltaLakeColumnHandle>> projectedColumns;
    // UPDATE only: The list of columns being updated
    private final Optional<List<DeltaLakeColumnHandle>> updatedColumns;
    // UPDATE only: The list of columns which need to be copied when applying updates to the new Parquet file
    private final Optional<List<DeltaLakeColumnHandle>> updateRowIdColumns;

    // ANALYZE only
    private final Optional<AnalyzeHandle> analyzeHandle;

    // OPTIMIZE only. Coordinator-only
    private final boolean recordScannedFiles;
    private final boolean isOptimize;
    private final Optional<DataSize> maxScannedFileSize;
    // Used only for validation when config property delta.query-partition-filter-required is enabled.
    private final Set<DeltaLakeColumnHandle> constraintColumns;

    @JsonCreator
    public DeltaLakeTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("managed") boolean managed,
            @JsonProperty("location") String location,
            @JsonProperty("metadataEntry") MetadataEntry metadataEntry,
            @JsonProperty("protocolEntry") ProtocolEntry protocolEntry,
            @JsonProperty("enforcedPartitionConstraint") TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint,
            @JsonProperty("nonPartitionConstraint") TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint,
            @JsonProperty("writeType") Optional<WriteType> writeType,
            @JsonProperty("projectedColumns") Optional<Set<DeltaLakeColumnHandle>> projectedColumns,
            @JsonProperty("updatedColumns") Optional<List<DeltaLakeColumnHandle>> updatedColumns,
            @JsonProperty("updateRowIdColumns") Optional<List<DeltaLakeColumnHandle>> updateRowIdColumns,
            @JsonProperty("analyzeHandle") Optional<AnalyzeHandle> analyzeHandle,
            @JsonProperty("readVersion") long readVersion,
            @JsonProperty("timeTravel") boolean timeTravel)
    {
        this(
                schemaName,
                tableName,
                managed,
                location,
                metadataEntry,
                protocolEntry,
                enforcedPartitionConstraint,
                nonPartitionConstraint,
                ImmutableSet.of(),
                writeType,
                projectedColumns,
                updatedColumns,
                updateRowIdColumns,
                analyzeHandle,
                false,
                false,
                Optional.empty(),
                readVersion,
                timeTravel);
    }

    public DeltaLakeTableHandle(
            String schemaName,
            String tableName,
            boolean managed,
            String location,
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint,
            TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint,
            Set<DeltaLakeColumnHandle> constraintColumns,
            Optional<WriteType> writeType,
            Optional<Set<DeltaLakeColumnHandle>> projectedColumns,
            Optional<List<DeltaLakeColumnHandle>> updatedColumns,
            Optional<List<DeltaLakeColumnHandle>> updateRowIdColumns,
            Optional<AnalyzeHandle> analyzeHandle,
            boolean recordScannedFiles,
            boolean isOptimize,
            Optional<DataSize> maxScannedFileSize,
            long readVersion,
            boolean timeTravel)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.managed = managed;
        this.location = requireNonNull(location, "location is null");
        this.metadataEntry = requireNonNull(metadataEntry, "metadataEntry is null");
        this.protocolEntry = requireNonNull(protocolEntry, "protocolEntry is null");
        this.enforcedPartitionConstraint = requireNonNull(enforcedPartitionConstraint, "enforcedPartitionConstraint is null");
        this.nonPartitionConstraint = requireNonNull(nonPartitionConstraint, "nonPartitionConstraint is null");
        this.writeType = requireNonNull(writeType, "writeType is null");
        checkArgument(updatedColumns.isPresent() == (writeType.isPresent() && writeType.get() == UPDATE));
        checkArgument(updateRowIdColumns.isPresent() == (writeType.isPresent() && writeType.get() == UPDATE));
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
        this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
        this.updateRowIdColumns = requireNonNull(updateRowIdColumns, "rowIdColumns is null");
        this.analyzeHandle = requireNonNull(analyzeHandle, "analyzeHandle is null");
        this.recordScannedFiles = recordScannedFiles;
        this.isOptimize = isOptimize;
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
        this.readVersion = readVersion;
        this.timeTravel = timeTravel;
        this.constraintColumns = ImmutableSet.copyOf(requireNonNull(constraintColumns, "constraintColumns is null"));
    }

    public DeltaLakeTableHandle withProjectedColumns(Set<DeltaLakeColumnHandle> projectedColumns)
    {
        return new DeltaLakeTableHandle(
                schemaName,
                tableName,
                managed,
                location,
                metadataEntry,
                protocolEntry,
                enforcedPartitionConstraint,
                nonPartitionConstraint,
                constraintColumns,
                writeType,
                Optional.of(projectedColumns),
                updatedColumns,
                updateRowIdColumns,
                analyzeHandle,
                recordScannedFiles,
                isOptimize,
                maxScannedFileSize,
                readVersion,
                timeTravel);
    }

    public DeltaLakeTableHandle forOptimize(boolean recordScannedFiles, DataSize maxScannedFileSize)
    {
        return new DeltaLakeTableHandle(
                schemaName,
                tableName,
                managed,
                location,
                metadataEntry,
                protocolEntry,
                enforcedPartitionConstraint,
                nonPartitionConstraint,
                constraintColumns,
                writeType,
                projectedColumns,
                updatedColumns,
                updateRowIdColumns,
                analyzeHandle,
                recordScannedFiles,
                true,
                Optional.of(maxScannedFileSize),
                readVersion,
                timeTravel);
    }

    @Override
    public SchemaTableName schemaTableName()
    {
        return getSchemaTableName();
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public boolean managed()
    {
        return isManaged();
    }

    @JsonProperty
    public boolean isManaged()
    {
        return managed;
    }

    @Override
    public String location()
    {
        return getLocation();
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
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
    public TupleDomain<DeltaLakeColumnHandle> getEnforcedPartitionConstraint()
    {
        return enforcedPartitionConstraint;
    }

    @JsonProperty
    public TupleDomain<DeltaLakeColumnHandle> getNonPartitionConstraint()
    {
        return nonPartitionConstraint;
    }

    @JsonProperty
    public Optional<WriteType> getWriteType()
    {
        return writeType;
    }

    // Projected columns are not needed on workers
    @JsonIgnore
    public Optional<Set<DeltaLakeColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public Optional<List<DeltaLakeColumnHandle>> getUpdatedColumns()
    {
        return updatedColumns;
    }

    @JsonProperty
    public Optional<List<DeltaLakeColumnHandle>> getUpdateRowIdColumns()
    {
        return updateRowIdColumns;
    }

    @JsonProperty
    public Optional<AnalyzeHandle> getAnalyzeHandle()
    {
        return analyzeHandle;
    }

    @JsonIgnore
    public boolean isRecordScannedFiles()
    {
        return recordScannedFiles;
    }

    @JsonIgnore
    public boolean isOptimize()
    {
        return isOptimize;
    }

    @JsonIgnore
    public Optional<DataSize> getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    @JsonIgnore
    public Set<DeltaLakeColumnHandle> getConstraintColumns()
    {
        return constraintColumns;
    }

    @JsonProperty
    public long getReadVersion()
    {
        return readVersion;
    }

    @JsonProperty
    public boolean isTimeTravel()
    {
        return timeTravel;
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
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

        DeltaLakeTableHandle that = (DeltaLakeTableHandle) o;
        return recordScannedFiles == that.recordScannedFiles &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                managed == that.managed &&
                Objects.equals(location, that.location) &&
                Objects.equals(metadataEntry, that.metadataEntry) &&
                Objects.equals(protocolEntry, that.protocolEntry) &&
                Objects.equals(enforcedPartitionConstraint, that.enforcedPartitionConstraint) &&
                Objects.equals(nonPartitionConstraint, that.nonPartitionConstraint) &&
                Objects.equals(writeType, that.writeType) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(updatedColumns, that.updatedColumns) &&
                Objects.equals(updateRowIdColumns, that.updateRowIdColumns) &&
                Objects.equals(analyzeHandle, that.analyzeHandle) &&
                isOptimize == that.isOptimize &&
                Objects.equals(maxScannedFileSize, that.maxScannedFileSize) &&
                readVersion == that.readVersion &&
                timeTravel == that.timeTravel;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                schemaName,
                tableName,
                managed,
                location,
                metadataEntry,
                protocolEntry,
                enforcedPartitionConstraint,
                nonPartitionConstraint,
                writeType,
                projectedColumns,
                updatedColumns,
                updateRowIdColumns,
                analyzeHandle,
                recordScannedFiles,
                isOptimize,
                maxScannedFileSize,
                readVersion,
                timeTravel);
    }
}
