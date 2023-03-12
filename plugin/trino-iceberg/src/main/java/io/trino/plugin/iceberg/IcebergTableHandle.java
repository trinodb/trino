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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final Optional<Long> snapshotId;
    private final String tableSchemaJson;
    private final List<TrinoSortField> sortOrder;
    // Empty means the partitioning spec is not known (can be the case for certain time travel queries).
    private final Optional<String> partitionSpecJson;
    private final int formatVersion;
    private final String tableLocation;
    private final Map<String, String> storageProperties;
    private final RetryMode retryMode;

    // UPDATE only
    private final List<IcebergColumnHandle> updatedColumns;

    // Filter used during split generation and table scan, but not required to be strictly enforced by Iceberg Connector
    private final TupleDomain<IcebergColumnHandle> unenforcedPredicate;

    // Filter guaranteed to be enforced by Iceberg connector
    private final TupleDomain<IcebergColumnHandle> enforcedPredicate;

    private final Set<IcebergColumnHandle> projectedColumns;
    private final Optional<String> nameMappingJson;

    // OPTIMIZE only. Coordinator-only
    private final boolean recordScannedFiles;
    private final Optional<DataSize> maxScannedFileSize;

    @JsonCreator
    public static IcebergTableHandle fromJsonForDeserializationOnly(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("tableSchemaJson") String tableSchemaJson,
            @JsonProperty("sortOrder") List<TrinoSortField> sortOrder,
            @JsonProperty("partitionSpecJson") Optional<String> partitionSpecJson,
            @JsonProperty("formatVersion") int formatVersion,
            @JsonProperty("unenforcedPredicate") TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            @JsonProperty("enforcedPredicate") TupleDomain<IcebergColumnHandle> enforcedPredicate,
            @JsonProperty("projectedColumns") Set<IcebergColumnHandle> projectedColumns,
            @JsonProperty("nameMappingJson") Optional<String> nameMappingJson,
            @JsonProperty("tableLocation") String tableLocation,
            @JsonProperty("storageProperties") Map<String, String> storageProperties,
            @JsonProperty("retryMode") RetryMode retryMode,
            @JsonProperty("updatedColumns") List<IcebergColumnHandle> updatedColumns)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                sortOrder,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                false,
                Optional.empty());
    }

    public IcebergTableHandle(
            String schemaName,
            String tableName,
            TableType tableType,
            Optional<Long> snapshotId,
            String tableSchemaJson,
            List<TrinoSortField> sortOrder,
            Optional<String> partitionSpecJson,
            int formatVersion,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation,
            Map<String, String> storageProperties,
            RetryMode retryMode,
            List<IcebergColumnHandle> updatedColumns,
            boolean recordScannedFiles,
            Optional<DataSize> maxScannedFileSize)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.tableSchemaJson = requireNonNull(tableSchemaJson, "schemaJson is null");
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.formatVersion = formatVersion;
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.enforcedPredicate = requireNonNull(enforcedPredicate, "enforcedPredicate is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.nameMappingJson = requireNonNull(nameMappingJson, "nameMappingJson is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.retryMode = requireNonNull(retryMode, "retryMode is null");
        this.updatedColumns = ImmutableList.copyOf(requireNonNull(updatedColumns, "updatedColumns is null"));
        this.recordScannedFiles = recordScannedFiles;
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
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

    @JsonProperty
    public TableType getTableType()
    {
        return tableType;
    }

    // Empty only when reading from a table that has no snapshots yet.
    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public String getTableSchemaJson()
    {
        return tableSchemaJson;
    }

    @JsonProperty
    public List<TrinoSortField> getSortOrder()
    {
        return sortOrder;
    }

    @JsonProperty
    public Optional<String> getPartitionSpecJson()
    {
        return partitionSpecJson;
    }

    @JsonProperty
    public int getFormatVersion()
    {
        return formatVersion;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getUnenforcedPredicate()
    {
        return unenforcedPredicate;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getEnforcedPredicate()
    {
        return enforcedPredicate;
    }

    @JsonProperty
    public Set<IcebergColumnHandle> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public Optional<String> getNameMappingJson()
    {
        return nameMappingJson;
    }

    @JsonProperty
    public String getTableLocation()
    {
        return tableLocation;
    }

    @JsonProperty
    public Map<String, String> getStorageProperties()
    {
        return storageProperties;
    }

    @JsonProperty
    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getUpdatedColumns()
    {
        return updatedColumns;
    }

    @JsonIgnore
    public boolean isRecordScannedFiles()
    {
        return recordScannedFiles;
    }

    @JsonIgnore
    public Optional<DataSize> getMaxScannedFileSize()
    {
        return maxScannedFileSize;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public SchemaTableName getSchemaTableNameWithType()
    {
        return new SchemaTableName(schemaName, tableName + "$" + tableType.name().toLowerCase(Locale.ROOT));
    }

    public IcebergTableHandle withProjectedColumns(Set<IcebergColumnHandle> projectedColumns)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                sortOrder,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public IcebergTableHandle withRetryMode(RetryMode retryMode)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                sortOrder,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public IcebergTableHandle withUpdatedColumns(List<IcebergColumnHandle> updatedColumns)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                sortOrder,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public IcebergTableHandle forOptimize(boolean recordScannedFiles, DataSize maxScannedFileSize)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                sortOrder,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                retryMode,
                updatedColumns,
                recordScannedFiles,
                Optional.of(maxScannedFileSize));
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

        IcebergTableHandle that = (IcebergTableHandle) o;
        return recordScannedFiles == that.recordScannedFiles &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                tableType == that.tableType &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(tableSchemaJson, that.tableSchemaJson) &&
                Objects.equals(sortOrder, that.sortOrder) &&
                Objects.equals(partitionSpecJson, that.partitionSpecJson) &&
                formatVersion == that.formatVersion &&
                Objects.equals(unenforcedPredicate, that.unenforcedPredicate) &&
                Objects.equals(enforcedPredicate, that.enforcedPredicate) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(nameMappingJson, that.nameMappingJson) &&
                Objects.equals(tableLocation, that.tableLocation) &&
                Objects.equals(retryMode, that.retryMode) &&
                Objects.equals(updatedColumns, that.updatedColumns) &&
                Objects.equals(storageProperties, that.storageProperties) &&
                Objects.equals(maxScannedFileSize, that.maxScannedFileSize);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableType, snapshotId, tableSchemaJson, sortOrder, partitionSpecJson, formatVersion, unenforcedPredicate, enforcedPredicate,
                projectedColumns, nameMappingJson, tableLocation, storageProperties, retryMode, updatedColumns, recordScannedFiles, maxScannedFileSize);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(getSchemaTableNameWithType().toString());
        snapshotId.ifPresent(snapshotId -> builder.append("@").append(snapshotId));
        if (enforcedPredicate.isNone()) {
            builder.append(" constraint=FALSE");
        }
        else if (!enforcedPredicate.isAll()) {
            builder.append(" constraint on ");
            builder.append(enforcedPredicate.getDomains().orElseThrow().keySet().stream()
                    .map(IcebergColumnHandle::getQualifiedName)
                    .collect(joining(", ", "[", "]")));
        }
        return builder.toString();
    }
}
