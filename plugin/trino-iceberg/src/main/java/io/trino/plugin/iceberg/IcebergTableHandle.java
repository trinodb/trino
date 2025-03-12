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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.units.DataSize;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private final CatalogHandle catalog;
    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final Optional<Long> snapshotId;
    private final String tableSchemaJson;
    // Empty means the partitioning spec is not known (can be the case for certain time travel queries).
    private final Optional<String> partitionSpecJson;
    private final int formatVersion;
    private final String tableLocation;
    private final Map<String, String> storageProperties;

    // Filter used during split generation and table scan, but not required to be strictly enforced by Iceberg Connector
    private final TupleDomain<IcebergColumnHandle> unenforcedPredicate;

    // Filter guaranteed to be enforced by Iceberg connector
    private final TupleDomain<IcebergColumnHandle> enforcedPredicate;

    // Columns that are present in {@link Constraint#predicate()} applied on the table scan
    private final Set<IcebergColumnHandle> constraintColumns;

    // semantically limit is applied after enforcedPredicate
    private final OptionalLong limit;

    private final Set<IcebergColumnHandle> projectedColumns;
    private final Optional<String> nameMappingJson;

    // Coordinator-only - table partitioning applied to the table splits if available and active
    private final Optional<IcebergTablePartitioning> tablePartitioning;

    // OPTIMIZE only. Coordinator-only
    private final boolean recordScannedFiles;
    private final Optional<DataSize> maxScannedFileSize;

    // ANALYZE only. Coordinator-only
    private final Optional<Boolean> forAnalyze;

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static IcebergTableHandle fromJsonForDeserializationOnly(
            @JsonProperty("catalog") CatalogHandle catalog,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("tableSchemaJson") String tableSchemaJson,
            @JsonProperty("partitionSpecJson") Optional<String> partitionSpecJson,
            @JsonProperty("formatVersion") int formatVersion,
            @JsonProperty("unenforcedPredicate") TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            @JsonProperty("enforcedPredicate") TupleDomain<IcebergColumnHandle> enforcedPredicate,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("projectedColumns") Set<IcebergColumnHandle> projectedColumns,
            @JsonProperty("nameMappingJson") Optional<String> nameMappingJson,
            @JsonProperty("tableLocation") String tableLocation,
            @JsonProperty("storageProperties") Map<String, String> storageProperties)
    {
        return new IcebergTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                Optional.empty(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty());
    }

    public IcebergTableHandle(
            CatalogHandle catalog,
            String schemaName,
            String tableName,
            TableType tableType,
            Optional<Long> snapshotId,
            String tableSchemaJson,
            Optional<String> partitionSpecJson,
            int formatVersion,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            OptionalLong limit,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            String tableLocation,
            Map<String, String> storageProperties,
            Optional<IcebergTablePartitioning> tablePartitioning,
            boolean recordScannedFiles,
            Optional<DataSize> maxScannedFileSize,
            Set<IcebergColumnHandle> constraintColumns,
            Optional<Boolean> forAnalyze)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.tableSchemaJson = requireNonNull(tableSchemaJson, "schemaJson is null");
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.formatVersion = formatVersion;
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.enforcedPredicate = requireNonNull(enforcedPredicate, "enforcedPredicate is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.nameMappingJson = requireNonNull(nameMappingJson, "nameMappingJson is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.tablePartitioning = requireNonNull(tablePartitioning, "tablePartitioning is null");
        this.recordScannedFiles = recordScannedFiles;
        this.maxScannedFileSize = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null");
        this.constraintColumns = ImmutableSet.copyOf(requireNonNull(constraintColumns, "constraintColumns is null"));
        this.forAnalyze = requireNonNull(forAnalyze, "forAnalyze is null");
    }

    @JsonProperty
    public CatalogHandle getCatalog()
    {
        return catalog;
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
    public OptionalLong getLimit()
    {
        return limit;
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

    /**
     * Get the partitioning for the table splits.
     */
    @JsonIgnore
    public Optional<IcebergTablePartitioning> getTablePartitioning()
    {
        return tablePartitioning;
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

    @JsonIgnore
    public Set<IcebergColumnHandle> getConstraintColumns()
    {
        return constraintColumns;
    }

    @JsonIgnore
    public Optional<Boolean> getForAnalyze()
    {
        return forAnalyze;
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
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                tablePartitioning,
                recordScannedFiles,
                maxScannedFileSize,
                constraintColumns,
                forAnalyze);
    }

    public IcebergTableHandle forAnalyze()
    {
        return new IcebergTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                tablePartitioning,
                recordScannedFiles,
                maxScannedFileSize,
                constraintColumns,
                Optional.of(true));
    }

    public IcebergTableHandle forOptimize(boolean recordScannedFiles, DataSize maxScannedFileSize)
    {
        return new IcebergTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                tablePartitioning,
                recordScannedFiles,
                Optional.of(maxScannedFileSize),
                constraintColumns,
                forAnalyze);
    }

    public IcebergTableHandle withTablePartitioning(Optional<IcebergTablePartitioning> requiredTablePartitioning)
    {
        return new IcebergTableHandle(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                requiredTablePartitioning,
                recordScannedFiles,
                maxScannedFileSize,
                constraintColumns,
                forAnalyze);
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
                Objects.equals(catalog, that.catalog) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                tableType == that.tableType &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(tableSchemaJson, that.tableSchemaJson) &&
                Objects.equals(partitionSpecJson, that.partitionSpecJson) &&
                formatVersion == that.formatVersion &&
                Objects.equals(unenforcedPredicate, that.unenforcedPredicate) &&
                Objects.equals(enforcedPredicate, that.enforcedPredicate) &&
                Objects.equals(limit, that.limit) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(nameMappingJson, that.nameMappingJson) &&
                Objects.equals(tableLocation, that.tableLocation) &&
                Objects.equals(storageProperties, that.storageProperties) &&
                Objects.equals(maxScannedFileSize, that.maxScannedFileSize) &&
                Objects.equals(constraintColumns, that.constraintColumns) &&
                Objects.equals(forAnalyze, that.forAnalyze);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                catalog,
                schemaName,
                tableName,
                tableType,
                snapshotId,
                tableSchemaJson,
                partitionSpecJson,
                formatVersion,
                unenforcedPredicate,
                enforcedPredicate,
                limit,
                projectedColumns,
                nameMappingJson,
                tableLocation,
                storageProperties,
                recordScannedFiles,
                maxScannedFileSize,
                constraintColumns,
                forAnalyze);
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
        limit.ifPresent(limit -> builder.append(" LIMIT ").append(limit));
        return builder.toString();
    }
}
