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
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.plugin.iceberg.serdes.IcebergTableWrapper;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TableType tableType;
    private final IcebergTableWrapper tableWrapper;
    private final Optional<Long> snapshotId;

    // Filter used during split generation and table scan, but not required to be strictly enforced by Iceberg Connector
    private final TupleDomain<IcebergColumnHandle> unenforcedPredicate;

    // Filter guaranteed to be enforced by Iceberg connector
    private final TupleDomain<IcebergColumnHandle> enforcedPredicate;

    private final Set<IcebergColumnHandle> projectedColumns;
    private final Optional<String> nameMappingJson;
    private final List<IcebergColumnHandle> updateColumns;

    // OPTIMIZE only. Coordinator-only
    private final boolean recordScannedFiles;
    private final Optional<DataSize> maxScannedFileSize;

    // cache table object from wrapper
    private final Table table;

    @JsonCreator
    public IcebergTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("table") IcebergTableWrapper tableWrapper,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("unenforcedPredicate") TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            @JsonProperty("enforcedPredicate") TupleDomain<IcebergColumnHandle> enforcedPredicate,
            @JsonProperty("projectedColumns") Set<IcebergColumnHandle> projectedColumns,
            @JsonProperty("nameMappingJson") Optional<String> nameMappingJson,
            @JsonProperty("updateColumns") List<IcebergColumnHandle> updateColumns)
    {
        this(
                schemaName,
                tableName,
                tableType,
                tableWrapper,
                snapshotId,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                updateColumns,
                false,
                Optional.empty());
    }

    public IcebergTableHandle(
            String schemaName,
            String tableName,
            TableType tableType,
            IcebergTableWrapper tableWrapper,
            Optional<Long> snapshotId,
            TupleDomain<IcebergColumnHandle> unenforcedPredicate,
            TupleDomain<IcebergColumnHandle> enforcedPredicate,
            Set<IcebergColumnHandle> projectedColumns,
            Optional<String> nameMappingJson,
            List<IcebergColumnHandle> updateColumns,
            boolean recordScannedFiles,
            Optional<DataSize> maxScannedFileSize)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.tableWrapper = requireNonNull(tableWrapper, "tableWrapper is null");
        this.table = tableWrapper.getTable();
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.unenforcedPredicate = requireNonNull(unenforcedPredicate, "unenforcedPredicate is null");
        this.enforcedPredicate = requireNonNull(enforcedPredicate, "enforcedPredicate is null");
        this.projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.nameMappingJson = requireNonNull(nameMappingJson, "nameMappingJson is null");
        this.updateColumns = requireNonNull(updateColumns, "updateColumns is null");
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

    @JsonProperty
    public IcebergTableWrapper getTableWrapper()
    {
        return tableWrapper;
    }

    @JsonIgnore
    public Table getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
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
    public List<IcebergColumnHandle> getUpdateColumns()
    {
        return updateColumns;
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
                tableWrapper,
                snapshotId,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                updateColumns,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public IcebergTableHandle withUpdateColumns(List<IcebergColumnHandle> updateColumns)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                tableWrapper,
                snapshotId,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                updateColumns,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public IcebergTableHandle withPredicates(TupleDomain<IcebergColumnHandle> unenforcedPredicate, TupleDomain<IcebergColumnHandle> enforcedPredicate)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                tableWrapper,
                snapshotId,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                updateColumns,
                recordScannedFiles,
                maxScannedFileSize);
    }

    public IcebergTableHandle forOptimize(boolean recordScannedFiles, DataSize maxScannedFileSize)
    {
        return new IcebergTableHandle(
                schemaName,
                tableName,
                tableType,
                tableWrapper,
                snapshotId,
                unenforcedPredicate,
                enforcedPredicate,
                projectedColumns,
                nameMappingJson,
                updateColumns,
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
                Objects.equals(unenforcedPredicate, that.unenforcedPredicate) &&
                Objects.equals(enforcedPredicate, that.enforcedPredicate) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(nameMappingJson, that.nameMappingJson) &&
                Objects.equals(maxScannedFileSize, that.maxScannedFileSize);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, tableType, snapshotId, unenforcedPredicate, enforcedPredicate, projectedColumns, nameMappingJson, recordScannedFiles, maxScannedFileSize);
    }

    @Override
    public String toString()
    {
        return getSchemaTableNameWithType() + snapshotId.map(v -> "@" + v).orElse("");
    }
}
