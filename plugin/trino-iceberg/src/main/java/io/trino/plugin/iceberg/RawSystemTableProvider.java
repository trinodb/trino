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

import com.google.common.base.VerifyException;
import com.google.inject.Inject;
import io.trino.plugin.base.connector.SystemTableProvider;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTableName.isDataTable;
import static io.trino.plugin.iceberg.IcebergTableName.isIcebergTableName;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static java.util.Objects.requireNonNull;

public class RawSystemTableProvider
        implements SystemTableProvider
{
    private final TypeManager typeManager;

    @Inject
    public RawSystemTableProvider(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<SchemaTableName> getSourceTableName(SchemaTableName table)
    {
        try {
            String name = tableNameFrom(table.getTableName());
            return Optional.of(new SchemaTableName(table.getSchemaName(), name));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorMetadata metadata, ConnectorSession session, SchemaTableName tableName)
    {
        IcebergMetadata icebergMetadata = (IcebergMetadata) metadata;
        if (!isIcebergTableName(tableName.getTableName()) || isDataTable(tableName.getTableName()) || isMaterializedViewStorage(tableName.getTableName())) {
            return Optional.empty();
        }

        // Only when dealing with an actual system table proceed to retrieve the base table for the system table
        String name = tableNameFrom(tableName.getTableName());
        Table table;
        try {
            table = icebergMetadata.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
        catch (UnknownTableTypeException e) {
            // avoid dealing with non Iceberg tables
            return Optional.empty();
        }

        TableType tableType = IcebergTableName.tableTypeFrom(tableName.getTableName());
        return switch (tableType) {
            case DATA, MATERIALIZED_VIEW_STORAGE ->
                    throw new VerifyException("Unexpected table type: " + tableType); // Handled above.
            case HISTORY -> Optional.of(new HistoryTable(tableName, table));
            case METADATA_LOG_ENTRIES -> Optional.of(new MetadataLogEntriesTable(tableName, table));
            case SNAPSHOTS -> Optional.of(new SnapshotsTable(tableName, typeManager, table));
            case PARTITIONS ->
                    Optional.of(new PartitionTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case MANIFESTS -> Optional.of(new ManifestsTable(tableName, table, getCurrentSnapshotId(table)));
            case FILES -> Optional.of(new FilesTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case PROPERTIES -> Optional.of(new PropertiesTable(tableName, table));
            case REFS -> Optional.of(new RefsTable(tableName, table));
        };
    }

    private Optional<Long> getCurrentSnapshotId(Table table)
    {
        return Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
    }
}
