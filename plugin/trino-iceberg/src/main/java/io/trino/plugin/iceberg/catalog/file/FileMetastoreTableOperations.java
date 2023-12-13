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
package io.trino.plugin.iceberg.catalog.file;

import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;

@NotThreadSafe
public class FileMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    public FileMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, metastore, session, database, table, owner, location);
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        Table currentTable = getTable();
        commitTableUpdate(currentTable, metadata, (table, newMetadataLocation) -> Table.builder(table)
                .apply(builder -> updateMetastoreTable(builder, metadata, newMetadataLocation, Optional.of(currentMetadataLocation)))
                .build());
    }

    @Override
    protected void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata)
    {
        Table materializedView = getTable(database, tableNameFrom(tableName));
        commitTableUpdate(materializedView, metadata, (table, newMetadataLocation) -> Table.builder(table)
                .apply(builder -> builder.setParameter(METADATA_LOCATION_PROP, newMetadataLocation).setParameter(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation))
                .build());
    }

    private void commitTableUpdate(Table table, TableMetadata metadata, BiFunction<Table, String, Table> tableUpdateFunction)
    {
        checkState(currentMetadataLocation != null, "No current metadata location for existing table");
        String metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (!currentMetadataLocation.equals(metadataLocation)) {
            throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                    currentMetadataLocation, metadataLocation, getSchemaTableName());
        }

        String newMetadataLocation = writeNewMetadata(metadata, version.orElseThrow() + 1);

        Table updatedTable = tableUpdateFunction.apply(table, newMetadataLocation);

        // todo privileges should not be replaced for an alter
        PrincipalPrivileges privileges = table.getOwner().map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);

        try {
            metastore.replaceTable(database, table.getTableName(), updatedTable, privileges);
        }
        catch (RuntimeException e) {
            if (e instanceof TrinoException trinoException &&
                    trinoException.getErrorCode() == HIVE_CONCURRENT_MODIFICATION_DETECTED.toErrorCode()) {
                // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
                throw new CommitFailedException(e, "Failed to replace table due to concurrent updates: %s.%s", database, tableName);
            }
            throw new CommitStateUnknownException(e);
        }
    }
}
