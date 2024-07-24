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
package io.trino.plugin.iceberg.catalog.hms;

import io.airlift.log.Logger;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.hive.metastore.AcidTransactionOwner;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.IcebergUtil.fixBrokenMetadataLocation;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;

@NotThreadSafe
public class HiveMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    private static final Logger log = Logger.get(HiveMetastoreTableOperations.class);
    private final ThriftMetastore thriftMetastore;

    public HiveMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ThriftMetastore thriftMetastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, metastore, session, database, table, owner, location);
        this.thriftMetastore = requireNonNull(thriftMetastore, "thriftMetastore is null");
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
    protected final void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata)
    {
        Table materializedView = getTable(database, tableNameFrom(tableName));
        commitTableUpdate(materializedView, metadata, (table, newMetadataLocation) -> Table.builder(table)
                .apply(builder -> builder
                        .setParameter(METADATA_LOCATION_PROP, newMetadataLocation)
                        .setParameter(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation))
                .build());
    }

    private void commitTableUpdate(Table table, TableMetadata metadata, BiFunction<Table, String, Table> tableUpdateFunction)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version.orElseThrow() + 1);
        long lockId = thriftMetastore.acquireTableExclusiveLock(
                new AcidTransactionOwner(session.getUser()),
                session.getQueryId(),
                table.getDatabaseName(),
                table.getTableName());
        try {
            Table currentTable = fromMetastoreApiTable(thriftMetastore.getTable(database, table.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(getSchemaTableName())));

            checkState(currentMetadataLocation != null, "No current metadata location for existing table");
            String metadataLocation = fixBrokenMetadataLocation(currentTable.getParameters().get(METADATA_LOCATION_PROP));
            if (!currentMetadataLocation.equals(metadataLocation)) {
                throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                        currentMetadataLocation, metadataLocation, getSchemaTableName());
            }

            Table updatedTable = tableUpdateFunction.apply(table, newMetadataLocation);

            // todo privileges should not be replaced for an alter
            PrincipalPrivileges privileges = table.getOwner().map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
            try {
                metastore.replaceTable(table.getDatabaseName(), table.getTableName(), updatedTable, privileges);
            }
            catch (RuntimeException e) {
                // Cannot determine whether the `replaceTable` operation was successful,
                // regardless of the exception thrown (e.g. : timeout exception) or it actually failed
                throw new CommitStateUnknownException(e);
            }
        }
        finally {
            try {
                thriftMetastore.releaseTableLock(lockId);
            }
            catch (RuntimeException e) {
                // Release lock step has failed. Not throwing this exception, after commit has already succeeded.
                // So, that underlying iceberg API will not do the metadata cleanup, otherwise table will be in unusable state.
                // If configured and supported, the unreleased lock will be automatically released by the metastore after not hearing a heartbeat for a while,
                // or otherwise it might need to be manually deleted from the metastore backend storage.
                log.error(e, "Failed to release lock %s when committing to table %s", lockId, table.getTableName());
            }
        }

        shouldRefresh = true;
    }
}
