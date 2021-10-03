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

import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.iceberg.catalog.AbstractMetastoreTableOperations;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class HiveMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    private final ThriftMetastore thriftMetastore;

    public HiveMetastoreTableOperations(
            FileIO fileIo,
            HiveMetastore metastore,
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
        String newMetadataLocation = writeNewMetadata(metadata, version + 1);
        HiveIdentity identity = new HiveIdentity(session.getIdentity());

        long lockId = thriftMetastore.acquireTableExclusiveLock(
                identity,
                session.getQueryId(),
                database,
                tableName);
        try {
            Table table;
            try {
                Table currentTable = fromMetastoreApiTable(thriftMetastore.getTable(identity, database, tableName)
                        .orElseThrow(() -> new TableNotFoundException(getSchemaTableName())));

                checkState(currentMetadataLocation != null, "No current metadata location for existing table");
                String metadataLocation = currentTable.getParameters().get(METADATA_LOCATION);
                if (!currentMetadataLocation.equals(metadataLocation)) {
                    throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                            currentMetadataLocation, metadataLocation, getSchemaTableName());
                }

                table = Table.builder(currentTable)
                        .setDataColumns(toHiveColumns(metadata.schema().columns()))
                        .withStorage(storage -> storage.setLocation(metadata.location()))
                        .setParameter(METADATA_LOCATION, newMetadataLocation)
                        .setParameter(PREVIOUS_METADATA_LOCATION, currentMetadataLocation)
                        .build();
            }
            catch (RuntimeException e) {
                try {
                    io().deleteFile(newMetadataLocation);
                }
                catch (RuntimeException ex) {
                    e.addSuppressed(ex);
                }
                throw e;
            }

            // todo privileges should not be replaced for an alter
            PrincipalPrivileges privileges = owner.isEmpty() && table.getOwner().isPresent() ? NO_PRIVILEGES : buildInitialPrivilegeSet(table.getOwner().get());
            metastore.replaceTable(database, tableName, table, privileges);
        }
        finally {
            thriftMetastore.releaseTableLock(identity, lockId);
        }

        shouldRefresh = true;
    }
}
