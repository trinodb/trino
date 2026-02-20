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

import com.google.common.collect.ImmutableMap;
import io.trino.annotation.NotThreadSafe;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.Nullable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH;
import static org.apache.iceberg.TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

@NotThreadSafe
public class FileMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    private final Optional<KeyManagementClient> keyManagementClient;
    @Nullable
    private String tableKeyId;
    private int encryptionDekLength = ENCRYPTION_DEK_LENGTH_DEFAULT;
    private EncryptionManager encryptionManager;

    public FileMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location,
            Optional<KeyManagementClient> keyManagementClient)
    {
        super(fileIo, metastore, session, database, table, owner, location);
        this.keyManagementClient = keyManagementClient;
    }

    @Override
    public EncryptionManager encryption()
    {
        if (encryptionManager != null) {
            return encryptionManager;
        }

        if (tableKeyId == null) {
            return PlaintextEncryptionManager.instance();
        }

        KeyManagementClient kmsClient = keyManagementClient.orElseThrow(() -> new RuntimeException("Can't create encryption manager, because key management client is not set"));
        encryptionManager = EncryptionUtil.createEncryptionManager(
                ImmutableMap.of(
                        ENCRYPTION_TABLE_KEY, tableKeyId,
                        ENCRYPTION_DEK_LENGTH, Integer.toString(encryptionDekLength)),
                kmsClient);
        return encryptionManager;
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        String refreshedLocation = super.getRefreshedLocation(invalidateCaches);
        updateEncryptionPropertiesFromMetastore();
        return refreshedLocation;
    }

    @Override
    public void initializeFromMetadata(TableMetadata tableMetadata)
    {
        super.initializeFromMetadata(tableMetadata);
        updateEncryptionPropertiesFromMetastore();
    }

    private void updateEncryptionPropertiesFromMetastore()
    {
        Table table = isMaterializedViewStorage(tableName) ? getTable(database, tableNameFrom(tableName)) : getTable();
        String newTableKeyId = table.getParameters().get(ENCRYPTION_TABLE_KEY);
        String dekLength = table.getParameters().get(ENCRYPTION_DEK_LENGTH);
        int newEncryptionDekLength = dekLength == null ? ENCRYPTION_DEK_LENGTH_DEFAULT : Integer.parseInt(dekLength);
        if (!Objects.equals(tableKeyId, newTableKeyId) || encryptionDekLength != newEncryptionDekLength) {
            tableKeyId = newTableKeyId;
            encryptionDekLength = newEncryptionDekLength;
            encryptionManager = null;
        }
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
            metastore.replaceTable(database, table.getTableName(), updatedTable, privileges, ImmutableMap.of());
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
