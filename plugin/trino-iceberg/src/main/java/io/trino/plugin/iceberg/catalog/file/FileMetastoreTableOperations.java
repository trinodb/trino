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

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;

@NotThreadSafe
public class FileMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    public FileMetastoreTableOperations(
            FileIO fileIo,
            HiveMetastore metastore,
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

        checkState(currentMetadataLocation != null, "No current metadata location for existing table");
        String metadataLocation = currentTable.getParameters().get(METADATA_LOCATION_PROP);
        if (!currentMetadataLocation.equals(metadataLocation)) {
            throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                    currentMetadataLocation, metadataLocation, getSchemaTableName());
        }

        String newMetadataLocation = writeNewMetadata(metadata, version + 1);

        Table table = Table.builder(currentTable)
                .setDataColumns(toHiveColumns(metadata.schema().columns()))
                .withStorage(storage -> storage.setLocation(metadata.location()))
                .setParameter(METADATA_LOCATION_PROP, newMetadataLocation)
                .setParameter(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation)
                .build();

        // todo privileges should not be replaced for an alter
        PrincipalPrivileges privileges = table.getOwner().map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
        metastore.replaceTable(database, tableName, table, privileges);
    }
}
