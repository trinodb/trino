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
package io.trino.plugin.iceberg.catalog.nessie;

import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.model.IcebergTable;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class NessieIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final NessieIcebergClient nessieClient;

    protected NessieIcebergTableOperations(
            NessieIcebergClient nessieClient,
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.nessieClient = requireNonNull(nessieClient, "nessieClient is null");
    }

    @Override
    public TableMetadata refresh()
    {
        nessieClient.refreshReference();
        return super.refresh();
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        IcebergTable table = nessieClient.loadTable(new SchemaTableName(database, tableName));

        if (table == null) {
            throw new TableNotFoundException(getSchemaTableName());
        }

        return table.getMetadataLocation();
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version == -1, "commitNewTable called on a table which already exists");
        nessieClient.commitTable(metadata, new SchemaTableName(database, this.tableName), writeNewMetadata(metadata, 0), session.getUser());
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        verify(version >= 0, "commitToExistingTable called on a new table");
        nessieClient.commitTable(metadata, new SchemaTableName(database, this.tableName), writeNewMetadata(metadata, version + 1), session.getUser());
        shouldRefresh = true;
    }
}
