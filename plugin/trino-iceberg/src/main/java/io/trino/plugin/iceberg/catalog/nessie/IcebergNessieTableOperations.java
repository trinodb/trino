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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.catalog.nessie.IcebergNessieUtil.toIdentifier;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IcebergNessieTableOperations
        extends AbstractIcebergTableOperations
{
    private final NessieIcebergClient nessieClient;
    private IcebergTable table;

    protected IcebergNessieTableOperations(
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
        refreshNessieClient();
        return super.refresh();
    }

    private void refreshNessieClient()
    {
        try {
            nessieClient.refresh();
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to refresh as ref '%s' is no longer valid.", nessieClient.refName()), e);
        }
    }

    @Override
    public TableMetadata refresh(boolean invalidateCaches)
    {
        refreshNessieClient();
        return super.refresh(invalidateCaches);
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        table = nessieClient.table(toIdentifier(new SchemaTableName(database, tableName)));

        if (table == null) {
            throw new TableNotFoundException(getSchemaTableName());
        }

        return table.getMetadataLocation();
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version.isEmpty(), "commitNewTable called on a table which already exists");
        try {
            nessieClient.commitTable(null, metadata, writeNewMetadata(metadata, 0), table, toKey(new SchemaTableName(database, this.tableName)));
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Cannot commit: ref '%s' no longer exists", nessieClient.refName()), e);
        }
        catch (NessieConflictException e) {
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            throw new CommitFailedException(e, "Cannot commit: ref hash is out of date. Update the ref '%s' and try again", nessieClient.refName());
        }
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        verify(version.orElseThrow() >= 0, "commitToExistingTable called on a new table");
        try {
            nessieClient.commitTable(base, metadata, writeNewMetadata(metadata, version.getAsInt() + 1), table, toKey(new SchemaTableName(database, this.tableName)));
        }
        catch (NessieNotFoundException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Cannot commit: ref '%s' no longer exists", nessieClient.refName()), e);
        }
        catch (NessieConflictException e) {
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            throw new CommitFailedException(e, "Cannot commit: ref hash is out of date. Update the ref '%s' and try again", nessieClient.refName());
        }
        shouldRefresh = true;
    }

    private static ContentKey toKey(SchemaTableName tableName)
    {
        return ContentKey.of(Namespace.parse(tableName.getSchemaName()), tableName.getTableName());
    }
}
