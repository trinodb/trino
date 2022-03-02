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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class GlueIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;

    protected GlueIcebergTableOperations(
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    protected String getRefreshedLocation()
    {
        Table table = getTable();

        if (isPrestoView(table.getParameters()) && isHiveOrPrestoView(table.getTableType())) {
            // this is a Presto Hive view, hence not a table
            throw new TableNotFoundException(getSchemaTableName());
        }
        if (!isIcebergTable(table.getParameters())) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION_PROP, getSchemaTableName()));
        }
        return metadataLocation;
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version == -1, "commitNewTable called on a table which already exists");
        String newMetadataLocation = writeNewMetadata(metadata, 0);
        TableInput tableInput = getTableInput(tableName, owner, ImmutableMap.<String, String>builder()
                        .put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                        .put(METADATA_LOCATION_PROP, newMetadataLocation)
                        .buildOrThrow());

        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withDatabaseName(database)
                .withTableInput(tableInput);
        stats.getCreateTable().call(() -> glueClient.createTable(createTableRequest));
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version + 1);
        TableInput tableInput = getTableInput(tableName, owner, ImmutableMap.<String, String>builder()
                .put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                .put(METADATA_LOCATION_PROP, newMetadataLocation)
                .put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation)
                .buildOrThrow());

        Table table = getTable();

        checkState(currentMetadataLocation != null, "No current metadata location for existing table");
        String metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (!currentMetadataLocation.equals(metadataLocation)) {
            throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                    currentMetadataLocation, metadataLocation, getSchemaTableName());
        }

        UpdateTableRequest updateTableRequest = new UpdateTableRequest()
                .withDatabaseName(database)
                .withTableInput(tableInput);
        stats.getUpdateTable().call(() -> glueClient.updateTable(updateTableRequest));
        shouldRefresh = true;
    }

    private Table getTable()
    {
        try {
            GetTableRequest getTableRequest = new GetTableRequest()
                    .withDatabaseName(database)
                    .withName(tableName);
            return stats.getGetTable().call(() -> glueClient.getTable(getTableRequest).getTable());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(getSchemaTableName(), e);
        }
    }
}
