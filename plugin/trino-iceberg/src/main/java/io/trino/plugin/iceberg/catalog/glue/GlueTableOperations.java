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
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.ConcurrentModificationException;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class GlueTableOperations
        extends AbstractTableOperations
{
    private static final Logger log = Logger.get(GlueTableOperations.class);

    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;
    private final String catalogId;

    public GlueTableOperations(
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            String catalogId,
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.glueClient = glueClient;
        this.stats = stats;
        this.catalogId = catalogId;
    }

    @Override
    protected String getRefreshedLocation()
    {
        return stats.getGetTable().call(() -> {
            Table table = glueClient.getTable(new GetTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(database)
                    .withName(tableName)).getTable();

            if (isPrestoView(table) && isHiveOrPrestoView(table)) {
                // this is a Presto Hive view, hence not a table
                throw new TableNotFoundException(getSchemaTableName());
            }
            if (!isIcebergTable(table)) {
                throw new UnknownTableTypeException(getSchemaTableName());
            }

            String metadataLocation = table.getParameters().get(METADATA_LOCATION);
            if (metadataLocation == null) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION, getSchemaTableName()));
            }
            return metadataLocation;
        });
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version + 1);
        Map<String, String> parameters = ImmutableMap.of(
                TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
                METADATA_LOCATION, newMetadataLocation);
        TableInput tableInput = new TableInput()
                .withName(tableName)
                .withTableType(EXTERNAL_TABLE.name())
                .withOwner(owner.orElse(null))
                .withParameters(parameters);

        boolean succeeded = false;
        try {
            stats.getCreateTable().call(() -> {
                glueClient.createTable(new CreateTableRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(database)
                        .withTableInput(tableInput));
                return null;
            });
            succeeded = true;
        }
        catch (ConcurrentModificationException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Cannot commit %s because Glue detected concurrent update", getSchemaTableName()), e);
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(getSchemaTableName());
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Cannot commit %s due to unexpected exception", getSchemaTableName()), e);
        }
        finally {
            cleanupMetadataLocation(!succeeded, newMetadataLocation);
        }
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table update is not supported by Trino Glue catalog");
    }

    private boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    private boolean isHiveOrPrestoView(Table table)
    {
        return table.getTableType().equals(TableType.VIRTUAL_VIEW.name());
    }

    private boolean isIcebergTable(Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    private void cleanupMetadataLocation(boolean shouldCleanup, String metadataLocation)
    {
        if (shouldCleanup) {
            try {
                io().deleteFile(metadataLocation);
            }
            catch (RuntimeException ex) {
                log.error("Fail to cleanup metadata file at {}", metadataLocation, ex);
                throw ex;
            }
        }
    }
}
