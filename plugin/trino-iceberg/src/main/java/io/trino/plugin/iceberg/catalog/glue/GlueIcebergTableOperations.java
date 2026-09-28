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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.ValidationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.getTableType;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getMaterializedViewTableInput;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;

public class GlueIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private static final Logger log = Logger.get(GlueIcebergTableOperations.class);

    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final StatsRecordingGlueClient glueClient;
    private final GetGlueTable getGlueTable;

    @Nullable
    private String glueVersionId;

    protected GlueIcebergTableOperations(
            TypeManager typeManager,
            boolean cacheTableMetadata,
            StatsRecordingGlueClient glueClient,
            GetGlueTable getGlueTable,
            FileIO fileIo,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        super(fileIo, session, database, table, owner, location, encryptionManagerFactory);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = cacheTableMetadata;
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.getGlueTable = requireNonNull(getGlueTable, "getGlueTable is null");
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        boolean isMaterializedViewStorageTable = isMaterializedViewStorage(tableName);

        Table table;
        if (isMaterializedViewStorageTable) {
            table = getTable(database, tableNameFrom(tableName), invalidateCaches);
        }
        else {
            table = getTable(database, tableName, invalidateCaches);
        }
        glueVersionId = table.versionId();

        String tableType = getTableType(table);
        Map<String, String> parameters = table.parameters();
        if (!isMaterializedViewStorageTable && (isTrinoView(tableType, parameters) || isTrinoMaterializedView(tableType, parameters))) {
            // this is a Hive view or Trino/Presto view, or Trino materialized view, hence not a table
            // TODO table operations should not be constructed for views (remove exception-driven code path)
            throw new TableNotFoundException(getSchemaTableName());
        }
        if (!isMaterializedViewStorageTable && !isIcebergTable(parameters)) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataLocation = parameters.get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION_PROP, getSchemaTableName()));
        }
        return metadataLocation;
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version.isEmpty(), "commitNewTable called on a table which already exists");
        String newMetadataLocation = writeNewMetadata(metadata, 0);
        TableInput tableInput = getTableInput(typeManager, tableName, owner, metadata, metadata.location(), newMetadataLocation, ImmutableMap.of(), cacheTableMetadata);

        try {
            glueClient.createTable(database, tableInput);
        }
        catch (GlueException e) {
            switch (e) {
                // A retried request can observe the table that the first (successful) attempt created and report
                // AlreadyExists. Deleting the new metadata file in that case would corrupt the just-created table, so
                // the actual commit outcome is verified before any cleanup is performed.
                case AlreadyExistsException _ -> {
                    switch (checkNewTableCommitStatus(newMetadataLocation, metadata.uuid(), this::committedMetadataLocation)) {
                        case SUCCESS -> {
                            log.warn(e, "Received an error from Glue while creating table %s, but the table was actually created; treating the commit as successful", getSchemaTableName());
                            shouldRefresh = true;
                            return;
                        }
                        // Cannot determine whether the create was applied. Preserve every new file so the table
                        // remains recoverable; CommitStateUnknownException stops the Iceberg transaction layer from
                        // cleaning them up.
                        case UNKNOWN -> throw new CommitStateUnknownException(e);
                        case FAILURE -> throw deleteOrphanedMetadata(newMetadataLocation, e);
                    }
                }
                // clean up metadata files corresponding to the current transaction
                case EntityNotFoundException _,
                     InvalidInputException _,
                     ResourceNumberLimitExceededException _,
                     ValidationException _ -> throw deleteOrphanedMetadata(newMetadataLocation, e);
                default -> {}
            }
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Cannot commit table creation", e);
        }
        shouldRefresh = true;
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        commitTableUpdate(
                getTable(database, tableName, false),
                metadata,
                (table, newMetadataLocation) ->
                        getTableInput(
                                typeManager,
                                tableName,
                                owner,
                                metadata,
                                Optional.ofNullable(table.storageDescriptor()).map(StorageDescriptor::location).orElse(null),
                                newMetadataLocation,
                                ImmutableMap.of(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation),
                                cacheTableMetadata));
    }

    @Override
    protected void commitMaterializedView(TableMetadata base, TableMetadata metadata)
    {
        commitTableUpdate(
                getTable(database, tableNameFrom(tableName), false),
                metadata,
                (table, newMetadataLocation) -> {
                    if (materializedViewCommitData.isPresent()) {
                        table = table.toBuilder()
                                .viewOriginalText(materializedViewCommitData.get().viewOriginalText())
                                .parameters(materializedViewCommitData.get().parameters())
                                .build();
                    }

                    Map<String, String> parameters = new HashMap<>(table.parameters());
                    parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);
                    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation);

                    return getMaterializedViewTableInput(
                            table.name(),
                            table.viewOriginalText(),
                            table.owner(),
                            parameters);
                });
    }

    private void commitTableUpdate(Table table, TableMetadata metadata, BiFunction<Table, String, TableInput> tableUpdateFunction)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version.orElseThrow() + 1);
        TableInput tableInput = tableUpdateFunction.apply(table, newMetadataLocation);

        try {
            glueClient.updateTable(database, tableInput, Optional.ofNullable(glueVersionId));
        }
        catch (ConcurrentModificationException e) {
            // CommitFailedException is handled as a special case in the Iceberg library. This commit will automatically retry
            throw new CommitFailedException(e, "Failed to commit to Glue table: %s.%s", database, tableName);
        }
        catch (EntityNotFoundException | InvalidInputException | ResourceNumberLimitExceededException | ValidationException e) {
            // Signal a non-retriable commit failure and eventually clean up metadata files corresponding to the current transaction
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Cannot commit table update", e);
        }
        catch (RuntimeException e) {
            // Cannot determine whether the `updateTable` operation was successful,
            // regardless of the exception thrown (e.g. : timeout exception) or it actually failed
            throw new CommitStateUnknownException(e);
        }
        shouldRefresh = true;
    }

    private Optional<String> committedMetadataLocation()
    {
        try {
            return Optional.ofNullable(getTable(database, tableName, true).parameters().get(METADATA_LOCATION_PROP));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
    }

    private Table getTable(String database, String tableName, boolean invalidateCaches)
    {
        return getGlueTable.get(new SchemaTableName(database, tableName), invalidateCaches);
    }

    public interface GetGlueTable
    {
        Table get(SchemaTableName tableName, boolean invalidateCaches);
    }
}
