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
package io.trino.plugin.iceberg.catalog;

import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergMaterializedViewDefinition;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergMaterializedViewAdditionalProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewAdditionalProperties.getStorageSchema;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.commit;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.Transactions.createTableTransaction;

public abstract class AbstractTrinoCatalog
        implements TrinoCatalog
{
    public static final String TRINO_CREATED_BY_VALUE = "Trino Iceberg connector";
    protected static final String TRINO_CREATED_BY = HiveMetadata.TRINO_CREATED_BY;
    protected static final String PRESTO_QUERY_ID_NAME = HiveMetadata.PRESTO_QUERY_ID_NAME;

    private final CatalogName catalogName;
    private final TypeManager typeManager;
    protected final IcebergTableOperationsProvider tableOperationsProvider;
    private final boolean useUniqueTableLocation;

    protected AbstractTrinoCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            boolean useUniqueTableLocation)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        icebergTable.updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (SchemaTableName name : listViews(session, namespace)) {
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.buildOrThrow();
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        try {
            return Failsafe.with(RetryPolicy.builder()
                            .withMaxAttempts(10)
                            .withBackoff(1, 5_000, ChronoUnit.MILLIS, 4)
                            .withMaxDuration(Duration.ofSeconds(30))
                            .abortOn(failure -> !(failure instanceof MaterializedViewMayBeBeingRemovedException))
                            .build())
                    .get(() -> doGetMaterializedView(session, schemaViewName));
        }
        catch (MaterializedViewMayBeBeingRemovedException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    protected abstract Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName);

    protected Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    protected String createNewTableName(String baseTableName)
    {
        String tableName = baseTableName;
        if (useUniqueTableLocation) {
            tableName += "-" + randomUUID().toString().replace("-", "");
        }
        return tableName;
    }

    protected void deleteTableDirectory(TrinoFileSystem fileSystem, SchemaTableName schemaTableName, String tableLocation)
    {
        try {
            fileSystem.deleteDirectory(tableLocation);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", tableLocation, schemaTableName), e);
        }
    }

    protected SchemaTableName createMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + randomUUID().toString().replace("-", "");
        Map<String, Object> storageTableProperties = new HashMap<>(definition.getProperties());
        storageTableProperties.putIfAbsent(FILE_FORMAT_PROPERTY, DEFAULT_FILE_FORMAT_DEFAULT);

        String storageSchema = getStorageSchema(definition.getProperties()).orElse(viewName.getSchemaName());
        SchemaTableName storageTable = new SchemaTableName(storageSchema, storageTableName);
        List<ColumnMetadata> columns = definition.getColumns().stream()
                .map(column -> new ColumnMetadata(column.getName(), typeManager.getType(column.getType())))
                .collect(toImmutableList());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, storageTableProperties, Optional.empty());
        Transaction transaction = IcebergUtil.newCreateTableTransaction(this, tableMetadata, session);
        AppendFiles appendFiles = transaction.newAppend();
        commit(appendFiles, session);
        transaction.commitTransaction();
        return storageTable;
    }

    protected ConnectorMaterializedViewDefinition getMaterializedViewDefinition(
            Table icebergTable,
            Optional<String> owner,
            String viewOriginalText,
            SchemaTableName storageTableName)
    {
        IcebergMaterializedViewDefinition definition = decodeMaterializedViewData(viewOriginalText);
        return new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), storageTableName)),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(column -> new ConnectorMaterializedViewDefinition.Column(column.getName(), column.getType()))
                        .collect(toImmutableList()),
                definition.getComment(),
                owner,
                ImmutableMap.<String, Object>builder()
                        .putAll(getIcebergTableProperties(icebergTable))
                        .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                        .buildOrThrow());
    }

    protected Map<String, String> createMaterializedViewProperties(ConnectorSession session, SchemaTableName storageTableName)
    {
        return ImmutableMap.<String, String>builder()
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                .put(STORAGE_TABLE, storageTableName.getTableName())
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .buildOrThrow();
    }

    protected static class MaterializedViewMayBeBeingRemovedException
            extends RuntimeException
    {
        public MaterializedViewMayBeBeingRemovedException(Throwable cause)
        {
            super(requireNonNull(cause, "cause is null"));
        }
    }
}
