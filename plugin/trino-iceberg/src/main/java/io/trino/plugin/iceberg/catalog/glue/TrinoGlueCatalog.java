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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hive.HiveViewNotSupportedException;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergMaterializedViewDefinition;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.TRINO_CREATED_BY;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getViewInput;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.CatalogUtil.dropTableData;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class TrinoGlueCatalog
        extends AbstractTrinoCatalog
{
    private static final String PRESTO_VIEW_EXPANDED_TEXT_MARKER = HiveMetadata.PRESTO_VIEW_EXPANDED_TEXT_MARKER;
    private static final String TRINO_CREATED_BY_VALUE = "Trino iceberg connector";
    private static final String ICEBERG_MATERIALIZED_VIEW_COMMENT = "Presto Materialized View";
    private static final String VIEW_PREFIX = "/* Presto View: ";
    private static final String VIEW_SUFFIX = " */";
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ConnectorViewDefinition.class);

    private final HdfsEnvironment hdfsEnvironment;
    private final Optional<String> defaultSchemaLocation;
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;
    private final TypeManager typeManager;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoGlueCatalog(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            Optional<String> defaultSchemaLocation,
            boolean useUniqueTableLocation)
    {
        super(tableOperationsProvider, useUniqueTableLocation);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.defaultSchemaLocation = requireNonNull(defaultSchemaLocation, "defaultSchemaLocation is null");
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        try {
            return getPaginatedResults(
                    glueClient::getDatabases,
                    new GetDatabasesRequest(),
                    GetDatabasesRequest::setNextToken,
                    GetDatabasesResult::getNextToken,
                    stats.getGetDatabases())
                    .map(GetDatabasesResult::getDatabaseList)
                    .flatMap(List::stream)
                    .map(com.amazonaws.services.glue.model.Database::getName)
                    .collect(toImmutableList());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        try {
            stats.getDeleteDatabase().call(() ->
                    glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(namespace)));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        try {
            GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withName(namespace);
            Database database = stats.getGetDatabase().call(() ->
                    glueClient.getDatabase(getDatabaseRequest).getDatabase());
            ImmutableMap.Builder<String, Object> metadata = ImmutableMap.builder();
            if (database.getLocationUri() != null) {
                metadata.put(LOCATION_PROPERTY, database.getLocationUri());
            }
            if (database.getParameters() != null) {
                metadata.putAll(database.getParameters());
            }
            return metadata.buildOrThrow();
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(owner.getType() == PrincipalType.USER, "Owner type must be USER");
        checkArgument(owner.getName().equals(session.getUser()), "Explicit schema owner is not supported");

        try {
            stats.getCreateDatabase().call(() ->
                    glueClient.createDatabase(new CreateDatabaseRequest()
                            .withDatabaseInput(createDatabaseInput(namespace, properties))));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(namespace);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    private DatabaseInput createDatabaseInput(String namespace, Map<String, Object> properties)
    {
        DatabaseInput databaseInput = new DatabaseInput().withName(namespace);
        Object location = properties.get(LOCATION_PROPERTY);
        if (location != null) {
            databaseInput.setLocationUri((String) location);
        }

        return databaseInput;
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for Iceberg Glue catalogs");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        return listObjects(session, namespace, TrinoGlueCatalog::isTable);
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName table)
    {
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                table,
                ignore -> {
                    TableOperations operations = tableOperationsProvider.createTableOperations(
                            this,
                            session,
                            table.getSchemaName(),
                            table.getTableName(),
                            Optional.empty(),
                            Optional.empty());
                    return new BaseTable(operations, quotedTableName(table)).operations().current();
                });

        return getIcebergTableWithMetadata(
                this,
                tableOperationsProvider,
                session,
                table,
                metadata);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BaseTable table = (BaseTable) loadTable(session, schemaTableName);
        validateTableCanBeDropped(table);
        try {
            deleteTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        dropTableData(table.io(), table.operations().current());
        deleteTableDirectory(session, schemaTableName, hdfsEnvironment, new Path(table.location()));
    }

    @Override
    public Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties)
    {
        return newCreateTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                location,
                properties,
                Optional.of(session.getUser()));
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        boolean newTableCreated = false;
        try {
            GetTableRequest getTableRequest = new GetTableRequest()
                    .withDatabaseName(from.getSchemaName())
                    .withName(from.getTableName());
            com.amazonaws.services.glue.model.Table table = stats.getGetTable().call(() -> glueClient.getTable(getTableRequest).getTable());
            TableInput tableInput = getTableInput(to.getTableName(), Optional.ofNullable(table.getOwner()), table.getParameters());
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withDatabaseName(to.getSchemaName())
                    .withTableInput(tableInput);
            stats.getCreateTable().call(() -> glueClient.createTable(createTableRequest));
            newTableCreated = true;
            deleteTable(from.getSchemaName(), from.getTableName());
        }
        catch (RuntimeException e) {
            if (newTableCreated) {
                try {
                    deleteTable(to.getSchemaName(), to.getTableName());
                }
                catch (RuntimeException cleanupException) {
                    if (!cleanupException.equals(e)) {
                        e.addSuppressed(cleanupException);
                    }
                }
            }
            throw e;
        }
    }

    private void deleteTable(String schema, String table)
    {
        stats.getDeleteTable().call(() ->
                glueClient.deleteTable(new DeleteTableRequest()
                        .withDatabaseName(schema)
                        .withName(table)));
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest()
                .withName(schemaTableName.getSchemaName());
        String databaseLocation = stats.getGetDatabase().call(() ->
                glueClient.getDatabase(getDatabaseRequest)
                        .getDatabase()
                        .getLocationUri());

        String tableName = createNewTableName(schemaTableName.getTableName());

        Path location;
        if (databaseLocation == null) {
            if (defaultSchemaLocation.isEmpty()) {
                throw new TrinoException(
                        HIVE_DATABASE_LOCATION_ERROR,
                        format("Schema '%s' location cannot be determined. " +
                                        "Either set the 'location' property when creating the schema, or set the 'hive.metastore.glue.default-warehouse-dir' " +
                                        "catalog property.",
                                schemaTableName.getSchemaName()));
            }
            String schemaDirectoryName = schemaTableName.getSchemaName() + ".db";
            location = new Path(new Path(defaultSchemaLocation.get(), schemaDirectoryName), tableName);
        }
        else {
            location = new Path(databaseLocation, tableName);
        }

        return location.toString();
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setDatabaseName(schemaViewName.getSchemaName());
        createTableRequest.setTableInput(getViewInput(
                schemaViewName.getTableName(),
                Optional.of(session.getUser()),
                ImmutableMap.<String, String>builder()
                        .put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                        .put(TABLE_COMMENT, PRESTO_VIEW_COMMENT)
                        .put(PRESTO_VIEW_FLAG, "true")
                        .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                        .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                        // TODO: add presto version just like in Hive?
                        .buildOrThrow())
                .withViewOriginalText(encodeViewData(definition))
                .withViewExpandedText(PRESTO_VIEW_EXPANDED_TEXT_MARKER));

        // TODO: support replace

        glueClient.createTable(createTableRequest);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        try {
            deleteTable(schemaViewName.getSchemaName(), schemaViewName.getTableName());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return listObjects(session, namespace, TrinoGlueCatalog::isView);
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
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewIdentifier)
    {
        GetTableRequest getTableRequest = new GetTableRequest()
                .withDatabaseName(viewIdentifier.getSchemaName())
                .withName(viewIdentifier.getTableName());

        return Optional.ofNullable(stats.getGetTable().call(() -> {
            try {
                return glueClient.getTable(getTableRequest).getTable();
            }
            catch (EntityNotFoundException e) {
                return null;
            }
            catch (AmazonServiceException e) {
                throw new TrinoException(HIVE_METASTORE_ERROR, e);
            }
        }))
                .filter(TrinoGlueCatalog::isView)
                .map(view -> {
                    if (!isPrestoView(view.getParameters())) {
                        throw new HiveViewNotSupportedException(viewIdentifier);
                    }

                    ConnectorViewDefinition definition = decodeViewData(view.getViewOriginalText());
                    // use owner from table metadata if it exists
                    if (view.getOwner() != null && !definition.isRunAsInvoker()) {
                        definition = new ConnectorViewDefinition(
                                definition.getOriginalSql(),
                                definition.getCatalog(),
                                definition.getSchema(),
                                definition.getColumns(),
                                definition.getComment(),
                                Optional.ofNullable(view.getOwner()),
                                false);
                    }
                    return definition;
                });
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return listObjects(session, namespace, TrinoGlueCatalog::isMaterializedView);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + randomUUID().toString().replace("-", "");
        Map<String, Object> storageTableProperties = new HashMap<>(definition.getProperties());
        storageTableProperties.putIfAbsent(FILE_FORMAT_PROPERTY, DEFAULT_FILE_FORMAT_DEFAULT);

        SchemaTableName storageTable = new SchemaTableName(schemaViewName.getSchemaName(), storageTableName);
        List<ColumnMetadata> columns = definition.getColumns().stream()
                .map(column -> new ColumnMetadata(column.getName(), typeManager.getType(column.getType())))
                .collect(toImmutableList());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, storageTableProperties, Optional.empty());
        Transaction transaction = IcebergUtil.newCreateTableTransaction(this, tableMetadata, session);
        transaction.newAppend().commit();
        transaction.commitTransaction();

        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setDatabaseName(schemaViewName.getSchemaName());
        createTableRequest.setTableInput(getViewInput(
                schemaViewName.getTableName(),
                Optional.of(session.getUser()),
                ImmutableMap.<String, String>builder()
                        .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                        .put(STORAGE_TABLE, storageTableName)
                        .put(PRESTO_VIEW_FLAG, "true")
                        .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                        .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                        .buildOrThrow())
                .withViewOriginalText(encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(definition)))
                .withViewExpandedText(PRESTO_VIEW_EXPANDED_TEXT_MARKER));

        // TODO: support replace

        glueClient.createTable(createTableRequest);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        try {
            deleteTable(schemaViewName.getSchemaName(), schemaViewName.getTableName());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        GetTableRequest getTableRequest = new GetTableRequest()
                .withDatabaseName(schemaViewName.getSchemaName())
                .withName(schemaViewName.getTableName());

        return Optional.ofNullable(stats.getGetTable().call(() -> {
            try {
                return glueClient.getTable(getTableRequest).getTable();
            }
            catch (EntityNotFoundException e) {
                return null;
            }
            catch (AmazonServiceException e) {
                throw new TrinoException(HIVE_METASTORE_ERROR, e);
            }
        }))
                .filter(TrinoGlueCatalog::isMaterializedView)
                .map(materializedView -> {
                    String storageTable = materializedView.getParameters().get(STORAGE_TABLE);
                    checkState(storageTable != null, "Storage table missing in definition of materialized view " + schemaViewName);

                    IcebergMaterializedViewDefinition definition = decodeMaterializedViewData(Optional.ofNullable(materializedView.getViewOriginalText())
                            .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + schemaViewName)));

                    Table icebergTable = loadTable(session, new SchemaTableName(schemaViewName.getSchemaName(), storageTable));
                    ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
                    properties.put(FILE_FORMAT_PROPERTY, IcebergUtil.getFileFormat(icebergTable));
                    if (!icebergTable.spec().fields().isEmpty()) {
                        properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
                    }

                    // TODO: why catalog name is embedded in the view definition?
                    String catalogName = definition.getCatalog().orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No catalog defined in materialized view: " + schemaViewName));
                    return new ConnectorMaterializedViewDefinition(
                        definition.getOriginalSql(),
                        Optional.of(new CatalogSchemaTableName(catalogName, new SchemaTableName(schemaViewName.getSchemaName(), storageTable))),
                        definition.getCatalog(),
                        definition.getSchema(),
                        definition.getColumns().stream()
                                .map(column -> new ConnectorMaterializedViewDefinition.Column(column.getName(), column.getType()))
                                .collect(toImmutableList()),
                        definition.getComment(),
                        Optional.ofNullable(materializedView.getOwner()),
                        properties.buildOrThrow());
                });
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for Iceberg Glue catalogs");
    }

    // TODO: check if this logic can generalized over hive and iceberg connector
    private static ConnectorViewDefinition decodeViewData(String viewData)
    {
        checkCondition(viewData.startsWith(VIEW_PREFIX), HIVE_INVALID_VIEW_DATA, "View data missing prefix: %s", viewData);
        checkCondition(viewData.endsWith(VIEW_SUFFIX), HIVE_INVALID_VIEW_DATA, "View data missing suffix: %s", viewData);
        viewData = viewData.substring(VIEW_PREFIX.length());
        viewData = viewData.substring(0, viewData.length() - VIEW_SUFFIX.length());
        byte[] bytes = Base64.getDecoder().decode(viewData);
        return VIEW_CODEC.fromJson(bytes);
    }

    private List<SchemaTableName> listObjects(ConnectorSession session, Optional<String> namespace, Predicate<com.amazonaws.services.glue.model.Table> predicate)
    {
        try {
            List<String> namespaces = namespace.map(List::of).orElseGet(() -> listNamespaces(session));
            return namespaces.stream()
                    .flatMap(glueNamespace -> {
                        try {
                            return getPaginatedResults(
                                    glueClient::getTables,
                                    new GetTablesRequest().withDatabaseName(glueNamespace),
                                    GetTablesRequest::setNextToken,
                                    GetTablesResult::getNextToken,
                                    stats.getGetTables())
                                    .map(GetTablesResult::getTableList)
                                    .flatMap(List::stream)
                                    .filter(predicate)
                                    .map(table -> new SchemaTableName(glueNamespace, table.getName()));
                        }
                        catch (EntityNotFoundException e) {
                            // Namespace may have been deleted
                            return Stream.empty();
                        }
                    })
                    .collect(toImmutableList());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    private static Boolean isTable(com.amazonaws.services.glue.model.Table table)
    {
        return !table.getTableType().equals(TableType.VIRTUAL_VIEW.toString());
    }

    private static Boolean isView(com.amazonaws.services.glue.model.Table table)
    {
        return table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) &&
                PRESTO_VIEW_COMMENT.equals(table.getParameters().get(TABLE_COMMENT));
    }

    private static Boolean isMaterializedView(com.amazonaws.services.glue.model.Table table)
    {
        return table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) &&
                isPrestoView(table.getParameters()) &&
                isHiveOrPrestoView(table.getTableType()) &&
                table.getParameters().containsKey(STORAGE_TABLE);
    }
}
