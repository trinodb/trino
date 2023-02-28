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
import com.amazonaws.services.glue.model.AccessDeniedException;
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
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TrinoViewUtil;
import io.trino.plugin.hive.ViewAlreadyExistsException;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.FileIO;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.TrinoViewUtil.createViewProperties;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableType;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableTypeNullable;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergMaterializedViewAdditionalProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getHiveCatalogName;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.plugin.iceberg.TrinoMetricsReporter.TRINO_METRICS_REPORTER;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getMaterializedViewTableInput;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getViewTableInput;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.CatalogUtil.dropTableData;

public class TrinoGlueCatalog
        extends AbstractTrinoCatalog
{
    private static final Logger LOG = Logger.get(TrinoGlueCatalog.class);

    private final String trinoVersion;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Optional<String> defaultSchemaLocation;
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> viewCache = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViewCache = new ConcurrentHashMap<>();

    public TrinoGlueCatalog(
            CatalogName catalogName,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            String trinoVersion,
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            Optional<String> defaultSchemaLocation,
            boolean useUniqueTableLocation)
    {
        super(catalogName, typeManager, tableOperationsProvider, useUniqueTableLocation);
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.defaultSchemaLocation = requireNonNull(defaultSchemaLocation, "defaultSchemaLocation is null");
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        if (!namespace.equals(namespace.toLowerCase(ENGLISH))) {
            // Currently, Trino schemas are always lowercase, so this one cannot exist (https://github.com/trinodb/trino/issues/17)
            // In fact, Glue stores database names lowercase only (but accepted mixed case on lookup).
            return false;
        }
        return stats.getGetDatabase().call(() -> {
            try {
                glueClient.getDatabase(new GetDatabaseRequest().withName(namespace));
                return true;
            }
            catch (EntityNotFoundException e) {
                return false;
            }
            catch (AmazonServiceException e) {
                throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
            }
        });
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

    private List<String> listNamespaces(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent()) {
            if (isHiveSystemSchema(namespace.get())) {
                // TODO https://github.com/trinodb/trino/issues/1559 information_schema should be handled by the engine fully
                return ImmutableList.of();
            }
            return ImmutableList.of(namespace.get());
        }
        return listNamespaces(session);
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
        properties.forEach((property, value) -> {
            switch (property) {
                case LOCATION_PROPERTY -> databaseInput.setLocationUri((String) value);
                default -> throw new IllegalArgumentException("Unrecognized property: " + property);
            }
        });

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
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        try {
            List<String> namespaces = listNamespaces(session, namespace);
            for (String glueNamespace : namespaces) {
                try {
                    // Add all tables from a namespace together, in case it is removed while fetching paginated results
                    tables.addAll(
                            getPaginatedResults(
                                    glueClient::getTables,
                                    new GetTablesRequest().withDatabaseName(glueNamespace),
                                    GetTablesRequest::setNextToken,
                                    GetTablesResult::getNextToken,
                                    stats.getGetTables())
                                    .map(GetTablesResult::getTableList)
                                    .flatMap(List::stream)
                                    .map(table -> new SchemaTableName(glueNamespace, table.getName()))
                                    .collect(toImmutableList()));
                }
                catch (EntityNotFoundException | AccessDeniedException e) {
                    // Namespace may have been deleted or permission denied
                }
            }
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
        return tables.build();
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName table)
    {
        if (viewCache.containsKey(table) || materializedViewCache.containsKey(table)) {
            throw new TableNotFoundException(table);
        }

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
                    return new BaseTable(operations, quotedTableName(table), TRINO_METRICS_REPORTER).operations().current();
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
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, table.location());
    }

    @Override
    public Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        return newCreateTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                sortOrder,
                location,
                properties,
                Optional.of(session.getUser()));
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName schemaTableName, String tableLocation, String metadataLocation)
            throws TrinoException
    {
        TableInput tableInput = getTableInput(schemaTableName.getTableName(), Optional.of(session.getUser()), ImmutableMap.of(METADATA_LOCATION_PROP, metadataLocation));
        createTable(schemaTableName.getSchemaName(), tableInput);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        com.amazonaws.services.glue.model.Table table = getTable(session, schemaTableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (!isIcebergTable(firstNonNull(table.getParameters(), ImmutableMap.of()))) {
            throw new UnknownTableTypeException(schemaTableName);
        }

        try {
            deleteTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        boolean newTableCreated = false;
        try {
            com.amazonaws.services.glue.model.Table table = getTable(session, from)
                    .orElseThrow(() -> new TableNotFoundException(from));
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

    private Optional<com.amazonaws.services.glue.model.Table> getTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            com.amazonaws.services.glue.model.Table table = stats.getGetTable().call(() ->
                    glueClient.getTable(new GetTableRequest()
                                    .withDatabaseName(schemaTableName.getSchemaName())
                                    .withName(schemaTableName.getTableName()))
                            .getTable());

            Map<String, String> parameters = firstNonNull(table.getParameters(), ImmutableMap.of());
            if (isIcebergTable(parameters) && !tableMetadataCache.containsKey(schemaTableName)) {
                if (viewCache.containsKey(schemaTableName) || materializedViewCache.containsKey(schemaTableName)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Glue table cache inconsistency. Table cannot also be a view/materialized view");
                }

                String metadataLocation = parameters.get(METADATA_LOCATION_PROP);
                try {
                    // Cache the TableMetadata while we have the Table retrieved anyway
                    TableOperations operations = tableOperationsProvider.createTableOperations(
                            this,
                            session,
                            schemaTableName.getSchemaName(),
                            schemaTableName.getTableName(),
                            Optional.empty(),
                            Optional.empty());
                    FileIO io = operations.io();
                    tableMetadataCache.put(schemaTableName, TableMetadataParser.read(io, io.newInputFile(metadataLocation)));
                }
                catch (RuntimeException e) {
                    LOG.warn(e, "Failed to cache table metadata from table at %s", metadataLocation);
                }
            }
            else if (isTrinoMaterializedView(getTableType(table), parameters)) {
                if (viewCache.containsKey(schemaTableName) || tableMetadataCache.containsKey(schemaTableName)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Glue table cache inconsistency. Materialized View cannot also be a table or view");
                }

                try {
                    createMaterializedViewDefinition(session, schemaTableName, table)
                            .ifPresent(materializedView -> materializedViewCache.put(schemaTableName, materializedView));
                }
                catch (RuntimeException e) {
                    LOG.warn(e, "Failed to cache materialized view from %s", schemaTableName);
                }
            }
            else if (isPrestoView(parameters) && !viewCache.containsKey(schemaTableName)) {
                if (materializedViewCache.containsKey(schemaTableName) || tableMetadataCache.containsKey(schemaTableName)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Glue table cache inconsistency. View cannot also be a materialized view or table");
                }

                try {
                    TrinoViewUtil.getView(schemaTableName,
                            Optional.ofNullable(table.getViewOriginalText()),
                            getTableType(table),
                            parameters,
                            Optional.ofNullable(table.getOwner()))
                            .ifPresent(viewDefinition -> viewCache.put(schemaTableName, viewDefinition));
                }
                catch (RuntimeException e) {
                    LOG.warn(e, "Failed to cache view from %s", schemaTableName);
                }
            }

            return Optional.of(table);
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
    }

    private void createTable(String schemaName, TableInput tableInput)
    {
        stats.getCreateTable().call(() ->
                glueClient.createTable(new CreateTableRequest()
                        .withDatabaseName(schemaName)
                        .withTableInput(tableInput)));
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
        // If a view is created between listing the existing view and calling createTable, retry
        TableInput viewTableInput = getViewTableInput(
                schemaViewName.getTableName(),
                encodeViewData(definition),
                session.getUser(),
                createViewProperties(session, trinoVersion, TRINO_CREATED_BY_VALUE));
        Failsafe.with(RetryPolicy.builder()
                .withMaxRetries(3)
                .withDelay(Duration.ofMillis(100))
                .abortIf(throwable -> !replace || throwable instanceof ViewAlreadyExistsException)
                .build())
                .run(() -> doCreateView(session, schemaViewName, viewTableInput, replace));
    }

    private void doCreateView(ConnectorSession session, SchemaTableName schemaViewName, TableInput viewTableInput, boolean replace)
    {
        Optional<com.amazonaws.services.glue.model.Table> existing = getTable(session, schemaViewName);
        if (existing.isPresent()) {
            if (!replace || !isPrestoView(firstNonNull(existing.get().getParameters(), ImmutableMap.of()))) {
                // TODO: ViewAlreadyExists is misleading if the name is used by a table https://github.com/trinodb/trino/issues/10037
                throw new ViewAlreadyExistsException(schemaViewName);
            }

            stats.getUpdateTable().call(() ->
                    glueClient.updateTable(new UpdateTableRequest()
                            .withDatabaseName(schemaViewName.getSchemaName())
                            .withTableInput(viewTableInput)));
            return;
        }

        try {
            stats.getCreateTable().call(() ->
                    glueClient.createTable(new CreateTableRequest()
                            .withDatabaseName(schemaViewName.getSchemaName())
                            .withTableInput(viewTableInput)));
        }
        catch (AlreadyExistsException e) {
            throw new ViewAlreadyExistsException(schemaViewName);
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        boolean newTableCreated = false;
        try {
            com.amazonaws.services.glue.model.Table existingView = getTable(session, source)
                    .orElseThrow(() -> new TableNotFoundException(source));
            viewCache.remove(source);
            TableInput viewTableInput = getViewTableInput(
                    target.getTableName(),
                    existingView.getViewOriginalText(),
                    existingView.getOwner(),
                    createViewProperties(session, trinoVersion, TRINO_CREATED_BY_VALUE));
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withDatabaseName(target.getSchemaName())
                    .withTableInput(viewTableInput);
            stats.getCreateTable().call(() -> glueClient.createTable(createTableRequest));
            newTableCreated = true;
            deleteTable(source.getSchemaName(), source.getTableName());
        }
        catch (Exception e) {
            if (newTableCreated) {
                try {
                    deleteTable(target.getSchemaName(), target.getTableName());
                }
                catch (Exception cleanupException) {
                    if (!cleanupException.equals(e)) {
                        e.addSuppressed(cleanupException);
                    }
                }
            }
            throw e;
        }
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        if (getView(session, schemaViewName).isEmpty()) {
            throw new ViewNotFoundException(schemaViewName);
        }

        try {
            viewCache.remove(schemaViewName);
            deleteTable(schemaViewName.getSchemaName(), schemaViewName.getTableName());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableList.Builder<SchemaTableName> views = ImmutableList.builder();
        try {
            List<String> namespaces = listNamespaces(session, namespace);
            for (String glueNamespace : namespaces) {
                try {
                    views.addAll(getPaginatedResults(
                            glueClient::getTables,
                            new GetTablesRequest().withDatabaseName(glueNamespace),
                            GetTablesRequest::setNextToken,
                            GetTablesResult::getNextToken,
                            stats.getGetTables())
                            .map(GetTablesResult::getTableList)
                            .flatMap(List::stream)
                            .filter(table -> isPrestoView(firstNonNull(table.getParameters(), ImmutableMap.of())))
                            .map(table -> new SchemaTableName(glueNamespace, table.getName()))
                            .collect(toImmutableList()));
                }
                catch (EntityNotFoundException | AccessDeniedException e) {
                    // Namespace may have been deleted or permission denied
                }
            }
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
        return views.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorViewDefinition cachedView = viewCache.get(viewName);
        if (cachedView != null) {
            return Optional.of(cachedView);
        }

        if (tableMetadataCache.containsKey(viewName) || materializedViewCache.containsKey(viewName)) {
            // Entries in these caches are not views
            return Optional.empty();
        }

        Optional<com.amazonaws.services.glue.model.Table> table = getTable(session, viewName);
        if (table.isEmpty()) {
            return Optional.empty();
        }
        com.amazonaws.services.glue.model.Table viewDefinition = table.get();
        return TrinoViewUtil.getView(
                viewName,
                Optional.ofNullable(viewDefinition.getViewOriginalText()),
                getTableType(viewDefinition),
                firstNonNull(viewDefinition.getParameters(), ImmutableMap.of()),
                Optional.ofNullable(viewDefinition.getOwner()));
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        ConnectorViewDefinition definition = getView(session, viewName)
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        ConnectorViewDefinition newDefinition = new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                comment,
                definition.getOwner(),
                definition.isRunAsInvoker());

        updateView(session, viewName, newDefinition);
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        ConnectorViewDefinition definition = getView(session, viewName)
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        ConnectorViewDefinition newDefinition = new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(currentViewColumn -> Objects.equals(columnName, currentViewColumn.getName()) ? new ConnectorViewDefinition.ViewColumn(currentViewColumn.getName(), currentViewColumn.getType(), comment) : currentViewColumn)
                        .collect(toImmutableList()),
                definition.getComment(),
                definition.getOwner(),
                definition.isRunAsInvoker());

        updateView(session, viewName, newDefinition);
    }

    private void updateView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition newDefinition)
    {
        TableInput viewTableInput = getViewTableInput(
                viewName.getTableName(),
                encodeViewData(newDefinition),
                session.getUser(),
                createViewProperties(session, trinoVersion, TRINO_CREATED_BY_VALUE));

        try {
            stats.getUpdateTable().call(() ->
                    glueClient.updateTable(new UpdateTableRequest()
                            .withDatabaseName(viewName.getSchemaName())
                            .withTableInput(viewTableInput)));
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableList.Builder<SchemaTableName> materializedViews = ImmutableList.builder();
        try {
            List<String> namespaces = listNamespaces(session, namespace);
            for (String glueNamespace : namespaces) {
                try {
                    materializedViews.addAll(getPaginatedResults(
                            glueClient::getTables,
                            new GetTablesRequest().withDatabaseName(glueNamespace),
                            GetTablesRequest::setNextToken,
                            GetTablesResult::getNextToken,
                            stats.getGetTables())
                            .map(GetTablesResult::getTableList)
                            .flatMap(List::stream)
                            .filter(table -> isTrinoMaterializedView(getTableType(table), firstNonNull(table.getParameters(), ImmutableMap.of())))
                            .map(table -> new SchemaTableName(glueNamespace, table.getName()))
                            .collect(toImmutableList()));
                }
                catch (EntityNotFoundException | AccessDeniedException e) {
                    // Namespace may have been deleted or permission denied
                }
            }
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
        return materializedViews.build();
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            boolean replace,
            boolean ignoreExisting)
    {
        Optional<com.amazonaws.services.glue.model.Table> existing = getTable(session, viewName);

        if (existing.isPresent()) {
            if (!isTrinoMaterializedView(getTableType(existing.get()), firstNonNull(existing.get().getParameters(), ImmutableMap.of()))) {
                throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Existing table is not a Materialized View: " + viewName);
            }
            if (!replace) {
                if (ignoreExisting) {
                    return;
                }
                throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
            }
        }

        // Create the storage table
        SchemaTableName storageTable = createMaterializedViewStorageTable(session, viewName, definition);
        // Create a view indicating the storage table
        TableInput materializedViewTableInput = getMaterializedViewTableInput(
                viewName.getTableName(),
                encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(definition)),
                session.getUser(),
                createMaterializedViewProperties(session, storageTable));

        if (existing.isPresent()) {
            try {
                stats.getUpdateTable().call(() ->
                        glueClient.updateTable(new UpdateTableRequest()
                                .withDatabaseName(viewName.getSchemaName())
                                .withTableInput(materializedViewTableInput)));
            }
            catch (RuntimeException e) {
                try {
                    // Update failed, clean up new storage table
                    dropTable(session, storageTable);
                }
                catch (RuntimeException suppressed) {
                    LOG.warn(suppressed, "Failed to drop new storage table '%s' for materialized view '%s'", storageTable, viewName);
                    if (e != suppressed) {
                        e.addSuppressed(suppressed);
                    }
                }
            }
            dropStorageTable(session, existing.get());
        }
        else {
            createTable(viewName.getSchemaName(), materializedViewTableInput);
        }
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        com.amazonaws.services.glue.model.Table view = getTable(session, viewName)
                .orElseThrow(() -> new MaterializedViewNotFoundException(viewName));

        if (!isTrinoMaterializedView(getTableType(view), firstNonNull(view.getParameters(), ImmutableMap.of()))) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Not a Materialized View: " + view.getDatabaseName() + "." + view.getName());
        }
        materializedViewCache.remove(viewName);
        dropStorageTable(session, view);
        deleteTable(view.getDatabaseName(), view.getName());
    }

    private void dropStorageTable(ConnectorSession session, com.amazonaws.services.glue.model.Table view)
    {
        Map<String, String> parameters = firstNonNull(view.getParameters(), ImmutableMap.of());
        String storageTableName = parameters.get(STORAGE_TABLE);
        if (storageTableName != null) {
            String storageSchema = Optional.ofNullable(parameters.get(STORAGE_SCHEMA))
                    .orElse(view.getDatabaseName());
            try {
                dropTable(session, new SchemaTableName(storageSchema, storageTableName));
            }
            catch (TrinoException e) {
                LOG.warn(e, "Failed to drop storage table '%s.%s' for materialized view '%s'", storageSchema, storageTableName, view.getName());
            }
        }
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorMaterializedViewDefinition materializedViewDefinition = materializedViewCache.get(viewName);
        if (materializedViewDefinition != null) {
            return Optional.of(materializedViewDefinition);
        }

        if (tableMetadataCache.containsKey(viewName) || viewCache.containsKey(viewName)) {
            // Entries in these caches are not materialized views.
            return Optional.empty();
        }

        Optional<com.amazonaws.services.glue.model.Table> maybeTable = getTable(session, viewName);
        if (maybeTable.isEmpty()) {
            return Optional.empty();
        }

        com.amazonaws.services.glue.model.Table table = maybeTable.get();
        if (!isTrinoMaterializedView(getTableType(table), firstNonNull(table.getParameters(), ImmutableMap.of()))) {
            return Optional.empty();
        }

        return createMaterializedViewDefinition(session, viewName, table);
    }

    private Optional<ConnectorMaterializedViewDefinition> createMaterializedViewDefinition(
            ConnectorSession session,
            SchemaTableName viewName,
            com.amazonaws.services.glue.model.Table table)
    {
        Map<String, String> materializedViewParameters = firstNonNull(table.getParameters(), ImmutableMap.of());
        String storageTable = materializedViewParameters.get(STORAGE_TABLE);
        checkState(storageTable != null, "Storage table missing in definition of materialized view " + viewName);
        String storageSchema = Optional.ofNullable(materializedViewParameters.get(STORAGE_SCHEMA))
                .orElse(viewName.getSchemaName());
        SchemaTableName storageTableName = new SchemaTableName(storageSchema, storageTable);

        Table icebergTable;
        try {
            icebergTable = loadTable(session, storageTableName);
        }
        catch (RuntimeException e) {
            // The materialized view could be removed concurrently. This may manifest in a number of ways, e.g.
            // - io.trino.spi.connector.TableNotFoundException
            // - org.apache.iceberg.exceptions.NotFoundException when accessing manifest file
            // - other failures when reading storage table's metadata files
            // Retry, as we're catching broadly.
            throw new MaterializedViewMayBeBeingRemovedException(e);
        }

        String viewOriginalText = table.getViewOriginalText();
        if (viewOriginalText == null) {
            throw new TrinoException(ICEBERG_BAD_DATA, "Materialized view did not have original text " + viewName);
        }
        return Optional.of(getMaterializedViewDefinition(
                icebergTable,
                Optional.ofNullable(table.getOwner()),
                viewOriginalText,
                storageTableName));
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        boolean newTableCreated = false;
        try {
            com.amazonaws.services.glue.model.Table glueTable = getTable(session, source)
                    .orElseThrow(() -> new TableNotFoundException(source));
            materializedViewCache.remove(source);
            if (!isTrinoMaterializedView(getTableType(glueTable), firstNonNull(glueTable.getParameters(), ImmutableMap.of()))) {
                throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Not a Materialized View: " + source);
            }
            TableInput tableInput = getMaterializedViewTableInput(target.getTableName(), glueTable.getViewOriginalText(), glueTable.getOwner(), glueTable.getParameters());
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withDatabaseName(target.getSchemaName())
                    .withTableInput(tableInput);
            stats.getCreateTable().call(() -> glueClient.createTable(createTableRequest));
            newTableCreated = true;
            deleteTable(source.getSchemaName(), source.getTableName());
        }
        catch (RuntimeException e) {
            if (newTableCreated) {
                try {
                    deleteTable(target.getSchemaName(), target.getTableName());
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

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        Optional<String> targetCatalogName = getHiveCatalogName(session);
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return Optional.empty();
        }

        // we need to chop off any "$partitions" and similar suffixes from table name while querying the metastore for the Table object
        int metadataMarkerIndex = tableName.getTableName().lastIndexOf('$');
        SchemaTableName tableNameBase = (metadataMarkerIndex == -1) ? tableName : schemaTableName(
                tableName.getSchemaName(),
                tableName.getTableName().substring(0, metadataMarkerIndex));

        Optional<com.amazonaws.services.glue.model.Table> table = getTable(session, new SchemaTableName(tableNameBase.getSchemaName(), tableNameBase.getTableName()));

        if (table.isEmpty() || VIRTUAL_VIEW.name().equals(getTableTypeNullable(table.get()))) {
            return Optional.empty();
        }
        if (!isIcebergTable(firstNonNull(table.get().getParameters(), ImmutableMap.of()))) {
            // After redirecting, use the original table name, with "$partitions" and similar suffixes
            return targetCatalogName.map(catalog -> new CatalogSchemaTableName(catalog, tableName));
        }
        return Optional.empty();
    }
}
