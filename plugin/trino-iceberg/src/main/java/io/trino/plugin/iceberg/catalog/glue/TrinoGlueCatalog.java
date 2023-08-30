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
import com.amazonaws.services.glue.model.Column;
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
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TrinoViewUtil;
import io.trino.plugin.hive.ViewAlreadyExistsException;
import io.trino.plugin.hive.ViewReaderUtil;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.IcebergMaterializedViewDefinition;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.TrinoViewUtil.createViewProperties;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getColumnParameters;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableType;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableTypeNullable;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewAdditionalProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_NOT_NULL_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_TYPE_ID_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_TABLE_METADATA_INFO_VALID_FOR;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnMetadatas;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.getTableComment;
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
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.CatalogUtil.dropTableData;

public class TrinoGlueCatalog
        extends AbstractTrinoCatalog
{
    private static final Logger LOG = Logger.get(TrinoGlueCatalog.class);

    private static final int PER_QUERY_CACHE_SIZE = 1000;

    private final String trinoVersion;
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Optional<String> defaultSchemaLocation;
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;

    private final Cache<SchemaTableName, com.amazonaws.services.glue.model.Table> glueTableCache = EvictableCacheBuilder.newBuilder()
            // Even though this is query-scoped, this still needs to be bounded. information_schema queries can access large number of tables.
            .maximumSize(Math.max(PER_QUERY_CACHE_SIZE, IcebergMetadata.GET_METADATA_BATCH_SIZE))
            .build();
    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> viewCache = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViewCache = new ConcurrentHashMap<>();

    public TrinoGlueCatalog(
            CatalogName catalogName,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            boolean cacheTableMetadata,
            IcebergTableOperationsProvider tableOperationsProvider,
            String trinoVersion,
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            Optional<String> defaultSchemaLocation,
            boolean useUniqueTableLocation)
    {
        super(catalogName, typeManager, tableOperationsProvider, useUniqueTableLocation);
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = cacheTableMetadata;
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
            return ImmutableList.of(namespace.get());
        }
        return listNamespaces(session);
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        try {
            glueTableCache.invalidateAll();
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
        checkArgument(owner.getName().equals(session.getUser().toLowerCase(ENGLISH)), "Explicit schema owner is not supported");

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
                    tables.addAll(getGlueTables(glueNamespace)
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
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        ImmutableList.Builder<RelationColumnsMetadata> unfilteredResult = ImmutableList.builder();
        ImmutableList.Builder<RelationColumnsMetadata> filteredResult = ImmutableList.builder();
        Map<SchemaTableName, com.amazonaws.services.glue.model.Table> unprocessed = new HashMap<>();

        listNamespaces(session, namespace).stream()
                .flatMap(glueNamespace -> getGlueTables(glueNamespace)
                        .map(table -> Map.entry(new SchemaTableName(glueNamespace, table.getName()), table)))
                .forEach(entry -> {
                    SchemaTableName name = entry.getKey();
                    com.amazonaws.services.glue.model.Table table = entry.getValue();
                    String tableType = getTableType(table);
                    Map<String, String> tableParameters = getTableParameters(table);
                    if (isTrinoMaterializedView(tableType, tableParameters)) {
                        IcebergMaterializedViewDefinition definition = decodeMaterializedViewData(table.getViewOriginalText());
                        unfilteredResult.add(RelationColumnsMetadata.forMaterializedView(name, toSpiMaterializedViewColumns(definition.getColumns())));
                    }
                    else if (isTrinoView(tableType, tableParameters)) {
                        ConnectorViewDefinition definition = ViewReaderUtil.PrestoViewReader.decodeViewData(table.getViewOriginalText());
                        unfilteredResult.add(RelationColumnsMetadata.forView(name, definition.getColumns()));
                    }
                    else if (isRedirected.test(name)) {
                        unfilteredResult.add(RelationColumnsMetadata.forRedirectedTable(name));
                    }
                    else if (!isIcebergTable(tableParameters)) {
                        // This can be e.g. Hive, Delta table, a Hive view, etc. Skip for columns listing
                    }
                    else {
                        Optional<List<ColumnMetadata>> columnMetadata = getCachedColumnMetadata(table);
                        if (columnMetadata.isPresent()) {
                            unfilteredResult.add(RelationColumnsMetadata.forTable(name, columnMetadata.get()));
                        }
                        else {
                            unprocessed.put(name, table);
                            if (unprocessed.size() >= PER_QUERY_CACHE_SIZE) {
                                getColumnsFromIcebergMetadata(session, unprocessed, relationFilter, filteredResult::add);
                                unprocessed.clear();
                            }
                        }
                    }
                });

        if (!unprocessed.isEmpty()) {
            getColumnsFromIcebergMetadata(session, unprocessed, relationFilter, filteredResult::add);
        }

        List<RelationColumnsMetadata> unfilteredResultList = unfilteredResult.build();
        Set<SchemaTableName> availableNames = relationFilter.apply(unfilteredResultList.stream()
                .map(RelationColumnsMetadata::name)
                .collect(toImmutableSet()));

        return Optional.of(Stream.concat(
                        unfilteredResultList.stream()
                                .filter(commentMetadata -> availableNames.contains(commentMetadata.name())),
                        filteredResult.build().stream())
                .iterator());
    }

    private void getColumnsFromIcebergMetadata(
            ConnectorSession session,
            Map<SchemaTableName, com.amazonaws.services.glue.model.Table> glueTables, // only Iceberg tables
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Consumer<RelationColumnsMetadata> resultsCollector)
    {
        for (SchemaTableName tableName : relationFilter.apply(glueTables.keySet())) {
            com.amazonaws.services.glue.model.Table table = glueTables.get(tableName);
            // potentially racy with invalidation, but TrinoGlueCatalog is session-scoped
            uncheckedCacheGet(glueTableCache, tableName, () -> table);
            List<ColumnMetadata> columns;
            try {
                columns = getColumnMetadatas(loadTable(session, tableName).schema(), typeManager);
            }
            catch (RuntimeException e) {
                // Table may be concurrently deleted
                LOG.warn(e, "Failed to get metadata for table: %s", tableName);
                return;
            }
            resultsCollector.accept(RelationColumnsMetadata.forTable(tableName, columns));
        }
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        if (!cacheTableMetadata) {
            return Optional.empty();
        }

        ImmutableList.Builder<RelationCommentMetadata> unfilteredResult = ImmutableList.builder();
        ImmutableList.Builder<RelationCommentMetadata> filteredResult = ImmutableList.builder();
        Map<SchemaTableName, com.amazonaws.services.glue.model.Table> unprocessed = new HashMap<>();

        listNamespaces(session, namespace).stream()
                .flatMap(glueNamespace -> getGlueTables(glueNamespace)
                        .map(table -> Map.entry(new SchemaTableName(glueNamespace, table.getName()), table)))
                .forEach(entry -> {
                    SchemaTableName name = entry.getKey();
                    com.amazonaws.services.glue.model.Table table = entry.getValue();
                    String tableType = getTableType(table);
                    Map<String, String> tableParameters = getTableParameters(table);
                    if (isTrinoMaterializedView(tableType, tableParameters)) {
                        Optional<String> comment = decodeMaterializedViewData(table.getViewOriginalText()).getComment();
                        unfilteredResult.add(RelationCommentMetadata.forRelation(name, comment));
                    }
                    else if (isTrinoView(tableType, tableParameters)) {
                        Optional<String> comment = ViewReaderUtil.PrestoViewReader.decodeViewData(table.getViewOriginalText()).getComment();
                        unfilteredResult.add(RelationCommentMetadata.forRelation(name, comment));
                    }
                    else if (isRedirected.test(name)) {
                        unfilteredResult.add(RelationCommentMetadata.forRedirectedTable(name));
                    }
                    else if (!isIcebergTable(tableParameters)) {
                        // This can be e.g. Hive, Delta table, a Hive view, etc. Would be returned by listTables, so do not skip it
                        unfilteredResult.add(RelationCommentMetadata.forRelation(name, Optional.empty()));
                    }
                    else {
                        String metadataLocation = tableParameters.get(METADATA_LOCATION_PROP);
                        String metadataValidForMetadata = tableParameters.get(TRINO_TABLE_METADATA_INFO_VALID_FOR);
                        if (metadataValidForMetadata != null && metadataValidForMetadata.equals(metadataLocation)) {
                            Optional<String> comment = Optional.ofNullable(tableParameters.get(TABLE_COMMENT));
                            unfilteredResult.add(RelationCommentMetadata.forRelation(name, comment));
                        }
                        else {
                            unprocessed.put(name, table);
                            if (unprocessed.size() >= PER_QUERY_CACHE_SIZE) {
                                getCommentsFromIcebergMetadata(session, unprocessed, relationFilter, filteredResult::add);
                                unprocessed.clear();
                            }
                        }
                    }
                });

        if (!unprocessed.isEmpty()) {
            getCommentsFromIcebergMetadata(session, unprocessed, relationFilter, filteredResult::add);
        }

        List<RelationCommentMetadata> unfilteredResultList = unfilteredResult.build();
        Set<SchemaTableName> availableNames = relationFilter.apply(unfilteredResultList.stream()
                .map(RelationCommentMetadata::name)
                .collect(toImmutableSet()));

        return Optional.of(Stream.concat(
                        unfilteredResultList.stream()
                                .filter(commentMetadata -> availableNames.contains(commentMetadata.name())),
                        filteredResult.build().stream())
                .iterator());
    }

    private void getCommentsFromIcebergMetadata(
            ConnectorSession session,
            Map<SchemaTableName, com.amazonaws.services.glue.model.Table> glueTables, // only Iceberg tables
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Consumer<RelationCommentMetadata> resultsCollector)
    {
        for (SchemaTableName tableName : relationFilter.apply(glueTables.keySet())) {
            com.amazonaws.services.glue.model.Table table = glueTables.get(tableName);
            // potentially racy with invalidation, but TrinoGlueCatalog is session-scoped
            uncheckedCacheGet(glueTableCache, tableName, () -> table);
            Optional<String> comment;
            try {
                comment = getTableComment(loadTable(session, tableName));
            }
            catch (RuntimeException e) {
                // Table may be concurrently deleted
                LOG.warn(e, "Failed to get metadata for table: %s", tableName);
                return;
            }
            resultsCollector.accept(RelationCommentMetadata.forRelation(tableName, comment));
        }
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
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        if (!cacheTableMetadata) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> metadatas = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            Optional<List<ColumnMetadata>> columnMetadata;
            try {
                columnMetadata = getCachedColumnMetadata(tableName);
            }
            catch (TableNotFoundException ignore) {
                // Table disappeared during listing.
                continue;
            }
            catch (RuntimeException e) {
                // Handle exceptions gracefully during metadata listing. Log, because we're catching broadly.
                LOG.warn(e, "Failed to access get metadata of table %s during bulk retrieval of table columns", tableName);
                continue;
            }
            columnMetadata.ifPresent(columns -> metadatas.put(tableName, columns));
        }
        return metadatas.buildOrThrow();
    }

    private Optional<List<ColumnMetadata>> getCachedColumnMetadata(SchemaTableName tableName)
    {
        if (!cacheTableMetadata || viewCache.containsKey(tableName) || materializedViewCache.containsKey(tableName)) {
            return Optional.empty();
        }

        com.amazonaws.services.glue.model.Table glueTable = getTable(tableName, false);
        return getCachedColumnMetadata(glueTable);
    }

    private Optional<List<ColumnMetadata>> getCachedColumnMetadata(com.amazonaws.services.glue.model.Table glueTable)
    {
        if (!cacheTableMetadata) {
            return Optional.empty();
        }

        Map<String, String> tableParameters = getTableParameters(glueTable);
        String metadataLocation = tableParameters.get(METADATA_LOCATION_PROP);
        String metadataValidForMetadata = tableParameters.get(TRINO_TABLE_METADATA_INFO_VALID_FOR);
        if (metadataLocation == null || !metadataLocation.equals(metadataValidForMetadata) ||
                glueTable.getStorageDescriptor() == null ||
                glueTable.getStorageDescriptor().getColumns() == null) {
            return Optional.empty();
        }

        List<Column> glueColumns = glueTable.getStorageDescriptor().getColumns();
        if (glueColumns.stream().noneMatch(column -> getColumnParameters(column).containsKey(COLUMN_TRINO_TYPE_ID_PROPERTY))) {
            // No column has type parameter, maybe the parameters were erased
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builderWithExpectedSize(glueColumns.size());
        for (Column glueColumn : glueColumns) {
            Map<String, String> columnParameters = getColumnParameters(glueColumn);
            String trinoTypeId = columnParameters.getOrDefault(COLUMN_TRINO_TYPE_ID_PROPERTY, glueColumn.getType());
            boolean notNull = parseBoolean(columnParameters.getOrDefault(COLUMN_TRINO_NOT_NULL_PROPERTY, "false"));
            Type type = typeManager.getType(TypeId.of(trinoTypeId));
            columns.add(ColumnMetadata.builder()
                    .setName(glueColumn.getName())
                    .setType(type)
                    .setComment(Optional.ofNullable(glueColumn.getComment()))
                    .setNullable(!notNull)
                    .build());
        }
        return Optional.of(columns.build());
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
        try {
            dropTableData(table.io(), table.operations().current());
        }
        catch (RuntimeException e) {
            // If the snapshot file is not found, an exception will be thrown by the dropTableData function.
            // So log the exception and continue with deleting the table location
            LOG.warn(e, "Failed to delete table data referenced by metadata");
        }
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, table.location());
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        com.amazonaws.services.glue.model.Table table = dropTableFromMetastore(session, schemaTableName);
        String metadataLocation = getTableParameters(table).get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table %s is missing [%s] property", schemaTableName, METADATA_LOCATION_PROP));
        }
        String tableLocation = metadataLocation.replaceFirst("/metadata/[^/]*$", "");
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, tableLocation);
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
    public void registerTable(ConnectorSession session, SchemaTableName schemaTableName, TableMetadata tableMetadata)
            throws TrinoException
    {
        TableInput tableInput = getTableInput(
                typeManager,
                schemaTableName.getTableName(),
                Optional.of(session.getUser()),
                tableMetadata,
                tableMetadata.metadataFileLocation(),
                ImmutableMap.of(),
                cacheTableMetadata);
        createTable(schemaTableName.getSchemaName(), tableInput);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        dropTableFromMetastore(session, schemaTableName);
    }

    private com.amazonaws.services.glue.model.Table dropTableFromMetastore(ConnectorSession session, SchemaTableName schemaTableName)
    {
        com.amazonaws.services.glue.model.Table table = getTableAndCacheMetadata(session, schemaTableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (!isIcebergTable(getTableParameters(table))) {
            throw new UnknownTableTypeException(schemaTableName);
        }

        try {
            deleteTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        return table;
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        boolean newTableCreated = false;
        try {
            com.amazonaws.services.glue.model.Table table = getTableAndCacheMetadata(session, from)
                    .orElseThrow(() -> new TableNotFoundException(from));
            Map<String, String> tableParameters = new HashMap<>(getTableParameters(table));
            FileIO io = loadTable(session, from).io();
            String metadataLocation = tableParameters.remove(METADATA_LOCATION_PROP);
            if (metadataLocation == null) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table %s is missing [%s] property", from, METADATA_LOCATION_PROP));
            }
            TableMetadata metadata = TableMetadataParser.read(io, io.newInputFile(metadataLocation));
            TableInput tableInput = getTableInput(
                    typeManager,
                    to.getTableName(),
                    Optional.ofNullable(table.getOwner()),
                    metadata,
                    metadataLocation,
                    tableParameters,
                    cacheTableMetadata);
            createTable(to.getSchemaName(), tableInput);
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

    private Optional<com.amazonaws.services.glue.model.Table> getTableAndCacheMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        com.amazonaws.services.glue.model.Table table;
        try {
            table = getTable(schemaTableName, false);
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }

        String tableType = getTableType(table);
        Map<String, String> parameters = getTableParameters(table);
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
        else if (isTrinoMaterializedView(tableType, parameters)) {
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
        else if (isTrinoView(tableType, parameters) && !viewCache.containsKey(schemaTableName)) {
            if (materializedViewCache.containsKey(schemaTableName) || tableMetadataCache.containsKey(schemaTableName)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Glue table cache inconsistency. View cannot also be a materialized view or table");
            }

            try {
                TrinoViewUtil.getView(
                                Optional.ofNullable(table.getViewOriginalText()),
                                tableType,
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
            databaseLocation = appendPath(defaultSchemaLocation.get(), schemaDirectoryName);
        }

        return appendPath(databaseLocation, tableName);
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
        Optional<com.amazonaws.services.glue.model.Table> existing = getTableAndCacheMetadata(session, schemaViewName);
        if (existing.isPresent()) {
            if (!replace || !isTrinoView(getTableType(existing.get()), getTableParameters(existing.get()))) {
                // TODO: ViewAlreadyExists is misleading if the name is used by a table https://github.com/trinodb/trino/issues/10037
                throw new ViewAlreadyExistsException(schemaViewName);
            }

            updateTable(schemaViewName.getSchemaName(), viewTableInput);
            return;
        }

        try {
            createTable(schemaViewName.getSchemaName(), viewTableInput);
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
            com.amazonaws.services.glue.model.Table existingView = getTableAndCacheMetadata(session, source)
                    .orElseThrow(() -> new TableNotFoundException(source));
            viewCache.remove(source);
            TableInput viewTableInput = getViewTableInput(
                    target.getTableName(),
                    existingView.getViewOriginalText(),
                    existingView.getOwner(),
                    createViewProperties(session, trinoVersion, TRINO_CREATED_BY_VALUE));
            createTable(target.getSchemaName(), viewTableInput);
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
                    views.addAll(getGlueTables(glueNamespace)
                            .filter(table -> isTrinoView(getTableType(table), getTableParameters(table)))
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

        Optional<com.amazonaws.services.glue.model.Table> table = getTableAndCacheMetadata(session, viewName);
        if (table.isEmpty()) {
            return Optional.empty();
        }
        com.amazonaws.services.glue.model.Table viewDefinition = table.get();
        return TrinoViewUtil.getView(
                Optional.ofNullable(viewDefinition.getViewOriginalText()),
                getTableType(viewDefinition),
                getTableParameters(viewDefinition),
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
            updateTable(viewName.getSchemaName(), viewTableInput);
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
                    materializedViews.addAll(getGlueTables(glueNamespace)
                            .filter(table -> isTrinoMaterializedView(getTableType(table), getTableParameters(table)))
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
        Optional<com.amazonaws.services.glue.model.Table> existing = getTableAndCacheMetadata(session, viewName);

        if (existing.isPresent()) {
            if (!isTrinoMaterializedView(getTableType(existing.get()), getTableParameters(existing.get()))) {
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
                updateTable(viewName.getSchemaName(), materializedViewTableInput);
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
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        ConnectorMaterializedViewDefinition definition = doGetMaterializedView(session, viewName)
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        ConnectorMaterializedViewDefinition newDefinition = new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                definition.getStorageTable(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(currentViewColumn -> Objects.equals(columnName, currentViewColumn.getName()) ? new ConnectorMaterializedViewDefinition.Column(currentViewColumn.getName(), currentViewColumn.getType(), comment) : currentViewColumn)
                        .collect(toImmutableList()),
                definition.getGracePeriod(),
                definition.getComment(),
                definition.getOwner(),
                definition.getProperties());

        updateMaterializedView(session, viewName, newDefinition);
    }

    private void updateMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition newDefinition)
    {
        TableInput materializedViewTableInput = getMaterializedViewTableInput(
                viewName.getTableName(),
                encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(newDefinition)),
                session.getUser(),
                createMaterializedViewProperties(session, newDefinition.getStorageTable().orElseThrow().getSchemaTableName()));

        try {
            updateTable(viewName.getSchemaName(), materializedViewTableInput);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        com.amazonaws.services.glue.model.Table view = getTableAndCacheMetadata(session, viewName)
                .orElseThrow(() -> new MaterializedViewNotFoundException(viewName));

        if (!isTrinoMaterializedView(getTableType(view), getTableParameters(view))) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Not a Materialized View: " + view.getDatabaseName() + "." + view.getName());
        }
        materializedViewCache.remove(viewName);
        dropStorageTable(session, view);
        deleteTable(view.getDatabaseName(), view.getName());
    }

    private void dropStorageTable(ConnectorSession session, com.amazonaws.services.glue.model.Table view)
    {
        Map<String, String> parameters = getTableParameters(view);
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

        Optional<com.amazonaws.services.glue.model.Table> maybeTable = getTableAndCacheMetadata(session, viewName);
        if (maybeTable.isEmpty()) {
            return Optional.empty();
        }

        com.amazonaws.services.glue.model.Table table = maybeTable.get();
        if (!isTrinoMaterializedView(getTableType(table), getTableParameters(table))) {
            return Optional.empty();
        }

        return createMaterializedViewDefinition(session, viewName, table);
    }

    private Optional<ConnectorMaterializedViewDefinition> createMaterializedViewDefinition(
            ConnectorSession session,
            SchemaTableName viewName,
            com.amazonaws.services.glue.model.Table table)
    {
        Map<String, String> materializedViewParameters = getTableParameters(table);
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
            com.amazonaws.services.glue.model.Table glueTable = getTableAndCacheMetadata(session, source)
                    .orElseThrow(() -> new TableNotFoundException(source));
            materializedViewCache.remove(source);
            Map<String, String> tableParameters = getTableParameters(glueTable);
            if (!isTrinoMaterializedView(getTableType(glueTable), tableParameters)) {
                throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Not a Materialized View: " + source);
            }
            TableInput tableInput = getMaterializedViewTableInput(target.getTableName(), glueTable.getViewOriginalText(), glueTable.getOwner(), tableParameters);
            createTable(target.getSchemaName(), tableInput);
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
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(hiveCatalogName, "hiveCatalogName is null");

        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return Optional.empty();
        }

        // we need to chop off any "$partitions" and similar suffixes from table name while querying the metastore for the Table object
        int metadataMarkerIndex = tableName.getTableName().lastIndexOf('$');
        SchemaTableName tableNameBase = (metadataMarkerIndex == -1) ? tableName : schemaTableName(
                tableName.getSchemaName(),
                tableName.getTableName().substring(0, metadataMarkerIndex));

        Optional<com.amazonaws.services.glue.model.Table> table = getTableAndCacheMetadata(session, new SchemaTableName(tableNameBase.getSchemaName(), tableNameBase.getTableName()));

        if (table.isEmpty() || VIRTUAL_VIEW.name().equals(getTableTypeNullable(table.get()))) {
            return Optional.empty();
        }
        if (!isIcebergTable(getTableParameters(table.get()))) {
            // After redirecting, use the original table name, with "$partitions" and similar suffixes
            return Optional.of(new CatalogSchemaTableName(hiveCatalogName, tableName));
        }
        return Optional.empty();
    }

    com.amazonaws.services.glue.model.Table getTable(SchemaTableName tableName, boolean invalidateCaches)
    {
        if (invalidateCaches) {
            glueTableCache.invalidate(tableName);
        }

        try {
            return uncheckedCacheGet(glueTableCache, tableName, () -> {
                try {
                    GetTableRequest getTableRequest = new GetTableRequest()
                            .withDatabaseName(tableName.getSchemaName())
                            .withName(tableName.getTableName());
                    return stats.getGetTable().call(() -> glueClient.getTable(getTableRequest).getTable());
                }
                catch (EntityNotFoundException e) {
                    throw new TableNotFoundException(tableName, e);
                }
            });
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Get table request failed: " + firstNonNull(e.getMessage(), e), e.getCause());
        }
    }

    private Stream<com.amazonaws.services.glue.model.Table> getGlueTables(String glueNamespace)
    {
        return getPaginatedResults(
                glueClient::getTables,
                new GetTablesRequest().withDatabaseName(glueNamespace),
                GetTablesRequest::setNextToken,
                GetTablesResult::getNextToken,
                stats.getGetTables())
                .map(GetTablesResult::getTableList)
                .flatMap(List::stream);
    }

    private void createTable(String schemaName, TableInput tableInput)
    {
        glueTableCache.invalidateAll();
        stats.getCreateTable().call(() ->
                glueClient.createTable(new CreateTableRequest()
                        .withDatabaseName(schemaName)
                        .withTableInput(tableInput)));
    }

    private void updateTable(String schemaName, TableInput tableInput)
    {
        glueTableCache.invalidateAll();
        stats.getUpdateTable().call(() ->
                glueClient.updateTable(new UpdateTableRequest()
                        .withDatabaseName(schemaName)
                        .withTableInput(tableInput)));
    }

    private void deleteTable(String schema, String table)
    {
        glueTableCache.invalidateAll();
        stats.getDeleteTable().call(() ->
                glueClient.deleteTable(new DeleteTableRequest()
                        .withDatabaseName(schema)
                        .withName(table)));
    }
}
