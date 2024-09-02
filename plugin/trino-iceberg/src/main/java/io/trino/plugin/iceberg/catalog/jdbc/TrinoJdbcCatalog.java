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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.view.ReplaceViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.UpdateViewProperties;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.transformValues;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_UNSUPPORTED_VIEW_DIALECT;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogUtil.dropTableData;
import static org.apache.iceberg.view.ViewProperties.COMMENT;

public class TrinoJdbcCatalog
        extends AbstractTrinoCatalog
{
    private static final Logger LOG = Logger.get(TrinoJdbcCatalog.class);

    private static final int PER_QUERY_CACHE_SIZE = 1000;

    private final JdbcCatalog jdbcCatalog;
    private final IcebergJdbcClient jdbcClient;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final String defaultWarehouseDir;

    private final Cache<SchemaTableName, TableMetadata> tableMetadataCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(PER_QUERY_CACHE_SIZE)
            .build();

    public TrinoJdbcCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            JdbcCatalog jdbcCatalog,
            IcebergJdbcClient jdbcClient,
            TrinoFileSystemFactory fileSystemFactory,
            boolean useUniqueTableLocation,
            String defaultWarehouseDir)
    {
        super(catalogName, typeManager, tableOperationsProvider, fileSystemFactory, useUniqueTableLocation);
        this.jdbcCatalog = requireNonNull(jdbcCatalog, "jdbcCatalog is null");
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.defaultWarehouseDir = requireNonNull(defaultWarehouseDir, "defaultWarehouseDir is null");
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        return jdbcCatalog.namespaceExists(Namespace.of(namespace));
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return jdbcCatalog.listNamespaces().stream()
                .map(namespace -> namespace.level(0).toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        return jdbcCatalog.loadNamespaceMetadata(Namespace.of(namespace)).entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        try {
            jdbcCatalog.createNamespace(Namespace.of(namespace), transformValues(properties, String.class::cast));
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to create a namespace " + namespace, e);
        }
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        jdbcCatalog.dropNamespace(Namespace.of(namespace));
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace)
    {
        List<String> namespaces = listNamespaces(session, namespace);

        // Build as a map and convert to list for removing duplicate entries due to case difference
        Map<SchemaTableName, TableInfo> tablesListBuilder = new HashMap<>();
        for (String schemaName : namespaces) {
            try {
                listTableIdentifiers(schemaName, () -> jdbcCatalog.listTables(Namespace.of(schemaName))).stream()
                        .map(tableId -> SchemaTableName.schemaTableName(schemaName, tableId.name()))
                        .forEach(schemaTableName -> tablesListBuilder.put(schemaTableName, new TableInfo(schemaTableName, TableInfo.ExtendedRelationType.TABLE)));
                listTableIdentifiers(schemaName, () -> jdbcCatalog.listViews(Namespace.of(schemaName))).stream()
                        .map(tableId -> SchemaTableName.schemaTableName(schemaName, tableId.name()))
                        .forEach(schemaTableName -> tablesListBuilder.put(schemaTableName, new TableInfo(schemaTableName, TableInfo.ExtendedRelationType.OTHER_VIEW)));
            }
            catch (NoSuchNamespaceException e) {
                // Namespace may have been deleted
            }
        }
        return ImmutableList.copyOf(tablesListBuilder.values());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        List<String> namespaces = listNamespaces(session, namespace);

        ImmutableList.Builder<SchemaTableName> viewNames = ImmutableList.builder();
        for (String ns : namespaces) {
            listTableIdentifiers(ns, () -> jdbcCatalog.listViews(Namespace.of(ns))).stream()
                    .map(id -> SchemaTableName.schemaTableName(id.namespace().toString(), id.name()))
                    .forEach(viewNames::add);
        }
        return viewNames.build();
    }

    private static List<TableIdentifier> listTableIdentifiers(String namespace, Supplier<List<TableIdentifier>> tableIdentifiersProvider)
    {
        try {
            return tableIdentifiersProvider.get();
        }
        catch (NoSuchNamespaceException e) {
            // Namespace may have been deleted during listing
        }
        catch (UncheckedSQLException | UncheckedInterruptedException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list tables from namespace: " + namespace, e);
        }
        return ImmutableList.of();
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    private List<String> listNamespaces(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent() && namespaceExists(session, namespace.get())) {
            return ImmutableList.of(namespace.get());
        }
        return listNamespaces(session);
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
    public Transaction newCreateOrReplaceTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        return newCreateOrReplaceTableTransaction(
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
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        // Using IcebergJdbcClient because JdbcCatalog.registerTable causes the below error.
        // "Cannot invoke "org.apache.iceberg.util.SerializableSupplier.get()" because "this.hadoopConf" is null"
        jdbcClient.createTable(tableName.getSchemaName(), tableName.getTableName(), tableMetadata.metadataFileLocation());
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (!jdbcCatalog.dropTable(toIdentifier(tableName), false)) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BaseTable table = (BaseTable) loadTable(session, schemaTableName);
        validateTableCanBeDropped(table);

        jdbcCatalog.dropTable(toIdentifier(schemaTableName), false);
        try {
            dropTableData(table.io(), table.operations().current());
        }
        catch (RuntimeException e) {
            // If the snapshot file is not found, an exception will be thrown by the dropTableData function.
            // So log the exception and continue with deleting the table location
            LOG.warn(e, "Failed to delete table data referenced by metadata");
        }
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, table.location());
        invalidateTableCache(schemaTableName);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Optional<String> metadataLocation = jdbcClient.getMetadataLocation(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (!jdbcCatalog.dropTable(toIdentifier(schemaTableName), false)) {
            throw new TableNotFoundException(schemaTableName);
        }
        if (metadataLocation.isEmpty()) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Could not find metadata_location for table %s", schemaTableName));
        }
        String tableLocation = metadataLocation.get().replaceFirst("/metadata/[^/]*$", "");
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, tableLocation);
        invalidateTableCache(schemaTableName);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        try {
            jdbcCatalog.renameTable(toIdentifier(from), toIdentifier(to));
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to rename table from %s to %s".formatted(from, to), e);
        }
        invalidateTableCache(from);
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableMetadata metadata;
        try {
            metadata = uncheckedCacheGet(
                    tableMetadataCache,
                    schemaTableName,
                    () -> ((BaseTable) loadIcebergTable(this, tableOperationsProvider, session, schemaTableName)).operations().current());
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw e;
        }

        return getIcebergTableWithMetadata(this, tableOperationsProvider, session, schemaTableName, metadata);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        View view = Optional.ofNullable(jdbcCatalog.loadView(toIdentifier(schemaViewName))).orElseThrow(() -> new ViewNotFoundException(schemaViewName));
        UpdateViewProperties updateViewProperties = view.updateProperties();
        comment.ifPresentOrElse(
                value -> updateViewProperties.set(COMMENT, value),
                () -> updateViewProperties.remove(COMMENT));
        updateViewProperties.commit();
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        View view = Optional.ofNullable(jdbcCatalog.loadView(toIdentifier(schemaViewName)))
                .orElseThrow(() -> new ViewNotFoundException(schemaViewName));

        ViewVersion current = view.currentVersion();
        Schema updatedSchema = IcebergUtil.updateColumnComment(view.schema(), columnName, comment.orElse(null));
        ReplaceViewVersion replaceViewVersion = view.replaceVersion()
                .withSchema(updatedSchema)
                .withDefaultCatalog(current.defaultCatalog())
                .withDefaultNamespace(current.defaultNamespace());
        for (ViewRepresentation representation : view.currentVersion().representations()) {
            if (representation instanceof SQLViewRepresentation sqlViewRepresentation) {
                replaceViewVersion.withQuery(sqlViewRepresentation.dialect(), sqlViewRepresentation.sql());
            }
        }

        replaceViewVersion.commit();
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Namespace namespace = Namespace.of(schemaTableName.getSchemaName());
        String tableName = createNewTableName(schemaTableName.getTableName());

        Optional<String> databaseLocation = Optional.empty();
        if (jdbcCatalog.namespaceExists(namespace)) {
            databaseLocation = Optional.ofNullable(jdbcCatalog.loadNamespaceMetadata(namespace).get(LOCATION_PROPERTY));
        }

        String schemaLocation = databaseLocation.orElseGet(() ->
                appendPath(defaultWarehouseDir, schemaTableName.getSchemaName()));

        return appendPath(schemaLocation, tableName);
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        definition.getOwner().ifPresent(owner -> properties.put(ICEBERG_VIEW_RUN_AS_OWNER, owner));
        definition.getComment().ifPresent(comment -> properties.put(COMMENT, comment));
        Schema schema = IcebergUtil.schemaFromViewColumns(typeManager, definition.getColumns());
        ViewBuilder viewBuilder = jdbcCatalog.buildView(toIdentifier(schemaViewName));
        viewBuilder = viewBuilder.withSchema(schema)
                .withQuery("trino", definition.getOriginalSql())
                .withDefaultNamespace(Namespace.of(schemaViewName.getSchemaName()))
                .withDefaultCatalog(definition.getCatalog().orElse(null))
                .withProperties(properties.buildOrThrow())
                .withLocation(defaultTableLocation(session, schemaViewName));

        if (replace) {
            viewBuilder.createOrReplace();
        }
        else {
            viewBuilder.create();
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        jdbcCatalog.renameView(toIdentifier(source), toIdentifier(target));
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        jdbcCatalog.dropView(toIdentifier(schemaViewName));
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (Namespace ns : jdbcCatalog.listNamespaces()) {
            for (TableIdentifier restView : jdbcCatalog.listViews(ns)) {
                SchemaTableName schemaTableName = SchemaTableName.schemaTableName(restView.namespace().toString(), restView.name());
                try {
                    getView(session, schemaTableName).ifPresent(view -> views.put(schemaTableName, view));
                }
                catch (TrinoException e) {
                    if (e.getErrorCode().equals(ICEBERG_UNSUPPORTED_VIEW_DIALECT.toErrorCode())) {
                        LOG.debug(e, "Skip unsupported view dialect: %s", schemaTableName);
                        continue;
                    }
                    throw e;
                }
            }
        }

        return views.buildOrThrow();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewIdentifier)
    {
        if (!jdbcCatalog.viewExists(toIdentifier(viewIdentifier))) {
            return Optional.empty();
        }

        return Optional.of(jdbcCatalog.loadView(toIdentifier(viewIdentifier))).flatMap(view -> {
            SQLViewRepresentation sqlView = view.sqlFor("trino");
            if (!sqlView.dialect().equalsIgnoreCase("trino")) {
                throw new TrinoException(ICEBERG_UNSUPPORTED_VIEW_DIALECT, "Cannot read unsupported dialect '%s' for view '%s'".formatted(sqlView.dialect(), viewIdentifier));
            }

            Optional<String> comment = Optional.ofNullable(view.properties().get(COMMENT));
            List<ConnectorViewDefinition.ViewColumn> viewColumns = IcebergUtil.viewColumnsFromSchema(typeManager, view.schema());
            ViewVersion currentVersion = view.currentVersion();
            Optional<String> catalog = Optional.ofNullable(currentVersion.defaultCatalog());
            Optional<String> schema = Optional.empty();
            if (catalog.isPresent() && !currentVersion.defaultNamespace().isEmpty()) {
                schema = Optional.of(currentVersion.defaultNamespace().toString());
            }

            Optional<String> owner = Optional.ofNullable(view.properties().get(ICEBERG_VIEW_RUN_AS_OWNER));
            return Optional.of(new ConnectorViewDefinition(sqlView.sql(), catalog, schema, viewColumns, comment, owner, owner.isEmpty(), null));
        });
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "The Iceberg JDBC catalog does not support materialized views");
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName schemaViewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateMaterializedViewColumnComment is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        throw new TrinoException(NOT_SUPPORTED, "getMaterializedViewProperties is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        return Optional.empty();
    }

    @Override
    protected void invalidateTableCache(SchemaTableName schemaTableName)
    {
        tableMetadataCache.invalidate(schemaTableName);
    }

    private static TableIdentifier toIdentifier(SchemaTableName table)
    {
        return TableIdentifier.of(table.getSchemaName(), table.getTableName());
    }
}
