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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.jackson.io.JacksonSerializer;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergSchemaProperties;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
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
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.view.ReplaceViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.UpdateViewProperties;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_UNSUPPORTED_VIEW_DIALECT;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.ICEBERG_VIEW_RUN_AS_OWNER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.view.ViewProperties.COMMENT;

public class TrinoRestCatalog
        implements TrinoCatalog
{
    private static final Logger log = Logger.get(TrinoRestCatalog.class);

    private static final int PER_QUERY_CACHE_SIZE = 1000;
    private static final String NAMESPACE_SEPARATOR = ".";

    private final RESTSessionCatalog restSessionCatalog;
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final SessionType sessionType;
    private final Map<String, String> credentials;
    private final boolean nestedNamespaceEnabled;
    private final String trinoVersion;
    private final boolean useUniqueTableLocation;
    private final boolean caseInsensitiveNameMatching;
    private final Cache<Namespace, Namespace> remoteNamespaceMappingCache;
    private final Cache<TableIdentifier, TableIdentifier> remoteTableMappingCache;

    private final Cache<SchemaTableName, Table> tableCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(PER_QUERY_CACHE_SIZE)
            .build();

    public TrinoRestCatalog(
            RESTSessionCatalog restSessionCatalog,
            CatalogName catalogName,
            SessionType sessionType,
            Map<String, String> credentials,
            boolean nestedNamespaceEnabled,
            String trinoVersion,
            TypeManager typeManager,
            boolean useUniqueTableLocation,
            boolean caseInsensitiveNameMatching,
            Cache<Namespace, Namespace> remoteNamespaceMappingCache,
            Cache<TableIdentifier, TableIdentifier> remoteTableMappingCache)
    {
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.sessionType = requireNonNull(sessionType, "sessionType is null");
        this.credentials = ImmutableMap.copyOf(requireNonNull(credentials, "credentials is null"));
        this.nestedNamespaceEnabled = nestedNamespaceEnabled;
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        this.remoteNamespaceMappingCache = requireNonNull(remoteNamespaceMappingCache, "remoteNamespaceMappingCache is null");
        this.remoteTableMappingCache = requireNonNull(remoteTableMappingCache, "remoteTableMappingCache is null");
    }

    @Override
    public Optional<String> getNamespaceSeparator()
    {
        return Optional.of(NAMESPACE_SEPARATOR);
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        return restSessionCatalog.namespaceExists(convert(session), toRemoteNamespace(session, toNamespace(namespace)));
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        if (nestedNamespaceEnabled) {
            return collectNamespaces(session, Namespace.empty());
        }
        return restSessionCatalog.listNamespaces(convert(session)).stream()
                .map(this::toSchemaName)
                .collect(toImmutableList());
    }

    private List<String> collectNamespaces(ConnectorSession session, Namespace parentNamespace)
    {
        return restSessionCatalog.listNamespaces(convert(session), parentNamespace).stream()
                .flatMap(childNamespace -> Stream.concat(
                        Stream.of(childNamespace.toString()),
                        collectNamespaces(session, childNamespace).stream()))
                .collect(toImmutableList());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        try {
            restSessionCatalog.dropNamespace(convert(session), toRemoteNamespace(session, toNamespace(namespace)));
        }
        catch (NoSuchNamespaceException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to drop namespace: %s", namespace), e);
        }
        if (caseInsensitiveNameMatching) {
            remoteNamespaceMappingCache.invalidate(toNamespace(namespace));
        }
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        try {
            // Return immutable metadata as direct modifications will not be reflected on the namespace
            return ImmutableMap.copyOf(restSessionCatalog.loadNamespaceMetadata(convert(session), toRemoteNamespace(session, toNamespace(namespace))));
        }
        catch (NoSuchNamespaceException e) {
            throw new SchemaNotFoundException(namespace);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        // the REST specification currently does not have a way of defining ownership
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        restSessionCatalog.createNamespace(
                convert(session),
                toNamespace(namespace),
                Maps.transformValues(properties, property -> {
                    if (property instanceof String stringProperty) {
                        return stringProperty;
                    }
                    throw new TrinoException(NOT_SUPPORTED, "Non-string properties are not support for Iceberg REST catalog");
                }));
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for Iceberg REST catalog");
    }

    @Override
    public List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace)
    {
        SessionContext sessionContext = convert(session);
        List<Namespace> namespaces = listNamespaces(session, namespace);

        ImmutableList.Builder<TableInfo> tables = ImmutableList.builder();
        for (Namespace restNamespace : namespaces) {
            listTableIdentifiers(restNamespace, () -> restSessionCatalog.listTables(sessionContext, toRemoteNamespace(session, restNamespace))).stream()
                    .map(id -> new TableInfo(SchemaTableName.schemaTableName(toSchemaName(id.namespace()), id.name()), TableInfo.ExtendedRelationType.TABLE))
                    .forEach(tables::add);
            listTableIdentifiers(restNamespace, () -> restSessionCatalog.listViews(sessionContext, toRemoteNamespace(session, restNamespace))).stream()
                    .map(id -> new TableInfo(SchemaTableName.schemaTableName(toSchemaName(id.namespace()), id.name()), TableInfo.ExtendedRelationType.OTHER_VIEW))
                    .forEach(tables::add);
        }
        return tables.build();
    }

    @Override
    public List<SchemaTableName> listIcebergTables(ConnectorSession session, Optional<String> namespace)
    {
        SessionContext sessionContext = convert(session);
        List<Namespace> namespaces = listNamespaces(session, namespace);

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (Namespace restNamespace : namespaces) {
            listTableIdentifiers(restNamespace, () -> restSessionCatalog.listTables(sessionContext, toRemoteNamespace(session, restNamespace))).stream()
                    .map(id -> SchemaTableName.schemaTableName(toSchemaName(id.namespace()), id.name()))
                    .forEach(tables::add);
        }
        return tables.build();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        SessionContext sessionContext = convert(session);
        List<Namespace> namespaces = listNamespaces(session, namespace);

        ImmutableList.Builder<SchemaTableName> viewNames = ImmutableList.builder();
        for (Namespace restNamespace : namespaces) {
            listTableIdentifiers(restNamespace, () -> restSessionCatalog.listViews(sessionContext, toRemoteNamespace(session, restNamespace))).stream()
                    .map(id -> SchemaTableName.schemaTableName(id.namespace().toString(), id.name()))
                    .forEach(viewNames::add);
        }
        return viewNames.build();
    }

    private static List<TableIdentifier> listTableIdentifiers(Namespace restNamespace, Supplier<List<TableIdentifier>> tableIdentifiersProvider)
    {
        try {
            return tableIdentifiersProvider.get();
        }
        catch (NoSuchNamespaceException e) {
            // Namespace may have been deleted during listing
        }
        catch (ForbiddenException e) {
            log.debug(e, "Failed to list tables from %s namespace because of insufficient permissions", restNamespace);
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to list tables from namespace: %s", restNamespace), e);
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
        return restSessionCatalog.buildTable(convert(session), toRemoteTable(session, schemaTableName, true), schema)
                .withPartitionSpec(partitionSpec)
                .withSortOrder(sortOrder)
                .withLocation(location)
                .withProperties(properties)
                .createTransaction();
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
        return restSessionCatalog.buildTable(convert(session), toRemoteTable(session, schemaTableName, true), schema)
                .withPartitionSpec(partitionSpec)
                .withSortOrder(sortOrder)
                .withLocation(location)
                .withProperties(properties)
                .createOrReplaceTransaction();
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        TableIdentifier tableIdentifier = TableIdentifier.of(toRemoteNamespace(session, toNamespace(tableName.getSchemaName())), tableName.getTableName());
        restSessionCatalog.registerTable(convert(session), tableIdentifier, tableMetadata.metadataFileLocation());
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (!restSessionCatalog.dropTable(convert(session), toRemoteTable(session, tableName, true))) {
            throw new TableNotFoundException(tableName);
        }
        invalidateTableCache(tableName);
        invalidateTableMappingCache(tableName);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (!restSessionCatalog.purgeTable(convert(session), toRemoteTable(session, schemaTableName, true))) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to drop table: %s", schemaTableName));
        }
        invalidateTableCache(schemaTableName);
        invalidateTableMappingCache(schemaTableName);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        // Since it is currently not possible to obtain the table location, even if we drop the table from the metastore,
        // it is still impossible to delete the table location.
        throw new TrinoException(NOT_SUPPORTED, "Cannot drop corrupted table %s from Iceberg REST catalog".formatted(schemaTableName));
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        try {
            restSessionCatalog.renameTable(convert(session), toRemoteTable(session, from, true), toRemoteTable(session, to, true));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to rename table %s to %s", from, to), e);
        }
        invalidateTableCache(from);
        invalidateTableMappingCache(from);
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Namespace namespace = toNamespace(schemaTableName.getSchemaName());
        try {
            return uncheckedCacheGet(
                    tableCache,
                    schemaTableName,
                    () -> {
                        BaseTable baseTable = (BaseTable) restSessionCatalog.loadTable(convert(session), toRemoteObject(session, schemaTableName));
                        // Creating a new base table is necessary to adhere to Trino's expectations for quoted table names
                        return new BaseTable(baseTable.operations(), quotedTableName(schemaTableName));
                    });
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof NoSuchTableException) {
                throw new TableNotFoundException(schemaTableName, e.getCause());
            }
            throw new TrinoException(ICEBERG_CATALOG_ERROR, format("Failed to load table: %s in %s namespace", schemaTableName.getTableName(), namespace), e.getCause());
        }
    }

    private TableIdentifier toRemoteObject(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableIdentifier remoteTable = toRemoteTable(session, schemaTableName, false);
        if (!remoteTable.name().equals(schemaTableName.getTableName())) {
            return remoteTable;
        }

        TableIdentifier remoteView = toRemoteView(session, schemaTableName, false);
        if (!remoteView.name().equals(schemaTableName.getTableName())) {
            return remoteView;
        }
        if (remoteView.name().equals(schemaTableName.getTableName()) && remoteTable.name().equals(schemaTableName.getTableName())) {
            return remoteTable;
        }
        throw new RuntimeException("Unable to find remote object");
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = restSessionCatalog.loadTable(convert(session), toRemoteTable(session, schemaTableName, true));
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
        invalidateTableCache(schemaTableName);
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String tableName = createLocationForTable(schemaTableName.getTableName());

        Map<String, Object> properties = loadNamespaceMetadata(session, schemaTableName.getSchemaName());
        String databaseLocation = (String) properties.get(IcebergSchemaProperties.LOCATION_PROPERTY);
        checkArgument(databaseLocation != null, "location must be set for %s", schemaTableName.getSchemaName());

        return appendPath(databaseLocation, tableName);
    }

    private String createLocationForTable(String baseTableName)
    {
        String tableName = baseTableName;
        if (useUniqueTableLocation) {
            tableName += "-" + randomUUID().toString().replace("-", "");
        }
        return tableName;
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        definition.getOwner().ifPresent(owner -> properties.put(ICEBERG_VIEW_RUN_AS_OWNER, owner));
        definition.getComment().ifPresent(comment -> properties.put(COMMENT, comment));
        Schema schema = IcebergUtil.schemaFromViewColumns(typeManager, definition.getColumns());
        ViewBuilder viewBuilder = restSessionCatalog.buildView(convert(session), toRemoteView(session, schemaViewName, true));
        viewBuilder = viewBuilder.withSchema(schema)
                .withQuery("trino", definition.getOriginalSql())
                .withDefaultNamespace(toRemoteNamespace(session, toNamespace(schemaViewName.getSchemaName())))
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
        restSessionCatalog.renameView(convert(session), toRemoteView(session, source, true), toRemoteView(session, target, true));
        invalidateTableMappingCache(source);
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        restSessionCatalog.dropView(convert(session), toRemoteView(session, schemaViewName, true));
        invalidateTableMappingCache(schemaViewName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        SessionContext sessionContext = convert(session);
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (Namespace restNamespace : listNamespaces(session, namespace)) {
            for (TableIdentifier restView : restSessionCatalog.listViews(sessionContext, toRemoteNamespace(session, restNamespace))) {
                SchemaTableName schemaTableName = SchemaTableName.schemaTableName(restView.namespace().toString(), restView.name());
                try {
                    getView(session, schemaTableName).ifPresent(view -> views.put(schemaTableName, view));
                }
                catch (TrinoException e) {
                    if (e.getErrorCode().equals(ICEBERG_UNSUPPORTED_VIEW_DIALECT.toErrorCode())) {
                        log.debug(e, "Skip unsupported view dialect: %s", schemaTableName);
                        continue;
                    }
                    throw e;
                }
            }
        }

        return views.buildOrThrow();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return getIcebergView(session, viewName, false).flatMap(view -> {
            SQLViewRepresentation sqlView = view.sqlFor("trino");
            if (!sqlView.dialect().equalsIgnoreCase("trino")) {
                throw new TrinoException(ICEBERG_UNSUPPORTED_VIEW_DIALECT, "Cannot read unsupported dialect '%s' for view '%s'".formatted(sqlView.dialect(), viewName));
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

    private Optional<View> getIcebergView(ConnectorSession session, SchemaTableName viewName, boolean getCached)
    {
        try {
            return Optional.of(restSessionCatalog.loadView(convert(session), toRemoteView(session, viewName, getCached)));
        }
        catch (NoSuchViewException e) {
            return Optional.empty();
        }
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateMaterializedViewColumnComment is not supported for Iceberg REST catalog");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        throw new TrinoException(NOT_SUPPORTED, "The Iceberg REST catalog does not support materialized views");
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "The Iceberg REST catalog does not support materialized views");
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        loadTable(session, schemaTableName).updateSchema()
                .updateColumnDoc(columnIdentity.getName(), comment.orElse(null))
                .commit();
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        return Optional.empty();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        View view = getIcebergView(session, schemaViewName, true).orElseThrow(() -> new ViewNotFoundException(schemaViewName));
        UpdateViewProperties updateViewProperties = view.updateProperties();
        comment.ifPresentOrElse(
                value -> updateViewProperties.set(COMMENT, value),
                () -> updateViewProperties.remove(COMMENT));
        updateViewProperties.commit();
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        View view = getIcebergView(session, schemaViewName, true)
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

    private SessionCatalog.SessionContext convert(ConnectorSession session)
    {
        return switch (sessionType) {
            case NONE -> new SessionContext(randomUUID().toString(), null, credentials, ImmutableMap.of(), session.getIdentity());
            case USER -> {
                String sessionId = format("%s-%s", session.getUser(), session.getSource().orElse("default"));

                Map<String, String> properties = ImmutableMap.of(
                        "user", session.getUser(),
                        "source", session.getSource().orElse(""),
                        "trinoCatalog", catalogName.toString(),
                        "trinoVersion", trinoVersion);

                Map<String, Object> claims = ImmutableMap.<String, Object>builder()
                        .putAll(properties)
                        .buildOrThrow();

                String subjectJwt = new DefaultJwtBuilder()
                        .subject(session.getUser())
                        .issuer(trinoVersion)
                        .issuedAt(new Date())
                        .claims(claims)
                        .json(new JacksonSerializer<>())
                        .compact();

                Map<String, String> credentials = ImmutableMap.<String, String>builder()
                        .putAll(session.getIdentity().getExtraCredentials())
                        .put(OAuth2Properties.JWT_TOKEN_TYPE, subjectJwt)
                        .buildOrThrow();

                yield new SessionCatalog.SessionContext(sessionId, session.getUser(), credentials, properties, session.getIdentity());
            }
        };
    }

    private void invalidateTableCache(SchemaTableName schemaTableName)
    {
        tableCache.invalidate(schemaTableName);
    }

    private void invalidateTableMappingCache(SchemaTableName schemaTableName)
    {
        if (caseInsensitiveNameMatching) {
            remoteTableMappingCache.invalidate(toIdentifier(schemaTableName));
        }
    }

    private Namespace toNamespace(String schemaName)
    {
        if (!nestedNamespaceEnabled && schemaName.contains(NAMESPACE_SEPARATOR)) {
            throw new TrinoException(NOT_SUPPORTED, "Nested namespace is not enabled for this catalog");
        }
        return Namespace.of(Splitter.on(NAMESPACE_SEPARATOR).omitEmptyStrings().trimResults().splitToList(schemaName).toArray(new String[0]));
    }

    private String toSchemaName(Namespace namespace)
    {
        if (!nestedNamespaceEnabled && namespace.length() != 1) {
            throw new TrinoException(NOT_SUPPORTED, "Nested namespace is not enabled for this catalog");
        }
        return String.join(NAMESPACE_SEPARATOR, namespace.levels());
    }

    private TableIdentifier toIdentifier(SchemaTableName schemaTableName)
    {
        return TableIdentifier.of(toNamespace(schemaTableName.getSchemaName()), schemaTableName.getTableName());
    }

    private List<Namespace> listNamespaces(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isEmpty()) {
            return listNamespaces(session).stream()
                    .map(this::toNamespace)
                    .collect(toImmutableList());
        }

        return ImmutableList.of(toNamespace(namespace.get()));
    }

    private TableIdentifier toRemoteTable(ConnectorSession session, SchemaTableName schemaTableName, boolean getCached)
    {
        TableIdentifier tableIdentifier = toIdentifier(schemaTableName);
        return toRemoteObject(tableIdentifier, () -> findRemoteTable(session, tableIdentifier), getCached);
    }

    private TableIdentifier findRemoteTable(ConnectorSession session, TableIdentifier tableIdentifier)
    {
        Namespace remoteNamespace = toRemoteNamespace(session, tableIdentifier.namespace());
        List<TableIdentifier> tableIdentifiers = restSessionCatalog.listTables(convert(session), remoteNamespace);
        TableIdentifier matchingTable = null;
        for (TableIdentifier identifier : tableIdentifiers) {
            if (identifier.name().equalsIgnoreCase(tableIdentifier.name())) {
                if (matchingTable != null) {
                    throw new TrinoException(NOT_SUPPORTED, "Duplicate table names are not supported with Iceberg REST catalog: "
                            + Joiner.on(", ").join(matchingTable, identifier.name()));
                }
                matchingTable = identifier;
            }
        }
        return matchingTable == null ? TableIdentifier.of(remoteNamespace, tableIdentifier.name()) : matchingTable;
    }

    private TableIdentifier toRemoteView(ConnectorSession session, SchemaTableName schemaViewName, boolean getCached)
    {
        TableIdentifier tableIdentifier = toIdentifier(schemaViewName);
        return toRemoteObject(tableIdentifier, () -> findRemoteView(session, tableIdentifier), getCached);
    }

    private TableIdentifier findRemoteView(ConnectorSession session, TableIdentifier tableIdentifier)
    {
        Namespace remoteNamespace = toRemoteNamespace(session, tableIdentifier.namespace());
        List<TableIdentifier> tableIdentifiers = restSessionCatalog.listViews(convert(session), remoteNamespace);
        TableIdentifier matchingView = null;
        for (TableIdentifier identifier : tableIdentifiers) {
            if (identifier.name().equalsIgnoreCase(tableIdentifier.name())) {
                if (matchingView != null) {
                    throw new TrinoException(NOT_SUPPORTED, "Duplicate view names are not supported with Iceberg REST catalog: "
                            + Joiner.on(", ").join(matchingView.name(), identifier.name()));
                }
                matchingView = identifier;
            }
        }
        return matchingView == null ? TableIdentifier.of(remoteNamespace, tableIdentifier.name()) : matchingView;
    }

    private TableIdentifier toRemoteObject(TableIdentifier tableIdentifier, Supplier<TableIdentifier> remoteObjectProvider, boolean getCached)
    {
        if (caseInsensitiveNameMatching) {
            if (getCached) {
                return uncheckedCacheGet(remoteTableMappingCache, tableIdentifier, remoteObjectProvider);
            }
            return remoteObjectProvider.get();
        }
        return tableIdentifier;
    }

    private Namespace toRemoteNamespace(ConnectorSession session, Namespace trinoNamespace)
    {
        if (caseInsensitiveNameMatching) {
            return uncheckedCacheGet(remoteNamespaceMappingCache, trinoNamespace, () -> findRemoteNamespace(session, trinoNamespace));
        }
        return trinoNamespace;
    }

    private Namespace findRemoteNamespace(ConnectorSession session, Namespace trinoNamespace)
    {
        List<Namespace> matchingRemoteNamespaces = listNamespaces(session, Namespace.empty()).stream()
                .filter(ns -> toTrinoNamespace(ns).equals(trinoNamespace))
                .collect(toImmutableList());
        if (matchingRemoteNamespaces.size() > 1) {
            throw new TrinoException(NOT_SUPPORTED, "Duplicate namespace names are not supported with Iceberg REST catalog: " + matchingRemoteNamespaces);
        }
        return matchingRemoteNamespaces.isEmpty() ? trinoNamespace : matchingRemoteNamespaces.getFirst();
    }

    private List<Namespace> listNamespaces(ConnectorSession session, Namespace parentNamespace)
    {
        List<Namespace> childNamespaces = restSessionCatalog.listNamespaces(convert(session), parentNamespace);
        return childNamespaces.stream().flatMap(childNamespace -> Stream.concat(Stream.of(childNamespace), listNamespaces(session, childNamespace).stream())).toList();
    }

    private static Namespace toTrinoNamespace(Namespace namespace)
    {
        return Namespace.of(Arrays.stream(namespace.levels()).map(level -> level.toLowerCase(ENGLISH)).toArray(String[]::new));
    }
}
