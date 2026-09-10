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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.jackson.io.JacksonSerializer;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.IcebergViewProperties;
import io.trino.plugin.iceberg.catalog.MaterializedViewStorageColumns;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.view.RefreshState;
import org.apache.iceberg.view.RefreshStateParser;
import org.apache.iceberg.view.ReplaceViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.SourceState;
import org.apache.iceberg.view.SourceTableState;
import org.apache.iceberg.view.SourceViewState;
import org.apache.iceberg.view.UpdateViewProperties;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_UNSUPPORTED_VIEW_DIALECT;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.getStorageSchema;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.SUPPORTED_SCHEMA_PROPERTIES;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergUtil.commit;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.ICEBERG_VIEW_RUN_AS_OWNER;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH_WITHIN_GRACE_PERIOD;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.CatalogUtil.dropTableData;
import static org.apache.iceberg.view.ViewProperties.COMMENT;

public class TrinoRestCatalog
        implements TrinoCatalog
{
    private static final Logger log = Logger.get(TrinoRestCatalog.class);

    private static final int PER_QUERY_CACHE_SIZE = 1000;
    private static final String NAMESPACE_SEPARATOR = ".";

    private static final String MATERIALIZED_VIEW_GRACE_PERIOD_PROPERTY = "trino.materialized-view.grace-period";
    private static final String MATERIALIZED_VIEW_WHEN_STALE_BEHAVIOR_PROPERTY = "trino.materialized-view.when-stale-behavior";
    private static final String MATERIALIZED_VIEW_PATH_PROPERTY = "trino.materialized-view.path";
    private static final JsonCodec<List<CatalogSchemaName>> MATERIALIZED_VIEW_PATH_CODEC = new JsonCodecFactory().listJsonCodec(CatalogSchemaName.class);
    // The view's own Iceberg Schema must use Iceberg-representable types (same coercion as the storage
    // table), which can differ from what the user declared (e.g. timestamp(3), or timestamp with time
    // zone coerced to varchar). The original type is preserved here since StatementAnalyzer.analyzeView
    // re-validates the materialized view on every use against it, and rejects a coerced type it can't
    // reconcile with the live query's projected type.
    private static final String MATERIALIZED_VIEW_COLUMN_TYPES_PROPERTY = "trino.materialized-view.column-types";
    private static final JsonCodec<Map<String, String>> MATERIALIZED_VIEW_COLUMN_TYPES_CODEC = new JsonCodecFactory().mapJsonCodec(String.class, String.class);
    // Forces MaterializedViewFreshness.Freshness.UNKNOWN when the native scheme can't fully verify freshness.
    private static final String FRESHNESS_UNKNOWN_PROPERTY = "trino.materialized-view.freshness-unknown";

    private final IcebergFileSystemFactory fileSystemFactory;
    private final RESTSessionCatalog restSessionCatalog;
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final Security security;
    private final SessionType sessionType;
    private final Map<String, String> credentials;
    private final boolean nestedNamespaceEnabled;
    private final String trinoVersion;
    private final boolean useUniqueTableLocation;
    private final boolean caseInsensitiveNameMatching;
    private final Cache<Namespace, Namespace> remoteNamespaceMappingCache;
    private final Cache<TableIdentifier, TableIdentifier> remoteTableMappingCache;
    private final boolean viewEndpointsEnabled;
    private final boolean serverAssignedTableLocationEnabled;

    private final Cache<SchemaTableName, BaseTable> tableCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(PER_QUERY_CACHE_SIZE)
            .build();

    public TrinoRestCatalog(
            IcebergFileSystemFactory fileSystemFactory,
            RESTSessionCatalog restSessionCatalog,
            CatalogName catalogName,
            Security security,
            SessionType sessionType,
            Map<String, String> credentials,
            boolean nestedNamespaceEnabled,
            String trinoVersion,
            TypeManager typeManager,
            boolean useUniqueTableLocation,
            boolean caseInsensitiveNameMatching,
            Cache<Namespace, Namespace> remoteNamespaceMappingCache,
            Cache<TableIdentifier, TableIdentifier> remoteTableMappingCache,
            boolean viewEndpointsEnabled,
            boolean serverAssignedTableLocationEnabled)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.security = requireNonNull(security, "security is null");
        this.sessionType = requireNonNull(sessionType, "sessionType is null");
        this.credentials = ImmutableMap.copyOf(requireNonNull(credentials, "credentials is null"));
        this.nestedNamespaceEnabled = nestedNamespaceEnabled;
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        this.remoteNamespaceMappingCache = requireNonNull(remoteNamespaceMappingCache, "remoteNamespaceMappingCache is null");
        this.remoteTableMappingCache = requireNonNull(remoteTableMappingCache, "remoteTableMappingCache is null");
        this.viewEndpointsEnabled = viewEndpointsEnabled;
        this.serverAssignedTableLocationEnabled = serverAssignedTableLocationEnabled;
    }

    @Override
    public Optional<String> getNamespaceSeparator()
    {
        return Optional.of(NAMESPACE_SEPARATOR);
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        try {
            return restSessionCatalog.namespaceExists(convert(session), toRemoteNamespace(session, toNamespace(namespace)));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to check namespace '%s'".formatted(namespace), e);
        }
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        if (nestedNamespaceEnabled) {
            return collectNamespaces(convert(session), Namespace.empty());
        }
        try {
            return restSessionCatalog.listNamespaces(convert(session)).stream()
                    .map(this::toSchemaName)
                    .collect(toImmutableList());
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list namespaces", e);
        }
    }

    private List<String> collectNamespaces(SessionContext sessionContext, Namespace parentNamespace)
    {
        try {
            return restSessionCatalog.listNamespaces(sessionContext, parentNamespace).stream()
                    .flatMap(childNamespace -> collectNamespaceIfExists(sessionContext, childNamespace).stream())
                    .collect(toImmutableList());
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list namespaces", e);
        }
    }

    private List<String> collectNamespaceIfExists(SessionContext sessionContext, Namespace namespace)
    {
        try {
            return Stream.concat(
                            Stream.of(namespace.toString()),
                            collectNamespaces(sessionContext, namespace).stream())
                    .collect(toImmutableList());
        }
        catch (NoSuchNamespaceException e) {
            return ImmutableList.of();
        }
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
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to drop namespace '%s'".formatted(namespace), e);
        }
        finally {
            if (caseInsensitiveNameMatching) {
                remoteNamespaceMappingCache.invalidate(toNamespace(namespace));
            }
        }
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        try {
            // Return immutable metadata as direct modifications will not be reflected on the namespace
            return restSessionCatalog.loadNamespaceMetadata(convert(session), toRemoteNamespace(session, toNamespace(namespace))).entrySet().stream()
                    .filter(property -> SUPPORTED_SCHEMA_PROPERTIES.contains(property.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        catch (NoSuchNamespaceException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to load metadata for namespace '%s'".formatted(namespace), e);
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
        try {
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
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to create namespace '%s'".formatted(namespace), e);
        }
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
            listTableIdentifiers(restNamespace, () -> {
                try {
                    return restSessionCatalog.listTables(sessionContext, toRemoteNamespace(session, restNamespace));
                }
                catch (RESTException e) {
                    throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list tables", e);
                }
            }).stream()
                    .map(id -> new TableInfo(SchemaTableName.schemaTableName(toSchemaName(id.namespace()), id.name()), TableInfo.ExtendedRelationType.TABLE))
                    .forEach(tables::add);
            if (viewEndpointsEnabled) {
                listTableIdentifiers(restNamespace, () -> {
                    try {
                        return restSessionCatalog.listViews(sessionContext, toRemoteNamespace(session, restNamespace));
                    }
                    catch (RESTException e) {
                        throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list views", e);
                    }
                }).stream()
                        .map(id -> new TableInfo(SchemaTableName.schemaTableName(toSchemaName(id.namespace()), id.name()), TableInfo.ExtendedRelationType.OTHER_VIEW))
                        .forEach(tables::add);
            }
        }
        return tables.build();
    }

    @Override
    public List<SchemaTableName> listIcebergTables(ConnectorSession session, List<String> filter)
    {
        SessionContext sessionContext = convert(session);
        List<Namespace> namespaces = filter.isEmpty()
                ? listNamespaces(session).stream().map(this::toNamespace).collect(toImmutableList())
                : filter.stream().map(this::toNamespace).collect(toImmutableList());

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (Namespace restNamespace : namespaces) {
            listTableIdentifiers(restNamespace, () -> {
                try {
                    return restSessionCatalog.listTables(sessionContext, toRemoteNamespace(session, restNamespace));
                }
                catch (RESTException e) {
                    throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list tables", e);
                }
            }).stream()
                    .map(id -> SchemaTableName.schemaTableName(toSchemaName(id.namespace()), id.name()))
                    .forEach(tables::add);
        }
        return tables.build();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return listClassifiedViews(session, namespace, false);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return listClassifiedViews(session, namespace, true);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> namespace)
    {
        // listTables reports every view/materialized view as OTHER_VIEW (see above), so unlike the
        // default TrinoCatalog.getRelationTypes, it can't be relied on alone to classify a materialized
        // view; overlay listMaterializedViews so it's still reported as such.
        Map<SchemaTableName, RelationType> result = new LinkedHashMap<>(TrinoCatalog.super.getRelationTypes(session, namespace));
        for (SchemaTableName name : listMaterializedViews(session, namespace)) {
            result.put(name, RelationType.MATERIALIZED_VIEW);
        }
        return ImmutableMap.copyOf(result);
    }

    // REST has no bulk endpoint returning view metadata, so distinguishing a materialized view
    // (an Iceberg view whose current version has a non-null storageTable(), created by Trino)
    // from a plain view requires loading each view individually.
    private List<SchemaTableName> listClassifiedViews(ConnectorSession session, Optional<String> namespace, boolean materializedViews)
    {
        if (!viewEndpointsEnabled) {
            return ImmutableList.of();
        }

        SessionContext sessionContext = convert(session);
        List<Namespace> namespaces = listNamespaces(session, namespace);

        ImmutableList.Builder<SchemaTableName> viewNames = ImmutableList.builder();
        for (Namespace restNamespace : namespaces) {
            for (TableIdentifier identifier : listTableIdentifiers(restNamespace, () -> {
                try {
                    return restSessionCatalog.listViews(sessionContext, toRemoteNamespace(session, restNamespace));
                }
                catch (RESTException e) {
                    throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list views", e);
                }
            })) {
                SchemaTableName schemaTableName = SchemaTableName.schemaTableName(identifier.namespace().toString(), identifier.name());
                boolean isMaterializedView = getIcebergView(session, schemaTableName, false)
                        .map(view -> view.currentVersion().storageTable() != null)
                        .orElse(false);
                if (isMaterializedView == materializedViews) {
                    viewNames.add(schemaTableName);
                }
            }
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
            Optional<String> location,
            Map<String, String> properties)
    {
        try {
            Catalog.TableBuilder tableBuilder = restSessionCatalog.buildTable(convert(session), toRemoteTable(session, schemaTableName, true), schema)
                    .withPartitionSpec(partitionSpec)
                    .withSortOrder(sortOrder)
                    .withProperties(properties);
            if (location.isEmpty()) {
                // TODO Replace with createTransaction once S3 Tables supports stage-create option
                return tableBuilder.create().newTransaction();
            }
            return tableBuilder.withLocation(location.get()).createTransaction();
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to create transaction", e);
        }
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
        try {
            return restSessionCatalog.buildTable(convert(session), toRemoteTable(session, schemaTableName, true), schema)
                    .withPartitionSpec(partitionSpec)
                    .withSortOrder(sortOrder)
                    .withLocation(location)
                    .withProperties(properties)
                    .createOrReplaceTransaction();
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to create transaction", e);
        }
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        TableIdentifier tableIdentifier = TableIdentifier.of(toRemoteNamespace(session, toNamespace(tableName.getSchemaName())), tableName.getTableName());
        try {
            restSessionCatalog.registerTable(convert(session), tableIdentifier, tableMetadata.metadataFileLocation());
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to register table '%s'".formatted(tableName.getTableName()), e);
        }
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            if (!restSessionCatalog.dropTable(convert(session), toRemoteTable(session, tableName, true))) {
                throw new TableNotFoundException(tableName);
            }
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to unregister table '%s'".formatted(tableName.getTableName()), e);
        }
        finally {
            invalidateTableCache(tableName);
            invalidateTableMappingCache(tableName);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            if (security == Security.GOOGLE) {
                purgeBigLakeTable(session, schemaTableName);
            }
            else {
                purgeTable(session, schemaTableName);
            }
        }
        finally {
            invalidateTableCache(schemaTableName);
            invalidateTableMappingCache(schemaTableName);
        }
    }

    private void purgeBigLakeTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BaseTable table = loadTable(session, schemaTableName);
        unregisterTable(session, schemaTableName);
        try {
            // Explicitly remove data like TrinoGlueCatalog.dropTable since BigLake doesn't delete its data and metadata
            dropTableData(table.io(), table.operations().current());
        }
        catch (RuntimeException e) {
            // If the snapshot file is not found, an exception will be thrown by the dropTableData function.
            // So log the exception and continue with deleting the table location
            log.warn(e, "Failed to delete table data referenced by metadata");
        }
        deleteTableDirectory(fileSystemFactory.create(session.getIdentity(), IcebergTableCredentials.forFileIO(table.io())), schemaTableName, table.location());
    }

    private static void deleteTableDirectory(TrinoFileSystem fileSystem, SchemaTableName schemaTableName, String tableLocation)
    {
        try {
            fileSystem.deleteDirectory(Location.of(tableLocation));
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", tableLocation, schemaTableName), e);
        }
    }

    private void purgeTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            if (!restSessionCatalog.purgeTable(convert(session), toRemoteTable(session, schemaTableName, true))) {
                throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to drop table '%s'".formatted(schemaTableName));
            }
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to drop table '%s'".formatted(schemaTableName.getTableName()), e);
        }
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
        finally {
            invalidateTableCache(from);
            invalidateTableMappingCache(from);
        }
    }

    @Override
    public BaseTable loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Namespace namespace = toNamespace(schemaTableName.getSchemaName());
        try {
            return uncheckedCacheGet(
                    tableCache,
                    schemaTableName,
                    () -> {
                        BaseTable baseTable;
                        try {
                            baseTable = (BaseTable) restSessionCatalog.loadTable(convert(session), toRemoteObject(session, schemaTableName));
                        }
                        catch (RESTException e) {
                            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to load table '%s'".formatted(schemaTableName.getTableName()), e);
                        }
                        // Creating a new base table is necessary to adhere to Trino's expectations for quoted table names
                        return new BaseTable(baseTable.operations(), quotedTableName(schemaTableName), baseTable.reporter());
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
        TableIdentifier tableIdentifier = toIdentifier(schemaTableName);
        return toRemoteTableIfExists(session, tableIdentifier, false)
                .orElseGet(() -> toRemoteViewIfExists(session, tableIdentifier, false)
                        .orElseGet(() -> toRemoteIdentifier(session, tableIdentifier)));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable;
        try {
            icebergTable = restSessionCatalog.loadTable(convert(session), toRemoteTable(session, schemaTableName, true));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to load table '%s'".formatted(schemaTableName.getTableName()), e);
        }
        try {
            if (comment.isEmpty()) {
                icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
            }
            else {
                icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
            }
        }
        finally {
            invalidateTableCache(schemaTableName);
        }
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (serverAssignedTableLocationEnabled) {
            return null;
        }

        String tableName = createLocationForTable(schemaTableName.getTableName());

        Map<String, Object> properties = loadNamespaceMetadata(session, schemaTableName.getSchemaName());
        String databaseLocation = (String) properties.get(LOCATION_PROPERTY);
        if (databaseLocation == null) {
            // Iceberg REST catalog doesn't require location property.
            // S3 Tables doesn't return the property.
            return null;
        }

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
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        definition.getOwner().ifPresent(owner -> properties.put(ICEBERG_VIEW_RUN_AS_OWNER, owner));
        definition.getComment().ifPresent(comment -> properties.put(COMMENT, comment));
        Schema schema = IcebergUtil.schemaFromViewColumns(typeManager, definition.getColumns());
        ViewBuilder viewBuilder = restSessionCatalog.buildView(convert(session), toRemoteView(session, schemaViewName, true));
        Optional<String> locationProperty = IcebergViewProperties.getLocation(viewProperties);
        String viewLocation = locationProperty.map(LocationUtil::stripTrailingSlash).orElse(defaultTableLocation(session, schemaViewName));
        if (replace) {
            Optional<View> view = getIcebergView(session, schemaViewName, true);
            if (view.isPresent()) {
                viewLocation = view.get().location();
                if (locationProperty.isPresent() && !viewLocation.equals(locationProperty.get())) {
                    throw new TrinoException(ICEBERG_CATALOG_ERROR, "Cannot change location of existing view '%s'".formatted(schemaViewName));
                }
            }
        }
        viewBuilder = viewBuilder.withSchema(schema)
                .withQuery("trino", definition.getOriginalSql())
                .withDefaultNamespace(toRemoteNamespace(session, toNamespace(schemaViewName.getSchemaName())))
                .withDefaultCatalog(definition.getCatalog().orElse(null))
                .withProperties(properties.buildOrThrow())
                .withLocation(viewLocation);
        try {
            if (replace) {
                viewBuilder.createOrReplace();
            }
            else {
                viewBuilder.create();
            }
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to create view '%s'".formatted(schemaViewName.getTableName()), e);
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        try {
            restSessionCatalog.renameView(convert(session), toRemoteView(session, source, true), toRemoteView(session, target, true));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to rename view '%s' to '%s'".formatted(source, target), e);
        }
        finally {
            invalidateTableMappingCache(source);
        }
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg REST catalog");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        try {
            restSessionCatalog.dropView(convert(session), toRemoteView(session, schemaViewName, true));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to drop view '%s'".formatted(schemaViewName.getTableName()), e);
        }
        finally {
            invalidateTableMappingCache(schemaViewName);
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        SessionContext sessionContext = convert(session);
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (Namespace restNamespace : listNamespaces(session, namespace)) {
            List<TableIdentifier> restViews;
            try {
                restViews = restSessionCatalog.listViews(sessionContext, toRemoteNamespace(session, restNamespace));
            }
            catch (NoSuchNamespaceException e) {
                continue;
            }
            catch (RESTException e) {
                throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list views", e);
            }
            for (TableIdentifier restView : restViews) {
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
            if (view.currentVersion().storageTable() != null) {
                // Materialized view, not a plain view
                return Optional.empty();
            }
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

    @Override
    public Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        getIcebergView(session, viewName, false).ifPresent(view -> {
            properties.put(LOCATION_PROPERTY, view.location());
        });
        return properties.buildOrThrow();
    }

    @VisibleForTesting
    protected Optional<View> getIcebergView(ConnectorSession session, SchemaTableName viewName, boolean getCached)
    {
        if (!viewEndpointsEnabled) {
            return Optional.empty();
        }

        try {
            return Optional.of(restSessionCatalog.loadView(convert(session), toRemoteView(session, viewName, getCached)));
        }
        catch (NoSuchViewException e) {
            return Optional.empty();
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to load view '%s'".formatted(viewName.getTableName()), e);
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
        Optional<View> existing = getIcebergView(session, viewName, true);
        if (existing.isPresent()) {
            if (existing.get().currentVersion().storageTable() == null) {
                throw new TrinoException(NOT_SUPPORTED, "Existing object is not a Materialized View: " + viewName);
            }
            if (!replace) {
                if (ignoreExisting) {
                    return;
                }
                throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
            }
            replaceMaterializedView(session, viewName, existing.get(), definition, materializedViewProperties);
            return;
        }
        if (tableExists(session, viewName)) {
            throw new TrinoException(NOT_SUPPORTED, "Existing table is not a Materialized View: " + viewName);
        }

        SchemaTableName storageTableName = createMaterializedViewStorageTable(session, viewName, definition, materializedViewProperties);
        createMaterializedViewObject(session, viewName, storageTableName, definition, materializedViewProperties, false);
    }

    private boolean tableExists(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            loadTable(session, tableName);
            return true;
        }
        catch (TableNotFoundException e) {
            return false;
        }
    }

    private void replaceMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            View existingView,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        SchemaTableName storageTableName = toSchemaTableName(existingView.currentVersion().storageTable());
        replaceMaterializedViewStorageTable(session, storageTableName, definition, materializedViewProperties);
        createMaterializedViewObject(session, viewName, storageTableName, definition, materializedViewProperties, true);
    }

    private SchemaTableName createMaterializedViewStorageTable(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        String storageSchema = getStorageSchema(materializedViewProperties).orElse(viewName.getSchemaName());
        SchemaTableName storageTableName = new SchemaTableName(storageSchema, "st_" + randomUUID().toString().replace("-", ""));
        List<ColumnMetadata> columns = MaterializedViewStorageColumns.columnsForMaterializedView(typeManager, definition, materializedViewProperties);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTableName, columns, materializedViewProperties, Optional.empty());
        String tableLocation = getTableLocation(tableMetadata.getProperties())
                .orElseGet(() -> defaultTableLocation(session, storageTableName));

        Transaction transaction = IcebergUtil.newCreateTableTransaction(this, tableMetadata, session, false, tableLocation, _ -> false, ImmutableList.of());
        transaction.commitTransaction();
        return storageTableName;
    }

    private void replaceMaterializedViewStorageTable(
            ConnectorSession session,
            SchemaTableName storageTableName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        BaseTable existingStorageTable = loadTable(session, storageTableName);
        Optional<String> providedLocation = getTableLocation(materializedViewProperties);
        if (providedLocation.isPresent() && !LocationUtil.stripTrailingSlash(providedLocation.get()).equals(existingStorageTable.location())) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "The provided location '%s' does not match the existing storage table location '%s'".formatted(providedLocation.get(), existingStorageTable.location()));
        }
        List<ColumnMetadata> columns = MaterializedViewStorageColumns.columnsForMaterializedView(typeManager, definition, materializedViewProperties);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTableName, columns, materializedViewProperties, Optional.empty());

        Transaction transaction = IcebergUtil.newCreateTableTransaction(this, tableMetadata, session, true, existingStorageTable.location(), _ -> false, ImmutableList.of());
        // Replacing the storage table's definition doesn't carry over old data
        transaction.newDelete()
                .deleteFromRowFilter(Expressions.alwaysTrue())
                .commit();
        transaction.commitTransaction();
        invalidateTableCache(storageTableName);
    }

    private void createMaterializedViewObject(
            ConnectorSession session,
            SchemaTableName viewName,
            SchemaTableName storageTableName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        definition.getOwner().ifPresent(owner -> properties.put(ICEBERG_VIEW_RUN_AS_OWNER, owner));
        definition.getComment().ifPresent(comment -> properties.put(COMMENT, comment));
        definition.getGracePeriod().ifPresent(gracePeriod -> properties.put(MATERIALIZED_VIEW_GRACE_PERIOD_PROPERTY, gracePeriod.toString()));
        definition.getWhenStaleBehavior().ifPresent(whenStaleBehavior -> properties.put(MATERIALIZED_VIEW_WHEN_STALE_BEHAVIOR_PROPERTY, whenStaleBehavior.name()));
        if (!definition.getPath().isEmpty()) {
            properties.put(MATERIALIZED_VIEW_PATH_PROPERTY, MATERIALIZED_VIEW_PATH_CODEC.toJson(definition.getPath()));
        }
        Map<String, String> originalColumnTypes = definition.getColumns().stream()
                .collect(toImmutableMap(ConnectorMaterializedViewDefinition.Column::getName, column -> column.getType().getId()));
        properties.put(MATERIALIZED_VIEW_COLUMN_TYPES_PROPERTY, MATERIALIZED_VIEW_COLUMN_TYPES_CODEC.toJson(originalColumnTypes));

        // The view's own schema needs the same type coercion as the storage table's columns.
        Map<String, Optional<String>> columnComments = definition.getColumns().stream()
                .collect(toImmutableMap(ConnectorMaterializedViewDefinition.Column::getName, ConnectorMaterializedViewDefinition.Column::getComment));
        List<ConnectorViewDefinition.ViewColumn> viewColumns = MaterializedViewStorageColumns.columnsForMaterializedView(typeManager, definition, materializedViewProperties).stream()
                .map(column -> new ConnectorViewDefinition.ViewColumn(column.getName(), column.getType().getTypeId(), columnComments.get(column.getName())))
                .collect(toImmutableList());
        Schema schema = IcebergUtil.schemaFromViewColumns(typeManager, viewColumns);

        Namespace defaultNamespace = definition.getSchema()
                .map(schemaName -> toRemoteNamespace(session, toNamespace(schemaName)))
                .orElse(Namespace.empty());
        ViewBuilder viewBuilder = restSessionCatalog.buildView(convert(session), toRemoteView(session, viewName, true))
                .withSchema(schema)
                .withQuery("trino", definition.getOriginalSql())
                .withDefaultNamespace(defaultNamespace)
                .withDefaultCatalog(definition.getCatalog().orElse(null))
                .withProperties(properties.buildOrThrow())
                .withLocation(defaultTableLocation(session, viewName))
                .withStorageTableIdentifier(toRemoteTable(session, storageTableName, true));
        try {
            if (replace) {
                viewBuilder.createOrReplace();
            }
            else {
                viewBuilder.create();
            }
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to create materialized view '%s'".formatted(viewName.getTableName()), e);
        }
        finally {
            invalidateTableMappingCache(viewName);
        }
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        View view = getIcebergView(session, schemaViewName, true).orElseThrow(() -> new MaterializedViewNotFoundException(schemaViewName));
        TableIdentifier storageTableIdentifier = view.currentVersion().storageTable();
        if (storageTableIdentifier == null) {
            throw new MaterializedViewNotFoundException(schemaViewName);
        }

        // Column comments live in the view's own Schema (like plain views), which can only change via a
        // new ViewVersion. Rebuilding through ViewBuilder.createOrReplace() (rather than
        // view.replaceVersion(), used for plain views) is required to keep storageTable() set on the new
        // version; everything except the updated column's comment is carried over unchanged.
        ViewVersion currentVersion = view.currentVersion();
        Schema updatedSchema = IcebergUtil.updateColumnComment(view.schema(), columnName, comment.orElse(null));
        SQLViewRepresentation sqlView = view.sqlFor("trino");

        ViewBuilder viewBuilder = restSessionCatalog.buildView(convert(session), toRemoteView(session, schemaViewName, true))
                .withSchema(updatedSchema)
                .withQuery("trino", sqlView.sql())
                .withDefaultNamespace(currentVersion.defaultNamespace())
                .withDefaultCatalog(currentVersion.defaultCatalog())
                .withProperties(view.properties())
                .withLocation(view.location())
                .withStorageTableIdentifier(toRemoteTable(session, toSchemaTableName(storageTableIdentifier), true));
        try {
            viewBuilder.createOrReplace();
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to update column comment for materialized view '%s'".formatted(schemaViewName.getTableName()), e);
        }
        finally {
            invalidateTableMappingCache(schemaViewName);
        }
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        View view = getIcebergView(session, viewName, true).orElseThrow(() -> new MaterializedViewNotFoundException(viewName));
        TableIdentifier storageTableIdentifier = view.currentVersion().storageTable();
        if (storageTableIdentifier == null) {
            throw new TrinoException(NOT_SUPPORTED, "Not a Materialized View: " + viewName);
        }

        try {
            restSessionCatalog.dropView(convert(session), toRemoteView(session, viewName, true));
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to drop materialized view '%s'".formatted(viewName.getTableName()), e);
        }
        finally {
            invalidateTableMappingCache(viewName);
        }

        SchemaTableName storageTableName = toSchemaTableName(storageTableIdentifier);
        try {
            purgeTable(session, storageTableName);
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to drop storage table '%s' of materialized view '%s'", storageTableName, viewName);
        }
        finally {
            invalidateTableCache(storageTableName);
            invalidateTableMappingCache(storageTableName);
        }
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return getIcebergView(session, viewName, false).flatMap(this::decodeMaterializedView);
    }

    private Optional<ConnectorMaterializedViewDefinition> decodeMaterializedView(View view)
    {
        TableIdentifier storageTableIdentifier = view.currentVersion().storageTable();
        if (storageTableIdentifier == null) {
            // Regular view, not a materialized view
            return Optional.empty();
        }
        Map<String, String> properties = view.properties();
        SQLViewRepresentation sqlView = view.sqlFor("trino");
        if (!sqlView.dialect().equalsIgnoreCase("trino")) {
            throw new TrinoException(ICEBERG_UNSUPPORTED_VIEW_DIALECT, "Cannot read unsupported dialect '%s' for materialized view '%s'".formatted(sqlView.dialect(), view.name()));
        }

        ViewVersion currentVersion = view.currentVersion();
        Optional<String> catalog = Optional.ofNullable(currentVersion.defaultCatalog());
        Optional<String> schema = Optional.empty();
        if (catalog.isPresent() && !currentVersion.defaultNamespace().isEmpty()) {
            schema = Optional.of(currentVersion.defaultNamespace().toString());
        }

        // Prefer the preserved original column type (see the property's declaration); fall back to the
        // (possibly coerced) schema type for a foreign-engine-created materialized view, which never
        // wrote this property.
        Map<String, String> originalColumnTypes = Optional.ofNullable(properties.get(MATERIALIZED_VIEW_COLUMN_TYPES_PROPERTY))
                .map(MATERIALIZED_VIEW_COLUMN_TYPES_CODEC::fromJson)
                .orElse(ImmutableMap.of());
        List<ConnectorMaterializedViewDefinition.Column> columns = IcebergUtil.viewColumnsFromSchema(typeManager, view.schema()).stream()
                .map(column -> new ConnectorMaterializedViewDefinition.Column(
                        column.getName(),
                        Optional.ofNullable(originalColumnTypes.get(column.getName())).map(TypeId::of).orElse(column.getType()),
                        column.getComment()))
                .collect(toImmutableList());

        Optional<Duration> gracePeriod = Optional.ofNullable(properties.get(MATERIALIZED_VIEW_GRACE_PERIOD_PROPERTY)).map(Duration::parse);
        Optional<ConnectorMaterializedViewDefinition.WhenStaleBehavior> whenStaleBehavior = Optional.ofNullable(properties.get(MATERIALIZED_VIEW_WHEN_STALE_BEHAVIOR_PROPERTY))
                .map(ConnectorMaterializedViewDefinition.WhenStaleBehavior::valueOf);
        List<CatalogSchemaName> path = Optional.ofNullable(properties.get(MATERIALIZED_VIEW_PATH_PROPERTY))
                .map(MATERIALIZED_VIEW_PATH_CODEC::fromJson)
                .orElse(ImmutableList.of());

        Optional<String> comment = Optional.ofNullable(properties.get(COMMENT));
        Optional<String> owner = Optional.ofNullable(properties.get(ICEBERG_VIEW_RUN_AS_OWNER));
        SchemaTableName storageTableName = toSchemaTableName(storageTableIdentifier);

        return Optional.of(new ConnectorMaterializedViewDefinition(
                sqlView.sql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), storageTableName)),
                catalog,
                schema,
                columns,
                gracePeriod,
                whenStaleBehavior,
                comment,
                owner,
                path));
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        SchemaTableName storageTableName = definition.getStorageTable()
                .orElseThrow(() -> new TrinoException(ICEBERG_CATALOG_ERROR, "Materialized view definition is missing a storage table"))
                .getSchemaTableName();
        BaseTable storageTable = loadTable(session, storageTableName);
        return ImmutableMap.<String, Object>builder()
                .putAll(getIcebergTableProperties(storageTable))
                .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                .buildOrThrow();
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        return getIcebergView(session, viewName, false)
                .map(View::currentVersion)
                .map(ViewVersion::storageTable)
                .map(this::toSchemaTableName)
                .map(storageTableName -> loadTable(session, storageTableName));
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        View view = getIcebergView(session, source, true).orElseThrow(() -> new MaterializedViewNotFoundException(source));
        if (view.currentVersion().storageTable() == null) {
            throw new TrinoException(NOT_SUPPORTED, "Not a Materialized View: " + source);
        }
        renameView(session, source, target);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName, boolean considerGracePeriod)
    {
        Optional<View> view = getIcebergView(session, materializedViewName, false);
        if (view.isEmpty()) {
            // View not found, might have been concurrently deleted
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }
        Optional<ConnectorMaterializedViewDefinition> definition = decodeMaterializedView(view.get());
        if (definition.isEmpty()) {
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        Table storageTable;
        try {
            storageTable = loadTable(session, toSchemaTableName(view.get().currentVersion().storageTable()));
        }
        catch (RuntimeException e) {
            return new MaterializedViewFreshness(UNKNOWN, Optional.empty());
        }

        Snapshot currentSnapshot = storageTable.currentSnapshot();
        if (currentSnapshot == null) {
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }
        Map<String, String> summary = currentSnapshot.summary();
        String refreshStateJson = summary.get(RefreshState.REFRESH_STATE_SUMMARY_KEY);
        if (refreshStateJson == null) {
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }
        RefreshState refreshState = RefreshStateParser.fromJson(refreshStateJson);
        Instant refreshTime = Instant.ofEpochMilli(refreshState.refreshStartTimestampMs());

        if (refreshState.viewVersionId() != view.get().currentVersion().versionId()) {
            // The materialized view definition was replaced since the last refresh
            return new MaterializedViewFreshness(STALE, Optional.of(refreshTime));
        }

        if (Boolean.parseBoolean(summary.get(FRESHNESS_UNKNOWN_PROPERTY))) {
            return new MaterializedViewFreshness(UNKNOWN, Optional.of(refreshTime));
        }

        if (considerGracePeriod && withinGracePeriod(session.getStart(), refreshTime, definition.get().getGracePeriod())) {
            return new MaterializedViewFreshness(FRESH_WITHIN_GRACE_PERIOD, Optional.empty());
        }

        boolean hasStaleSources = false;
        boolean hasUnknownSources = false;
        for (SourceState sourceState : refreshState.sourceStates()) {
            if (sourceState instanceof SourceTableState tableState) {
                if (tableState.catalog() != null && !tableState.catalog().equals(catalogName.toString())) {
                    // Recorded against a different Trino catalog; this catalog cannot verify it
                    hasUnknownSources = true;
                    continue;
                }
                SchemaTableName sourceTableName = SchemaTableName.schemaTableName(String.join(NAMESPACE_SEPARATOR, tableState.namespace()), tableState.name());
                Table sourceTable;
                try {
                    sourceTable = loadTable(session, sourceTableName);
                }
                catch (RuntimeException e) {
                    // Base table is gone, or corrupted, or can't otherwise be resolved: treat conservatively as changed
                    hasStaleSources = true;
                    continue;
                }
                if (!tableState.uuid().equals(sourceTable.uuid().toString())) {
                    // Table was dropped and re-created since the last refresh
                    hasStaleSources = true;
                    continue;
                }
                long sourceCurrentSnapshotId;
                if (tableState.ref() != null) {
                    SnapshotRef ref = sourceTable.refs().get(tableState.ref());
                    if (ref == null) {
                        // The branch was dropped or renamed since the last refresh
                        hasStaleSources = true;
                        continue;
                    }
                    sourceCurrentSnapshotId = ref.snapshotId();
                }
                else {
                    Snapshot sourceCurrentSnapshot = sourceTable.currentSnapshot();
                    sourceCurrentSnapshotId = sourceCurrentSnapshot == null ? -1 : sourceCurrentSnapshot.snapshotId();
                }
                if (sourceCurrentSnapshotId != tableState.snapshotId()) {
                    hasStaleSources = true;
                }
                else {
                    // A schema-only change (rename, added column, etc.) doesn't bump the snapshot id.
                    Snapshot recordedSnapshot = sourceTable.snapshot(tableState.snapshotId());
                    Integer recordedSchemaId = recordedSnapshot == null ? null : recordedSnapshot.schemaId();
                    if (recordedSchemaId != null && !recordedSchemaId.equals(sourceTable.schema().schemaId())) {
                        hasStaleSources = true;
                    }
                }
            }
            else if (sourceState instanceof SourceViewState viewState) {
                if (viewState.catalog() != null && !viewState.catalog().equals(catalogName.toString())) {
                    // Recorded against a different Trino catalog; this catalog cannot verify it
                    hasUnknownSources = true;
                    continue;
                }
                SchemaTableName sourceViewName = SchemaTableName.schemaTableName(String.join(NAMESPACE_SEPARATOR, viewState.namespace()), viewState.name());
                Optional<View> sourceView = getIcebergView(session, sourceViewName, true);
                if (sourceView.isEmpty()) {
                    // View is gone, or corrupted, or can't otherwise be resolved: treat conservatively as changed
                    hasStaleSources = true;
                    continue;
                }
                if (!viewState.uuid().equals(sourceView.get().uuid().toString())) {
                    // View was dropped and re-created since the last refresh
                    hasStaleSources = true;
                    continue;
                }
                if (sourceView.get().currentVersion().versionId() != viewState.versionId()) {
                    // View definition was replaced since the last refresh
                    hasStaleSources = true;
                }
            }
        }

        if (hasStaleSources) {
            return new MaterializedViewFreshness(STALE, Optional.of(refreshTime));
        }
        if (hasUnknownSources) {
            return new MaterializedViewFreshness(UNKNOWN, Optional.of(refreshTime));
        }
        return new MaterializedViewFreshness(FRESH, Optional.empty());
    }

    private static boolean withinGracePeriod(Instant sessionStart, Instant refreshTime, Optional<Duration> gracePeriod)
    {
        if (gracePeriod.isEmpty()) {
            // infinite grace period
            return true;
        }
        return refreshTime.plus(gracePeriod.get()).isAfter(sessionStart);
    }

    @Override
    public void recordMaterializedViewRefresh(
            ConnectorSession session,
            SchemaTableName materializedViewName,
            AppendFiles appendFiles,
            List<ConnectorTableHandle> sourceTableHandles,
            List<CatalogSchemaTableName> sourceViewNames,
            boolean hasForeignSourceTables,
            boolean hasSourceTableFunctions,
            boolean hasNonDeterministicFunctions)
    {
        View view = getIcebergView(session, materializedViewName, true)
                .orElseThrow(() -> new MaterializedViewNotFoundException(materializedViewName));

        ImmutableList.Builder<SourceState> sourceStates = ImmutableList.builder();
        for (ConnectorTableHandle handle : sourceTableHandles) {
            IcebergTableHandle sourceTableHandle = (IcebergTableHandle) handle;
            SchemaTableName sourceTableName = sourceTableHandle.getSchemaTableName();
            Table sourceTable = loadTable(session, sourceTableName);
            Optional<String> branch = sourceTableHandle.getBranch();
            long currentSnapshotId;
            if (branch.isPresent()) {
                SnapshotRef ref = sourceTable.refs().get(branch.get());
                if (ref == null) {
                    throw new TrinoException(ICEBERG_CATALOG_ERROR, "Branch '%s' no longer exists on source table '%s'".formatted(branch.get(), sourceTableName));
                }
                currentSnapshotId = ref.snapshotId();
            }
            else {
                Snapshot currentSnapshot = sourceTable.currentSnapshot();
                currentSnapshotId = currentSnapshot == null ? -1 : currentSnapshot.snapshotId();
            }
            sourceStates.add(new SourceTableState(
                    sourceTableName.getTableName(),
                    ImmutableList.copyOf(toNamespace(sourceTableName.getSchemaName()).levels()),
                    catalogName.toString(),
                    sourceTable.uuid().toString(),
                    currentSnapshotId,
                    branch.orElse(null)));
        }

        boolean hasUnverifiedSourceViews = false;
        for (CatalogSchemaTableName sourceViewName : sourceViewNames) {
            if (!sourceViewName.getCatalogName().equals(catalogName.toString())) {
                // Cross-catalog view reference: this catalog has no way to load it
                hasUnverifiedSourceViews = true;
                continue;
            }
            SchemaTableName schemaTableName = sourceViewName.getSchemaTableName();
            Optional<View> sourceView = getIcebergView(session, schemaTableName, true);
            if (sourceView.isEmpty()) {
                hasUnverifiedSourceViews = true;
                continue;
            }
            sourceStates.add(new SourceViewState(
                    schemaTableName.getTableName(),
                    ImmutableList.copyOf(toNamespace(schemaTableName.getSchemaName()).levels()),
                    catalogName.toString(),
                    sourceView.get().uuid().toString(),
                    sourceView.get().currentVersion().versionId()));
        }

        RefreshState refreshState = new RefreshState(view.currentVersion().versionId(), sourceStates.build(), session.getStart().toEpochMilli());
        appendFiles.set(RefreshState.REFRESH_STATE_SUMMARY_KEY, RefreshStateParser.toJson(refreshState));
        if (hasForeignSourceTables || hasSourceTableFunctions || hasNonDeterministicFunctions || hasUnverifiedSourceViews) {
            appendFiles.set(FRESHNESS_UNKNOWN_PROPERTY, "true");
        }
    }

    @Override
    public OptionalLong getMaterializedViewIncrementalRefreshFromSnapshot(Table storageTable, List<ConnectorTableHandle> sourceTableHandles)
    {
        if (sourceTableHandles.size() != 1) {
            return OptionalLong.empty();
        }
        Snapshot currentSnapshot = storageTable.currentSnapshot();
        if (currentSnapshot == null) {
            return OptionalLong.empty();
        }
        Map<String, String> summary = currentSnapshot.summary();
        if (Boolean.parseBoolean(summary.get(FRESHNESS_UNKNOWN_PROPERTY))) {
            // An untracked dependency existed at the last refresh; can't safely narrow the read
            return OptionalLong.empty();
        }
        String refreshStateJson = summary.get(RefreshState.REFRESH_STATE_SUMMARY_KEY);
        if (refreshStateJson == null) {
            return OptionalLong.empty();
        }
        RefreshState refreshState = RefreshStateParser.fromJson(refreshStateJson);
        List<SourceState> sourceStates = refreshState.sourceStates();
        if (sourceStates.size() != 1 || !(sourceStates.get(0) instanceof SourceTableState tableState)) {
            // Either no dependencies were recorded, or there's a source view dependency alongside
            // the source table: incremental refresh from a single table's snapshot could silently
            // miss a view-induced change, so fall back to full refresh.
            return OptionalLong.empty();
        }
        if (tableState.catalog() != null && !tableState.catalog().equals(catalogName.toString())) {
            return OptionalLong.empty();
        }
        IcebergTableHandle handle = (IcebergTableHandle) getOnlyElement(sourceTableHandles);
        SchemaTableName sourceSchemaTable = SchemaTableName.schemaTableName(String.join(NAMESPACE_SEPARATOR, tableState.namespace()), tableState.name());
        if (!sourceSchemaTable.equals(handle.getSchemaTableName())) {
            return OptionalLong.empty();
        }
        if (!Objects.equals(handle.getBranch().orElse(null), tableState.ref())) {
            // The source table is now read from a different branch than the last refresh recorded;
            // an incremental scan anchored on that recorded snapshot would not be on the same lineage.
            return OptionalLong.empty();
        }
        return OptionalLong.of(tableState.snapshotId());
    }

    private SchemaTableName toSchemaTableName(TableIdentifier identifier)
    {
        return SchemaTableName.schemaTableName(toSchemaName(identifier.namespace()), identifier.name());
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
                String sessionId = format("%s-%s-%s", session.getUser(), session.getQueryId(), session.getSource().orElse("default"));

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
        return toRemoteTableIfExists(session, tableIdentifier, getCached)
                .orElseGet(() -> toRemoteIdentifier(session, tableIdentifier));
    }

    private Optional<TableIdentifier> toRemoteTableIfExists(ConnectorSession session, TableIdentifier tableIdentifier, boolean getCached)
    {
        return toRemoteObject(tableIdentifier, () -> findRemoteTable(session, tableIdentifier), getCached);
    }

    private TableIdentifier findRemoteTable(ConnectorSession session, TableIdentifier tableIdentifier)
    {
        Namespace remoteNamespace = toRemoteNamespace(session, tableIdentifier.namespace());
        List<TableIdentifier> tableIdentifiers;
        try {
            tableIdentifiers = restSessionCatalog.listTables(convert(session), remoteNamespace);
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list tables", e);
        }
        TableIdentifier matchingTable = null;
        for (TableIdentifier identifier : tableIdentifiers) {
            if (identifier.name().equalsIgnoreCase(tableIdentifier.name())) {
                if (matchingTable != null) {
                    throw new TrinoException(NOT_SUPPORTED, "Duplicate table names are not supported with Iceberg REST catalog: "
                            + matchingTable + ", " + identifier.name());
                }
                matchingTable = identifier;
            }
        }
        if (matchingTable == null) {
            throw new RemoteObjectNotFoundException();
        }
        return matchingTable;
    }

    private TableIdentifier toRemoteView(ConnectorSession session, SchemaTableName schemaViewName, boolean getCached)
    {
        TableIdentifier tableIdentifier = toIdentifier(schemaViewName);
        return toRemoteViewIfExists(session, tableIdentifier, getCached)
                .orElseGet(() -> toRemoteIdentifier(session, tableIdentifier));
    }

    private Optional<TableIdentifier> toRemoteViewIfExists(ConnectorSession session, TableIdentifier tableIdentifier, boolean getCached)
    {
        if (!viewEndpointsEnabled) {
            return Optional.empty();
        }
        return toRemoteObject(tableIdentifier, () -> findRemoteView(session, tableIdentifier), getCached);
    }

    private TableIdentifier findRemoteView(ConnectorSession session, TableIdentifier tableIdentifier)
    {
        Namespace remoteNamespace = toRemoteNamespace(session, tableIdentifier.namespace());
        List<TableIdentifier> tableIdentifiers;
        try {
            tableIdentifiers = restSessionCatalog.listViews(convert(session), remoteNamespace);
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list views", e);
        }
        TableIdentifier matchingView = null;
        for (TableIdentifier identifier : tableIdentifiers) {
            if (identifier.name().equalsIgnoreCase(tableIdentifier.name())) {
                if (matchingView != null) {
                    throw new TrinoException(NOT_SUPPORTED, "Duplicate view names are not supported with Iceberg REST catalog: "
                            + String.join(", ", matchingView.name(), identifier.name()));
                }
                matchingView = identifier;
            }
        }
        if (matchingView == null) {
            throw new RemoteObjectNotFoundException();
        }
        return matchingView;
    }

    private Optional<TableIdentifier> toRemoteObject(TableIdentifier tableIdentifier, Supplier<TableIdentifier> remoteObjectProvider, boolean getCached)
    {
        if (caseInsensitiveNameMatching) {
            try {
                if (getCached) {
                    return Optional.of(getAndCache(tableIdentifier, remoteObjectProvider));
                }
                return Optional.of(remoteObjectProvider.get());
            }
            catch (RemoteObjectNotFoundException e) {
                return Optional.empty();
            }
        }
        return Optional.of(tableIdentifier);
    }

    private TableIdentifier getAndCache(TableIdentifier tableIdentifier, Supplier<TableIdentifier> remoteObjectProvider)
    {
        try {
            return uncheckedCacheGet(remoteTableMappingCache, tableIdentifier, remoteObjectProvider);
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw e;
        }
    }

    private TableIdentifier toRemoteIdentifier(ConnectorSession session, TableIdentifier tableIdentifier)
    {
        return TableIdentifier.of(toRemoteNamespace(session, tableIdentifier.namespace()), tableIdentifier.name());
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
        return listNamespaces(convert(session), parentNamespace);
    }

    private List<Namespace> listNamespaceIfExists(SessionContext sessionContext, Namespace namespace)
    {
        try {
            return Stream.concat(Stream.of(namespace), listNamespaces(sessionContext, namespace).stream()).toList();
        }
        catch (NoSuchNamespaceException e) {
            return ImmutableList.of();
        }
    }

    private List<Namespace> listNamespaces(SessionContext sessionContext, Namespace parentNamespace)
    {
        List<Namespace> childNamespaces;
        try {
            childNamespaces = restSessionCatalog.listNamespaces(sessionContext, parentNamespace);
        }
        catch (RESTException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to list namespaces", e);
        }
        return childNamespaces.stream().flatMap(childNamespace -> listNamespaceIfExists(sessionContext, childNamespace).stream()).toList();
    }

    private static Namespace toTrinoNamespace(Namespace namespace)
    {
        return Namespace.of(Arrays.stream(namespace.levels()).map(level -> level.toLowerCase(ENGLISH)).toArray(String[]::new));
    }

    private static class RemoteObjectNotFoundException
            extends RuntimeException
    {
        public RemoteObjectNotFoundException()
        {
            // This exception is a sentinel used only to signal a cache miss to the enclosing catch;
            // it never escapes and is never logged, so the stack trace is pointless overhead and is suppressed.
            super(null, null, false, false);
        }
    }
}
