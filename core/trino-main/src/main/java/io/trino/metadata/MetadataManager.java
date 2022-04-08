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
package io.trino.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.connector.CatalogName;
import io.trino.metadata.Catalog.SecurityManagement;
import io.trino.metadata.ResolvedFunction.ResolvedFunctionDecoder;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorResolvedIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.ConnectorExpressions;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.tree.QualifiedName;
import io.trino.transaction.TransactionManager;
import io.trino.type.BlockTypeOperators;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.client.NodeVersion.UNKNOWN;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.trino.metadata.RedirectionAwareTableHandle.noRedirection;
import static io.trino.metadata.RedirectionAwareTableHandle.withRedirectionTo;
import static io.trino.metadata.Signature.mangleOperatorName;
import static io.trino.metadata.SignatureBinder.applyBoundVariables;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class MetadataManager
        implements Metadata
{
    @VisibleForTesting
    public static final int MAX_TABLE_REDIRECTIONS = 10;

    private final GlobalFunctionCatalog functions;
    private final FunctionResolver functionResolver;
    private final SystemSecurityMetadata systemSecurityMetadata;
    private final TransactionManager transactionManager;
    private final TypeManager typeManager;

    private final ConcurrentMap<QueryId, QueryCatalogs> catalogsByQueryId = new ConcurrentHashMap<>();

    private final ResolvedFunctionDecoder functionDecoder;

    private final NonEvictableCache<OperatorCacheKey, ResolvedFunction> operatorCache;
    private final NonEvictableCache<CoercionCacheKey, ResolvedFunction> coercionCache;

    @Inject
    public MetadataManager(
            FeaturesConfig featuresConfig,
            SystemSecurityMetadata systemSecurityMetadata,
            TransactionManager transactionManager,
            GlobalFunctionCatalog globalFunctionCatalog,
            TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        functions = requireNonNull(globalFunctionCatalog, "globalFunctionCatalog is null");
        functionResolver = new FunctionResolver(this, typeManager);

        this.systemSecurityMetadata = requireNonNull(systemSecurityMetadata, "systemSecurityMetadata is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        functionDecoder = new ResolvedFunctionDecoder(typeManager::getType);

        operatorCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
        coercionCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogName catalogName)
    {
        return getCatalogMetadata(session, catalogName).getConnectorCapabilities();
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        return getOptionalCatalogMetadata(session, catalogName).isPresent();
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, schema.getCatalogName());
        if (catalog.isEmpty()) {
            return false;
        }
        CatalogMetadata catalogMetadata = catalog.get();
        ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
        return catalogMetadata.listConnectorIds().stream()
                .map(catalogName -> catalogMetadata.getMetadataFor(session, catalogName))
                .anyMatch(metadata -> metadata.schemaExists(connectorSession, schema.getSchemaName()));
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, catalogName);

        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
            for (CatalogName connectorId : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, connectorId);
                metadata.listSchemaNames(connectorSession).stream()
                        .map(schema -> schema.toLowerCase(Locale.ENGLISH))
                        .forEach(schemaNames::add);
            }
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table)
    {
        return getTableHandle(session, table, Optional.empty(), Optional.empty());
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        requireNonNull(table, "table is null");

        if (table.getCatalogName().isEmpty() || table.getSchemaName().isEmpty() || table.getObjectName().isEmpty()) {
            // Table cannot exist
            return Optional.empty();
        }

        return getOptionalCatalogMetadata(session, table.getCatalogName()).flatMap(catalogMetadata -> {
            CatalogName catalogName = catalogMetadata.getConnectorId(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);

            // GetTableHandle with the optional version handle field will throw an error if it is not implemented, so only try calling it when we have a version
            if (startVersion.isPresent() || endVersion.isPresent()) {
                ConnectorTableHandle versionedTableHandle = metadata.getTableHandle(
                        connectorSession,
                        table.asSchemaTableName(),
                        toConnectorVersion(startVersion),
                        toConnectorVersion(endVersion));
                return Optional.ofNullable(versionedTableHandle)
                        .map(connectorTableHandle -> new TableHandle(
                                catalogName,
                                connectorTableHandle,
                                catalogMetadata.getTransactionHandleFor(catalogName)));
            }

            return Optional.ofNullable(metadata.getTableHandle(connectorSession, table.asSchemaTableName()))
                    .map(connectorTableHandle -> new TableHandle(
                            catalogName,
                            connectorTableHandle,
                            catalogMetadata.getTransactionHandleFor(catalogName)));
        });
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName table, Map<String, Object> analyzeProperties)
    {
        requireNonNull(table, "table is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, table.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

            ConnectorTableHandle tableHandle = metadata.getTableHandleForStatisticsCollection(session.toConnectorSession(catalogName), table.asSchemaTableName(), analyzeProperties);
            if (tableHandle != null) {
                return Optional.of(new TableHandle(
                        catalogName,
                        tableHandle,
                        catalogMetadata.getTransactionHandleFor(catalogName)));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableExecuteHandle> getTableHandleForExecute(Session session, TableHandle tableHandle, String procedure, Map<String, Object> executeProperties)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(procedure, "procedure is null");
        requireNonNull(executeProperties, "executeProperties is null");

        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

        Optional<ConnectorTableExecuteHandle> executeHandle = metadata.getTableHandleForExecute(
                session.toConnectorSession(catalogName),
                tableHandle.getConnectorHandle(),
                procedure,
                executeProperties,
                getRetryPolicy(session).getRetryMode());

        return executeHandle.map(handle -> new TableExecuteHandle(
                catalogName,
                tableHandle.getTransaction(),
                handle));
    }

    @Override
    public Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        CatalogName catalogName = tableExecuteHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        return metadata.getLayoutForTableExecute(session.toConnectorSession(catalogName), tableExecuteHandle.getConnectorHandle())
                .map(layout -> new TableLayout(catalogName, catalogMetadata.getTransactionHandleFor(catalogName), layout));
    }

    @Override
    public BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle tableExecuteHandle, TableHandle sourceHandle)
    {
        CatalogName catalogName = tableExecuteHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> connectorBeginResult = metadata.beginTableExecute(session.toConnectorSession(), tableExecuteHandle.getConnectorHandle(), sourceHandle.getConnectorHandle());

        return new BeginTableExecuteResult<>(
                tableExecuteHandle.withConnectorHandle(connectorBeginResult.getTableExecuteHandle()),
                sourceHandle.withConnectorHandle(connectorBeginResult.getSourceHandle()));
    }

    @Override
    public void finishTableExecute(Session session, TableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        CatalogName catalogName = tableExecuteHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        metadata.finishTableExecute(session.toConnectorSession(catalogName), tableExecuteHandle.getConnectorHandle(), fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        CatalogName catalogName = tableExecuteHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        metadata.executeTableExecute(session.toConnectorSession(catalogName), tableExecuteHandle.getConnectorHandle());
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, tableName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            // we query only main connector for runtime system tables
            CatalogName catalogName = catalogMetadata.getCatalogName();
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

            return metadata.getSystemTable(session.toConnectorSession(catalogName), tableName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        CatalogName catalogName = handle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        return new TableProperties(catalogName, handle.getTransaction(), metadata.getTableProperties(connectorSession, handle.getConnectorHandle()));
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        checkArgument(partitioningHandle.getConnectorId().isPresent(), "Expect partitioning handle from connector, got system partitioning handle");
        CatalogName catalogName = partitioningHandle.getConnectorId().get();
        checkArgument(catalogName.equals(tableHandle.getCatalogName()), "ConnectorId of tableHandle and partitioningHandle does not match");
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);

        ConnectorTableHandle newTableHandle = metadata.makeCompatiblePartitioning(
                session.toConnectorSession(catalogName),
                tableHandle.getConnectorHandle(),
                partitioningHandle.getConnectorHandle());
        return new TableHandle(catalogName, newTableHandle, transaction);
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        Optional<CatalogName> leftConnectorId = left.getConnectorId();
        Optional<CatalogName> rightConnectorId = right.getConnectorId();
        if (leftConnectorId.isEmpty() || rightConnectorId.isEmpty() || !leftConnectorId.equals(rightConnectorId)) {
            return Optional.empty();
        }
        if (!left.getTransactionHandle().equals(right.getTransactionHandle())) {
            return Optional.empty();
        }
        CatalogName catalogName = leftConnectorId.get();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
        Optional<ConnectorPartitioningHandle> commonHandle = metadata.getCommonPartitioningHandle(session.toConnectorSession(catalogName), left.getConnectorHandle(), right.getConnectorHandle());
        return commonHandle.map(handle -> new PartitioningHandle(Optional.of(catalogName), left.getTransactionHandle(), handle));
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        CatalogName catalogName = handle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        return metadata.getInfo(handle.getConnectorHandle());
    }

    @Override
    public TableSchema getTableSchema(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        ConnectorTableSchema tableSchema = metadata.getTableSchema(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());

        return new TableSchema(catalogName, tableSchema);
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());

        return new TableMetadata(catalogName, tableMetadata);
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        TableStatistics tableStatistics = metadata.getTableStatistics(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        verifyNotNull(tableStatistics, "%s returned null tableStatistics for %s", metadata, tableHandle);
        return tableStatistics;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());

        ImmutableMap.Builder<String, ColumnHandle> map = ImmutableMap.builder();
        for (Entry<String, ColumnHandle> mapEntry : handles.entrySet()) {
            map.put(mapEntry.getKey().toLowerCase(ENGLISH), mapEntry.getValue());
        }
        return map.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnHandle, "columnHandle is null");

        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.getColumnMetadata(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), columnHandle);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<QualifiedObjectName> objectName = prefix.asQualifiedObjectName();
        if (objectName.isPresent()) {
            Optional<Boolean> exists = isExistingRelationForListing(session, objectName.get());
            if (exists.isPresent()) {
                return exists.get() ? ImmutableList.of(objectName.get()) : ImmutableList.of();
            }
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());
        Set<QualifiedObjectName> tables = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                metadata.listTables(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(prefix::matches)
                        .forEach(tables::add);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    private Optional<Boolean> isExistingRelationForListing(Session session, QualifiedObjectName name)
    {
        if (isMaterializedView(session, name)) {
            return Optional.of(true);
        }
        if (isView(session, name)) {
            return Optional.of(true);
        }

        // TODO: consider a better way to resolve relation names: https://github.com/trinodb/trino/issues/9400
        try {
            return Optional.of(getRedirectionAwareTableHandle(session, name).getTableHandle().isPresent());
        }
        catch (TrinoException e) {
            // ignore redirection errors for consistency with listing
            if (e.getErrorCode().equals(TABLE_REDIRECTION_ERROR.toErrorCode())) {
                return Optional.of(true);
            }
            // we don't know if it exists or not
            return Optional.empty();
        }
    }

    @Override
    public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        // Track column metadata for every object name to resolve ties between table and view
        Map<SchemaTableName, Optional<List<ColumnMetadata>>> tableColumns = new HashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

                ConnectorSession connectorSession = session.toConnectorSession(catalogName);

                // Collect column metadata from tables
                metadata.streamTableColumns(connectorSession, tablePrefix)
                        .forEach(columnsMetadata -> tableColumns.put(columnsMetadata.getTable(), columnsMetadata.getColumns()));

                // Collect column metadata from views. if table and view names overlap, the view wins
                for (Entry<QualifiedObjectName, ViewInfo> entry : getViews(session, prefix).entrySet()) {
                    ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                    for (ViewColumn column : entry.getValue().getColumns()) {
                        try {
                            columns.add(new ColumnMetadata(column.getName(), typeManager.getType(column.getType())));
                        }
                        catch (TypeNotFoundException e) {
                            throw new TrinoException(INVALID_VIEW, format("Unknown type '%s' for column '%s' in view: %s", column.getType(), column.getName(), entry.getKey()));
                        }
                    }
                    tableColumns.put(entry.getKey().asSchemaTableName(), Optional.of(columns.build()));
                }

                // if view and materialized view names overlap, the materialized view wins
                for (Entry<QualifiedObjectName, ViewInfo> entry : getMaterializedViews(session, prefix).entrySet()) {
                    ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                    for (ViewColumn column : entry.getValue().getColumns()) {
                        try {
                            columns.add(new ColumnMetadata(column.getName(), typeManager.getType(column.getType())));
                        }
                        catch (TypeNotFoundException e) {
                            throw new TrinoException(INVALID_VIEW, format("Unknown type '%s' for column '%s' in materialized view: %s", column.getType(), column.getName(), entry.getKey()));
                        }
                    }
                    tableColumns.put(entry.getKey().asSchemaTableName(), Optional.of(columns.build()));
                }
            }
        }
        return tableColumns.entrySet().stream()
                .map(entry -> new TableColumnsMetadata(entry.getKey(), entry.getValue()))
                .collect(toImmutableList());
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.createSchema(session.toConnectorSession(catalogName), schema.getSchemaName(), properties, principal);
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.schemaCreated(session, schema);
        }
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.dropSchema(session.toConnectorSession(catalogName), schema.getSchemaName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.schemaDropped(session, schema);
        }
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.renameSchema(session.toConnectorSession(catalogName), source.getSchemaName(), target);
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.schemaRenamed(session, source, new CatalogSchemaName(source.getCatalogName(), target));
        }
    }

    @Override
    public void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.setSchemaOwner(session, source, principal);
        }
        else {
            metadata.setSchemaAuthorization(session.toConnectorSession(catalogName), source.getSchemaName(), principal);
        }
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.createTable(session.toConnectorSession(catalog), tableMetadata, ignoreExisting);
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableCreated(session, new CatalogSchemaTableName(catalogName, tableMetadata.getTable()));
        }
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        String catalogName = newTableName.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        if (!tableHandle.getCatalogName().equals(catalog)) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }
        Optional<CatalogSchemaTableName> sourceTableName = getTableNameIfSystemSecurity(session, catalogMetadata, tableHandle);

        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.renameTable(session.toConnectorSession(catalog), tableHandle.getConnectorHandle(), newTableName.asSchemaTableName());
        sourceTableName.ifPresent(name -> systemSecurityMetadata.tableRenamed(session, name, newTableName.asCatalogSchemaTableName()));
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.setTableProperties(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), properties);
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.setTableComment(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), comment);
    }

    @Override
    public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.setColumnComment(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), column, comment);
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.renameColumn(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), source, target.toLowerCase(ENGLISH));
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.addColumn(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), column);
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.dropColumn(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), column);
    }

    @Override
    public void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        CatalogName catalogName = new CatalogName(table.getCatalogName());
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.setTableOwner(session, table, principal);
        }
        else {
            metadata.setTableAuthorization(session.toConnectorSession(catalogName), table.getSchemaTableName(), principal);
        }
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        Optional<CatalogSchemaTableName> tableName = getTableNameIfSystemSecurity(session, catalogMetadata, tableHandle);
        metadata.dropTable(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        tableName.ifPresent(name -> systemSecurityMetadata.tableDropped(session, name));
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.truncateTable(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
    }

    @Override
    public Optional<TableLayout> getInsertLayout(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        return metadata.getInsertLayout(session.toConnectorSession(catalogName), table.getConnectorHandle())
                .map(layout -> new TableLayout(catalogName, catalogMetadata.getTransactionHandleFor(catalogName), layout));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        CatalogName catalog = catalogMetadata.getCatalogName();
        return metadata.getStatisticsCollectionMetadataForWrite(session.toConnectorSession(catalog), tableMetadata);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        CatalogName catalog = catalogMetadata.getCatalogName();
        return metadata.getStatisticsCollectionMetadata(session.toConnectorSession(catalog), tableMetadata);
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorTableHandle connectorTableHandle = metadata.beginStatisticsCollection(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return new AnalyzeTableHandle(catalogName, transactionHandle, connectorTableHandle);
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        catalogMetadata.getMetadata(session).finishStatisticsCollection(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), computedStatistics);
    }

    @Override
    public Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalog);
        ConnectorSession connectorSession = session.toConnectorSession(catalog);
        return metadata.getNewTableLayout(connectorSession, tableMetadata)
                .map(layout -> new TableLayout(catalog, transactionHandle, layout));
    }

    @Override
    public void cleanupQuery(Session session)
    {
        QueryCatalogs queryCatalogs = catalogsByQueryId.remove(session.getQueryId());
        if (queryCatalogs != null) {
            queryCatalogs.finish();
        }
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalog);
        ConnectorSession connectorSession = session.toConnectorSession(catalog);
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata, layout.map(TableLayout::getLayout), getRetryPolicy(session).getRetryMode());
        // TODO this should happen after finish but there is no way to get table name in finish step
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableCreated(session, new CatalogSchemaTableName(catalogName, tableMetadata.getTable()));
        }
        return new OutputTableHandle(catalog, transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.finishCreateTable(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorInsertTableHandle handle = metadata.beginInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), columns, getRetryPolicy(session).getRetryMode());
        return new InsertTableHandle(tableHandle.getCatalogName(), transactionHandle, handle);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        return catalogMetadata.getMetadata(session).supportsMissingColumnsOnInsert();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.finishInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName)
    {
        CatalogName catalogName = new CatalogName(viewName.getCatalogName());
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.delegateMaterializedViewRefreshToConnector(session.toConnectorSession(catalogName), viewName.asSchemaTableName());
    }

    @Override
    public ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CatalogName catalogName = new CatalogName(viewName.getCatalogName());
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return asVoid(toListenableFuture(metadata.refreshMaterializedView(session.toConnectorSession(catalogName), viewName.asSchemaTableName())));
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);

        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .map(TableHandle::getConnectorHandle)
                .collect(Collectors.toList());
        sourceConnectorHandles.add(tableHandle.getConnectorHandle());

        if (sourceConnectorHandles.stream()
                .map(Object::getClass)
                .distinct()
                .count() > 1) {
            throw new TrinoException(NOT_SUPPORTED, "Cross connector materialized views are not supported");
        }

        ConnectorInsertTableHandle handle = metadata.beginRefreshMaterializedView(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), sourceConnectorHandles, getRetryPolicy(session).getRetryMode());

        return new InsertTableHandle(tableHandle.getCatalogName(), transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            Session session,
            TableHandle tableHandle,
            InsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<TableHandle> sourceTableHandles)
    {
        CatalogName catalogName = insertHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .map(TableHandle::getConnectorHandle)
                .collect(toImmutableList());
        return metadata.finishRefreshMaterializedView(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), insertHandle.getConnectorHandle(),
                fragments, computedStatistics, sourceConnectorHandles);
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.getDeleteRowIdColumnHandle(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        return metadata.getUpdateRowIdColumnHandle(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), updatedColumns);
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyDelete(connectorSession, table.getConnectorHandle())
                .map(newHandle -> new TableHandle(catalogName, newHandle, table.getTransaction()));
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        return metadata.executeDelete(connectorSession, table.getConnectorHandle());
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorTableHandle newHandle = metadata.beginDelete(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), getRetryPolicy(session).getRetryMode());
        return new TableHandle(tableHandle.getCatalogName(), newHandle, tableHandle.getTransaction());
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        metadata.finishDelete(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public TableHandle beginUpdate(Session session, TableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorTableHandle newHandle = metadata.beginUpdate(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), updatedColumns, getRetryPolicy(session).getRetryMode());
        return new TableHandle(tableHandle.getCatalogName(), newHandle, tableHandle.getTransaction());
    }

    @Override
    public void finishUpdate(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        metadata.finishUpdate(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName).map(CatalogMetadata::getCatalogName);
    }

    @Override
    public Map<String, Catalog> getCatalogs(Session session)
    {
        return transactionManager.getCatalogs(session.getRequiredTransactionId());
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<QualifiedObjectName> objectName = prefix.asQualifiedObjectName();
        if (objectName.isPresent()) {
            return getView(session, objectName.get())
                    .map(handle -> ImmutableList.of(objectName.get()))
                    .orElseGet(ImmutableList::of);
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Set<QualifiedObjectName> views = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                metadata.listViews(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(prefix::matches)
                        .forEach(views::add);
            }
        }
        return ImmutableList.copyOf(views);
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Map<QualifiedObjectName, ViewInfo> views = new LinkedHashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);

                Map<SchemaTableName, ConnectorViewDefinition> viewMap;
                if (tablePrefix.getTable().isPresent()) {
                    viewMap = metadata.getView(connectorSession, tablePrefix.toSchemaTableName())
                            .map(view -> ImmutableMap.of(tablePrefix.toSchemaTableName(), view))
                            .orElse(ImmutableMap.of());
                }
                else {
                    viewMap = metadata.getViews(connectorSession, tablePrefix.getSchema());
                }

                for (Entry<SchemaTableName, ConnectorViewDefinition> entry : viewMap.entrySet()) {
                    QualifiedObjectName viewName = new QualifiedObjectName(
                            prefix.getCatalogName(),
                            entry.getKey().getSchemaName(),
                            entry.getKey().getTableName());
                    views.put(viewName, new ViewInfo(entry.getValue()));
                }
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName)
    {
        if (!schemaExists(session, schemaName)) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, new CatalogName(schemaName.getCatalogName()));
        CatalogName catalogName = catalogMetadata.getConnectorIdForSchema(schemaName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.getSchemaProperties(connectorSession, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schemaName)
    {
        if (!schemaExists(session, schemaName)) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, new CatalogName(schemaName.getCatalogName()));
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            return systemSecurityMetadata.getSchemaOwner(session, schemaName);
        }
        CatalogName catalogName = catalogMetadata.getConnectorIdForSchema(schemaName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.getSchemaOwner(connectorSession, schemaName);
    }

    @Override
    public boolean isView(Session session, QualifiedObjectName viewName)
    {
        return getViewInternal(session, viewName).isPresent();
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        Optional<ConnectorViewDefinition> connectorView = getViewInternal(session, viewName);
        if (connectorView.isEmpty() || connectorView.get().isRunAsInvoker() || isCatalogManagedSecurity(session, viewName.getCatalogName())) {
            return connectorView.map(view -> new ViewDefinition(viewName, view));
        }

        Identity runAsIdentity = systemSecurityMetadata.getViewRunAsIdentity(session, viewName.asCatalogSchemaTableName())
                .or(() -> connectorView.get().getOwner().map(Identity::ofUser))
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Catalog does not support run-as DEFINER views: " + viewName));
        return Optional.of(new ViewDefinition(viewName, connectorView.get(), runAsIdentity));
    }

    private Optional<ConnectorViewDefinition> getViewInternal(Session session, QualifiedObjectName viewName)
    {
        if (viewName.getCatalogName().isEmpty() || viewName.getSchemaName().isEmpty() || viewName.getObjectName().isEmpty()) {
            // View cannot exist
            return Optional.empty();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);
            return metadata.getView(connectorSession, viewName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, boolean replace)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.createView(session.toConnectorSession(catalogName), viewName.asSchemaTableName(), definition.toConnectorViewDefinition(), replace);
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableCreated(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void renameView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, target.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (!source.getCatalogName().equals(catalogName.getCatalogName())) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename views across catalogs");
        }

        metadata.renameView(session.toConnectorSession(catalogName), source.asSchemaTableName(), target.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableRenamed(session, source.asCatalogSchemaTableName(), target.asCatalogSchemaTableName());
        }
    }

    @Override
    public void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, view.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.setViewOwner(session, view, principal);
        }
        else {
            metadata.setViewAuthorization(session.toConnectorSession(catalogName), view.getSchemaTableName(), principal);
        }
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.dropView(session.toConnectorSession(catalogName), viewName.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableDropped(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void createMaterializedView(Session session, QualifiedObjectName viewName, MaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.createMaterializedView(
                session.toConnectorSession(catalogName),
                viewName.asSchemaTableName(),
                definition.toConnectorMaterializedViewDefinition(),
                replace,
                ignoreExisting);
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableCreated(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.dropMaterializedView(session.toConnectorSession(catalogName), viewName.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableDropped(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<QualifiedObjectName> objectName = prefix.asQualifiedObjectName();
        if (objectName.isPresent()) {
            return isMaterializedView(session, objectName.get()) ? ImmutableList.of(objectName.get()) : ImmutableList.of();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Set<QualifiedObjectName> materializedViews = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                metadata.listMaterializedViews(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(prefix::matches)
                        .forEach(materializedViews::add);
            }
        }
        return ImmutableList.copyOf(materializedViews);
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Map<QualifiedObjectName, ViewInfo> views = new LinkedHashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);

                Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViewMap;
                if (tablePrefix.getTable().isPresent()) {
                    materializedViewMap = metadata.getMaterializedView(connectorSession, tablePrefix.toSchemaTableName())
                            .map(view -> ImmutableMap.of(tablePrefix.toSchemaTableName(), view))
                            .orElse(ImmutableMap.of());
                }
                else {
                    materializedViewMap = metadata.getMaterializedViews(connectorSession, tablePrefix.getSchema());
                }

                for (Entry<SchemaTableName, ConnectorMaterializedViewDefinition> entry : materializedViewMap.entrySet()) {
                    QualifiedObjectName viewName = new QualifiedObjectName(
                            prefix.getCatalogName(),
                            entry.getKey().getSchemaName(),
                            entry.getKey().getTableName());
                    views.put(viewName, new ViewInfo(entry.getValue()));
                }
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public boolean isMaterializedView(Session session, QualifiedObjectName viewName)
    {
        return getMaterializedViewInternal(session, viewName).isPresent();
    }

    @Override
    public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        Optional<ConnectorMaterializedViewDefinition> connectorView = getMaterializedViewInternal(session, viewName);
        if (connectorView.isEmpty() || isCatalogManagedSecurity(session, viewName.getCatalogName())) {
            return connectorView.map(view -> {
                String runAsUser = view.getOwner().orElseThrow(() -> new TrinoException(INVALID_VIEW, "Owner not set for a run-as invoker view: " + viewName));
                return new MaterializedViewDefinition(view, Identity.ofUser(runAsUser));
            });
        }

        Identity runAsIdentity = systemSecurityMetadata.getViewRunAsIdentity(session, viewName.asCatalogSchemaTableName())
                .or(() -> connectorView.get().getOwner().map(Identity::ofUser))
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Materialized view does not have an owner: " + viewName));
        return Optional.of(new MaterializedViewDefinition(connectorView.get(), runAsIdentity));
    }

    private Optional<ConnectorMaterializedViewDefinition> getMaterializedViewInternal(Session session, QualifiedObjectName viewName)
    {
        if (viewName.getCatalogName().isEmpty() || viewName.getSchemaName().isEmpty() || viewName.getObjectName().isEmpty()) {
            // View cannot exist
            return Optional.empty();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);
            return metadata.getMaterializedView(connectorSession, viewName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName viewName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);
            return metadata.getMaterializedViewFreshness(connectorSession, viewName.asSchemaTableName());
        }
        return new MaterializedViewFreshness(false);
    }

    @Override
    public void renameMaterializedView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, target.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (!source.getCatalogName().equals(catalogName.getCatalogName())) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename materialized views across catalogs");
        }

        metadata.renameMaterializedView(session.toConnectorSession(catalogName), source.asSchemaTableName(), target.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.tableRenamed(session, source.asCatalogSchemaTableName(), target.asCatalogSchemaTableName());
        }
    }

    @Override
    public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.setMaterializedViewProperties(session.toConnectorSession(catalogName), viewName.asSchemaTableName(), properties);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyTableScanRedirect(connectorSession, tableHandle.getConnectorHandle());
    }

    private QualifiedObjectName getRedirectedTableName(Session session, QualifiedObjectName originalTableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(originalTableName, "originalTableName is null");

        if (originalTableName.getCatalogName().isEmpty() || originalTableName.getSchemaName().isEmpty() || originalTableName.getObjectName().isEmpty()) {
            // table cannot exist
            return originalTableName;
        }

        QualifiedObjectName tableName = originalTableName;
        Set<QualifiedObjectName> visitedTableNames = new LinkedHashSet<>();
        visitedTableNames.add(tableName);

        for (int count = 0; count < MAX_TABLE_REDIRECTIONS; count++) {
            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, tableName.getCatalogName());

            if (catalog.isEmpty()) {
                // Stop redirection
                return tableName;
            }

            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, tableName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);

            Optional<QualifiedObjectName> redirectedTableName = metadata.redirectTable(session.toConnectorSession(catalogName), tableName.asSchemaTableName())
                    .map(name -> convertFromSchemaTableName(name.getCatalogName()).apply(name.getSchemaTableName()));

            if (redirectedTableName.isEmpty()) {
                return tableName;
            }

            tableName = redirectedTableName.get();

            // Check for loop in redirection
            if (!visitedTableNames.add(tableName)) {
                throw new TrinoException(TABLE_REDIRECTION_ERROR,
                        format("Table redirections form a loop: %s",
                                Streams.concat(visitedTableNames.stream(), Stream.of(tableName))
                                        .map(QualifiedObjectName::toString)
                                        .collect(Collectors.joining(" -> "))));
            }
        }
        throw new TrinoException(TABLE_REDIRECTION_ERROR, format("Table redirected too many times (%d): %s", MAX_TABLE_REDIRECTIONS, visitedTableNames));
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName)
    {
        return getRedirectionAwareTableHandle(session, tableName, Optional.empty(), Optional.empty());
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        QualifiedObjectName targetTableName = getRedirectedTableName(session, tableName);
        if (targetTableName.equals(tableName)) {
            return noRedirection(getTableHandle(session, tableName, startVersion, endVersion));
        }

        Optional<TableHandle> tableHandle = getTableHandle(session, targetTableName, startVersion, endVersion);
        if (tableHandle.isPresent()) {
            return withRedirectionTo(targetTableName, tableHandle.get());
        }

        // Redirected table must exist
        if (getCatalogHandle(session, targetTableName.getCatalogName()).isEmpty()) {
            throw new TrinoException(TABLE_REDIRECTION_ERROR, format("Table '%s' redirected to '%s', but the target catalog '%s' does not exist", tableName, targetTableName, targetTableName.getCatalogName()));
        }
        if (!schemaExists(session, new CatalogSchemaName(targetTableName.getCatalogName(), targetTableName.getSchemaName()))) {
            throw new TrinoException(TABLE_REDIRECTION_ERROR, format("Table '%s' redirected to '%s', but the target schema '%s' does not exist", tableName, targetTableName, targetTableName.getSchemaName()));
        }
        throw new TrinoException(TABLE_REDIRECTION_ERROR, format("Table '%s' redirected to '%s', but the target table '%s' does not exist", tableName, targetTableName, targetTableName));
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.getConnectorHandle(), indexableColumns, outputColumns, tupleDomain);
        return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.getCatalogName(), transaction, resolved));
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyLimit(connectorSession, table.getConnectorHandle(), limit)
                .map(result -> new LimitApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction()),
                        result.isLimitGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applySample(connectorSession, table.getConnectorHandle(), sampleType, sampleRatio)
                .map(result -> new SampleApplicationResult<>(new TableHandle(
                        catalogName,
                        result.getHandle(),
                        table.getTransaction()),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<AggregationApplicationResult<TableHandle>> applyAggregation(
            Session session,
            TableHandle table,
            List<AggregateFunction> aggregations,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        // Global aggregation is represented by [[]]
        checkArgument(!groupingSets.isEmpty(), "No grouping sets provided");

        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyAggregation(connectorSession, table.getConnectorHandle(), aggregations, assignments, groupingSets)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), aggregations.size());

                    return new AggregationApplicationResult<>(
                            new TableHandle(catalogName, result.getHandle(), table.getTransaction()),
                            result.getProjections(),
                            result.getAssignments(),
                            result.getGroupingColumnMapping(),
                            result.isPrecalculateStatistics());
                });
    }

    @Override
    public Optional<JoinApplicationResult<TableHandle>> applyJoin(
            Session session,
            JoinType joinType,
            TableHandle left,
            TableHandle right,
            List<JoinCondition> joinConditions,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics)
    {
        if (!right.getCatalogName().equals(left.getCatalogName())) {
            // Exact comparison is fine as catalog name here is passed from CatalogMetadata and is normalized to lowercase
            return Optional.empty();
        }
        CatalogName catalogName = left.getCatalogName();

        ConnectorTransactionHandle transaction = left.getTransaction();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        Optional<JoinApplicationResult<ConnectorTableHandle>> connectorResult =
                metadata.applyJoin(
                        connectorSession,
                        joinType,
                        left.getConnectorHandle(),
                        right.getConnectorHandle(),
                        joinConditions,
                        leftAssignments,
                        rightAssignments,
                        statistics);

        return connectorResult.map(result -> {
            Set<ColumnHandle> leftColumnHandles = ImmutableSet.copyOf(getColumnHandles(session, left).values());
            Set<ColumnHandle> rightColumnHandles = ImmutableSet.copyOf(getColumnHandles(session, right).values());
            Set<ColumnHandle> leftColumnHandlesMappingKeys = result.getLeftColumnHandles().keySet();
            Set<ColumnHandle> rightColumnHandlesMappingKeys = result.getRightColumnHandles().keySet();

            if (leftColumnHandlesMappingKeys.size() != leftColumnHandles.size()
                    || rightColumnHandlesMappingKeys.size() != rightColumnHandles.size()
                    || !leftColumnHandlesMappingKeys.containsAll(leftColumnHandles)
                    || !rightColumnHandlesMappingKeys.containsAll(rightColumnHandles)) {
                throw new IllegalStateException(format(
                        "Column handle mappings do not match old column handles: left=%s; right=%s; newLeft=%s, newRight=%s",
                        leftColumnHandles,
                        rightColumnHandles,
                        leftColumnHandlesMappingKeys,
                        rightColumnHandlesMappingKeys));
            }

            return new JoinApplicationResult<>(
                    new TableHandle(
                            catalogName,
                            result.getTableHandle(),
                            transaction),
                    result.getLeftColumnHandles(),
                    result.getRightColumnHandles(),
                    result.isPrecalculateStatistics());
        });
    }

    @Override
    public Optional<TopNApplicationResult<TableHandle>> applyTopN(
            Session session,
            TableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyTopN(connectorSession, table.getConnectorHandle(), topNCount, sortItems, assignments)
                .map(result -> new TopNApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction()),
                        result.isTopNGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    private void verifyProjection(TableHandle table, List<ConnectorExpression> projections, List<Assignment> assignments, int expectedProjectionSize)
    {
        projections.forEach(projection -> requireNonNull(projection, "one of the projections is null"));
        assignments.forEach(assignment -> requireNonNull(assignment, "one of the assignments is null"));

        verify(
                expectedProjectionSize == projections.size(),
                "ConnectorMetadata returned invalid number of projections: %s instead of %s for %s",
                projections.size(),
                expectedProjectionSize,
                table);

        Set<String> assignedVariables = assignments.stream()
                .map(Assignment::getVariable)
                .collect(toImmutableSet());
        projections.stream()
                .flatMap(connectorExpression -> ConnectorExpressions.extractVariables(connectorExpression).stream())
                .map(Variable::getName)
                .filter(variableName -> !assignedVariables.contains(variableName))
                .findAny()
                .ifPresent(variableName -> { throw new IllegalStateException("Unbound variable: " + variableName); });
    }

    @Override
    public void validateScan(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        metadata.validateScan(session.toConnectorSession(catalogName), table.getConnectorHandle());
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyFilter(connectorSession, table.getConnectorHandle(), constraint)
                .map(result -> result.transform(handle -> new TableHandle(catalogName, handle, table.getTransaction())));
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyProjection(connectorSession, table.getConnectorHandle(), projections, assignments)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), projections.size());

                    return new ProjectionApplicationResult<>(
                            new TableHandle(catalogName, result.getHandle(), table.getTransaction()),
                            result.getProjections(),
                            result.getAssignments(),
                            result.isPrecalculateStatistics());
                });
    }

    //
    // Roles and Grants
    //

    @Override
    public boolean isCatalogManagedSecurity(Session session, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, new CatalogName(catalog));
        return catalogMetadata.getSecurityManagement() == SecurityManagement.CONNECTOR;
    }

    @Override
    public boolean roleExists(Session session, String role, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            return systemSecurityMetadata.roleExists(session, role);
        }

        CatalogMetadata catalogMetadata = getCatalogMetadata(session, new CatalogName(catalog.get()));
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return metadata.roleExists(session.toConnectorSession(catalogName), role);
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            systemSecurityMetadata.createRole(session, role, grantor);
            return;
        }

        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog.get());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.createRole(session.toConnectorSession(catalogName), role, grantor);
    }

    @Override
    public void dropRole(Session session, String role, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            systemSecurityMetadata.dropRole(session, role);
            return;
        }

        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog.get());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.dropRole(session.toConnectorSession(catalogName), role);
    }

    @Override
    public Set<String> listRoles(Session session, Optional<String> catalog)
    {
        if (catalog.isPresent()) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog.get());
            if (catalogMetadata.isEmpty()) {
                return ImmutableSet.of();
            }
            // If the connector is using system security management, we fall through to the system call
            // instead of returning nothing, so information schema role tables will work properly
            if (catalogMetadata.get().getSecurityManagement() == SecurityManagement.CONNECTOR) {
                CatalogName catalogName = catalogMetadata.get().getCatalogName();
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogName);
                return metadata.listRoles(connectorSession).stream()
                        .map(role -> role.toLowerCase(ENGLISH))
                        .collect(toImmutableSet());
            }
        }

        return systemSecurityMetadata.listRoles(session);
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(Session session, Optional<String> catalog, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        if (catalog.isPresent()) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog.get());
            if (catalogMetadata.isEmpty()) {
                return ImmutableSet.of();
            }
            // If the connector is using system security management, we fall through to the system call
            // instead of returning nothing, so information schema role tables will work properly
            if (catalogMetadata.get().getSecurityManagement() == SecurityManagement.CONNECTOR) {
                CatalogName catalogName = catalogMetadata.get().getCatalogName();
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogName);
                return metadata.listAllRoleGrants(connectorSession, roles, grantees, limit);
            }
        }

        return systemSecurityMetadata.listAllRoleGrants(session, roles, grantees, limit);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, Optional<String> catalog, TrinoPrincipal principal)
    {
        if (catalog.isPresent()) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog.get());
            if (catalogMetadata.isEmpty()) {
                return ImmutableSet.of();
            }
            // If the connector is using system security management, we fall through to the system call
            // instead of returning nothing, so information schema role tables will work properly
            if (catalogMetadata.get().getSecurityManagement() == SecurityManagement.CONNECTOR) {
                CatalogName catalogName = catalogMetadata.get().getCatalogName();
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogName);
                return metadata.listRoleGrants(connectorSession, principal);
            }
        }

        return systemSecurityMetadata.listRoleGrants(session, principal);
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            systemSecurityMetadata.grantRoles(session, roles, grantees, adminOption, grantor);
            return;
        }

        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog.get());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.grantRoles(session.toConnectorSession(catalogName), roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            systemSecurityMetadata.revokeRoles(session, roles, grantees, adminOption, grantor);
            return;
        }

        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog.get());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.revokeRoles(session.toConnectorSession(catalogName), roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal, Optional<String> catalog)
    {
        if (catalog.isPresent()) {
            Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog.get());
            if (catalogMetadata.isEmpty()) {
                return ImmutableSet.of();
            }
            // If the connector is using system security management, we fall through to the system call
            // instead of returning nothing, so information schema role tables will work properly
            if (catalogMetadata.get().getSecurityManagement() == SecurityManagement.CONNECTOR) {
                CatalogName catalogName = catalogMetadata.get().getCatalogName();
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogName);
                return ImmutableSet.copyOf(metadata.listApplicableRoles(connectorSession, principal));
            }
        }

        return systemSecurityMetadata.listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        return systemSecurityMetadata.listEnabledRoles(identity);
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (catalogMetadata.isEmpty()) {
            return ImmutableSet.of();
        }
        // If the connector is using system security management, we fall through to the system call
        // instead of returning nothing, so information schema role tables will work properly
        if (catalogMetadata.get().getSecurityManagement() == SecurityManagement.SYSTEM) {
            return systemSecurityMetadata.listEnabledRoles(session.getIdentity());
        }

        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogName);
        return ImmutableSet.copyOf(metadata.listEnabledRoles(connectorSession));
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
            return;
        }
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.grantTablePrivileges(session.toConnectorSession(catalogName), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.denyTablePrivileges(session, tableName, privileges, grantee);
            return;
        }
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.denyTablePrivileges(session.toConnectorSession(catalogName), tableName.asSchemaTableName(), privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
            return;
        }
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.revokeTablePrivileges(session.toConnectorSession(catalogName), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
            return;
        }
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.grantSchemaPrivileges(session.toConnectorSession(catalogName), schemaName.getSchemaName(), privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.denySchemaPrivileges(session, schemaName, privileges, grantee);
            return;
        }
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.denySchemaPrivileges(session.toConnectorSession(catalogName), schemaName.getSchemaName(), privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
            systemSecurityMetadata.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
            return;
        }
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.revokeSchemaPrivileges(session.toConnectorSession(catalogName), schemaName.getSchemaName(), privileges, grantee, grantOption);
    }

    // TODO support table redirection
    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        ImmutableSet.Builder<GrantInfo> grantInfos = ImmutableSet.builder();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());

            List<CatalogName> connectorIds = prefix.asQualifiedObjectName()
                    .map(qualifiedTableName -> singletonList(catalogMetadata.getConnectorId(session, qualifiedTableName)))
                    .orElseGet(catalogMetadata::listConnectorIds);
            for (CatalogName catalogName : connectorIds) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogName);
                if (catalogMetadata.getSecurityManagement() == SecurityManagement.SYSTEM) {
                    grantInfos.addAll(systemSecurityMetadata.listTablePrivileges(session, prefix));
                }
                else {
                    grantInfos.addAll(metadata.listTablePrivileges(connectorSession, prefix.asSchemaTablePrefix()));
                }
            }
        }
        return ImmutableList.copyOf(grantInfos.build());
    }

    //
    // Functions
    //

    @Override
    public Collection<FunctionMetadata> listFunctions()
    {
        return functions.listFunctions();
    }

    @Override
    public ResolvedFunction decodeFunction(QualifiedName name)
    {
        return functionDecoder.fromQualifiedName(name)
                .orElseThrow(() -> new IllegalArgumentException("Function is not resolved: " + name));
    }

    @Override
    public ResolvedFunction resolveFunction(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return resolvedFunctionInternal(session, name, parameterTypes);
    }

    @Override
    public ResolvedFunction resolveOperator(Session session, OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        try {
            // todo we should not be caching functions across session
            return uncheckedCacheGet(operatorCache, new OperatorCacheKey(operatorType, argumentTypes), () -> {
                String name = mangleOperatorName(operatorType);
                return resolvedFunctionInternal(session, QualifiedName.of(name), fromTypes(argumentTypes));
            });
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TrinoException) {
                TrinoException cause = (TrinoException) e.getCause();
                if (cause.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                    throw new OperatorNotFoundException(operatorType, argumentTypes, cause);
                }
                throw cause;
            }
            throw e;
        }
    }

    private ResolvedFunction resolvedFunctionInternal(Session session, QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return functionDecoder.fromQualifiedName(name)
                .orElseGet(() -> resolve(session, functionResolver.resolveFunction(session, functions.getFunctions(name), name, parameterTypes)));
    }

    @Override
    public ResolvedFunction getCoercion(Session session, OperatorType operatorType, Type fromType, Type toType)
    {
        checkArgument(operatorType == OperatorType.CAST || operatorType == OperatorType.SATURATED_FLOOR_CAST);
        try {
            // todo we should not be caching functions across session
            return uncheckedCacheGet(coercionCache, new CoercionCacheKey(operatorType, fromType, toType), () -> {
                String name = mangleOperatorName(operatorType);
                FunctionBinding functionBinding = functionResolver.resolveCoercion(
                        session,
                        functions.getFunctions(QualifiedName.of(name)),
                        new Signature(name, toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature())));
                return resolve(session, functionBinding);
            });
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TrinoException) {
                TrinoException cause = (TrinoException) e.getCause();
                if (cause.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                    throw new OperatorNotFoundException(operatorType, ImmutableList.of(fromType), toType.getTypeSignature(), cause);
                }
                throw cause;
            }
            throw e;
        }
    }

    @Override
    public ResolvedFunction getCoercion(Session session, QualifiedName name, Type fromType, Type toType)
    {
        FunctionBinding functionBinding = functionResolver.resolveCoercion(
                session,
                functions.getFunctions(name),
                new Signature(name.getSuffix(), toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature())));
        return resolve(session, functionBinding);
    }

    private ResolvedFunction resolve(Session session, FunctionBinding functionBinding)
    {
        FunctionDependencyDeclaration declaration = functions.getFunctionDependencies(functionBinding.getFunctionId(), functionBinding.getBoundSignature());
        FunctionMetadata functionMetadata = getFunctionMetadata(functionBinding.getFunctionId(), functionBinding.getBoundSignature());
        return resolve(session, functionBinding, functionMetadata, declaration);
    }

    @VisibleForTesting
    public ResolvedFunction resolve(Session session, FunctionBinding functionBinding, FunctionMetadata functionMetadata, FunctionDependencyDeclaration declaration)
    {
        Map<TypeSignature, Type> dependentTypes = declaration.getTypeDependencies().stream()
                .map(typeSignature -> applyBoundVariables(typeSignature, functionBinding))
                .collect(toImmutableMap(Function.identity(), typeManager::getType, (left, right) -> left));

        ImmutableSet.Builder<ResolvedFunction> functions = ImmutableSet.builder();
        declaration.getFunctionDependencies().stream()
                .map(functionDependency -> {
                    try {
                        List<TypeSignature> argumentTypes = applyBoundVariables(functionDependency.getArgumentTypes(), functionBinding);
                        return resolvedFunctionInternal(session, functionDependency.getName(), fromTypeSignatures(argumentTypes));
                    }
                    catch (TrinoException e) {
                        if (functionDependency.isOptional()) {
                            return null;
                        }
                        throw e;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(functions::add);

        declaration.getOperatorDependencies().stream()
                .map(operatorDependency -> {
                    try {
                        List<TypeSignature> argumentTypes = applyBoundVariables(operatorDependency.getArgumentTypes(), functionBinding);
                        return resolvedFunctionInternal(session, QualifiedName.of(mangleOperatorName(operatorDependency.getOperatorType())), fromTypeSignatures(argumentTypes));
                    }
                    catch (TrinoException e) {
                        if (operatorDependency.isOptional()) {
                            return null;
                        }
                        throw e;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(functions::add);

        declaration.getCastDependencies().stream()
                .map(castDependency -> {
                    try {
                        Type fromType = typeManager.getType(applyBoundVariables(castDependency.getFromType(), functionBinding));
                        Type toType = typeManager.getType(applyBoundVariables(castDependency.getToType(), functionBinding));
                        return getCoercion(session, fromType, toType);
                    }
                    catch (TrinoException e) {
                        if (castDependency.isOptional()) {
                            return null;
                        }
                        throw e;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(functions::add);

        return new ResolvedFunction(
                functionBinding.getBoundSignature(),
                functionBinding.getFunctionId(),
                functionMetadata.getKind(),
                functionMetadata.isDeterministic(),
                functionMetadata.getFunctionNullability(),
                dependentTypes,
                functions.build());
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        return functions.getFunctions(name).stream()
                .map(FunctionMetadata::getKind)
                .anyMatch(AGGREGATE::equals);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ResolvedFunction resolvedFunction)
    {
        return getFunctionMetadata(resolvedFunction.getFunctionId(), resolvedFunction.getSignature());
    }

    private FunctionMetadata getFunctionMetadata(FunctionId functionId, BoundSignature signature)
    {
        FunctionMetadata functionMetadata = functions.getFunctionMetadata(functionId);

        // specialize function metadata to resolvedFunction
        List<Boolean> argumentNullability = functionMetadata.getFunctionNullability().getArgumentNullable();
        if (functionMetadata.getSignature().isVariableArity()) {
            List<Boolean> fixedArgumentNullability = argumentNullability.subList(0, argumentNullability.size() - 1);
            int variableArgumentCount = signature.getArgumentTypes().size() - fixedArgumentNullability.size();
            argumentNullability = ImmutableList.<Boolean>builder()
                    .addAll(fixedArgumentNullability)
                    .addAll(nCopies(variableArgumentCount, argumentNullability.get(argumentNullability.size() - 1)))
                    .build();
        }

        return new FunctionMetadata(
                functionMetadata.getFunctionId(),
                signature.toSignature(),
                functionMetadata.getCanonicalName(),
                new FunctionNullability(functionMetadata.getFunctionNullability().isReturnNullable(), argumentNullability),
                functionMetadata.isHidden(),
                functionMetadata.isDeterministic(),
                functionMetadata.getDescription(),
                functionMetadata.getKind(),
                functionMetadata.isDeprecated());
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(ResolvedFunction resolvedFunction)
    {
        AggregationFunctionMetadata aggregationFunctionMetadata = functions.getAggregationFunctionMetadata(resolvedFunction.getFunctionId());
        List<TypeSignature> intermediateTypes = aggregationFunctionMetadata.getIntermediateTypes();
        if (!intermediateTypes.isEmpty()) {
            FunctionBinding functionBinding = toFunctionBinding(resolvedFunction);
            intermediateTypes = aggregationFunctionMetadata.getIntermediateTypes().stream()
                    .map(typeSignature -> applyBoundVariables(typeSignature, functionBinding))
                    .collect(toImmutableList());
        }
        return new AggregationFunctionMetadata(aggregationFunctionMetadata.isOrderSensitive(), intermediateTypes);
    }

    private FunctionBinding toFunctionBinding(ResolvedFunction resolvedFunction)
    {
        Signature functionSignature = functions.getFunctionMetadata(resolvedFunction.getFunctionId()).getSignature();
        return toFunctionBinding(resolvedFunction.getFunctionId(), resolvedFunction.getSignature(), functionSignature);
    }

    @VisibleForTesting
    public static FunctionBinding toFunctionBinding(FunctionId functionId, BoundSignature boundSignature, Signature functionSignature)
    {
        return SignatureBinder.bindFunction(
                functionId,
                functionSignature,
                boundSignature);
    }

    //
    // Blocks
    //

    //
    // Helpers
    //

    private static Optional<CatalogSchemaTableName> getTableNameIfSystemSecurity(Session session, CatalogMetadata catalogMetadata, TableHandle tableHandle)
    {
        if (catalogMetadata.getSecurityManagement() == SecurityManagement.CONNECTOR) {
            return Optional.empty();
        }
        ConnectorTableSchema tableSchema = catalogMetadata.getMetadata(session).getTableSchema(session.toConnectorSession(tableHandle.getCatalogName()), tableHandle.getConnectorHandle());
        return Optional.of(new CatalogSchemaTableName(tableHandle.getCatalogName().getCatalogName(), tableSchema.getTable()));
    }

    private Optional<CatalogMetadata> getOptionalCatalogMetadata(Session session, String catalogName)
    {
        Optional<CatalogMetadata> optionalCatalogMetadata = transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName);
        optionalCatalogMetadata.ifPresent(catalogMetadata -> registerCatalogForQuery(session, catalogMetadata));
        return optionalCatalogMetadata;
    }

    private CatalogMetadata getCatalogMetadata(Session session, CatalogName catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadata(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, String catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, CatalogName catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private ConnectorMetadata getMetadata(Session session, CatalogName catalogName)
    {
        return getCatalogMetadata(session, catalogName).getMetadataFor(session, catalogName);
    }

    private ConnectorMetadata getMetadataForWrite(Session session, CatalogName catalogName)
    {
        return getCatalogMetadataForWrite(session, catalogName).getMetadata(session);
    }

    private void registerCatalogForQuery(Session session, CatalogMetadata catalogMetadata)
    {
        catalogsByQueryId.computeIfAbsent(session.getQueryId(), queryId -> new QueryCatalogs(session))
                .registerCatalog(catalogMetadata);
    }

    @VisibleForTesting
    public Set<QueryId> getActiveQueryIds()
    {
        return ImmutableSet.copyOf(catalogsByQueryId.keySet());
    }

    private static class QueryCatalogs
    {
        private final Session session;
        @GuardedBy("this")
        private final Map<CatalogName, CatalogMetadata> catalogs = new HashMap<>();
        @GuardedBy("this")
        private boolean finished;

        public QueryCatalogs(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        private synchronized void registerCatalog(CatalogMetadata catalogMetadata)
        {
            checkState(!finished, "Query is already finished");
            if (catalogs.putIfAbsent(catalogMetadata.getCatalogName(), catalogMetadata) == null) {
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
                catalogMetadata.getMetadata(session).beginQuery(connectorSession);
            }
        }

        private synchronized void finish()
        {
            List<CatalogMetadata> catalogs;
            synchronized (this) {
                checkState(!finished, "Query is already finished");
                finished = true;
                catalogs = new ArrayList<>(this.catalogs.values());
            }

            for (CatalogMetadata catalogMetadata : catalogs) {
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogName());
                catalogMetadata.getMetadata(session).cleanupQuery(connectorSession);
            }
        }
    }

    @Override
    public boolean isValidTableVersion(Session session, QualifiedObjectName tableName, TableVersion version)
    {
        requireNonNull(version, "Version must not be null for table " + tableName);

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, tableName.getCatalogName());
        if (!catalog.isPresent()) {
            return false;
        }

        CatalogMetadata catalogMetadata = catalog.get();
        CatalogName connectorId = catalogMetadata.getConnectorId(session, tableName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, connectorId);
        return metadata.isSupportedVersionType(session.toConnectorSession(), tableName.asSchemaTableName(), version.getPointerType(), version.getObjectType());
    }

    private Optional<ConnectorTableVersion> toConnectorVersion(Optional<TableVersion> version)
    {
        Optional<ConnectorTableVersion> connectorVersion = Optional.empty();
        if (version.isPresent()) {
            connectorVersion = Optional.of(new ConnectorTableVersion(version.get().getPointerType(), version.get().getObjectType(), version.get().getPointer()));
        }
        return connectorVersion;
    }

    private static class OperatorCacheKey
    {
        private final OperatorType operatorType;
        private final List<? extends Type> argumentTypes;

        private OperatorCacheKey(OperatorType operatorType, List<? extends Type> argumentTypes)
        {
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
            this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        }

        public OperatorType getOperatorType()
        {
            return operatorType;
        }

        public List<? extends Type> getArgumentTypes()
        {
            return argumentTypes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operatorType, argumentTypes);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof OperatorCacheKey)) {
                return false;
            }
            OperatorCacheKey other = (OperatorCacheKey) obj;
            return Objects.equals(this.operatorType, other.operatorType) &&
                    Objects.equals(this.argumentTypes, other.argumentTypes);
        }
    }

    private static class CoercionCacheKey
    {
        private final OperatorType operatorType;
        private final Type fromType;
        private final Type toType;

        private CoercionCacheKey(OperatorType operatorType, Type fromType, Type toType)
        {
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
            this.fromType = requireNonNull(fromType, "fromType is null");
            this.toType = requireNonNull(toType, "toType is null");
        }

        public OperatorType getOperatorType()
        {
            return operatorType;
        }

        public Type getFromType()
        {
            return fromType;
        }

        public Type getToType()
        {
            return toType;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operatorType, fromType, toType);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CoercionCacheKey)) {
                return false;
            }
            CoercionCacheKey other = (CoercionCacheKey) obj;
            return Objects.equals(this.operatorType, other.operatorType) &&
                    Objects.equals(this.fromType, other.fromType) &&
                    Objects.equals(this.toType, other.toType);
        }
    }

    public static MetadataManager createTestMetadataManager()
    {
        return testMetadataManagerBuilder().build();
    }

    public static TestMetadataManagerBuilder testMetadataManagerBuilder()
    {
        return new TestMetadataManagerBuilder();
    }

    public static class TestMetadataManagerBuilder
    {
        private FeaturesConfig featuresConfig;
        private TransactionManager transactionManager;
        private TypeManager typeManager = TESTING_TYPE_MANAGER;
        private GlobalFunctionCatalog globalFunctionCatalog;

        private TestMetadataManagerBuilder() {}

        public TestMetadataManagerBuilder withCatalogManager(CatalogManager catalogManager)
        {
            this.transactionManager = createTestTransactionManager(catalogManager);
            return this;
        }

        public TestMetadataManagerBuilder withFeaturesConfig(FeaturesConfig featuresConfig)
        {
            this.featuresConfig = featuresConfig;
            return this;
        }

        public TestMetadataManagerBuilder withTransactionManager(TransactionManager transactionManager)
        {
            this.transactionManager = transactionManager;
            return this;
        }

        public TestMetadataManagerBuilder withTypeManager(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            return this;
        }

        public TestMetadataManagerBuilder withGlobalFunctionCatalog(GlobalFunctionCatalog globalFunctionCatalog)
        {
            this.globalFunctionCatalog = globalFunctionCatalog;
            return this;
        }

        public MetadataManager build()
        {
            FeaturesConfig featuresConfig = this.featuresConfig;
            if (featuresConfig == null) {
                featuresConfig = new FeaturesConfig();
            }

            TransactionManager transactionManager = this.transactionManager;
            if (transactionManager == null) {
                transactionManager = createTestTransactionManager();
            }

            GlobalFunctionCatalog globalFunctionCatalog = this.globalFunctionCatalog;
            if (globalFunctionCatalog == null) {
                globalFunctionCatalog = new GlobalFunctionCatalog();
                TypeOperators typeOperators = new TypeOperators();
                globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(featuresConfig, typeOperators, new BlockTypeOperators(typeOperators), UNKNOWN));
                globalFunctionCatalog.addFunctions(new InternalFunctionBundle(new LiteralFunction(new InternalBlockEncodingSerde(new BlockEncodingManager(), typeManager))));
            }

            return new MetadataManager(
                    featuresConfig,
                    new DisabledSystemSecurityMetadata(),
                    transactionManager,
                    globalFunctionCatalog,
                    typeManager);
        }
    }
}
