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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.cache.NonEvictableCache;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.FunctionResolver.CatalogFunctionBinding;
import io.trino.metadata.FunctionResolver.CatalogFunctionMetadata;
import io.trino.metadata.ResolvedFunction.ResolvedFunctionDecoder;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
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
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationFunctionMetadata.AggregationFunctionMetadataBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.QualifiedFunctionName;
import io.trino.spi.function.Signature;
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
import io.trino.sql.SqlPathElement;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.ConnectorExpressions;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import io.trino.transaction.TransactionManager;
import io.trino.type.BlockTypeOperators;
import io.trino.type.TypeCoercion;

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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.client.NodeVersion.UNKNOWN;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.CONNECTOR;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.SYSTEM;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.trino.metadata.RedirectionAwareTableHandle.noRedirection;
import static io.trino.metadata.RedirectionAwareTableHandle.withRedirectionTo;
import static io.trino.metadata.SignatureBinder.applyBoundVariables;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
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
    private static final Logger log = Logger.get(MetadataManager.class);

    @VisibleForTesting
    public static final int MAX_TABLE_REDIRECTIONS = 10;

    private final GlobalFunctionCatalog functions;
    private final FunctionResolver functionResolver;
    private final SystemSecurityMetadata systemSecurityMetadata;
    private final TransactionManager transactionManager;
    private final TypeManager typeManager;
    private final TypeCoercion typeCoercion;

    private final ConcurrentMap<QueryId, QueryCatalogs> catalogsByQueryId = new ConcurrentHashMap<>();

    private final ResolvedFunctionDecoder functionDecoder;

    private final NonEvictableCache<OperatorCacheKey, ResolvedFunction> operatorCache;
    private final NonEvictableCache<CoercionCacheKey, ResolvedFunction> coercionCache;

    @Inject
    public MetadataManager(
            SystemSecurityMetadata systemSecurityMetadata,
            TransactionManager transactionManager,
            GlobalFunctionCatalog globalFunctionCatalog,
            TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        functions = requireNonNull(globalFunctionCatalog, "globalFunctionCatalog is null");
        functionResolver = new FunctionResolver(this, typeManager);
        this.typeCoercion = new TypeCoercion(typeManager::getType);

        this.systemSecurityMetadata = requireNonNull(systemSecurityMetadata, "systemSecurityMetadata is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        functionDecoder = new ResolvedFunctionDecoder(typeManager::getType);

        operatorCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
        coercionCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
    {
        return getCatalogMetadata(session, catalogHandle).getConnectorCapabilities();
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
        ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogHandle());
        return catalogMetadata.listCatalogHandles().stream()
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
            ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogHandle());
            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                metadata.listSchemaNames(connectorSession).stream()
                        .map(schema -> schema.toLowerCase(Locale.ENGLISH))
                        .filter(schema -> !isExternalInformationSchema(catalogHandle, schema))
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
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

            ConnectorTableHandle tableHandle = metadata.getTableHandle(
                    connectorSession,
                    table.asSchemaTableName(),
                    toConnectorVersion(startVersion),
                    toConnectorVersion(endVersion));
            return Optional.ofNullable(tableHandle)
                    .map(connectorTableHandle -> new TableHandle(
                            catalogHandle,
                            connectorTableHandle,
                            catalogMetadata.getTransactionHandleFor(catalogHandle)));
        });
    }

    @Override
    public Optional<TableExecuteHandle> getTableHandleForExecute(Session session, TableHandle tableHandle, String procedure, Map<String, Object> executeProperties)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(procedure, "procedure is null");
        requireNonNull(executeProperties, "executeProperties is null");

        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

        Optional<ConnectorTableExecuteHandle> executeHandle = metadata.getTableHandleForExecute(
                session.toConnectorSession(catalogHandle),
                tableHandle.getConnectorHandle(),
                procedure,
                executeProperties,
                getRetryPolicy(session).getRetryMode());

        return executeHandle.map(handle -> new TableExecuteHandle(
                catalogHandle,
                tableHandle.getTransaction(),
                handle));
    }

    @Override
    public Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        return metadata.getLayoutForTableExecute(session.toConnectorSession(catalogHandle), tableExecuteHandle.getConnectorHandle())
                .map(layout -> new TableLayout(catalogHandle, catalogMetadata.getTransactionHandleFor(catalogHandle), layout));
    }

    @Override
    public BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle tableExecuteHandle, TableHandle sourceHandle)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> connectorBeginResult = metadata.beginTableExecute(session.toConnectorSession(), tableExecuteHandle.getConnectorHandle(), sourceHandle.getConnectorHandle());

        return new BeginTableExecuteResult<>(
                tableExecuteHandle.withConnectorHandle(connectorBeginResult.getTableExecuteHandle()),
                sourceHandle.withConnectorHandle(connectorBeginResult.getSourceHandle()));
    }

    @Override
    public void finishTableExecute(Session session, TableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        metadata.finishTableExecute(session.toConnectorSession(catalogHandle), tableExecuteHandle.getConnectorHandle(), fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        metadata.executeTableExecute(session.toConnectorSession(catalogHandle), tableExecuteHandle.getConnectorHandle());
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
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            return metadata.getSystemTable(session.toConnectorSession(catalogHandle), tableName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        CatalogHandle catalogHandle = handle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        return new TableProperties(catalogHandle, handle.getTransaction(), metadata.getTableProperties(connectorSession, handle.getConnectorHandle()));
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        checkArgument(partitioningHandle.getCatalogHandle().isPresent(), "Expect partitioning handle from connector, got system partitioning handle");
        CatalogHandle catalogHandle = partitioningHandle.getCatalogHandle().get();
        checkArgument(catalogHandle.equals(tableHandle.getCatalogHandle()), "ConnectorId of tableHandle and partitioningHandle does not match");
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogHandle);

        ConnectorTableHandle newTableHandle = metadata.makeCompatiblePartitioning(
                session.toConnectorSession(catalogHandle),
                tableHandle.getConnectorHandle(),
                partitioningHandle.getConnectorHandle());
        return new TableHandle(catalogHandle, newTableHandle, transaction);
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        Optional<CatalogHandle> leftCatalogHandle = left.getCatalogHandle();
        Optional<CatalogHandle> rightCatalogHandle = right.getCatalogHandle();
        if (leftCatalogHandle.isEmpty() || rightCatalogHandle.isEmpty() || !leftCatalogHandle.equals(rightCatalogHandle)) {
            return Optional.empty();
        }
        if (!left.getTransactionHandle().equals(right.getTransactionHandle())) {
            return Optional.empty();
        }
        CatalogHandle catalogHandle = leftCatalogHandle.get();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        Optional<ConnectorPartitioningHandle> commonHandle = metadata.getCommonPartitioningHandle(session.toConnectorSession(catalogHandle), left.getConnectorHandle(), right.getConnectorHandle());
        return commonHandle.map(handle -> new PartitioningHandle(Optional.of(catalogHandle), left.getTransactionHandle(), handle));
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        CatalogHandle catalogHandle = handle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        return metadata.getInfo(handle.getConnectorHandle());
    }

    @Override
    public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        SchemaTableName tableName = metadata.getTableName(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());

        return new CatalogSchemaTableName(catalogMetadata.getCatalogName(), tableName);
    }

    @Override
    public TableSchema getTableSchema(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTableSchema tableSchema = metadata.getTableSchema(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());

        return new TableSchema(catalogMetadata.getCatalogName(), tableSchema);
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());

        return new TableMetadata(catalogMetadata.getCatalogName(), tableMetadata);
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        TableStatistics tableStatistics = metadata.getTableStatistics(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());
        verifyNotNull(tableStatistics, "%s returned null tableStatistics for %s", metadata, tableHandle);
        return tableStatistics;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());

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

        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        return metadata.getColumnMetadata(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), columnHandle);
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

            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                if (isExternalInformationSchema(catalogHandle, prefix.getSchemaName())) {
                    continue;
                }
                metadata.listTables(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(table -> !isExternalInformationSchema(catalogHandle, table.getSchemaName()))
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
            return Optional.of(getRedirectionAwareTableHandle(session, name).tableHandle().isPresent());
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
    public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        requireNonNull(prefix, "prefix is null");

        String catalogName = prefix.getCatalogName();
        Optional<String> schemaName = prefix.getSchemaName();
        Optional<String> relationName = prefix.getTableName();

        if (catalogName.isEmpty() ||
                (schemaName.isPresent() && schemaName.get().isEmpty()) ||
                (relationName.isPresent() && relationName.get().isEmpty())) {
            // Cannot exist
            return ImmutableList.of();
        }

        if (relationName.isPresent()) {
            QualifiedObjectName objectName = new QualifiedObjectName(catalogName, schemaName.orElseThrow(), relationName.get());
            SchemaTableName schemaTableName = objectName.asSchemaTableName();

            return Optional.<RelationColumnsMetadata>empty()
                    .or(() -> getMaterializedViewInternal(session, objectName)
                            .map(materializedView -> RelationColumnsMetadata.forMaterializedView(schemaTableName, materializedView.getColumns())))
                    .or(() -> getViewInternal(session, objectName)
                            .map(view -> RelationColumnsMetadata.forView(schemaTableName, view.getColumns())))
                    .or(() -> {
                        try {
                            // TODO: redirects are handled inefficiently: we currently throw-away redirect info and redo it later
                            RedirectionAwareTableHandle redirectionAware = getRedirectionAwareTableHandle(session, objectName);
                            if (redirectionAware.redirectedTableName().isPresent()) {
                                return Optional.of(RelationColumnsMetadata.forRedirectedTable(schemaTableName));
                            }
                            if (redirectionAware.tableHandle().isPresent()) {
                                return Optional.of(RelationColumnsMetadata.forTable(schemaTableName, getTableMetadata(session, redirectionAware.tableHandle().get()).getColumns()));
                            }
                        }
                        catch (RuntimeException e) {
                            if (!(e instanceof TrinoException trinoException) || !trinoException.getErrorCode().equals(UNSUPPORTED_TABLE_TYPE.toErrorCode())) {
                                log.warn(e, "Failed to get metadata for table: %s", objectName);
                            }
                        }
                        // Not found, or getting metadata failed.
                        return Optional.empty();
                    })
                    .filter(relationColumnsMetadata -> relationFilter.apply(ImmutableSet.of(relationColumnsMetadata.name())).contains(relationColumnsMetadata.name()))
                    .map(relationColumnsMetadata -> ImmutableList.of(tableColumnsMetadata(catalogName, relationColumnsMetadata)))
                    .orElse(ImmutableList.of());
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, catalogName);
        Map<SchemaTableName, TableColumnsMetadata> tableColumns = new HashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                if (isExternalInformationSchema(catalogHandle, schemaName)) {
                    continue;
                }
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                metadata.streamRelationColumns(connectorSession, schemaName, relationFilter)
                        .forEachRemaining(relationColumnsMetadata -> {
                            if (!isExternalInformationSchema(catalogHandle, relationColumnsMetadata.name().getSchemaName())) {
                                // putIfAbsent to resolve any potential conflicts between system tables and regular tables
                                tableColumns.putIfAbsent(relationColumnsMetadata.name(), tableColumnsMetadata(catalogName, relationColumnsMetadata));
                            }
                        });
            }
        }
        return ImmutableList.copyOf(tableColumns.values());
    }

    private TableColumnsMetadata tableColumnsMetadata(String catalogName, RelationColumnsMetadata relationColumnsMetadata)
    {
        SchemaTableName relationName = relationColumnsMetadata.name();
        Optional<List<ColumnMetadata>> columnsMetadata = Optional.<List<ColumnMetadata>>empty()
                .or(() -> relationColumnsMetadata.materializedViewColumns()
                        .map(columns -> materializedViewColumnMetadata(catalogName, relationName, columns)))
                .or(() -> relationColumnsMetadata.viewColumns()
                        .map(columns -> viewColumnMetadata(catalogName, relationName, columns)))
                .or(relationColumnsMetadata::tableColumns)
                .or(() -> {
                    checkState(relationColumnsMetadata.redirected(), "Invalid RelationColumnsMetadata: %s", relationColumnsMetadata);
                    return Optional.empty();
                });
        return new TableColumnsMetadata(relationName, columnsMetadata);
    }

    private List<ColumnMetadata> materializedViewColumnMetadata(String catalogName, SchemaTableName materializedViewName, List<ConnectorMaterializedViewDefinition.Column> columns)
    {
        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builderWithExpectedSize(columns.size());
        for (ConnectorMaterializedViewDefinition.Column column : columns) {
            try {
                columnMetadata.add(ColumnMetadata.builder()
                        .setName(column.getName())
                        .setType(typeManager.getType(column.getType()))
                        .setComment(column.getComment())
                        .build());
            }
            catch (TypeNotFoundException e) {
                QualifiedObjectName name = new QualifiedObjectName(catalogName, materializedViewName.getSchemaName(), materializedViewName.getTableName());
                throw new TrinoException(INVALID_VIEW, format("Unknown type '%s' for column '%s' in materialized view: %s", column.getType(), column.getName(), name));
            }
        }
        return columnMetadata.build();
    }

    private List<ColumnMetadata> viewColumnMetadata(String catalogName, SchemaTableName viewName, List<ConnectorViewDefinition.ViewColumn> columns)
    {
        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builderWithExpectedSize(columns.size());
        for (ConnectorViewDefinition.ViewColumn column : columns) {
            try {
                columnMetadata.add(ColumnMetadata.builder()
                        .setName(column.getName())
                        .setType(typeManager.getType(column.getType()))
                        .setComment(column.getComment())
                        .build());
            }
            catch (TypeNotFoundException e) {
                QualifiedObjectName name = new QualifiedObjectName(catalogName, viewName.getSchemaName(), viewName.getTableName());
                throw new TrinoException(INVALID_VIEW, format("Unknown type '%s' for column '%s' in view: %s", column.getType(), column.getName(), name));
            }
        }
        return columnMetadata.build();
    }

    @Override
    public List<RelationCommentMetadata> listRelationComments(Session session, String catalogName, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, catalogName);

        ImmutableList.Builder<RelationCommentMetadata> tableComments = ImmutableList.builder();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                if (isExternalInformationSchema(catalogHandle, schemaName)) {
                    continue;
                }

                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                stream(metadata.streamRelationComments(connectorSession, schemaName, relationFilter))
                        .filter(commentMetadata -> !isExternalInformationSchema(catalogHandle, commentMetadata.name().getSchemaName()))
                        .forEach(tableComments::add);
            }
        }
        return tableComments.build();
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.createSchema(session.toConnectorSession(catalogHandle), schema.getSchemaName(), properties, principal);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.schemaCreated(session, schema);
        }
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema, boolean cascade)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.dropSchema(session.toConnectorSession(catalogHandle), schema.getSchemaName(), cascade);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.schemaDropped(session, schema);
        }
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.renameSchema(session.toConnectorSession(catalogHandle), source.getSchemaName(), target);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.schemaRenamed(session, source, new CatalogSchemaName(source.getCatalogName(), target));
        }
    }

    @Override
    public void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.setSchemaOwner(session, source, principal);
        }
        else {
            metadata.setSchemaAuthorization(session.toConnectorSession(catalogHandle), source.getSchemaName(), principal);
        }
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.createTable(session.toConnectorSession(catalogHandle), tableMetadata, ignoreExisting);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, new CatalogSchemaTableName(catalogName, tableMetadata.getTable()));
        }
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName sourceTableName, QualifiedObjectName newTableName)
    {
        String catalogName = newTableName.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        if (!tableHandle.getCatalogHandle().equals(catalogHandle)) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.renameTable(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), newTableName.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() != CONNECTOR) {
            systemSecurityMetadata.tableRenamed(session, sourceTableName, newTableName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setTableProperties(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), properties);
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setTableComment(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), comment);
    }

    @Override
    public void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.setViewComment(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), comment);
    }

    @Override
    public void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.setViewColumnComment(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), columnName, comment);
    }

    @Override
    public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setColumnComment(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), column, comment);
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle.getCatalogName());
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.renameColumn(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), source, target.toLowerCase(ENGLISH));
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, source);
            systemSecurityMetadata.columnRenamed(session, table, columnMetadata.getName(), target);
        }
    }

    @Override
    public void renameField(Session session, TableHandle tableHandle, List<String> fieldPath, String target)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.renameField(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), fieldPath, target.toLowerCase(ENGLISH));
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle.getCatalogName());
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.addColumn(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), column);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.columnCreated(session, table, column.getName());
        }
    }

    @Override
    public void addField(Session session, TableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.addField(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), parentPath, fieldName, type, ignoreExisting);
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle column)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle.getCatalogName());
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.dropColumn(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), column);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, column);
            systemSecurityMetadata.columnDropped(session, table, columnMetadata.getName());
        }
    }

    @Override
    public void dropField(Session session, TableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.dropField(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), column, fieldPath);
    }

    @Override
    public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle column, Type type)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setColumnType(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), column, type);
    }

    @Override
    public void setFieldType(Session session, TableHandle tableHandle, List<String> fieldPath, Type type)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setFieldType(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), fieldPath, type);
    }

    @Override
    public void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, table.getCatalogName());
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.setTableOwner(session, table, principal);
        }
        else {
            metadata.setTableAuthorization(session.toConnectorSession(catalogMetadata.getCatalogHandle()), table.getSchemaTableName(), principal);
        }
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle, CatalogSchemaTableName tableName)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.dropTable(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());
        if (catalogMetadata.getSecurityManagement() != CONNECTOR) {
            systemSecurityMetadata.tableDropped(session, tableName);
        }
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.truncateTable(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());
    }

    @Override
    public Optional<TableLayout> getInsertLayout(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        return metadata.getInsertLayout(session.toConnectorSession(catalogHandle), table.getConnectorHandle())
                .map(layout -> new TableLayout(catalogHandle, catalogMetadata.getTransactionHandleFor(catalogHandle), layout));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, CatalogHandle catalogHandle, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return metadata.getStatisticsCollectionMetadataForWrite(session.toConnectorSession(catalogHandle), tableMetadata);
    }

    @Override
    public AnalyzeMetadata getStatisticsCollectionMetadata(Session session, TableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorAnalyzeMetadata analyze = metadata.getStatisticsCollectionMetadata(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), analyzeProperties);
        return new AnalyzeMetadata(analyze.getStatisticsMetadata(), new TableHandle(catalogHandle, analyze.getTableHandle(), tableHandle.getTransaction()));
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorTableHandle connectorTableHandle = metadata.beginStatisticsCollection(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());
        return new AnalyzeTableHandle(catalogHandle, transactionHandle, connectorTableHandle);
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        catalogMetadata.getMetadata(session).finishStatisticsCollection(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), computedStatistics);
    }

    @Override
    public Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.getNewTableLayout(connectorSession, tableMetadata)
                .map(layout -> new TableLayout(catalogHandle, transactionHandle, layout));
    }

    @Override
    public Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Type type)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return metadata.getSupportedType(session.toConnectorSession(catalogHandle), type)
                .map(newType -> {
                    if (!typeCoercion.isCompatible(newType, type)) {
                        throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, format("Type '%s' is not compatible with the supplied type '%s' in getSupportedType", type, newType));
                    }
                    return newType;
                });
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
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata, layout.map(TableLayout::getLayout), getRetryPolicy(session).getRetryMode());
        return new OutputTableHandle(catalogHandle, tableMetadata.getTable(), transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        Optional<ConnectorOutputMetadata> output = metadata.finishCreateTable(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), fragments, computedStatistics);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, new CatalogSchemaTableName(catalogHandle.getCatalogName(), tableHandle.getTableName()));
        }
        return output;
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorInsertTableHandle handle = metadata.beginInsert(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), columns, getRetryPolicy(session).getRetryMode());
        return new InsertTableHandle(tableHandle.getCatalogHandle(), transactionHandle, handle);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        return catalogMetadata.getMetadata(session).supportsMissingColumnsOnInsert();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        return metadata.finishInsert(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return metadata.delegateMaterializedViewRefreshToConnector(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName());
    }

    @Override
    public ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return asVoid(toListenableFuture(metadata.refreshMaterializedView(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName())));
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);

        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .map(TableHandle::getConnectorHandle)
                .collect(Collectors.toList());
        sourceConnectorHandles.add(tableHandle.getConnectorHandle());

        ConnectorInsertTableHandle handle = metadata.beginRefreshMaterializedView(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), sourceConnectorHandles, getRetryPolicy(session).getRetryMode());

        return new InsertTableHandle(tableHandle.getCatalogHandle(), transactionHandle, handle);
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
        CatalogHandle catalogHandle = insertHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .map(TableHandle::getConnectorHandle)
                .collect(toImmutableList());
        return metadata.finishRefreshMaterializedView(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), insertHandle.getConnectorHandle(),
                fragments, computedStatistics, sourceConnectorHandles);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        return metadata.getMergeRowIdColumnHandle(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());
    }

    @Override
    public Optional<PartitioningHandle> getUpdateLayout(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);

        return metadata.getUpdateLayout(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle())
                .map(partitioning -> new PartitioningHandle(Optional.of(catalogHandle), Optional.of(transactionHandle), partitioning));
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyDelete(connectorSession, table.getConnectorHandle())
                .map(newHandle -> new TableHandle(catalogHandle, newHandle, table.getTransaction()));
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        return metadata.executeDelete(connectorSession, table.getConnectorHandle());
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        return metadata.getRowChangeParadigm(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle());
    }

    @Override
    public MergeHandle beginMerge(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        ConnectorMergeTableHandle newHandle = metadata.beginMerge(session.toConnectorSession(catalogHandle), tableHandle.getConnectorHandle(), getRetryPolicy(session).getRetryMode());
        return new MergeHandle(tableHandle.withConnectorHandle(newHandle.getTableHandle()), newHandle);
    }

    @Override
    public void finishMerge(Session session, MergeHandle mergeHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = mergeHandle.getTableHandle().getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        metadata.finishMerge(session.toConnectorSession(catalogHandle), mergeHandle.getConnectorMergeHandle(), fragments, computedStatistics);
    }

    @Override
    public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName).map(CatalogMetadata::getCatalogHandle);
    }

    @Override
    public List<CatalogInfo> listCatalogs(Session session)
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

            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                if (isExternalInformationSchema(catalogHandle, prefix.getSchemaName())) {
                    continue;
                }
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                metadata.listViews(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(view -> !isExternalInformationSchema(catalogHandle, view.getSchemaName()))
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
            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                if (isExternalInformationSchema(catalogHandle, tablePrefix.getSchema())) {
                    continue;
                }

                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

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
                    if (isExternalInformationSchema(catalogHandle, entry.getKey().getSchemaName())) {
                        continue;
                    }
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
        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, schemaName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getConnectorHandleForSchema(schemaName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.getSchemaProperties(connectorSession, schemaName.getSchemaName());
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schemaName)
    {
        if (!schemaExists(session, schemaName)) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, schemaName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            return systemSecurityMetadata.getSchemaOwner(session, schemaName);
        }
        CatalogHandle catalogHandle = catalogMetadata.getConnectorHandleForSchema(schemaName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.getSchemaOwner(connectorSession, schemaName.getSchemaName());
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
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return metadata.getView(connectorSession, viewName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, boolean replace)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.createView(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), definition.toConnectorViewDefinition(), replace);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void renameView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, target.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (!source.getCatalogName().equals(target.getCatalogName())) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename views across catalogs");
        }

        metadata.renameView(session.toConnectorSession(catalogHandle), source.asSchemaTableName(), target.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableRenamed(session, source.asCatalogSchemaTableName(), target.asCatalogSchemaTableName());
        }
    }

    @Override
    public void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, view.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.setViewOwner(session, view, principal);
        }
        else {
            metadata.setViewAuthorization(session.toConnectorSession(catalogHandle), view.getSchemaTableName(), principal);
        }
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.dropView(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableDropped(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void createMaterializedView(Session session, QualifiedObjectName viewName, MaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.createMaterializedView(
                session.toConnectorSession(catalogHandle),
                viewName.asSchemaTableName(),
                definition.toConnectorMaterializedViewDefinition(),
                replace,
                ignoreExisting);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.dropMaterializedView(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
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

            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                if (isExternalInformationSchema(catalogHandle, prefix.getSchemaName())) {
                    continue;
                }
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                metadata.listMaterializedViews(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(materializedView -> !isExternalInformationSchema(catalogHandle, materializedView.getSchemaName()))
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
            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                if (isExternalInformationSchema(catalogHandle, tablePrefix.getSchema())) {
                    continue;
                }
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

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
                    if (isExternalInformationSchema(catalogHandle, entry.getKey().getSchemaName())) {
                        continue;
                    }
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
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
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
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return metadata.getMaterializedViewFreshness(connectorSession, viewName.asSchemaTableName());
        }
        return new MaterializedViewFreshness(STALE);
    }

    @Override
    public void renameMaterializedView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, target.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (!source.getCatalogName().equals(target.getCatalogName())) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename materialized views across catalogs");
        }

        metadata.renameMaterializedView(session.toConnectorSession(catalogHandle), source.asSchemaTableName(), target.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableRenamed(session, source.asCatalogSchemaTableName(), target.asCatalogSchemaTableName());
        }
    }

    @Override
    public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.setMaterializedViewProperties(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), properties);
    }

    @Override
    public void setMaterializedViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.setMaterializedViewColumnComment(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), columnName, comment);
    }

    private static boolean isExternalInformationSchema(CatalogHandle catalogHandle, Optional<String> schemaName)
    {
        return schemaName.isPresent() && isExternalInformationSchema(catalogHandle, schemaName.get());
    }

    private static boolean isExternalInformationSchema(CatalogHandle catalogHandle, String schemaName)
    {
        return !catalogHandle.getType().isInternal() && "information_schema".equalsIgnoreCase(schemaName);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
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
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, tableName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            Optional<QualifiedObjectName> redirectedTableName = metadata.redirectTable(session.toConnectorSession(catalogHandle), tableName.asSchemaTableName())
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
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.getConnectorHandle(), indexableColumns, outputColumns, tupleDomain);
        return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.getCatalogHandle(), transaction, resolved));
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyLimit(connectorSession, table.getConnectorHandle(), limit)
                .map(result -> new LimitApplicationResult<>(
                        new TableHandle(catalogHandle, result.getHandle(), table.getTransaction()),
                        result.isLimitGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applySample(connectorSession, table.getConnectorHandle(), sampleType, sampleRatio)
                .map(result -> new SampleApplicationResult<>(new TableHandle(
                        catalogHandle,
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

        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyAggregation(connectorSession, table.getConnectorHandle(), aggregations, assignments, groupingSets)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), aggregations.size());

                    return new AggregationApplicationResult<>(
                            new TableHandle(catalogHandle, result.getHandle(), table.getTransaction()),
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
            ConnectorExpression joinCondition,
            Map<String, ColumnHandle> leftAssignments,
            Map<String, ColumnHandle> rightAssignments,
            JoinStatistics statistics)
    {
        if (!right.getCatalogHandle().equals(left.getCatalogHandle())) {
            // Exact comparison is fine as catalog name here is passed from CatalogMetadata and is normalized to lowercase
            return Optional.empty();
        }
        CatalogHandle catalogHandle = left.getCatalogHandle();

        ConnectorTransactionHandle transaction = left.getTransaction();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        Optional<JoinApplicationResult<ConnectorTableHandle>> connectorResult =
                metadata.applyJoin(
                        connectorSession,
                        joinType,
                        left.getConnectorHandle(),
                        right.getConnectorHandle(),
                        joinCondition,
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
                            catalogHandle,
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
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyTopN(connectorSession, table.getConnectorHandle(), topNCount, sortItems, assignments)
                .map(result -> new TopNApplicationResult<>(
                        new TableHandle(catalogHandle, result.getHandle(), table.getTransaction()),
                        result.isTopNGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle)
    {
        CatalogHandle catalogHandle = handle.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        return metadata.applyTableFunction(session.toConnectorSession(catalogHandle), handle.getFunctionHandle())
                .map(result -> new TableFunctionApplicationResult<>(
                        new TableHandle(catalogHandle, result.getTableHandle(), handle.getTransactionHandle()),
                        result.getColumnHandles()));
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
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        metadata.validateScan(session.toConnectorSession(catalogHandle), table.getConnectorHandle());
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyFilter(connectorSession, table.getConnectorHandle(), constraint)
                .map(result -> result.transform(handle -> new TableHandle(catalogHandle, handle, table.getTransaction())));
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        CatalogHandle catalogHandle = table.getCatalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyProjection(connectorSession, table.getConnectorHandle(), projections, assignments)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), projections.size());

                    return new ProjectionApplicationResult<>(
                            new TableHandle(catalogHandle, result.getHandle(), table.getTransaction()),
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
        return getRequiredCatalogMetadata(session, catalog).getSecurityManagement() == CONNECTOR;
    }

    @Override
    public boolean roleExists(Session session, String role, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            return systemSecurityMetadata.roleExists(session, role);
        }

        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, catalog.get());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return metadata.roleExists(session.toConnectorSession(catalogHandle), role);
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            systemSecurityMetadata.createRole(session, role, grantor);
            return;
        }

        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog.get());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.createRole(session.toConnectorSession(catalogHandle), role, grantor);
    }

    @Override
    public void dropRole(Session session, String role, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            systemSecurityMetadata.dropRole(session, role);
            return;
        }

        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog.get());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.dropRole(session.toConnectorSession(catalogHandle), role);
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
            if (catalogMetadata.get().getSecurityManagement() == CONNECTOR) {
                CatalogHandle catalogHandle = catalogMetadata.get().getCatalogHandle();
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogHandle);
                return metadata.listRoles(connectorSession).stream()
                        .map(role -> role.toLowerCase(ENGLISH))
                        .collect(toImmutableSet());
            }
        }

        return systemSecurityMetadata.listRoles(session);
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
            if (catalogMetadata.get().getSecurityManagement() == CONNECTOR) {
                CatalogHandle catalogHandle = catalogMetadata.get().getCatalogHandle();
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogHandle);
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
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.grantRoles(session.toConnectorSession(catalogHandle), roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        if (catalog.isEmpty()) {
            systemSecurityMetadata.revokeRoles(session, roles, grantees, adminOption, grantor);
            return;
        }

        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog.get());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.revokeRoles(session.toConnectorSession(catalogHandle), roles, grantees, adminOption, grantor);
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
            if (catalogMetadata.get().getSecurityManagement() == CONNECTOR) {
                CatalogHandle catalogHandle = catalogMetadata.get().getCatalogHandle();
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogHandle);
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
        if (catalogMetadata.get().getSecurityManagement() == SYSTEM) {
            return systemSecurityMetadata.listEnabledRoles(session.getIdentity());
        }

        CatalogHandle catalogHandle = catalogMetadata.get().getCatalogHandle();
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(session, catalogHandle);
        return ImmutableSet.copyOf(metadata.listEnabledRoles(connectorSession));
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
            return;
        }
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.grantTablePrivileges(session.toConnectorSession(catalogHandle), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.denyTablePrivileges(session, tableName, privileges, grantee);
            return;
        }
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.denyTablePrivileges(session.toConnectorSession(catalogHandle), tableName.asSchemaTableName(), privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
            return;
        }
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.revokeTablePrivileges(session.toConnectorSession(catalogHandle), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
            return;
        }
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.grantSchemaPrivileges(session.toConnectorSession(catalogHandle), schemaName.getSchemaName(), privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.denySchemaPrivileges(session, schemaName, privileges, grantee);
            return;
        }
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.denySchemaPrivileges(session.toConnectorSession(catalogHandle), schemaName.getSchemaName(), privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
            return;
        }
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.revokeSchemaPrivileges(session.toConnectorSession(catalogHandle), schemaName.getSchemaName(), privileges, grantee, grantOption);
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
            ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogHandle());

            List<CatalogHandle> catalogHandles = prefix.asQualifiedObjectName()
                    .map(qualifiedTableName -> singletonList(catalogMetadata.getCatalogHandle(session, qualifiedTableName)))
                    .orElseGet(catalogMetadata::listCatalogHandles);
            for (CatalogHandle catalogHandle : catalogHandles) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                if (catalogMetadata.getSecurityManagement() == SYSTEM) {
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
    public Collection<FunctionMetadata> listFunctions(Session session)
    {
        ImmutableList.Builder<FunctionMetadata> functions = ImmutableList.builder();
        functions.addAll(this.functions.listFunctions());
        for (SqlPathElement sqlPathElement : session.getPath().getParsedPath()) {
            String catalog = sqlPathElement.getCatalog().map(Identifier::getValue).or(session::getCatalog)
                    .orElseThrow(() -> new IllegalArgumentException("Session default catalog must be set to resolve a partial function name: " + sqlPathElement));
            getOptionalCatalogMetadata(session, catalog).ifPresent(metadata -> {
                ConnectorSession connectorSession = session.toConnectorSession(metadata.getCatalogHandle());
                functions.addAll(metadata.getMetadata(session).listFunctions(connectorSession, sqlPathElement.getSchema().getValue().toLowerCase(ENGLISH)));
            });
        }
        return functions.build();
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
            if (e.getCause() instanceof TrinoException cause) {
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
                .orElseGet(() -> resolvedFunctionInternal(session, toQualifiedFunctionName(name), parameterTypes));
    }

    private ResolvedFunction resolvedFunctionInternal(Session session, QualifiedFunctionName name, List<TypeSignatureProvider> parameterTypes)
    {
        CatalogFunctionBinding catalogFunctionBinding = functionResolver.resolveFunction(
                session,
                name,
                parameterTypes,
                catalogSchemaFunctionName -> getFunctions(session, catalogSchemaFunctionName));
        return resolve(session, catalogFunctionBinding);
    }

    // this is only public for TableFunctionRegistry, which is effectively part of MetadataManager but for some reason is a separate class
    public static QualifiedFunctionName toQualifiedFunctionName(QualifiedName qualifiedName)
    {
        List<String> parts = qualifiedName.getParts();
        checkArgument(parts.size() <= 3, "Function name can only have 3 parts: " + qualifiedName);
        if (parts.size() == 3) {
            return QualifiedFunctionName.of(parts.get(0), parts.get(1), parts.get(2));
        }
        if (parts.size() == 2) {
            return QualifiedFunctionName.of(parts.get(0), parts.get(1));
        }
        return QualifiedFunctionName.of(parts.get(0));
    }

    @Override
    public ResolvedFunction getCoercion(Session session, OperatorType operatorType, Type fromType, Type toType)
    {
        checkArgument(operatorType == OperatorType.CAST || operatorType == OperatorType.SATURATED_FLOOR_CAST);
        try {
            // todo we should not be caching functions across session
            return uncheckedCacheGet(coercionCache, new CoercionCacheKey(operatorType, fromType, toType), () -> {
                String name = mangleOperatorName(operatorType);
                CatalogFunctionBinding functionBinding = functionResolver.resolveCoercion(
                        session,
                        QualifiedFunctionName.of(name),
                        Signature.builder()
                                .name(name)
                                .returnType(toType)
                                .argumentType(fromType)
                                .build(),
                        catalogSchemaFunctionName -> getFunctions(session, catalogSchemaFunctionName));
                return resolve(session, functionBinding);
            });
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TrinoException cause) {
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
        CatalogFunctionBinding catalogFunctionBinding = functionResolver.resolveCoercion(
                session,
                toQualifiedFunctionName(name),
                Signature.builder()
                        .name(name.getSuffix())
                        .returnType(toType)
                        .argumentType(fromType)
                        .build(),
                catalogSchemaFunctionName -> getFunctions(session, catalogSchemaFunctionName));
        return resolve(session, catalogFunctionBinding);
    }

    private ResolvedFunction resolve(Session session, CatalogFunctionBinding functionBinding)
    {
        FunctionDependencyDeclaration dependencies = getDependencies(
                session,
                functionBinding.getCatalogHandle(),
                functionBinding.getFunctionBinding().getFunctionId(),
                functionBinding.getFunctionBinding().getBoundSignature());
        FunctionMetadata functionMetadata = getFunctionMetadata(
                session,
                functionBinding.getCatalogHandle(),
                functionBinding.getFunctionBinding().getFunctionId(),
                functionBinding.getFunctionBinding().getBoundSignature());
        return resolve(session, functionBinding.getCatalogHandle(), functionBinding.getFunctionBinding(), functionMetadata, dependencies);
    }

    private FunctionDependencyDeclaration getDependencies(Session session, CatalogHandle catalogHandle, FunctionId functionId, BoundSignature boundSignature)
    {
        if (catalogHandle.equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            return functions.getFunctionDependencies(functionId, boundSignature);
        }
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return getMetadata(session, catalogHandle)
                .getFunctionDependencies(connectorSession, functionId, boundSignature);
    }

    @VisibleForTesting
    public ResolvedFunction resolve(Session session, CatalogHandle catalogHandle, FunctionBinding functionBinding, FunctionMetadata functionMetadata, FunctionDependencyDeclaration dependencies)
    {
        Map<TypeSignature, Type> dependentTypes = dependencies.getTypeDependencies().stream()
                .map(typeSignature -> applyBoundVariables(typeSignature, functionBinding))
                .collect(toImmutableMap(Function.identity(), typeManager::getType, (left, right) -> left));

        ImmutableSet.Builder<ResolvedFunction> functions = ImmutableSet.builder();
        dependencies.getFunctionDependencies().stream()
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

        dependencies.getOperatorDependencies().stream()
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

        dependencies.getCastDependencies().stream()
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
                catalogHandle,
                functionBinding.getFunctionId(),
                functionMetadata.getKind(),
                functionMetadata.isDeterministic(),
                functionMetadata.getFunctionNullability(),
                dependentTypes,
                functions.build());
    }

    @Override
    public boolean isAggregationFunction(Session session, QualifiedName name)
    {
        return functionResolver.isAggregationFunction(session, toQualifiedFunctionName(name), catalogSchemaFunctionName -> getFunctions(session, catalogSchemaFunctionName));
    }

    @Override
    public boolean isWindowFunction(Session session, QualifiedName name)
    {
        return functionResolver.isWindowFunction(session, toQualifiedFunctionName(name), catalogSchemaFunctionName -> getFunctions(session, catalogSchemaFunctionName));
    }

    private Collection<CatalogFunctionMetadata> getFunctions(Session session, CatalogSchemaFunctionName name)
    {
        if (name.getCatalogName().equals(GlobalSystemConnector.NAME)) {
            return functions.getFunctions(name.getSchemaFunctionName()).stream()
                    .map(function -> new CatalogFunctionMetadata(GlobalSystemConnector.CATALOG_HANDLE, function))
                    .collect(toImmutableList());
        }

        return getOptionalCatalogMetadata(session, name.getCatalogName())
                .map(metadata -> metadata.getMetadata(session)
                        .getFunctions(session.toConnectorSession(metadata.getCatalogHandle()), name.getSchemaFunctionName()).stream()
                        .map(function -> new CatalogFunctionMetadata(metadata.getCatalogHandle(), function))
                        .collect(toImmutableList()))
                .orElse(ImmutableList.of());
    }

    @Override
    public FunctionMetadata getFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
    {
        return getFunctionMetadata(session, resolvedFunction.getCatalogHandle(), resolvedFunction.getFunctionId(), resolvedFunction.getSignature());
    }

    private FunctionMetadata getFunctionMetadata(Session session, CatalogHandle catalogHandle, FunctionId functionId, BoundSignature signature)
    {
        FunctionMetadata functionMetadata;
        if (catalogHandle.equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            functionMetadata = functions.getFunctionMetadata(functionId);
        }
        else {
            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            functionMetadata = getMetadata(session, catalogHandle)
                    .getFunctionMetadata(connectorSession, functionId);
        }

        FunctionMetadata.Builder newMetadata = FunctionMetadata.builder(functionMetadata.getKind())
                .functionId(functionMetadata.getFunctionId())
                .signature(signature.toSignature())
                .canonicalName(functionMetadata.getCanonicalName());

        if (functionMetadata.getDescription().isEmpty()) {
            newMetadata.noDescription();
        }
        else {
            newMetadata.description(functionMetadata.getDescription());
        }

        if (functionMetadata.isHidden()) {
            newMetadata.hidden();
        }
        if (!functionMetadata.isDeterministic()) {
            newMetadata.nondeterministic();
        }
        if (functionMetadata.isDeprecated()) {
            newMetadata.deprecated();
        }
        if (functionMetadata.getFunctionNullability().isReturnNullable()) {
            newMetadata.nullable();
        }

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
        newMetadata.argumentNullability(argumentNullability);

        return newMetadata.build();
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
    {
        Signature functionSignature;
        AggregationFunctionMetadata aggregationFunctionMetadata;
        if (resolvedFunction.getCatalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            functionSignature = functions.getFunctionMetadata(resolvedFunction.getFunctionId()).getSignature();
            aggregationFunctionMetadata = functions.getAggregationFunctionMetadata(resolvedFunction.getFunctionId());
        }
        else {
            ConnectorSession connectorSession = session.toConnectorSession(resolvedFunction.getCatalogHandle());
            ConnectorMetadata metadata = getMetadata(session, resolvedFunction.getCatalogHandle());
            functionSignature = metadata.getFunctionMetadata(connectorSession, resolvedFunction.getFunctionId()).getSignature();
            aggregationFunctionMetadata = metadata.getAggregationFunctionMetadata(connectorSession, resolvedFunction.getFunctionId());
        }

        AggregationFunctionMetadataBuilder builder = AggregationFunctionMetadata.builder();
        if (aggregationFunctionMetadata.isOrderSensitive()) {
            builder.orderSensitive();
        }

        if (!aggregationFunctionMetadata.getIntermediateTypes().isEmpty()) {
            FunctionBinding functionBinding = toFunctionBinding(resolvedFunction.getFunctionId(), resolvedFunction.getSignature(), functionSignature);
            aggregationFunctionMetadata.getIntermediateTypes().stream()
                    .map(typeSignature -> applyBoundVariables(typeSignature, functionBinding))
                    .forEach(builder::intermediateType);
        }

        return builder.build();
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
    // Helpers
    //

    private Optional<CatalogMetadata> getOptionalCatalogMetadata(Session session, String catalogName)
    {
        Optional<CatalogMetadata> optionalCatalogMetadata = transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName);
        optionalCatalogMetadata.ifPresent(catalogMetadata -> registerCatalogForQuery(session, catalogMetadata));
        return optionalCatalogMetadata;
    }

    private CatalogMetadata getRequiredCatalogMetadata(Session session, String catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getRequiredCatalogMetadata(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private CatalogMetadata getCatalogMetadata(Session session, CatalogHandle catalogHandle)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadata(session.getRequiredTransactionId(), catalogHandle);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, String catalogName)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogName);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private CatalogMetadata getCatalogMetadataForWrite(Session session, CatalogHandle catalogHandle)
    {
        CatalogMetadata catalogMetadata = transactionManager.getCatalogMetadataForWrite(session.getRequiredTransactionId(), catalogHandle);
        registerCatalogForQuery(session, catalogMetadata);
        return catalogMetadata;
    }

    private ConnectorMetadata getMetadata(Session session, CatalogHandle catalogHandle)
    {
        return getCatalogMetadata(session, catalogHandle).getMetadataFor(session, catalogHandle);
    }

    private ConnectorMetadata getMetadataForWrite(Session session, CatalogHandle catalogHandle)
    {
        return getCatalogMetadataForWrite(session, catalogHandle).getMetadata(session);
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
        private final Map<CatalogHandle, CatalogMetadata> catalogs = new HashMap<>();
        @GuardedBy("this")
        private boolean finished;

        public QueryCatalogs(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        private synchronized void registerCatalog(CatalogMetadata catalogMetadata)
        {
            checkState(!finished, "Query is already finished");
            if (catalogs.putIfAbsent(catalogMetadata.getCatalogHandle(), catalogMetadata) == null) {
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogHandle());
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
                ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogHandle());
                catalogMetadata.getMetadata(session).cleanupQuery(connectorSession);
            }
        }
    }

    @Override
    public OptionalInt getMaxWriterTasks(Session session, String catalogName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, catalogName);
        if (catalog.isEmpty()) {
            return OptionalInt.empty();
        }

        CatalogMetadata catalogMetadata = catalog.get();
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        return catalogMetadata.getMetadata(session).getMaxWriterTasks(session.toConnectorSession(catalogHandle));
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(Session session, QualifiedObjectName tableName, Map<String, Object> tableProperties)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, tableName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        return metadata.getNewTableWriterScalingOptions(session.toConnectorSession(catalogHandle), tableName.asSchemaTableName(), tableProperties);
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(Session session, TableHandle tableHandle)
    {
        ConnectorMetadata metadata = getMetadataForWrite(session, tableHandle.getCatalogHandle());
        return metadata.getInsertWriterScalingOptions(session.toConnectorSession(tableHandle.getCatalogHandle()), tableHandle.getConnectorHandle());
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
        private TransactionManager transactionManager;
        private TypeManager typeManager = TESTING_TYPE_MANAGER;
        private GlobalFunctionCatalog globalFunctionCatalog;

        private TestMetadataManagerBuilder() {}

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
            TransactionManager transactionManager = this.transactionManager;
            if (transactionManager == null) {
                transactionManager = createTestTransactionManager();
            }

            GlobalFunctionCatalog globalFunctionCatalog = this.globalFunctionCatalog;
            if (globalFunctionCatalog == null) {
                globalFunctionCatalog = new GlobalFunctionCatalog();
                TypeOperators typeOperators = new TypeOperators();
                globalFunctionCatalog.addFunctions(SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, new BlockTypeOperators(typeOperators), UNKNOWN));
                globalFunctionCatalog.addFunctions(new InternalFunctionBundle(new LiteralFunction(new InternalBlockEncodingSerde(new BlockEncodingManager(), typeManager))));
            }

            return new MetadataManager(
                    new DisabledSystemSecurityMetadata(),
                    transactionManager,
                    globalFunctionCatalog,
                    typeManager);
        }
    }
}
