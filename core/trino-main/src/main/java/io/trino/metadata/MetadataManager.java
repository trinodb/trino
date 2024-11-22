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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.execution.QueryManager;
import io.trino.metadata.LanguageFunctionManager.RunAsIdentityLoader;
import io.trino.security.AccessControl;
import io.trino.security.InjectedConnectorAccessControl;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.RefreshType;
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
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
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
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationFunctionMetadata.AggregationFunctionMetadataBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.SchemaFunctionName;
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
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.transaction.TransactionManager;
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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.CONNECTOR;
import static io.trino.metadata.CatalogMetadata.SecurityManagement.SYSTEM;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.LanguageFunctionManager.isTrinoSqlLanguageFunction;
import static io.trino.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.trino.metadata.RedirectionAwareTableHandle.noRedirection;
import static io.trino.metadata.RedirectionAwareTableHandle.withRedirectionTo;
import static io.trino.metadata.SignatureBinder.applyBoundVariables;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractVariables;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class MetadataManager
        implements Metadata
{
    private static final Logger log = Logger.get(MetadataManager.class);

    @VisibleForTesting
    public static final int MAX_TABLE_REDIRECTIONS = 10;

    private final AccessControl accessControl;
    private final GlobalFunctionCatalog functions;
    private final BuiltinFunctionResolver functionResolver;
    private final SystemSecurityMetadata systemSecurityMetadata;
    private final TransactionManager transactionManager;
    private final LanguageFunctionManager languageFunctionManager;
    private final TableFunctionRegistry tableFunctionRegistry;
    private final TypeManager typeManager;
    private final TypeCoercion typeCoercion;
    private final QueryManager queryManager;

    private final ConcurrentMap<QueryId, QueryCatalogs> catalogsByQueryId = new ConcurrentHashMap<>();

    @Inject
    public MetadataManager(
            AccessControl accessControl,
            SystemSecurityMetadata systemSecurityMetadata,
            TransactionManager transactionManager,
            GlobalFunctionCatalog globalFunctionCatalog,
            LanguageFunctionManager languageFunctionManager,
            TableFunctionRegistry tableFunctionRegistry,
            TypeManager typeManager,
            QueryManager queryManager)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        functions = requireNonNull(globalFunctionCatalog, "globalFunctionCatalog is null");
        functionResolver = new BuiltinFunctionResolver(this, typeManager, globalFunctionCatalog);
        this.typeCoercion = new TypeCoercion(typeManager::getType);
        this.queryManager = requireNonNull(queryManager, "queryManager is null");

        this.systemSecurityMetadata = requireNonNull(systemSecurityMetadata, "systemSecurityMetadata is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.languageFunctionManager = requireNonNull(languageFunctionManager, "languageFunctionManager is null");
        this.tableFunctionRegistry = requireNonNull(tableFunctionRegistry, "tableFunctionRegistry is null");
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
        if (cannotExist(table)) {
            return Optional.empty();
        }

        return getOptionalCatalogMetadata(session, table.catalogName()).flatMap(catalogMetadata -> {
            Optional<ConnectorTableVersion> startTableVersion = toConnectorVersion(startVersion);
            Optional<ConnectorTableVersion> endTableVersion = toConnectorVersion(endVersion);

            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, table, startTableVersion, endTableVersion);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

            ConnectorTableHandle tableHandle = metadata.getTableHandle(
                    connectorSession,
                    table.asSchemaTableName(),
                    startTableVersion,
                    endTableVersion);
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

        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

        Optional<ConnectorTableExecuteHandle> executeHandle = metadata.getTableHandleForExecute(
                session.toConnectorSession(catalogHandle),
                new InjectedConnectorAccessControl(accessControl, session.toSecurityContext(), catalogHandle.getCatalogName().toString()),
                tableHandle.connectorHandle(),
                procedure,
                executeProperties,
                getRetryPolicy(session).getRetryMode());

        return executeHandle.map(handle -> new TableExecuteHandle(
                catalogHandle,
                tableHandle.transaction(),
                handle));
    }

    @Override
    public Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        return metadata.getLayoutForTableExecute(session.toConnectorSession(catalogHandle), tableExecuteHandle.connectorHandle())
                .map(layout -> new TableLayout(catalogHandle, catalogMetadata.getTransactionHandleFor(catalogHandle), layout));
    }

    @Override
    public BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle tableExecuteHandle, TableHandle sourceHandle)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> connectorBeginResult = metadata.beginTableExecute(session.toConnectorSession(catalogHandle), tableExecuteHandle.connectorHandle(), sourceHandle.connectorHandle());

        return new BeginTableExecuteResult<>(
                tableExecuteHandle.withConnectorHandle(connectorBeginResult.getTableExecuteHandle()),
                sourceHandle.withConnectorHandle(connectorBeginResult.getSourceHandle()));
    }

    @Override
    public void finishTableExecute(Session session, TableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        metadata.finishTableExecute(session.toConnectorSession(catalogHandle), tableExecuteHandle.connectorHandle(), fragments, tableExecuteState);
    }

    @Override
    public void executeTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        CatalogHandle catalogHandle = tableExecuteHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        metadata.executeTableExecute(session.toConnectorSession(catalogHandle), tableExecuteHandle.connectorHandle());
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, tableName.catalogName());
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
        CatalogHandle catalogHandle = handle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        return new TableProperties(catalogHandle, handle.transaction(), metadata.getTableProperties(connectorSession, handle.connectorHandle()));
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        checkArgument(partitioningHandle.getCatalogHandle().isPresent(), "Expect partitioning handle from connector, got system partitioning handle");
        CatalogHandle catalogHandle = partitioningHandle.getCatalogHandle().get();
        checkArgument(catalogHandle.equals(tableHandle.catalogHandle()), "ConnectorId of tableHandle and partitioningHandle does not match");
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogHandle);

        ConnectorTableHandle newTableHandle = metadata.makeCompatiblePartitioning(
                session.toConnectorSession(catalogHandle),
                tableHandle.connectorHandle(),
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
        CatalogHandle catalogHandle = handle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        return metadata.getInfo(handle.connectorHandle());
    }

    @Override
    public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        SchemaTableName tableName = metadata.getTableName(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());

        return new CatalogSchemaTableName(catalogMetadata.getCatalogName().toString(), tableName);
    }

    @Override
    public TableSchema getTableSchema(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTableSchema tableSchema = metadata.getTableSchema(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());

        return new TableSchema(catalogMetadata.getCatalogName(), tableSchema);
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());

        return new TableMetadata(catalogMetadata.getCatalogName(), tableMetadata);
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle)
    {
        try {
            CatalogHandle catalogHandle = tableHandle.catalogHandle();
            ConnectorMetadata metadata = getMetadata(session, catalogHandle);
            TableStatistics tableStatistics = metadata.getTableStatistics(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());
            verifyNotNull(tableStatistics, "%s returned null tableStatistics for %s", metadata, tableHandle);
            return tableStatistics;
        }
        catch (RuntimeException e) {
            if (isQueryDone(session)) {
                // getting statistics for finished query may result in many different execeptions being thrown.
                // As we do not care about the result anyway mask it by returning empty statistics.
                return TableStatistics.empty();
            }
            throw e;
        }
    }

    private boolean isQueryDone(Session session)
    {
        boolean done;
        try {
            done = queryManager.getQueryState(session.getQueryId()).isDone();
        }
        catch (NoSuchElementException ex) {
            done = true;
        }
        return done;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());

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

        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        return metadata.getColumnMetadata(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), columnHandle);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        if (cannotExist(prefix)) {
            return ImmutableList.of();
        }

        Optional<QualifiedObjectName> objectName = prefix.asQualifiedObjectName();
        if (objectName.isPresent()) {
            try {
                Optional<RelationType> relationType = getRelationTypeIfExists(session, objectName.get());
                if (relationType.isPresent()) {
                    return ImmutableList.of(objectName.get());
                }
                // TODO we can probably return empty list here
            }
            catch (RuntimeException e) {
                handleListingError(e, prefix);
                // TODO This could be potentially improved to not return empty results https://github.com/trinodb/trino/issues/6551
                return ImmutableList.of();
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
                        .filter(table -> !isExternalInformationSchema(catalogHandle, table.schemaName()))
                        .forEach(tables::add);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        if (cannotExist(prefix)) {
            return ImmutableMap.of();
        }

        Optional<QualifiedObjectName> objectName = prefix.asQualifiedObjectName();
        if (objectName.isPresent()) {
            Optional<RelationType> relationType = getRelationTypeIfExists(session, objectName.get());
            if (relationType.isPresent()) {
                return ImmutableMap.of(objectName.get().asSchemaTableName(), relationType.get());
            }
            return ImmutableMap.of();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());
        Map<SchemaTableName, RelationType> relationTypes = new LinkedHashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogHandle catalogHandle : catalogMetadata.listCatalogHandles()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
                ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                if (isExternalInformationSchema(catalogHandle, prefix.getSchemaName())) {
                    continue;
                }
                metadata.getRelationTypes(connectorSession, prefix.getSchemaName()).entrySet().stream()
                        .filter(entry -> !isExternalInformationSchema(catalogHandle, entry.getKey().getSchemaName()))
                        .forEach(entry -> relationTypes.put(entry.getKey(), entry.getValue()));
            }
        }
        return ImmutableMap.copyOf(relationTypes);
    }

    private Optional<RelationType> getRelationTypeIfExists(Session session, QualifiedObjectName name)
    {
        if (isMaterializedView(session, name)) {
            return Optional.of(RelationType.MATERIALIZED_VIEW);
        }
        if (isView(session, name)) {
            return Optional.of(RelationType.VIEW);
        }

        // TODO: consider a better way to resolve relation names: https://github.com/trinodb/trino/issues/9400
        try {
            if (getRedirectionAwareTableHandle(session, name).tableHandle().isPresent()) {
                return Optional.of(RelationType.TABLE);
            }
            return Optional.empty();
        }
        catch (TrinoException e) {
            // ignore redirection errors for consistency with listing
            if (e.getErrorCode().equals(TABLE_REDIRECTION_ERROR.toErrorCode())) {
                return Optional.of(RelationType.TABLE);
            }
            return Optional.empty();
        }
    }

    @Override
    public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        requireNonNull(prefix, "prefix is null");
        if (cannotExist(prefix)) {
            return ImmutableList.of();
        }

        String catalogName = prefix.getCatalogName();
        Optional<String> schemaName = prefix.getSchemaName();
        Optional<String> relationName = prefix.getTableName();

        if (relationName.isPresent()) {
            QualifiedObjectName objectName = new QualifiedObjectName(catalogName, schemaName.orElseThrow(), relationName.get());
            SchemaTableName schemaTableName = objectName.asSchemaTableName();

            try {
                return Optional.<RelationColumnsMetadata>empty()
                        .or(() -> getMaterializedViewInternal(session, objectName)
                                .map(materializedView -> RelationColumnsMetadata.forMaterializedView(schemaTableName, materializedView.getColumns())))
                        .or(() -> getViewInternal(session, objectName)
                                .map(view -> RelationColumnsMetadata.forView(schemaTableName, view.getColumns())))
                        .or(() -> {
                            // TODO: redirects are handled inefficiently: we currently throw-away redirect info and redo it later
                            RedirectionAwareTableHandle redirectionAware = getRedirectionAwareTableHandle(session, objectName);
                            if (redirectionAware.redirectedTableName().isPresent()) {
                                return Optional.of(RelationColumnsMetadata.forRedirectedTable(schemaTableName));
                            }
                            if (redirectionAware.tableHandle().isPresent()) {
                                return Optional.of(RelationColumnsMetadata.forTable(schemaTableName, getTableMetadata(session, redirectionAware.tableHandle().get()).columns()));
                            }
                            // Not found
                            return Optional.empty();
                        })
                        .filter(relationColumnsMetadata -> relationFilter.apply(ImmutableSet.of(relationColumnsMetadata.name())).contains(relationColumnsMetadata.name()))
                        .map(relationColumnsMetadata -> ImmutableList.of(tableColumnsMetadata(catalogName, relationColumnsMetadata)))
                        .orElse(ImmutableList.of());
            }
            catch (RuntimeException e) {
                handleListingError(e, prefix);
                // Empty in case of metadata error.
                return ImmutableList.of();
            }
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

    private static void handleListingError(RuntimeException e, QualifiedTablePrefix tablePrefix)
    {
        boolean silent = false;
        if (e instanceof TrinoException trinoException) {
            ErrorCode errorCode = trinoException.getErrorCode();
            silent = errorCode.equals(UNSUPPORTED_TABLE_TYPE.toErrorCode()) ||
                    // e.g. table deleted concurrently
                    errorCode.equals(TABLE_NOT_FOUND.toErrorCode()) ||
                    errorCode.equals(NOT_FOUND.toErrorCode()) ||
                    // e.g. Iceberg/Delta table being deleted concurrently resulting in failure to load metadata from filesystem
                    errorCode.getType() == EXTERNAL;
        }
        if (silent) {
            log.debug(e, "Failed to get metadata for table: %s", tablePrefix);
        }
        else {
            log.warn(e, "Failed to get metadata for table: %s", tablePrefix);
        }
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
        if (cannotExist(new QualifiedTablePrefix(catalogName, schemaName, Optional.empty()))) {
            return ImmutableList.of();
        }

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
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.createTable(session.toConnectorSession(catalogHandle), tableMetadata, saveMode);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, new CatalogSchemaTableName(catalogName, tableMetadata.getTable()));
        }
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName sourceTableName, QualifiedObjectName newTableName)
    {
        String catalogName = newTableName.catalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        if (!tableHandle.catalogHandle().equals(catalogHandle)) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.renameTable(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), newTableName.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() != CONNECTOR) {
            systemSecurityMetadata.tableRenamed(session, sourceTableName, newTableName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setTableProperties(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), properties);
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setTableComment(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), comment);
    }

    @Override
    public void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.setViewComment(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), comment);
    }

    @Override
    public void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.setViewColumnComment(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), columnName, comment);
    }

    @Override
    public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setColumnComment(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), column, comment);
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle.getCatalogName().toString());
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.renameColumn(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), source, target.toLowerCase(ENGLISH));
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, source);
            systemSecurityMetadata.columnRenamed(session, table, columnMetadata.getName(), target);
        }
    }

    @Override
    public void renameField(Session session, TableHandle tableHandle, List<String> fieldPath, String target)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.renameField(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), fieldPath, target.toLowerCase(ENGLISH));
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle.getCatalogName().toString());
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.addColumn(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), column);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.columnCreated(session, table, column.getName());
        }
    }

    @Override
    public void addField(Session session, TableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.addField(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), parentPath, fieldName, type, ignoreExisting);
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle column)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle.getCatalogName().toString());
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.dropColumn(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), column);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, column);
            systemSecurityMetadata.columnDropped(session, table, columnMetadata.getName());
        }
    }

    @Override
    public void dropField(Session session, TableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.dropField(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), column, fieldPath);
    }

    @Override
    public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle column, Type type)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle.getCatalogName().toString());
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, column);
        metadata.setColumnType(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), column, type);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.columnTypeChanged(
                    session,
                    getTableName(session, tableHandle),
                    columnMetadata.getName(),
                    columnMetadata.getType().getDisplayName(),
                    type.getDisplayName());
        }
    }

    @Override
    public void setFieldType(Session session, TableHandle tableHandle, List<String> fieldPath, Type type)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.setFieldType(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), fieldPath, type);
    }

    @Override
    public void dropNotNullConstraint(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ColumnMetadata columnMetadata = getColumnMetadata(session, tableHandle, column);

        metadata.dropNotNullConstraint(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), column);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.columnNotNullConstraintDropped(
                    session,
                    getTableName(session, tableHandle),
                    columnMetadata.getName());
        }
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
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        metadata.dropTable(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());
        if (catalogMetadata.getSecurityManagement() != CONNECTOR) {
            systemSecurityMetadata.tableDropped(session, tableName);
        }
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        metadata.truncateTable(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());
    }

    @Override
    public Optional<TableLayout> getInsertLayout(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        return metadata.getInsertLayout(session.toConnectorSession(catalogHandle), table.connectorHandle())
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
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorAnalyzeMetadata analyze = metadata.getStatisticsCollectionMetadata(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), analyzeProperties);
        return new AnalyzeMetadata(analyze.getStatisticsMetadata(), new TableHandle(catalogHandle, analyze.getTableHandle(), tableHandle.transaction()));
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorTableHandle connectorTableHandle = metadata.beginStatisticsCollection(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());
        return new AnalyzeTableHandle(catalogHandle, transactionHandle, connectorTableHandle);
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        catalogMetadata.getMetadata(session).finishStatisticsCollection(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), computedStatistics);
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
    public Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return metadata.getSupportedType(session.toConnectorSession(catalogHandle), tableProperties, type)
                .map(newType -> {
                    if (!typeCoercion.isCompatible(newType, type)) {
                        throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, format("Type '%s' is not compatible with the supplied type '%s' in getSupportedType", type, newType));
                    }
                    return newType;
                });
    }

    @Override
    public void beginQuery(Session session)
    {
        languageFunctionManager.registerQuery(session);
    }

    @Override
    public void cleanupQuery(Session session)
    {
        QueryCatalogs queryCatalogs = catalogsByQueryId.remove(session.getQueryId());
        if (queryCatalogs != null) {
            queryCatalogs.finish();
        }
        languageFunctionManager.unregisterQuery(session);
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout, boolean replace)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata, layout.map(TableLayout::getLayout), getRetryPolicy(session).getRetryMode(), replace);
        return new OutputTableHandle(catalogHandle, tableMetadata.getTable(), transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        Optional<ConnectorOutputMetadata> output = metadata.finishCreateTable(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), fragments, computedStatistics);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, new CatalogSchemaTableName(catalogHandle.getCatalogName().toString(), tableHandle.tableName()));
        }
        return output;
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorInsertTableHandle handle = metadata.beginInsert(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), columns, getRetryPolicy(session).getRetryMode());
        return new InsertTableHandle(tableHandle.catalogHandle(), transactionHandle, handle);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        return catalogMetadata.getMetadata(session).supportsMissingColumnsOnInsert();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .filter(handle -> handle.catalogHandle().equals(catalogHandle))
                .map(TableHandle::connectorHandle)
                .collect(toImmutableList());
        return metadata.finishInsert(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), sourceConnectorHandles, fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return metadata.delegateMaterializedViewRefreshToConnector(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName());
    }

    @Override
    public ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        return asVoid(toListenableFuture(metadata.refreshMaterializedView(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName())));
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles, RefreshType refreshType)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);

        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .map(TableHandle::connectorHandle)
                .collect(Collectors.toList());

        ConnectorInsertTableHandle handle = metadata.beginRefreshMaterializedView(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle(), sourceConnectorHandles, getRetryPolicy(session).getRetryMode(), refreshType);

        return new InsertTableHandle(tableHandle.catalogHandle(), transactionHandle, handle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            Session session,
            TableHandle tableHandle,
            InsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<TableHandle> sourceTableHandles,
            List<String> sourceTableFunctions)
    {
        CatalogHandle catalogHandle = insertHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .map(TableHandle::connectorHandle)
                .collect(toImmutableList());
        return metadata.finishRefreshMaterializedView(
                session.toConnectorSession(catalogHandle),
                tableHandle.connectorHandle(),
                insertHandle.connectorHandle(),
                fragments,
                computedStatistics,
                sourceConnectorHandles,
                sourceTableFunctions);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        return metadata.getMergeRowIdColumnHandle(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());
    }

    @Override
    public Optional<PartitioningHandle> getUpdateLayout(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogHandle);

        return metadata.getUpdateLayout(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle())
                .map(partitioning -> new PartitioningHandle(Optional.of(catalogHandle), Optional.of(transactionHandle), partitioning));
    }

    @Override
    public Optional<TableHandle> applyUpdate(Session session, TableHandle table, Map<ColumnHandle, Constant> assignments)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyUpdate(connectorSession, table.connectorHandle(), assignments)
                .map(newHandle -> new TableHandle(catalogHandle, newHandle, table.transaction()));
    }

    @Override
    public OptionalLong executeUpdate(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        return metadata.executeUpdate(connectorSession, table.connectorHandle());
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyDelete(connectorSession, table.connectorHandle())
                .map(newHandle -> new TableHandle(catalogHandle, newHandle, table.transaction()));
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        return metadata.executeDelete(connectorSession, table.connectorHandle());
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        return metadata.getRowChangeParadigm(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle());
    }

    @Override
    public MergeHandle beginMerge(Session session, TableHandle tableHandle, Multimap<Integer, ColumnHandle> updateCaseColumns)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogHandle);
        ConnectorMergeTableHandle newHandle = metadata.beginMerge(
                session.toConnectorSession(catalogHandle),
                tableHandle.connectorHandle(),
                updateCaseColumns.asMap(),
                getRetryPolicy(session).getRetryMode());
        return new MergeHandle(tableHandle.withConnectorHandle(newHandle.getTableHandle()), newHandle);
    }

    @Override
    public void finishMerge(Session session, MergeHandle mergeHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogHandle catalogHandle = mergeHandle.tableHandle().catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        List<ConnectorTableHandle> sourceConnectorHandles = sourceTableHandles.stream()
                .filter(handle -> handle.catalogHandle().equals(catalogHandle))
                .map(TableHandle::connectorHandle)
                .collect(toImmutableList());
        metadata.finishMerge(session.toConnectorSession(catalogHandle), mergeHandle.connectorMergeHandle(), sourceConnectorHandles, fragments, computedStatistics);
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
        if (cannotExist(prefix)) {
            return ImmutableList.of();
        }

        Optional<QualifiedObjectName> objectName = prefix.asQualifiedObjectName();
        if (objectName.isPresent()) {
            return isView(session, objectName.get())
                    ? ImmutableList.of(objectName.get())
                    : ImmutableList.of();
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
                        .filter(view -> !isExternalInformationSchema(catalogHandle, view.schemaName()))
                        .forEach(views::add);
            }
        }
        return ImmutableList.copyOf(views);
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        if (cannotExist(prefix)) {
            return ImmutableMap.of();
        }

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
                    try {
                        viewMap = metadata.getView(connectorSession, tablePrefix.toSchemaTableName())
                                .map(view -> ImmutableMap.of(tablePrefix.toSchemaTableName(), view))
                                .orElse(ImmutableMap.of());
                    }
                    catch (RuntimeException e) {
                        handleListingError(e, prefix);
                        // Empty in case of metadata error.
                        viewMap = ImmutableMap.of();
                    }
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
        if (cannotExist(viewName)) {
            return false;
        }

        return getOptionalCatalogMetadata(session, viewName.catalogName())
                .map(catalog -> {
                    CatalogHandle catalogHandle = catalog.getCatalogHandle(session, viewName, Optional.empty(), Optional.empty());
                    ConnectorMetadata metadata = catalog.getMetadataFor(session, catalogHandle);
                    ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
                    return metadata.isView(connectorSession, viewName.asSchemaTableName());
                })
                .orElse(false);
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        Optional<ConnectorViewDefinition> connectorView = getViewInternal(session, viewName);
        if (connectorView.isEmpty() || connectorView.get().isRunAsInvoker() || isCatalogManagedSecurity(session, viewName.catalogName())) {
            return connectorView.map(view -> createViewDefinition(viewName, view, view.getOwner().map(Identity::ofUser)));
        }

        Identity runAsIdentity = systemSecurityMetadata.getViewRunAsIdentity(session, viewName.asCatalogSchemaTableName())
                .or(() -> connectorView.get().getOwner().map(Identity::ofUser))
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Catalog does not support run-as DEFINER views: " + viewName));
        return Optional.of(createViewDefinition(viewName, connectorView.get(), Optional.of(runAsIdentity)));
    }

    @Override
    public Map<String, Object> getViewProperties(Session session, QualifiedObjectName viewName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.catalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName, Optional.empty(), Optional.empty());
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return ImmutableMap.copyOf(metadata.getViewProperties(
                    connectorSession,
                    viewName.asSchemaTableName()));
        }
        return ImmutableMap.of();
    }

    private static ViewDefinition createViewDefinition(QualifiedObjectName viewName, ConnectorViewDefinition view, Optional<Identity> runAsIdentity)
    {
        if (view.isRunAsInvoker() && runAsIdentity.isPresent()) {
            throw new TrinoException(INVALID_VIEW, "Run-as identity cannot be set for a run-as invoker view: " + viewName);
        }
        if (!view.isRunAsInvoker() && runAsIdentity.isEmpty()) {
            throw new TrinoException(INVALID_VIEW, "Run-as identity must be set for a run-as definer view: " + viewName);
        }

        return new ViewDefinition(
                view.getOriginalSql(),
                view.getCatalog(),
                view.getSchema(),
                view.getColumns().stream()
                        .map(column -> new ViewColumn(column.getName(), column.getType(), column.getComment()))
                        .collect(toImmutableList()),
                view.getComment(),
                runAsIdentity,
                view.getPath());
    }

    private Optional<ConnectorViewDefinition> getViewInternal(Session session, QualifiedObjectName viewName)
    {
        if (cannotExist(viewName)) {
            return Optional.empty();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.catalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName, Optional.empty(), Optional.empty());
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return metadata.getView(connectorSession, viewName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.createView(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), definition.toConnectorViewDefinition(), viewProperties, replace);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void renameView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, target.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (!source.catalogName().equals(target.catalogName())) {
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
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.dropView(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName());
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableDropped(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void createMaterializedView(
            Session session,
            QualifiedObjectName viewName,
            MaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.createMaterializedView(
                session.toConnectorSession(catalogHandle),
                viewName.asSchemaTableName(),
                definition.toConnectorMaterializedViewDefinition(),
                properties,
                replace,
                ignoreExisting);
        if (catalogMetadata.getSecurityManagement() == SYSTEM) {
            systemSecurityMetadata.tableCreated(session, viewName.asCatalogSchemaTableName());
        }
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
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
                        .filter(materializedView -> !isExternalInformationSchema(catalogHandle, materializedView.schemaName()))
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
        if (connectorView.isEmpty()) {
            return Optional.empty();
        }

        if (isCatalogManagedSecurity(session, viewName.catalogName())) {
            String runAsUser = connectorView.get().getOwner().orElseThrow(() -> new TrinoException(INVALID_VIEW, "Owner not set for a run-as invoker view: " + viewName));
            return Optional.of(createMaterializedViewDefinition(connectorView.get(), Identity.ofUser(runAsUser)));
        }

        Identity runAsIdentity = systemSecurityMetadata.getViewRunAsIdentity(session, viewName.asCatalogSchemaTableName())
                .or(() -> connectorView.get().getOwner().map(Identity::ofUser))
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Materialized view does not have an owner: " + viewName));
        return Optional.of(createMaterializedViewDefinition(connectorView.get(), runAsIdentity));
    }

    private static MaterializedViewDefinition createMaterializedViewDefinition(ConnectorMaterializedViewDefinition view, Identity runAsIdentity)
    {
        return new MaterializedViewDefinition(
                view.getOriginalSql(),
                view.getCatalog(),
                view.getSchema(),
                view.getColumns().stream()
                        .map(column -> new ViewColumn(column.getName(), column.getType(), Optional.empty()))
                        .collect(toImmutableList()),
                view.getGracePeriod(),
                view.getComment(),
                runAsIdentity,
                view.getPath(),
                view.getStorageTable());
    }

    private Optional<ConnectorMaterializedViewDefinition> getMaterializedViewInternal(Session session, QualifiedObjectName viewName)
    {
        if (cannotExist(viewName)) {
            return Optional.empty();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.catalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName, Optional.empty(), Optional.empty());
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return metadata.getMaterializedView(connectorSession, viewName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(Session session, QualifiedObjectName viewName, MaterializedViewDefinition materializedViewDefinition)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.catalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName, Optional.empty(), Optional.empty());
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return ImmutableMap.copyOf(metadata.getMaterializedViewProperties(
                    connectorSession,
                    viewName.asSchemaTableName(),
                    materializedViewDefinition.toConnectorMaterializedViewDefinition()));
        }
        return ImmutableMap.of();
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName viewName)
    {
        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.catalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, viewName, Optional.empty(), Optional.empty());
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);

            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            return metadata.getMaterializedViewFreshness(connectorSession, viewName.asSchemaTableName());
        }
        return new MaterializedViewFreshness(STALE, Optional.empty());
    }

    @Override
    public void renameMaterializedView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, target.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
        if (!source.catalogName().equals(target.catalogName())) {
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
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.setMaterializedViewProperties(session.toConnectorSession(catalogHandle), viewName.asSchemaTableName(), properties);
    }

    @Override
    public void setMaterializedViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.catalogName());
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
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyTableScanRedirect(connectorSession, tableHandle.connectorHandle());
    }

    private QualifiedObjectName getRedirectedTableName(Session session, QualifiedObjectName originalTableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        requireNonNull(session, "session is null");
        requireNonNull(originalTableName, "originalTableName is null");
        if (cannotExist(originalTableName)) {
            return originalTableName;
        }

        QualifiedObjectName tableName = originalTableName;
        Set<QualifiedObjectName> visitedTableNames = new LinkedHashSet<>();
        visitedTableNames.add(tableName);

        for (int count = 0; count < MAX_TABLE_REDIRECTIONS; count++) {
            Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, tableName.catalogName());

            if (catalog.isEmpty()) {
                // Stop redirection
                return tableName;
            }

            CatalogMetadata catalogMetadata = catalog.get();
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, tableName, toConnectorVersion(startVersion), toConnectorVersion(endVersion));
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
        QualifiedObjectName targetTableName = getRedirectedTableName(session, tableName, startVersion, endVersion);
        if (targetTableName.equals(tableName)) {
            return noRedirection(getTableHandle(session, tableName, startVersion, endVersion));
        }

        Optional<TableHandle> tableHandle = getTableHandle(session, targetTableName, startVersion, endVersion);
        if (tableHandle.isPresent()) {
            return withRedirectionTo(targetTableName, tableHandle.get());
        }

        // Redirected table must exist
        if (getCatalogHandle(session, targetTableName.catalogName()).isEmpty()) {
            throw new TrinoException(TABLE_REDIRECTION_ERROR, format("Table '%s' redirected to '%s', but the target catalog '%s' does not exist", tableName, targetTableName, targetTableName.catalogName()));
        }
        if (!schemaExists(session, new CatalogSchemaName(targetTableName.catalogName(), targetTableName.schemaName()))) {
            throw new TrinoException(TABLE_REDIRECTION_ERROR, format("Table '%s' redirected to '%s', but the target schema '%s' does not exist", tableName, targetTableName, targetTableName.schemaName()));
        }
        throw new TrinoException(TABLE_REDIRECTION_ERROR, format("Table '%s' redirected to '%s', but the target table '%s' does not exist", tableName, targetTableName, targetTableName));
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.connectorHandle(), indexableColumns, outputColumns, tupleDomain);
        return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.catalogHandle(), transaction, resolved));
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyLimit(connectorSession, table.connectorHandle(), limit)
                .map(result -> new LimitApplicationResult<>(
                        new TableHandle(catalogHandle, result.getHandle(), table.transaction()),
                        result.isLimitGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applySample(connectorSession, table.connectorHandle(), sampleType, sampleRatio)
                .map(result -> new SampleApplicationResult<>(new TableHandle(
                        catalogHandle,
                        result.getHandle(),
                        table.transaction()),
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

        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyAggregation(connectorSession, table.connectorHandle(), aggregations, assignments, groupingSets)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), aggregations.size());

                    return new AggregationApplicationResult<>(
                            new TableHandle(catalogHandle, result.getHandle(), table.transaction()),
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
        if (!right.catalogHandle().equals(left.catalogHandle())) {
            // Exact comparison is fine as catalog name here is passed from CatalogMetadata and is normalized to lowercase
            return Optional.empty();
        }
        CatalogHandle catalogHandle = left.catalogHandle();

        ConnectorTransactionHandle transaction = left.transaction();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);

        Optional<JoinApplicationResult<ConnectorTableHandle>> connectorResult =
                metadata.applyJoin(
                        connectorSession,
                        joinType,
                        left.connectorHandle(),
                        right.connectorHandle(),
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
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyTopN(connectorSession, table.connectorHandle(), topNCount, sortItems, assignments)
                .map(result -> new TopNApplicationResult<>(
                        new TableHandle(catalogHandle, result.getHandle(), table.transaction()),
                        result.isTopNGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle)
    {
        CatalogHandle catalogHandle = handle.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        return metadata.applyTableFunction(session.toConnectorSession(catalogHandle), handle.functionHandle())
                .map(result -> new TableFunctionApplicationResult<>(
                        new TableHandle(catalogHandle, result.getTableHandle(), handle.transactionHandle()),
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
                .flatMap(connectorExpression -> extractVariables(connectorExpression).stream())
                .map(Variable::getName)
                .filter(variableName -> !assignedVariables.contains(variableName))
                .findAny()
                .ifPresent(variableName -> { throw new IllegalStateException("Unbound variable: " + variableName); });
    }

    @Override
    public void validateScan(Session session, TableHandle table)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);
        metadata.validateScan(session.toConnectorSession(catalogHandle), table.connectorHandle());
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyFilter(connectorSession, table.connectorHandle(), constraint)
                .map(result -> result.transform(handle -> new TableHandle(catalogHandle, handle, table.transaction())));
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        CatalogHandle catalogHandle = table.catalogHandle();
        ConnectorMetadata metadata = getMetadata(session, catalogHandle);

        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return metadata.applyProjection(connectorSession, table.connectorHandle(), projections, assignments)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), projections.size());

                    return new ProjectionApplicationResult<>(
                            new TableHandle(catalogHandle, result.getHandle(), table.transaction()),
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
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.catalogName());
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
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.catalogName());
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
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.catalogName());
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
                    .map(qualifiedTableName -> singletonList(catalogMetadata.getCatalogHandle(session, qualifiedTableName, Optional.empty(), Optional.empty())))
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

    @Override
    public Set<EntityPrivilege> getAllEntityKindPrivileges(String entityKind)
    {
        requireNonNull(entityKind, "entityKind is null");
        return systemSecurityMetadata.getAllEntityKindPrivileges(entityKind);
    }

    @Override
    public void grantEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        systemSecurityMetadata.grantEntityPrivileges(session, entity, privileges, grantee, grantOption);
    }

    @Override
    public void denyEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee)
    {
        systemSecurityMetadata.denyEntityPrivileges(session, entity, privileges, grantee);
    }

    @Override
    public void revokeEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        systemSecurityMetadata.revokeEntityPrivileges(session, entity, privileges, grantee, grantOption);
    }

    //
    // Functions
    //

    @Override
    public Collection<FunctionMetadata> listGlobalFunctions(Session session)
    {
        return ImmutableList.<FunctionMetadata>builder()
                .addAll(functions.listFunctions())
                .addAll(listTableFunctions(session))
                .build();
    }

    private Collection<FunctionMetadata> listTableFunctions(Session session)
    {
        ImmutableList.Builder<FunctionMetadata> functions = ImmutableList.builder();
        for (CatalogInfo catalog : listCatalogs(session)) {
            functions.addAll(tableFunctionRegistry.listTableFunctions(catalog.catalogHandle()));
        }
        return functions.build();
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(Session session, CatalogSchemaName schema)
    {
        ImmutableList.Builder<FunctionMetadata> functions = ImmutableList.builder();
        getOptionalCatalogMetadata(session, schema.getCatalogName()).ifPresent(catalogMetadata -> {
            CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
            functions.addAll(metadata.listFunctions(connectorSession, schema.getSchemaName()));
            functions.addAll(languageFunctionManager.listFunctions(session, metadata.listLanguageFunctions(connectorSession, schema.getSchemaName())));
            functions.addAll(tableFunctionRegistry.listTableFunctions(catalogHandle, schema.getSchemaName()));
        });
        return functions.build();
    }

    @Override
    public ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        return functionResolver.resolveBuiltinFunction(name, parameterTypes);
    }

    @Override
    public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return functionResolver.resolveOperator(operatorType, argumentTypes);
    }

    @Override
    public ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType)
    {
        return functionResolver.resolveCoercion(operatorType, fromType, toType);
    }

    @Override
    public ResolvedFunction getCoercion(CatalogSchemaFunctionName name, Type fromType, Type toType)
    {
        // coercion can only be resolved for builtin functions
        if (!isBuiltinFunctionName(name)) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", name));
        }

        return functionResolver.resolveCoercion(name.getFunctionName(), fromType, toType);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(Session session, CatalogHandle catalogHandle, FunctionId functionId, BoundSignature boundSignature)
    {
        if (isTrinoSqlLanguageFunction(functionId)) {
            throw new IllegalArgumentException("Function dependencies for SQL functions must be fetched directly from the language manager");
        }
        if (catalogHandle.equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            return functions.getFunctionDependencies(functionId, boundSignature);
        }
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return getMetadata(session, catalogHandle)
                .getFunctionDependencies(connectorSession, functionId, boundSignature);
    }

    @Override
    public Collection<CatalogFunctionMetadata> getFunctions(Session session, CatalogSchemaFunctionName name)
    {
        if (isBuiltinFunctionName(name)) {
            return getBuiltinFunctions(name.getFunctionName());
        }

        return getOptionalCatalogMetadata(session, name.getCatalogName())
                .map(metadata -> getFunctions(session, metadata.getMetadata(session), metadata.getCatalogHandle(), name.getSchemaFunctionName()))
                .orElse(ImmutableList.of());
    }

    private Collection<CatalogFunctionMetadata> getBuiltinFunctions(String functionName)
    {
        return functions.getBuiltInFunctions(functionName).stream()
                .map(function -> new CatalogFunctionMetadata(GlobalSystemConnector.CATALOG_HANDLE, BUILTIN_SCHEMA, function))
                .collect(toImmutableList());
    }

    private List<CatalogFunctionMetadata> getFunctions(Session session, ConnectorMetadata metadata, CatalogHandle catalogHandle, SchemaFunctionName name)
    {
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        ImmutableList.Builder<CatalogFunctionMetadata> functions = ImmutableList.builder();

        metadata.getFunctions(connectorSession, name).stream()
                .map(function -> new CatalogFunctionMetadata(catalogHandle, name.getSchemaName(), function))
                .forEach(functions::add);

        RunAsIdentityLoader identityLoader = owner -> {
            CatalogSchemaFunctionName functionName = new CatalogSchemaFunctionName(catalogHandle.getCatalogName().toString(), name);

            Optional<Identity> systemIdentity = Optional.empty();
            if (getCatalogMetadata(session, catalogHandle).getSecurityManagement() == SYSTEM) {
                systemIdentity = systemSecurityMetadata.getFunctionRunAsIdentity(session, functionName);
            }

            return systemIdentity.or(() -> owner.map(Identity::ofUser))
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "No identity for SECURITY DEFINER function: " + functionName));
        };

        languageFunctionManager.getFunctions(session, catalogHandle, name, metadata::getLanguageFunctions, identityLoader).stream()
                .map(function -> new CatalogFunctionMetadata(catalogHandle, name.getSchemaName(), function))
                .forEach(functions::add);

        return functions.build();
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
    {
        Signature functionSignature;
        AggregationFunctionMetadata aggregationFunctionMetadata;
        if (resolvedFunction.catalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            functionSignature = functions.getFunctionMetadata(resolvedFunction.functionId()).getSignature();
            aggregationFunctionMetadata = functions.getAggregationFunctionMetadata(resolvedFunction.functionId());
        }
        else {
            ConnectorSession connectorSession = session.toConnectorSession(resolvedFunction.catalogHandle());
            ConnectorMetadata metadata = getMetadata(session, resolvedFunction.catalogHandle());
            functionSignature = metadata.getFunctionMetadata(connectorSession, resolvedFunction.functionId()).getSignature();
            aggregationFunctionMetadata = metadata.getAggregationFunctionMetadata(connectorSession, resolvedFunction.functionId());
        }

        AggregationFunctionMetadataBuilder builder = AggregationFunctionMetadata.builder();
        if (aggregationFunctionMetadata.isOrderSensitive()) {
            builder.orderSensitive();
        }

        if (!aggregationFunctionMetadata.getIntermediateTypes().isEmpty()) {
            FunctionBinding functionBinding = toFunctionBinding(resolvedFunction.functionId(), resolvedFunction.signature(), functionSignature);
            aggregationFunctionMetadata.getIntermediateTypes().stream()
                    .map(typeSignature -> applyBoundVariables(typeSignature, functionBinding))
                    .forEach(builder::intermediateType);
        }

        return builder.build();
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(Session session, QualifiedObjectName name)
    {
        CatalogMetadata catalogMetadata = getRequiredCatalogMetadata(session, name.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        return metadata.getLanguageFunctions(session.toConnectorSession(catalogHandle), name.asSchemaFunctionName());
    }

    @Override
    public boolean languageFunctionExists(Session session, QualifiedObjectName name, String signatureToken)
    {
        return getOptionalCatalogMetadata(session, name.catalogName())
                .map(catalogMetadata -> {
                    ConnectorMetadata metadata = catalogMetadata.getMetadata(session);
                    ConnectorSession connectorSession = session.toConnectorSession(catalogMetadata.getCatalogHandle());
                    return metadata.languageFunctionExists(connectorSession, name.asSchemaFunctionName(), signatureToken);
                })
                .orElse(false);
    }

    @Override
    public void createLanguageFunction(Session session, QualifiedObjectName name, LanguageFunction function, boolean replace)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, name.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.createLanguageFunction(session.toConnectorSession(catalogHandle), name.asSchemaFunctionName(), function, replace);
    }

    @Override
    public void dropLanguageFunction(Session session, QualifiedObjectName name, String signatureToken)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, name.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle();
        ConnectorMetadata metadata = catalogMetadata.getMetadata(session);

        metadata.dropLanguageFunction(session.toConnectorSession(catalogHandle), name.asSchemaFunctionName(), signatureToken);
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

        private void finish()
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
    public boolean allowSplittingReadIntoMultipleSubQueries(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        if (catalogHandle.getType().isInternal()) {
            return false;
        }
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogHandle);
        ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
        return catalogMetadata.getMetadata(session).allowSplittingReadIntoMultipleSubQueries(connectorSession, tableHandle.connectorHandle());
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(Session session, QualifiedObjectName tableName, Map<String, Object> tableProperties)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.catalogName());
        CatalogHandle catalogHandle = catalogMetadata.getCatalogHandle(session, tableName, Optional.empty(), Optional.empty());
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(session, catalogHandle);
        return metadata.getNewTableWriterScalingOptions(session.toConnectorSession(catalogHandle), tableName.asSchemaTableName(), tableProperties);
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(Session session, TableHandle tableHandle)
    {
        ConnectorMetadata metadata = getMetadataForWrite(session, tableHandle.catalogHandle());
        return metadata.getInsertWriterScalingOptions(session.toConnectorSession(tableHandle.catalogHandle()), tableHandle.connectorHandle());
    }

    private Optional<ConnectorTableVersion> toConnectorVersion(Optional<TableVersion> version)
    {
        Optional<ConnectorTableVersion> connectorVersion = Optional.empty();
        if (version.isPresent()) {
            connectorVersion = Optional.of(new ConnectorTableVersion(version.get().pointerType(), version.get().objectType(), version.get().pointer()));
        }
        return connectorVersion;
    }

    private static boolean cannotExist(QualifiedTablePrefix prefix)
    {
        return prefix.getCatalogName().isEmpty() ||
                (prefix.getSchemaName().isPresent() && prefix.getSchemaName().get().isEmpty()) ||
                (prefix.getTableName().isPresent() && prefix.getTableName().get().isEmpty());
    }

    private static boolean cannotExist(QualifiedObjectName name)
    {
        return name.catalogName().isEmpty() || name.schemaName().isEmpty() || name.objectName().isEmpty();
    }
}
