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
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogName;
import io.trino.metadata.ResolvedFunction.ResolvedFunctionDecoder;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.operator.window.WindowFunctionSupplier;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockEncoding;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.ByteArrayBlockEncoding;
import io.trino.spi.block.DictionaryBlockEncoding;
import io.trino.spi.block.Int128ArrayBlockEncoding;
import io.trino.spi.block.Int96ArrayBlockEncoding;
import io.trino.spi.block.IntArrayBlockEncoding;
import io.trino.spi.block.LazyBlockEncoding;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.block.MapBlockEncoding;
import io.trino.spi.block.RowBlockEncoding;
import io.trino.spi.block.RunLengthBlockEncoding;
import io.trino.spi.block.ShortArrayBlockEncoding;
import io.trino.spi.block.SingleMapBlockEncoding;
import io.trino.spi.block.SingleRowBlockEncoding;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTableLayoutResult;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
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
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.ConnectorExpressions;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.tree.QualifiedName;
import io.trino.transaction.TransactionManager;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import static com.google.common.primitives.Primitives.wrap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.metadata.FunctionKind.AGGREGATE;
import static io.trino.metadata.QualifiedObjectName.convertFromSchemaTableName;
import static io.trino.metadata.RedirectionAwareTableHandle.noRedirection;
import static io.trino.metadata.RedirectionAwareTableHandle.withRedirectionTo;
import static io.trino.metadata.Signature.mangleOperatorName;
import static io.trino.metadata.SignatureBinder.applyBoundVariables;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;
import static io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.COMPARISON;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
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

    private final FunctionRegistry functions;
    private final TypeOperators typeOperators;
    private final FunctionResolver functionResolver;
    private final ProcedureRegistry procedures;
    private final SessionPropertyManager sessionPropertyManager;
    private final SchemaPropertyManager schemaPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;
    private final ColumnPropertyManager columnPropertyManager;
    private final AnalyzePropertyManager analyzePropertyManager;
    private final TransactionManager transactionManager;
    private final TypeRegistry typeRegistry;

    private final ConcurrentMap<String, BlockEncoding> blockEncodings = new ConcurrentHashMap<>();
    private final ConcurrentMap<QueryId, QueryCatalogs> catalogsByQueryId = new ConcurrentHashMap<>();

    private final ResolvedFunctionDecoder functionDecoder;

    private final LoadingCache<OperatorCacheKey, ResolvedFunction> operatorCache;
    private final LoadingCache<CoercionCacheKey, ResolvedFunction> coercionCache;

    @Inject
    public MetadataManager(
            FeaturesConfig featuresConfig,
            SessionPropertyManager sessionPropertyManager,
            SchemaPropertyManager schemaPropertyManager,
            TablePropertyManager tablePropertyManager,
            MaterializedViewPropertyManager materializedViewPropertyManager,
            ColumnPropertyManager columnPropertyManager,
            AnalyzePropertyManager analyzePropertyManager,
            TransactionManager transactionManager,
            TypeOperators typeOperators,
            BlockTypeOperators blockTypeOperators,
            NodeVersion nodeVersion)
    {
        requireNonNull(nodeVersion, "nodeVersion is null");
        typeRegistry = new TypeRegistry(featuresConfig);
        functions = new FunctionRegistry(this::getBlockEncodingSerde, featuresConfig, typeOperators, blockTypeOperators, nodeVersion.getVersion());
        functionResolver = new FunctionResolver(this);

        this.procedures = new ProcedureRegistry();
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.schemaPropertyManager = requireNonNull(schemaPropertyManager, "schemaPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.materializedViewPropertyManager = requireNonNull(materializedViewPropertyManager, "materializedViewPropertyManager is null");
        this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
        this.analyzePropertyManager = requireNonNull(analyzePropertyManager, "analyzePropertyManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");

        // add the built-in BlockEncodings
        addBlockEncoding(new VariableWidthBlockEncoding());
        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new IntArrayBlockEncoding());
        addBlockEncoding(new LongArrayBlockEncoding());
        addBlockEncoding(new Int96ArrayBlockEncoding());
        addBlockEncoding(new Int128ArrayBlockEncoding());
        addBlockEncoding(new DictionaryBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new MapBlockEncoding());
        addBlockEncoding(new SingleMapBlockEncoding());
        addBlockEncoding(new RowBlockEncoding());
        addBlockEncoding(new SingleRowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());
        addBlockEncoding(new LazyBlockEncoding());

        verifyTypes();

        functionDecoder = new ResolvedFunctionDecoder(this::getType);

        operatorCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> {
                    String name = mangleOperatorName(key.getOperatorType());
                    return resolveFunction(QualifiedName.of(name), fromTypes(key.getArgumentTypes()));
                }));

        coercionCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(key -> {
                    String name = mangleOperatorName(key.getOperatorType());
                    Type fromType = key.getFromType();
                    Type toType = key.getToType();
                    Signature signature = new Signature(name, toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature()));
                    return resolve(functionResolver.resolveCoercion(functions.get(QualifiedName.of(name)), signature));
                }));
    }

    public static MetadataManager createTestMetadataManager()
    {
        return createTestMetadataManager(new FeaturesConfig());
    }

    public static MetadataManager createTestMetadataManager(FeaturesConfig featuresConfig)
    {
        return createTestMetadataManager(new CatalogManager(), featuresConfig);
    }

    public static MetadataManager createTestMetadataManager(CatalogManager catalogManager)
    {
        return createTestMetadataManager(catalogManager, new FeaturesConfig());
    }

    public static MetadataManager createTestMetadataManager(CatalogManager catalogManager, FeaturesConfig featuresConfig)
    {
        return createTestMetadataManager(createTestTransactionManager(catalogManager), featuresConfig);
    }

    public static MetadataManager createTestMetadataManager(TransactionManager transactionManager, FeaturesConfig featuresConfig)
    {
        TypeOperators typeOperators = new TypeOperators();
        return new MetadataManager(
                featuresConfig,
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new MaterializedViewPropertyManager(),
                new ColumnPropertyManager(),
                new AnalyzePropertyManager(),
                transactionManager,
                typeOperators,
                new BlockTypeOperators(typeOperators),
                NodeVersion.UNKNOWN);
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
                .map(catalogMetadata::getMetadataFor)
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
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);
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
        requireNonNull(table, "table is null");

        if (table.getCatalogName().isEmpty() || table.getSchemaName().isEmpty() || table.getObjectName().isEmpty()) {
            // Table cannot exist
            return Optional.empty();
        }

        return getOptionalCatalogMetadata(session, table.getCatalogName()).flatMap(catalogMetadata -> {
            CatalogName catalogName = catalogMetadata.getConnectorId(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);

            return Optional.ofNullable(metadata.getTableHandle(connectorSession, table.asSchemaTableName()))
                    .map(connectorTableHandle -> new TableHandle(
                            catalogName,
                            connectorTableHandle,
                            catalogMetadata.getTransactionHandleFor(catalogName),
                            Optional.empty()));
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
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            ConnectorTableHandle tableHandle = metadata.getTableHandleForStatisticsCollection(session.toConnectorSession(catalogName), table.asSchemaTableName(), analyzeProperties);
            if (tableHandle != null) {
                return Optional.of(new TableHandle(
                        catalogName,
                        tableHandle,
                        catalogMetadata.getTransactionHandleFor(catalogName),
                        Optional.empty()));
            }
        }
        return Optional.empty();
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
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            return metadata.getSystemTable(session.toConnectorSession(catalogName), tableName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableLayoutResult> getLayout(Session session, TableHandle table, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (constraint.getSummary().isNone()) {
            return Optional.empty();
        }

        CatalogName catalogName = table.getCatalogName();
        ConnectorTableHandle connectorTable = table.getConnectorHandle();

        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

        checkState(metadata.usesLegacyTableLayouts(), "getLayout() was called even though connector doesn't support legacy Table Layout");

        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        List<ConnectorTableLayoutResult> layouts = metadata.getTableLayouts(connectorSession, connectorTable, constraint, desiredColumns);
        if (layouts.isEmpty()) {
            return Optional.empty();
        }

        if (layouts.size() > 1) {
            throw new TrinoException(NOT_SUPPORTED, format("Connector returned multiple layouts for table %s", table));
        }

        ConnectorTableLayout tableLayout = layouts.get(0).getTableLayout();
        return Optional.of(new TableLayoutResult(
                new TableHandle(catalogName, connectorTable, transaction, Optional.of(tableLayout.getHandle())),
                new TableProperties(catalogName, transaction, new ConnectorTableProperties(tableLayout)),
                layouts.get(0).getUnenforcedConstraint()));
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        CatalogName catalogName = handle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return handle.getLayout()
                    .map(layout -> new TableProperties(catalogName, handle.getTransaction(), new ConnectorTableProperties(metadata.getTableLayout(connectorSession, layout))))
                    .orElseGet(() -> getLayout(session, handle, Constraint.alwaysTrue(), Optional.empty())
                            .get()
                            .getTableProperties());
        }

        return new TableProperties(catalogName, handle.getTransaction(), metadata.getTableProperties(connectorSession, handle.getConnectorHandle()));
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle tableHandle, PartitioningHandle partitioningHandle)
    {
        checkArgument(partitioningHandle.getConnectorId().isPresent(), "Expect partitioning handle from connector, got system partitioning handle");
        CatalogName catalogName = partitioningHandle.getConnectorId().get();
        checkArgument(catalogName.equals(tableHandle.getCatalogName()), "ConnectorId of tableHandle and partitioningHandle does not match");
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);
        if (metadata.usesLegacyTableLayouts()) {
            ConnectorTableLayoutHandle newTableLayoutHandle = metadata.makeCompatiblePartitioning(session.toConnectorSession(catalogName), tableHandle.getLayout().get(), partitioningHandle.getConnectorHandle());
            return new TableHandle(catalogName, tableHandle.getConnectorHandle(), transaction, Optional.of(newTableLayoutHandle));
        }
        verify(tableHandle.getLayout().isEmpty(), "layout should not be present");
        ConnectorTableHandle newTableHandle = metadata.makeCompatiblePartitioning(
                session.toConnectorSession(catalogName),
                tableHandle.getConnectorHandle(),
                partitioningHandle.getConnectorHandle());
        return new TableHandle(catalogName, newTableHandle, transaction, Optional.empty());
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
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        Optional<ConnectorPartitioningHandle> commonHandle = metadata.getCommonPartitioningHandle(session.toConnectorSession(catalogName), left.getConnectorHandle(), right.getConnectorHandle());
        return commonHandle.map(handle -> new PartitioningHandle(Optional.of(catalogName), left.getTransactionHandle(), handle));
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        CatalogName catalogName = handle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (usesLegacyTableLayouts(session, handle)) {
            ConnectorTableLayoutHandle layoutHandle = handle.getLayout()
                    .orElseGet(() -> getLayout(session, handle, Constraint.alwaysTrue(), Optional.empty())
                            .get()
                            .getNewTableHandle()
                            .getLayout()
                            .get());

            return metadata.getInfo(layoutHandle);
        }

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
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint constraint)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);
        TableStatistics tableStatistics = metadata.getTableStatistics(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), constraint);
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
        return map.build();
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
            if (isExistingRelation(session, objectName.get())) {
                return ImmutableList.of(objectName.get());
            }
            return ImmutableList.of();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());
        Set<QualifiedObjectName> tables = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
                ConnectorSession connectorSession = session.toConnectorSession(catalogName);
                metadata.listTables(connectorSession, prefix.getSchemaName()).stream()
                        .map(convertFromSchemaTableName(prefix.getCatalogName()))
                        .filter(prefix::matches)
                        .forEach(tables::add);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    private boolean isExistingRelation(Session session, QualifiedObjectName name)
    {
        if (getMaterializedView(session, name).isPresent()) {
            return true;
        }
        if (getView(session, name).isPresent()) {
            return true;
        }

        return getTableHandle(session, name).isPresent();
    }

    @Override
    public Map<CatalogName, List<TableColumnsMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        // Track column metadata for every object name to resolve ties between table and view
        Map<SchemaTableName, Optional<List<ColumnMetadata>>> tableColumns = new HashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

                ConnectorSession connectorSession = session.toConnectorSession(catalogName);

                // Collect column metadata from tables
                metadata.streamTableColumns(connectorSession, tablePrefix)
                        .forEach(columnsMetadata -> tableColumns.put(columnsMetadata.getTable(), columnsMetadata.getColumns()));

                // Collect column metadata from views. if table and view names overlap, the view wins
                for (Entry<QualifiedObjectName, ConnectorViewDefinition> entry : getViews(session, prefix).entrySet()) {
                    ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                    for (ViewColumn column : entry.getValue().getColumns()) {
                        try {
                            columns.add(new ColumnMetadata(column.getName(), getType(column.getType())));
                        }
                        catch (TypeNotFoundException e) {
                            throw new TrinoException(INVALID_VIEW, format("Unknown type '%s' for column '%s' in view: %s", column.getType(), column.getName(), entry.getKey()));
                        }
                    }
                    tableColumns.put(entry.getKey().asSchemaTableName(), Optional.of(columns.build()));
                }

                // if view and materialized view names overlap, the materialized view wins
                for (Entry<QualifiedObjectName, ConnectorMaterializedViewDefinition> entry : getMaterializedViews(session, prefix).entrySet()) {
                    ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                    for (ConnectorMaterializedViewDefinition.Column column : entry.getValue().getColumns()) {
                        try {
                            columns.add(new ColumnMetadata(column.getName(), getType(column.getType())));
                        }
                        catch (TypeNotFoundException e) {
                            throw new TrinoException(INVALID_VIEW, format("Unknown type '%s' for column '%s' in materialized view: %s", column.getType(), column.getName(), entry.getKey()));
                        }
                    }
                    tableColumns.put(entry.getKey().asSchemaTableName(), Optional.of(columns.build()));
                }
            }
        }
        return ImmutableMap.of(
                new CatalogName(prefix.getCatalogName()),
                tableColumns.entrySet().stream()
                        .map(entry -> new TableColumnsMetadata(entry.getKey(), entry.getValue()))
                        .collect(toImmutableList()));
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.createSchema(session.toConnectorSession(catalogName), schema.getSchemaName(), properties, principal);
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schema.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.dropSchema(session.toConnectorSession(catalogName), schema.getSchemaName());
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.renameSchema(session.toConnectorSession(catalogName), source.getSchemaName(), target);
    }

    @Override
    public void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, source.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.setSchemaAuthorization(session.toConnectorSession(catalogName), source.getSchemaName(), principal);
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.createTable(session.toConnectorSession(catalog), tableMetadata, ignoreExisting);
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

        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        metadata.renameTable(session.toConnectorSession(catalog), tableHandle.getConnectorHandle(), newTableName.asSchemaTableName());
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
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.setTableAuthorization(session.toConnectorSession(catalogName), table.getSchemaTableName(), principal);
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        metadata.dropTable(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        return metadata.getInsertLayout(session.toConnectorSession(catalogName), table.getConnectorHandle())
                .map(layout -> new NewTableLayout(catalogName, catalogMetadata.getTransactionHandleFor(catalogName), layout));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        CatalogName catalog = catalogMetadata.getCatalogName();
        return metadata.getStatisticsCollectionMetadataForWrite(session.toConnectorSession(catalog), tableMetadata);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        CatalogName catalog = catalogMetadata.getCatalogName();
        return metadata.getStatisticsCollectionMetadata(session.toConnectorSession(catalog), tableMetadata);
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorTableHandle connectorTableHandle = metadata.beginStatisticsCollection(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return new AnalyzeTableHandle(catalogName, transactionHandle, connectorTableHandle);
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        catalogMetadata.getMetadata().finishStatisticsCollection(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), computedStatistics);
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalog);
        ConnectorSession connectorSession = session.toConnectorSession(catalog);
        return metadata.getNewTableLayout(connectorSession, tableMetadata)
                .map(layout -> new NewTableLayout(catalog, transactionHandle, layout));
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
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalogName);
        CatalogName catalog = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalog);
        ConnectorSession connectorSession = session.toConnectorSession(catalog);
        ConnectorOutputTableHandle handle = metadata.beginCreateTable(connectorSession, tableMetadata, layout.map(NewTableLayout::getLayout));
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
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        ConnectorTransactionHandle transactionHandle = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorInsertTableHandle handle = metadata.beginInsert(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), columns);
        return new InsertTableHandle(tableHandle.getCatalogName(), transactionHandle, handle);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        return catalogMetadata.getMetadata().supportsMissingColumnsOnInsert();
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
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
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

        ConnectorInsertTableHandle handle = metadata.beginRefreshMaterializedView(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), sourceConnectorHandles);

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
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (!metadata.usesLegacyTableLayouts()) {
            return false;
        }

        return metadata.supportsMetadataDelete(
                session.toConnectorSession(catalogName),
                tableHandle.getConnectorHandle(),
                tableHandle.getLayout().get());
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyDelete(connectorSession, table.getConnectorHandle())
                .map(newHandle -> new TableHandle(catalogName, newHandle, table.getTransaction(), Optional.empty()));
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle table)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            checkArgument(table.getLayout().isPresent(), "table layout is missing");
            return metadata.metadataDelete(session.toConnectorSession(catalogName), table.getConnectorHandle(), table.getLayout().get());
        }
        checkArgument(table.getLayout().isEmpty(), "table layout should not be present");

        return metadata.executeDelete(connectorSession, table.getConnectorHandle());
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        ConnectorMetadata metadata = getMetadataForWrite(session, catalogName);
        ConnectorTableHandle newHandle = metadata.beginDelete(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle());
        return new TableHandle(tableHandle.getCatalogName(), newHandle, tableHandle.getTransaction(), tableHandle.getLayout());
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
        ConnectorTableHandle newHandle = metadata.beginUpdate(session.toConnectorSession(catalogName), tableHandle.getConnectorHandle(), updatedColumns);
        return new TableHandle(tableHandle.getCatalogName(), newHandle, tableHandle.getTransaction(), tableHandle.getLayout());
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
    public Map<String, CatalogName> getCatalogNames(Session session)
    {
        return transactionManager.getCatalogNames(session.getRequiredTransactionId());
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
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
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
    public Map<QualifiedObjectName, ConnectorViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Map<QualifiedObjectName, ConnectorViewDefinition> views = new LinkedHashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
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
                    views.put(viewName, entry.getValue());
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
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

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
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.getSchemaOwner(connectorSession, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        if (viewName.getCatalogName().isEmpty() || viewName.getSchemaName().isEmpty() || viewName.getObjectName().isEmpty()) {
            // View cannot exist
            return Optional.empty();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);
            return metadata.getView(connectorSession, viewName.asSchemaTableName());
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.createView(session.toConnectorSession(catalogName), viewName.asSchemaTableName(), definition, replace);
    }

    @Override
    public void renameView(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, target.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();
        if (!source.getCatalogName().equals(catalogName.getCatalogName())) {
            throw new TrinoException(SYNTAX_ERROR, "Cannot rename views across catalogs");
        }

        metadata.renameView(session.toConnectorSession(catalogName), source.asSchemaTableName(), target.asSchemaTableName());
    }

    @Override
    public void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, view.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.setViewAuthorization(session.toConnectorSession(catalogName), view.getSchemaTableName(), principal);
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.dropView(session.toConnectorSession(catalogName), viewName.asSchemaTableName());
    }

    @Override
    public void createMaterializedView(Session session, QualifiedObjectName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.createMaterializedView(session.toConnectorSession(catalogName), viewName.asSchemaTableName(), definition, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, viewName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.dropMaterializedView(session.toConnectorSession(catalogName), viewName.asSchemaTableName());
    }

    @Override
    public List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<QualifiedObjectName> objectName = prefix.asQualifiedObjectName();
        if (objectName.isPresent()) {
            return getMaterializedView(session, objectName.get())
                    .map(handle -> ImmutableList.of(objectName.get()))
                    .orElseGet(ImmutableList::of);
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Set<QualifiedObjectName> materializedViews = new LinkedHashSet<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
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
    public Map<QualifiedObjectName, ConnectorMaterializedViewDefinition> getMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, prefix.getCatalogName());

        Map<QualifiedObjectName, ConnectorMaterializedViewDefinition> views = new LinkedHashMap<>();
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();

            SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();
            for (CatalogName catalogName : catalogMetadata.listConnectorIds()) {
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
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
                    views.put(viewName, entry.getValue());
                }
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        if (viewName.getCatalogName().isEmpty() || viewName.getSchemaName().isEmpty() || viewName.getObjectName().isEmpty()) {
            // View cannot exist
            return Optional.empty();
        }

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, viewName.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            CatalogName catalogName = catalogMetadata.getConnectorId(session, viewName);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

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
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

            ConnectorSession connectorSession = session.toConnectorSession(catalogName);
            return metadata.getMaterializedViewFreshness(connectorSession, viewName.asSchemaTableName());
        }
        return new MaterializedViewFreshness(false);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle)
    {
        CatalogName catalogName = tableHandle.getCatalogName();
        CatalogMetadata catalogMetadata = getCatalogMetadata(session, catalogName);
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyTableScanRedirect(connectorSession, tableHandle.getConnectorHandle());
    }

    private QualifiedObjectName getRedirectedTableName(Session session, QualifiedObjectName originalTableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(originalTableName, "originalTableName is null");

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
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);

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
        QualifiedObjectName targetTableName = getRedirectedTableName(session, tableName);
        if (targetTableName.equals(tableName)) {
            return noRedirection(getTableHandle(session, tableName));
        }

        Optional<TableHandle> tableHandle = getTableHandle(session, targetTableName);
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
        ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
        ConnectorTransactionHandle transaction = catalogMetadata.getTransactionHandleFor(catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        Optional<ConnectorResolvedIndex> resolvedIndex = metadata.resolveIndex(connectorSession, tableHandle.getConnectorHandle(), indexableColumns, outputColumns, tupleDomain);
        return resolvedIndex.map(resolved -> new ResolvedIndex(tableHandle.getCatalogName(), transaction, resolved));
    }

    @Override
    public boolean usesLegacyTableLayouts(Session session, TableHandle table)
    {
        return getMetadata(session, table.getCatalogName()).usesLegacyTableLayouts();
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyLimit(connectorSession, table.getConnectorHandle(), limit)
                .map(result -> new LimitApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
                        result.isLimitGuaranteed(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applySample(connectorSession, table.getConnectorHandle(), sampleType, sampleRatio)
                .map(result -> new SampleApplicationResult<>(new TableHandle(
                        catalogName,
                        result.getHandle(),
                        table.getTransaction(),
                        Optional.empty()),
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

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyAggregation(connectorSession, table.getConnectorHandle(), aggregations, assignments, groupingSets)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), aggregations.size());

                    return new AggregationApplicationResult<>(
                            new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
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
                            transaction,
                            Optional.empty()),
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

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyTopN(connectorSession, table.getConnectorHandle(), topNCount, sortItems, assignments)
                .map(result -> new TopNApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
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

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyFilter(connectorSession, table.getConnectorHandle(), constraint)
                .map(result -> new ConstraintApplicationResult<>(
                        new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
                        result.getRemainingFilter(),
                        result.isPrecalculateStatistics()));
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        CatalogName catalogName = table.getCatalogName();
        ConnectorMetadata metadata = getMetadata(session, catalogName);

        if (metadata.usesLegacyTableLayouts()) {
            return Optional.empty();
        }

        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        return metadata.applyProjection(connectorSession, table.getConnectorHandle(), projections, assignments)
                .map(result -> {
                    verifyProjection(table, result.getProjections(), result.getAssignments(), projections.size());

                    return new ProjectionApplicationResult<>(
                            new TableHandle(catalogName, result.getHandle(), table.getTransaction(), Optional.empty()),
                            result.getProjections(),
                            result.getAssignments(),
                            result.isPrecalculateStatistics());
                });
    }

    //
    // Roles and Grants
    //

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.createRole(session.toConnectorSession(catalogName), role, grantor);
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.dropRole(session.toConnectorSession(catalogName), role);
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (catalogMetadata.isEmpty()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return metadata.listRoles(connectorSession).stream()
                .map(role -> role.toLowerCase(ENGLISH))
                .collect(toImmutableSet());
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(Session session, String catalog, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (catalogMetadata.isEmpty()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return metadata.listAllRoleGrants(connectorSession, roles, grantees, limit);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, TrinoPrincipal principal)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (catalogMetadata.isEmpty()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return metadata.listRoleGrants(connectorSession, principal);
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.grantRoles(session.toConnectorSession(catalogName), roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, String catalog)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, catalog);
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.revokeRoles(session.toConnectorSession(catalogName), roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal, String catalog)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (catalogMetadata.isEmpty()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return ImmutableSet.copyOf(metadata.listApplicableRoles(connectorSession, principal));
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        Optional<CatalogMetadata> catalogMetadata = getOptionalCatalogMetadata(session, catalog);
        if (catalogMetadata.isEmpty()) {
            return ImmutableSet.of();
        }
        CatalogName catalogName = catalogMetadata.get().getCatalogName();
        ConnectorSession connectorSession = session.toConnectorSession(catalogName);
        ConnectorMetadata metadata = catalogMetadata.get().getMetadataFor(catalogName);
        return ImmutableSet.copyOf(metadata.listEnabledRoles(connectorSession));
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.grantTablePrivileges(session.toConnectorSession(catalogName), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, tableName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.revokeTablePrivileges(session.toConnectorSession(catalogName), tableName.asSchemaTableName(), privileges, grantee, grantOption);
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

        metadata.grantSchemaPrivileges(session.toConnectorSession(catalogName), schemaName.getSchemaName(), privileges, grantee, grantOption);
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadataForWrite(session, schemaName.getCatalogName());
        CatalogName catalogName = catalogMetadata.getCatalogName();
        ConnectorMetadata metadata = catalogMetadata.getMetadata();

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
                ConnectorMetadata metadata = catalogMetadata.getMetadataFor(catalogName);
                grantInfos.addAll(metadata.listTablePrivileges(connectorSession, prefix.asSchemaTablePrefix()));
            }
        }
        return ImmutableList.copyOf(grantInfos.build());
    }

    //
    // Types
    //

    @Override
    public Type getType(TypeSignature signature)
    {
        return typeRegistry.getType(new InternalTypeManager(this, typeOperators), signature);
    }

    @Override
    public Type fromSqlType(String sqlType)
    {
        return typeRegistry.fromSqlType(new InternalTypeManager(this, typeOperators), sqlType);
    }

    @Override
    public Type getType(TypeId id)
    {
        return typeRegistry.getType(new InternalTypeManager(this, typeOperators), id);
    }

    @Override
    public Collection<Type> getTypes()
    {
        return typeRegistry.getTypes();
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        return typeRegistry.getParametricTypes();
    }

    public void addType(Type type)
    {
        typeRegistry.addType(type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        typeRegistry.addParametricType(parametricType);
    }

    @Override
    public void verifyTypes()
    {
        Set<Type> missingOperatorDeclaration = new HashSet<>();
        Multimap<Type, OperatorType> missingOperators = HashMultimap.create();
        for (Type type : typeRegistry.getTypes()) {
            if (type.getTypeOperatorDeclaration(typeOperators) == null) {
                missingOperatorDeclaration.add(type);
                continue;
            }
            if (type.isComparable()) {
                if (!hasEqualMethod(type)) {
                    missingOperators.put(type, EQUAL);
                }
                if (!hasHashCodeMethod(type)) {
                    missingOperators.put(type, HASH_CODE);
                }
                if (!hasXxHash64Method(type)) {
                    missingOperators.put(type, XX_HASH_64);
                }
                if (!hasDistinctFromMethod(type)) {
                    missingOperators.put(type, IS_DISTINCT_FROM);
                }
                if (!hasIndeterminateMethod(type)) {
                    missingOperators.put(type, INDETERMINATE);
                }
            }
            if (type.isOrderable()) {
                if (!hasComparisonMethod(type)) {
                    missingOperators.put(type, COMPARISON);
                }
                if (!hasLessThanMethod(type)) {
                    missingOperators.put(type, LESS_THAN);
                }
                if (!hasLessThanOrEqualMethod(type)) {
                    missingOperators.put(type, LESS_THAN_OR_EQUAL);
                }
            }
        }
        // TODO: verify the parametric types too
        if (!missingOperators.isEmpty()) {
            List<String> messages = new ArrayList<>();
            for (Type type : missingOperatorDeclaration) {
                messages.add(format("%s types operators is null", type));
            }
            for (Type type : missingOperators.keySet()) {
                messages.add(format("%s missing for %s", missingOperators.get(type), type));
            }
            throw new IllegalStateException(Joiner.on(", ").join(messages));
        }
    }

    private boolean hasEqualMethod(Type type)
    {
        try {
            typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasHashCodeMethod(Type type)
    {
        try {
            typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasXxHash64Method(Type type)
    {
        try {
            typeOperators.getXxHash64Operator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasDistinctFromMethod(Type type)
    {
        try {
            typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasIndeterminateMethod(Type type)
    {
        try {
            typeOperators.getIndeterminateOperator(type, simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasComparisonMethod(Type type)
    {
        try {
            typeOperators.getComparisonOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasLessThanMethod(Type type)
    {
        try {
            typeOperators.getLessThanOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasLessThanOrEqualMethod(Type type)
    {
        try {
            typeOperators.getLessThanOrEqualOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    //
    // Functions
    //

    @Override
    public void addFunctions(List<? extends SqlFunction> functionInfos)
    {
        functions.addFunctions(functionInfos);
    }

    @Override
    public List<FunctionMetadata> listFunctions()
    {
        return functions.list();
    }

    @Override
    public ResolvedFunction decodeFunction(QualifiedName name)
    {
        return functionDecoder.fromQualifiedName(name)
                .orElseThrow(() -> new IllegalArgumentException("Function is not resolved: " + name));
    }

    @Override
    public ResolvedFunction resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return functionDecoder.fromQualifiedName(name)
                .orElseGet(() -> resolve(functionResolver.resolveFunction(functions.get(name), name, parameterTypes)));
    }

    @Override
    public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        try {
            return operatorCache.getUnchecked(new OperatorCacheKey(operatorType, argumentTypes));
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

    @Override
    public ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType)
    {
        checkArgument(operatorType == OperatorType.CAST || operatorType == OperatorType.SATURATED_FLOOR_CAST);
        try {
            return coercionCache.getUnchecked(new CoercionCacheKey(operatorType, fromType, toType));
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
    public ResolvedFunction getCoercion(QualifiedName name, Type fromType, Type toType)
    {
        return resolve(functionResolver.resolveCoercion(functions.get(name), new Signature(name.getSuffix(), toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature()))));
    }

    private ResolvedFunction resolve(FunctionBinding functionBinding)
    {
        FunctionDependencyDeclaration declaration = functions.getFunctionDependencies(functionBinding);
        return resolve(functionBinding, declaration);
    }

    @VisibleForTesting
    public ResolvedFunction resolve(FunctionBinding functionBinding, FunctionDependencyDeclaration declaration)
    {
        Map<TypeSignature, Type> dependentTypes = declaration.getTypeDependencies().stream()
                .map(typeSignature -> applyBoundVariables(typeSignature, functionBinding))
                .collect(toImmutableMap(Function.identity(), this::getType, (left, right) -> left));

        ImmutableSet.Builder<ResolvedFunction> functions = ImmutableSet.builder();
        declaration.getFunctionDependencies().stream()
                .map(functionDependency -> {
                    try {
                        List<TypeSignature> argumentTypes = applyBoundVariables(functionDependency.getArgumentTypes(), functionBinding);
                        return resolveFunction(functionDependency.getName(), fromTypeSignatures(argumentTypes));
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
                        return resolveFunction(QualifiedName.of(mangleOperatorName(operatorDependency.getOperatorType())), fromTypeSignatures(argumentTypes));
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
                        Type fromType = getType(applyBoundVariables(castDependency.getFromType(), functionBinding));
                        Type toType = getType(applyBoundVariables(castDependency.getToType(), functionBinding));
                        return getCoercion(fromType, toType);
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
                dependentTypes,
                functions.build());
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        return functions.get(name).stream()
                .map(FunctionMetadata::getKind)
                .anyMatch(AGGREGATE::equals);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ResolvedFunction resolvedFunction)
    {
        FunctionMetadata functionMetadata = functions.get(resolvedFunction.getFunctionId());

        // specialize function metadata to resolvedFunction
        List<FunctionArgumentDefinition> argumentDefinitions;
        if (functionMetadata.getSignature().isVariableArity()) {
            List<FunctionArgumentDefinition> fixedArguments = functionMetadata.getArgumentDefinitions().subList(0, functionMetadata.getArgumentDefinitions().size() - 1);
            int variableArgumentCount = resolvedFunction.getSignature().getArgumentTypes().size() - fixedArguments.size();
            argumentDefinitions = ImmutableList.<FunctionArgumentDefinition>builder()
                    .addAll(fixedArguments)
                    .addAll(nCopies(variableArgumentCount, functionMetadata.getArgumentDefinitions().get(functionMetadata.getArgumentDefinitions().size() - 1)))
                    .build();
        }
        else {
            argumentDefinitions = functionMetadata.getArgumentDefinitions();
        }
        return new FunctionMetadata(
                functionMetadata.getFunctionId(),
                resolvedFunction.getSignature().toSignature(),
                functionMetadata.getActualName(),
                functionMetadata.isNullable(),
                argumentDefinitions,
                functionMetadata.isHidden(),
                functionMetadata.isDeterministic(),
                functionMetadata.getDescription(),
                functionMetadata.getKind(),
                functionMetadata.isDeprecated());
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(ResolvedFunction resolvedFunction)
    {
        return functions.getAggregationFunctionMetadata(toFunctionBinding(resolvedFunction));
    }

    @Override
    public WindowFunctionSupplier getWindowFunctionImplementation(ResolvedFunction resolvedFunction)
    {
        FunctionDependencies functionDependencies = new FunctionDependencies(this, resolvedFunction.getTypeDependencies(), resolvedFunction.getFunctionDependencies());
        return functions.getWindowFunctionImplementation(toFunctionBinding(resolvedFunction), functionDependencies);
    }

    @Override
    public InternalAggregationFunction getAggregateFunctionImplementation(ResolvedFunction resolvedFunction)
    {
        FunctionDependencies functionDependencies = new FunctionDependencies(this, resolvedFunction.getTypeDependencies(), resolvedFunction.getFunctionDependencies());
        return functions.getAggregateFunctionImplementation(toFunctionBinding(resolvedFunction), functionDependencies);
    }

    @Override
    public FunctionInvoker getScalarFunctionInvoker(ResolvedFunction resolvedFunction, InvocationConvention invocationConvention)
    {
        FunctionDependencies functionDependencies = new FunctionDependencies(this, resolvedFunction.getTypeDependencies(), resolvedFunction.getFunctionDependencies());
        FunctionInvoker functionInvoker = functions.getScalarFunctionInvoker(toFunctionBinding(resolvedFunction), functionDependencies, invocationConvention);
        verifyMethodHandleSignature(resolvedFunction.getSignature(), functionInvoker, invocationConvention);
        return functionInvoker;
    }

    private static void verifyMethodHandleSignature(BoundSignature boundSignature, FunctionInvoker functionInvoker, InvocationConvention convention)
    {
        MethodHandle methodHandle = functionInvoker.getMethodHandle();
        MethodType methodType = methodHandle.type();

        checkArgument(convention.getArgumentConventions().size() == boundSignature.getArgumentTypes().size(),
                "Expected %s arguments, but got %s", boundSignature.getArgumentTypes().size(), convention.getArgumentConventions().size());

        int expectedParameterCount = convention.getArgumentConventions().stream()
                .mapToInt(InvocationArgumentConvention::getParameterCount)
                .sum();
        expectedParameterCount += methodType.parameterList().stream().filter(ConnectorSession.class::equals).count();
        if (functionInvoker.getInstanceFactory().isPresent()) {
            expectedParameterCount++;
        }
        checkArgument(expectedParameterCount == methodType.parameterCount(),
                "Expected %s method parameters, but got %s", expectedParameterCount, methodType.parameterCount());

        int parameterIndex = 0;
        if (functionInvoker.getInstanceFactory().isPresent()) {
            verifyFunctionSignature(convention.supportsInstanceFactor(), "Method requires instance factory, but calling convention does not support an instance factory");
            MethodHandle factoryMethod = functionInvoker.getInstanceFactory().orElseThrow();
            verifyFunctionSignature(methodType.parameterType(parameterIndex).equals(factoryMethod.type().returnType()), "Invalid return type");
            parameterIndex++;
        }

        int lambdaArgumentIndex = 0;
        for (int argumentIndex = 0; argumentIndex < boundSignature.getArgumentTypes().size(); argumentIndex++) {
            // skip session parameters
            while (methodType.parameterType(parameterIndex).equals(ConnectorSession.class)) {
                verifyFunctionSignature(convention.supportsSession(), "Method requires session, but calling convention does not support session");
                parameterIndex++;
            }

            Class<?> parameterType = methodType.parameterType(parameterIndex);
            Type argumentType = boundSignature.getArgumentTypes().get(argumentIndex);
            InvocationArgumentConvention argumentConvention = convention.getArgumentConvention(argumentIndex);
            switch (argumentConvention) {
                case NEVER_NULL:
                    verifyFunctionSignature(parameterType.isAssignableFrom(argumentType.getJavaType()),
                            "Expected argument type to be %s, but is %s", argumentType, parameterType);
                    break;
                case NULL_FLAG:
                    verifyFunctionSignature(parameterType.isAssignableFrom(argumentType.getJavaType()),
                            "Expected argument type to be %s, but is %s", argumentType.getJavaType(), parameterType);
                    verifyFunctionSignature(methodType.parameterType(parameterIndex + 1).equals(boolean.class),
                            "Expected null flag parameter to be followed by a boolean parameter");
                    break;
                case BOXED_NULLABLE:
                    verifyFunctionSignature(parameterType.isAssignableFrom(wrap(argumentType.getJavaType())),
                            "Expected argument type to be %s, but is %s", wrap(argumentType.getJavaType()), parameterType);
                    break;
                case BLOCK_POSITION:
                    verifyFunctionSignature(parameterType.equals(Block.class) && methodType.parameterType(parameterIndex + 1).equals(int.class),
                            "Expected BLOCK_POSITION argument have parameters Block and int");
                    break;
                case FUNCTION:
                    Class<?> lambdaInterface = functionInvoker.getLambdaInterfaces().get(lambdaArgumentIndex);
                    verifyFunctionSignature(parameterType.equals(lambdaInterface),
                            "Expected function interface to be %s, but is %s", lambdaInterface, parameterType);
                    lambdaArgumentIndex++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown argument convention: " + argumentConvention);
            }
            parameterIndex += argumentConvention.getParameterCount();
        }

        Type returnType = boundSignature.getReturnType();
        switch (convention.getReturnConvention()) {
            case FAIL_ON_NULL:
                verifyFunctionSignature(methodType.returnType().isAssignableFrom(returnType.getJavaType()),
                        "Expected return type to be %s, but is %s", returnType.getJavaType(), methodType.returnType());
                break;
            case NULLABLE_RETURN:
                verifyFunctionSignature(methodType.returnType().isAssignableFrom(wrap(returnType.getJavaType())),
                        "Expected return type to be %s, but is %s", returnType.getJavaType(), wrap(methodType.returnType()));
                break;
            default:
                throw new UnsupportedOperationException("Unknown return convention: " + convention.getReturnConvention());
        }
    }

    private static void verifyFunctionSignature(boolean check, String message, Object... args)
    {
        if (!check) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, format(message, args));
        }
    }

    private FunctionBinding toFunctionBinding(ResolvedFunction resolvedFunction)
    {
        Signature functionSignature = functions.get(resolvedFunction.getFunctionId()).getSignature();
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

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        return procedures;
    }

    //
    // Blocks
    //

    private BlockEncoding getBlockEncoding(String encodingName)
    {
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding: %s", encodingName);
        return blockEncoding;
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return new InternalBlockEncodingSerde(this::getBlockEncoding, this::getType);
    }

    public void addBlockEncoding(BlockEncoding blockEncoding)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncoding existingEntry = blockEncodings.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntry == null, "Encoding already registered: %s", blockEncoding.getName());
    }

    //
    // Properties
    //

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        return schemaPropertyManager;
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    @Override
    public MaterializedViewPropertyManager getMaterializedViewPropertyManager()
    {
        return materializedViewPropertyManager;
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        return columnPropertyManager;
    }

    @Override
    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        return analyzePropertyManager;
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
        return getCatalogMetadata(session, catalogName).getMetadataFor(catalogName);
    }

    private ConnectorMetadata getMetadataForWrite(Session session, CatalogName catalogName)
    {
        return getCatalogMetadataForWrite(session, catalogName).getMetadata();
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
                catalogMetadata.getMetadata().beginQuery(connectorSession);
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
                catalogMetadata.getMetadata().cleanupQuery(connectorSession);
            }
        }
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
}
