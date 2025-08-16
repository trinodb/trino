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
package io.trino.plugin.pinot;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.deepstore.PinotDeepStore;
import io.trino.plugin.pinot.encoders.PinotWrittenSegments;
import io.trino.plugin.pinot.query.AggregateExpression;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.plugin.pinot.query.DynamicTableBuilder;
import io.trino.plugin.pinot.query.aggregation.ImplementApproxDistinct;
import io.trino.plugin.pinot.query.aggregation.ImplementAvg;
import io.trino.plugin.pinot.query.aggregation.ImplementCountAll;
import io.trino.plugin.pinot.query.aggregation.ImplementCountDistinct;
import io.trino.plugin.pinot.query.aggregation.ImplementMinMax;
import io.trino.plugin.pinot.query.aggregation.ImplementSum;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.pinot.PinotPageSink.PROCESSED_SEGMENT_METADATA_JSON_CODEC;
import static io.trino.plugin.pinot.PinotSessionProperties.isAggregationPushdownEnabled;
import static io.trino.plugin.pinot.query.AggregateExpression.replaceIdentifier;
import static io.trino.plugin.pinot.query.DynamicTablePqlExtractor.quoteIdentifier;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.RelationColumnsMetadata.forTable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toList;

public class PinotMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    // Pinot does not yet have full support for predicates that are always TRUE/FALSE
    // See https://github.com/apache/incubator-pinot/issues/10601
    private static final Set<Type> SUPPORTS_ALWAYS_FALSE = Set.of(BIGINT, INTEGER, REAL, DOUBLE);

    private final NonEvictableLoadingCache<String, Schema> pinotTableSchemaCache;
    private final int maxRowsPerBrokerQuery;
    private final AggregateFunctionRewriter<AggregateExpression, Void> aggregateFunctionRewriter;
    private final ImplementCountDistinct implementCountDistinct;
    private final PinotClient pinotClient;
    private final PinotTypeConverter typeConverter;
    private final NodeManager nodeManager;
    private final PinotDeepStore.DeepStoreProvider deepStoreProvider;

    @Inject
    public PinotMetadata(
            PinotClient pinotClient,
            PinotConfig pinotConfig,
            @ForPinot ExecutorService executor,
            PinotTypeConverter typeConverter,
            NodeManager nodeManager)
    {
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        long metadataCacheExpiryMillis = pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pinotTableSchemaCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS),
                asyncReloading(new CacheLoader<>()
                {
                    @Override
                    public Schema load(String tableName)
                            throws Exception
                    {
                        return pinotClient.getTableSchema(tableName);
                    }
                }, executor));

        this.maxRowsPerBrokerQuery = pinotConfig.getMaxRowsForBrokerQueries();
        Function<String, String> identifierQuote = identity(); // TODO identifier quoting not needed here?
        this.implementCountDistinct = new ImplementCountDistinct(identifierQuote);
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                new ConnectorExpressionRewriter<>(ImmutableSet.of()),
                ImmutableSet.<AggregateFunctionRule<AggregateExpression, Void>>builder()
                        .add(new ImplementCountAll())
                        .add(new ImplementAvg(identifierQuote))
                        .add(new ImplementMinMax(identifierQuote))
                        .add(new ImplementSum(identifierQuote))
                        .add(new ImplementApproxDistinct(identifierQuote))
                        .add(implementCountDistinct)
                        .build());
        this.deepStoreProvider = pinotConfig.getDeepStoreProvider();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public PinotTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (tableName.getTableName().trim().contains("select ")) {
            DynamicTable dynamicTable = DynamicTableBuilder.buildFromPql(this, tableName, pinotClient, typeConverter);
            return new PinotTableHandle(tableName.getSchemaName(), dynamicTable.tableName(), false, TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable), Optional.empty(), OptionalInt.empty(), Optional.empty());
        }
        String pinotTableName = pinotClient.getPinotTableNameFromTrinoTableNameIfExists(tableName.getTableName());
        if (pinotTableName == null) {
            return null;
        }
        return new PinotTableHandle(
                tableName.getSchemaName(),
                pinotTableName,
                getFromCache(pinotTableSchemaCache, pinotTableName).isEnableColumnBasedNullHandling(),
                TupleDomain.all(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.of(getNodes()),
                OptionalInt.of(pinotClient.getSegments(pinotTableName).size()),
                getPinotDateTimeField(pinotTableName));
    }

    private List<String> getNodes()
    {
        List<String> nodes = nodeManager.getRequiredWorkerNodes().stream().map(Node::getNodeIdentifier).collect(toList());
        return nodes;
    }

    private Optional<PinotDateTimeField> getPinotDateTimeField(String pinotTableName)
    {
        Schema schema;
        try {
            schema = pinotClient.getTableSchema(pinotTableName);
        }
        catch (Exception e) {
            throw new TrinoException(TABLE_NOT_FOUND, "Failed to get table schema for " + pinotTableName, e);
        }
        PinotClient.PinotTableConfig pinotTableConfig = pinotClient.getTableConfig(pinotTableName);
        TableConfig tableConfig;
        if (pinotTableConfig.getOfflineConfig().isPresent()) {
            tableConfig = pinotTableConfig.getOfflineConfig().get();
        }
        else {
            tableConfig = pinotTableConfig.getRealtimeConfig().get();
        }
        DateTimeFieldSpec dateTimeFieldSpec = schema.getDateTimeSpec(tableConfig.getValidationConfig().getTimeColumnName());
        return Optional.ofNullable(dateTimeFieldSpec).map(fieldSpec -> new PinotDateTimeField(dateTimeFieldSpec.getName(), new DateTimeFormatSpec(dateTimeFieldSpec.getFormat()).getColumnUnit(), typeConverter.toTrinoType(dateTimeFieldSpec)));
    }

    private List<String> getPartitionColumns(String tableName)
    {
        try {
            String pinotTableName = pinotClient.getPinotTableNameFromTrinoTableName(tableName);
            PinotClient.PinotTableConfig pinotTableConfig = pinotClient.getTableConfig(pinotTableName);
            TableConfig tableConfig;
            if (pinotTableConfig.getOfflineConfig().isPresent()) {
                tableConfig = pinotTableConfig.getOfflineConfig().get();
            }
            else if (pinotTableConfig.getRealtimeConfig().isPresent()) {
                tableConfig = pinotTableConfig.getRealtimeConfig().get();
            }
            else {
                return ImmutableList.of();
            }

            // Get partition columns from segmentPartitionConfig
            if (tableConfig.getIndexingConfig() != null &&
                    tableConfig.getIndexingConfig().getSegmentPartitionConfig() != null &&
                    tableConfig.getIndexingConfig().getSegmentPartitionConfig().getColumnPartitionMap() != null) {
                return tableConfig.getIndexingConfig().getSegmentPartitionConfig().getColumnPartitionMap().keySet().stream()
                        .map(column -> column.toLowerCase(ENGLISH))
                        .collect(toImmutableList());
            }
        }
        catch (Exception e) {
            // Log warning but don't fail - partitioning is optional
        }
        return ImmutableList.of();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
        if (pinotTableHandle.query().isPresent()) {
            DynamicTable dynamicTable = pinotTableHandle.query().get();
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
            for (PinotColumnHandle pinotColumnHandle : dynamicTable.projections()) {
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }
            dynamicTable.aggregateColumns()
                    .forEach(columnHandle -> columnMetadataBuilder.add(columnHandle.getColumnMetadata()));
            SchemaTableName schemaTableName = new SchemaTableName(pinotTableHandle.schemaName(), dynamicTable.tableName());
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        }
        SchemaTableName tableName = new SchemaTableName(pinotTableHandle.schemaName(), pinotTableHandle.tableName());

        return new ConnectorTableMetadata(tableName, getColumnsMetadata(tableName.getTableName()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        ImmutableSet.Builder<SchemaTableName> builder = ImmutableSet.builder();
        for (String table : pinotClient.getPinotTableNames()) {
            builder.add(new SchemaTableName(SCHEMA_NAME, table));
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        if (pinotTableHandle.query().isPresent()) {
            return getDynamicTableColumnHandles(pinotTableHandle);
        }
        return getPinotColumnHandles(pinotTableHandle.tableName());
    }

    public Map<String, ColumnHandle> getPinotColumnHandles(String tableName)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        String pinotTableName = pinotClient.getPinotTableNameFromTrinoTableName(tableName);
        for (PinotColumnHandle columnHandle : getPinotColumnHandlesForPinotSchema(pinotTableName)) {
            columnHandlesBuilder.put(columnHandle.getColumnName().toLowerCase(ENGLISH), columnHandle);
        }
        return columnHandlesBuilder.buildOrThrow();
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;

        // Get all partition columns from table configuration
        List<String> partitionColumns = getPartitionColumns(pinotTableHandle.tableName());

        // If the table has time-based partitioning, include it
        if (pinotTableHandle.dateTimeField().isPresent()) {
            PinotDateTimeField dateTimeField = pinotTableHandle.dateTimeField().get();
            String lowerCaseDateTimeColumn = dateTimeField.columnName().toLowerCase(ENGLISH);
            if (!partitionColumns.contains(lowerCaseDateTimeColumn)) {
                partitionColumns = ImmutableList.<String>builder()
                        .addAll(partitionColumns)
                        .add(lowerCaseDateTimeColumn)
                        .build();
            }
        }

        List<String> lowerCasePartitionColumns = partitionColumns.stream()
                .map(column -> column.toLowerCase(ENGLISH))
                .collect(toImmutableList());

        // If there are partition columns, create the layout
        if (!lowerCasePartitionColumns.isEmpty()) {
            PinotPartitioningHandle partitioningHandle = new PinotPartitioningHandle(
                    pinotTableHandle.nodes(),
                    pinotTableHandle.dateTimeField(),
                    pinotTableHandle.segmentCount());

            return Optional.of(new ConnectorTableLayout(
                    partitioningHandle,
                    lowerCasePartitionColumns,
                    false));  // Pinot doesn't support multiple writers per partition
        }
        return Optional.empty();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();

        for (SchemaTableName tableName : listTables(session, schemaName)) {
            try {
                relationColumns.put(tableName, forTable(tableName, getColumnsMetadata(tableName.getTableName())));
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }

        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((PinotColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle table)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
        if (pinotTableHandle.dateTimeField().isPresent()) {
            PinotDateTimeField dateTimeField = pinotTableHandle.dateTimeField().get();
            PinotColumnHandle partitionColumn = new PinotColumnHandle(dateTimeField.columnName(), dateTimeField.type());
            return new ConnectorTableProperties(
                    TupleDomain.all(),
                    Optional.of(new ConnectorTablePartitioning(new PinotPartitioningHandle(pinotTableHandle.nodes(), pinotTableHandle.dateTimeField(), pinotTableHandle.segmentCount()), ImmutableList.of(partitionColumn))),
                    Optional.empty(),
                    ImmutableList.of());
        }
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorPartitioningHandle> partitioningHandle, List<ColumnHandle> columns)
    {
        if (!partitioningHandle.isPresent()) {
            return Optional.empty();
        }
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        PinotPartitioningHandle pinotPartitioningHandle = (PinotPartitioningHandle) partitioningHandle.get();

        return Optional.of(new PinotTableHandle(
                pinotTableHandle.schemaName(),
                pinotTableHandle.tableName(),
                false,
                pinotTableHandle.constraint(),
                pinotTableHandle.limit(),
                pinotTableHandle.query(),
                pinotPartitioningHandle.nodes(),
                pinotTableHandle.segmentCount(),
                pinotPartitioningHandle.dateTimeField()));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        String pinotTableName = pinotClient.getPinotTableNameFromTrinoTableName(((PinotTableHandle) tableHandle).tableName());
        List<PinotColumnHandle> pinotColumnHandles = columns.stream()
                .map(column -> (PinotColumnHandle) column)
                .collect(toImmutableList());
        return new PinotInsertTableHandle(pinotTableName, pinotTableHandle.dateTimeField(), pinotColumnHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        if (deepStoreProvider != PinotDeepStore.DeepStoreProvider.NONE) {
            List<String> processedSegmentMetadata = fragments.stream()
                    .map(Slice::getBytes)
                    .map(PROCESSED_SEGMENT_METADATA_JSON_CODEC::fromJson)
                    .map(PROCESSED_SEGMENT_METADATA_JSON_CODEC::toJson)
                    .collect(toImmutableList());
            return Optional.of(new PinotWrittenSegments(processedSegmentMetadata));
        }
        return Optional.empty();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        if (handle.limit().isPresent() && handle.limit().getAsLong() <= limit) {
            return Optional.empty();
        }
        Optional<DynamicTable> dynamicTable = handle.query();
        if (dynamicTable.isPresent() &&
                (dynamicTable.get().limit().isEmpty() || dynamicTable.get().limit().getAsLong() > limit)) {
            dynamicTable = Optional.of(new DynamicTable(dynamicTable.get().tableName(),
                    dynamicTable.get().suffix(),
                    dynamicTable.get().projections(),
                    dynamicTable.get().filter(),
                    dynamicTable.get().groupingColumns(),
                    dynamicTable.get().aggregateColumns(),
                    dynamicTable.get().havingExpression(),
                    dynamicTable.get().orderBy(),
                    OptionalLong.of(limit),
                    dynamicTable.get().offset(),
                    dynamicTable.get().queryOptions(),
                    dynamicTable.get().query()));
        }

        handle = new PinotTableHandle(
                handle.schemaName(),
                handle.tableName(),
                handle.enableNullHandling(),
                handle.constraint(),
                OptionalLong.of(limit),
                dynamicTable,
                handle.nodes(),
                handle.segmentCount(),
                handle.dateTimeField());
        boolean singleSplit = dynamicTable.isPresent();
        return Optional.of(new LimitApplicationResult<>(handle, singleSplit, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.constraint();

        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        TupleDomain<ColumnHandle> remainingFilter;
        if (newDomain.isNone()) {
            remainingFilter = TupleDomain.all();
        }
        else {
            Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();

            Map<ColumnHandle, Domain> supported = new HashMap<>();
            Map<ColumnHandle, Domain> unsupported = new HashMap<>();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                Type columnType = ((PinotColumnHandle) entry.getKey()).getDataType();
                if (columnType instanceof ArrayType) {
                    // Pinot does not support array literals
                    unsupported.put(entry.getKey(), entry.getValue());
                }
                else if (typeConverter.isJsonType(columnType)) {
                    // Pinot does not support filtering on json values
                    unsupported.put(entry.getKey(), entry.getValue());
                }
                else if (isFilterPushdownUnsupported(entry.getValue())) {
                    unsupported.put(entry.getKey(), entry.getValue());
                }
                else {
                    supported.put(entry.getKey(), entry.getValue());
                }
            }
            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new PinotTableHandle(
                handle.schemaName(),
                handle.tableName(),
                handle.enableNullHandling(),
                newDomain,
                handle.limit(),
                handle.query(),
                handle.nodes(),
                handle.segmentCount(),
                handle.dateTimeField());
        return Optional.of(new ConstraintApplicationResult<>(handle, remainingFilter, constraint.getExpression(), false));
    }

    // IS NULL and IS NOT NULL are handled differently in Pinot, pushing down would lead to inconsistent results.
    // See https://docs.pinot.apache.org/developers/advanced/null-value-support for more info.
    private boolean isFilterPushdownUnsupported(Domain domain)
    {
        ValueSet valueSet = domain.getValues();
        boolean isNotNull = valueSet.isAll() && !domain.isNullAllowed();
        boolean isUnsupportedAlwaysFalse = domain.isNone() && !SUPPORTS_ALWAYS_FALSE.contains(domain.getType());
        boolean isInOrNull = !valueSet.getRanges().getOrderedRanges().isEmpty() && domain.isNullAllowed();
        return isNotNull ||
                domain.isOnlyNull() ||
                isUnsupportedAlwaysFalse ||
                isInOrNull;
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (!isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }

        // Global aggregation is represented by [[]]
        verify(!groupingSets.isEmpty(), "No grouping sets provided");

        // Pinot currently only supports simple GROUP BY clauses with a single grouping set
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }

        PinotTableHandle tableHandle = (PinotTableHandle) handle;
        Schema schema = getFromCache(pinotTableSchemaCache, tableHandle.tableName());
        if (schema.isEnableColumnBasedNullHandling()) {
            // Pinot has a correctness issue when null handling is enabled
            return Optional.empty();
        }

        // Do not push aggregations down if a grouping column is an array type.
        // Pinot treats each element of array as a grouping key
        // See https://github.com/apache/pinot/issues/8353 for more details.
        if (getOnlyElement(groupingSets).stream()
                .filter(columnHandle -> ((PinotColumnHandle) columnHandle).getDataType() instanceof ArrayType)
                .findFirst().isPresent()) {
            return Optional.empty();
        }
        // If aggregates are present than no further aggregations
        // can be pushed down: there are currently no subqueries in pinot.
        // If there is an offset then do not push the aggregation down as the results will not be correct
        if (tableHandle.query().isPresent() &&
                (!isAggregationPushdownSupported(session, tableHandle.query(), aggregates, assignments) ||
                        !tableHandle.query().get().aggregateColumns().isEmpty() ||
                        tableHandle.query().get().aggregateInProjections() ||
                        tableHandle.query().get().offset().isPresent())) {
            return Optional.empty();
        }

        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        ImmutableList.Builder<PinotColumnHandle> aggregateColumnsBuilder = ImmutableList.builder();

        for (AggregateFunction aggregate : aggregates) {
            Optional<AggregateExpression> rewriteResult = aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
            rewriteResult = applyCountDistinct(session, aggregate, assignments, tableHandle, rewriteResult);
            if (rewriteResult.isEmpty()) {
                return Optional.empty();
            }
            AggregateExpression aggregateExpression = rewriteResult.get();
            PinotColumnHandle pinotColumnHandle = new PinotColumnHandle(aggregateExpression.fieldName(), aggregate.getOutputType(), aggregateExpression.expression(), false, true, aggregateExpression.returnNullOnEmptyGroup(), Optional.of(aggregateExpression.function()), Optional.of(aggregateExpression.argument()));
            aggregateColumnsBuilder.add(pinotColumnHandle);
            projections.add(new Variable(pinotColumnHandle.getColumnName(), pinotColumnHandle.getDataType()));
            resultAssignments.add(new Assignment(pinotColumnHandle.getColumnName(), pinotColumnHandle, pinotColumnHandle.getDataType()));
        }

        List<PinotColumnHandle> groupingColumns = getOnlyElement(groupingSets).stream()
                .map(PinotColumnHandle.class::cast)
                .map(PinotMetadata::toNonAggregateColumnHandle)
                .collect(toImmutableList());

        OptionalLong limitForDynamicTable = OptionalLong.empty();
        // Ensure that pinot default limit of 10 rows is not used
        // By setting the limit to maxRowsPerBrokerQuery + 1 the connector will
        // know when the limit was exceeded and throw an error
        if (tableHandle.limit().isEmpty() && !groupingColumns.isEmpty()) {
            limitForDynamicTable = OptionalLong.of(maxRowsPerBrokerQuery + 1);
        }
        List<PinotColumnHandle> aggregationColumns = aggregateColumnsBuilder.build();
        String newQuery = "";
        List<PinotColumnHandle> newSelections = groupingColumns;
        if (tableHandle.query().isPresent()) {
            newQuery = tableHandle.query().get().query();
            Map<String, PinotColumnHandle> projectionsMap = tableHandle.query().get().projections().stream()
                    .collect(toImmutableMap(PinotColumnHandle::getColumnName, identity()));
            groupingColumns = groupingColumns.stream()
                    .map(groupIngColumn -> projectionsMap.getOrDefault(groupIngColumn.getColumnName(), groupIngColumn))
                    .collect(toImmutableList());
            ImmutableList.Builder<PinotColumnHandle> newSelectionsBuilder = ImmutableList.<PinotColumnHandle>builder()
                    .addAll(groupingColumns);

            aggregationColumns = aggregationColumns.stream()
                    .map(aggregateExpression -> resolveAggregateExpressionWithAlias(aggregateExpression, projectionsMap))
                    .collect(toImmutableList());

            newSelections = newSelectionsBuilder.build();
        }

        DynamicTable dynamicTable = new DynamicTable(
                tableHandle.tableName(),
                Optional.empty(),
                newSelections,
                tableHandle.query().flatMap(DynamicTable::filter),
                groupingColumns,
                aggregationColumns,
                Optional.empty(),
                ImmutableList.of(),
                limitForDynamicTable,
                OptionalLong.empty(),
                ImmutableMap.of(),
                newQuery);
        tableHandle = new PinotTableHandle(
                tableHandle.schemaName(),
                tableHandle.tableName(),
                tableHandle.enableNullHandling(),
                tableHandle.constraint(),
                tableHandle.limit(),
                Optional.of(dynamicTable),
                tableHandle.nodes(),
                tableHandle.segmentCount(),
                tableHandle.dateTimeField());

        return Optional.of(new AggregationApplicationResult<>(tableHandle, projections.build(), resultAssignments.build(), ImmutableMap.of(), false));
    }

    public static PinotColumnHandle toNonAggregateColumnHandle(PinotColumnHandle columnHandle)
    {
        return new PinotColumnHandle(columnHandle.getColumnName(), columnHandle.getDataType(), quoteIdentifier(columnHandle.getColumnName()), false, false, true, Optional.empty(), Optional.empty());
    }

    private boolean isAggregationPushdownSupported(ConnectorSession session, Optional<DynamicTable> dynamicTable, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments)
    {
        if (dynamicTable.isEmpty()) {
            return true;
        }
        List<PinotColumnHandle> groupingColumns = dynamicTable.get().groupingColumns();
        if (groupingColumns.isEmpty()) {
            return true;
        }
        // Either second pass of applyAggregation or dynamic table exists
        if (aggregates.size() != 1) {
            return false;
        }
        AggregateFunction aggregate = getOnlyElement(aggregates);
        AggregateFunctionRule.RewriteContext<Void> context = new CountDistinctContext(assignments, session);

        return implementCountDistinct.getPattern().matches(aggregate, context);
    }

    private Optional<AggregateExpression> applyCountDistinct(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments, PinotTableHandle tableHandle, Optional<AggregateExpression> rewriteResult)
    {
        AggregateFunctionRule.RewriteContext<Void> context = new CountDistinctContext(assignments, session);

        if (implementCountDistinct.getPattern().matches(aggregate, context)) {
            Variable argument = (Variable) getOnlyElement(aggregate.getArguments());
            // If this is the second pass to applyAggregation for count distinct then
            // the first pass will have added the distinct column to the grouping columns,
            // otherwise do not push down the aggregation.
            // This is to avoid count(column_name) being pushed into pinot, which is currently unsupported.
            // Currently Pinot treats count(column_name) as count(*), i.e. it counts nulls.
            PinotColumnHandle columnHandle = (PinotColumnHandle) assignments.get(argument.getName());
            if (tableHandle.query().isEmpty() || tableHandle.query().get().groupingColumns().stream()
                    .noneMatch(groupingExpression -> groupingExpression.getColumnName().equals(columnHandle.getColumnName()))) {
                return Optional.empty();
            }
        }
        return rewriteResult;
    }

    private static PinotColumnHandle resolveAggregateExpressionWithAlias(PinotColumnHandle aggregateColumn, Map<String, PinotColumnHandle> projectionsMap)
    {
        checkState(aggregateColumn.isAggregate() && aggregateColumn.getPushedDownAggregateFunctionName().isPresent() && aggregateColumn.getPushedDownAggregateFunctionArgument().isPresent(), "Column is not a pushed down aggregate column");
        PinotColumnHandle selection = projectionsMap.get(aggregateColumn.getPushedDownAggregateFunctionArgument().get());
        if (selection != null && selection.isAliased()) {
            AggregateExpression pushedDownAggregateExpression = new AggregateExpression(aggregateColumn.getPushedDownAggregateFunctionName().get(),
                    aggregateColumn.getPushedDownAggregateFunctionArgument().get(),
                    aggregateColumn.isReturnNullOnEmptyGroup());
            AggregateExpression newPushedDownAggregateExpression = replaceIdentifier(pushedDownAggregateExpression, selection);

            return new PinotColumnHandle(pushedDownAggregateExpression.fieldName(),
                    aggregateColumn.getDataType(),
                    newPushedDownAggregateExpression.expression(),
                    true,
                    aggregateColumn.isAggregate(),
                    aggregateColumn.isReturnNullOnEmptyGroup(),
                    aggregateColumn.getPushedDownAggregateFunctionName(),
                    Optional.of(newPushedDownAggregateExpression.argument()));
        }
        return aggregateColumn;
    }

    private List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        String pinotTableName = pinotClient.getPinotTableNameFromTrinoTableName(tableName);
        return getPinotColumnHandlesForPinotSchema(pinotTableName).stream()
                .map(PinotColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
    }

    private static <K, V> V getFromCache(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.get(key);
        }
        catch (ExecutionException e) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Cannot fetch from cache " + key, e.getCause());
        }
    }

    private Map<String, ColumnHandle> getDynamicTableColumnHandles(PinotTableHandle pinotTableHandle)
    {
        checkState(pinotTableHandle.query().isPresent(), "dynamic table not present");
        DynamicTable dynamicTable = pinotTableHandle.query().get();

        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (PinotColumnHandle pinotColumnHandle : dynamicTable.projections()) {
            columnHandlesBuilder.put(pinotColumnHandle.getColumnName().toLowerCase(ENGLISH), pinotColumnHandle);
        }
        dynamicTable.aggregateColumns()
                .forEach(columnHandle -> columnHandlesBuilder.put(columnHandle.getColumnName().toLowerCase(ENGLISH), columnHandle));
        return columnHandlesBuilder.buildOrThrow();
    }

    private List<PinotColumnHandle> getPinotColumnHandlesForPinotSchema(String tableName)
    {
        Schema pinotTableSchema = getFromCache(pinotTableSchemaCache, tableName);
        return pinotTableSchema.getColumnNames().stream()
                .filter(columnName -> !columnName.startsWith("$")) // Hidden columns starts with "$", ignore them as we can't use them in PQL
                .map(columnName -> new PinotColumnHandle(columnName, typeConverter.toTrinoType(pinotTableSchema.getFieldSpecFor(columnName))))
                .collect(toImmutableList());
    }

    private static class CountDistinctContext
            implements AggregateFunctionRule.RewriteContext<Void>
    {
        private final Map<String, ColumnHandle> assignments;
        private final ConnectorSession session;

        CountDistinctContext(Map<String, ColumnHandle> assignments, ConnectorSession session)
        {
            this.assignments = requireNonNull(assignments, "assignments is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Map<String, ColumnHandle> getAssignments()
        {
            return assignments;
        }

        @Override
        public ConnectorSession getSession()
        {
            return session;
        }

        @Override
        public Optional<Void> rewriteExpression(ConnectorExpression expression)
        {
            throw new UnsupportedOperationException();
        }
    }
}
