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

package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.inject.Inject;
import net.datafaker.Faker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.filterKeys;
import static io.trino.plugin.faker.ColumnInfo.ALLOWED_VALUES_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MAX_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MIN_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.NULL_PROBABILITY_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.STEP_PROPERTY;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FakerMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    public static final String ROW_ID_COLUMN_NAME = "$row_id";
    public static final double MIN_SEQUENCE_RATIO = 0.98;
    public static final long MAX_DICTIONARY_SIZE = 1000L;

    @GuardedBy("this")
    private final List<SchemaInfo> schemas = new ArrayList<>();
    private final double nullProbability;
    private final long defaultLimit;
    private final boolean isSequenceDetectionEnabled;
    private final boolean isDictionaryDetectionEnabled;
    private final FakerFunctionProvider functionsProvider;

    private final Random random;
    private final Faker faker;

    @GuardedBy("this")
    private final Map<SchemaTableName, TableInfo> tables = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

    @Inject
    public FakerMetadata(FakerConfig config, FakerFunctionProvider functionProvider)
    {
        this.schemas.add(new SchemaInfo(SCHEMA_NAME, Map.of()));
        this.nullProbability = config.getNullProbability();
        this.defaultLimit = config.getDefaultLimit();
        this.isSequenceDetectionEnabled = config.isSequenceDetectionEnabled();
        this.isDictionaryDetectionEnabled = config.isDictionaryDetectionEnabled();
        this.functionsProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.random = new Random(1);
        this.faker = new Faker(random);
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return schemas.stream()
                .map(SchemaInfo::name)
                .collect(toImmutableList());
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        if (schemas.stream().anyMatch(schema -> schema.name().equals(schemaName))) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS, format("Schema '%s' already exists", schemaName));
        }
        schemas.add(new SchemaInfo(schemaName, properties));
    }

    @Override
    public synchronized void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        verify(schemas.remove(getSchema(schemaName)));
    }

    private synchronized SchemaInfo getSchema(String name)
    {
        Optional<SchemaInfo> schema = schemas.stream()
                .filter(schemaInfo -> schemaInfo.name().equals(name))
                .findAny();
        if (schema.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", name));
        }
        return schema.get();
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        SchemaInfo schema = getSchema(tableName.getSchemaName());
        TableInfo tableInfo = tables.get(tableName);
        if (tableInfo == null) {
            return null;
        }
        long schemaLimit = (long) schema.properties().getOrDefault(SchemaInfo.DEFAULT_LIMIT_PROPERTY, defaultLimit);
        long tableLimit = (long) tables.get(tableName).properties().getOrDefault(TableInfo.DEFAULT_LIMIT_PROPERTY, schemaLimit);
        return new FakerTableHandle(tableName, tableLimit);
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = (FakerTableHandle) connectorTableHandle;
        SchemaTableName name = tableHandle.schemaTableName();
        TableInfo tableInfo = tables.get(tableHandle.schemaTableName());
        return new ConnectorTableMetadata(
                name,
                tableInfo.columns().stream().map(ColumnInfo::metadata).collect(toImmutableList()),
                tableInfo.properties(),
                tableInfo.comment());
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return Stream.concat(tables.keySet().stream(), views.keySet().stream())
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = (FakerTableHandle) connectorTableHandle;
        return tables.get(tableHandle.schemaTableName())
                .columns().stream()
                .collect(toImmutableMap(ColumnInfo::name, ColumnInfo::handle));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        FakerTableHandle tableHandle = (FakerTableHandle) connectorTableHandle;
        return tables.get(tableHandle.schemaTableName())
                .column(columnHandle)
                .metadata();
    }

    @Override
    public synchronized Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();

        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new)
                .orElseGet(SchemaTablePrefix::new);
        tables.entrySet().stream()
                .filter(entry -> prefix.matches(entry.getKey()))
                .forEach(entry -> {
                    SchemaTableName name = entry.getKey();
                    RelationColumnsMetadata columns = RelationColumnsMetadata.forTable(
                            name,
                            entry.getValue().columns().stream()
                                    .map(ColumnInfo::metadata)
                                    .collect(toImmutableList()));
                    relationColumns.put(name, columns);
                });

        for (Map.Entry<SchemaTableName, ConnectorViewDefinition> entry : getViews(session, schemaName).entrySet()) {
            relationColumns.put(entry.getKey(), RelationColumnsMetadata.forView(entry.getKey(), entry.getValue().getColumns()));
        }

        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        tables.remove(handle.schemaTableName());
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());
        checkTableNotExists(newTableName);
        checkViewNotExists(newTableName);

        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName oldTableName = handle.schemaTableName();

        tables.put(newTableName, tables.remove(oldTableName));
    }

    @Override
    public synchronized void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName tableName = handle.schemaTableName();

        TableInfo oldInfo = tables.get(tableName);
        Map<String, Object> newProperties = Stream.concat(
                        oldInfo.properties().entrySet().stream()
                                .filter(entry -> !properties.containsKey(entry.getKey())),
                        properties.entrySet().stream()
                                .filter(entry -> entry.getValue().isPresent())
                                .map(entry -> Map.entry(entry.getKey(), entry.getValue().get())))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        tables.put(tableName, oldInfo.withProperties(newProperties));
    }

    @Override
    public synchronized void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName tableName = handle.schemaTableName();

        TableInfo oldInfo = requireNonNull(tables.get(tableName), "tableInfo is null");
        tables.put(tableName, oldInfo.withComment(comment));
    }

    @Override
    public synchronized void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName tableName = handle.schemaTableName();

        TableInfo oldInfo = tables.get(tableName);
        List<ColumnInfo> columns = oldInfo.columns().stream()
                .map(columnInfo -> {
                    if (columnInfo.handle().equals(column)) {
                        if (ROW_ID_COLUMN_NAME.equals(columnInfo.handle().name())) {
                            throw new TrinoException(INVALID_COLUMN_REFERENCE, "Cannot set comment for %s column".formatted(ROW_ID_COLUMN_NAME));
                        }
                        return columnInfo.withComment(comment);
                    }
                    return columnInfo;
                })
                .collect(toImmutableList());
        tables.put(tableName, oldInfo.withColumns(columns));
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES, false);
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized FakerOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName tableName = tableMetadata.getTable();
        SchemaInfo schema = getSchema(tableName.getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        checkViewNotExists(tableMetadata.getTable());

        double schemaNullProbability = (double) schema.properties().getOrDefault(SchemaInfo.NULL_PROBABILITY_PROPERTY, nullProbability);
        double tableNullProbability = (double) tableMetadata.getProperties().getOrDefault(TableInfo.NULL_PROBABILITY_PROPERTY, schemaNullProbability);

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        int columnId = 0;
        for (; columnId < tableMetadata.getColumns().size(); columnId++) {
            ColumnMetadata column = tableMetadata.getColumns().get(columnId);
            columns.add(new ColumnInfo(
                    FakerColumnHandle.of(columnId, column, tableNullProbability),
                    column));
        }

        columns.add(new ColumnInfo(
                new FakerColumnHandle(
                        columnId,
                        ROW_ID_COLUMN_NAME,
                        BIGINT,
                        0,
                        "",
                        Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 0L)), false),
                        ValueSet.of(BIGINT, 1L)),
                ColumnMetadata.builder()
                        .setName(ROW_ID_COLUMN_NAME)
                        .setType(BIGINT)
                        .setHidden(true)
                        .setNullable(false)
                        .build()));

        tables.put(tableName, new TableInfo(
                columns.build(),
                tableMetadata.getProperties(),
                tableMetadata.getComment()));

        return new FakerOutputTableHandle(tableName);
    }

    private static boolean isRangeType(Type type)
    {
        return !(type instanceof CharType || type instanceof VarcharType || type instanceof VarbinaryType);
    }

    private static boolean isSequenceType(Type type)
    {
        return BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type);
    }

    private synchronized void checkSchemaExists(String schemaName)
    {
        if (schemas.stream().noneMatch(schema -> schema.name().equals(schemaName))) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }
    }

    private synchronized void checkTableNotExists(SchemaTableName tableName)
    {
        if (tables.containsKey(tableName)) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table '%s' already exists", tableName));
        }
    }

    private synchronized void checkViewNotExists(SchemaTableName viewName)
    {
        if (views.containsKey(viewName)) {
            throw new TrinoException(ALREADY_EXISTS, format("View '%s' already exists", viewName));
        }
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        FakerOutputTableHandle fakerOutputHandle = (FakerOutputTableHandle) tableHandle;

        SchemaTableName tableName = fakerOutputHandle.schemaTableName();

        TableInfo info = tables.get(tableName);
        requireNonNull(info, "info is null");

        tables.put(tableName, createTableInfoFromStats(tableName, info, computedStatistics));

        return Optional.empty();
    }

    private synchronized TableInfo createTableInfoFromStats(SchemaTableName tableName, TableInfo info, Collection<ComputedStatistics> computedStatistics)
    {
        if (computedStatistics.isEmpty()) {
            return info;
        }
        ImmutableMap.Builder<String, Object> minimumsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Object> maximumsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Long> distinctValuesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Long> nonNullValuesBuilder = ImmutableMap.builder();
        List<ColumnInfo> columns = info.columns();
        Map<String, Type> types = columns.stream().collect(toImmutableMap(ColumnInfo::name, ColumnInfo::type));
        Long rowCount = null;
        Optional<ComputedStatistics> optionalStatistic = computedStatistics.stream().reduce((_, _) -> {
            throw new IllegalStateException("Found more than one computed statistic");
        });
        if (optionalStatistic.isPresent()) {
            ComputedStatistics statistic = optionalStatistic.get();
            if (!statistic.getTableStatistics().get(ROW_COUNT).isNull(0)) {
                rowCount = BIGINT.getLong(statistic.getTableStatistics().get(ROW_COUNT), 0);
            }
            statistic.getColumnStatistics().forEach((metadata, block) -> {
                checkState(block.getPositionCount() == 1, "Expected a single position in aggregation result block");
                String columnName = metadata.getColumnName();
                Type type = types.get(columnName);
                if (block.isNull(0) || type == null) {
                    return;
                }
                switch (metadata.getStatisticType()) {
                    case MIN_VALUE -> minimumsBuilder.put(columnName, requireNonNull(readNativeValue(type, block, 0)));
                    case MAX_VALUE -> maximumsBuilder.put(columnName, requireNonNull(readNativeValue(type, block, 0)));
                    case NUMBER_OF_DISTINCT_VALUES -> distinctValuesBuilder.put(columnName, BIGINT.getLong(block, 0));
                    case NUMBER_OF_NON_NULL_VALUES -> nonNullValuesBuilder.put(columnName, BIGINT.getLong(block, 0));
                    default -> throw new IllegalArgumentException("Unexpected column statistic type: " + metadata.getStatisticType());
                }
            });
        }
        Map<String, Object> minimums = minimumsBuilder.buildOrThrow();
        Map<String, Object> maximums = maximumsBuilder.buildOrThrow();
        Map<String, Long> distinctValues = distinctValuesBuilder.buildOrThrow();
        Map<String, Long> nonNullValues = info.properties().containsKey(TableInfo.NULL_PROBABILITY_PROPERTY) ? ImmutableMap.of() : nonNullValuesBuilder.buildOrThrow();

        if (!info.properties().containsKey(TableInfo.DEFAULT_LIMIT_PROPERTY) && rowCount != null) {
            info = info.withProperties(ImmutableMap.<String, Object>builder()
                    .putAll(info.properties())
                    .put(TableInfo.DEFAULT_LIMIT_PROPERTY, rowCount)
                    .buildOrThrow());
        }

        long finalRowCount = firstNonNull(rowCount, 1L);
        SchemaInfo schema = getSchema(tableName.getSchemaName());
        boolean isSchemaSequenceDetectionEnabled = (boolean) schema.properties().getOrDefault(SchemaInfo.SEQUENCE_DETECTION_ENABLED, isSequenceDetectionEnabled);
        boolean isTableSequenceDetectionEnabled = (boolean) info.properties().getOrDefault(TableInfo.SEQUENCE_DETECTION_ENABLED, isSchemaSequenceDetectionEnabled);
        Map<String, List<Object>> columnValues = getColumnValues(tableName, info, distinctValues, minimums, maximums);
        return info.withColumns(columns.stream().map(column -> createColumnInfoFromStats(
                        column,
                        minimums.get(column.name()),
                        maximums.get(column.name()),
                        requireNonNull(distinctValues.getOrDefault(column.name(), 0L)),
                        Optional.ofNullable(nonNullValues.get(column.name())),
                        finalRowCount,
                        isTableSequenceDetectionEnabled,
                        columnValues.get(column.name())))
                .collect(toImmutableList()));
    }

    private static ColumnInfo createColumnInfoFromStats(ColumnInfo column, Object min, Object max, long distinctValues, Optional<Long> nonNullValues, long rowCount, boolean isSequenceDetectionEnabled, List<Object> allowedValues)
    {
        if (!isRangeType(column.type())) {
            return column;
        }
        FakerColumnHandle handle = column.handle();
        Map<String, Object> properties = new HashMap<>(column.metadata().getProperties());
        if (allowedValues != null) {
            handle = handle.withDomain(Domain.create(ValueSet.copyOf(column.type(), allowedValues), false));
            properties.put(ALLOWED_VALUES_PROPERTY, allowedValues.stream()
                    .map(value -> Literal.format(column.type(), value))
                    .collect(toImmutableList()));
        }
        else if (min != null && max != null) {
            handle = handle.withDomain(Domain.create(ValueSet.ofRanges(Range.range(column.type(), min, true, max, true)), false));
            properties.put(MIN_PROPERTY, Literal.format(column.type(), min));
            properties.put(MAX_PROPERTY, Literal.format(column.type(), max));
        }
        if (nonNullValues.isPresent()) {
            double nullProbability = 1 - (double) nonNullValues.get() / rowCount;
            handle = handle.withNullProbability(nullProbability);
            properties.put(NULL_PROBABILITY_PROPERTY, nullProbability);
        }
        // Only include types that support generating sequences in FakerPageSource,
        // but don't include types with configurable precision, dates, or intervals.
        // The number of distinct values is an approximation, so compare it with a margin.
        if (isSequenceDetectionEnabled && isSequenceType(column.type()) && (double) distinctValues / rowCount >= MIN_SEQUENCE_RATIO) {
            handle = handle.withStep(ValueSet.of(column.type(), 1L));
            properties.put(STEP_PROPERTY, "1");
        }
        return column
                .withHandle(handle)
                .withProperties(properties);
    }

    private Map<String, List<Object>> getColumnValues(SchemaTableName tableName, TableInfo info, Map<String, Long> distinctValues, Map<String, Object> minimums, Map<String, Object> maximums)
    {
        boolean schemaDictionaryDetectionEnabled = (boolean) getSchema(tableName.getSchemaName()).properties().getOrDefault(SchemaInfo.DICTIONARY_DETECTION_ENABLED, isDictionaryDetectionEnabled);
        boolean tableDictionaryDetectionEnabled = (boolean) info.properties().getOrDefault(TableInfo.DICTIONARY_DETECTION_ENABLED, schemaDictionaryDetectionEnabled);
        if (!tableDictionaryDetectionEnabled || distinctValues.isEmpty()) {
            return ImmutableMap.of();
        }
        List<ColumnInfo> columns = info.columns();
        Map<String, FakerColumnHandle> columnHandles = info.columns().stream()
                .collect(toImmutableMap(ColumnInfo::name, column -> column.handle().withNullProbability(0)));
        List<FakerColumnHandle> dictionaryColumns = distinctValues.entrySet().stream()
                .filter(entry -> entry.getValue() <= MAX_DICTIONARY_SIZE)
                .map(entry -> columnHandles.get(entry.getKey()))
                .filter(Objects::nonNull)
                .filter(column -> isRangeType(column.type()))
                .map(column -> !minimums.containsKey(column.name()) ? column : column.withDomain(Domain.create(ValueSet.ofRanges(Range.range(
                        column.type(),
                        minimums.get(column.name()),
                        true,
                        maximums.get(column.name()),
                        true)), false)))
                .collect(toImmutableList());
        if (dictionaryColumns.isEmpty()) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<String, List<Object>> columnValues = ImmutableMap.builder();
        try (FakerPageSource pageSource = new FakerPageSource(faker, random, dictionaryColumns, 0, MAX_DICTIONARY_SIZE * 2)) {
            Page page = null;
            while (page == null) {
                page = pageSource.getNextPage();
            }
            Map<String, Type> types = columns.stream().collect(toImmutableMap(ColumnInfo::name, ColumnInfo::type));
            for (int channel = 0; channel < dictionaryColumns.size(); channel++) {
                String column = dictionaryColumns.get(channel).name();
                Block block = page.getBlock(channel);
                Type type = types.get(column);
                List<Object> values = IntStream.range(0, page.getPositionCount())
                        .mapToObj(position -> readNativeValue(type, block, position))
                        .distinct()
                        .limit(distinctValues.get(column))
                        .collect(toImmutableList());
                columnValues.put(column, values);
            }
        }
        return columnValues.buildOrThrow();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return new TableStatisticsMetadata(
                tableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .flatMap(column -> Stream.of(
                                new ColumnStatisticMetadata(column.getName(), MIN_VALUE),
                                new ColumnStatisticMetadata(column.getName(), MAX_VALUE),
                                new ColumnStatisticMetadata(column.getName(), NUMBER_OF_DISTINCT_VALUES),
                                new ColumnStatisticMetadata(column.getName(), NUMBER_OF_NON_NULL_VALUES)))
                        .collect(toImmutableSet()),
                Set.of(ROW_COUNT),
                List.of());
    }

    @Override
    public synchronized List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return views.keySet().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return ImmutableMap.copyOf(filterKeys(views, prefix::matches));
    }

    @Override
    public synchronized Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(views.get(viewName));
    }

    @Override
    public synchronized void createView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorViewDefinition definition,
            Map<String, Object> viewProperties,
            boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        checkSchemaExists(viewName.getSchemaName());
        checkTableNotExists(viewName);

        if (replace) {
            views.put(viewName, definition);
        }
        else if (views.putIfAbsent(viewName, definition) != null) {
            throw new TrinoException(ALREADY_EXISTS, "View '%s' already exists".formatted(viewName));
        }
    }

    @Override
    public synchronized void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        ConnectorViewDefinition view = getView(session, viewName).orElseThrow(() -> new ViewNotFoundException(viewName));
        views.put(viewName, new ConnectorViewDefinition(
                view.getOriginalSql(),
                view.getCatalog(),
                view.getSchema(),
                view.getColumns(),
                comment,
                view.getOwner(),
                view.isRunAsInvoker(),
                view.getPath()));
    }

    @Override
    public synchronized void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        ConnectorViewDefinition view = getView(session, viewName).orElseThrow(() -> new ViewNotFoundException(viewName));
        views.put(viewName, new ConnectorViewDefinition(
                view.getOriginalSql(),
                view.getCatalog(),
                view.getSchema(),
                view.getColumns().stream()
                        .map(currentViewColumn -> columnName.equals(currentViewColumn.getName()) ?
                                new ConnectorViewDefinition.ViewColumn(currentViewColumn.getName(), currentViewColumn.getType(), comment)
                                : currentViewColumn)
                        .collect(toImmutableList()),
                view.getComment(),
                view.getOwner(),
                view.isRunAsInvoker(),
                view.getPath()));
    }

    @Override
    public synchronized void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        checkSchemaExists(target.getSchemaName());
        if (!views.containsKey(source)) {
            throw new TrinoException(NOT_FOUND, "View not found: " + source);
        }
        checkTableNotExists(target);

        if (views.putIfAbsent(target, views.remove(source)) != null) {
            throw new TrinoException(ALREADY_EXISTS, "View '%s' already exists".formatted(target));
        }
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
    }

    @Override
    public synchronized Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        Optional<SchemaInfo> schema = schemas.stream()
                .filter(s -> s.name().equals(schemaName))
                .findAny();
        if (schema.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }

        return schema.get().properties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit)
    {
        FakerTableHandle fakerTable = (FakerTableHandle) table;
        if (fakerTable.limit() == limit) {
            return Optional.empty();
        }
        return Optional.of(new LimitApplicationResult<>(
                fakerTable.withLimit(limit),
                false,
                true));
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return functionsProvider.functionsMetadata();
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return functionsProvider.functionsMetadata().stream()
                .filter(function -> function.getCanonicalName().equals(name.getFunctionName()))
                .collect(toImmutableList());
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return functionsProvider.functionsMetadata().stream()
                .filter(function -> function.getFunctionId().equals(functionId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown function " + functionId));
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return FunctionDependencyDeclaration.NO_DEPENDENCIES;
    }

    @Override
    public synchronized TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        FakerTableHandle fakerTableHandle = (FakerTableHandle) tableHandle;
        TableInfo info = tables.get(fakerTableHandle.schemaTableName());

        TableStatistics.Builder tableStatisitics = TableStatistics.builder();
        tableStatisitics.setRowCount(Estimate.of(fakerTableHandle.limit()));

        info.columns().forEach(columnInfo -> {
            Object min = PropertyValues.propertyValue(columnInfo.metadata(), MIN_PROPERTY);
            Object max = PropertyValues.propertyValue(columnInfo.metadata(), MAX_PROPERTY);
            Object step = PropertyValues.propertyValue(columnInfo.metadata(), STEP_PROPERTY);
            Collection<?> allowedValues = (Collection<?>) columnInfo.metadata().getProperties().get(ALLOWED_VALUES_PROPERTY); // skip parsing as we don't need the values

            checkState(allowedValues == null || (min == null && max == null), "The `%s` property cannot be set together with `%s` and `%s` properties".formatted(ALLOWED_VALUES_PROPERTY, MIN_PROPERTY, MAX_PROPERTY));

            ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
            if (allowedValues != null) {
                columnStatistics.setDistinctValuesCount(Estimate.of(allowedValues.size()));
            }
            else {
                Type type = columnInfo.metadata().getType();
                if (min != null && max != null && type.getJavaType() == long.class) {
                    long distinctValuesCount = (long) max - (long) min;
                    if (step != null) {
                        distinctValuesCount = distinctValuesCount / (long) step;
                    }
                    columnStatistics.setDistinctValuesCount(Estimate.of(distinctValuesCount));
                }
            }
            columnStatistics.setNullsFraction(Estimate.of(columnInfo.handle().nullProbability()));
            tableStatisitics.setColumnStatistics(columnInfo.handle(), columnStatistics.build());
        });
        return tableStatisitics.build();
    }
}
