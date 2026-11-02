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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
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
import io.trino.spi.connector.SourcePage;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionBundle;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.P4HyperLogLogType;
import io.trino.spi.type.QuantileDigestType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import net.datafaker.Faker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Random;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.filterKeys;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.faker.ColumnInfo.ALLOWED_VALUES_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.GENERATOR_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MAX_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.MIN_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.NULL_PROBABILITY_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.REFERENCES_PROPERTY;
import static io.trino.plugin.faker.ColumnInfo.STEP_PROPERTY;
import static io.trino.plugin.faker.Distribution.UNIFORM;
import static io.trino.plugin.faker.FakerPageSource.POWERS_OF_TEN;
import static io.trino.plugin.faker.FakerPageSource.hasGenerator;
import static io.trino.plugin.faker.GeneratorDetection.detectGenerator;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static io.trino.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class FakerMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    public static final String ROW_ID_COLUMN_NAME = "$row_id";
    public static final double MIN_SEQUENCE_RATIO = 0.98;
    public static final long MAX_DICTIONARY_SIZE = 1000L;
    public static final int MAX_DICTIONARY_VALUE_LENGTH = 1000;

    @GuardedBy("this")
    private final List<SchemaInfo> schemas = new ArrayList<>();
    private final double nullProbability;
    private final long defaultLimit;
    private final boolean isSequenceDetectionEnabled;
    private final boolean isDictionaryDetectionEnabled;
    private final boolean isGeneratorDetectionEnabled;
    private final FunctionBundle functionBundle;
    private final Locale locale;

    @GuardedBy("this")
    private final Map<SchemaTableName, TableInfo> tables = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

    @Inject
    public FakerMetadata(FakerConfig config, FunctionBundle functionBundle)
    {
        this.schemas.add(new SchemaInfo(SCHEMA_NAME, Map.of()));
        this.nullProbability = config.getNullProbability();
        this.defaultLimit = config.getDefaultLimit();
        this.isSequenceDetectionEnabled = config.isSequenceDetectionEnabled();
        this.isDictionaryDetectionEnabled = config.isDictionaryDetectionEnabled();
        this.isGeneratorDetectionEnabled = config.isGeneratorDetectionEnabled();
        this.functionBundle = requireNonNull(functionBundle, "functionBundle is null");
        this.locale = config.getLocale();
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
        return new FakerTableHandle(tableName, tableLimit, seed(tableName, tableInfo));
    }

    private static long seed(SchemaTableName tableName, TableInfo tableInfo)
    {
        Long seed = (Long) tableInfo.properties().get(TableInfo.SEED_PROPERTY);
        if (seed != null) {
            return seed;
        }
        // hash the names separately, since joining them with a period makes some pairs of names equal
        return XxHash64.hash(XxHash64.hash(utf8Slice(tableName.getSchemaName())), utf8Slice(tableName.getTableName()));
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
                .collect(toImmutableMap(ColumnInfo::name, column -> columnHandle(connectorSession, tableHandle.schemaTableName(), column)));
    }

    private synchronized FakerColumnHandle columnHandle(ConnectorSession session, SchemaTableName tableName, ColumnInfo column)
    {
        return columnHandle(session, tableName, column, ImmutableList.of());
    }

    // The referenced column is resolved every time the table is read, because the referenced table can be dropped,
    // recreated, or have its properties changed after the referencing table is created
    private synchronized FakerColumnHandle columnHandle(ConnectorSession session, SchemaTableName tableName, ColumnInfo column, List<String> referencingColumns)
    {
        String references = (String) column.metadata().getProperties().get(REFERENCES_PROPERTY);
        if (references == null) {
            return column.handle();
        }
        String columnName = tableName + "." + column.name();
        if (referencingColumns.contains(columnName)) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "Column references form a cycle: %s -> %s".formatted(String.join(" -> ", referencingColumns), columnName));
        }
        List<String> parts = Splitter.on('.').splitToList(references);
        if (parts.size() < 2 || parts.size() > 3 || parts.contains("")) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `%s` property must be in the 'table.column' or 'schema.table.column' format".formatted(REFERENCES_PROPERTY));
        }
        if (parts.size() == 2) {
            parts = ImmutableList.of(tableName.getSchemaName(), parts.get(0), parts.get(1));
        }
        SchemaTableName referencedTableName = new SchemaTableName(parts.get(0), parts.get(1));
        String referencedColumnName = parts.get(2).toLowerCase(ENGLISH);

        TableInfo referencedTable = tables.get(referencedTableName);
        if (referencedTable == null) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "Table '%s' referenced by column '%s' does not exist".formatted(referencedTableName, columnName));
        }
        ColumnInfo referencedColumn = referencedTable.columns().stream()
                .filter(candidate -> candidate.name().equals(referencedColumnName))
                .findFirst()
                .orElseThrow(() -> new TrinoException(INVALID_COLUMN_PROPERTY, "Column '%s.%s' referenced by column '%s' does not exist".formatted(referencedTableName, referencedColumnName, columnName)));
        if (!referencedColumn.type().equals(column.type())) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "Column '%s' of type %s cannot reference column '%s.%s' of type %s".formatted(columnName, column.type(), referencedTableName, referencedColumnName, referencedColumn.type()));
        }
        // generating a value picks referenced rows until it finds one that is not null, and columns of types without a generator only contain nulls
        if (referencedColumn.handle().nullProbability() == 1 || !hasGenerator(referencedColumn.type())) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "Column '%s' cannot reference column '%s.%s', which only has null values".formatted(columnName, referencedTableName, referencedColumnName));
        }
        FakerTableHandle referencedTableHandle = (FakerTableHandle) getTableHandle(session, referencedTableName, Optional.empty(), Optional.empty());
        // tables created from an empty query have the default_limit property set to 0
        if (referencedTableHandle.limit() < 1) {
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "Table '%s' referenced by column '%s' has no rows".formatted(referencedTableName, columnName));
        }
        return column.handle().withReferencedColumn(new ReferencedColumn(
                referencedTableHandle.seed(),
                referencedTableHandle.limit(),
                columnHandle(session, referencedTableName, referencedColumn, ImmutableList.<String>builder()
                        .addAll(referencingColumns)
                        .add(columnName)
                        .build())));
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

        for (Entry<SchemaTableName, ConnectorViewDefinition> entry : getViews(session, schemaName).entrySet()) {
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
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
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
        ColumnInfo commentedColumn = oldInfo.column(column);
        List<ColumnInfo> columns = oldInfo.columns().stream()
                .map(columnInfo -> {
                    if (columnInfo.equals(commentedColumn)) {
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
        boolean schemaGeneratorDetectionEnabled = (boolean) schema.properties().getOrDefault(SchemaInfo.GENERATOR_DETECTION_ENABLED, isGeneratorDetectionEnabled);
        boolean tableGeneratorDetectionEnabled = (boolean) tableMetadata.getProperties().getOrDefault(TableInfo.GENERATOR_DETECTION_ENABLED, schemaGeneratorDetectionEnabled);

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        int columnId = 0;
        for (; columnId < tableMetadata.getColumns().size(); columnId++) {
            ColumnMetadata column = tableMetadata.getColumns().get(columnId);
            IntervalValues.checkSupported(column.getType());
            if (tableGeneratorDetectionEnabled) {
                column = withDetectedGenerator(column);
            }
            FakerColumnHandle handle = FakerColumnHandle.of(columnId, column, tableNullProbability);
            checkGeneratedValue(handle);
            columns.add(new ColumnInfo(handle, column));
        }

        columns.add(new ColumnInfo(
                new FakerColumnHandle(
                        columnId,
                        ROW_ID_COLUMN_NAME,
                        BIGINT,
                        0,
                        "",
                        Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 0L)), false),
                        ValueSet.of(BIGINT, 1L),
                        Optional.empty(),
                        UNIFORM,
                        ImmutableList.of()),
                ColumnMetadata.builder()
                        .setName(ROW_ID_COLUMN_NAME)
                        .setType(BIGINT)
                        .setHidden(true)
                        .setNullable(false)
                        .build()));

        TableInfo tableInfo = new TableInfo(
                columns.build(),
                tableMetadata.getProperties(),
                tableMetadata.getComment());
        // the table is added before validating references, so its columns can reference each other
        tables.put(tableName, tableInfo);
        try {
            tableInfo.columns().forEach(column -> columnHandle(session, tableName, column));
        }
        catch (RuntimeException e) {
            tables.remove(tableName);
            throw e;
        }

        return new FakerOutputTableHandle(tableName);
    }

    private void checkGeneratedValue(FakerColumnHandle column)
    {
        if (column.generator() == null || isCharacterType(column.type())) {
            return;
        }
        // Use a fixed seed so that the same column definition is always either accepted or rejected
        Faker faker = new Faker(locale, new Random(1));
        column.parseGeneratedValue(faker.expression(column.generator()));
    }

    private static ColumnMetadata withDetectedGenerator(ColumnMetadata column)
    {
        Map<String, Object> properties = column.getProperties();
        if (properties.containsKey(GENERATOR_PROPERTY) || properties.containsKey(ALLOWED_VALUES_PROPERTY) || properties.containsKey(REFERENCES_PROPERTY)) {
            return column;
        }
        return detectGenerator(column.getName(), column.getType())
                .map(generator -> ColumnMetadata.builderFrom(column)
                        .setProperties(ImmutableMap.<String, Object>builder()
                                .putAll(properties)
                                .put(GENERATOR_PROPERTY, generator)
                                .buildOrThrow())
                        .build())
                .orElse(column);
    }

    private static boolean isRangeType(Type type)
    {
        return hasGenerator(type) && !(type instanceof BooleanType ||
                type instanceof HyperLogLogType ||
                type instanceof QuantileDigestType ||
                type.getBaseName().equals(StandardTypes.TDIGEST) ||
                type instanceof P4HyperLogLogType ||
                isCharacterType(type) ||
                type instanceof RowType ||
                type instanceof ArrayType ||
                type instanceof MapType ||
                type.getBaseName().equals(StandardTypes.JSON) ||
                type.getBaseName().equals(StandardTypes.IPADDRESS) ||
                type instanceof UuidType);
    }

    private static boolean isCharacterType(Type type)
    {
        return type instanceof CharType || type instanceof VarcharType || type instanceof VarbinaryType;
    }

    private static boolean isTextType(Type type)
    {
        return type instanceof CharType || type instanceof VarcharType;
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
        ImmutableMap.Builder<String, Long> maxValueSizesBuilder = ImmutableMap.builder();
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
                    case MAX_VALUE_SIZE_IN_BYTES -> maxValueSizesBuilder.put(columnName, BIGINT.getLong(block, 0));
                    default -> throw new IllegalArgumentException("Unexpected column statistic type: " + metadata.getStatisticType());
                }
            });
        }
        Map<String, Object> minimums = minimumsBuilder.buildOrThrow();
        Map<String, Object> maximums = maximumsBuilder.buildOrThrow();
        Map<String, Long> distinctValues = distinctValuesBuilder.buildOrThrow();
        Map<String, Long> nonNullValues = info.properties().containsKey(TableInfo.NULL_PROBABILITY_PROPERTY) ? ImmutableMap.of() : nonNullValuesBuilder.buildOrThrow();
        Map<String, Long> maxValueSizes = maxValueSizesBuilder.buildOrThrow();

        if (!info.properties().containsKey(TableInfo.DEFAULT_LIMIT_PROPERTY) && rowCount != null) {
            info = info.withProperties(ImmutableMap.<String, Object>builder()
                    .putAll(info.properties())
                    .put(TableInfo.DEFAULT_LIMIT_PROPERTY, rowCount)
                    .buildOrThrow());
        }

        long finalRowCount = requireNonNullElse(rowCount, 1L);
        SchemaInfo schema = getSchema(tableName.getSchemaName());
        boolean isSchemaSequenceDetectionEnabled = (boolean) schema.properties().getOrDefault(SchemaInfo.SEQUENCE_DETECTION_ENABLED, isSequenceDetectionEnabled);
        boolean isTableSequenceDetectionEnabled = (boolean) info.properties().getOrDefault(TableInfo.SEQUENCE_DETECTION_ENABLED, isSchemaSequenceDetectionEnabled);
        Map<String, List<Object>> columnValues = getColumnValues(tableName, info, distinctValues, minimums, maximums, maxValueSizes);
        return info.withColumns(columns.stream()
                .map(column -> createColumnInfoFromStats(
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
        FakerColumnHandle handle = column.handle();
        Map<String, Object> properties = new HashMap<>(column.metadata().getProperties());
        if (allowedValues != null) {
            handle = handle.withDomain(Domain.create(ValueSet.copyOf(column.type(), allowedValues), false));
            properties.put(ALLOWED_VALUES_PROPERTY, allowedValues.stream()
                    .map(value -> Literal.format(column.type(), value))
                    .collect(toImmutableList()));
        }
        else if (isRangeType(column.type()) && min != null && max != null) {
            handle = handle.withDomain(Domain.create(ValueSet.ofRanges(Range.range(column.type(), min, true, max, true)), false));
            properties.put(MIN_PROPERTY, Literal.format(column.type(), min));
            properties.put(MAX_PROPERTY, Literal.format(column.type(), max));
        }
        if (nonNullValues.isPresent()) {
            // columns of types without a generator only contain nulls
            double nullProbability = hasGenerator(column.type()) ? 1 - (double) nonNullValues.get() / rowCount : 1;
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

    private Map<String, List<Object>> getColumnValues(SchemaTableName tableName, TableInfo info, Map<String, Long> distinctValues, Map<String, Object> minimums, Map<String, Object> maximums, Map<String, Long> maxValueSizes)
    {
        boolean schemaDictionaryDetectionEnabled = (boolean) getSchema(tableName.getSchemaName()).properties().getOrDefault(SchemaInfo.DICTIONARY_DETECTION_ENABLED, isDictionaryDetectionEnabled);
        boolean tableDictionaryDetectionEnabled = (boolean) info.properties().getOrDefault(TableInfo.DICTIONARY_DETECTION_ENABLED, schemaDictionaryDetectionEnabled);
        if (!tableDictionaryDetectionEnabled || distinctValues.isEmpty()) {
            return ImmutableMap.of();
        }
        List<ColumnInfo> columns = info.columns();
        Map<String, FakerColumnHandle> columnHandles = info.columns().stream()
                .collect(toImmutableMap(ColumnInfo::name, column -> column.handle().withNullProbability(0)));
        ImmutableMap.Builder<String, List<Object>> columnValues = ImmutableMap.builder();
        Random random = new Random(seed(tableName, info));
        Faker faker = new Faker(locale, random);
        // maximum value sizes are only collected for text columns, and generated text values are never empty
        maxValueSizes.forEach((column, maxValueSize) -> {
            long count = distinctValues.getOrDefault(column, Long.MAX_VALUE);
            FakerColumnHandle handle = columnHandles.get(column);
            // Generators produce more realistic values than random dictionaries
            if (count <= MAX_DICTIONARY_SIZE && maxValueSize > 0 && handle.generator() == null) {
                columnValues.put(column, textValues(faker, random, handle.type(), count, maxValueSize));
            }
        });
        List<FakerColumnHandle> dictionaryColumns = distinctValues.entrySet().stream()
                // columns with only nulls have no values to build a dictionary from
                .filter(entry -> entry.getValue() > 0 && entry.getValue() <= MAX_DICTIONARY_SIZE)
                .map(entry -> columnHandles.get(entry.getKey()))
                .filter(Objects::nonNull)
                // Generators produce more realistic values than random dictionaries
                .filter(column -> column.generator() == null)
                .filter(column -> isRangeType(column.type()))
                .map(column -> !minimums.containsKey(column.name()) ? column : column.withDomain(Domain.create(ValueSet.ofRanges(Range.range(
                        column.type(),
                        minimums.get(column.name()),
                        true,
                        maximums.get(column.name()),
                        true)), false)))
                .collect(toImmutableList());
        if (dictionaryColumns.isEmpty()) {
            return columnValues.buildOrThrow();
        }
        try (FakerPageSource pageSource = new FakerPageSource(locale, seed(tableName, info), dictionaryColumns, 0, MAX_DICTIONARY_SIZE * 2)) {
            SourcePage page = null;
            while (page == null) {
                page = pageSource.getNextSourcePage();
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

    private static List<Object> textValues(Faker faker, Random random, Type type, long count, long maxValueSize)
    {
        int declaredLength = switch (type) {
            case CharType charType -> charType.getLength();
            case VarcharType varcharType -> varcharType.getLength().orElse(Integer.MAX_VALUE);
            default -> throw new IllegalArgumentException("Unsupported type " + type);
        };
        // the size in bytes is only an upper bound of the length in characters, unless all characters are ASCII,
        // and allowed values are stored in the table metadata and column handles, so limit their length
        int maxLength = toIntExact(min(maxValueSize, min(declaredLength, MAX_DICTIONARY_VALUE_LENGTH)));
        Set<Object> values = new LinkedHashSet<>();
        // there might not be enough distinct short values, so stop after a limited number of attempts
        for (long attempt = 0; values.size() < count && attempt < count * 100; attempt++) {
            values.add(utf8Slice(faker.lorem().maxLengthSentence(1 + random.nextInt(maxLength))));
        }
        return ImmutableList.copyOf(values);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
    {
        return new TableStatisticsMetadata(
                tableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .flatMap(column -> columnStatisticMetadata(column).stream())
                        .collect(toImmutableSet()),
                Set.of(ROW_COUNT),
                List.of());
    }

    private static List<ColumnStatisticMetadata> columnStatisticMetadata(ColumnMetadata column)
    {
        ImmutableList.Builder<ColumnStatisticType> statisticTypes = ImmutableList.builder();
        if (column.getType().isOrderable()) {
            statisticTypes.add(MIN_VALUE, MAX_VALUE);
        }
        // the number of distinct values is computed using approx_distinct, which only accepts comparable types
        if (column.getType().isComparable()) {
            statisticTypes.add(NUMBER_OF_DISTINCT_VALUES);
        }
        statisticTypes.add(NUMBER_OF_NON_NULL_VALUES);
        if (isTextType(column.getType())) {
            statisticTypes.add(MAX_VALUE_SIZE_IN_BYTES);
        }
        return statisticTypes.build().stream()
                .map(statisticType -> new ColumnStatisticMetadata(column.getName(), statisticType))
                .collect(toImmutableList());
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
        return schemaName.equals(SCHEMA_NAME) ? functionBundle.getFunctions() : List.of();
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        if (!name.schemaName().equals(SCHEMA_NAME)) {
            return List.of();
        }
        return functionBundle.getFunctions().stream()
                .filter(function -> function.getCanonicalName().equals(name.functionName()))
                .collect(toImmutableList());
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return functionBundle.getFunctions().stream()
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
        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);

        TableStatistics.Builder tableStatisitics = TableStatistics.builder();
        tableStatisitics.setRowCount(Estimate.of(fakerTableHandle.limit()));

        info.columns().forEach(columnInfo -> {
            Object min = PropertyValues.propertyValue(columnInfo.metadata(), MIN_PROPERTY);
            Object max = PropertyValues.propertyValue(columnInfo.metadata(), MAX_PROPERTY);
            Object step = PropertyValues.propertyValue(columnInfo.metadata(), STEP_PROPERTY);
            Collection<?> allowedValues = (Collection<?>) columnInfo.metadata().getProperties().get(ALLOWED_VALUES_PROPERTY); // skip parsing as we don't need the values

            checkState(allowedValues == null || (min == null && max == null), "The `%s` property cannot be set together with `%s` and `%s` properties".formatted(ALLOWED_VALUES_PROPERTY, MIN_PROPERTY, MAX_PROPERTY));

            ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
            // columns of types without a generator only contain nulls
            double nullsFraction = hasGenerator(columnInfo.type()) ? columnInfo.handle().nullProbability() : 1;
            columnStatistics.setNullsFraction(Estimate.of(nullsFraction));
            if (nullsFraction == 1) {
                columnStatistics.setDistinctValuesCount(Estimate.zero());
            }
            else if (allowedValues != null) {
                // the domain doesn't contain duplicate values
                columnStatistics.setDistinctValuesCount(Estimate.of(Math.min(columnInfo.handle().domain().getValues().getDiscreteSet().size(), fakerTableHandle.limit())));
            }
            else if (min != null && max != null) {
                Type type = columnInfo.metadata().getType();
                distinctValuesCount(type, min, max, step)
                        .ifPresent(count -> columnStatistics.setDistinctValuesCount(Estimate.of(Math.min(count, fakerTableHandle.limit()))));
                DoubleRange.from(type, min, max).ifPresent(columnStatistics::setRange);
            }
            tableStatisitics.setColumnStatistics(columnHandles.get(columnInfo.name()), columnStatistics.build());
        });
        return tableStatisitics.build();
    }

    /**
     * Counts values between {@code min} and {@code max} the same way {@link FakerPageSource} generates them.
     * Other types are skipped, because their values are floating point numbers, are not stored in a long, or have a time zone offset.
     */
    private static OptionalDouble distinctValuesCount(Type type, Object min, Object max, Object step)
    {
        if (BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type) || (type instanceof DecimalType decimalType && decimalType.isShort())) {
            return distinctValuesCount((long) min, (long) max, 1, (Long) step, 1);
        }
        if (DATE.equals(type)) {
            return distinctValuesCount((long) min, (long) max, 1, (Long) step, NANOSECONDS_PER_DAY);
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            long factor = POWERS_OF_TEN[6 - timestampType.getPrecision()];
            return distinctValuesCount((long) min, (long) max, factor, (Long) step, factor * NANOSECONDS_PER_MICROSECOND);
        }
        if (type instanceof TimestampWithTimeZoneType timestampType && timestampType.isShort()) {
            long factor = POWERS_OF_TEN[3 - timestampType.getPrecision()];
            return distinctValuesCount(unpackMillisUtc((long) min), unpackMillisUtc((long) max), factor, (Long) step, factor * NANOSECONDS_PER_MILLISECOND);
        }
        if (type instanceof TimeType timeType) {
            long factor = POWERS_OF_TEN[12 - timeType.getPrecision()];
            return distinctValuesCount((long) min, (long) max, factor, step == null ? null : (long) step * PICOSECONDS_PER_NANOSECOND, factor);
        }
        if (IntervalValues.isInterval(type)) {
            long quantum = IntervalValues.quantum(type);
            return distinctValuesCount((long) min, (long) max, quantum, (Long) step, quantum);
        }
        return OptionalDouble.empty();
    }

    private static OptionalDouble distinctValuesCount(long min, long max, long valueUnit, Long step, long stepUnit)
    {
        // converting large values to a double loses precision, so only do that when max - min overflows a long
        long difference = max - min;
        double valueUnits = (difference >= 0 ? (double) difference : (double) max - min) / valueUnit;
        if (step == null) {
            return OptionalDouble.of(valueUnits + 1);
        }
        // a step that is not a multiple of the type precision generates values at uneven intervals, and a step that is not positive does not generate a sequence
        if (step <= 0 || step % stepUnit != 0) {
            return OptionalDouble.empty();
        }
        // FakerPageSource generates max after the last full step, which might be shorter
        return OptionalDouble.of(Math.ceil(valueUnits / (step / stepUnit)) + 1);
    }
}
