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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.FieldStatistics;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.client.IndexMetadata.DateTimeType;
import io.trino.plugin.elasticsearch.client.IndexMetadata.ObjectType;
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.elasticsearch.client.IndexMetadata.ScaledFloatType;
import io.trino.plugin.elasticsearch.client.IndexStatistics;
import io.trino.plugin.elasticsearch.ElasticsearchAggregate.Function;
import io.trino.plugin.elasticsearch.decoders.ArrayDecoder;
import io.trino.plugin.elasticsearch.decoders.BigintDecoder;
import io.trino.plugin.elasticsearch.decoders.BooleanDecoder;
import io.trino.plugin.elasticsearch.decoders.DoubleDecoder;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.IpAddressDecoder;
import io.trino.plugin.elasticsearch.decoders.RawJsonDecoder;
import io.trino.plugin.elasticsearch.decoders.RealDecoder;
import io.trino.plugin.elasticsearch.decoders.RowDecoder;
import io.trino.plugin.elasticsearch.decoders.SmallintDecoder;
import io.trino.plugin.elasticsearch.decoders.TimestampDecoder;
import io.trino.plugin.elasticsearch.decoders.TinyintDecoder;
import io.trino.plugin.elasticsearch.decoders.VarbinaryDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.ptf.RawQuery.RawQueryFunctionHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.elasticsearch.client.ResponseException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.plugin.elasticsearch.ElasticsearchSessionProperties.getFullTextPushdownMode;
import static io.trino.plugin.elasticsearch.ElasticsearchSessionProperties.isAggregationPushdownEnabled;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.SCAN;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private static final String PASSTHROUGH_QUERY_RESULT_COLUMN_NAME = "result";
    private static final ColumnMetadata PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA = ColumnMetadata.builder()
            .setName(PASSTHROUGH_QUERY_RESULT_COLUMN_NAME)
            .setType(VARCHAR)
            .setNullable(true)
            .setHidden(false)
            .build();

    private static final Map<String, ColumnHandle> PASSTHROUGH_QUERY_COLUMNS = ImmutableMap.of(
            PASSTHROUGH_QUERY_RESULT_COLUMN_NAME, new ElasticsearchColumnHandle(
                    ImmutableList.of(PASSTHROUGH_QUERY_RESULT_COLUMN_NAME),
                    VARCHAR,
                    new IndexMetadata.PrimitiveType("text"),
                    new VarcharDecoder.Descriptor(PASSTHROUGH_QUERY_RESULT_COLUMN_NAME),
                    false));

    // See https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html
    private static final FunctionName STARTS_WITH_FUNCTION_NAME = new FunctionName("starts_with");
    private static final FunctionName REGEXP_LIKE_FUNCTION_NAME = new FunctionName("regexp_like");
    private static final FunctionName SUBSTR_FUNCTION_NAME = new FunctionName("substr");
    private static final FunctionName SUBSTRING_FUNCTION_NAME = new FunctionName("substring");
    private static final Set<Integer> REGEXP_RESERVED_CHARACTERS = IntStream.of('.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '#', '@', '&', '<', '>', '~')
            .boxed()
            .collect(toImmutableSet());

    private final Type ipAddressType;
    private final ElasticsearchClient client;
    private final String schemaName;
    private final boolean statisticsEnabled;

    @Inject
    public ElasticsearchMetadata(TypeManager typeManager, ElasticsearchClient client, ElasticsearchConfig config)
    {
        this.ipAddressType = typeManager.getType(new TypeDescriptor(StandardTypes.IPADDRESS));
        this.client = requireNonNull(client, "client is null");
        this.schemaName = config.getDefaultSchema();
        this.statisticsEnabled = config.isStatisticsEnabled();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(schemaName);
    }

    @Override
    public ElasticsearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        requireNonNull(tableName, "tableName is null");

        if (tableName.getSchemaName().equals(schemaName)) {
            String[] parts = tableName.getTableName().split(":", 2);
            String table = parts[0];
            Optional<String> query = Optional.empty();
            if (parts.length == 2) {
                query = Optional.of(parts[1]);
            }

            if (client.indexExists(table) && !client.getIndexMetadata(table).schema().fields().isEmpty()) {
                return new ElasticsearchTableHandle(SCAN, schemaName, table, query);
            }
        }

        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            return new ConnectorTableMetadata(
                    new SchemaTableName(handle.schema(), handle.index()),
                    ImmutableList.of(PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA));
        }
        return getTableMetadata(handle.schema(), handle.index());
    }

    private ConnectorTableMetadata getTableMetadata(String schemaName, String tableName)
    {
        InternalTableMetadata internalTableMetadata = makeInternalTableMetadata(schemaName, tableName);
        return new ConnectorTableMetadata(new SchemaTableName(schemaName, tableName), internalTableMetadata.columnMetadata());
    }

    private InternalTableMetadata makeInternalTableMetadata(ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        return makeInternalTableMetadata(handle.schema(), handle.index());
    }

    private InternalTableMetadata makeInternalTableMetadata(String schema, String tableName)
    {
        IndexMetadata metadata = client.getIndexMetadata(tableName);
        List<IndexMetadata.Field> fields = getColumnFields(metadata);
        return new InternalTableMetadata(new SchemaTableName(schema, tableName), makeColumnMetadata(fields), makeColumnHandles(fields));
    }

    private List<IndexMetadata.Field> getColumnFields(IndexMetadata metadata)
    {
        Map<String, Long> counts = metadata.schema()
                .fields().stream()
                .collect(Collectors.groupingBy(f -> f.name().toLowerCase(ENGLISH), Collectors.counting()));

        return metadata.schema().fields().stream()
                .filter(field -> toTrino(field) != null && counts.get(field.name().toLowerCase(ENGLISH)) <= 1)
                .collect(toImmutableList());
    }

    private List<ColumnMetadata> makeColumnMetadata(List<IndexMetadata.Field> fields)
    {
        ImmutableList.Builder<ColumnMetadata> result = ImmutableList.builder();

        for (BuiltinColumns builtinColumn : BuiltinColumns.values()) {
            result.add(builtinColumn.getMetadata());
        }

        for (IndexMetadata.Field field : fields) {
            result.add(ColumnMetadata.builder()
                    .setName(field.name())
                    .setType(toTrino(field).type())
                    .build());
        }
        return result.build();
    }

    private Map<String, ColumnHandle> makeColumnHandles(List<IndexMetadata.Field> fields)
    {
        ImmutableMap.Builder<String, ColumnHandle> result = ImmutableMap.builder();

        for (BuiltinColumns builtinColumn : BuiltinColumns.values()) {
            result.put(builtinColumn.getName(), builtinColumn.getColumnHandle());
        }

        for (IndexMetadata.Field field : fields) {
            TypeAndDecoder converted = toTrino(field);
            result.put(field.name(), new ElasticsearchColumnHandle(
                    ImmutableList.of(field.name()),
                    converted.type(),
                    field.type(),
                    converted.decoderDescriptor(),
                    supportsPredicates(field.type(), converted.type)));
        }

        return result.buildOrThrow();
    }

    private TypeAndDecoder toTrino(IndexMetadata.Field field)
    {
        return toTrino("", field);
    }

    private TypeAndDecoder toTrino(String prefix, IndexMetadata.Field field)
    {
        String path = appendPath(prefix, field.name());

        checkArgument(!field.asRawJson() || !field.isArray(), "A column, (%s) cannot be declared as a Trino array and also be rendered as json.", path);

        if (field.asRawJson()) {
            return new TypeAndDecoder(VARCHAR, new RawJsonDecoder.Descriptor(path));
        }

        if (field.isArray()) {
            TypeAndDecoder element = toTrino(path, elementField(field));
            return new TypeAndDecoder(new ArrayType(element.type()), new ArrayDecoder.Descriptor(element.decoderDescriptor()));
        }

        IndexMetadata.Type type = field.type();
        if (type instanceof PrimitiveType primitiveType) {
            return switch (primitiveType.name()) {
                case "float" -> new TypeAndDecoder(REAL, new RealDecoder.Descriptor(path));
                case "double" -> new TypeAndDecoder(DOUBLE, new DoubleDecoder.Descriptor(path));
                case "byte" -> new TypeAndDecoder(TINYINT, new TinyintDecoder.Descriptor(path));
                case "short" -> new TypeAndDecoder(SMALLINT, new SmallintDecoder.Descriptor(path));
                case "integer" -> new TypeAndDecoder(INTEGER, new IntegerDecoder.Descriptor(path));
                case "long" -> new TypeAndDecoder(BIGINT, new BigintDecoder.Descriptor(path));
                case "text", "keyword" -> new TypeAndDecoder(VARCHAR, new VarcharDecoder.Descriptor(path));
                case "ip" -> new TypeAndDecoder(ipAddressType, new IpAddressDecoder.Descriptor(path, ipAddressType));
                case "boolean" -> new TypeAndDecoder(BOOLEAN, new BooleanDecoder.Descriptor(path));
                case "binary" -> new TypeAndDecoder(VARBINARY, new VarbinaryDecoder.Descriptor(path));
                default -> null;
            };
        }
        else if (type instanceof ScaledFloatType) {
            return new TypeAndDecoder(DOUBLE, new DoubleDecoder.Descriptor(path));
        }
        else if (type instanceof DateTimeType dateTimeType) {
            if (dateTimeType.formats().isEmpty()) {
                return new TypeAndDecoder(TIMESTAMP_MILLIS, new TimestampDecoder.Descriptor(path));
            }
            // otherwise, skip -- we don't support custom formats, yet
        }
        else if (type instanceof ObjectType objectType) {
            ImmutableList.Builder<RowType.Field> rowFieldsBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowDecoder.NameAndDescriptor> decoderFields = ImmutableList.builder();
            for (IndexMetadata.Field rowField : objectType.fields()) {
                String name = rowField.name();
                TypeAndDecoder child = toTrino(path, rowField);

                if (child != null) {
                    decoderFields.add(new RowDecoder.NameAndDescriptor(name, child.decoderDescriptor()));
                    rowFieldsBuilder.add(RowType.field(name, child.type()));
                }
            }

            List<RowType.Field> rowFields = rowFieldsBuilder.build();
            if (!rowFields.isEmpty()) {
                return new TypeAndDecoder(RowType.from(rowFields), new RowDecoder.Descriptor(path, decoderFields.build()));
            }

            // otherwise, skip -- row types must have at least 1 field
        }

        return null;
    }

    private static String appendPath(String base, String element)
    {
        if (base.isEmpty()) {
            return element;
        }

        return base + "." + element;
    }

    public static IndexMetadata.Field elementField(IndexMetadata.Field field)
    {
        checkArgument(field.isArray(), "Cannot get element field from a non-array field");
        return new IndexMetadata.Field(field.asRawJson(), false, field.name(), field.type());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !schemaName.get().equals(this.schemaName)) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<SchemaTableName> result = ImmutableList.builder();
        Set<String> indexes = ImmutableSet.copyOf(client.getIndexes());

        indexes.stream()
                .map(index -> new SchemaTableName(this.schemaName, index))
                .forEach(result::add);

        client.getAliases().entrySet().stream()
                .filter(entry -> indexes.contains(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream()
                        .map(alias -> new SchemaTableName(this.schemaName, alias)))
                .distinct()
                .forEach(result::add);

        return result.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle table = (ElasticsearchTableHandle) tableHandle;

        if (isPassthroughQuery(table)) {
            return PASSTHROUGH_QUERY_COLUMNS;
        }

        InternalTableMetadata tableMetadata = makeInternalTableMetadata(tableHandle);
        return tableMetadata.columnHandles();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ElasticsearchTableHandle table = (ElasticsearchTableHandle) tableHandle;
        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) columnHandle;

        if (isPassthroughQuery(table)) {
            if (column.name().equals(PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA.getName())) {
                return PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA;
            }

            throw new IllegalArgumentException(format("Unexpected column for table '%s$query': %s", table.index(), column.name()));
        }

        return BuiltinColumns.of(column.name())
                .map(BuiltinColumns::getMetadata)
                .orElse(ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(column.type())
                        .build());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated streamTableColumns is not supported because streamRelationColumns is implemented instead");
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        if (schemaName.isPresent() && !schemaName.get().equals(this.schemaName)) {
            return emptyIterator();
        }

        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();
        listTables(session, schemaName).stream()
                .flatMap(name -> {
                    try {
                        ConnectorTableMetadata tableMetadata = getTableMetadata(name.getSchemaName(), name.getTableName());
                        return Stream.of(TableColumnsMetadata.forTable(tableMetadata.getTable(), tableMetadata.getColumns()));
                    }
                    catch (TrinoException e) {
                        // this may happen when table is being deleted concurrently
                        if (e.getCause() instanceof ResponseException cause && cause.getResponse().getStatusLine().getStatusCode() == 404) {
                            return Stream.empty();
                        }
                        throw e;
                    }
                })
                .forEach(columnsMetadata -> {
                    SchemaTableName name = columnsMetadata.getTable();
                    relationColumns.put(name, columnsMetadata.getColumns()
                            .map(columns -> RelationColumnsMetadata.forTable(name, columns))
                            .orElseGet(() -> RelationColumnsMetadata.forRedirectedTable(name)));
                });

        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        return new ConnectorTableProperties(
                handle.constraint(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        if (!statisticsEnabled || isPassthroughQuery(handle)) {
            return TableStatistics.empty();
        }

        // Even when no column is aggregatable the row count is still valuable to the cost-based optimizer
        List<ElasticsearchColumnHandle> columns = statisticsColumns(handle);

        // Text fields with a safe keyword sub-field are aggregated on the sub-field, so use the predicate field name
        List<String> fields = columns.stream()
                .map(ElasticsearchColumnHandle::predicateName)
                .collect(toImmutableList());
        Set<String> rangeFields = columns.stream()
                .filter(column -> hasRange(column.type()))
                .map(ElasticsearchColumnHandle::predicateName)
                .collect(toImmutableSet());

        JsonNode query = buildSearchQuery(handle.constraint().transformKeys(ElasticsearchColumnHandle.class::cast), handle.query(), handle.regexes(), handle.prefixes());
        IndexStatistics indexStatistics = client.getIndexStatistics(handle.index(), query, fields, rangeFields);

        long rowCount = indexStatistics.documentCount();
        TableStatistics.Builder tableStatistics = TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount));
        for (ElasticsearchColumnHandle column : columns) {
            FieldStatistics fieldStatistics = indexStatistics.fields().get(column.predicateName());
            if (fieldStatistics != null) {
                tableStatistics.setColumnStatistics(column, columnStatistics(column.type(), rowCount, fieldStatistics));
            }
        }
        return tableStatistics.build();
    }

    private List<ElasticsearchColumnHandle> statisticsColumns(ElasticsearchTableHandle handle)
    {
        Collection<ElasticsearchColumnHandle> columns;
        if (handle.columns().isEmpty()) {
            columns = makeInternalTableMetadata(handle.schema(), handle.index()).columnHandles().values().stream()
                    .map(ElasticsearchColumnHandle.class::cast)
                    .collect(toImmutableList());
        }
        else {
            columns = handle.columns();
        }
        return columns.stream()
                .filter(column -> column.path().size() == 1)
                // Array columns are excluded: value_count aggregates every array element and can exceed the row count
                .filter(column -> !(column.type() instanceof ArrayType))
                .filter(column -> !BuiltinColumns.isBuiltinColumn(column.name()))
                .filter(column -> isAggregatable(column.elasticsearchType()))
                .collect(toImmutableList());
    }

    private static boolean isAggregatable(IndexMetadata.Type type)
    {
        if (type instanceof PrimitiveType primitiveType) {
            // A text field with a safe keyword sub-field is aggregated on that sub-field (see ElasticsearchColumnHandle#predicateName)
            if (primitiveType.keyword().isPresent()) {
                return true;
            }
            return switch (primitiveType.name()) {
                case "byte", "short", "integer", "long", "float", "double", "keyword", "boolean", "ip" -> true;
                default -> false;
            };
        }
        return type instanceof ScaledFloatType || type instanceof DateTimeType;
    }

    private static boolean hasRange(Type type)
    {
        return type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)
                || type.equals(REAL) || type.equals(DOUBLE) || type.equals(TIMESTAMP_MILLIS);
    }

    private static ColumnStatistics columnStatistics(Type type, long rowCount, FieldStatistics statistics)
    {
        ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
        statistics.cardinality().ifPresent(distinct -> columnStatistics.setDistinctValuesCount(Estimate.of(distinct)));
        if (rowCount > 0) {
            statistics.valueCount().ifPresent(valueCount -> columnStatistics.setNullsFraction(Estimate.of((double) (rowCount - valueCount) / rowCount)));
        }
        if (statistics.min().isPresent() && statistics.max().isPresent()) {
            double min = statistics.min().getAsDouble();
            double max = statistics.max().getAsDouble();
            if (type.equals(TIMESTAMP_MILLIS)) {
                // Elasticsearch returns epoch milliseconds; the statistics domain for timestamp(3) is epoch microseconds
                min *= MICROSECONDS_PER_MILLISECOND;
                max *= MICROSECONDS_PER_MILLISECOND;
            }
            columnStatistics.setRange(new DoubleRange(min, max));
        }
        return columnStatistics.build();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            // limit pushdown currently not supported passthrough query
            return Optional.empty();
        }

        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }

        if (handle.limit().isPresent() && handle.limit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new ElasticsearchTableHandle(
                handle.type(),
                handle.schema(),
                handle.index(),
                handle.constraint(),
                handle.regexes(),
                handle.prefixes(),
                handle.query(),
                OptionalLong.of(limit),
                handle.sortOrder(),
                ImmutableSet.of(),
                handle.aggregation());

        return Optional.of(new LimitApplicationResult<>(handle, false, false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        if (isPassthroughQuery(handle)) {
            return Optional.empty();
        }

        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<ElasticsearchColumnSort> sortOrder = ImmutableList.builder();
        for (SortItem sortItem : sortItems) {
            ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) assignments.get(sortItem.getName());
            // Only columns backed by doc_values (the same gate as aggregation) can be sorted by Elasticsearch
            if (BuiltinColumns.isBuiltinColumn(column.name()) || !isAggregatable(column.elasticsearchType())) {
                return Optional.empty();
            }
            SortOrder order = sortItem.getSortOrder();
            sortOrder.add(new ElasticsearchColumnSort(column.predicateName(), order.isAscending(), order.isNullsFirst()));
        }
        List<ElasticsearchColumnSort> newSortOrder = sortOrder.build();

        if (handle.sortOrder().equals(newSortOrder) && handle.limit().equals(OptionalLong.of(topNCount))) {
            return Optional.empty();
        }

        // topNGuaranteed is false: each split returns its own sorted top N and the engine merges the partial results
        return Optional.of(new TopNApplicationResult<>(handle.withTopN(topNCount, newSortOrder), false, false));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (!isAggregationPushdownEnabled(session)) {
            return Optional.empty();
        }

        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        if (isPassthroughQuery(handle) || handle.aggregation().isPresent() || handle.limit().isPresent()) {
            // Push only one level of aggregation, and never on top of an already pushed-down limit
            return Optional.empty();
        }
        // Only a single grouping set is supported (no GROUPING SETS / CUBE / ROLLUP)
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }

        ImmutableSet.Builder<ElasticsearchColumnHandle> outputColumns = ImmutableSet.builder();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        ImmutableMap.Builder<ColumnHandle, ColumnHandle> groupingColumnMapping = ImmutableMap.builder();

        ImmutableList.Builder<ElasticsearchColumnHandle> groupingColumns = ImmutableList.builder();
        for (ColumnHandle columnHandle : groupingSets.get(0)) {
            ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) columnHandle;
            // Grouping keys become composite terms sources: the field must be aggregatable (doc_values), exact, and a type we can decode
            if (BuiltinColumns.isBuiltinColumn(column.name()) || !isAggregatable(column.elasticsearchType()) || !isSupportedGroupingType(column.type())) {
                return Optional.empty();
            }
            // Grouping columns pass through unchanged: they belong only in the grouping-column mapping,
            // not in the projections/assignments, which the SPI reserves for the aggregate outputs
            groupingColumns.add(column);
            outputColumns.add(column);
            groupingColumnMapping.put(column, column);
        }

        ImmutableList.Builder<ElasticsearchAggregate> metrics = ImmutableList.builder();
        int counter = 0;
        for (AggregateFunction aggregate : aggregates) {
            Optional<ElasticsearchAggregate> converted = toElasticsearchAggregate(aggregate, assignments, "agg_" + counter);
            if (converted.isEmpty()) {
                return Optional.empty();
            }
            ElasticsearchAggregate metric = converted.get();
            metrics.add(metric);

            ElasticsearchColumnHandle column = aggregationColumnHandle(metric);
            outputColumns.add(column);
            projections.add(new Variable(metric.outputName(), aggregate.getOutputType()));
            resultAssignments.add(new Assignment(metric.outputName(), column, aggregate.getOutputType()));
            counter++;
        }

        List<ElasticsearchColumnHandle> grouping = groupingColumns.build();
        List<ElasticsearchAggregate> aggregateList = metrics.build();
        if (grouping.isEmpty() && aggregateList.isEmpty()) {
            return Optional.empty();
        }

        ElasticsearchTableHandle aggregatedHandle = new ElasticsearchTableHandle(
                handle.type(),
                handle.schema(),
                handle.index(),
                handle.constraint(),
                handle.regexes(),
                handle.prefixes(),
                handle.query(),
                OptionalLong.empty(),
                ImmutableList.of(),
                outputColumns.build(),
                Optional.of(new ElasticsearchAggregation(grouping, aggregateList)));

        return Optional.of(new AggregationApplicationResult<>(
                aggregatedHandle,
                projections.build(),
                resultAssignments.build(),
                groupingColumnMapping.buildOrThrow(),
                false));
    }

    private static Optional<ElasticsearchAggregate> toElasticsearchAggregate(AggregateFunction aggregate, Map<String, ColumnHandle> assignments, String outputName)
    {
        if (aggregate.isDistinct()) {
            // Exact DISTINCT aggregates cannot be represented by Elasticsearch's approximate cardinality
            return Optional.empty();
        }
        Type outputType = aggregate.getOutputType();
        List<ConnectorExpression> arguments = aggregate.getArguments();
        switch (aggregate.getFunctionName()) {
            case "count":
                if (arguments.isEmpty()) {
                    return Optional.of(new ElasticsearchAggregate(outputName, Function.COUNT_ALL, Optional.empty(), outputType));
                }
                return countAggregate(outputName, Function.COUNT, arguments, assignments, outputType);
            case "approx_distinct":
                return countAggregate(outputName, Function.COUNT_DISTINCT, arguments, assignments, outputType);
            case "sum":
                return numericAggregate(outputName, Function.SUM, arguments, assignments, outputType);
            case "avg":
                return numericAggregate(outputName, Function.AVG, arguments, assignments, outputType);
            case "min":
                return numericAggregate(outputName, Function.MIN, arguments, assignments, outputType);
            case "max":
                return numericAggregate(outputName, Function.MAX, arguments, assignments, outputType);
            default:
                return Optional.empty();
        }
    }

    private static Optional<ElasticsearchAggregate> countAggregate(String outputName, Function function, List<ConnectorExpression> arguments, Map<String, ColumnHandle> assignments, Type outputType)
    {
        Optional<ElasticsearchColumnHandle> column = aggregateColumn(arguments, assignments);
        if (column.isEmpty() || !isAggregatable(column.get().elasticsearchType())) {
            return Optional.empty();
        }
        return Optional.of(new ElasticsearchAggregate(outputName, function, Optional.of(column.get().predicateName()), outputType));
    }

    private static Optional<ElasticsearchAggregate> numericAggregate(String outputName, Function function, List<ConnectorExpression> arguments, Map<String, ColumnHandle> assignments, Type outputType)
    {
        Optional<ElasticsearchColumnHandle> column = aggregateColumn(arguments, assignments);
        if (column.isEmpty() || !isNumericType(column.get().type())) {
            return Optional.empty();
        }
        return Optional.of(new ElasticsearchAggregate(outputName, function, Optional.of(column.get().predicateName()), outputType));
    }

    private static Optional<ElasticsearchColumnHandle> aggregateColumn(List<ConnectorExpression> arguments, Map<String, ColumnHandle> assignments)
    {
        if (arguments.size() != 1 || !(arguments.get(0) instanceof Variable variable)) {
            return Optional.empty();
        }
        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) assignments.get(variable.getName());
        if (column == null || BuiltinColumns.isBuiltinColumn(column.name())) {
            return Optional.empty();
        }
        return Optional.of(column);
    }

    private static boolean isNumericType(Type type)
    {
        return type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT) || type.equals(REAL) || type.equals(DOUBLE);
    }

    private static boolean isSupportedGroupingType(Type type)
    {
        // Types the composite bucket key can be decoded into (see AggregationQueryPageSource)
        return type instanceof VarcharType || isNumericType(type) || type.equals(BOOLEAN);
    }

    private static ElasticsearchColumnHandle aggregationColumnHandle(ElasticsearchAggregate aggregate)
    {
        Type type = aggregate.outputType();
        DecoderDescriptor decoder;
        if (type.equals(REAL) || type.equals(DOUBLE)) {
            decoder = new DoubleDecoder.Descriptor(aggregate.outputName());
        }
        else {
            decoder = new BigintDecoder.Descriptor(aggregate.outputName());
        }
        // The Elasticsearch type and decoder are placeholders: aggregation results are read from the composite response, not from _source
        return new ElasticsearchColumnHandle(
                ImmutableList.of(aggregate.outputName()),
                type,
                new IndexMetadata.PrimitiveType("_aggregation"),
                decoder,
                false);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            // filter pushdown currently not supported for passthrough query
            return Optional.empty();
        }

        if (handle.aggregation().isPresent()) {
            // Filters above an aggregation (HAVING) are not pushed into the composite aggregation
            return Optional.empty();
        }

        FullTextPushdownMode fullTextMode = getFullTextPushdownMode(session);
        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();
        Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));
        for (Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) entry.getKey();

            if (column.supportsPredicates()) {
                supported.put(column, entry.getValue());
            }
            else if (fullTextMode != FullTextPushdownMode.DISABLED && isFullTextCandidate(column, entry.getValue())) {
                // Push an analyzed text predicate as a full-text match_phrase query
                supported.put(column, entry.getValue());
                if (fullTextMode == FullTextPushdownMode.SAFE) {
                    // Keep the exact predicate as a residual so the engine re-applies it over the full-text pre-filter
                    unsupported.put(column, entry.getValue());
                }
            }
            else {
                unsupported.put(column, entry.getValue());
            }
        }

        TupleDomain<ColumnHandle> oldDomain = handle.constraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(supported));

        ConnectorExpression oldExpression = constraint.getExpression();
        Map<String, String> newRegexes = new HashMap<>(handle.regexes());
        Map<String, String> newPrefixes = new HashMap<>(handle.prefixes());
        List<ConnectorExpression> expressions = ConnectorExpressions.extractConjuncts(constraint.getExpression());
        List<ConnectorExpression> notHandledExpressions = new ArrayList<>();
        for (ConnectorExpression expression : expressions) {
            if (expression instanceof Call call) {
                if (isSupportedLikeCall(call)) {
                    List<ConnectorExpression> arguments = call.getArguments();
                    String variableName = ((Variable) arguments.get(0)).getName();
                    ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) constraint.getAssignments().get(variableName);
                    verifyNotNull(column, "No assignment for %s", variableName);
                    Object pattern = ((Constant) arguments.get(1)).getValue();
                    Optional<Slice> escape = Optional.empty();
                    if (arguments.size() == 3) {
                        escape = Optional.of((Slice) ((Constant) arguments.get(2)).getValue());
                    }

                    boolean exactLike = supportsLikePushdown(column);
                    boolean fullTextLike = !exactLike && fullTextMode != FullTextPushdownMode.DISABLED && column.type() instanceof VarcharType;
                    if (pattern instanceof Slice slice && (exactLike || fullTextLike)) {
                        String predicateName = column.predicateName();
                        if (!newRegexes.containsKey(predicateName) && !newPrefixes.containsKey(predicateName)) {
                            Optional<String> prefix = likePrefix(slice, escape);
                            if (prefix.isPresent()) {
                                // A pure prefix pattern (literal%) maps to a fast Elasticsearch prefix query
                                newPrefixes.put(predicateName, prefix.get());
                            }
                            else {
                                newRegexes.put(predicateName, likeToRegexp(slice, escape));
                            }
                            // Exact (keyword) or UNSAFE full text is authoritative; SAFE full text keeps the exact residual
                            if (exactLike || fullTextMode == FullTextPushdownMode.UNSAFE) {
                                continue;
                            }
                        }
                    }
                }

                // regexp_like(col, 'pattern') → Elasticsearch regexp (Lucene syntax, differs from Java); full-text mode only
                if (fullTextMode != FullTextPushdownMode.DISABLED && REGEXP_LIKE_FUNCTION_NAME.equals(call.getFunctionName())) {
                    List<ConnectorExpression> arguments = call.getArguments();
                    if (arguments.size() == 2
                            && arguments.get(0) instanceof Variable variable
                            && arguments.get(1) instanceof Constant constant && constant.getValue() instanceof Slice regexp) {
                        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) constraint.getAssignments().get(variable.getName());
                        if (column != null && column.type() instanceof VarcharType) {
                            String predicateName = column.predicateName();
                            if (!newRegexes.containsKey(predicateName) && !newPrefixes.containsKey(predicateName)) {
                                newRegexes.put(predicateName, regexp.toStringUtf8());
                                if (fullTextMode == FullTextPushdownMode.UNSAFE) {
                                    continue;
                                }
                            }
                        }
                    }
                }

                Optional<Map.Entry<ElasticsearchColumnHandle, String>> prefixPredicate = prefixFromCall(call, constraint);
                if (prefixPredicate.isPresent()) {
                    String predicateName = prefixPredicate.get().getKey().predicateName();
                    if (!newRegexes.containsKey(predicateName) && !newPrefixes.containsKey(predicateName)) {
                        newPrefixes.put(predicateName, prefixPredicate.get().getValue());
                        continue;
                    }
                }
            }
            notHandledExpressions.add(expression);
        }

        ConnectorExpression newExpression = ConnectorExpressions.and(notHandledExpressions);
        if (oldDomain.equals(newDomain) && oldExpression.equals(newExpression)
                && handle.regexes().equals(newRegexes) && handle.prefixes().equals(newPrefixes)) {
            return Optional.empty();
        }

        handle = new ElasticsearchTableHandle(
                handle.type(),
                handle.schema(),
                handle.index(),
                newDomain,
                newRegexes,
                newPrefixes,
                handle.query(),
                handle.limit(),
                handle.sortOrder(),
                ImmutableSet.of(),
                handle.aggregation());

        return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.withColumnDomains(unsupported), newExpression, false));
    }

    protected static boolean isSupportedLikeCall(Call call)
    {
        if (!LIKE_FUNCTION_NAME.equals(call.getFunctionName())) {
            return false;
        }

        List<ConnectorExpression> arguments = call.getArguments();
        if (arguments.size() < 2 || arguments.size() > 3) {
            return false;
        }

        if (!(arguments.get(0) instanceof Variable) || !(arguments.get(1) instanceof Constant)) {
            return false;
        }

        if (arguments.size() == 3) {
            return arguments.get(2) instanceof Constant;
        }

        return true;
    }

    /**
     * Recognizes a prefix predicate — {@code starts_with(col, 'x')} or {@code substr(col, 1, n) = 'x'} (with
     * {@code n} equal to the length of {@code 'x'}) — and returns the column plus the literal prefix. Both are exact
     * on a keyword field, so they are pushed as an Elasticsearch {@code prefix} query.
     */
    private static boolean isFullTextCandidate(ElasticsearchColumnHandle column, Domain domain)
    {
        // An analyzed text column (not exact-match pushable) with only discrete values (= / IN) can be a full-text match
        return column.type() instanceof VarcharType && domain.getValues().isDiscreteSet();
    }

    private static Optional<Map.Entry<ElasticsearchColumnHandle, String>> prefixFromCall(Call call, Constraint constraint)
    {
        List<ConnectorExpression> arguments = call.getArguments();
        if (STARTS_WITH_FUNCTION_NAME.equals(call.getFunctionName())
                && arguments.size() == 2
                && arguments.get(0) instanceof Variable variable
                && arguments.get(1) instanceof Constant constant
                && constant.getValue() instanceof Slice prefix) {
            return prefixColumn(variable, prefix, constraint);
        }
        if (EQUAL_OPERATOR_FUNCTION_NAME.equals(call.getFunctionName()) && arguments.size() == 2) {
            // substr(col, 1, n) = 'x' matches only when n equals the character length of 'x' (else it can never hold or is exact)
            for (int i = 0; i < 2; i++) {
                if (arguments.get(i) instanceof Call inner
                        && (SUBSTR_FUNCTION_NAME.equals(inner.getFunctionName()) || SUBSTRING_FUNCTION_NAME.equals(inner.getFunctionName()))
                        && inner.getArguments().size() == 3
                        && inner.getArguments().get(0) instanceof Variable variable
                        && inner.getArguments().get(1) instanceof Constant start && start.getValue() instanceof Long from && from == 1L
                        && inner.getArguments().get(2) instanceof Constant length && length.getValue() instanceof Long count
                        && arguments.get(1 - i) instanceof Constant constant && constant.getValue() instanceof Slice prefix
                        && count == countCodePoints(prefix)) {
                    return prefixColumn(variable, prefix, constraint);
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Map.Entry<ElasticsearchColumnHandle, String>> prefixColumn(Variable variable, Slice prefix, Constraint constraint)
    {
        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) constraint.getAssignments().get(variable.getName());
        // Only exact-match (keyword or text-with-keyword) columns can honor a prefix query
        if (column == null || !supportsLikePushdown(column)) {
            return Optional.empty();
        }
        return Optional.of(Map.entry(column, prefix.toStringUtf8()));
    }

    protected static Optional<String> likePrefix(Slice pattern, Optional<Slice> escape)
    {
        Optional<Character> escapeChar = escape.map(ElasticsearchMetadata::getEscapeChar);
        StringBuilder prefix = new StringBuilder();
        boolean escaped = false;
        int position = 0;
        while (position < pattern.length()) {
            int currentChar = getCodePointAt(pattern, position);
            position += lengthOfCodePoint(currentChar);
            checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar.get());
            if (!escaped && escapeChar.isPresent() && currentChar == escapeChar.get()) {
                escaped = true;
            }
            else {
                if (!escaped && currentChar == '%') {
                    // A literal prefix followed by a single trailing '%' is a prefix match; a '%' elsewhere is not
                    if (position == pattern.length()) {
                        return Optional.of(prefix.toString());
                    }
                    return Optional.empty();
                }
                if (!escaped && currentChar == '_') {
                    // '_' matches exactly one character, which a prefix query cannot express
                    return Optional.empty();
                }
                prefix.appendCodePoint(currentChar);
                escaped = false;
            }
        }

        // No trailing '%': the pattern is an exact match, left to regexp (equality is normally a domain predicate)
        checkEscape(!escaped);
        return Optional.empty();
    }

    protected static String likeToRegexp(Slice pattern, Optional<Slice> escape)
    {
        Optional<Character> escapeChar = escape.map(ElasticsearchMetadata::getEscapeChar);
        StringBuilder regex = new StringBuilder();
        boolean escaped = false;
        int position = 0;
        while (position < pattern.length()) {
            int currentChar = getCodePointAt(pattern, position);
            position += lengthOfCodePoint(currentChar);
            checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar.get());
            if (!escaped && escapeChar.isPresent() && currentChar == escapeChar.get()) {
                escaped = true;
            }
            else {
                switch (currentChar) {
                    case '%' -> {
                        regex.append(escaped ? "%" : ".*");
                        escaped = false;
                    }
                    case '_' -> {
                        regex.append(escaped ? "_" : ".");
                        escaped = false;
                    }
                    case '\\' -> regex.append("\\\\");
                    default -> {
                        // escape special regex characters
                        if (REGEXP_RESERVED_CHARACTERS.contains(currentChar)) {
                            regex.append('\\');
                        }

                        regex.appendCodePoint(currentChar);
                        escaped = false;
                    }
                }
            }
        }

        checkEscape(!escaped);
        return regex.toString();
    }

    private static void checkEscape(boolean condition)
    {
        if (!condition) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Escape character must be followed by '%', '_' or the escape character itself");
        }
    }

    private static char getEscapeChar(Slice escape)
    {
        String escapeString = escape.toStringUtf8();
        if (escapeString.length() == 1) {
            return escapeString.charAt(0);
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Escape string must be a single character");
    }

    private static boolean isPassthroughQuery(ElasticsearchTableHandle table)
    {
        return table.type().equals(QUERY);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression, ElasticsearchMetadata::isSupportedForPushdown).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        ElasticsearchTableHandle elasticsearchTableHandle = (ElasticsearchTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<ElasticsearchColumnHandle> projectedColumns = assignments.values().stream()
                    .map(ElasticsearchColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (elasticsearchTableHandle.columns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((ElasticsearchColumnHandle) assignment.getValue()).type()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    elasticsearchTableHandle.withColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<ElasticsearchColumnHandle> columns = ImmutableSet.builder();

        for (Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            ElasticsearchColumnHandle baseColumnHandle = (ElasticsearchColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            ElasticsearchColumnHandle projectedColumnHandle = projectColumn(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.name();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            columns.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.buildOrThrow();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = newAssignments.values().stream().collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                elasticsearchTableHandle.withColumns(columns.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private static boolean isSupportedForPushdown(ConnectorExpression connectorExpression)
    {
        if (connectorExpression instanceof Variable) {
            return true;
        }
        if (connectorExpression instanceof FieldDereference fieldDereference) {
            RowType rowType = (RowType) fieldDereference.getTarget().getType();
            RowType.Field field = rowType.getFields().get(fieldDereference.getField());
            return field.getName().isPresent();
        }
        return false;
    }

    private static ElasticsearchColumnHandle projectColumn(ElasticsearchColumnHandle baseColumn, List<Integer> indices, Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return baseColumn;
        }
        ImmutableList.Builder<String> path = ImmutableList.builder();
        path.addAll(baseColumn.path());

        DecoderDescriptor decoderDescriptor = baseColumn.decoderDescriptor();
        IndexMetadata.Type elasticsearchType = baseColumn.elasticsearchType();
        Type type = baseColumn.type();

        for (int index : indices) {
            verify(type instanceof RowType, "type should be Row type");
            RowType rowType = (RowType) type;
            RowType.Field field = rowType.getFields().get(index);
            path.add(field.getName()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ROW type does not have field name declared: " + rowType)));
            type = field.getType();

            verify(decoderDescriptor instanceof RowDecoder.Descriptor, "decoderDescriptor should be RowDecoder.Descriptor type");
            decoderDescriptor = ((RowDecoder.Descriptor) decoderDescriptor).getFields().get(index).getDescriptor();
            elasticsearchType = ((IndexMetadata.ObjectType) elasticsearchType).fields().get(index).type();
        }

        return new ElasticsearchColumnHandle(
                path.build(),
                projectedColumnType,
                elasticsearchType,
                decoderDescriptor,
                supportsPredicates(elasticsearchType, projectedColumnType));
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof RawQueryFunctionHandle rawQueryFunctionHandle)) {
            return Optional.empty();
        }

        ConnectorTableHandle tableHandle = rawQueryFunctionHandle.getTableHandle();
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(getColumnHandles(session, tableHandle).values());
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    private static boolean supportsPredicates(IndexMetadata.Type type, Type trinoType)
    {
        return switch (trinoType) {
            case TimestampType _, BooleanType _, TinyintType _, SmallintType _, IntegerType _, BigintType _, RealType _ -> true;
            case DoubleType _ -> !(type instanceof ScaledFloatType);
            case VarcharType _ when type instanceof PrimitiveType primitiveType && (primitiveType.name().toLowerCase(ENGLISH).equals("keyword") || primitiveType.keyword().isPresent()) -> true;
            default -> false;
        };
    }

    private static boolean supportsLikePushdown(ElasticsearchColumnHandle column)
    {
        return column.elasticsearchType() instanceof PrimitiveType primitiveType
                && (primitiveType.name().toLowerCase(ENGLISH).equals("keyword") || primitiveType.keyword().isPresent());
    }

    private record InternalTableMetadata(SchemaTableName tableName, List<ColumnMetadata> columnMetadata, Map<String, ColumnHandle> columnHandles) {}

    private record TypeAndDecoder(Type type, DecoderDescriptor decoderDescriptor) {}
}
