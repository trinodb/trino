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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.elasticsearch.aggregation.MetricAggregation;
import io.trino.plugin.elasticsearch.aggregation.TermAggregation;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.client.IndexMetadata.DateTimeType;
import io.trino.plugin.elasticsearch.client.IndexMetadata.ObjectType;
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
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
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.elasticsearch.Version;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.elasticsearch.ElasticsearchConnector.AGGREGATION_PUSHDOWN_ENABLED;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.AGG;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.SCAN;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private static final ObjectMapper JSON_PARSER = new ObjectMapperProvider().get();
    private static final String SYNTHETIC_COLUMN_NAME_PREFIX = "_efgnrtd_";

    private static final String PASSTHROUGH_QUERY_SUFFIX = "$query";
    private static final String PASSTHROUGH_QUERY_RESULT_COLUMN_NAME = "result";
    private static final ColumnMetadata PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA = ColumnMetadata.builder()
            .setName(PASSTHROUGH_QUERY_RESULT_COLUMN_NAME)
            .setType(VARCHAR)
            .setNullable(true)
            .setHidden(false)
            .build();

    private static final Map<String, ColumnHandle> PASSTHROUGH_QUERY_COLUMNS = ImmutableMap.of(
            PASSTHROUGH_QUERY_RESULT_COLUMN_NAME,
            new ElasticsearchColumnHandle(PASSTHROUGH_QUERY_RESULT_COLUMN_NAME, VARCHAR, false));

    private final Type ipAddressType;
    private final ElasticsearchClient client;
    private final String schemaName;

    @Inject
    public ElasticsearchMetadata(TypeManager typeManager, ElasticsearchClient client, ElasticsearchConfig config)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.ipAddressType = typeManager.getType(new TypeSignature(StandardTypes.IPADDRESS));
        this.client = requireNonNull(client, "client is null");
        requireNonNull(config, "config is null");
        this.schemaName = config.getDefaultSchema();
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        if (!session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class)) {
            return Optional.empty();
        }
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        if (isPassthroughQuery(handle)) {
            // aggregation pushdown currently not supported passthrough query
            return Optional.empty();
        }
        Version version = client.getVersion();
        // in order to support null bucket, we need missing_bucket in composite aggregation which is introduced after 6.4.0
        if (version == null || version.before(ElasticsearchClient.MINIMUM_VERSION_REQUIRE_FOR_AGG_PUSHDOWN)) {
            return Optional.empty();
        }
        // Global aggregation is represented by [[]]
        verify(!groupingSets.isEmpty(), "No grouping sets provided");
        if (handle.getTermAggregations() != null && !handle.getTermAggregations().isEmpty()) {
            // applyAggregation may be called multiple times target on the same ElasticsearchTableHandle
            // for example
            // SELECT sum(DISTINCT regionkey) FROM nation
            // will be split into two logic phrase:
            // 1. table a -> select regionkey from nation group by regionkey
            // 2. select sum(regionkey) from a
            // so the first call of applyAggregation will only contains groupby set
            // and the second call will contains aggregates(sum)
            // we skip the second one, but the first group by will be still applied
            return Optional.empty();
        }

        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        ImmutableList.Builder<MetricAggregation> metricAggregations = ImmutableList.builder();
        ImmutableList.Builder<TermAggregation> termAggregations = ImmutableList.builder();
        for (int i = 0; i < aggregates.size(); i++) {
            AggregateFunction aggregationFunction = aggregates.get(i);
            String colName = SYNTHETIC_COLUMN_NAME_PREFIX + i;
            Optional<MetricAggregation> metricAggregation =
                    MetricAggregation.handleAggregation(aggregationFunction, assignments, colName);
            if (metricAggregation.isEmpty()) {
                return Optional.empty();
            }
            ElasticsearchColumnHandle newColumn = new ElasticsearchColumnHandle(
                    colName,
                    aggregationFunction.getOutputType(),
                    // new column never support predicates
                    false);
            projections.add(new Variable(colName, aggregationFunction.getOutputType()));
            resultAssignments.add(new Assignment(colName, newColumn, aggregationFunction.getOutputType()));
            metricAggregations.add(metricAggregation.get());
        }
        for (ColumnHandle columnHandle : groupingSets.get(0)) {
            Optional<TermAggregation> termAggregation = TermAggregation.fromColumnHandle(columnHandle);
            if (termAggregation.isEmpty()) {
                return Optional.empty();
            }
            termAggregations.add(termAggregation.get());
        }
        List<MetricAggregation> aggregationList = metricAggregations.build();
        List<TermAggregation> termAggregationList = termAggregations.build();
        ElasticsearchTableHandle tableHandle = new ElasticsearchTableHandle(
                AGG,
                handle.getSchema(),
                handle.getIndex(),
                handle.getConstraint(),
                handle.getQuery(),
                handle.getLimit(),
                termAggregationList,
                aggregationList);
        return Optional.of(new AggregationApplicationResult<>(tableHandle, projections.build(), resultAssignments.build(), ImmutableMap.of()));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(schemaName);
    }

    @Override
    public ElasticsearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");

        if (tableName.getSchemaName().equals(schemaName)) {
            String[] parts = tableName.getTableName().split(":", 2);
            String table = parts[0];
            Optional<String> query = Optional.empty();
            ElasticsearchTableHandle.Type type = SCAN;
            if (parts.length == 2) {
                if (table.endsWith(PASSTHROUGH_QUERY_SUFFIX)) {
                    table = table.substring(0, table.length() - PASSTHROUGH_QUERY_SUFFIX.length());
                    byte[] decoded;
                    try {
                        decoded = BaseEncoding.base32().decode(parts[1].toUpperCase(ENGLISH));
                    }
                    catch (IllegalArgumentException e) {
                        throw new TrinoException(INVALID_ARGUMENTS, format("Elasticsearch query for '%s' is not base32-encoded correctly", table), e);
                    }

                    String queryJson = new String(decoded, UTF_8);
                    try {
                        // Ensure this is valid json
                        JSON_PARSER.readTree(queryJson);
                    }
                    catch (JsonProcessingException e) {
                        throw new TrinoException(INVALID_ARGUMENTS, format("Elasticsearch query for '%s' is not valid JSON", table), e);
                    }

                    query = Optional.of(queryJson);
                    type = QUERY;
                }
                else {
                    query = Optional.of(parts[1]);
                }
            }

            if (client.indexExists(table) && !client.getIndexMetadata(table).getSchema().getFields().isEmpty()) {
                return new ElasticsearchTableHandle(type, schemaName, table, query);
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
                    new SchemaTableName(handle.getSchema(), handle.getIndex()),
                    ImmutableList.of(PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA));
        }
        return getTableMetadata(handle.getSchema(), handle.getIndex());
    }

    private ConnectorTableMetadata getTableMetadata(String schemaName, String tableName)
    {
        InternalTableMetadata internalTableMetadata = makeInternalTableMetadata(schemaName, tableName);
        return new ConnectorTableMetadata(new SchemaTableName(schemaName, tableName), internalTableMetadata.getColumnMetadata());
    }

    private InternalTableMetadata makeInternalTableMetadata(ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        return makeInternalTableMetadata(handle.getSchema(), handle.getIndex());
    }

    private InternalTableMetadata makeInternalTableMetadata(String schema, String tableName)
    {
        IndexMetadata metadata = client.getIndexMetadata(tableName);
        List<IndexMetadata.Field> fields = getColumnFields(metadata);
        return new InternalTableMetadata(new SchemaTableName(schema, tableName), makeColumnMetadata(fields), makeColumnHandles(fields));
    }

    private List<IndexMetadata.Field> getColumnFields(IndexMetadata metadata)
    {
        ImmutableList.Builder<IndexMetadata.Field> result = ImmutableList.builder();
        Map<String, Long> counts = metadata.getSchema()
                .getFields().stream()
                .collect(Collectors.groupingBy(f -> f.getName().toLowerCase(ENGLISH), Collectors.counting()));

        for (IndexMetadata.Field field : metadata.getSchema().getFields()) {
            Type type = toTrinoType(field);
            if (type == null || counts.get(field.getName().toLowerCase(ENGLISH)) > 1) {
                continue;
            }
            result.add(field);
        }
        return result.build();
    }

    private List<ColumnMetadata> makeColumnMetadata(List<IndexMetadata.Field> fields)
    {
        ImmutableList.Builder<ColumnMetadata> result = ImmutableList.builder();

        for (BuiltinColumns builtinColumn : BuiltinColumns.values()) {
            result.add(builtinColumn.getMetadata());
        }

        for (IndexMetadata.Field field : fields) {
            result.add(ColumnMetadata.builder()
                    .setName(field.getName())
                    .setType(toTrinoType(field))
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
            result.put(field.getName(), new ElasticsearchColumnHandle(
                    field.getName(),
                    toTrinoType(field),
                    supportsPredicates(field.getType())));
        }

        return result.build();
    }

    private static boolean supportsPredicates(IndexMetadata.Type type)
    {
        if (type instanceof DateTimeType) {
            return true;
        }

        if (type instanceof PrimitiveType) {
            switch (((PrimitiveType) type).getName().toLowerCase(ENGLISH)) {
                case "boolean":
                case "byte":
                case "short":
                case "integer":
                case "long":
                case "double":
                case "float":
                case "keyword":
                    return true;
            }
        }

        return false;
    }

    private Type toTrinoType(IndexMetadata.Field metaDataField)
    {
        return toTrinoType(metaDataField, metaDataField.isArray());
    }

    private Type toTrinoType(IndexMetadata.Field metaDataField, boolean isArray)
    {
        IndexMetadata.Type type = metaDataField.getType();
        if (isArray) {
            Type elementType = toTrinoType(metaDataField, false);
            return new ArrayType(elementType);
        }
        if (type instanceof PrimitiveType) {
            switch (((PrimitiveType) type).getName()) {
                case "float":
                    return REAL;
                case "double":
                    return DOUBLE;
                case "byte":
                    return TINYINT;
                case "short":
                    return SMALLINT;
                case "integer":
                    return INTEGER;
                case "long":
                    return BIGINT;
                case "text":
                case "keyword":
                    return VARCHAR;
                case "ip":
                    return ipAddressType;
                case "boolean":
                    return BOOLEAN;
                case "binary":
                    return VARBINARY;
            }
        }
        else if (type instanceof DateTimeType) {
            if (((DateTimeType) type).getFormats().isEmpty()) {
                return TIMESTAMP_MILLIS;
            }
            // otherwise, skip -- we don't support custom formats, yet
        }
        else if (type instanceof ObjectType) {
            ObjectType objectType = (ObjectType) type;

            ImmutableList.Builder<RowType.Field> builder = ImmutableList.builder();
            for (IndexMetadata.Field field : objectType.getFields()) {
                Type trinoType = toTrinoType(field);
                if (trinoType != null) {
                    builder.add(RowType.field(field.getName(), trinoType));
                }
            }

            List<RowType.Field> fields = builder.build();

            if (!fields.isEmpty()) {
                return RowType.from(fields);
            }

            // otherwise, skip -- row types must have at least 1 field
        }

        return null;
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
        return tableMetadata.getColumnHandles();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ElasticsearchTableHandle table = (ElasticsearchTableHandle) tableHandle;
        ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) columnHandle;

        if (isPassthroughQuery(table)) {
            if (column.getName().equals(PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA.getName())) {
                return PASSTHROUGH_QUERY_RESULT_COLUMN_METADATA;
            }

            throw new IllegalArgumentException(format("Unexpected column for table '%s$query': %s", table.getIndex(), column.getName()));
        }

        return BuiltinColumns.of(column.getName())
                .map(BuiltinColumns::getMetadata)
                .orElse(ColumnMetadata.builder()
                        .setName(column.getName())
                        .setType(column.getType())
                        .build());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().isPresent() && !prefix.getSchema().get().equals(schemaName)) {
            return ImmutableMap.of();
        }

        if (prefix.getSchema().isPresent() && prefix.getTable().isPresent()) {
            ConnectorTableMetadata metadata = getTableMetadata(prefix.getSchema().get(), prefix.getTable().get());
            return ImmutableMap.of(metadata.getTable(), metadata.getColumns());
        }

        return listTables(session, prefix.getSchema()).stream()
                .map(name -> getTableMetadata(name.getSchemaName(), name.getTableName()))
                .collect(toImmutableMap(ConnectorTableMetadata::getTable, ConnectorTableMetadata::getColumns));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        return new ConnectorTableProperties(
                handle.getConstraint(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            // limit pushdown currently not supported passthrough query
            return Optional.empty();
        }

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new ElasticsearchTableHandle(
                handle.getType(),
                handle.getSchema(),
                handle.getIndex(),
                handle.getConstraint(),
                handle.getQuery(),
                OptionalLong.of(limit),
                handle.getTermAggregations(),
                handle.getMetricAggregations());

        return Optional.of(new LimitApplicationResult<>(handle, false, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            // filter pushdown currently not supported for passthrough query
            return Optional.empty();
        }

        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();
        if (constraint.getSummary().getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getSummary().getDomains().get().entrySet()) {
                ElasticsearchColumnHandle column = (ElasticsearchColumnHandle) entry.getKey();

                if (column.isSupportsPredicates()) {
                    supported.put(column, entry.getValue());
                }
                else {
                    unsupported.put(column, entry.getValue());
                }
            }
        }

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(supported));
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new ElasticsearchTableHandle(
                handle.getType(),
                handle.getSchema(),
                handle.getIndex(),
                newDomain,
                handle.getQuery(),
                handle.getLimit(),
                handle.getTermAggregations(),
                handle.getMetricAggregations());

        return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.withColumnDomains(unsupported), false));
    }

    private static boolean isPassthroughQuery(ElasticsearchTableHandle table)
    {
        return table.getType().equals(QUERY);
    }

    private static class InternalTableMetadata
    {
        private final SchemaTableName tableName;
        private final List<ColumnMetadata> columnMetadata;
        private final Map<String, ColumnHandle> columnHandles;

        public InternalTableMetadata(
                SchemaTableName tableName,
                List<ColumnMetadata> columnMetadata,
                Map<String, ColumnHandle> columnHandles)
        {
            this.tableName = tableName;
            this.columnMetadata = columnMetadata;
            this.columnHandles = columnHandles;
        }

        public SchemaTableName getTableName()
        {
            return tableName;
        }

        public List<ColumnMetadata> getColumnMetadata()
        {
            return columnMetadata;
        }

        public Map<String, ColumnHandle> getColumnHandles()
        {
            return columnHandles;
        }
    }
}
