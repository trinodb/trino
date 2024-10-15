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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.opensearch.client.IndexMetadata;
import io.trino.plugin.opensearch.client.IndexMetadata.DateTimeType;
import io.trino.plugin.opensearch.client.IndexMetadata.ObjectType;
import io.trino.plugin.opensearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.opensearch.client.IndexMetadata.ScaledFloatType;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.ArrayDecoder;
import io.trino.plugin.opensearch.decoders.BigintDecoder;
import io.trino.plugin.opensearch.decoders.BooleanDecoder;
import io.trino.plugin.opensearch.decoders.DoubleDecoder;
import io.trino.plugin.opensearch.decoders.IntegerDecoder;
import io.trino.plugin.opensearch.decoders.IpAddressDecoder;
import io.trino.plugin.opensearch.decoders.RawJsonDecoder;
import io.trino.plugin.opensearch.decoders.RealDecoder;
import io.trino.plugin.opensearch.decoders.RowDecoder;
import io.trino.plugin.opensearch.decoders.SmallintDecoder;
import io.trino.plugin.opensearch.decoders.TimestampDecoder;
import io.trino.plugin.opensearch.decoders.TinyintDecoder;
import io.trino.plugin.opensearch.decoders.VarbinaryDecoder;
import io.trino.plugin.opensearch.decoders.VarcharDecoder;
import io.trino.plugin.opensearch.ptf.RawQuery.RawQueryFunctionHandle;
import io.trino.spi.TrinoException;
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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
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
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import org.opensearch.client.ResponseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterators.singletonIterator;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.opensearch.OpenSearchSessionProperties.isProjectionPushdownEnabled;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
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
import static java.util.Collections.emptyIterator;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class OpenSearchMetadata
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
            PASSTHROUGH_QUERY_RESULT_COLUMN_NAME,
            new OpenSearchColumnHandle(
                    ImmutableList.of(PASSTHROUGH_QUERY_RESULT_COLUMN_NAME),
                    VARCHAR,
                    new IndexMetadata.PrimitiveType("text"),
                    new VarcharDecoder.Descriptor(PASSTHROUGH_QUERY_RESULT_COLUMN_NAME),
                    false));

    // See https://opensearch.org/docs/latest/query-dsl/term/regexp/
    private static final Set<Integer> REGEXP_RESERVED_CHARACTERS = IntStream.of('.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '#', '@', '&', '<', '>', '~')
            .boxed()
            .collect(toImmutableSet());

    private final Type ipAddressType;
    private final OpenSearchClient client;
    private final String schemaName;

    @Inject
    public OpenSearchMetadata(TypeManager typeManager, OpenSearchClient client, OpenSearchConfig config)
    {
        this.ipAddressType = typeManager.getType(new TypeSignature(StandardTypes.IPADDRESS));
        this.client = requireNonNull(client, "client is null");
        this.schemaName = config.getDefaultSchema();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(schemaName);
    }

    @Override
    public OpenSearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        requireNonNull(tableName, "tableName is null");

        if (tableName.getSchemaName().equals(schemaName)) {
            if (client.indexExists(tableName.getTableName()) && !client.getIndexMetadata(tableName.getTableName()).schema().fields().isEmpty()) {
                return new OpenSearchTableHandle(OpenSearchTableHandle.Type.SCAN, schemaName, tableName.getTableName(), Optional.empty());
            }
        }

        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        OpenSearchTableHandle handle = (OpenSearchTableHandle) table;

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
        OpenSearchTableHandle handle = (OpenSearchTableHandle) table;
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
            result.put(field.name(), new OpenSearchColumnHandle(
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

        checkArgument(!field.asRawJson() || !field.isArray(), format("A column, (%s) cannot be declared as a Trino array and also be rendered as json.", path));

        if (field.asRawJson()) {
            return new TypeAndDecoder(VARCHAR, new RawJsonDecoder.Descriptor(path));
        }

        if (field.isArray()) {
            TypeAndDecoder element = toTrino(path, elementField(field));
            return new TypeAndDecoder(new ArrayType(element.type()), new ArrayDecoder.Descriptor(element.decoderDescriptor()));
        }

        IndexMetadata.Type type = field.type();
        if (type instanceof PrimitiveType primitiveType) {
            switch (primitiveType.name()) {
                case "float":
                    return new TypeAndDecoder(REAL, new RealDecoder.Descriptor(path));
                case "double":
                    return new TypeAndDecoder(DOUBLE, new DoubleDecoder.Descriptor(path));
                case "byte":
                    return new TypeAndDecoder(TINYINT, new TinyintDecoder.Descriptor(path));
                case "short":
                    return new TypeAndDecoder(SMALLINT, new SmallintDecoder.Descriptor(path));
                case "integer":
                    return new TypeAndDecoder(INTEGER, new IntegerDecoder.Descriptor(path));
                case "long":
                    return new TypeAndDecoder(BIGINT, new BigintDecoder.Descriptor(path));
                case "text":
                case "keyword":
                    return new TypeAndDecoder(VARCHAR, new VarcharDecoder.Descriptor(path));
                case "ip":
                    return new TypeAndDecoder(ipAddressType, new IpAddressDecoder.Descriptor(path, ipAddressType));
                case "boolean":
                    return new TypeAndDecoder(BOOLEAN, new BooleanDecoder.Descriptor(path));
                case "binary":
                    return new TypeAndDecoder(VARBINARY, new VarbinaryDecoder.Descriptor(path));
            }
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
        OpenSearchTableHandle table = (OpenSearchTableHandle) tableHandle;

        if (isPassthroughQuery(table)) {
            return PASSTHROUGH_QUERY_COLUMNS;
        }

        InternalTableMetadata tableMetadata = makeInternalTableMetadata(tableHandle);
        return tableMetadata.columnHandles();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        OpenSearchTableHandle table = (OpenSearchTableHandle) tableHandle;
        OpenSearchColumnHandle column = (OpenSearchColumnHandle) columnHandle;

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

    @SuppressWarnings("deprecation")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @SuppressWarnings("deprecation") // TODO Implement streamRelationColumns method
    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().isPresent() && !prefix.getSchema().get().equals(schemaName)) {
            return emptyIterator();
        }

        if (prefix.getSchema().isPresent() && prefix.getTable().isPresent()) {
            ConnectorTableMetadata metadata = getTableMetadata(prefix.getSchema().get(), prefix.getTable().get());
            return singletonIterator(TableColumnsMetadata.forTable(metadata.getTable(), metadata.getColumns()));
        }

        return listTables(session, prefix.getSchema()).stream()
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
                .iterator();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        OpenSearchTableHandle handle = (OpenSearchTableHandle) table;

        return new ConnectorTableProperties(
                handle.constraint(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        OpenSearchTableHandle handle = (OpenSearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            // limit pushdown currently not supported passthrough query
            return Optional.empty();
        }

        if (handle.limit().isPresent() && handle.limit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new OpenSearchTableHandle(
                handle.type(),
                handle.schema(),
                handle.index(),
                handle.constraint(),
                handle.regexes(),
                handle.query(),
                OptionalLong.of(limit),
                ImmutableSet.of());

        return Optional.of(new LimitApplicationResult<>(handle, false, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        OpenSearchTableHandle handle = (OpenSearchTableHandle) table;

        if (isPassthroughQuery(handle)) {
            // filter pushdown currently not supported for passthrough query
            return Optional.empty();
        }

        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();
        Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            OpenSearchColumnHandle column = (OpenSearchColumnHandle) entry.getKey();

            if (column.supportsPredicates()) {
                supported.put(column, entry.getValue());
            }
            else {
                unsupported.put(column, entry.getValue());
            }
        }

        TupleDomain<ColumnHandle> oldDomain = handle.constraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(supported));

        ConnectorExpression oldExpression = constraint.getExpression();
        Map<String, String> newRegexes = new HashMap<>(handle.regexes());
        List<ConnectorExpression> expressions = ConnectorExpressions.extractConjuncts(constraint.getExpression());
        List<ConnectorExpression> notHandledExpressions = new ArrayList<>();
        for (ConnectorExpression expression : expressions) {
            if (expression instanceof Call call) {
                if (isSupportedLikeCall(call)) {
                    List<ConnectorExpression> arguments = call.getArguments();
                    String variableName = ((Variable) arguments.get(0)).getName();
                    OpenSearchColumnHandle column = (OpenSearchColumnHandle) constraint.getAssignments().get(variableName);
                    verifyNotNull(column, "No assignment for %s", variableName);
                    String columnName = column.name();
                    Object pattern = ((Constant) arguments.get(1)).getValue();
                    Optional<Slice> escape = Optional.empty();
                    if (arguments.size() == 3) {
                        escape = Optional.of((Slice) ((Constant) arguments.get(2)).getValue());
                    }

                    if (!newRegexes.containsKey(columnName) && pattern instanceof Slice) {
                        IndexMetadata metadata = client.getIndexMetadata(handle.index());
                        if (metadata.schema()
                                    .fields().stream()
                                    .anyMatch(field -> columnName.equals(field.name()) && field.type() instanceof PrimitiveType && "keyword".equals(((PrimitiveType) field.type()).name()))) {
                            newRegexes.put(columnName, likeToRegexp((Slice) pattern, escape));
                            continue;
                        }
                    }
                }
            }
            notHandledExpressions.add(expression);
        }

        ConnectorExpression newExpression = ConnectorExpressions.and(notHandledExpressions);
        if (oldDomain.equals(newDomain) && oldExpression.equals(newExpression)) {
            return Optional.empty();
        }

        handle = new OpenSearchTableHandle(
                handle.type(),
                handle.schema(),
                handle.index(),
                newDomain,
                newRegexes,
                handle.query(),
                handle.limit(),
                ImmutableSet.of());

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

    protected static String likeToRegexp(Slice pattern, Optional<Slice> escape)
    {
        Optional<Character> escapeChar = escape.map(OpenSearchMetadata::getEscapeChar);
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
                    case '%':
                        regex.append(escaped ? "%" : ".*");
                        escaped = false;
                        break;
                    case '_':
                        regex.append(escaped ? "_" : ".");
                        escaped = false;
                        break;
                    case '\\':
                        regex.append("\\\\");
                        break;
                    default:
                        // escape special regex characters
                        if (REGEXP_RESERVED_CHARACTERS.contains(currentChar)) {
                            regex.append('\\');
                        }

                        regex.appendCodePoint(currentChar);
                        escaped = false;
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

    private static boolean isPassthroughQuery(OpenSearchTableHandle table)
    {
        return table.type().equals(OpenSearchTableHandle.Type.QUERY);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }
        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression, OpenSearchMetadata::isSupportedForPushdown).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ApplyProjectionUtil.ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        OpenSearchTableHandle openSearchTableHandle = (OpenSearchTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ApplyProjectionUtil.ProjectedColumnRepresentation::isVariable)) {
            Set<OpenSearchColumnHandle> projectedColumns = assignments.values().stream()
                    .map(OpenSearchColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (openSearchTableHandle.columns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((OpenSearchColumnHandle) assignment.getValue()).type()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    openSearchTableHandle.withColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<OpenSearchColumnHandle> columns = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ApplyProjectionUtil.ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ApplyProjectionUtil.ProjectedColumnRepresentation projectedColumn = entry.getValue();

            OpenSearchColumnHandle baseColumnHandle = (OpenSearchColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            OpenSearchColumnHandle projectedColumnHandle = projectColumn(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
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
                openSearchTableHandle.withColumns(columns.build()),
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

    private static OpenSearchColumnHandle projectColumn(OpenSearchColumnHandle baseColumn, List<Integer> indices, Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return baseColumn;
        }
        ImmutableList.Builder<String> path = ImmutableList.builder();
        path.addAll(baseColumn.path());

        DecoderDescriptor decoderDescriptor = baseColumn.decoderDescriptor();
        IndexMetadata.Type opensearchType = baseColumn.opensearchType();
        Type type = baseColumn.type();

        for (int index : indices) {
            checkArgument(type instanceof RowType, "type should be Row type");
            RowType rowType = (RowType) type;
            RowType.Field field = rowType.getFields().get(index);
            path.add(field.getName()
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ROW type does not have field names declared: " + rowType)));
            type = field.getType();

            checkArgument(decoderDescriptor instanceof RowDecoder.Descriptor, "decoderDescriptor should be RowDecoder.Descriptor type");
            decoderDescriptor = ((RowDecoder.Descriptor) decoderDescriptor).getFields().get(index).getDescriptor();
            opensearchType = ((IndexMetadata.ObjectType) opensearchType).fields().get(index).type();
        }

        return new OpenSearchColumnHandle(
                path.build(),
                projectedColumnType,
                opensearchType,
                decoderDescriptor,
                supportsPredicates(opensearchType, projectedColumnType));
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof RawQueryFunctionHandle)) {
            return Optional.empty();
        }

        ConnectorTableHandle tableHandle = ((RawQueryFunctionHandle) handle).getTableHandle();
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(getColumnHandles(session, tableHandle).values());
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    private static boolean supportsPredicates(IndexMetadata.Type type, Type trinoType)
    {
        return switch (trinoType) {
            case TimestampType _, BooleanType _, TinyintType _, SmallintType _, IntegerType _, BigintType _, RealType _ -> true;
            case DoubleType _ -> !(type instanceof ScaledFloatType);
            case VarcharType _ when type instanceof PrimitiveType primitiveType && primitiveType.name().toLowerCase(ENGLISH).equals("keyword") -> true;
            default -> false;
        };
    }

    private record InternalTableMetadata(SchemaTableName tableName, List<ColumnMetadata> columnMetadata, Map<String, ColumnHandle> columnHandles) {}

    private record TypeAndDecoder(Type type, DecoderDescriptor decoderDescriptor) {}
}
