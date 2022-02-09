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
package io.trino.plugin.pinot.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.PinotMetadata;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.PinotColumnHandle.fromNonAggregateColumnHandle;
import static io.trino.plugin.pinot.PinotColumnHandle.getTrinoTypeFromPinotType;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static io.trino.plugin.pinot.query.PinotExpressionRewriter.rewriteExpression;
import static io.trino.plugin.pinot.query.PinotPatterns.WILDCARD;
import static io.trino.plugin.pinot.query.PinotSqlFormatter.formatExpression;
import static io.trino.plugin.pinot.query.PinotSqlFormatter.formatFilter;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class DynamicTableBuilder
{
    private static final CalciteSqlCompiler REQUEST_COMPILER = new CalciteSqlCompiler();
    public static final String OFFLINE_SUFFIX = "_OFFLINE";
    public static final String REALTIME_SUFFIX = "_REALTIME";

    private DynamicTableBuilder()
    {
    }

    public static DynamicTable buildFromPql(PinotMetadata pinotMetadata, SchemaTableName schemaTableName, PinotClient pinotClient)
    {
        requireNonNull(pinotMetadata, "pinotMetadata is null");
        requireNonNull(schemaTableName, "schemaTableName is null");
        String query = schemaTableName.getTableName();
        BrokerRequest request = REQUEST_COMPILER.compileToBrokerRequest(query);
        PinotQuery pinotQuery = request.getPinotQuery();
        QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(request);
        String pinotTableName = stripSuffix(request.getQuerySource().getTableName());
        Optional<String> suffix = getSuffix(request.getQuerySource().getTableName());

        Map<String, ColumnHandle> columnHandles = pinotMetadata.getPinotColumnHandles(pinotTableName);
        List<OrderByExpression> orderBy = ImmutableList.of();
        PinotTypeResolver pinotTypeResolver = new PinotTypeResolver(pinotClient, pinotTableName);
        List<PinotColumnHandle> selectColumns = ImmutableList.of();

        ImmutableMap.Builder<String, Type> aggregateTypesBuilder = ImmutableMap.builder();
        if (queryContext.getAggregationFunctions() != null) {
            checkState(queryContext.getAggregationFunctions().length > 0, "Aggregation Functions is empty");
            for (AggregationFunction<?, ?> aggregationFunction : queryContext.getAggregationFunctions()) {
                aggregateTypesBuilder.put(aggregationFunction.getResultColumnName(), toTrinoType(aggregationFunction.getFinalResultColumnType()));
            }
        }
        Map<String, Type> aggregateTypes = aggregateTypesBuilder.buildOrThrow();
        if (queryContext.getSelectExpressions() != null) {
            checkState(!queryContext.getSelectExpressions().isEmpty(), "Pinot selections is empty");
            selectColumns = getPinotColumns(schemaTableName, queryContext.getSelectExpressions(), queryContext.getAliasList(), columnHandles, pinotTypeResolver, aggregateTypes);
        }

        if (queryContext.getOrderByExpressions() != null) {
            ImmutableList.Builder<OrderByExpression> orderByBuilder = ImmutableList.builder();
            for (OrderByExpressionContext orderByExpressionContext : queryContext.getOrderByExpressions()) {
                ExpressionContext expressionContext = orderByExpressionContext.getExpression();
                PinotColumnHandle pinotColumnHandle = getPinotColumnHandle(schemaTableName, expressionContext, Optional.empty(), columnHandles, pinotTypeResolver, aggregateTypes);
                orderByBuilder.add(new OrderByExpression(pinotColumnHandle.getExpression(), orderByExpressionContext.isAsc()));
            }
            orderBy = orderByBuilder.build();
        }

        List<PinotColumnHandle> groupByColumns = ImmutableList.of();
        if (queryContext.getGroupByExpressions() != null) {
            groupByColumns = getPinotColumns(schemaTableName, queryContext.getGroupByExpressions(), ImmutableList.of(), columnHandles, pinotTypeResolver, aggregateTypes);
        }

        Optional<String> filter = Optional.empty();
        if (pinotQuery.getFilterExpression() != null) {
            String formatted = formatFilter(schemaTableName, queryContext.getFilter(), columnHandles);
            filter = Optional.of(formatted);
        }

        return new DynamicTable(pinotTableName, suffix, selectColumns, filter, groupByColumns, ImmutableList.of(), orderBy, OptionalLong.of(queryContext.getLimit()), getOffset(queryContext), query);
    }

    private static Type toTrinoType(DataSchema.ColumnDataType columnDataType)
    {
        switch (columnDataType) {
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return VARCHAR;
            case BYTES:
                return VARBINARY;
            case INT_ARRAY:
                return new ArrayType(INTEGER);
            case LONG_ARRAY:
                return new ArrayType(BIGINT);
            case DOUBLE_ARRAY:
                return new ArrayType(DOUBLE);
            case STRING_ARRAY:
                return new ArrayType(VARCHAR);
            default:
                break;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported column data type: " + columnDataType);
    }

    private static List<PinotColumnHandle> getPinotColumns(SchemaTableName schemaTableName, List<ExpressionContext> expressions, List<String> aliases, Map<String, ColumnHandle> columnHandles, PinotTypeResolver pinotTypeResolver, Map<String, Type> aggregateTypes)
    {
        ImmutableList.Builder<PinotColumnHandle> pinotColumnsBuilder = ImmutableList.builder();
        for (int index = 0; index < expressions.size(); index++) {
            ExpressionContext expressionContext = expressions.get(index);
            Optional<String> alias = getAlias(aliases, index);
            // Only substitute * with columns for top level SELECT *.
            // Since Pinot doesn't support subqueries yet, we can only have one occurrence of SELECT *
            if (expressionContext.getType() == ExpressionContext.Type.IDENTIFIER && expressionContext.getIdentifier().equals(WILDCARD)) {
                pinotColumnsBuilder.addAll(columnHandles.values().stream()
                        .map(handle -> fromNonAggregateColumnHandle((PinotColumnHandle) handle))
                        .collect(toImmutableList()));
            }
            else {
                pinotColumnsBuilder.add(getPinotColumnHandle(schemaTableName, expressionContext, alias, columnHandles, pinotTypeResolver, aggregateTypes));
            }
        }
        return pinotColumnsBuilder.build();
    }

    private static PinotColumnHandle getPinotColumnHandle(SchemaTableName schemaTableName, ExpressionContext expressionContext, Optional<String> alias, Map<String, ColumnHandle> columnHandles, PinotTypeResolver pinotTypeResolver, Map<String, Type> aggregateTypes)
    {
        ExpressionContext rewritten = rewriteExpression(schemaTableName, expressionContext, columnHandles);
        // If there is no alias, pinot autogenerates the column name:
        String columnName = rewritten.toString();
        String pinotExpression = formatExpression(schemaTableName, rewritten);
        Type trinoType;
        boolean isAggregate = isAggregate(rewritten);
        if (isAggregate) {
            trinoType = requireNonNull(aggregateTypes.get(columnName.toLowerCase(ENGLISH)), format("Unexpected aggregate expression: '%s'", rewritten));
        }
        else {
            trinoType = getTrinoTypeFromPinotType(pinotTypeResolver.resolveExpressionType(rewritten, schemaTableName, columnHandles));
        }

        return new PinotColumnHandle(alias.orElse(columnName), trinoType, pinotExpression, alias.isPresent(), isAggregate, true, Optional.empty(), Optional.empty());
    }

    private static Optional<String> getAlias(List<String> aliases, int index)
    {
        // SELECT * is expanded to all columns with no aliases
        if (index >= aliases.size()) {
            return Optional.empty();
        }
        return Optional.ofNullable(aliases.get(index));
    }

    private static boolean isAggregate(ExpressionContext expressionContext)
    {
        return expressionContext.getType() == ExpressionContext.Type.FUNCTION && expressionContext.getFunction().getType() == FunctionContext.Type.AGGREGATION;
    }

    private static OptionalLong getOffset(QueryContext queryContext)
    {
        if (queryContext.getOffset() > 0) {
            return OptionalLong.of(queryContext.getOffset());
        }
        else {
            return OptionalLong.empty();
        }
    }

    private static String stripSuffix(String tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (tableName.toUpperCase(ENGLISH).endsWith(OFFLINE_SUFFIX)) {
            return tableName.substring(0, tableName.length() - OFFLINE_SUFFIX.length());
        }
        else if (tableName.toUpperCase(ENGLISH).endsWith(REALTIME_SUFFIX)) {
            return tableName.substring(0, tableName.length() - REALTIME_SUFFIX.length());
        }
        else {
            return tableName;
        }
    }

    private static Optional<String> getSuffix(String tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (tableName.toUpperCase(ENGLISH).endsWith(OFFLINE_SUFFIX)) {
            return Optional.of(OFFLINE_SUFFIX);
        }
        else if (tableName.toUpperCase(ENGLISH).endsWith(REALTIME_SUFFIX)) {
            return Optional.of(REALTIME_SUFFIX);
        }
        else {
            return Optional.empty();
        }
    }
}
