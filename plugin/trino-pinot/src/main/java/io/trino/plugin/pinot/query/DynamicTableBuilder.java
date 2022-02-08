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
import org.apache.pinot.core.query.reduce.PostAggregationHandler;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.PinotColumnHandle.fromNonAggregateColumnHandle;
import static io.trino.plugin.pinot.PinotColumnHandle.getTrinoTypeFromPinotType;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
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
import static org.apache.pinot.segment.spi.AggregationFunctionType.COUNT;
import static org.apache.pinot.segment.spi.AggregationFunctionType.DISTINCTCOUNT;
import static org.apache.pinot.segment.spi.AggregationFunctionType.DISTINCTCOUNTHLL;
import static org.apache.pinot.segment.spi.AggregationFunctionType.getAggregationFunctionType;

public final class DynamicTableBuilder
{
    private static final CalciteSqlCompiler REQUEST_COMPILER = new CalciteSqlCompiler();
    public static final String OFFLINE_SUFFIX = "_OFFLINE";
    public static final String REALTIME_SUFFIX = "_REALTIME";
    private static final Set<AggregationFunctionType> NON_NULL_ON_EMPTY_AGGREGATIONS = EnumSet.of(COUNT, DISTINCTCOUNT, DISTINCTCOUNTHLL);

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

        Map<String, PinotColumnNameAndTrinoType> aggregateTypes = ImmutableMap.of();
        if (queryContext.getAggregationFunctions() != null) {
            checkState(queryContext.getAggregationFunctions().length > 0, "Aggregation Functions is empty");
            aggregateTypes = getAggregateTypes(schemaTableName, queryContext, columnHandles);
        }

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

    private static List<PinotColumnHandle> getPinotColumns(SchemaTableName schemaTableName, List<ExpressionContext> expressions, List<String> aliases, Map<String, ColumnHandle> columnHandles, PinotTypeResolver pinotTypeResolver, Map<String, PinotColumnNameAndTrinoType> aggregateTypes)
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

    private static PinotColumnHandle getPinotColumnHandle(SchemaTableName schemaTableName, ExpressionContext expressionContext, Optional<String> alias, Map<String, ColumnHandle> columnHandles, PinotTypeResolver pinotTypeResolver, Map<String, PinotColumnNameAndTrinoType> aggregateTypes)
    {
        ExpressionContext rewritten = rewriteExpression(schemaTableName, expressionContext, columnHandles);
        // If there is no alias, pinot autogenerates the column name:
        String columnName = rewritten.toString();
        String pinotExpression = formatExpression(schemaTableName, rewritten);
        Type trinoType;
        boolean isAggregate = hasAggregate(rewritten);
        if (isAggregate) {
            trinoType = requireNonNull(aggregateTypes.get(columnName).getTrinoType(), format("Unexpected aggregate expression: '%s'", rewritten));
            // For aggregation queries, the column name is set by the schema returned from PostAggregationHandler, see getAggregateTypes
            columnName = aggregateTypes.get(columnName).getPinotColumnName();
        }
        else {
            trinoType = getTrinoTypeFromPinotType(pinotTypeResolver.resolveExpressionType(rewritten, schemaTableName, columnHandles));
        }

        return new PinotColumnHandle(alias.orElse(columnName), trinoType, pinotExpression, alias.isPresent(), isAggregate, isReturnNullOnEmptyGroup(expressionContext), Optional.empty(), Optional.empty());
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

    private static boolean hasAggregate(ExpressionContext expressionContext)
    {
        switch (expressionContext.getType()) {
            case IDENTIFIER:
            case LITERAL:
                return false;
            case FUNCTION:
                if (isAggregate(expressionContext)) {
                    return true;
                }
                for (ExpressionContext argument : expressionContext.getFunction().getArguments()) {
                    if (hasAggregate(argument)) {
                        return true;
                    }
                }
                return false;
        }
        throw new PinotException(PINOT_EXCEPTION, Optional.empty(), format("Unsupported expression type '%s'", expressionContext.getType()));
    }

    private static Map<String, PinotColumnNameAndTrinoType> getAggregateTypes(SchemaTableName schemaTableName, QueryContext queryContext, Map<String, ColumnHandle> columnHandles)
    {
        // A mapping from pinot expression to the returned pinot column name and trino type
        // Note: the column name is set by the PostAggregationHandler
        List<ExpressionContext> aggregateColumnExpressions = queryContext.getSelectExpressions().stream()
                .filter(DynamicTableBuilder::hasAggregate)
                .collect(toImmutableList());
        queryContext = new QueryContext.Builder()
                .setAliasList(queryContext.getAliasList())
                .setSelectExpressions(aggregateColumnExpressions)
                .build();
        DataSchema preAggregationSchema = getPreAggregationDataSchema(queryContext);
        PostAggregationHandler postAggregationHandler = new PostAggregationHandler(queryContext, preAggregationSchema);
        DataSchema postAggregtionSchema = postAggregationHandler.getResultDataSchema();
        ImmutableMap.Builder<String, PinotColumnNameAndTrinoType> aggregationTypesBuilder = ImmutableMap.builder();
        for (int index = 0; index < postAggregtionSchema.size(); index++) {
            aggregationTypesBuilder.put(
                    // ExpressionContext#toString performs quoting of literals
                    // Quoting of identifiers is not done to match the corresponding column name in the ResultTable returned from Pinot. Quoting will be done by `DynamicTablePqlExtractor`.
                    rewriteExpression(schemaTableName,
                            aggregateColumnExpressions.get(index),
                            columnHandles).toString(),
                    new PinotColumnNameAndTrinoType(
                            postAggregtionSchema.getColumnName(index),
                            toTrinoType(postAggregtionSchema.getColumnDataType(index))));
        }
        return aggregationTypesBuilder.buildOrThrow();
    }

    // Extracted from org.apache.pinot.core.query.reduce.AggregationDataTableReducer
    private static DataSchema getPreAggregationDataSchema(QueryContext queryContext)
    {
        AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
        int numAggregationFunctions = aggregationFunctions.length;
        String[] columnNames = new String[numAggregationFunctions];
        DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numAggregationFunctions];
        for (int i = 0; i < numAggregationFunctions; i++) {
            AggregationFunction aggregationFunction = aggregationFunctions[i];
            columnNames[i] = aggregationFunction.getResultColumnName();
            columnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
        }
        return new DataSchema(columnNames, columnDataTypes);
    }

    // To keep consistent behavior with pushed down aggregates, only return non null on an empty group
    // if the top level function is in NON_NULL_ON_EMPTY_AGGREGATIONS.
    // For all other cases, keep the same behavior as Pinot, since likely the same results are expected.
    private static boolean isReturnNullOnEmptyGroup(ExpressionContext expressionContext)
    {
        if (isAggregate(expressionContext)) {
            return !NON_NULL_ON_EMPTY_AGGREGATIONS.contains(getAggregationFunctionType(expressionContext.getFunction().getFunctionName()));
        }
        return true;
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

    private static class PinotColumnNameAndTrinoType
    {
        private final String pinotColumnName;
        private final Type trinoType;

        public PinotColumnNameAndTrinoType(String pinotColumnName, Type trinoType)
        {
            this.pinotColumnName = requireNonNull(pinotColumnName, "pinotColumnName is null");
            this.trinoType = requireNonNull(trinoType, "trinoType is null");
        }

        public String getPinotColumnName()
        {
            return pinotColumnName;
        }

        public Type getTrinoType()
        {
            return trinoType;
        }
    }
}
