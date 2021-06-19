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
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.query.FilterToPinotSqlConverter.convertFilter;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class DynamicTableBuilder
{
    private static final CalciteSqlCompiler REQUEST_COMPILER = new CalciteSqlCompiler();
    private static final String COLUMN_KEY = "column";
    private static final String WILDCARD = "*";
    public static final Set<String> DOUBLE_AGGREGATIONS = ImmutableSet.of("distinctcounthll", "avg");
    public static final Set<String> BIGINT_AGGREGATIONS = ImmutableSet.of("count", "distinctcount");
    public static final String OFFLINE_SUFFIX = "_OFFLINE";
    public static final String REALTIME_SUFFIX = "_REALTIME";

    private DynamicTableBuilder()
    {
    }

    public static DynamicTable buildFromPql(PinotMetadata pinotMetadata, SchemaTableName schemaTableName)
    {
        requireNonNull(pinotMetadata, "pinotMetadata is null");
        requireNonNull(schemaTableName, "schemaTableName is null");
        String query = schemaTableName.getTableName();
        BrokerRequest request = REQUEST_COMPILER.compileToBrokerRequest(query);
        String pinotTableName = stripSuffix(request.getQuerySource().getTableName());
        Optional<String> suffix = getSuffix(request.getQuerySource().getTableName());

        Map<String, ColumnHandle> columnHandles = pinotMetadata.getPinotColumnHandles(pinotTableName);
        List<String> selectionColumns = ImmutableList.of();
        List<OrderByExpression> orderBy = ImmutableList.of();
        if (request.getSelections() != null) {
            selectionColumns = resolvePinotColumns(schemaTableName, request.getSelections().getSelectionColumns(), columnHandles);
            if (request.getSelections().getSelectionSortSequence() != null) {
                ImmutableList.Builder<OrderByExpression> orderByBuilder = ImmutableList.builder();
                for (SelectionSort sortItem : request.getSelections().getSelectionSortSequence()) {
                    PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(sortItem.getColumn());
                    if (columnHandle == null) {
                        throw new ColumnNotFoundException(schemaTableName, sortItem.getColumn());
                    }
                    orderByBuilder.add(new OrderByExpression(columnHandle.getColumnName(), sortItem.isIsAsc()));
                }
                orderBy = orderByBuilder.build();
            }
        }

        List<String> groupByColumns;
        if (request.getGroupBy() == null) {
            groupByColumns = ImmutableList.of();
        }
        else {
            groupByColumns = resolvePinotColumns(schemaTableName, request.getGroupBy().getExpressions(), columnHandles);
        }

        Optional<String> filter;
        if (request.getFilterQuery() != null) {
            filter = Optional.of(convertFilter(request.getPinotQuery(), columnHandles));
        }
        else {
            filter = Optional.empty();
        }

        ImmutableList.Builder<AggregationExpression> aggregationExpressionBuilder = ImmutableList.builder();
        if (request.getAggregationsInfo() != null) {
            for (AggregationInfo aggregationInfo : request.getAggregationsInfo()) {
                String baseColumnName = aggregationInfo.getAggregationParams().get(COLUMN_KEY);
                AggregationExpression aggregationExpression;
                if (baseColumnName.equals(WILDCARD)) {
                    aggregationExpression = new AggregationExpression(getOutputColumnName(aggregationInfo, baseColumnName),
                            baseColumnName,
                            aggregationInfo.getAggregationType());
                }
                else {
                    PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(baseColumnName);
                    if (columnHandle == null) {
                        throw new ColumnNotFoundException(schemaTableName, aggregationInfo.getAggregationParams().get(COLUMN_KEY));
                    }
                    aggregationExpression = new AggregationExpression(
                            getOutputColumnName(aggregationInfo, columnHandle.getColumnName()),
                            columnHandle.getColumnName(),
                            aggregationInfo.getAggregationType());
                }

                aggregationExpressionBuilder.add(aggregationExpression);
            }
        }

        return new DynamicTable(pinotTableName, suffix, selectionColumns, groupByColumns, filter, aggregationExpressionBuilder.build(), orderBy, getTopNOrLimit(request), getOffset(request), query);
    }

    private static List<String> resolvePinotColumns(SchemaTableName schemaTableName, List<String> trinoColumnNames, Map<String, ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<String> pinotColumnNamesBuilder = ImmutableList.builder();
        for (String trinoColumnName : trinoColumnNames) {
            if (trinoColumnName.equals(WILDCARD)) {
                pinotColumnNamesBuilder.addAll(columnHandles.values().stream().map(handle -> ((PinotColumnHandle) handle).getColumnName()).collect(toImmutableList()));
            }
            else {
                PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(trinoColumnName);
                if (columnHandle == null) {
                    throw new ColumnNotFoundException(schemaTableName, trinoColumnName);
                }
                pinotColumnNamesBuilder.add(columnHandle.getColumnName());
            }
        }
        return pinotColumnNamesBuilder.build();
    }

    private static String getOutputColumnName(AggregationInfo aggregationInfo, String pinotColumnName)
    {
        return format("%s(%s)", aggregationInfo.getAggregationType(), pinotColumnName).toLowerCase(ENGLISH);
    }

    private static OptionalLong getTopNOrLimit(BrokerRequest request)
    {
        if (request.getGroupBy() != null) {
            return OptionalLong.of(request.getGroupBy().getTopN());
        }
        else if (request.getSelections() != null) {
            return OptionalLong.of(request.getSelections().getSize());
        }
        else {
            return OptionalLong.empty();
        }
    }

    private static OptionalLong getOffset(BrokerRequest request)
    {
        if (request.getSelections() != null && request.getSelections().getOffset() > 0) {
            return OptionalLong.of(request.getSelections().getOffset());
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
