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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.aggregation.MaxDataSizeForStats;
import io.trino.operator.aggregation.SumDataSizeForStats;
import io.trino.spi.TrinoException;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.Objects.requireNonNull;

public class StatisticsAggregationPlanner
{
    private final SymbolAllocator symbolAllocator;
    private final Metadata metadata;
    private final Session session;

    public StatisticsAggregationPlanner(SymbolAllocator symbolAllocator, Metadata metadata, Session session)
    {
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
    }

    public TableStatisticAggregation createStatisticsAggregation(TableStatisticsMetadata statisticsMetadata, Map<String, Symbol> columnToSymbolMap)
    {
        StatisticAggregationsDescriptor.Builder<Symbol> descriptor = StatisticAggregationsDescriptor.builder();

        List<String> groupingColumns = statisticsMetadata.getGroupingColumns();
        List<Symbol> groupingSymbols = groupingColumns.stream()
                .map(columnToSymbolMap::get)
                .collect(toImmutableList());

        for (int i = 0; i < groupingSymbols.size(); i++) {
            descriptor.addGrouping(groupingColumns.get(i), groupingSymbols.get(i));
        }

        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        for (TableStatisticType type : statisticsMetadata.getTableStatistics()) {
            if (type != ROW_COUNT) {
                throw new TrinoException(NOT_SUPPORTED, "Table-wide statistic type not supported: " + type);
            }
            AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                    metadata.resolveFunction(session, QualifiedName.of("count"), ImmutableList.of()),
                    ImmutableList.of(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            Symbol symbol = symbolAllocator.newSymbol("rowCount", BIGINT);
            aggregations.put(symbol, aggregation);
            descriptor.addTableStatistic(ROW_COUNT, symbol);
        }

        for (ColumnStatisticMetadata columnStatisticMetadata : statisticsMetadata.getColumnStatistics()) {
            String columnName = columnStatisticMetadata.getColumnName();
            Symbol inputSymbol = columnToSymbolMap.get(columnName);
            verifyNotNull(inputSymbol, "no symbol for [%s] column, these columns exist: %s", columnName, columnToSymbolMap.keySet());
            Type inputType = symbolAllocator.getTypes().get(inputSymbol);
            verifyNotNull(inputType, "inputType is null for symbol: %s", inputSymbol);
            ColumnStatisticsAggregation aggregation;
            String symbolHint;
            if (columnStatisticMetadata.getStatisticTypeIfPresent().isPresent()) {
                ColumnStatisticType statisticType = columnStatisticMetadata.getStatisticType();
                aggregation = createColumnAggregation(statisticType, inputSymbol, inputType);
                symbolHint = statisticType + ":" + columnName;
            }
            else {
                FunctionName aggregationName = columnStatisticMetadata.getAggregation();
                aggregation = createColumnAggregation(aggregationName, inputSymbol, inputType);
                symbolHint = aggregationName.getName() + ":" + columnName;
            }
            Symbol symbol = symbolAllocator.newSymbol(symbolHint, aggregation.getOutputType());
            aggregations.put(symbol, aggregation.getAggregation());
            descriptor.addColumnStatistic(columnStatisticMetadata, symbol);
        }

        StatisticAggregations aggregation = new StatisticAggregations(aggregations.buildOrThrow(), groupingSymbols);
        return new TableStatisticAggregation(aggregation, descriptor.build());
    }

    private ColumnStatisticsAggregation createColumnAggregation(ColumnStatisticType statisticType, Symbol input, Type inputType)
    {
        return switch (statisticType) {
            case MIN_VALUE -> createAggregation(QualifiedName.of("min"), input, inputType);
            case MAX_VALUE -> createAggregation(QualifiedName.of("max"), input, inputType);
            case NUMBER_OF_DISTINCT_VALUES -> createAggregation(QualifiedName.of("approx_distinct"), input, inputType);
            case NUMBER_OF_DISTINCT_VALUES_SUMMARY ->
                // we use $approx_set here and not approx_set because latter is not defined for all types supported by Trino
                    createAggregation(QualifiedName.of("$approx_set"), input, inputType);
            case NUMBER_OF_NON_NULL_VALUES -> createAggregation(QualifiedName.of("count"), input, inputType);
            case NUMBER_OF_TRUE_VALUES -> createAggregation(QualifiedName.of("count_if"), input, BOOLEAN);
            case TOTAL_SIZE_IN_BYTES -> createAggregation(QualifiedName.of(SumDataSizeForStats.NAME), input, inputType);
            case MAX_VALUE_SIZE_IN_BYTES -> createAggregation(QualifiedName.of(MaxDataSizeForStats.NAME), input, inputType);
        };
    }

    private ColumnStatisticsAggregation createColumnAggregation(FunctionName aggregation, Symbol input, Type inputType)
    {
        checkArgument(aggregation.getCatalogSchema().isEmpty(), "Catalog/schema name not supported");
        return createAggregation(QualifiedName.of(aggregation.getName()), input, inputType);
    }

    private ColumnStatisticsAggregation createAggregation(QualifiedName functionName, Symbol input, Type inputType)
    {
        ResolvedFunction resolvedFunction = metadata.resolveFunction(session, functionName, fromTypes(inputType));
        Type resolvedType = getOnlyElement(resolvedFunction.getSignature().getArgumentTypes());
        verify(resolvedType.equals(inputType), "resolved function input type does not match the input type: %s != %s", resolvedType, inputType);
        return new ColumnStatisticsAggregation(
                new AggregationNode.Aggregation(
                        resolvedFunction,
                        ImmutableList.of(input.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                resolvedFunction.getSignature().getReturnType());
    }

    public static class TableStatisticAggregation
    {
        private final StatisticAggregations aggregations;
        private final StatisticAggregationsDescriptor<Symbol> descriptor;

        private TableStatisticAggregation(
                StatisticAggregations aggregations,
                StatisticAggregationsDescriptor<Symbol> descriptor)
        {
            this.aggregations = requireNonNull(aggregations, "aggregations is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
        }

        public StatisticAggregations getAggregations()
        {
            return aggregations;
        }

        public StatisticAggregationsDescriptor<Symbol> getDescriptor()
        {
            return descriptor;
        }
    }

    public static class ColumnStatisticsAggregation
    {
        private final AggregationNode.Aggregation aggregation;
        private final Type outputType;

        private ColumnStatisticsAggregation(AggregationNode.Aggregation aggregation, Type outputType)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
        }

        public AggregationNode.Aggregation getAggregation()
        {
            return aggregation;
        }

        public Type getOutputType()
        {
            return outputType;
        }
    }
}
