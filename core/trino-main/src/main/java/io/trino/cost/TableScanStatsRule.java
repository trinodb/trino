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
package io.trino.cost;

import io.trino.Session;
import io.trino.matching.Pattern;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isStatisticsPrecalculationForPushdownEnabled;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.lang.Double.NaN;
import static java.util.Objects.requireNonNull;

public class TableScanStatsRule
        extends SimpleStatsRule<TableScanNode>
{
    private static final double UNKNOWN_NULLS_FRACTION = 0.1;
    private static final Pattern<TableScanNode> PATTERN = tableScan();

    public TableScanStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer); // Use stats normalization since connector can return inconsistent stats values
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TableScanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types, TableStatsProvider tableStatsProvider)
    {
        if (isStatisticsPrecalculationForPushdownEnabled(session) && node.getStatistics().isPresent()) {
            return node.getStatistics();
        }

        TableStatistics tableStatistics = tableStatsProvider.getTableStatistics(node.getTable());

        Map<Symbol, SymbolStatsEstimate> outputSymbolStats = new HashMap<>();

        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            Optional<ColumnStatistics> columnStatistics = Optional.ofNullable(tableStatistics.getColumnStatistics().get(entry.getValue()));
            SymbolStatsEstimate symbolStatistics = columnStatistics
                    .map(statistics -> toSymbolStatistics(tableStatistics, statistics, types.get(symbol)))
                    .orElse(SymbolStatsEstimate.unknown());
            outputSymbolStats.put(symbol, symbolStatistics);
        }

        return Optional.of(PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount().getValue())
                .addSymbolStatistics(outputSymbolStats)
                .build());
    }

    private static SymbolStatsEstimate toSymbolStatistics(TableStatistics tableStatistics, ColumnStatistics columnStatistics, Type type)
    {
        requireNonNull(tableStatistics, "tableStatistics is null");
        requireNonNull(columnStatistics, "columnStatistics is null");
        requireNonNull(type, "type is null");

        double nullsFraction = getNullsFraction(columnStatistics, tableStatistics.getRowCount());
        double nonNullRowsCount = tableStatistics.getRowCount().getValue() * (1.0 - nullsFraction);
        double averageRowSize;
        if (nonNullRowsCount == 0) {
            averageRowSize = 0;
        }
        else if (type instanceof FixedWidthType) {
            // For a fixed-width type, engine knows the row size.
            averageRowSize = NaN;
        }
        else {
            averageRowSize = columnStatistics.getDataSize().getValue() / nonNullRowsCount;
        }
        SymbolStatsEstimate.Builder result = SymbolStatsEstimate.builder();
        result.setNullsFraction(nullsFraction);
        result.setDistinctValuesCount(columnStatistics.getDistinctValuesCount().getValue());
        result.setAverageRowSize(averageRowSize);
        columnStatistics.getRange().ifPresent(range -> {
            result.setLowValue(range.getMin());
            result.setHighValue(range.getMax());
        });
        return result.build();
    }

    private static double getNullsFraction(ColumnStatistics columnStatistics, Estimate rowCount)
    {
        if (!columnStatistics.getNullsFraction().isUnknown()
                || columnStatistics.getDistinctValuesCount().isUnknown()
                || rowCount.isUnknown()) {
            return columnStatistics.getNullsFraction().getValue();
        }
        // When NDV is greater than or equal to row count, there are no nulls
        if (columnStatistics.getDistinctValuesCount().getValue() >= rowCount.getValue()) {
            return 0;
        }

        double maxPossibleNulls = rowCount.getValue() - columnStatistics.getDistinctValuesCount().getValue();

        // If a connector provides NDV but is missing nulls fraction statistic for a column
        // (e.g. Delta Lake after "delta.dataSkippingNumIndexedCols" columns and MySql), populate a
        // 10% guess value so that the CBO can still produce some estimates rather failing to make
        // any estimates due to lack of nulls fraction.
        return Math.min(UNKNOWN_NULLS_FRACTION, maxPossibleNulls / rowCount.getValue());
    }
}
