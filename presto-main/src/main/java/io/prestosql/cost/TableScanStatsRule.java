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
package io.prestosql.cost;

import com.google.common.collect.ImmutableBiMap;
import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.DomainTranslator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.lang.Double.NaN;
import static java.util.Objects.requireNonNull;

public class TableScanStatsRule
        extends SimpleStatsRule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan();

    private final Metadata metadata;
    private final FilterStatsCalculator filterStatsCalculator;
    private final DomainTranslator domainTranslator;

    public TableScanStatsRule(Metadata metadata, StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer); // Use stats normalization since connector can return inconsistent stats values
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
        this.domainTranslator = new DomainTranslator(metadata);
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TableScanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        // TODO Construct predicate like AddExchanges's LayoutConstraintEvaluator
        Constraint constraint = new Constraint(metadata.getTableProperties(session, node.getTable()).getPredicate());

        TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
        verifyNotNull(tableStatistics, "tableStatistics is null for %s", node);
        Map<Symbol, SymbolStatsEstimate> outputSymbolStats = new HashMap<>();

        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            Optional<ColumnStatistics> columnStatistics = Optional.ofNullable(tableStatistics.getColumnStatistics().get(entry.getValue()));
            SymbolStatsEstimate symbolStatistics = columnStatistics
                    .map(statistics -> toSymbolStatistics(tableStatistics, statistics, types.get(symbol)))
                    .orElse(SymbolStatsEstimate.unknown());
            outputSymbolStats.put(symbol, symbolStatistics);
        }
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                                                              .setOutputRowCount(tableStatistics.getRowCount().getValue())
                                                              .addSymbolStatistics(outputSymbolStats)
                                                              .build();
        // Allow the connector delegate filtering statistics estimation of the pushed-down predicate back to the engine:
        TupleDomain<ColumnHandle> unestimatedPredicate = tableStatistics.getUnestimatedPredicate();
        if (!unestimatedPredicate.isAll()) {
            // If the connector don't support statistics estimation for pushed-down predicate, we use FilterStatsCalculator
            // to heuristically update the PlanNodeStatsEstimate (similarly to how it's done by FilterStatsRule).
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
            Expression predicate = domainTranslator.toPredicate(unestimatedPredicate.simplify().transform(assignments::get));
            estimate = filterStatsCalculator.filterStats(estimate, predicate, session, types);
        }
        return Optional.of(estimate);
    }

    private static SymbolStatsEstimate toSymbolStatistics(TableStatistics tableStatistics, ColumnStatistics columnStatistics, Type type)
    {
        requireNonNull(tableStatistics, "tableStatistics is null");
        requireNonNull(columnStatistics, "columnStatistics is null");
        requireNonNull(type, "type is null");

        double nullsFraction = columnStatistics.getNullsFraction().getValue();
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
}
