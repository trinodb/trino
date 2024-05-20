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

import io.trino.cost.ComposableStatsCalculator.Rule;
import io.trino.cost.StatsCalculator.Context;
import io.trino.matching.Pattern;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.planner.plan.Patterns.values;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.stream.Collectors.toList;

public class ValuesStatsRule
        implements Rule<ValuesNode>
{
    private static final Pattern<ValuesNode> PATTERN = values();

    @Override
    public Pattern<ValuesNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(ValuesNode node, Context context)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        statsBuilder.setOutputRowCount(node.getRowCount());

        try {
            for (int symbolId = 0; symbolId < node.getOutputSymbols().size(); ++symbolId) {
                Symbol symbol = node.getOutputSymbols().get(symbolId);
                List<Object> symbolValues = getSymbolValues(
                        node,
                        symbolId,
                        RowType.anonymous(node.getOutputSymbols().stream()
                                .map(Symbol::type)
                                .collect(toImmutableList())));
                statsBuilder.addSymbolStatistics(symbol, buildSymbolStatistics(symbolValues, symbol.type()));
            }
        }
        catch (RuntimeException e) {
            // prevent stats calculations (e.g. division by zero) from causing planning failures
            return Optional.empty();
        }

        return Optional.of(statsBuilder.build());
    }

    private List<Object> getSymbolValues(ValuesNode valuesNode, int symbolId, Type rowType)
    {
        Type symbolType = rowType.getTypeParameters().get(symbolId);
        if (UNKNOWN.equals(symbolType)) {
            // special casing for UNKNOWN as evaluateConstantExpression does not handle that
            return IntStream.range(0, valuesNode.getRowCount())
                    .mapToObj(rowId -> null)
                    .collect(toList());
        }
        checkState(valuesNode.getRows().isPresent(), "rows is empty");
        return valuesNode.getRows().get().stream()
                .map(row -> switch (row) {
                    case Row value -> ((Constant) value.items().get(symbolId)).value();
                    case Constant(Type type, SqlRow value) -> readNativeValue(symbolType, value.getRawFieldBlock(symbolId), value.getRawIndex());
                    default -> throw new IllegalArgumentException("Expected Row or Constant: " + row);
                })
                .collect(toList());
    }

    private SymbolStatsEstimate buildSymbolStatistics(List<Object> values, Type type)
    {
        List<Object> nonNullValues = values.stream()
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (nonNullValues.isEmpty()) {
            return SymbolStatsEstimate.zero();
        }

        double[] valuesAsDoubles = nonNullValues.stream()
                .map(value -> toStatsRepresentation(type, value))
                .filter(OptionalDouble::isPresent)
                .mapToDouble(OptionalDouble::getAsDouble)
                .toArray();

        double lowValue = DoubleStream.of(valuesAsDoubles).min().orElse(Double.NEGATIVE_INFINITY);
        double highValue = DoubleStream.of(valuesAsDoubles).max().orElse(Double.POSITIVE_INFINITY);
        double valuesCount = values.size();
        double nonNullValuesCount = nonNullValues.size();
        long distinctValuesCount = nonNullValues.stream().distinct().count();

        return SymbolStatsEstimate.builder()
                .setNullsFraction((valuesCount - nonNullValuesCount) / valuesCount)
                .setLowValue(lowValue)
                .setHighValue(highValue)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }
}
