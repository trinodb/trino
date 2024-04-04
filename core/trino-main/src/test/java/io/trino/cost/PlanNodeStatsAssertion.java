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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import org.assertj.core.api.Assertions;

import java.util.function.Consumer;

import static com.google.common.collect.Sets.union;
import static io.trino.cost.EstimateAssertion.assertEstimateEquals;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class PlanNodeStatsAssertion
{
    private final PlanNodeStatsEstimate actual;

    private PlanNodeStatsAssertion(PlanNodeStatsEstimate actual)
    {
        this.actual = actual;
    }

    public static PlanNodeStatsAssertion assertThat(PlanNodeStatsEstimate actual)
    {
        return new PlanNodeStatsAssertion(actual);
    }

    public PlanNodeStatsAssertion outputRowsCount(double expected)
    {
        assertEstimateEquals(actual.getOutputRowCount(), expected, "outputRowsCount mismatch");
        return this;
    }

    public PlanNodeStatsAssertion outputRowsCountUnknown()
    {
        Assertions.assertThat(Double.isNaN(actual.getOutputRowCount()))
                .describedAs("expected unknown outputRowsCount but got " + actual.getOutputRowCount())
                .isTrue();
        return this;
    }

    public PlanNodeStatsAssertion symbolStats(String symbolName, Consumer<SymbolStatsAssertion> symbolStatsAssertionConsumer)
    {
        return symbolStats(symbolName, DOUBLE, symbolStatsAssertionConsumer);
    }

    public PlanNodeStatsAssertion symbolStats(String symbolName, Type type, Consumer<SymbolStatsAssertion> symbolStatsAssertionConsumer)
    {
        return symbolStats(new Symbol(type, symbolName), symbolStatsAssertionConsumer);
    }

    public PlanNodeStatsAssertion symbolStats(Symbol symbol, Consumer<SymbolStatsAssertion> columnAssertionConsumer)
    {
        SymbolStatsAssertion columnAssertion = SymbolStatsAssertion.assertThat(actual.getSymbolStatistics(symbol));
        columnAssertionConsumer.accept(columnAssertion);
        return this;
    }

    public PlanNodeStatsAssertion symbolStatsUnknown(String symbolName, Type type)
    {
        return symbolStatsUnknown(new Symbol(type, symbolName));
    }

    public PlanNodeStatsAssertion symbolStatsUnknown(Symbol symbol)
    {
        return symbolStats(symbol,
                columnStats -> columnStats
                        .lowValueUnknown()
                        .highValueUnknown()
                        .nullsFractionUnknown()
                        .distinctValuesCountUnknown());
    }

    public PlanNodeStatsAssertion symbolsWithKnownStats(Symbol... symbols)
    {
        Assertions.assertThat(actual.getSymbolsWithKnownStatistics())
                .describedAs("symbols with known stats")
                .isEqualTo(ImmutableSet.copyOf(symbols));
        return this;
    }

    public PlanNodeStatsAssertion equalTo(PlanNodeStatsEstimate expected)
    {
        assertEstimateEquals(actual.getOutputRowCount(), expected.getOutputRowCount(), "outputRowCount mismatch");

        for (Symbol symbol : union(expected.getSymbolsWithKnownStatistics(), actual.getSymbolsWithKnownStatistics())) {
            assertSymbolStatsEqual(symbol, actual.getSymbolStatistics(symbol), expected.getSymbolStatistics(symbol));
        }
        return this;
    }

    private void assertSymbolStatsEqual(Symbol symbol, SymbolStatsEstimate actual, SymbolStatsEstimate expected)
    {
        assertEstimateEquals(actual.getNullsFraction(), expected.getNullsFraction(), "nullsFraction mismatch for %s", symbol.name());
        assertEstimateEquals(actual.getLowValue(), expected.getLowValue(), "lowValue mismatch for %s", symbol.name());
        assertEstimateEquals(actual.getHighValue(), expected.getHighValue(), "highValue mismatch for %s", symbol.name());
        assertEstimateEquals(actual.getDistinctValuesCount(), expected.getDistinctValuesCount(), "distinct values count mismatch for %s", symbol.name());
        assertEstimateEquals(actual.getAverageRowSize(), expected.getAverageRowSize(), "average row size mismatch for %s", symbol.name());
    }
}
