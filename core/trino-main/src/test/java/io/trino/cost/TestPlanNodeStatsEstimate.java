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

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPlanNodeStatsEstimate
{
    private static final Symbol A = new Symbol(BIGINT, "a");
    private static final Symbol B = new Symbol(VARCHAR, "b");
    private static final Symbol C = new Symbol(VARCHAR, "c");

    @Test
    public void testOutputSizeIsRememberedPerSymbolList()
    {
        List<Symbol> forward = ImmutableList.of(A, B, C);
        List<Symbol> backward = ImmutableList.of(C, B, A);
        List<Symbol> partial = ImmutableList.of(B);
        double forwardSize = estimate().getOutputSizeInBytes(forward);
        double backwardSize = estimate().getOutputSizeInBytes(backward);
        double partialSize = estimate().getOutputSizeInBytes(partial);
        assertThat(partialSize).isLessThan(forwardSize);

        // one estimate asked by turns gives what a fresh estimate gives for each list
        PlanNodeStatsEstimate estimate = estimate();
        for (int i = 0; i < 3; i++) {
            assertThat(estimate.getOutputSizeInBytes(forward)).isEqualTo(forwardSize);
            assertThat(estimate.getOutputSizeInBytes(backward)).isEqualTo(backwardSize);
            assertThat(estimate.getOutputSizeInBytes(partial)).isEqualTo(partialSize);
        }
    }

    @Test
    public void testOutputSizeFollowsMutableCollection()
    {
        PlanNodeStatsEstimate estimate = estimate();
        List<Symbol> symbols = new ArrayList<>(ImmutableList.of(A));
        double single = estimate.getOutputSizeInBytes(symbols);
        symbols.add(B);
        assertThat(estimate.getOutputSizeInBytes(symbols))
                .isGreaterThan(single)
                .isEqualTo(estimate().getOutputSizeInBytes(ImmutableList.of(A, B)));
    }

    @Test
    public void testBuilderDoesNotChangeBuiltEstimate()
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10)
                .addSymbolStatistics(A, symbolStats(1));
        PlanNodeStatsEstimate first = builder.build();
        PlanNodeStatsEstimate second = builder
                .addSymbolStatistics(B, symbolStats(2))
                .removeSymbolStatistics(A)
                .build();

        assertThat(first.getSymbolsWithKnownStatistics()).containsExactly(A);
        assertThat(second.getSymbolsWithKnownStatistics()).containsExactly(B);
        assertThat(PlanNodeStatsEstimate.buildFrom(first).build().getSymbolStatistics()).isSameAs(first.getSymbolStatistics());
        assertThatThrownBy(() -> first.getSymbolStatistics().put(B, symbolStats(2)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> builder.addSymbolStatistics(C, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("statistics is null");
    }

    private static PlanNodeStatsEstimate estimate()
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1000)
                .addSymbolStatistics(A, symbolStats(100))
                .addSymbolStatistics(B, symbolStats(0.1))
                .addSymbolStatistics(C, symbolStats(1e9 + 0.3))
                .build();
    }

    private static SymbolStatsEstimate symbolStats(double averageRowSize)
    {
        return SymbolStatsEstimate.builder()
                .setNullsFraction(0.1)
                .setAverageRowSize(averageRowSize)
                .setDistinctValuesCount(10)
                .build();
    }
}
