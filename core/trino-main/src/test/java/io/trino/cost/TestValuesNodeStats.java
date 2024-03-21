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
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Arithmetic;
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import static io.trino.cost.PlanNodeStatsEstimate.unknown;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.Arithmetic.Operator.ADD;
import static io.trino.sql.ir.Arithmetic.Operator.DIVIDE;
import static io.trino.type.UnknownType.UNKNOWN;

public class TestValuesNodeStats
        extends BaseStatsCalculatorTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction DIVIDE_INTEGER = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testStatsForValuesNode()
    {
        tester().assertStatsFor(pb -> pb
                        .values(ImmutableList.of(pb.symbol("a", BIGINT), pb.symbol("b", DOUBLE)),
                                ImmutableList.of(
                                        ImmutableList.of(new Arithmetic(ADD_BIGINT, ADD, new Constant(BIGINT, 3L), new Constant(BIGINT, 3L)), new Constant(DOUBLE, 13.5e0)),
                                        ImmutableList.of(new Constant(BIGINT, 55L), new Constant(UNKNOWN, null)),
                                        ImmutableList.of(new Constant(BIGINT, 6L), new Constant(DOUBLE, 13.5e0)))))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(3)
                                .addSymbolStatistics(
                                        new Symbol(UNKNOWN, "a"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0)
                                                .setLowValue(6)
                                                .setHighValue(55)
                                                .setDistinctValuesCount(2)
                                                .build())
                                .addSymbolStatistics(
                                        new Symbol(UNKNOWN, "b"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0.33333333333333333)
                                                .setLowValue(13.5)
                                                .setHighValue(13.5)
                                                .setDistinctValuesCount(1)
                                                .build())
                                .build()));

        tester().assertStatsFor(pb -> pb
                        .values(ImmutableList.of(pb.symbol("v", createVarcharType(30))),
                                ImmutableList.of(
                                        ImmutableList.of(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("Alice"))),
                                        ImmutableList.of(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("'has'"))),
                                        ImmutableList.of(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("'a cat'"))),
                                        ImmutableList.of(new Constant(UNKNOWN, null)))))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(4)
                                .addSymbolStatistics(
                                        new Symbol(UNKNOWN, "v"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0.25)
                                                .setDistinctValuesCount(3)
                                                // TODO .setAverageRowSize(4 + 1. / 3)
                                                .build())
                                .build()));
    }

    @Test
    public void testDivisionByZero()
    {
        tester().assertStatsFor(pb -> pb
                        .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                                ImmutableList.of(ImmutableList.of(new Arithmetic(DIVIDE_INTEGER, DIVIDE, new Constant(INTEGER, 1L), new Constant(INTEGER, 0L))))))
                .check(outputStats -> outputStats.equalTo(unknown()));
    }

    @Test
    public void testStatsForValuesNodeWithJustNulls()
    {
        PlanNodeStatsEstimate nullAStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1)
                .addSymbolStatistics(new Symbol(UNKNOWN, "a"), SymbolStatsEstimate.zero())
                .build();

        tester().assertStatsFor(pb -> pb
                        .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                                ImmutableList.of(
                                        ImmutableList.of(new Arithmetic(ADD_INTEGER, ADD, new Constant(INTEGER, 3L), new Constant(UNKNOWN, null))))))
                .check(outputStats -> outputStats.equalTo(nullAStats));

        tester().assertStatsFor(pb -> pb
                        .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                                ImmutableList.of(
                                        ImmutableList.of(new Constant(UNKNOWN, null)))))
                .check(outputStats -> outputStats.equalTo(nullAStats));

        tester().assertStatsFor(pb -> pb
                        .values(ImmutableList.of(pb.symbol("a", UNKNOWN)),
                                ImmutableList.of(
                                        ImmutableList.of(new Constant(UNKNOWN, null)))))
                .check(outputStats -> outputStats.equalTo(nullAStats));
    }

    @Test
    public void testStatsForEmptyValues()
    {
        tester().assertStatsFor(pb -> pb
                        .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                                ImmutableList.of()))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(0)
                                .addSymbolStatistics(new Symbol(UNKNOWN, "a"), SymbolStatsEstimate.zero())
                                .build()));
    }
}
