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

import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.SampleNode;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestSampleStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsForSampleNode()
    {
        tester()
                .assertStatsFor(pb -> {
                    Symbol a = pb.symbol("a", BIGINT);
                    Symbol b = pb.symbol("b", DOUBLE);
                    return pb.sample(.33, SampleNode.Type.BERNOULLI, pb.values(a, b));
                })
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(
                                new Symbol("a"),
                                SymbolStatsEstimate.builder()
                                        .setDistinctValuesCount(20)
                                        .setNullsFraction(0.3)
                                        .setLowValue(1)
                                        .setHighValue(30)
                                        .build())
                        .addSymbolStatistics(
                                new Symbol("b"),
                                SymbolStatsEstimate.builder()
                                        .setDistinctValuesCount(40)
                                        .setNullsFraction(0.6)
                                        .setLowValue(13.5)
                                        .setHighValue(POSITIVE_INFINITY)
                                        .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(33)
                        .symbolStats("a", assertion -> assertion
                                .dataSizeUnknown()
                                .distinctValuesCount(20)
                                .nullsFraction(0.3)
                                .lowValue(1)
                                .highValue(30))
                        .symbolStats("b", assertion -> assertion
                                .dataSizeUnknown()
                                .distinctValuesCount(23.1)
                                .nullsFraction(0.3)
                                .lowValue(13.5)
                                .highValueUnknown()));
    }
}
