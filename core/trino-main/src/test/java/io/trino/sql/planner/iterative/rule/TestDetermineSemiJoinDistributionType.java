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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.cost.CostComparator;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.RuleAssert;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.trino.sql.planner.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

@Test(singleThreaded = true)
public class TestDetermineSemiJoinDistributionType
{
    private static final CostComparator COST_COMPARATOR = new CostComparator(1, 1, 1);
    private static final int NODES_COUNT = 4;

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = RuleTester.builder()
                .withNodeCountForStats(NODES_COUNT)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testRetainDistributionType()
    {
        assertDetermineSemiJoinDistributionType()
                .on(p ->
                        p.semiJoin(
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                p.symbol("A1"),
                                p.symbol("B1"),
                                p.symbol("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(REPLICATED)))
                .doesNotFire();
    }

    @Test
    public void testPartitionWhenRequiredBySession()
    {
        Type symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 100;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.semiJoin(
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            a1,
                            b1,
                            p.symbol("output"),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1B")
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), SymbolStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), SymbolStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                p.symbol("A1"),
                                p.symbol("B1"),
                                p.symbol("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testPartitionsWhenBothTablesEqual()
    {
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), SymbolStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), SymbolStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                p.symbol("A1"),
                                p.symbol("B1"),
                                p.symbol("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenFilterMuchSmaller()
    {
        int aRows = 10_000;
        int bRows = 100;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), SymbolStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), SymbolStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                p.symbol("A1"),
                                p.symbol("B1"),
                                p.symbol("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenNotRestricted()
    {
        Type symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 10;

        PlanNodeStatsEstimate probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 10)))
                .build();

        // B table is small enough to be replicated according to JOIN_MAX_BROADCAST_TABLE_SIZE limit
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.semiJoin(
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            a1,
                            b1,
                            p.symbol("output"),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));

        probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();

        // B table exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit therefore it is partitioned
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.semiJoin(
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            a1,
                            b1,
                            p.symbol("output"),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenSourceIsSmall()
    {
        Type symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 10;

        PlanNodeStatsEstimate probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideSourceStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 64, 10)))
                .build();

        // build side exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit but source plan nodes are small
        // therefore replicated distribution type is chosen
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("filterB", buildSideStatsEstimate)
                .overrideStats("valuesB", buildSideSourceStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.semiJoin(
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), bRows, b1)),
                            a1,
                            b1,
                            p.symbol("output"),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        filter("true", values(ImmutableMap.of("B1", 0)))));
    }

    private RuleAssert assertDetermineSemiJoinDistributionType()
    {
        return assertDetermineSemiJoinDistributionType(COST_COMPARATOR);
    }

    private RuleAssert assertDetermineSemiJoinDistributionType(CostComparator costComparator)
    {
        return tester.assertThat(new DetermineSemiJoinDistributionType(costComparator, new TaskCountEstimator(() -> NODES_COUNT)));
    }
}
