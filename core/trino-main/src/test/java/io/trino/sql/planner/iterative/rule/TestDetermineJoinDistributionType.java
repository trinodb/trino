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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.cost.CostComparator;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleAssert;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.JoinNode.Type;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.enforceSingleRow;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.iterative.rule.DetermineJoinDistributionType.getFirstKnownOutputSizeInBytes;
import static io.trino.sql.planner.iterative.rule.DetermineJoinDistributionType.getSourceTablesSizeInBytes;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.lang.Double.NaN;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestDetermineJoinDistributionType
{
    private static final CostComparator COST_COMPARATOR = new CostComparator(1, 1, 1);
    private static final int NODES_COUNT = 4;

    private RuleTester tester;

    @BeforeAll
    public void setUp()
    {
        tester = RuleTester.builder()
                .withNodeCountForStats(NODES_COUNT)
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testDetermineDistributionType()
    {
        testDetermineDistributionType(JoinDistributionType.PARTITIONED, INNER, DistributionType.PARTITIONED);
        testDetermineDistributionType(JoinDistributionType.BROADCAST, INNER, DistributionType.REPLICATED);
        testDetermineDistributionType(JoinDistributionType.AUTOMATIC, INNER, DistributionType.PARTITIONED);
    }

    @Test
    public void testDetermineDistributionTypeForLeftOuter()
    {
        testDetermineDistributionType(JoinDistributionType.PARTITIONED, LEFT, DistributionType.PARTITIONED);
        testDetermineDistributionType(JoinDistributionType.BROADCAST, LEFT, DistributionType.REPLICATED);
        testDetermineDistributionType(JoinDistributionType.AUTOMATIC, LEFT, DistributionType.PARTITIONED);
    }

    private void testDetermineDistributionType(JoinDistributionType sessionDistributedJoin, Type joinType, DistributionType expectedDistribution)
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, sessionDistributedJoin.name())
                .matches(
                        join(joinType, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(expectedDistribution)
                                .left(values(ImmutableMap.of("B1", 0)))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    @Test
    public void testRepartitionRightOuter()
    {
        testRepartitionRightOuter(JoinDistributionType.PARTITIONED, FULL);
        testRepartitionRightOuter(JoinDistributionType.PARTITIONED, RIGHT);
        testRepartitionRightOuter(JoinDistributionType.BROADCAST, FULL);
        testRepartitionRightOuter(JoinDistributionType.BROADCAST, RIGHT);
        testRepartitionRightOuter(JoinDistributionType.AUTOMATIC, FULL);
        testRepartitionRightOuter(JoinDistributionType.AUTOMATIC, RIGHT);
    }

    private void testRepartitionRightOuter(JoinDistributionType sessionDistributedJoin, Type joinType)
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, sessionDistributedJoin.name())
                .matches(
                        join(joinType, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicateScalar()
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.enforceSingleRow(
                                        p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11")))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(enforceSingleRow(values(ImmutableMap.of("B1", 0))))));
    }

    @Test
    public void testReplicateNoEquiCriteria()
    {
        testReplicateNoEquiCriteria(INNER);
        testReplicateNoEquiCriteria(LEFT);
    }

    private void testReplicateNoEquiCriteria(Type joinType)
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.of(expression("A1 * B1 > 100"))))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .matches(
                        join(joinType, builder -> builder
                                .filter("A1 * B1 > 100")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testRetainDistributionType()
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(DistributionType.REPLICATED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    @Test
    public void testFlipAndReplicateWhenOneTableMuchSmaller()
    {
        VarcharType symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 100;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
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
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("B1", 0)))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    @Test
    public void testFlipAndReplicateWhenOneTableMuchSmallerAndJoinCardinalityUnknown()
    {
        int aRows = 100;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), SymbolStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), SymbolStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("B1", 0)))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    @Test
    public void testPartitionWhenRequiredBySession()
    {
        VarcharType symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 100;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
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
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("B1", 0)))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    @Test
    public void testPartitionWhenBothTablesEqual()
    {
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        VarcharType symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testPartitionFullOuterJoin()
    {
        int aRows = 10_000;
        int bRows = 10;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        join(FULL, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testPartitionRightOuterJoin()
    {
        int aRows = 10_000;
        int bRows = 10;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        join(RIGHT, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicateLeftOuterJoin()
    {
        int aRows = 10_000;
        int bRows = 10;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        join(LEFT, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testFlipAndReplicateRightOuterJoin()
    {
        int aRows = 10;
        int bRows = 1_000_000;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        join(LEFT, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testFlipAndReplicateRightOuterJoinWhenJoinCardinalityUnknown()
    {
        int aRows = 10;
        int bRows = 1_000_000;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), SymbolStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), SymbolStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                ImmutableList.of(p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(
                        join(LEFT, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicatesWhenNotRestricted()
    {
        VarcharType symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
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
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));

        probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();

        // B table exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit therefore it is partitioned
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicatesWhenSourceIsSmall()
    {
        VarcharType symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 10;

        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate aStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        // output size does not exceed JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bSourceStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 64, 10)))
                .build();

        // immediate join sources exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit but build tables are small
        // therefore replicated distribution type is chosen
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("filterB", bStatsEstimate)
                .overrideStats("valuesB", bSourceStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), bRows, b1)),
                            ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(filter("true", values(ImmutableMap.of("B1", 0))))));

        // same but with join sides reversed
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("filterB", bStatsEstimate)
                .overrideStats("valuesB", bSourceStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), bRows, b1)),
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                            ImmutableList.of(b1),
                            ImmutableList.of(a1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(filter("true", values(ImmutableMap.of("B1", 0))))));

        // only probe side (with small tables) source stats are available, join sides should be flipped
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", PlanNodeStatsEstimate.unknown())
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown())
                .overrideStats("valuesB", bSourceStatsEstimate)
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            LEFT,
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), bRows, b1)),
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                            ImmutableList.of(b1),
                            ImmutableList.of(a1),
                            Optional.empty());
                })
                .matches(
                        join(RIGHT, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(filter("true", values(ImmutableMap.of("B1", 0))))));
    }

    @Test
    public void testFlipWhenSizeDifferenceLarge()
    {
        VarcharType symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 1_000;

        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate aStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();

        // source tables size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit but one side is significantly bigger than the other
        // therefore repartitioned distribution type is chosen with the smaller side on build
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), bRows, b1)),
                            ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(filter("true", values(ImmutableMap.of("B1", 0))))));

        // same but with join sides reversed
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), bRows, b1)),
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                            ImmutableList.of(b1),
                            ImmutableList.of(a1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(filter("true", values(ImmutableMap.of("B1", 0))))));

        // Use REPLICATED join type for cross join
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), bRows, b1)),
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            ImmutableList.of(),
                            ImmutableList.of(b1),
                            ImmutableList.of(a1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .distributionType(REPLICATED)
                                .left(filter("true", values(ImmutableMap.of("B1", 0))))
                                .right(values(ImmutableMap.of("A1", 0)))));

        // Don't flip sides when both are similar in size
        bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_LITERAL,
                                    p.values(new PlanNodeId("valuesB"), aRows, b1)),
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                            ImmutableList.of(b1),
                            ImmutableList.of(a1),
                            Optional.empty());
                })
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(PARTITIONED)
                                .left(filter("true", values(ImmutableMap.of("B1", 0))))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    @Test
    public void testGetSourceTablesSizeInBytes()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), tester.getMetadata(), tester.getSession());
        Symbol symbol = planBuilder.symbol("col");
        Symbol sourceSymbol1 = planBuilder.symbol("source1");
        Symbol sourceSymbol2 = planBuilder.symbol("soruce2");

        // missing source stats
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.values(symbol),
                        noLookup(),
                        node -> PlanNodeStatsEstimate.unknown(),
                        planBuilder.getTypes()),
                NaN);

        // two source plan nodes
        PlanNodeStatsEstimate sourceStatsEstimate1 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10)
                .build();
        PlanNodeStatsEstimate sourceStatsEstimate2 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(20)
                .build();
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<Symbol, Symbol>builder()
                                        .put(symbol, sourceSymbol1)
                                        .put(symbol, sourceSymbol2)
                                        .build(),
                                ImmutableList.of(
                                        planBuilder.tableScan(
                                                ImmutableList.of(sourceSymbol1),
                                                ImmutableMap.of(sourceSymbol1, new TestingColumnHandle("col"))),
                                        planBuilder.values(new PlanNodeId("valuesNode"), sourceSymbol2))),
                        noLookup(),
                        node -> {
                            if (node instanceof TableScanNode) {
                                return sourceStatsEstimate1;
                            }

                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        },
                        planBuilder.getTypes()),
                270.0);

        // join node
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.join(
                                INNER,
                                planBuilder.values(sourceSymbol1),
                                planBuilder.values(sourceSymbol2)),
                        noLookup(),
                        node -> sourceStatsEstimate1,
                        planBuilder.getTypes()),
                NaN);

        // unnest node
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(sourceSymbol1, ImmutableList.of(sourceSymbol1))),
                                planBuilder.values(sourceSymbol1)),
                        noLookup(),
                        node -> sourceStatsEstimate1,
                        planBuilder.getTypes()),
                NaN);
    }

    @Test
    public void testGetApproximateSourceSizeInBytes()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), tester.getMetadata(), tester.getSession());
        Symbol symbol = planBuilder.symbol("col");
        Symbol sourceSymbol1 = planBuilder.symbol("source1");
        Symbol sourceSymbol2 = planBuilder.symbol("source2");

        // missing source stats
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.values(symbol),
                        noLookup(),
                        node -> PlanNodeStatsEstimate.unknown(),
                        planBuilder.getTypes()),
                NaN);

        // two source plan nodes
        PlanNodeStatsEstimate sourceStatsEstimate1 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1000)
                .build();
        PlanNodeStatsEstimate sourceStatsEstimate2 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(2000)
                .build();
        PlanNodeStatsEstimate filterStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(250)
                .build();
        PlanNodeStatsEstimate limitStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(20)
                .build();
        double sourceRowCount = sourceStatsEstimate1.getOutputRowCount() + sourceStatsEstimate2.getOutputRowCount();
        double unionInputRowCount = filterStatsEstimate.getOutputRowCount() + limitStatsEstimate.getOutputRowCount();
        double sourceSizeInBytes = sourceRowCount + sourceRowCount * BIGINT.getFixedSize();
        // un-estimated union with non-expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<Symbol, Symbol>builder()
                                        .put(symbol, sourceSymbol1)
                                        .put(symbol, sourceSymbol2)
                                        .build(),
                                ImmutableList.of(
                                        planBuilder.filter(
                                                TRUE_LITERAL,
                                                planBuilder.tableScan(
                                                        ImmutableList.of(sourceSymbol1),
                                                        ImmutableMap.of(sourceSymbol1, new TestingColumnHandle("col")))),
                                        planBuilder.limit(20, planBuilder.values(sourceSymbol2)))),
                        noLookup(),
                        node -> {
                            if (node instanceof TableScanNode) {
                                return sourceStatsEstimate1;
                            }
                            if (node instanceof FilterNode) {
                                return filterStatsEstimate;
                            }
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }
                            if (node instanceof LimitNode) {
                                return limitStatsEstimate;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        },
                        planBuilder.getTypes()),
                (unionInputRowCount / sourceRowCount) * sourceSizeInBytes);

        // join node with known estimate
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.join(
                                INNER,
                                planBuilder.values(sourceSymbol1),
                                planBuilder.values(sourceSymbol2)),
                        noLookup(),
                        node -> sourceStatsEstimate1,
                        planBuilder.getTypes()),
                sourceStatsEstimate1.getOutputRowCount() * 2 * (BIGINT.getFixedSize() + 1));

        // un-estimated join with non-expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.join(
                                INNER,
                                planBuilder.tableScan(
                                        ImmutableList.of(sourceSymbol1),
                                        ImmutableMap.of(sourceSymbol1, new TestingColumnHandle("col"))),
                                planBuilder.values(sourceSymbol2)),
                        noLookup(),
                        node -> {
                            if (node instanceof TableScanNode) {
                                return sourceStatsEstimate1;
                            }
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        },
                        planBuilder.getTypes()),
                NaN);

        // un-estimated union with estimated expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<Symbol, Symbol>builder()
                                        .put(symbol, sourceSymbol1)
                                        .put(symbol, sourceSymbol2)
                                        .build(),
                                ImmutableList.of(
                                        planBuilder.unnest(
                                                ImmutableList.of(),
                                                ImmutableList.of(new UnnestNode.Mapping(sourceSymbol1, ImmutableList.of(sourceSymbol1))),
                                                planBuilder.values(sourceSymbol1)),
                                        planBuilder.values(sourceSymbol2))),
                        noLookup(),
                        node -> {
                            if (node instanceof UnnestNode) {
                                return sourceStatsEstimate1;
                            }
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        },
                        planBuilder.getTypes()),
                sourceSizeInBytes);

        // un-estimated union with un-estimated expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<Symbol, Symbol>builder()
                                        .put(symbol, sourceSymbol1)
                                        .put(symbol, sourceSymbol2)
                                        .build(),
                                ImmutableList.of(
                                        planBuilder.unnest(
                                                ImmutableList.of(),
                                                ImmutableList.of(new UnnestNode.Mapping(sourceSymbol1, ImmutableList.of(sourceSymbol1))),
                                                planBuilder.values(sourceSymbol1)),
                                        planBuilder.values(sourceSymbol2))),
                        noLookup(),
                        node -> {
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        },
                        planBuilder.getTypes()),
                NaN);
    }

    private RuleAssert assertDetermineJoinDistributionType()
    {
        return assertDetermineJoinDistributionType(COST_COMPARATOR);
    }

    private RuleAssert assertDetermineJoinDistributionType(CostComparator costComparator)
    {
        return tester.assertThat(new DetermineJoinDistributionType(costComparator, new TaskCountEstimator(() -> NODES_COUNT)));
    }
}
