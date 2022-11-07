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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.RuleAssert;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.AUTOMATIC;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;

public class TestReorderJoins
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = RuleTester.builder()
                .addSessionProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .addSessionProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name())
                .withNodeCountForStats(4)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testKeepsOutputSymbols()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1"), p.symbol("A2")),
                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                ImmutableList.of(p.symbol("A2")),
                                ImmutableList.of(),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5000)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 100),
                                new Symbol("A2"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0, "A2", 1)))
                                .right(values(ImmutableMap.of("B1", 0))))
                                .withExactOutputs("A2"));
    }

    @Test
    public void testReplicatesAndFlipsWhenOneTableMuchSmaller()
    {
        Type symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        assertReorderJoins()
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1PB")
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), 2, a1),
                            p.values(new PlanNodeId("valuesB"), 2, b1),
                            ImmutableList.of(new EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("B1", 0)))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    @Test
    public void testRepartitionsWhenRequiredBySession()
    {
        Type symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        assertReorderJoins()
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), 2, a1),
                            p.values(new PlanNodeId("valuesB"), 2, b1),
                            ImmutableList.of(new EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("B1", 0)))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    @Test
    public void testRepartitionsWhenBothTablesEqual()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(p.symbol("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicatesUnrestrictedWhenRequiredBySession()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(p.symbol("B1")),
                                Optional.empty()))
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1kB")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicatedScalarJoinEvenWhereSessionRequiresRepartitioned()
    {
        PlanMatchPattern expectedPlan = join(INNER, builder -> builder
                .equiCriteria("A1", "B1")
                .distributionType(REPLICATED)
                .left(values(ImmutableMap.of("A1", 0)))
                .right(values(ImmutableMap.of("B1", 0))));

        PlanNodeStatsEstimate valuesA = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10000)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                .build();
        PlanNodeStatsEstimate valuesB = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10000)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                .build();

        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1")), // matches isAtMostScalar
                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(p.symbol("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", valuesA)
                .overrideStats("valuesB", valuesB)
                .matches(expectedPlan);

        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1")), // matches isAtMostScalar
                                ImmutableList.of(new EquiJoinClause(p.symbol("B1"), p.symbol("A1"))),
                                ImmutableList.of(p.symbol("B1")),
                                ImmutableList.of(p.symbol("A1")),
                                Optional.empty()))
                .overrideStats("valuesA", valuesA)
                .overrideStats("valuesB", valuesB)
                .matches(expectedPlan);
    }

    @Test
    public void testDoesNotFireForCrossJoin()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(p.symbol("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoStats()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.unknown())
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonDeterministicFilter()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1")),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(p.symbol("B1")),
                                Optional.of(new ComparisonExpression(
                                        LESS_THAN,
                                        p.symbol("A1").toSymbolReference(),
                                        new TestingFunctionResolution().functionCallBuilder(QualifiedName.of("random")).build()))))
                .doesNotFire();
    }

    @Test
    public void testPredicatesPushedDown()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                        p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1"), p.symbol("B2")),
                                        ImmutableList.of(),
                                        ImmutableList.of(p.symbol("A1")),
                                        ImmutableList.of(p.symbol("B1"), p.symbol("B2")),
                                        Optional.empty()),
                                p.values(new PlanNodeId("valuesC"), 2, p.symbol("C1")),
                                ImmutableList.of(
                                        new EquiJoinClause(p.symbol("B2"), p.symbol("C1"))),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(),
                                Optional.of(new ComparisonExpression(EQUAL, p.symbol("A1").toSymbolReference(), p.symbol("B1").toSymbolReference()))))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 5),
                                new Symbol("B2"), new SymbolStatsEstimate(0, 100, 0, 100, 5)))
                        .build())
                .overrideStats("valuesC", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("C1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("C1", "B2")
                                .left(values("C1"))
                                .right(
                                        join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                .equiCriteria("A1", "B1")
                                                .left(values("A1"))
                                                .right(values("B1", "B2"))))));
    }

    @Test
    public void testPushesProjectionsThroughJoin()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.project(
                                        Assignments.of(
                                                p.symbol("P1"), new ArithmeticUnaryExpression(MINUS, p.symbol("B1").toSymbolReference()),
                                                p.symbol("P2"), p.symbol("A1").toSymbolReference()),
                                        p.join(
                                                INNER,
                                                p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                                ImmutableList.of(),
                                                ImmutableList.of(p.symbol("A1")),
                                                ImmutableList.of(p.symbol("B1")),
                                                Optional.empty())),
                                p.values(new PlanNodeId("valuesC"), 2, p.symbol("C1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("P1"), p.symbol("C1"))),
                                ImmutableList.of(p.symbol("P1")),
                                ImmutableList.of(),
                                Optional.of(new ComparisonExpression(EQUAL, p.symbol("P2").toSymbolReference(), p.symbol("C1").toSymbolReference()))))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 5)))
                        .build())
                .overrideStats("valuesC", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("C1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("C1", "P1")
                                .left(values("C1"))
                                .right(
                                        join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                .equiCriteria("P2", "P1")
                                                .left(
                                                        strictProject(
                                                                ImmutableMap.of("P2", expression("A1")),
                                                                values("A1")))
                                                .right(
                                                        strictProject(
                                                                ImmutableMap.of("P1", expression("-(B1)")),
                                                                values("B1")))))));
    }

    @Test
    public void testDoesNotPushProjectionThroughJoinIfTooExpensive()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.project(
                                        Assignments.of(
                                                p.symbol("P1"), new ArithmeticUnaryExpression(MINUS, p.symbol("B1").toSymbolReference())),
                                        p.join(
                                                INNER,
                                                p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                                p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1")),
                                                ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                                ImmutableList.of(p.symbol("A1")),
                                                ImmutableList.of(p.symbol("B1")),
                                                Optional.empty())),
                                p.values(new PlanNodeId("valuesC"), 2, p.symbol("C1")),
                                ImmutableList.of(new EquiJoinClause(p.symbol("P1"), p.symbol("C1"))),
                                ImmutableList.of(p.symbol("P1")),
                                ImmutableList.of(),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 5)))
                        .build())
                .overrideStats("valuesC", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("C1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("C1", "P1")
                                .left(values("C1"))
                                .right(
                                        strictProject(
                                                ImmutableMap.of("P1", expression("-(B1)")),
                                                join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                        .equiCriteria("A1", "B1")
                                                        .left(values("A1"))
                                                        .right(values("B1")))))));
    }

    @Test
    public void testSmallerJoinFirst()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        p.values(new PlanNodeId("valuesA"), 2, p.symbol("A1")),
                                        p.values(new PlanNodeId("valuesB"), 2, p.symbol("B1"), p.symbol("B2")),
                                        ImmutableList.of(new EquiJoinClause(p.symbol("A1"), p.symbol("B1"))),
                                        ImmutableList.of(p.symbol("A1")),
                                        ImmutableList.of(p.symbol("B1"), p.symbol("B2")),
                                        Optional.empty()),
                                p.values(new PlanNodeId("valuesC"), 2, p.symbol("C1")),
                                ImmutableList.of(
                                        new EquiJoinClause(p.symbol("B2"), p.symbol("C1"))),
                                ImmutableList.of(p.symbol("A1")),
                                ImmutableList.of(),
                                Optional.of(new ComparisonExpression(EQUAL, p.symbol("A1").toSymbolReference(), p.symbol("B1").toSymbolReference()))))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(40)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(
                                new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 10),
                                new Symbol("B2"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesC", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("C1"), new SymbolStatsEstimate(99, 199, 0, 100, 100)))
                        .build())
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .left(values("A1"))
                                .right(
                                        join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                .equiCriteria("C1", "B2")
                                                .left(values("C1"))
                                                .right(values("B1", "B2"))))));
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
        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
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
        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("A1", "B1")
                                .distributionType(PARTITIONED)
                                .left(values(ImmutableMap.of("A1", 0)))
                                .right(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReorderAndReplicate()
    {
        Type symbolType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10;
        int bRows = 10_000;

        PlanNodeStatsEstimate probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 10)))
                .build();

        // A table is small enough to be replicated in JOIN_MAX_BROADCAST_TABLE_SIZE mode
        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "10MB")
                .on(p -> {
                    Symbol a1 = p.symbol("A1", symbolType);
                    Symbol b1 = p.symbol("B1", symbolType);
                    return p.join(
                            INNER,
                            p.values(new PlanNodeId("valuesA"), aRows, a1),
                            p.values(new PlanNodeId("valuesB"), bRows, b1),
                            ImmutableList.of(new EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1),
                            ImmutableList.of(b1),
                            Optional.empty());
                })
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria("B1", "A1")
                                .distributionType(REPLICATED)
                                .left(values(ImmutableMap.of("B1", 0)))
                                .right(values(ImmutableMap.of("A1", 0)))));
    }

    private RuleAssert assertReorderJoins()
    {
        return tester.assertThat(new ReorderJoins(PLANNER_CONTEXT, new CostComparator(1, 1, 1), createTestingTypeAnalyzer(PLANNER_CONTEXT)));
    }
}
