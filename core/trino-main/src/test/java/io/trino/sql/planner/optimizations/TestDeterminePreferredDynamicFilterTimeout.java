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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.ir.BetweenPredicate;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.ENABLE_STATS_CALCULATOR;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.SMALL_DYNAMIC_FILTER_MAX_NDV_COUNT;
import static io.trino.SystemSessionProperties.SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT;
import static io.trino.SystemSessionProperties.getSmallDynamicFilterWaitTimeout;
import static io.trino.spi.statistics.TableStatistics.empty;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDeterminePreferredDynamicFilterTimeout
        extends BasePlanTest
{
    private long waitForCascadingDynamicFiltersTimeout;

    @Override
    protected PlanTester createPlanTester()
    {
        String catalogName = "mock";
        Map<String, String> sessionProperties = ImmutableMap.of(
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, NONE.name(),
                JOIN_DISTRIBUTION_TYPE, BROADCAST.name());
        Map<String, TableStatistics> tables = Map.of(
                "table_undefined_a", new TableStatistics(
                        Estimate.unknown(),
                        Map.of(
                                new MockConnectorColumnHandle("a_1", INTEGER), ColumnStatistics.empty(),
                                new MockConnectorColumnHandle("a_2", INTEGER), ColumnStatistics.empty())),
                "table_undefined_b", new TableStatistics(
                        Estimate.unknown(),
                        Map.of(
                                new MockConnectorColumnHandle("b_1", INTEGER), ColumnStatistics.empty(),
                                new MockConnectorColumnHandle("b_2", INTEGER), ColumnStatistics.empty())),
                "table_small_a", new TableStatistics(
                        Estimate.of(10_000),
                        Map.of(
                                new MockConnectorColumnHandle("a_1", INTEGER), ColumnStatistics.empty(),
                                new MockConnectorColumnHandle("a_2", INTEGER), new ColumnStatistics(
                                        Estimate.unknown(), Estimate.of(400), Estimate.unknown(), Optional.empty()))),
                "table_small_b", new TableStatistics(
                        Estimate.of(10_000),
                        Map.of(
                                new MockConnectorColumnHandle("b_1", INTEGER), ColumnStatistics.empty(),
                                new MockConnectorColumnHandle("b_2", INTEGER), new ColumnStatistics(
                                        Estimate.unknown(), Estimate.of(400), Estimate.unknown(), Optional.empty()),
                                new MockConnectorColumnHandle("b_3", INTEGER), ColumnStatistics.empty())),
                "table_small_c", new TableStatistics(
                        Estimate.of(10_000),
                        Map.of(
                                new MockConnectorColumnHandle("c_1", INTEGER), ColumnStatistics.empty(),
                                new MockConnectorColumnHandle("c_2", INTEGER), new ColumnStatistics(
                                        Estimate.unknown(), Estimate.of(400), Estimate.unknown(), Optional.empty()))));
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withGetTableHandle((session, table) -> {
                    if (tables.containsKey(table.getTableName())) {
                        return new MockConnectorTableHandle(table);
                    }
                    return null;
                })
                .withGetColumns(schemaTableName -> {
                    TableStatistics table = tables.get(schemaTableName.getTableName());
                    if (table != null) {
                        return table.getColumnStatistics().keySet().stream()
                                .map(MockConnectorColumnHandle.class::cast)
                                .map(columnHandle -> new ColumnMetadata(columnHandle.getName(), columnHandle.getType()))
                                .toList();
                    }
                    return Collections.emptyList();
                })
                .withGetTableStatistics(tableName -> {
                    TableStatistics table = tables.get(tableName.getTableName());
                    if (table != null) {
                        return table;
                    }
                    else {
                        return empty();
                    }
                })
                .withName(catalogName)
                .build();

        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("default");
        sessionProperties.forEach(sessionBuilder::setSystemProperty);

        PlanTester planTester = PlanTester.create(sessionBuilder.build());
        planTester.createCatalog(
                catalogName,
                connectorFactory,
                ImmutableMap.of());
        return planTester;
    }

    @BeforeAll
    public void setup()
    {
        waitForCascadingDynamicFiltersTimeout = getSmallDynamicFilterWaitTimeout(getPlanTester().getDefaultSession()).toMillis();
    }

    @Test
    public void testWithUnknownTableSize()
    {
        assertPlan(
                "SELECT table_undefined_a.a_1 from table_undefined_a, table_undefined_b where table_undefined_a.a_1 = table_undefined_b.b_1",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_1", "B_1")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), EQUAL, "B_1", false, OptionalLong.empty())))
                                .left(
                                        node(FilterNode.class,
                                                tableScan("table_undefined_a", ImmutableMap.of("A_1", "a_1"))))
                                .right(
                                        exchange(
                                                tableScan("table_undefined_b", ImmutableMap.of("B_1", "b_1")))))));
    }

    @Test
    public void testWithSmallRowCountTable()
    {
        assertPlan(
                "SELECT table_small_a.a_1 from table_small_a, table_small_b where table_small_a.a_1 = table_small_b.b_1",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_1", "B_1")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), EQUAL, "B_1", false, OptionalLong.of(waitForCascadingDynamicFiltersTimeout))))
                                .left(
                                        node(FilterNode.class,
                                                tableScan("table_small_a", ImmutableMap.of("A_1", "a_1"))))
                                .right(
                                        exchange(
                                                tableScan("table_small_b", ImmutableMap.of("B_1", "b_1")))))));
    }

    @Test
    public void testWithExceedingRowCount()
    {
        assertPlan(
                "SELECT table_small_a.a_1 from table_small_a, table_small_b where table_small_a.a_1 = table_small_b.b_1",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT, "9999")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_1", "B_1")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), EQUAL, "B_1", false, OptionalLong.of(0L))))
                                .left(
                                        node(FilterNode.class,
                                                tableScan("table_small_a", ImmutableMap.of("A_1", "a_1"))))
                                .right(
                                        exchange(
                                                tableScan("table_small_b", ImmutableMap.of("B_1", "b_1")))))));
    }

    @Test
    public void testWithSmallNdvCount()
    {
        assertPlan(
                "SELECT table_small_a.a_1 from table_small_a, table_small_b where table_small_a.a_1 = table_small_b.b_2",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT, "1")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_1", "B_2")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), EQUAL, "B_2", false, OptionalLong.of(waitForCascadingDynamicFiltersTimeout))))
                                .left(
                                        node(FilterNode.class,
                                                tableScan("table_small_a", ImmutableMap.of("A_1", "a_1"))))
                                .right(
                                        exchange(
                                                tableScan("table_small_b", ImmutableMap.of("B_2", "b_2")))))));
    }

    @Test
    public void testWithExceedNdvCount()
    {
        assertPlan(
                "SELECT table_small_a.a_1 from table_small_a, table_small_b where table_small_a.a_1 = table_small_b.b_2",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_ROW_COUNT, "9999")
                        .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_NDV_COUNT, "399")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_1", "B_2")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), EQUAL, "B_2", false, OptionalLong.of(0L))))
                                .left(
                                        node(FilterNode.class,
                                                tableScan("table_small_a", ImmutableMap.of("A_1", "a_1"))))
                                .right(
                                        exchange(
                                                tableScan("table_small_b", ImmutableMap.of("B_2", "b_2")))))));
        assertPlan(
                "SELECT table_small_a.a_1 from table_small_a, table_small_b where table_small_a.a_2 = table_small_b.b_2",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(SMALL_DYNAMIC_FILTER_MAX_NDV_COUNT, "399")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_2", "B_2")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_2"), EQUAL, "B_2", false, OptionalLong.of(waitForCascadingDynamicFiltersTimeout))))
                                .left(
                                        node(FilterNode.class,
                                                tableScan("table_small_a", ImmutableMap.of("A_2", "a_2"))))
                                .right(
                                        exchange(
                                                tableScan("table_small_b", ImmutableMap.of("B_2", "b_2")))))));
    }

    @Test
    public void testWithDisabledStatistics()
    {
        assertPlan(
                "SELECT table_small_a.a_1 from table_small_a, table_small_b where table_small_a.a_1 = table_small_b.b_1",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(ENABLE_STATS_CALCULATOR, "false")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_1", "B_1")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), EQUAL, "B_1", false, OptionalLong.empty())))
                                .left(
                                        node(FilterNode.class,
                                                tableScan("table_small_a", ImmutableMap.of("A_1", "a_1"))))
                                .right(
                                        exchange(
                                                tableScan("table_small_b", ImmutableMap.of("B_1", "b_1")))))));
    }

    @Test
    public void testDependantDynamicFilterTable()
    {
        assertPlan(
                "SELECT table_small_c.c_1 from (table_small_a JOIN table_small_b ON table_small_a.a_2 = table_small_b.b_2) " +
                        "JOIN table_small_c ON table_small_a.a_2 = table_small_c.c_1",
                Session.builder(getPlanTester().getDefaultSession())
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_2", "C_1")
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_2"), EQUAL, "C_1", false, OptionalLong.of(waitForCascadingDynamicFiltersTimeout)),
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("B_2"), EQUAL, "C_1", false, OptionalLong.of(waitForCascadingDynamicFiltersTimeout))))
                                .left(
                                        join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria("A_2", "B_2")
                                                .dynamicFilter("A_2", "B_2", waitForCascadingDynamicFiltersTimeout)
                                                .left(
                                                        node(FilterNode.class,
                                                                tableScan("table_small_a", ImmutableMap.of("A_2", "a_2"))))
                                                .right(
                                                        anyTree(node(FilterNode.class,
                                                                tableScan("table_small_b", ImmutableMap.of("B_2", "b_2")))))))
                                .right(
                                        exchange(
                                                tableScan("table_small_c", ImmutableMap.of("C_1", "c_1")))))));
    }

    @Test
    public void testWithExpandingNodeAndSmallRowCount()
    {
        assertPlan(
                "SELECT a.a_1 FROM table_small_a a JOIN (SELECT b_1 FROM table_small_b UNION ALL SELECT b_1 FROM table_small_b) b ON a.a_1 = b.b_1",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("A_1", "B_1")
                                .dynamicFilter("A_1", "B_1", waitForCascadingDynamicFiltersTimeout)
                                .left(
                                        anyTree(
                                                tableScan("table_small_a", ImmutableMap.of("A_1", "a_1"))))
                                .right(
                                        exchange(
                                                LOCAL,
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                Set.of(),
                                                Optional.empty(),
                                                ImmutableList.of("B_1"),
                                                Optional.empty(),
                                                tableScan("table_small_b", ImmutableMap.of("B_1_1", "b_1")),
                                                tableScan("table_small_b", ImmutableMap.of("B_1_2", "b_1")))))));
    }

    @Test
    public void testWithMultiplyingNode()
    {
        assertPlan("""
                        SELECT a.a_1, a.a_2 FROM table_small_a a,table_small_b b
                        JOIN table_small_c c ON b.b_2 = c.c_1 AND c.c_2 = b.b_3
                        WHERE a.a_1 BETWEEN b.b_1 AND b.b_2
                        """,
                anyTree(filter(
                        new BetweenPredicate(new SymbolReference("A_1"), new SymbolReference("B_1"), new SymbolReference("B_2")),
                        join(INNER, builder -> builder
                                .dynamicFilter(
                                        ImmutableList.of(
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), GREATER_THAN_OR_EQUAL, "B_1", false, OptionalLong.of(0L)),
                                                new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("A_1"), LESS_THAN_OR_EQUAL, "B_2", false, OptionalLong.of(0L))))
                                .left(
                                        filter(
                                                TRUE_LITERAL,
                                                tableScan("table_small_a", ImmutableMap.of("A_1", "a_1", "A_2", "a_2"))))
                                .right(
                                        exchange(
                                                LOCAL,
                                                join(INNER, innerJoinBuilder -> innerJoinBuilder
                                                        .equiCriteria(ImmutableList.of(
                                                                equiJoinClause("B_2", "C_1"),
                                                                equiJoinClause("B_3", "C_2")))
                                                        .dynamicFilter(
                                                                ImmutableList.of(
                                                                        new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("B_2"), EQUAL, "C_1", false, OptionalLong.of(waitForCascadingDynamicFiltersTimeout)),
                                                                        new PlanMatchPattern.DynamicFilterPattern(new SymbolReference("B_3"), EQUAL, "C_2", false, OptionalLong.of(waitForCascadingDynamicFiltersTimeout))))
                                                        .left(
                                                                anyTree(
                                                                        tableScan("table_small_b", ImmutableMap.of("B_1", "b_1", "B_2", "b_2", "B_3", "b_3"))))
                                                        .right(
                                                                exchange(
                                                                        LOCAL,
                                                                        tableScan("table_small_c", ImmutableMap.of("C_2", "c_2", "C_1", "c_1")))))))))));
    }

    @Test
    public void testWithSemiJoinAndSmallRowCount()
    {
        assertPlan(
                "SELECT table_small_a.a_1 from table_small_a where table_small_a.a_1 IN (SELECT b_1 from table_small_b where b_1 = random(5))",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                        .build(),
                anyTree(
                        semiJoin("A_1", "B_1", "SEMI_JOIN_RESULT", true,
                                filter(TRUE_LITERAL,
                                        tableScan("table_small_a", ImmutableMap.of("A_1", "a_1")))
                                        .with(FilterNode.class, filterNode -> extractDynamicFilters(filterNode.getPredicate())
                                                .getDynamicConjuncts().get(0).getPreferredTimeout()
                                                .equals(OptionalLong.of(waitForCascadingDynamicFiltersTimeout))),
                                node(ExchangeNode.class,
                                        filter(
                                                new ComparisonExpression(EQUAL, new SymbolReference("B_1"), new FunctionCall(QualifiedName.of("random"), ImmutableList.of(new Constant(INTEGER, 5L)))),
                                                tableScan("table_small_b", ImmutableMap.of("B_1", "b_1")))))));
    }
}
