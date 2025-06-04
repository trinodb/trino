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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.COLOCATED_JOIN;
import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.ENABLE_STATS_CALCULATOR;
import static io.trino.SystemSessionProperties.IGNORE_DOWNSTREAM_PREFERENCES;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT;
import static io.trino.SystemSessionProperties.SPILL_ENABLED;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.SystemSessionProperties.USE_COST_BASED_PARTITIONING;
import static io.trino.SystemSessionProperties.USE_EXACT_PARTITIONING;
import static io.trino.SystemSessionProperties.isColocatedJoinEnabled;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.symbol;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.TopNNode.Step.FINAL;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAddExchangesPlans
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        PlanTester planTester = PlanTester.create(session);
        planTester.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testRepartitionForUnionWithAnyTableScans()
    {
        assertDistributedPlan("SELECT nationkey FROM nation UNION select regionkey from region",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("region"))))))));
        assertDistributedPlan("SELECT nationkey FROM nation UNION select 1",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L)))))))))));
    }

    @Test
    public void testRepartitionForUnionAllBeforeHashJoin()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, DistributionType.PARTITIONED.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .build();

        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select nationkey from nation) n join region r on n.nationkey = r.regionkey",
                session,
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey")
                                .left(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))))
                                .right(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));

        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select 1) n join region r on n.nationkey = r.regionkey",
                session,
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey")
                                .left(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                                exchange(REMOTE, REPARTITION,
                                                        values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L)))))))
                                .right(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }

    @Test
    public void testSingleGatheringExchangeForUnionAllWithLimit()
    {
        assertDistributedPlan(
                """
                SELECT * FROM (
                    SELECT nationkey FROM nation
                    UNION ALL
                    SELECT nationkey FROM nation
                    UNION ALL
                    SELECT nationkey FROM nation
                )
                LIMIT 2
                """,
                output(
                        limit(2, ImmutableList.of(), false,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, GATHER,
                                            limit(2, ImmutableList.of(), true,
                                                    exchange(LOCAL, REPARTITION,
                                                            limit(2, ImmutableList.of(), true, tableScan("nation")),
                                                            limit(2, ImmutableList.of(), true, tableScan("nation")),
                                                            limit(2, ImmutableList.of(), true, tableScan("nation")))))))));
    }

    @Test
    public void testNonSpillableBroadcastJoinAboveTableScan()
    {
        assertDistributedPlan(
                "SELECT * FROM nation n join region r on n.nationkey = r.regionkey",
                noJoinReordering(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey")
                                .distributionType(REPLICATED)
                                .spillable(false)
                                .left(
                                        node(
                                                FilterNode.class,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))
                                .right(
                                        anyTree(
                                                exchange(REMOTE, REPLICATE,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));

        assertDistributedPlan(
                "SELECT * FROM nation n join region r on n.nationkey = r.regionkey",
                spillEnabledWithJoinDistributionType(PARTITIONED),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey")
                                .distributionType(DistributionType.PARTITIONED)
                                .left(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))
                                .right(
                                        exchange(LOCAL, GATHER,
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }

    @Test
    public void testForcePartitioningMarkDistinctInput()
    {
        String query = "SELECT count(orderkey), count(distinct orderkey), custkey , count(1) FROM ( SELECT * FROM (VALUES (1, 2)) as t(custkey, orderkey) UNION ALL SELECT 3, 4) GROUP BY 3";
        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(IGNORE_DOWNSTREAM_PREFERENCES, "true")
                        .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                        .build(),
                anyTree(
                        node(MarkDistinctNode.class,
                                anyTree(
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partition1", "partition2"),
                                                values(
                                                        ImmutableList.of("partition1", "partition2", "field"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L), new Constant(INTEGER, 1L))))),
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partition3", "partition4"),
                                                values(
                                                        ImmutableList.of("partition3", "partition4", "field_0"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 3L), new Constant(INTEGER, 4L), new Constant(INTEGER, 1L)))))))));

        assertDistributedPlan(
                query,
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(IGNORE_DOWNSTREAM_PREFERENCES, "false")
                        .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                        .build(),
                anyTree(
                        node(MarkDistinctNode.class,
                                anyTree(
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partition1"),
                                                values(
                                                        ImmutableList.of("partition1", "partition2", "field"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L), new Constant(INTEGER, 1L))))),
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partition3"),
                                                values(
                                                        ImmutableList.of("partition3", "partition4", "field_0"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 3L), new Constant(INTEGER, 4L), new Constant(INTEGER, 1L)))))))));
    }

    @Test
    public void testImplementOffsetWithOrderedSource()
    {
        // no repartitioning exchange is added below row number, so the ordering established by topN is preserved.
        // also, no repartitioning exchange is added above row number, so the order is respected at output.
        assertPlan(
                "SELECT name FROM nation ORDER BY regionkey, name OFFSET 5 LIMIT 2",
                output(
                        project(
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "row_num"), new Constant(BIGINT, 5L)),
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                project(
                                                        ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                                        topN(
                                                                7,
                                                                ImmutableList.of(sort("regionkey", ASCENDING, LAST), sort("name", ASCENDING, LAST)),
                                                                FINAL,
                                                                anyTree(
                                                                        tableScan("nation", ImmutableMap.of("name", "name", "regionkey", "regionkey"))))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));
    }

    @Test
    public void testImplementOffsetWithUnorderedSource()
    {
        // no ordering of output is expected; repartitioning exchange is present in the plan
        assertPlan(
                "SELECT name FROM nation OFFSET 5 LIMIT 2",
                any(
                        project(
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "row_num"), new Constant(BIGINT, 5L)),
                                        exchange(
                                                LOCAL,
                                                REPARTITION,
                                                rowNumber(
                                                        pattern -> pattern
                                                                .partitionBy(ImmutableList.of()),
                                                        limit(
                                                                7,
                                                                anyTree(
                                                                        tableScan("nation", ImmutableMap.of("name", "name")))))
                                                        .withAlias("row_num", new RowNumberSymbolMatcher()))))));
    }

    @Test
    public void testExchangesAroundTrivialProjection()
    {
        // * source of Projection is single stream (topN)
        // * parent of Projection requires single stream distribution (rowNumber)
        // ==> Projection is planned with single stream distribution.
        assertPlan(
                "SELECT name, row_number() OVER () FROM (SELECT * FROM nation ORDER BY nationkey LIMIT 5)",
                any(
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(ImmutableList.of()),
                                project(
                                        ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                        topN(
                                                5,
                                                ImmutableList.of(sort("nationkey", ASCENDING, LAST)),
                                                FINAL,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("name", "name", "nationkey", "nationkey"))))))));

        // * source of Projection is distributed (filter)
        // * parent of Projection requires single distribution (rowNumber)
        // ==> Projection is planned with multiple distribution and gathering exchange is added on top of Projection.
        assertPlan(
                "SELECT b, row_number() OVER () FROM (VALUES (1, 2)) t(a, b) WHERE a < 10",
                disablePushFilterIntoValues(),
                any(
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(ImmutableList.of()),
                                exchange(
                                        LOCAL,
                                        GATHER,
                                        project(
                                                ImmutableMap.of("b", expression(new Reference(INTEGER, "b"))),
                                                filter(
                                                        new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 10L)),
                                                        exchange(
                                                                LOCAL,
                                                                REPARTITION,
                                                                values("a", "b"))))))));

        // * source of Projection is single stream (topN)
        // * parent of Projection requires hashed multiple distribution (rowNumber).
        // ==> Projection is planned with single distribution. Hash partitioning exchange is added on top of Projection.
        assertPlan(
                "SELECT row_number() OVER (PARTITION BY regionkey) FROM (SELECT * FROM nation ORDER BY nationkey LIMIT 5)",
                anyTree(
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(ImmutableList.of("regionkey")),
                                exchange(
                                        LOCAL,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("regionkey"),
                                        project(
                                                ImmutableMap.of("regionkey", expression(new Reference(BIGINT, "regionkey"))),
                                                topN(
                                                        5,
                                                        ImmutableList.of(sort("nationkey", ASCENDING, LAST)),
                                                        FINAL,
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("regionkey", "regionkey", "nationkey", "nationkey")))))))));

        // * source of Projection is distributed (filter)
        // * parent of Projection requires hashed multiple distribution (rowNumber).
        // ==> Projection is planned with multiple distribution (no exchange added below). Hash partitioning exchange is added on top of Projection.
        assertPlan(
                "SELECT row_number() OVER (PARTITION BY b) FROM (VALUES (1, 2)) t(a,b) WHERE a < 10",
                disablePushFilterIntoValues(),
                anyTree(
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(ImmutableList.of("b")),
                                exchange(
                                        LOCAL,
                                        REPARTITION,
                                        ImmutableList.of(),
                                        ImmutableSet.of("b"),
                                        project(
                                                ImmutableMap.of("b", expression(new Reference(INTEGER, "b"))),
                                                filter(
                                                        new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 10L)),
                                                        exchange(
                                                                LOCAL,
                                                                REPARTITION,
                                                                values("a", "b"))))))));

        // * source of Projection is single stream (topN)
        // * parent of Projection requires random multiple distribution (partial aggregation)
        // ==> Projection is planned with multiple distribution (round robin exchange is added below).
        assertPlan(
                "SELECT count(name) FROM (SELECT * FROM nation ORDER BY nationkey LIMIT 5)",
                anyTree(
                        aggregation(
                                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of("name"))),
                                PARTIAL,
                                project(
                                        ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                        exchange(
                                                LOCAL,
                                                REPARTITION,
                                                topN(
                                                        5,
                                                        ImmutableList.of(sort("nationkey", ASCENDING, LAST)),
                                                        FINAL,
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("name", "name", "nationkey", "nationkey")))))))));

        // * source of Projection is distributed (filter)
        // * parent of Projection requires random multiple distribution (aggregation)
        // ==> Projection is planned with multiple distribution (no exchange added)
        assertPlan(
                "SELECT count(b) FROM (VALUES (1, 2)) t(a,b) WHERE a < 10",
                disablePushFilterIntoValues(),
                anyTree(
                        aggregation(
                                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of("b"))),
                                PARTIAL,
                                project(
                                        ImmutableMap.of("b", expression(new Reference(INTEGER, "b"))),
                                        filter(
                                                new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 10L)),
                                                exchange(
                                                        LOCAL,
                                                        REPARTITION,
                                                        values("a", "b")))))));

        assertPlan(
                "SELECT 10, a FROM (VALUES 1) t(a)",
                anyTree(
                        values(ImmutableList.of("a", "expr"), ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 10L))))));

        assertPlan(
                "SELECT 1 UNION ALL SELECT 1",
                anyTree(
                        exchange(
                                LOCAL,
                                REPARTITION,
                                values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L)))),
                                values(ImmutableList.of("expr_0"), ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L)))))));
    }

    @Test
    public void testJoinBuildSideLocalExchange()
    {
        // build side smaller than threshold, local gathering exchanged expected
        assertDistributedPlan(
                "SELECT * FROM nation n join region r on n.nationkey = r.regionkey",
                noJoinReordering(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey")
                                .left(
                                        node(
                                                FilterNode.class,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))
                                .right(
                                        exchange(LOCAL, GATHER,
                                                exchange(REMOTE, REPLICATE,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));

        // build side bigger than threshold, local partitioned exchanged expected
        assertDistributedPlan(
                "SELECT * FROM nation n join region r on n.nationkey = r.regionkey",
                Session.builder(noJoinReordering())
                        .setSystemProperty(JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT, "1")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey")
                                .left(
                                        node(
                                                FilterNode.class,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))
                                .right(
                                        exchange(LOCAL, REPARTITION,
                                                exchange(REMOTE, REPLICATE,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
        // build side contains join, local partitioned exchanged expected
        assertDistributedPlan(
                "SELECT * FROM nation n join (select r.regionkey from region r join region r2 on r.regionkey = r2.regionkey) j on n.nationkey = j.regionkey ",
                noJoinReordering(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey2")
                                .left(
                                        node(
                                                FilterNode.class,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))
                                .right(
                                        exchange(LOCAL, REPARTITION,
                                                exchange(REMOTE, REPLICATE,
                                                        join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                                .equiCriteria("regionkey2", "regionkey1")
                                                                .left(
                                                                        node(
                                                                                FilterNode.class,
                                                                                tableScan("region", ImmutableMap.of("regionkey2", "regionkey"))))
                                                                .right(
                                                                        exchange(LOCAL, GATHER,
                                                                                exchange(REMOTE, REPLICATE,
                                                                                        tableScan("region", ImmutableMap.of("regionkey1", "regionkey"))))))))))));

        // build side smaller than threshold, but stats not available. local partitioned exchanged expected
        assertDistributedPlan(
                "SELECT * FROM nation n join region r on n.nationkey = r.regionkey",
                Session.builder(noJoinReordering())
                        .setSystemProperty(ENABLE_STATS_CALCULATOR, "false")
                        .build(),
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("nationkey", "regionkey")
                                .left(
                                        node(
                                                FilterNode.class,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))
                                .right(
                                        exchange(LOCAL, REPARTITION,
                                                exchange(REMOTE, REPLICATE,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }

    @Test
    public void testAggregateIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    AVG(1)\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(*)\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        "    GROUP BY\n" +
                        "        orderkey,\n" +
                        "        orderstatus\n" +
                        ")\n" +
                        "GROUP BY\n" +
                        "    orderkey",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ordertatus", "orderstatus",
                                                                "orderkey", "orderkey",
                                                                "orderdate", "orderdate"))))))));
    }

    @Test
    public void testAggregationPrefersParentPartitioning()
    {
        String singleColumnParentGroupBy =
                """
                SELECT (partkey, sum(count))
                FROM (
                    SELECT suppkey, partkey, count(*) as count
                    FROM lineitem
                    GROUP BY suppkey, partkey)
                GROUP BY partkey""";

        // parent aggregation partitioned by a single column
        assertDistributedPlan(
                singleColumnParentGroupBy,
                anyTree(aggregation(
                        singleGroupingSet("partkey"),
                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", false, ImmutableList.of(symbol("count")))),
                        Optional.empty(),
                        SINGLE, // no need for partial aggregation since data are already partitioned
                        project(aggregation(
                                ImmutableMap.of("count", aggregationFunction("count", false, ImmutableList.of(symbol("count_partial")))),
                                Step.FINAL,
                                exchange(LOCAL,
                                        // we only partition by partkey but aggregate by partkey and suppkey
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partkey"),
                                                aggregation(
                                                        singleGroupingSet("partkey", "suppkey"),
                                                        ImmutableMap.of(Optional.of("count_partial"), aggregationFunction("count", false, ImmutableList.of())),
                                                        Optional.empty(),
                                                        Step.PARTIAL,
                                                        tableScan("lineitem", ImmutableMap.of(
                                                                "partkey", "partkey",
                                                                "suppkey", "suppkey"))))))))));

        PlanMatchPattern exactPartitioningPlan = anyTree(aggregation(
                singleGroupingSet("partkey"),
                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", false, ImmutableList.of(symbol("sum_partial")))),
                Optional.empty(),
                Step.FINAL,
                exchange(LOCAL,
                        // additional remote exchange
                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partkey"),
                                aggregation(
                                        singleGroupingSet("partkey"),
                                        ImmutableMap.of(Optional.of("sum_partial"), aggregationFunction("sum", false, ImmutableList.of(symbol("count")))),
                                        Optional.empty(),
                                        PARTIAL,
                                        project(aggregation(
                                                ImmutableMap.of("count", aggregationFunction("count", false, ImmutableList.of(symbol("count_partial")))),
                                                Step.FINAL,
                                                exchange(LOCAL,
                                                        // forced exact partitioning
                                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partkey", "suppkey"),
                                                                aggregation(
                                                                        singleGroupingSet("partkey", "suppkey"),
                                                                        ImmutableMap.of(Optional.of("count_partial"), aggregationFunction("count", false, ImmutableList.of())),
                                                                        Optional.empty(),
                                                                        PARTIAL,
                                                                        tableScan("lineitem", ImmutableMap.of(
                                                                                "partkey", "partkey",
                                                                                "suppkey", "suppkey"))))))))))));
        // parent partitioning would be preferable but use_cost_based_partitioning=false prevents it
        assertDistributedPlan(singleColumnParentGroupBy, doNotUseCostBasedPartitioning(), exactPartitioningPlan);
        // parent partitioning would be preferable but use_exact_partitioning prevents it
        assertDistributedPlan(singleColumnParentGroupBy, useExactPartitioning(), exactPartitioningPlan);
        // no stats. fallback to exact partitioning expected
        assertDistributedPlan(singleColumnParentGroupBy, disableStats(), exactPartitioningPlan);
        // parent partitioning with estimated small number of distinct values. fallback to exact partitioning expected
        assertDistributedPlan(
                """
                SELECT (partkey_expr, sum(count))
                FROM (
                    SELECT suppkey, partkey % 10 as partkey_expr, count(*) as count
                    FROM lineitem
                    GROUP BY suppkey, partkey % 10)
                GROUP BY partkey_expr
                """,
                anyTree(aggregation(
                        singleGroupingSet("partkey_expr"),
                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", false, ImmutableList.of(symbol("sum_partial")))),
                        Optional.empty(),
                        Step.FINAL,
                        exchange(LOCAL,
                                // additional remote exchange
                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partkey_expr"),
                                        aggregation(
                                                singleGroupingSet("partkey_expr"),
                                                ImmutableMap.of(Optional.of("sum_partial"), aggregationFunction("sum", false, ImmutableList.of(symbol("count")))),
                                                Optional.empty(),
                                                PARTIAL,
                                                project(aggregation(
                                                        ImmutableMap.of("count", aggregationFunction("count", false, ImmutableList.of(symbol("count_partial")))),
                                                        Step.FINAL,
                                                        exchange(LOCAL,
                                                                // forced exact partitioning
                                                                exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("partkey_expr", "suppkey"),
                                                                        aggregation(
                                                                                singleGroupingSet("partkey_expr", "suppkey"),
                                                                                ImmutableMap.of(Optional.of("count_partial"), aggregationFunction("count", false, ImmutableList.of())),
                                                                                Optional.empty(),
                                                                                PARTIAL,
                                                                                project(
                                                                                        ImmutableMap.of("partkey_expr", expression(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "partkey"), new Constant(BIGINT, 10L))))),
                                                                                        tableScan("lineitem", ImmutableMap.of(
                                                                                                "partkey", "partkey",
                                                                                                "suppkey", "suppkey"))))))))))))));

        // parent aggregation partitioned by multiple columns
        assertDistributedPlan(
                """
                SELECT (orderkey % 10000, partkey, sum(count))
                FROM (
                    SELECT orderkey % 10000 as orderkey, partkey, suppkey, count(*) as count
                    FROM lineitem
                    GROUP BY orderkey % 10000, partkey, suppkey)
                GROUP BY orderkey, partkey
                """,
                anyTree(aggregation(
                        singleGroupingSet("orderkey_expr", "partkey"),
                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", false, ImmutableList.of(symbol("count")))),
                        Optional.empty(),
                        SINGLE, // no need for partial aggregation since data are already partitioned
                        project(aggregation(
                                ImmutableMap.of("count", aggregationFunction("count", false, ImmutableList.of(symbol("count_partial")))),
                                Step.FINAL,
                                exchange(LOCAL,
                                        // we don't partition by suppkey because it's not needed by the parent aggregation
                                        exchange(REMOTE, REPARTITION, ImmutableList.of(), ImmutableSet.of("orderkey_expr", "partkey"),
                                                aggregation(
                                                        singleGroupingSet("orderkey_expr", "partkey", "suppkey"),
                                                        ImmutableMap.of(Optional.of("count_partial"), aggregationFunction("count", false, ImmutableList.of())),
                                                        Optional.empty(),
                                                        Step.PARTIAL,
                                                        project(
                                                                ImmutableMap.of("orderkey_expr", expression(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 10000L))))),
                                                                tableScan("lineitem", ImmutableMap.of(
                                                                        "partkey", "partkey",
                                                                        "orderkey", "orderkey",
                                                                        "suppkey", "suppkey")))))))))));
    }

    @Test
    public void testWindowIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    AVG(otherwindow) OVER (\n" +
                        "        PARTITION BY\n" +
                        "            orderkey\n" +
                        "    )\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(*) OVER (\n" +
                        "            PARTITION BY\n" +
                        "                orderkey,\n" +
                        "                orderstatus\n" +
                        "        ) AS otherwindow\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        ")",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of(
                                                                "orderkey", "orderkey",
                                                                "orderdate", "orderdate"))))))));
    }

    @Test
    public void testRowNumberIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    *\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        a,\n" +
                        "        ROW_NUMBER() OVER (\n" +
                        "            PARTITION BY\n" +
                        "                a\n" +
                        "        ) rn\n" +
                        "    FROM (\n" +
                        "        VALUES\n" +
                        "            (1)\n" +
                        "    ) t (a)\n" +
                        ") t",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                values("a"))));
    }

    @Test
    public void testTopNRowNumberIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    a,\n" +
                        "    ROW_NUMBER() OVER (\n" +
                        "        PARTITION BY\n" +
                        "            a\n" +
                        "        ORDER BY\n" +
                        "            a\n" +
                        "    ) rn\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        a,\n" +
                        "        b,\n" +
                        "        COUNT(*)\n" +
                        "    FROM (\n" +
                        "        VALUES\n" +
                        "            (1, 2)\n" +
                        "    ) t (a, b)\n" +
                        "    GROUP BY\n" +
                        "        a,\n" +
                        "        b\n" +
                        ")\n" +
                        "LIMIT\n" +
                        "    2",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        values("a", "b")))));
    }

    @Test
    public void testJoinIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "SELECT\n" +
                        "    orders.orderkey,\n" +
                        "    orders.orderstatus\n" +
                        "FROM (\n" +
                        "    SELECT\n" +
                        "        orderkey,\n" +
                        "        ARBITRARY(orderstatus) AS orderstatus,\n" +
                        "        COUNT(*)\n" +
                        "    FROM orders\n" +
                        "    GROUP BY\n" +
                        "        orderkey\n" +
                        ") t,\n" +
                        "orders\n" +
                        "WHERE\n" +
                        "    orders.orderkey = t.orderkey\n" +
                        "    AND orders.orderstatus = t.orderstatus",
                useExactPartitioning(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        aggregation(
                                                singleGroupingSet("orderkey"),
                                                ImmutableMap.of(Optional.of("any_value"), PlanMatchPattern.aggregationFunction("any_value", ImmutableList.of("orderstatus"))),
                                                ImmutableList.of("orderkey"),
                                                ImmutableList.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                tableScan("orders", ImmutableMap.of(
                                                        "orderkey", "orderkey",
                                                        "orderstatus", "orderstatus"))))),
                        exchange(LOCAL, GATHER,
                                exchange(REMOTE, REPARTITION,
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey1", "orderkey",
                                                "orderstatus3", "orderstatus"))))));
    }

    @Test
    public void testMarkDistinctIsExactlyPartitioned()
    {
        assertDistributedPlan(
                "    SELECT\n" +
                        "        orderkey,\n" +
                        "        orderstatus,\n" +
                        "        COUNT(DISTINCT orderdate),\n" +
                        "        COUNT(DISTINCT clerk)\n" +
                        "    FROM orders\n" +
                        "    WHERE\n" +
                        "        orderdate > CAST('2042-01-01' AS DATE)\n" +
                        "    GROUP BY\n" +
                        "        orderkey,\n" +
                        "        orderstatus\n",
                useExactPartitioningWithMarkDistinct(),
                anyTree(
                        exchange(REMOTE, REPARTITION,
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                anyTree(
                                                        exchange(REMOTE, REPARTITION,
                                                                anyTree(
                                                                        tableScan("orders", ImmutableMap.of(
                                                                                "orderstatus", "orderstatus",
                                                                                "orderkey", "orderkey",
                                                                                "clerk", "clerk",
                                                                                "orderdate", "orderdate"))))))))));
    }

    // Negative test for use-exact-partitioning when colocated join is disabled
    @Test
    public void testJoinNotExactlyPartitionedWhenColocatedJoinDisabled()
    {
        assertDistributedPlan(
                """
                        SELECT
                            orders.orderkey,
                            orders.orderstatus
                        FROM (
                            SELECT
                                orderkey,
                                ARBITRARY(orderstatus) AS orderstatus,
                                COUNT(*)
                            FROM orders
                        GROUP BY
                            orderkey
                        ) t,
                        orders
                        WHERE
                            orders.orderkey = t.orderkey
                            AND orders.orderstatus = t.orderstatus
                """,
                noJoinReorderingColocatedJoinDisabled(),
                anyTree(
                        anyTree(
                                tableScan("orders")),
                        exchange(LOCAL, GATHER,
                                exchange(REMOTE, REPARTITION,
                                        tableScan("orders")))));
    }

    // Negative test for use-exact-partitioning when colocated join is enabled (default)
    @Test
    public void testJoinNotExactlyPartitioned()
    {
        assertThat(isColocatedJoinEnabled(getPlanTester().getDefaultSession())).isTrue();

        assertDistributedPlan(
                """
                    SELECT
                        orders.orderkey,
                        orders.orderstatus
                    FROM (
                        SELECT
                            orderkey,
                            ARBITRARY(orderstatus) AS orderstatus,
                            COUNT(*)
                        FROM orders
                        GROUP BY
                            orderkey
                    ) t,
                    orders
                    WHERE
                        orders.orderkey = t.orderkey
                        AND orders.orderstatus = t.orderstatus
                """,
                noJoinReordering(),
                anyTree(
                        anyTree(
                                tableScan("orders")),
                        exchange(LOCAL, GATHER,
                                tableScan("orders"))));
    }

    @Test
    public void testBroadcastJoinAboveUnionAll()
    {
        // Put union at build side
        assertDistributedPlan(
                """
                    SELECT * FROM region r JOIN (SELECT nationkey FROM nation UNION ALL SELECT nationkey as key FROM nation) n ON r.regionkey = n.nationkey
                """,
                noJoinReordering(),
                anyTree(
                        join(INNER, join -> join
                                .equiCriteria("regionkey", "nationkey")
                                .left(
                                        node(FilterNode.class,
                                                tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))
                                .right(
                                        exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                                exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION,
                                                        exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")),
                                                                tableScan("nation"))))))));
        // Put union at probe side
        assertDistributedPlan(
                """
                SELECT * FROM (SELECT nationkey FROM nation UNION ALL SELECT nationkey as key FROM nation) n JOIN region r ON r.regionkey = n.nationkey
                """,
                noJoinReordering(),
                anyTree(
                        join(INNER, join -> join
                                .equiCriteria("nationkey", "regionkey")
                                .left(
                                        exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                node(FilterNode.class,
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))),
                                                node(FilterNode.class,
                                                        tableScan("nation"))))
                                .right(
                                        exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                                exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION,
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }

    @Test
    public void testUnionAllAboveBroadcastJoin()
    {
        assertDistributedPlan(
                """
                    SELECT regionkey FROM nation UNION ALL (SELECT nationkey FROM nation n JOIN region r on r.regionkey = n.nationkey)
                """,
                noJoinReordering(),
                anyTree(
                        exchange(REMOTE, GATHER, SINGLE_DISTRIBUTION,
                                tableScan("nation"),
                                join(INNER, join -> join
                                        .equiCriteria("nationkey", "regionkey")
                                        .left(
                                                node(FilterNode.class,
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))
                                        .right(
                                                exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                                        exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION,
                                                                tableScan("region", ImmutableMap.of("regionkey", "regionkey")))))))));
    }

    @Test
    public void testGroupedAggregationAboveUnionAllCrossJoined()
    {
        assertDistributedPlan(
                """
                    SELECT sum(nationkey) FROM (SELECT nationkey FROM nation UNION ALL SELECT nationkey FROM nation), region group by nationkey
                """,
                noJoinReordering(),
                anyTree(
                        join(INNER, join -> join
                                .left(
                                        exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")),
                                                tableScan("nation")))
                                .right(
                                        exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                                exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION,
                                                        tableScan("region")))))));
    }

    @Test
    public void testGroupedAggregationAboveUnionAll()
    {
        assertDistributedPlan(
                """
                    SELECT sum(nationkey) FROM (SELECT nationkey FROM nation UNION ALL SELECT nationkey FROM nation) GROUP BY nationkey
                """,
                noJoinReordering(),
                anyTree(
                        exchange(LOCAL, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                project(
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                aggregation(ImmutableMap.of("partial_sum", aggregationFunction("sum", ImmutableList.of("nationkey"))),
                                                        PARTIAL,
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))),
                                project(
                                        exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                aggregation(ImmutableMap.of("partial_sum", aggregationFunction("sum", ImmutableList.of("nationkey"))),
                                                        PARTIAL,
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))))));
    }

    @Test
    public void testUnionAllOnPartitionedAndUnpartitionedSources()
    {
        assertDistributedPlan(
                """
                     SELECT * FROM (SELECT nationkey FROM nation UNION ALL VALUES (1))
                """,
                noJoinReordering(),
                output(
                        exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                values("1"),
                                exchange(REMOTE, GATHER, SINGLE_DISTRIBUTION,
                                        tableScan("nation")))));
    }

    @Test
    public void testNestedUnionAll()
    {
        assertDistributedPlan(
                """
                     SELECT * FROM ((SELECT nationkey FROM nation) UNION ALL (SELECT nationkey FROM nation)) UNION ALL (SELECT nationkey FROM nation)
                """,
                noJoinReordering(),
                output(
                        exchange(REMOTE, GATHER, SINGLE_DISTRIBUTION,
                                tableScan("nation"),
                                tableScan("nation"),
                                tableScan("nation"))));
    }

    @Test
    public void testUnionAllOnSourceAndHashDistributedChildren()
    {
        assertDistributedPlan(
                """
                     SELECT * FROM ((SELECT nationkey FROM nation) UNION ALL (SELECT nationkey FROM nation)) UNION ALL (SELECT sum(nationkey) FROM nation GROUP BY nationkey)
                """,
                noJoinReordering(),
                output(
                        exchange(REMOTE, GATHER, SINGLE_DISTRIBUTION,
                                tableScan("nation"),
                                tableScan("nation"),
                                project(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION, FIXED_HASH_DISTRIBUTION,
                                                        aggregation(ImmutableMap.of("partial_sum", aggregationFunction("sum", ImmutableList.of("nationkey"))),
                                                                PARTIAL,
                                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))))));
    }

    @Test
    public void testUnionAllOnDifferentCatalogs()
    {
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        new ColumnMetadata("nationkey", BigintType.BIGINT)))
                .withGetTableHandle((session, schemaTableName) -> new MockConnectorTableHandle(
                        SchemaTableName.schemaTableName("default", "nation"),
                        TupleDomain.all(),
                        Optional.of(ImmutableList.of(new MockConnectorColumnHandle("nationkey", BigintType.BIGINT)))))
                .withName("mock")
                .build();
        getPlanTester().createCatalog("mock", connectorFactory, ImmutableMap.of());

        // Need to use JOIN as parent of UNION ALL to expose replacing remote exchange with local exchange
        assertDistributedPlan(
                """
                         SELECT * FROM (SELECT nationkey FROM nation UNION ALL SELECT nationkey FROM mock.default.nation), region
                """,
                noJoinReordering(),
                output(
                        join(INNER, join -> join
                                .left(exchange(REMOTE, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                        tableScan("nation"),
                                        node(TableScanNode.class)))
                                .right(
                                        exchange(LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                                exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION,
                                                        tableScan("region")))))));
    }

    @Test
    public void testUnionAllOnInternalCatalog()
    {
        // Need to use JOIN as parent of UNION ALL to expose replacing remote exchange with local exchange
        // TODO: https://starburstdata.atlassian.net/browse/SEP-11273
        assertDistributedPlan(
                """
                         SELECT * FROM (SELECT table_catalog FROM system.information_schema.tables UNION ALL SELECT table_catalog FROM system.information_schema.tables), region
                """,
                noJoinReordering(),
                output(
                        join(INNER, join -> join
                                .left(exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                        tableScan("tables"),
                                        tableScan("tables")))
                                .right(
                                        exchange(
                                                LOCAL, GATHER, SINGLE_DISTRIBUTION,
                                                exchange(REMOTE, REPLICATE, FIXED_BROADCAST_DISTRIBUTION,
                                                        tableScan("region")))))));
    }

    @Test
    public void testUnionAllOnTableScanAndValues()
    {
        assertDistributedPlan(
                """
                         SELECT * FROM (SELECT nationkey FROM nation UNION ALL VALUES(1))
                """,
                noJoinReordering(),
                output(
                        exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                node(ValuesNode.class),
                                exchange(REMOTE, GATHER, SINGLE_DISTRIBUTION,
                                        tableScan("nation")))));
    }

    private Session spillEnabledWithJoinDistributionType(JoinDistributionType joinDistributionType)
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, joinDistributionType.toString())
                .setSystemProperty(SPILL_ENABLED, "true")
                .setSystemProperty(TASK_CONCURRENCY, "16")
                .build();
    }

    private Session noJoinReordering()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(SPILL_ENABLED, "true")
                .setSystemProperty(TASK_CONCURRENCY, "16")
                .build();
    }

    private Session noJoinReorderingColocatedJoinDisabled()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(TASK_CONCURRENCY, "16")
                .setSystemProperty(COLOCATED_JOIN, "false")
                .build();
    }

    private Session useExactPartitioning()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .setSystemProperty(USE_EXACT_PARTITIONING, "true")
                .build();
    }

    private Session useExactPartitioningWithMarkDistinct()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .setSystemProperty(USE_EXACT_PARTITIONING, "true")
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "mark_distinct")
                .build();
    }

    private Session doNotUseCostBasedPartitioning()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(USE_COST_BASED_PARTITIONING, "false")
                .build();
    }

    private Session disableStats()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(ENABLE_STATS_CALCULATOR, "false")
                .build();
    }

    private Session disablePushFilterIntoValues()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT, "0")
                .build();
    }
}
