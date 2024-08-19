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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.ImmutableLongArray;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.SubPlanMatcher;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator.OutputStatsEstimateResult;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.adaptivePlan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.remoteSource;
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestAdaptivePlanner
        extends BasePlanTest
{
    @Test
    public void testJoinOrderSwitchRule()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        SubPlanMatcher matcher = SubPlanMatcher.builder()
                .fragmentMatcher(fm -> fm
                        .fragmentId(3)
                        .planPattern(
                                any(
                                        adaptivePlan(
                                                join(INNER, builder -> builder
                                                        .equiCriteria(ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol(BIGINT, "nationkey"), new Symbol(BIGINT, "nationkey_1"))))
                                                        .left(remoteSource(ImmutableList.of(new PlanFragmentId("1"))))
                                                        .right(any(remoteSource(ImmutableList.of(new PlanFragmentId("2")))))),
                                                join(INNER, builder -> builder
                                                        .equiCriteria(ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol(BIGINT, "nationkey_1"), new Symbol(BIGINT, "nationkey"))))
                                                        .right(remoteSource(ImmutableList.of(new PlanFragmentId("1"))))
                                                        .left(any(remoteSource(ImmutableList.of(new PlanFragmentId("2"))))))))))
                .children(
                        spb -> spb.fragmentMatcher(fm -> fm.fragmentId(2).planPattern(node(TableScanNode.class))),
                        spb -> spb.fragmentMatcher(fm -> fm.fragmentId(1).planPattern(any(node(TableScanNode.class)))))
                .build();

        assertAdaptivePlan(
                "SELECT n.name FROM supplier AS s JOIN nation AS n on s.nationkey = n.nationkey",
                session,
                ImmutableList.of(new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new TestJoinOrderSwitchRule())
                                .build())),
                ImmutableMap.of(
                        new PlanFragmentId("1"), createRuntimeStats(ImmutableLongArray.of(10000L, 10000L, 10000L), 10000),
                        new PlanFragmentId("2"), createRuntimeStats(ImmutableLongArray.of(200L, 2000L, 1000L), 500)),
                matcher,
                false);
    }

    @Test
    public void testNoChangeInFragmentIdsForUnchangedSubPlans()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        SubPlanMatcher matcher = SubPlanMatcher.builder()
                .fragmentMatcher(fm -> fm
                        // This fragment id should change since it is downstream of adaptive stage
                        .fragmentId(5)
                        .planPattern(
                                output(
                                        node(AggregationNode.class,
                                                exchange(
                                                        remoteSource(ImmutableList.of(new PlanFragmentId("6"))))))))
                .children(
                        spb -> spb.fragmentMatcher(fm -> fm
                                // This fragment id should change since it has adaptive plan
                                .fragmentId(6)
                                .planPattern(node(AggregationNode.class,
                                        adaptivePlan(
                                                join(INNER, builder -> builder
                                                        .equiCriteria(ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol(BIGINT, "nationkey"), new Symbol(BIGINT, "count"))))
                                                        .left(remoteSource(ImmutableList.of(new PlanFragmentId("2"))))
                                                        .right(any(remoteSource(ImmutableList.of(new PlanFragmentId("3")))))),
                                                join(INNER, builder -> builder
                                                        .equiCriteria(ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol(BIGINT, "count"), new Symbol(BIGINT, "nationkey"))))
                                                        .right(remoteSource(ImmutableList.of(new PlanFragmentId("2"))))
                                                        .left(any(remoteSource(ImmutableList.of(new PlanFragmentId("3"))))))))))
                                .children(
                                        spb2 -> spb2.fragmentMatcher(fm -> fm
                                                // This fragment id should not change
                                                .fragmentId(3)
                                                .planPattern(
                                                        node(AggregationNode.class,
                                                                exchange(
                                                                        remoteSource(ImmutableList.of(new PlanFragmentId("4")))))))
                                                .children(spb3 -> spb3.fragmentMatcher(fm -> fm
                                                        // This fragment id should not change
                                                        .fragmentId(4)
                                                        .planPattern(node(AggregationNode.class, node(TableScanNode.class))))),
                                        spb2 -> spb2.fragmentMatcher(fm -> fm
                                                // This fragment id should not change
                                                .fragmentId(2).planPattern(any(node(TableScanNode.class))))))
                .build();

        assertAdaptivePlan(
                """
                    WITH t AS (SELECT regionkey, count(*) as some_count FROM nation group by regionkey)
                    SELECT max(s.nationkey), sum(t.regionkey)
                    FROM supplier AS s
                    JOIN t
                    ON s.nationkey = t.some_count
                """,
                session,
                ImmutableList.of(new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new TestJoinOrderSwitchRule())
                                .build())),
                ImmutableMap.of(
                        new PlanFragmentId("3"), createRuntimeStats(ImmutableLongArray.of(10000L, 10000L, 10000L), 10000),
                        new PlanFragmentId("2"), createRuntimeStats(ImmutableLongArray.of(200L, 2000L, 1000L), 500)),
                matcher,
                false);
    }

    @Test
    public void testNoChangeToRootSubPlanIfStatsAreAccurate()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        SubPlanMatcher matcher = SubPlanMatcher.builder()
                .fragmentMatcher(fm -> fm
                        .fragmentId(0)
                        .planPattern(
                                any(join(INNER, builder -> builder
                                        .equiCriteria(ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol(BIGINT, "nationkey"), new Symbol(BIGINT, "nationkey_1"))))
                                        .left(remoteSource(ImmutableList.of(new PlanFragmentId("1"))))
                                        .right(any(remoteSource(ImmutableList.of(new PlanFragmentId("2")))))))))
                .children(
                        spb -> spb.fragmentMatcher(fm -> fm.fragmentId(1).planPattern(any(node(TableScanNode.class)))),
                        spb -> spb.fragmentMatcher(fm -> fm.fragmentId(2).planPattern(node(TableScanNode.class))))
                .build();

        assertAdaptivePlan(
                "SELECT n.name FROM supplier AS s JOIN nation AS n on s.nationkey = n.nationkey",
                session,
                ImmutableList.of(new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new TestJoinOrderSwitchRule())
                                .build())),
                ImmutableMap.of(
                        new PlanFragmentId("1"), createRuntimeStats(ImmutableLongArray.of(10000L, 10000L, 10000L), 10000),
                        new PlanFragmentId("2"), createRuntimeStats(ImmutableLongArray.of(200L, 2000L, 1000L), 500),
                        // Since the runtime stats are accurate, adaptivePlanner will not change this subplan
                        new PlanFragmentId("0"), createRuntimeStats(ImmutableLongArray.of(10000L, 10000L, 10000L), 10000)),
                matcher,
                false);
    }

    @Test
    public void testNoChangeToNestedSubPlanIfStatsAreAccurate()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();

        SubPlanMatcher matcher = SubPlanMatcher.builder()
                .fragmentMatcher(fm -> fm
                        // This fragment id should change since it is downstream of adaptive stage
                        .fragmentId(0)
                        .planPattern(
                                output(
                                        node(AggregationNode.class,
                                                exchange(
                                                        remoteSource(ImmutableList.of(new PlanFragmentId("1"))))))))
                .children(
                        spb -> spb.fragmentMatcher(fm -> fm
                                        // This fragment id should change since it has adaptive plan
                                        .fragmentId(1)
                                        .planPattern(node(AggregationNode.class,
                                                join(INNER, builder -> builder
                                                        .equiCriteria(ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol(BIGINT, "nationkey"), new Symbol(BIGINT, "count"))))
                                                        .left(remoteSource(ImmutableList.of(new PlanFragmentId("2"))))
                                                        .right(any(remoteSource(ImmutableList.of(new PlanFragmentId("3")))))))))
                                .children(
                                        spb2 -> spb2.fragmentMatcher(fm -> fm
                                                // This fragment id should not change
                                                .fragmentId(2).planPattern(any(node(TableScanNode.class)))),
                                        spb2 -> spb2.fragmentMatcher(fm -> fm
                                                        // This fragment id should not change
                                                        .fragmentId(3)
                                                        .planPattern(
                                                                node(AggregationNode.class,
                                                                        exchange(
                                                                                remoteSource(ImmutableList.of(new PlanFragmentId("4")))))))
                                                .children(spb3 -> spb3.fragmentMatcher(fm -> fm
                                                        // This fragment id should not change
                                                        .fragmentId(4)
                                                        .planPattern(node(AggregationNode.class, node(TableScanNode.class)))))))
                .build();

        assertAdaptivePlan(
                """
                    WITH t AS (SELECT regionkey, count(*) as some_count FROM nation group by regionkey)
                    SELECT max(s.nationkey), sum(t.regionkey)
                    FROM supplier AS s
                    JOIN t
                    ON s.nationkey = t.some_count
                """,
                session,
                ImmutableList.of(new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new TestJoinOrderSwitchRule())
                                .build())),
                ImmutableMap.of(
                        // Since the runtime stats are accurate, adaptivePlanner will not change this subplan
                        new PlanFragmentId("1"), createRuntimeStats(ImmutableLongArray.of(10000L, 10000L, 10000L), 10000),
                        new PlanFragmentId("3"), createRuntimeStats(ImmutableLongArray.of(10000L, 10000L, 10000L), 10000),
                        new PlanFragmentId("4"), createRuntimeStats(ImmutableLongArray.of(10000L, 10000L, 10000L), 10000),
                        new PlanFragmentId("2"), createRuntimeStats(ImmutableLongArray.of(200L, 2000L, 1000L), 500)),
                matcher,
                false);
    }

    @Test
    public void testWhenSimilarColumnIsProjectedTwice()
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                // This is to ensure that the plan is not optimized to use partial aggregation
                .setSystemProperty("prefer_partial_aggregation", "false")
                .build();

        assertAdaptivePlan(
                    """
                    SELECT
                        sum(sales),
                        sum(another_sales),
                        sum(acctbal)
                    FROM (
                    SELECT
                        CAST(0 AS DECIMAL(7,2)) "sales",
                        CAST(0 AS DECIMAL(7,2)) "another_sales",
                        cast("acctbal" as DECIMAL(7,2)) "acctbal"
                    FROM customer
                    UNION ALL
                    SELECT
                        cast("acctbal" as DECIMAL(7,2)) "sales",
                        CAST(0 AS DECIMAL(7,2)) "another_sales",
                        CAST(0 AS DECIMAL(7,2)) "acctbal"
                    FROM customer
                    ) test_table
                """,
                session,
                ImmutableList.of(new IterativeOptimizer(
                        getPlanTester().getPlannerContext(),
                        new RuleStatsRecorder(),
                        getPlanTester().getStatsCalculator(),
                        getPlanTester().getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new TestJoinOrderSwitchRule())
                                .build())),
                ImmutableMap.of(),
                SubPlanMatcher.builder()
                        .fragmentMatcher(fm -> fm
                                .fragmentId(0)
                                .planPattern(
                                        output(
                                                node(AggregationNode.class,
                                                        exchange(
                                                                remoteSource(ImmutableList.of(new PlanFragmentId("1"), new PlanFragmentId("2"))))))))
                        .children(
                                spb -> spb.fragmentMatcher(fm -> fm
                                        .fragmentId(1)
                                        .planPattern(node(ProjectNode.class, node(TableScanNode.class)))),
                                spb -> spb.fragmentMatcher(fm -> fm
                                        .fragmentId(2)
                                        .planPattern(node(ProjectNode.class, node(TableScanNode.class)))))
                        .build(),
                false);
    }

    private OutputStatsEstimateResult createRuntimeStats(ImmutableLongArray partitionDataSizes, long outputRowCountEstimate)
    {
        return new OutputStatsEstimateResult(partitionDataSizes, outputRowCountEstimate, "FINISHED", true);
    }

    // This is a test rule which switches the join order of two tables.
    private static class TestJoinOrderSwitchRule
            implements Rule<JoinNode>
    {
        private static final Pattern<JoinNode> PATTERN = Patterns.join();
        private final Set<PlanNodeId> alreadyVisited = new HashSet<>();

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(JoinNode node, Captures captures, Context context)
        {
            if (alreadyVisited.contains(node.getId())) {
                return Result.empty();
            }
            alreadyVisited.add(node.getId());
            return Result.ofPlanNode(node.flipChildren());
        }
    }
}
