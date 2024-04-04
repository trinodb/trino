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
import com.google.common.primitives.ImmutableLongArray;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.scheduler.faulttolerant.OutputStatsEstimator;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.SubPlanMatcher;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_RUNTIME_ADAPTIVE_PARTITIONING_ENABLED;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_RUNTIME_ADAPTIVE_PARTITIONING_MAX_TASK_SIZE;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_RUNTIME_ADAPTIVE_PARTITIONING_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.remoteSource;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.JoinType.LEFT;

public class TestAdaptivePartitioning
        extends BasePlanTest
{
    private static final long ONE_MB = DataSize.of(1, MEGABYTE).toBytes();

    @Test
    public void testCreateTableAs()
    {
        // already started: 3, 4, 1
        // added fragments: 6, 8, 9
        // modified fragments: 7 (with partitionCount = 10)
        //   0                 5
        //  / \              /  \
        // 1*  2     =>    [6]   7
        //    / \           |   / \
        //   3* 4*         1* [8] [9]
        //                     |   |
        //                    3*   4*
        SubPlanMatcher matcher = SubPlanMatcher.builder()
                .fragmentMatcher(fm -> fm.fragmentId(5))
                .children(
                        sb -> sb.fragmentMatcher(fm -> fm.fragmentId(6).outputPartitionCount(10).inputPartitionCount(1))
                                .children(sb1 -> sb1.fragmentMatcher(fm -> fm.fragmentId(1).outputPartitionCount(1))),
                        sb -> sb.fragmentMatcher(fm -> fm.fragmentId(7).inputPartitionCount(10).outputPartitionCount(10))
                                .children(
                                        sb1 -> sb1.fragmentMatcher(fm -> fm.fragmentId(8).outputPartitionCount(10).inputPartitionCount(1))
                                                .children(sb2 -> sb2.fragmentMatcher(fm -> fm.fragmentId(3).outputPartitionCount(1))),
                                        sb1 -> sb1.fragmentMatcher(fm -> fm.fragmentId(9).outputPartitionCount(10).inputPartitionCount(1))
                                                .children(sb2 -> sb2.fragmentMatcher(fm -> fm.fragmentId(4).outputPartitionCount(1)))))
                .build();

        assertAdaptivePlan(
                """
                SELECT n1.* FROM nation n1
                RIGHT JOIN
                (SELECT n.nationkey FROM (SELECT * FROM lineitem WHERE suppkey BETWEEN 20 and 30) l LEFT JOIN nation n on l.suppkey = n.nationkey) n2
                ON n1.nationkey = n2.nationkey + 1
                """,
                getSession(),
                ImmutableMap.of(
                        new PlanFragmentId("3"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB * 2, ONE_MB), 10000),
                        new PlanFragmentId("4"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB, ONE_MB), 500),
                        new PlanFragmentId("1"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB, ONE_MB), 500)),
                matcher,
                true);
    }

    @Test
    public void testNoPartitionCountInLocalExchange()
    {
        SubPlanMatcher matcher = SubPlanMatcher.builder()
                .fragmentMatcher(fm -> fm
                        .fragmentId(3)
                        .inputPartitionCount(10)
                        .planPattern(
                                output(
                                        join(LEFT, builder -> builder
                                                .equiCriteria(ImmutableList.of(aliases ->
                                                        new JoinNode.EquiJoinClause(
                                                                new Symbol(BIGINT, "suppkey"),
                                                                new Symbol(BIGINT, "nationkey"))))
                                                .left(node(AdaptivePlanNode.class,
                                                        remoteSource(ImmutableList.of(new PlanFragmentId("4")))))
                                                // validate no partitionCount in local exchange
                                                .right(exchange(LOCAL, Optional.empty(),
                                                        node(AdaptivePlanNode.class,
                                                                remoteSource(ImmutableList.of(new PlanFragmentId("5"))))))))))
                .children(
                        sb1 -> sb1.fragmentMatcher(fm -> fm.fragmentId(4).outputPartitionCount(10).inputPartitionCount(1))
                                .children(sb2 -> sb2.fragmentMatcher(fm -> fm.fragmentId(1).outputPartitionCount(1))),
                        sb1 -> sb1.fragmentMatcher(fm -> fm.fragmentId(5).outputPartitionCount(10).inputPartitionCount(1))
                                .children(sb2 -> sb2.fragmentMatcher(fm -> fm.fragmentId(2).outputPartitionCount(1))))
                .build();

        assertAdaptivePlan(
                """
                SELECT l.* FROM lineitem l
                LEFT JOIN nation n
                ON l.suppkey = n.nationkey
                """,
                getSession(),
                ImmutableMap.of(
                        new PlanFragmentId("1"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB, ONE_MB), 500),
                        new PlanFragmentId("2"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB * 2, ONE_MB), 10000)),
                matcher,
                true);
    }

    @Test
    public void testSkipBroadcastSubtree()
    {
        // result of fragment 7 will be broadcast,
        // so no runtime adaptive partitioning will be applied to its subtree
        // already started: 4, 10, 11, 12
        // added fragments: 13
        //        0                  0
        //        |                  |
        //        1                  1
        //       / \              /     \
        //      2   7     =>     2       7
        //     / \  |           / \      |
        //    3  6  8          3  6      8
        //   / \   / \        / \       / \
        //  4* 5  9  12*    [13] 5     9  12*
        //       / \          |       / \
        //     10* 11*       4*     10* 11*

        SubPlanMatcher matcher = SubPlanMatcher.builder()
                .fragmentMatcher(fm -> fm.fragmentId(13))
                .children(sb -> sb.fragmentMatcher(fm -> fm.fragmentId(14).inputPartitionCount(10))
                        .children(
                                sb1 -> sb1.fragmentMatcher(fm -> fm.fragmentId(15).outputPartitionCount(10).inputPartitionCount(10))
                                        .children(
                                                sb2 -> sb2.fragmentMatcher(fm -> fm.fragmentId(16).outputPartitionCount(10).inputPartitionCount(10))
                                                        .children(
                                                                sb3 -> sb3.fragmentMatcher(fm -> fm.fragmentId(17).outputPartitionCount(10).inputPartitionCount(1))
                                                                        .children(sb4 -> sb4.fragmentMatcher(fm -> fm.fragmentId(4).outputPartitionCount(1))),
                                                                sb3 -> sb3.fragmentMatcher(fm -> fm.fragmentId(18).outputPartitionCount(10))),
                                                sb2 -> sb2.fragmentMatcher(fm -> fm.fragmentId(19).outputPartitionCount(10))),
                                sb1 -> sb1.fragmentMatcher(fm -> fm.fragmentId(7))
                                        .children(
                                                sb2 -> sb2.fragmentMatcher(fm -> fm.fragmentId(8).inputPartitionCount(1))
                                                        .children(
                                                                sb3 -> sb3.fragmentMatcher(fm -> fm.fragmentId(9).outputPartitionCount(1).inputPartitionCount(1))
                                                                        .children(
                                                                                sb4 -> sb4.fragmentMatcher(fm -> fm.fragmentId(10).outputPartitionCount(1)),
                                                                                sb4 -> sb4.fragmentMatcher(fm -> fm.fragmentId(11).outputPartitionCount(1))),
                                                                sb3 -> sb3.fragmentMatcher(fm -> fm.fragmentId(12).outputPartitionCount(1))))))
                .build();
        assertAdaptivePlan(
                "SELECT\n" +
                        "  ps.partkey,\n" +
                        "  sum(ps.supplycost * ps.availqty) AS value\n" +
                        "FROM\n" +
                        "  partsupp ps,\n" +
                        "  supplier s,\n" +
                        "  nation n\n" +
                        "WHERE\n" +
                        "  ps.suppkey = s.suppkey\n" +
                        "  AND s.nationkey = n.nationkey\n" +
                        "  AND n.name = 'GERMANY'\n" +
                        "GROUP BY\n" +
                        "  ps.partkey\n" +
                        "HAVING\n" +
                        "  sum(ps.supplycost * ps.availqty) > (\n" +
                        "    SELECT sum(ps.supplycost * ps.availqty) * 0.0001\n" +
                        "    FROM\n" +
                        "      partsupp ps,\n" +
                        "      supplier s,\n" +
                        "      nation n\n" +
                        "    WHERE\n" +
                        "      ps.suppkey = s.suppkey\n" +
                        "      AND s.nationkey = n.nationkey\n" +
                        "      AND n.name = 'GERMANY'\n" +
                        "  )\n" +
                        "ORDER BY\n" +
                        "  value DESC",
                getSession(),
                ImmutableMap.of(
                        new PlanFragmentId("4"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB * 2, ONE_MB), 10000),
                        new PlanFragmentId("10"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB, ONE_MB), 500),
                        new PlanFragmentId("11"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB, ONE_MB), 500),
                        new PlanFragmentId("12"), createRuntimeStats(ImmutableLongArray.of(ONE_MB, ONE_MB, ONE_MB), 500)),
                matcher,
                true);
    }



    private Session getSession()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .setSystemProperty(JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT, "0")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, OptimizerConfig.JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, OptimizerConfig.JoinDistributionType.PARTITIONED.name())
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_RUNTIME_ADAPTIVE_PARTITIONING_ENABLED, "true")
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_MAX_PARTITION_COUNT, "2")
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_MIN_PARTITION_COUNT, "1")
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_RUNTIME_ADAPTIVE_PARTITIONING_PARTITION_COUNT, "10")
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_RUNTIME_ADAPTIVE_PARTITIONING_MAX_TASK_SIZE, "1MB")
                .build();
    }

    private OutputStatsEstimator.OutputStatsEstimateResult createRuntimeStats(ImmutableLongArray partitionDataSizes, long outputRowCountEstimate)
    {
        return new OutputStatsEstimator.OutputStatsEstimateResult(partitionDataSizes, outputRowCountEstimate, "FINISHED", true);
    }
}
