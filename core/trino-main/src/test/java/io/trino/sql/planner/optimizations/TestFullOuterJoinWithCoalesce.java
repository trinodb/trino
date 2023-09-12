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
import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;

public class TestFullOuterJoinWithCoalesce
        extends BasePlanTest
{
    @Test
    @Disabled // TODO: re-enable once FULL join property derivations are re-introduced
    public void testFullOuterJoinWithCoalesce()
    {
        assertDistributedPlan(
                "SELECT coalesce(ts.a, r.a) " +
                        "FROM (" +
                        "   SELECT coalesce(t.a, s.a) AS a " +
                        "   FROM (VALUES 1, 2, 3) t(a) " +
                        "   FULL OUTER JOIN (VALUES 1, 4) s(a)" +
                        "   ON t.a = s.a) ts " +
                        "FULL OUTER JOIN (VALUES 2, 5) r(a) on ts.a = r.a",
                anyTree(
                        project(
                                ImmutableMap.of("expr", expression("coalesce(ts, r)")),
                                join(FULL, builder -> builder
                                        .equiCriteria("ts", "r")
                                        .left(
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("ts", expression("coalesce(t, s)")),
                                                                join(FULL, leftJoinBuilder -> leftJoinBuilder
                                                                        .equiCriteria("t", "s")
                                                                        .left(exchange(REMOTE, REPARTITION, anyTree(values(ImmutableList.of("t")))))
                                                                        .right(exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("s")))))))))
                                        .right(exchange(LOCAL, GATHER, anyTree(values(ImmutableList.of("r")))))))));
    }

    @Test
    @Disabled // TODO: re-enable once FULL join property derivations are re-introduced
    public void testArgumentsInDifferentOrder()
    {
        // ensure properties for full outer join are derived properly regardless of the order of arguments to coalesce, since they
        // are semantically equivalent

        assertDistributedPlan(
                "SELECT coalesce(l.a, r.a) " +
                        "FROM (VALUES 1, 2, 3) l(a) " +
                        "FULL OUTER JOIN (VALUES 1, 4) r(a) ON l.a = r.a " +
                        "GROUP BY 1",
                anyTree(
                        exchange(
                                LOCAL,
                                GATHER,
                                aggregation(
                                        ImmutableMap.of(),
                                        PARTIAL,
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("expr", expression("coalesce(l, r)")),
                                                        join(FULL, builder -> builder
                                                                .equiCriteria("l", "r")
                                                                .left(anyTree(values(ImmutableList.of("l"))))
                                                                .right(anyTree(values(ImmutableList.of("r")))))))))));

        assertDistributedPlan(
                "SELECT coalesce(r.a, l.a) " +
                        "FROM (VALUES 1, 2, 3) l(a) " +
                        "FULL OUTER JOIN (VALUES 1, 4) r(a) ON l.a = r.a " +
                        "GROUP BY 1",
                anyTree(
                        exchange(
                                LOCAL,
                                GATHER,
                                aggregation(
                                        ImmutableMap.of(),
                                        PARTIAL,
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("expr", expression("coalesce(r, l)")),
                                                        join(FULL, builder -> builder
                                                                .equiCriteria("l", "r")
                                                                .left(anyTree(values(ImmutableList.of("l"))))
                                                                .right(anyTree(values(ImmutableList.of("r")))))))))));
    }

    @Test
    public void testCoalesceWithManyArguments()
    {
        // ensure that properties are derived correctly when the arguments to coalesce are a
        // superset of the guarantees provided by full outer join

        assertDistributedPlan(
                "SELECT coalesce(l.a, m.a, r.a) " +
                        "FROM (VALUES 1, 2, 3) l(a) " +
                        "FULL OUTER JOIN (VALUES 1, 4) m(a) ON l.a = m.a " +
                        "FULL OUTER JOIN (VALUES 2, 5) r(a) ON l.a = r.a " +
                        "GROUP BY 1",
                anyTree(
                        exchange(
                                REMOTE,
                                REPARTITION,
                                aggregation(
                                        ImmutableMap.of(),
                                        PARTIAL,
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("expr", expression("coalesce(l, m, r)")),
                                                        join(FULL, builder -> builder
                                                                .equiCriteria("l", "r")
                                                                .left(
                                                                        anyTree(
                                                                                join(FULL, leftJoinBuilder -> leftJoinBuilder
                                                                                        .equiCriteria("l", "m")
                                                                                        .left(anyTree(values(ImmutableList.of("l"))))
                                                                                        .right(anyTree(values(ImmutableList.of("m")))))))
                                                                .right(anyTree(values(ImmutableList.of("r")))))))))));
    }

    @Test
    public void testComplexArgumentToCoalesce()
    {
        assertDistributedPlan(
                "SELECT coalesce(l.a, m.a + 1, r.a) " +
                        "FROM (VALUES 1, 2, 3) l(a) " +
                        "FULL OUTER JOIN (VALUES 1, 4) m(a) ON l.a = m.a " +
                        "FULL OUTER JOIN (VALUES 2, 5) r(a) ON l.a = r.a " +
                        "GROUP BY 1",
                anyTree(
                        exchange(
                                REMOTE,
                                REPARTITION,
                                aggregation(
                                        ImmutableMap.of(),
                                        PARTIAL,
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("expr", expression("coalesce(l, m + 1, r)")),
                                                        join(FULL, builder -> builder
                                                                .equiCriteria("l", "r")
                                                                .left(
                                                                        anyTree(
                                                                                join(FULL, leftJoinBuilder -> leftJoinBuilder
                                                                                        .equiCriteria("l", "m")
                                                                                        .left(anyTree(values(ImmutableList.of("l"))))
                                                                                        .right(anyTree(values(ImmutableList.of("m")))))))
                                                                .right(anyTree(values(ImmutableList.of("r")))))))))));
    }
}
