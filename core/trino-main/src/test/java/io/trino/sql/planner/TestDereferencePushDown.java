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
import io.trino.Session;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.GenericLiteral;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.MERGE_PROJECT_WITH_VALUES;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

public class TestDereferencePushDown
        extends BasePlanTest
{
    @Test
    public void testDereferencePushdownMultiLevel()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))), ROW(CAST(ROW(3, 4.0) AS ROW(x BIGINT, y DOUBLE)))) " +
                        "SELECT a.msg.x, a.msg, b.msg.y FROM t a CROSS JOIN t b",
                output(ImmutableList.of("a_msg_x", "a_msg", "b_msg_y"),
                        strictProject(
                                ImmutableMap.of(
                                        "a_msg_x", PlanMatchPattern.expression("a_msg[1]"),
                                        "a_msg", PlanMatchPattern.expression("a_msg"),
                                        "b_msg_y", PlanMatchPattern.expression("b_msg_y")),
                                join(INNER, builder -> builder
                                        .left(values("a_msg"))
                                        .right(
                                                values(ImmutableList.of("b_msg_y"), ImmutableList.of(ImmutableList.of(new DoubleLiteral("2e0")), ImmutableList.of(new DoubleLiteral("4e0")))))))));
    }

    @Test
    public void testDereferencePushdownJoin()
    {
        // dereference pushdown + constant folding
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a, t b " +
                        "WHERE a.msg.y = b.msg.y",
                output(
                        project(
                                ImmutableMap.of("b_x", expression("b_x")),
                                filter(
                                        "a_y = b_y",
                                        values(
                                                ImmutableList.of("b_x", "b_y", "a_y"),
                                                ImmutableList.of(ImmutableList.of(
                                                        new GenericLiteral("BIGINT", "1"),
                                                        new DoubleLiteral("2e0"),
                                                        new DoubleLiteral("2e0"))))))));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT a.msg.y " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x > BIGINT '5'",
                output(ImmutableList.of("a_y"),
                        values("a_y")));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x + b.msg.x < BIGINT '10'",
                output(ImmutableList.of("b_x"),
                        join(INNER, builder -> builder
                                .left(
                                        project(filter(
                                                "a_y = 2e0",
                                                values(ImmutableList.of("a_y"), ImmutableList.of(ImmutableList.of(new DoubleLiteral("2e0")))))))
                                .right(
                                        project(filter(
                                                "b_y = 2e0",
                                                values(
                                                        ImmutableList.of("b_x", "b_y"),
                                                        ImmutableList.of(ImmutableList.of(new GenericLiteral("BIGINT", "1"), new DoubleLiteral("2e0"))))))))));
    }

    @Test
    public void testDereferencePushdownFilter()
    {
        // dereference pushdown + constant folding
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT a.msg.y, b.msg.x " +
                        "FROM t a CROSS JOIN t b " +
                        "WHERE a.msg.x = 7 OR IS_FINITE(b.msg.y)",
                any(
                        project(
                                ImmutableMap.of("a_y", expression("a_y"), "b_x", expression("b_x")),
                                filter(
                                        "((a_x = BIGINT '7') OR is_finite(b_y))",
                                        values(
                                                ImmutableList.of("b_x", "b_y", "a_x", "a_y"),
                                                ImmutableList.of(ImmutableList.of(
                                                        new GenericLiteral("BIGINT", "1"),
                                                        new DoubleLiteral("2e0"),
                                                        new GenericLiteral("BIGINT", "1"),
                                                        new DoubleLiteral("2e0"))))))));
    }

    @Test
    public void testDereferencePushdownWindow()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT msg.x AS x, ROW_NUMBER() OVER (PARTITION BY msg.y) AS rn " +
                        "FROM t ",
                anyTree(
                        project(values(
                                ImmutableList.of("x", "y"),
                                ImmutableList.of(ImmutableList.of(new GenericLiteral("BIGINT", "1"), new DoubleLiteral("2e0")))))));

        assertPlanWithSession(
                "WITH t(msg1, msg2, msg3, msg4, msg5) AS (VALUES " +
                        // Use two rows to avoid any optimizations around short-circuting operations
                        "ROW(" +
                        "   CAST(ROW(1, 0.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(2, 0.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(3, 0.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(4, 0.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(5, 0.0) AS ROW(x BIGINT, y DOUBLE)))," +
                        "ROW(" +
                        "   CAST(ROW(1, 1.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(2, 2.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(3, 3.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(4, 4.0) AS ROW(x BIGINT, y DOUBLE))," +
                        "   CAST(ROW(5, 5.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT " +
                        "   msg1.x AS x1, " +
                        "   msg2.x AS x2, " +
                        "   msg3.x AS x3, " +
                        "   msg4.x AS x4, " +
                        "   msg5.x AS x5, " +
                        "   MIN(msg3) OVER (PARTITION BY msg1 ORDER BY msg2) AS msg6," +
                        "   MIN(msg4.x) OVER (PARTITION BY msg1 ORDER BY msg2) AS bigint_msg4 " +
                        "FROM t",
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(MERGE_PROJECT_WITH_VALUES, "false")
                        .build(),
                true,
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "msg1", expression("msg1"), // not pushed down because used in partition by
                                        "msg2", expression("msg2"), // not pushed down because used in order by
                                        "msg3", expression("msg3"), // not pushed down because used in window function
                                        "msg4_x", expression("msg4[1]"), // pushed down because msg4.x used in window function
                                        "msg5_x", expression("msg5[1]")), // pushed down because window node does not refer it
                                values("msg1", "msg2", "msg3", "msg4", "msg5"))));
    }

    @Test
    public void testDereferencePushdownSemiJoin()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0, 3) AS ROW(x BIGINT, y DOUBLE, z BIGINT)))) " +
                        "SELECT msg.y " +
                        "FROM t " +
                        "WHERE " +
                        "msg.x IN (SELECT msg.z FROM t)",
                Session.builder(getQueryRunner().getDefaultSession())
                        .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                        .build(),
                anyTree(
                        semiJoin("a_x", "b_z", "semi_join_symbol",
                                project(
                                        ImmutableMap.of("a_y", expression("msg[2]")),
                                        values(ImmutableList.of("msg", "a_x"), ImmutableList.of())),
                                project(values(ImmutableList.of("b_z"), ImmutableList.of())))));
    }

    @Test
    public void testDereferencePushdownLimit()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))), ROW(CAST(ROW(3, 4.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT msg.x * 3  FROM t limit 1",
                anyTree(
                        strictProject(ImmutableMap.of("x_into_3", expression("msg_x * BIGINT '3'")),
                                limit(1,
                                        strictProject(ImmutableMap.of("msg_x", expression("msg[1]")),
                                                values("msg"))))));

        // dereference pushdown + constant folding
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a, t b " +
                        "WHERE a.msg.y = b.msg.y " +
                        "LIMIT 100",
                output(
                        limit(
                                100,
                                project(
                                        ImmutableMap.of("b_x", expression("b_x")),
                                        filter(
                                                "a_y = b_y",
                                                values(
                                                        ImmutableList.of("b_x", "b_y", "a_y"),
                                                        ImmutableList.of(ImmutableList.of(
                                                                new GenericLiteral("BIGINT", "1"),
                                                                new DoubleLiteral("2e0"),
                                                                new DoubleLiteral("2e0")))))))));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT a.msg.y " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x > BIGINT '5' " +
                        "LIMIT 100",
                anyTree(limit(100, values("a_y"))));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x + b.msg.x < BIGINT '10' " +
                        "LIMIT 100",
                anyTree(
                        join(INNER, builder -> builder
                                .left(
                                        project(filter(
                                                "a_y = 2e0",
                                                values(ImmutableList.of("a_y"), ImmutableList.of(ImmutableList.of(new DoubleLiteral("2e0")))))))
                                .right(
                                        project(filter(
                                                "b_y = 2e0",
                                                values(
                                                        ImmutableList.of("b_x", "b_y"),
                                                        ImmutableList.of(ImmutableList.of(new GenericLiteral("BIGINT", "1"), new DoubleLiteral("2e0"))))))))));
    }

    @Test
    public void testDereferencePushdownUnnest()
    {
        assertPlan("WITH t(msg, array) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)), ARRAY[1, 2, 3])) " +
                        "SELECT a.msg.x " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "CROSS JOIN UNNEST (a.array) " +
                        "WHERE a.msg.x + b.msg.x < BIGINT '10'",
                output(ImmutableList.of("expr"),
                        strictProject(ImmutableMap.of("expr", expression("a_x")),
                                unnest(
                                        join(INNER, builder -> builder
                                                .left(
                                                        project(
                                                                filter(
                                                                        "a_y = 2e0",
                                                                        values("array", "a_x", "a_y"))))
                                                .right(
                                                        project(
                                                                filter(
                                                                        "b_y = 2e0",
                                                                        values(ImmutableList.of("b_y"), ImmutableList.of(ImmutableList.of(new DoubleLiteral("2e0"))))))))))));
    }
}
