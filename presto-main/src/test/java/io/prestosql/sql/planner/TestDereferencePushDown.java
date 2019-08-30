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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;

public class TestDereferencePushDown
        extends BasePlanTest
{
    @Test
    public void testDereferencePushdownJoin()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a, t b " +
                        "WHERE a.msg.y = b.msg.y",
                output(ImmutableList.of("b_x"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y")),
                                                values("msg"))),
                                anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                values("msg"))))));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT a.msg.y " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x > bigint '5'",
                output(ImmutableList.of("a_y"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y")),
                                                filter("msg.x > bigint '5'",
                                                        values("msg")))),
                                anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y")),
                                                values("msg"))))));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x + b.msg.x < BIGINT '10'",
                output(ImmutableList.of("b_x"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")), Optional.of("a_x + b_x < bigint '10'"),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x")),
                                                values("msg"))),
                                anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                values("msg"))))));
    }

    @Test
    public void testDereferencePushdownFilter()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT a.msg.y, b.msg.x " +
                        "FROM t a CROSS JOIN t b " +
                        "WHERE a.msg.x = 7 OR IS_FINITE(b.msg.y)",
                anyTree(
                        join(INNER, ImmutableList.of(),
                                project(ImmutableMap.of("a_x", expression("msg.x"), "a_y", expression("msg.y")),
                                        values("msg")),
                                project(ImmutableMap.of("b_x", expression("msg.x"), "b_y", expression("msg.y")),
                                        values("msg")))));
    }

    @Test
    public void testDereferencePushdownWindow()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT msg.x AS x, ROW_NUMBER() OVER (PARTITION BY msg.y ORDER BY msg.y) AS rn " +
                        "FROM t ",
                anyTree(
                        project(ImmutableMap.of("a_x", expression("msg.x"), "a_y", expression("msg.y")),
                                values("msg"))));
    }

    @Test
    public void testDereferencePushdownSemiJoin()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0, 3) AS ROW(x BIGINT, y DOUBLE, z BIGINT)))) " +
                        "SELECT msg.y " +
                        "FROM t " +
                        "WHERE " +
                        "msg.x IN (SELECT msg.z FROM t)",
                anyTree(
                        semiJoin("a_x", "b_z", "semi_join_symbol",
                                anyTree(
                                        project(ImmutableMap.of("a_x", expression("msg.x"), "a_y", expression("msg.y")),
                                                values("msg"))),
                                anyTree(
                                        project(ImmutableMap.of("b_z", expression("msg.z")),
                                                values("msg"))))));
    }

    @Test
    public void testDereferencePushdownLimit()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a, t b " +
                        "WHERE a.msg.y = b.msg.y " +
                        "LIMIT 100",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                        anyTree(
                                project(ImmutableMap.of("a_y", expression("msg.y")),
                                        values("msg"))),
                        anyTree(
                                project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                        values("msg"))))));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT a.msg.y " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x > BIGINT '5' " +
                        "LIMIT 100",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                        anyTree(
                                project(ImmutableMap.of("a_y", expression("msg.y")),
                                        filter("msg.x > bigint '5'",
                                                values("msg")))),
                        anyTree(
                                project(ImmutableMap.of("b_y", expression("msg.y")),
                                        values("msg"))))));

        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "WHERE a.msg.x + b.msg.x < BIGINT '10' " +
                        "LIMIT 100",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")), Optional.of("a_x + b_x < bigint '10'"),
                        anyTree(
                                project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x")),
                                        values("msg"))),
                        anyTree(
                                project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                        values("msg"))))));
    }

    @Test
    public void testDereferencePushdownUnnest()
    {
        assertPlan("WITH t(msg, array) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)), ARRAY[1, 2, 3]))) " +
                        "SELECT a.msg.x " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "CROSS JOIN UNNEST (a.array) " +
                        "WHERE a.msg.x + b.msg.x < BIGINT '10'",
                output(ImmutableList.of("expr"),
                        project(ImmutableMap.of("expr", expression("a_x")),
                                unnest(
                                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                                Optional.of("a_x + b_x < bigint '10'"),
                                                anyTree(
                                                        project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x"), "a_z", expression("array")),
                                                                values("msg", "array"))),
                                                anyTree(
                                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                                values("msg"))))))));
    }
}
