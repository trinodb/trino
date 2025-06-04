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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.MERGE_PROJECT_WITH_VALUES;
import static io.trino.SystemSessionProperties.PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Logical.Operator.OR;
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
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestDereferencePushDown
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction IS_FINITE = FUNCTIONS.resolveFunction("is_finite", fromTypes(DOUBLE));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testDereferencePushdownMultiLevel()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))), ROW(CAST(ROW(3, 4.0) AS ROW(x BIGINT, y DOUBLE)))) " +
                        "SELECT a.msg.x, a.msg, b.msg.y FROM t a CROSS JOIN t b",
                output(ImmutableList.of("a_msg_x", "a_msg", "b_msg_y"),
                        strictProject(
                                ImmutableMap.of(
                                        "a_msg_x", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "a_msg"), 0)),
                                        "a_msg", expression(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "a_msg")),
                                        "b_msg_y", expression(new Reference(DOUBLE, "b_msg_y"))),
                                join(INNER, builder -> builder
                                        .left(values("a_msg"))
                                        .right(
                                                values(ImmutableList.of("b_msg_y"), ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 2e0)), ImmutableList.of(new Constant(DOUBLE, 4e0)))))))));
    }

    @Test
    public void testDereferencePushdownJoin()
    {
        // dereference pushdown + constant folding
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a, t b " +
                        "WHERE a.msg.y = b.msg.y",
                disablePushFilterIntoValues(),
                output(
                        project(
                                ImmutableMap.of("b_x", expression(new Reference(BIGINT, "b_x"))),
                                filter(
                                        new Comparison(EQUAL, new Reference(DOUBLE, "a_y"), new Reference(DOUBLE, "b_y")),
                                        values(
                                                ImmutableList.of("b_x", "b_y", "a_y"),
                                                ImmutableList.of(ImmutableList.of(
                                                        new Constant(BIGINT, 1L),
                                                        new Constant(DOUBLE, 2e0),
                                                        new Constant(DOUBLE, 2e0))))))));

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
                disablePushFilterIntoValues(),
                output(ImmutableList.of("b_x"),
                        join(INNER, builder -> builder
                                .left(
                                        project(filter(
                                                new Comparison(EQUAL, new Reference(DOUBLE, "a_y"), new Constant(DOUBLE, 2.0)),
                                                values(ImmutableList.of("a_y"), ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 2e0)))))))
                                .right(
                                        project(filter(
                                                new Comparison(EQUAL, new Reference(DOUBLE, "b_y"), new Constant(DOUBLE, 2.0)),
                                                values(
                                                        ImmutableList.of("b_y", "b_x"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 2.0), new Constant(BIGINT, 1L))))))))));
    }

    @Test
    public void testDereferencePushdownFilter()
    {
        // dereference pushdown + constant folding
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT a.msg.y, b.msg.x " +
                        "FROM t a CROSS JOIN t b " +
                        "WHERE a.msg.x = 7 OR IS_FINITE(b.msg.y)",
                disablePushFilterIntoValues(),
                any(
                        project(
                                ImmutableMap.of("a_y", expression(new Reference(DOUBLE, "a_y")), "b_x", expression(new Reference(BIGINT, "b_x"))),
                                filter(
                                        new Logical(OR, ImmutableList.of(
                                                new Comparison(EQUAL, new Reference(BIGINT, "a_x"), new Constant(BIGINT, 7L)),
                                                new Call(IS_FINITE, ImmutableList.of(new Reference(DOUBLE, "b_y"))))),
                                        values(
                                                ImmutableList.of("b_x", "b_y", "a_y", "a_x"),
                                                ImmutableList.of(ImmutableList.of(
                                                        new Constant(BIGINT, 1L),
                                                        new Constant(DOUBLE, 2.0),
                                                        new Constant(DOUBLE, 2.0),
                                                        new Constant(BIGINT, 1L))))))));
    }

    @Test
    public void testDereferencePushdownWindow()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT msg.x AS x, ROW_NUMBER() OVER (PARTITION BY msg.y) AS rn " +
                        "FROM t ",
                anyTree(
                        values(
                                ImmutableList.of("x", "y"),
                                ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 2.0), new Constant(BIGINT, 1L))))));

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
                Session.builder(this.getPlanTester().getDefaultSession())
                        .setSystemProperty(MERGE_PROJECT_WITH_VALUES, "false")
                        .build(),
                true,
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "msg1", expression(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "msg1")), // not pushed down because used in partition by
                                        "msg2", expression(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "msg2")), // not pushed down because used in order by
                                        "msg3", expression(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "msg3")), // not pushed down because used in window function
                                        "msg4_x", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "msg4"), 0)), // pushed down because msg4.x used in window function
                                        "msg5_x", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "msg5"), 0))), // pushed down because window node does not refer it
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
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                        .build(),
                anyTree(
                        semiJoin("a_x", "b_z", "semi_join_symbol",
                                project(
                                        ImmutableMap.of("a_y", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, DOUBLE, BIGINT), "msg"), 1))),
                                        values(ImmutableList.of("msg", "a_x"), ImmutableList.of())),
                                values(ImmutableList.of("b_z"), ImmutableList.of()))));
    }

    @Test
    public void testDereferencePushdownLimit()
    {
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))), ROW(CAST(ROW(3, 4.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT msg.x * 3  FROM t limit 1",
                anyTree(
                        strictProject(ImmutableMap.of("x_into_3", expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "msg_x"), new Constant(BIGINT, 3L))))),
                                limit(1,
                                        strictProject(ImmutableMap.of("msg_x", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, DOUBLE), "msg"), 0))),
                                                values("msg"))))));

        // dereference pushdown + constant folding
        assertPlan("WITH t(msg) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))" +
                        "SELECT b.msg.x " +
                        "FROM t a, t b " +
                        "WHERE a.msg.y = b.msg.y " +
                        "LIMIT 100",
                disablePushFilterIntoValues(),
                output(
                        limit(
                                100,
                                project(
                                        ImmutableMap.of("b_x", expression(new Reference(BIGINT, "b_x"))),
                                        filter(
                                                new Comparison(EQUAL, new Reference(DOUBLE, "a_y"), new Reference(DOUBLE, "b_y")),
                                                values(
                                                        ImmutableList.of("b_x", "b_y", "a_y"),
                                                        ImmutableList.of(ImmutableList.of(
                                                                new Constant(BIGINT, 1L),
                                                                new Constant(DOUBLE, 2e0),
                                                                new Constant(DOUBLE, 2e0)))))))));

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
                disablePushFilterIntoValues(),
                anyTree(
                        join(INNER, builder -> builder
                                .left(
                                        project(filter(
                                                new Comparison(EQUAL, new Reference(DOUBLE, "a_y"), new Constant(DOUBLE, 2.0)),
                                                values(ImmutableList.of("a_y"), ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 2e0)))))))
                                .right(
                                        project(filter(
                                                new Comparison(EQUAL, new Reference(DOUBLE, "b_y"), new Constant(DOUBLE, 2.0)),
                                                values(
                                                        ImmutableList.of("b_y", "b_x"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 2.0), new Constant(BIGINT, 1L))))))))));
    }

    @Test
    public void testDereferencePushdownUnnest()
    {
        assertPlan("WITH t(msg, array) AS (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)), ARRAY[1, 2, 3])) " +
                        "SELECT a.msg.x " +
                        "FROM t a JOIN t b ON a.msg.y = b.msg.y " +
                        "CROSS JOIN UNNEST (a.array) " +
                        "WHERE a.msg.x + b.msg.x < BIGINT '10'",
                disablePushFilterIntoValues(),
                output(ImmutableList.of("expr"),
                        strictProject(ImmutableMap.of("expr", expression(new Reference(BIGINT, "a_x"))),
                                unnest(
                                        join(INNER, builder -> builder
                                                .left(
                                                        project(
                                                                filter(
                                                                        new Comparison(EQUAL, new Reference(DOUBLE, "a_y"), new Constant(DOUBLE, 2.0)),
                                                                        values("array", "a_y", "a_x"))))
                                                .right(
                                                        project(
                                                                filter(
                                                                        new Comparison(EQUAL, new Reference(DOUBLE, "b_y"), new Constant(DOUBLE, 2.0)),
                                                                        values(ImmutableList.of("b_y"), ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 2e0))))))))))));
    }

    private Session disablePushFilterIntoValues()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT, "0")
                .build();
    }
}
