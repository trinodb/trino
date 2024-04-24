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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.WindowNode;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.type.UnknownType.UNKNOWN;

public class TestWindowClause
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction ADD_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction NEGATION_INTEGER = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(INTEGER));

    @Test
    public void testPreprojectExpression()
    {
        @Language("SQL") String sql = "SELECT max(b) OVER w FROM (VALUES (1, 1)) t(a, b) WINDOW w AS (PARTITION BY a + 1)";
        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of("expr"),
                                                ImmutableList.of(),
                                                ImmutableMap.of()))
                                        .addFunction(
                                                "max_result",
                                                windowFunction("max", ImmutableList.of("b"), DEFAULT_FRAME)),
                                anyTree(project(
                                        ImmutableMap.of("expr", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))))),
                                        anyTree(values("a", "b"))))));

        assertPlan(sql, CREATED, pattern);
    }

    @Test
    public void testPreprojectExpressions()
    {
        @Language("SQL") String sql = "SELECT max(b) OVER w3 FROM (VALUES (1, 1, 1)) t(a, b, c) WINDOW w1 AS (PARTITION BY a + 1), w2 AS (w1 ORDER BY b + 2), w3 AS (w2 RANGE c + 3 PRECEDING)";
        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of("expr_a"),
                                                ImmutableList.of("expr_b"),
                                                ImmutableMap.of("expr_b", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "max_result",
                                                windowFunction(
                                                        "max",
                                                        ImmutableList.of("b"),
                                                        new WindowNode.Frame(
                                                                RANGE,
                                                                PRECEDING,
                                                                Optional.of(new Symbol(UNKNOWN, "frame_start")),
                                                                Optional.of(new Symbol(UNKNOWN, "expr_b")),
                                                                CURRENT_ROW,
                                                                Optional.empty(),
                                                                Optional.empty()))),
                                project(
                                        ImmutableMap.of("frame_start", expression(new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "expr_b"), new Reference(INTEGER, "expr_c"))))),
                                        anyTree(project(
                                                ImmutableMap.of(
                                                        "expr_a", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)))),
                                                        "expr_b", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 2L)))),
                                                        "expr_c", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "c"), new Constant(INTEGER, 3L))))),
                                                anyTree(values("a", "b", "c")))))));

        assertPlan(sql, CREATED, pattern);
    }

    @Test
    public void testWindowFunctionsInSelectAndOrderBy()
    {
        @Language("SQL") String sql = "SELECT array_agg(a) OVER (w ORDER BY a + 1), -a a FROM (VALUES 1, 2, 3) t(a) WINDOW w AS () ORDER BY max(a) OVER (w ORDER BY a + 1)";
        PlanMatchPattern pattern =
                anyTree(sort(
                        ImmutableList.of(sort("max_result", ASCENDING, LAST)),
                        any(window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("order_by_window_sortkey"),
                                                ImmutableMap.of("order_by_window_sortkey", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "max_result",
                                                windowFunction("max", ImmutableList.of("minus_a"), DEFAULT_FRAME)),
                                any(project(
                                        ImmutableMap.of("order_by_window_sortkey", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "minus_a"), new Constant(INTEGER, 1L))))),
                                        project(
                                                ImmutableMap.of("minus_a", expression(new Call(NEGATION_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"))))),
                                                window(
                                                        windowMatcherBuilder -> windowMatcherBuilder
                                                                .specification(specification(
                                                                        ImmutableList.of(),
                                                                        ImmutableList.of("select_window_sortkey"),
                                                                        ImmutableMap.of("select_window_sortkey", SortOrder.ASC_NULLS_LAST)))
                                                                .addFunction(
                                                                        "array_agg_result",
                                                                        windowFunction("array_agg", ImmutableList.of("a"), DEFAULT_FRAME)),
                                                        anyTree(project(
                                                                ImmutableMap.of("select_window_sortkey", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))))),
                                                                anyTree(values("a"))))))))))));

        assertPlan(sql, CREATED, pattern);
    }

    @Test
    public void testWindowWithFrameCoercions()
    {
        @Language("SQL") String sql = "SELECT a old_a, 2e0 a FROM (VALUES -100, -99, -98) t(a) WINDOW w AS (ORDER BY a + 1) ORDER BY count(*) OVER (w RANGE BETWEEN CURRENT ROW AND a + 1e0 FOLLOWING)";
        PlanMatchPattern pattern =
                anyTree(sort(// sort by window function result
                        ImmutableList.of(sort("count_result", ASCENDING, LAST)),
                        project(window(// window function in ORDER BY
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("sortkey"),
                                                ImmutableMap.of("sortkey", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "count_result",
                                                windowFunction(
                                                        "count",
                                                        ImmutableList.of(),
                                                        new WindowNode.Frame(
                                                                RANGE,
                                                                CURRENT_ROW,
                                                                Optional.empty(),
                                                                Optional.empty(),
                                                                FOLLOWING,
                                                                Optional.of(new Symbol(UNKNOWN, "frame_bound")),
                                                                Optional.of(new Symbol(UNKNOWN, "coerced_sortkey"))))),
                                project(// frame bound value computation
                                        ImmutableMap.of("frame_bound", expression(new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "coerced_sortkey"), new Reference(DOUBLE, "frame_offset"))))),
                                        project(// sort key coercion to frame bound type
                                                ImmutableMap.of("coerced_sortkey", expression(new Cast(new Reference(INTEGER, "sortkey"), DOUBLE))),
                                                node(FilterNode.class,
                                                        project(project(
                                                                ImmutableMap.of(
                                                                        // sort key based on "a" in source scope
                                                                        "sortkey", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)))),
                                                                        // frame offset based on "a" in output scope
                                                                        "frame_offset", expression(new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "new_a"), new Constant(DOUBLE, 1.0))))),
                                                                project(// output expression
                                                                        ImmutableMap.of("new_a", expression(new Constant(DOUBLE, 2E0))),
                                                                        project(project(values("a")))))))))))));

        assertPlan(sql, CREATED, pattern);
    }
}
