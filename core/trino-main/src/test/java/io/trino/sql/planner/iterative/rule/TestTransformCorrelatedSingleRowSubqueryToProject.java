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
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestTransformCorrelatedSingleRowSubqueryToProject
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void testRewrite()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p ->
                        p.correlatedJoin(
                                ImmutableList.of(p.symbol("l_nationkey")),
                                p.tableScan(
                                        tester().getCurrentCatalogTableHandle(TINY_SCHEMA_NAME, "nation"),
                                        ImmutableList.of(p.symbol("l_nationkey")),
                                        ImmutableMap.of(p.symbol("l_nationkey"), new TpchColumnHandle("nationkey",
                                                BIGINT))),
                                p.project(
                                        Assignments.of(p.symbol("l_expr2", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "l_nationkey"), new Constant(INTEGER, 1L)))),
                                        p.values(
                                                ImmutableList.of(),
                                                ImmutableList.of(
                                                        ImmutableList.of())))))
                .matches(project(
                        ImmutableMap.of(
                                "l_expr2", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "l_nationkey"), new Constant(INTEGER, 1L)))),
                                "l_nationkey", PlanMatchPattern.expression(new Reference(BIGINT, "l_nationkey"))),
                        tableScan("nation", ImmutableMap.of("l_nationkey", "nationkey"))));
    }

    @Test
    public void testDoesNotFireWithEmptyValuesNode()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p ->
                        p.correlatedJoin(
                                ImmutableList.of(p.symbol("a")),
                                p.values(p.symbol("a")),
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testCorrelatedValues()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(3, a),
                            p.values(ImmutableList.of(a), ImmutableList.of(ImmutableList.of(new Reference(BIGINT, "a")))));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(BIGINT, "a"))),
                                values(ImmutableList.of("a"), ImmutableList.of(
                                        ImmutableList.of(new Constant(BIGINT, null)),
                                        ImmutableList.of(new Constant(BIGINT, null)),
                                        ImmutableList.of(new Constant(BIGINT, null))))));

        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    Symbol c = p.symbol("c", BIGINT);
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(3, a, b),
                            p.values(ImmutableList.of(a, c), ImmutableList.of(ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)))));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new Reference(BIGINT, "a")),
                                        "b", PlanMatchPattern.expression(new Reference(BIGINT, "b")),
                                        "c", PlanMatchPattern.expression(new Constant(BIGINT, 1L))),
                                values(ImmutableList.of("a", "b"), ImmutableList.of(
                                        ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, null)),
                                        ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, null)),
                                        ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, null))))));
    }

    @Test
    public void testUncorrelatedValues()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.correlatedJoin(
                            ImmutableList.of(),
                            p.values(3, a),
                            p.values(1, b));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression(new Reference(BIGINT, "a")),
                                        "b", PlanMatchPattern.expression(new Constant(BIGINT, null))),
                                values(ImmutableList.of("a"), ImmutableList.of(
                                        ImmutableList.of(new Constant(BIGINT, null)),
                                        ImmutableList.of(new Constant(BIGINT, null)),
                                        ImmutableList.of(new Constant(BIGINT, null))))));
    }

    @Test
    public void testMultipleRowValues()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(3, a),
                            p.values(2, b));
                })
                .doesNotFire();
    }

    @Test
    public void testNonRowValues()
    {
        tester().assertThat(new TransformCorrelatedSingleRowSubqueryToProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.correlatedJoin(
                            ImmutableList.of(a),
                            p.values(3, a),
                            p.valuesOfExpressions(ImmutableList.of(p.symbol("b")), ImmutableList.of(new Cast(new Row(ImmutableList.of(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("true")))), rowType(field("col", BOOLEAN))))));
                })
                .doesNotFire();
    }
}
