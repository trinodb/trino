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
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.SubscriptExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestInlineProjections
        extends BaseRuleTest
{
    private static final RowType MSG_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), VARCHAR), new RowType.Field(Optional.of("y"), VARCHAR)));

    @Test
    public void test()
    {
        tester().assertThat(new InlineProjections(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("identity"), new SymbolReference("symbol")) // identity
                                        .put(p.symbol("multi_complex_1"), new ArithmeticBinaryExpression(ADD, new SymbolReference("complex"), new Constant(INTEGER, 1L))) // complex expression referenced multiple times
                                        .put(p.symbol("multi_complex_2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("complex"), new Constant(INTEGER, 2L))) // complex expression referenced multiple times
                                        .put(p.symbol("multi_literal_1"), new ArithmeticBinaryExpression(ADD, new SymbolReference("literal"), new Constant(INTEGER, 1L))) // literal referenced multiple times
                                        .put(p.symbol("multi_literal_2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("literal"), new Constant(INTEGER, 2L))) // literal referenced multiple times
                                        .put(p.symbol("single_complex"), new ArithmeticBinaryExpression(ADD, new SymbolReference("complex_2"), new Constant(INTEGER, 2L))) // complex expression reference only once
                                        .put(p.symbol("msg_xx"), new ArithmeticBinaryExpression(ADD, new SymbolReference("z"), new Constant(INTEGER, 1L)))
                                        .put(p.symbol("multi_symbol_reference"), new ArithmeticBinaryExpression(ADD, new SymbolReference("v"), new SymbolReference("v")))
                                        .build(),
                                p.project(Assignments.builder()
                                                .put(p.symbol("symbol"), new SymbolReference("x"))
                                                .put(p.symbol("complex"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("x"), new Constant(INTEGER, 2L)))
                                                .put(p.symbol("literal"), new Constant(INTEGER, 1L))
                                                .put(p.symbol("complex_2"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("x"), new Constant(INTEGER, 1L)))
                                                .put(p.symbol("z"), new SubscriptExpression(new SymbolReference("msg"), new Constant(INTEGER, 1L)))
                                                .put(p.symbol("v"), new SymbolReference("x"))
                                                .build(),
                                        p.values(p.symbol("x"), p.symbol("msg", MSG_TYPE)))))
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression(new SymbolReference("x")))
                                        .put("out2", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("y"), new Constant(INTEGER, 1L))))
                                        .put("out3", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("y"), new Constant(INTEGER, 2L))))
                                        .put("out4", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new Constant(INTEGER, 1L), new Constant(INTEGER, 1L))))
                                        .put("out5", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new Constant(INTEGER, 1L), new Constant(INTEGER, 2L))))
                                        .put("out6", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("x"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 2L))))
                                        .put("out8", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("z"), new Constant(INTEGER, 1L))))
                                        .put("out10", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("x"), new SymbolReference("x"))))
                                        .buildOrThrow(),
                                project(
                                        ImmutableMap.of(
                                                "x", PlanMatchPattern.expression(new SymbolReference("x")),
                                                "y", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("x"), new Constant(INTEGER, 2L))),
                                                "z", PlanMatchPattern.expression(new SubscriptExpression(new SymbolReference("msg"), new Constant(INTEGER, 1L)))),
                                        values(ImmutableMap.of("x", 0, "msg", 1)))));
    }

    /**
     * Verify that non-{@link Literal} but literal-like constant expression gets inlined.
     *
     * @implNote The test uses decimals, as decimals values do not have direct literal form (see {@link LiteralEncoder}).
     */
    @Test
    public void testInlineEffectivelyLiteral()
    {
        tester().assertThat(new InlineProjections(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        // Use the literal-like expression multiple times. Single-use expression may be inlined regardless of whether it's a literal
                                        .put(p.symbol("decimal_multiplication"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("decimal_literal"), new SymbolReference("decimal_literal")))
                                        .put(p.symbol("decimal_addition"), new ArithmeticBinaryExpression(ADD, new SymbolReference("decimal_literal"), new SymbolReference("decimal_literal")))
                                        .build(),
                                p.project(Assignments.builder()
                                                .put(p.symbol("decimal_literal", createDecimalType(8, 4)), new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))))
                                                .build(),
                                        p.values(p.symbol("x")))))
                .matches(
                        project(
                                Map.of(
                                        "decimal_multiplication", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))), new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))))),
                                        "decimal_addition", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))), new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5")))))),
                                values(Map.of("x", 0))));
    }

    @Test
    public void testEliminatesIdentityProjection()
    {
        tester().assertThat(new InlineProjections(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("single_complex"), new ArithmeticBinaryExpression(ADD, new SymbolReference("complex"), new Constant(INTEGER, 2L))) // complex expression referenced only once
                                        .build(),
                                p.project(Assignments.builder()
                                                .put(p.symbol("complex"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("x"), new Constant(INTEGER, 1L)))
                                                .build(),
                                        p.values(p.symbol("x")))))
                .matches(
                        project(
                                ImmutableMap.of("out1", PlanMatchPattern.expression(new ArithmeticBinaryExpression(ADD, new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("x"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 2L)))),
                                values(ImmutableMap.of("x", 0))));
    }

    @Test
    public void testIdentityProjections()
    {
        // projection renaming symbol
        tester().assertThat(new InlineProjections(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("output"), new SymbolReference("value")),
                                p.project(
                                        Assignments.identity(p.symbol("value")),
                                        p.values(p.symbol("value")))))
                .doesNotFire();

        // identity projection
        tester().assertThat(new InlineProjections(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.identity(p.symbol("x")),
                                p.project(
                                        Assignments.identity(p.symbol("x"), p.symbol("y")),
                                        p.values(p.symbol("x"), p.symbol("y")))))
                .matches(
                        project(
                                ImmutableMap.of("x", PlanMatchPattern.expression(new SymbolReference("x"))),
                                values(ImmutableMap.of("x", 0, "y", 1))));
    }

    @Test
    public void testSubqueryProjections()
    {
        tester().assertThat(new InlineProjections(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.identity(p.symbol("fromOuterScope"), p.symbol("value")),
                                p.project(
                                        Assignments.identity(p.symbol("value")),
                                        p.values(p.symbol("value")))))
                .matches(
                        project(
                                // cannot test outer scope symbol. projections were squashed, and the resulting assignments are:
                                // ImmutableMap.of("fromOuterScope", PlanMatchPattern.expression("fromOuterScope"), "value", PlanMatchPattern.expression("value")),
                                values(ImmutableMap.of("value", 0))));

        tester().assertThat(new InlineProjections(tester().getTypeAnalyzer()))
                .on(p ->
                        p.project(
                                Assignments.identity(p.symbol("fromOuterScope"), p.symbol("value_1")),
                                p.project(
                                        Assignments.of(p.symbol("value_1"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("value"), new Constant(INTEGER, 1L))),
                                        p.values(p.symbol("value")))))
                .matches(
                        project(
                                // cannot test outer scope symbol. projections were squashed, and the resulting assignments are:
                                // ImmutableMap.of("fromOuterScope", PlanMatchPattern.expression("fromOuterScope"), "value_1", PlanMatchPattern.expression("value - 1")),
                                values(ImmutableMap.of("value", 0))));
    }
}
