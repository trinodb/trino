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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestInlineProjections
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction ADD_DECIMAL_8_4 = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(createDecimalType(8, 4), createDecimalType(8, 4)));
    private static final ResolvedFunction MULTIPLY_DECIMAL_8_4 = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(createDecimalType(8, 4), createDecimalType(8, 4)));
    private static final RowType MSG_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), INTEGER), new RowType.Field(Optional.of("y"), INTEGER)));

    @Test
    public void test()
    {
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("identity", INTEGER), new Reference(INTEGER, "symbol")) // identity
                                        .put(p.symbol("multi_complex_1", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "complex"), new Constant(INTEGER, 1L)))) // complex expression referenced multiple times
                                        .put(p.symbol("multi_complex_2", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "complex"), new Constant(INTEGER, 2L)))) // complex expression referenced multiple times
                                        .put(p.symbol("multi_literal_1", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "literal"), new Constant(INTEGER, 1L)))) // literal referenced multiple times
                                        .put(p.symbol("multi_literal_2", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "literal"), new Constant(INTEGER, 2L)))) // literal referenced multiple times
                                        .put(p.symbol("single_complex", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "complex_2"), new Constant(INTEGER, 2L)))) // complex expression reference only once
                                        .put(p.symbol("msg_xx", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "z"), new Constant(INTEGER, 1L))))
                                        .put(p.symbol("multi_symbol_reference", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "v"), new Reference(INTEGER, "v"))))
                                        .build(),
                                p.project(Assignments.builder()
                                                .put(p.symbol("symbol", INTEGER), new Reference(INTEGER, "x"))
                                                .put(p.symbol("complex", INTEGER), new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 2L))))
                                                .put(p.symbol("literal", INTEGER), new Constant(INTEGER, 1L))
                                                .put(p.symbol("complex_2", INTEGER), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 1L))))
                                                .put(p.symbol("z", MSG_TYPE.getFields().get(0).getType()), new FieldReference(new Reference(MSG_TYPE, "msg"), 0))
                                                .put(p.symbol("v", INTEGER), new Reference(INTEGER, "x"))
                                                .build(),
                                        p.values(p.symbol("x", INTEGER), p.symbol("msg", MSG_TYPE)))))
                .matches(
                        project(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("out1", PlanMatchPattern.expression(new Reference(INTEGER, "x")))
                                        .put("out2", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "y"), new Constant(INTEGER, 1L)))))
                                        .put("out3", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "y"), new Constant(INTEGER, 2L)))))
                                        .put("out4", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 1L)))))
                                        .put("out5", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L)))))
                                        .put("out6", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 1L))), new Constant(INTEGER, 2L)))))
                                        .put("out8", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "z"), new Constant(INTEGER, 1L)))))
                                        .put("out10", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Reference(INTEGER, "x")))))
                                        .buildOrThrow(),
                                project(
                                        ImmutableMap.of(
                                                "x", PlanMatchPattern.expression(new Reference(INTEGER, "x")),
                                                "y", PlanMatchPattern.expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 2L)))),
                                                "z", PlanMatchPattern.expression(new FieldReference(new Reference(MSG_TYPE, "msg"), 0))),
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
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        // Use the literal-like expression multiple times. Single-use expression may be inlined regardless of whether it's a literal
                                        .put(p.symbol("decimal_multiplication", createDecimalType(16, 8)), new Call(MULTIPLY_DECIMAL_8_4, ImmutableList.of(new Reference(createDecimalType(8, 4), "decimal_literal"), new Reference(createDecimalType(8, 4), "decimal_literal"))))
                                        .put(p.symbol("decimal_addition", createDecimalType(9, 4)), new Call(ADD_DECIMAL_8_4, ImmutableList.of(new Reference(createDecimalType(8, 4), "decimal_literal"), new Reference(createDecimalType(8, 4), "decimal_literal"))))
                                        .build(),
                                p.project(Assignments.builder()
                                                .put(p.symbol("decimal_literal", createDecimalType(8, 4)), new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))))
                                                .build(),
                                        p.values(p.symbol("x")))))
                .matches(
                        project(
                                Map.of(
                                        "decimal_multiplication", PlanMatchPattern.expression(new Call(MULTIPLY_DECIMAL_8_4, ImmutableList.of(new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))), new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5")))))),
                                        "decimal_addition", PlanMatchPattern.expression(new Call(ADD_DECIMAL_8_4, ImmutableList.of(new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))), new Constant(createDecimalType(8, 4), Decimals.valueOfShort(new BigDecimal("12.5"))))))),
                                values(Map.of("x", 0))));
    }

    @Test
    public void testEliminatesIdentityProjection()
    {
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("single_complex", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "complex"), new Constant(INTEGER, 2L)))) // complex expression referenced only once
                                        .build(),
                                p.project(Assignments.builder()
                                                .put(p.symbol("complex", INTEGER), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 1L))))
                                                .build(),
                                        p.values(p.symbol("x", INTEGER)))))
                .matches(
                        project(
                                ImmutableMap.of("out1", PlanMatchPattern.expression(new Call(ADD_INTEGER, ImmutableList.of(new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 1L))), new Constant(INTEGER, 2L))))),
                                values("x")));
    }

    @Test
    public void testIdentityProjections()
    {
        // projection renaming symbol
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("output"), new Reference(BIGINT, "value")),
                                p.project(
                                        Assignments.identity(p.symbol("value")),
                                        p.values(p.symbol("value")))))
                .doesNotFire();

        // identity projection
        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.identity(p.symbol("x")),
                                p.project(
                                        Assignments.identity(p.symbol("x"), p.symbol("y")),
                                        p.values(p.symbol("x"), p.symbol("y")))))
                .matches(
                        project(
                                ImmutableMap.of("x", PlanMatchPattern.expression(new Reference(BIGINT, "x"))),
                                values(ImmutableMap.of("x", 0, "y", 1))));
    }

    @Test
    public void testSubqueryProjections()
    {
        tester().assertThat(new InlineProjections())
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

        tester().assertThat(new InlineProjections())
                .on(p ->
                        p.project(
                                Assignments.identity(p.symbol("fromOuterScope"), p.symbol("value_1", INTEGER)),
                                p.project(
                                        Assignments.of(p.symbol("value_1", INTEGER), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "value"), new Constant(INTEGER, 1L)))),
                                        p.values(p.symbol("value")))))
                .matches(
                        project(
                                // cannot test outer scope symbol. projections were squashed, and the resulting assignments are:
                                // ImmutableMap.of("fromOuterScope", PlanMatchPattern.expression("fromOuterScope"), "value_1", PlanMatchPattern.expression("value - 1")),
                                values(ImmutableMap.of("value", 0))));
    }
}
