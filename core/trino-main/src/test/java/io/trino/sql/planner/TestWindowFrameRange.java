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
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Decimals;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.WindowNode;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;

public class TestWindowFrameRange
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction FAIL = FUNCTIONS.resolveFunction("fail", fromTypes(INTEGER, VARCHAR));
    private static final ResolvedFunction ADD_DECIMAL_10_0 = createTestMetadataManager().resolveOperator(OperatorType.ADD, ImmutableList.of(createDecimalType(10, 0), createDecimalType(10, 0)));
    private static final ResolvedFunction SUBTRACT_DECIMAL_10_0 = createTestMetadataManager().resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(createDecimalType(10, 0), createDecimalType(10, 0)));
    private static final ResolvedFunction ADD_INTEGER = createTestMetadataManager().resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = createTestMetadataManager().resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testFramePrecedingWithSortKeyCoercions()
    {
        @Language("SQL") String sql = "SELECT array_agg(key) OVER(ORDER BY key RANGE x PRECEDING) " +
                "FROM (VALUES (1, 1.1), (2, 2.2)) t(key, x)";

        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("key"),
                                                ImmutableMap.of("key", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "array_agg_result",
                                                windowFunction(
                                                        "array_agg",
                                                        ImmutableList.of("key"),
                                                        new WindowNode.Frame(
                                                                RANGE,
                                                                PRECEDING,
                                                                Optional.of(new Symbol("frame_start_value")),
                                                                Optional.of(new Symbol("key_for_frame_start_comparison")),
                                                                CURRENT_ROW,
                                                                Optional.empty(),
                                                                Optional.empty()))),
                                project(// coerce sort key to compare sort key values with frame start values
                                        ImmutableMap.of("key_for_frame_start_comparison", expression(new Cast(new SymbolReference("key"), createDecimalType(12, 1)))),
                                        project(// calculate frame start value (sort key - frame offset)
                                                ImmutableMap.of("frame_start_value", expression(new FunctionCall(SUBTRACT_DECIMAL_10_0, ImmutableList.of(new SymbolReference("key_for_frame_start_calculation"), new SymbolReference("x"))))),
                                                project(// coerce sort key to calculate frame start values
                                                        ImmutableMap.of("key_for_frame_start_calculation", expression(new Cast(new SymbolReference("key"), createDecimalType(10, 0)))),
                                                        filter(// validate offset values
                                                                ifExpression(
                                                                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("x"), new Constant(createDecimalType(2, 1), 0L)),
                                                                        TRUE_LITERAL,
                                                                        new Cast(new FunctionCall(FAIL, ImmutableList.of(new Constant(INTEGER, (long) INVALID_WINDOW_FRAME.toErrorCode().getCode()), new Constant(VARCHAR, Slices.utf8Slice("Window frame offset value must not be negative or null")))), BOOLEAN)),
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x"),
                                                                                ImmutableList.of(
                                                                                        new Row(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(createDecimalType(2, 1), Decimals.valueOfShort(new BigDecimal("1.1"))))),
                                                                                        new Row(ImmutableList.of(new Constant(INTEGER, 2L), new Constant(createDecimalType(2, 1), Decimals.valueOfShort(new BigDecimal("2.2"))))))))))))));

        assertPlan(sql, CREATED, pattern);
    }

    @Test
    public void testFrameFollowingWithOffsetCoercion()
    {
        @Language("SQL") String sql = "SELECT array_agg(key) OVER(ORDER BY key RANGE BETWEEN CURRENT ROW AND x FOLLOWING) " +
                "FROM (VALUES (1.1, 1), (2.2, 2)) t(key, x)";

        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("key"),
                                                ImmutableMap.of("key", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "array_agg_result",
                                                windowFunction(
                                                        "array_agg",
                                                        ImmutableList.of("key"),
                                                        new WindowNode.Frame(
                                                                RANGE,
                                                                CURRENT_ROW,
                                                                Optional.empty(),
                                                                Optional.empty(),
                                                                FOLLOWING,
                                                                Optional.of(new Symbol("frame_end_value")),
                                                                Optional.of(new Symbol("key_for_frame_end_comparison"))))),
                                project(// coerce sort key to compare sort key values with frame end values
                                        ImmutableMap.of("key_for_frame_end_comparison", expression(new Cast(new SymbolReference("key"), createDecimalType(12, 1)))),
                                        project(// calculate frame end value (sort key + frame offset)
                                                ImmutableMap.of("frame_end_value", expression(new FunctionCall(ADD_DECIMAL_10_0, ImmutableList.of(new SymbolReference("key"), new SymbolReference("offset"))))),
                                                filter(// validate offset values
                                                        ifExpression(
                                                                new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("offset"), new Constant(createDecimalType(10, 0), 0L)),
                                                                TRUE_LITERAL,
                                                                new Cast(new FunctionCall(FAIL, ImmutableList.of(new Constant(INTEGER, (long) INVALID_WINDOW_FRAME.toErrorCode().getCode()), new Constant(VARCHAR, Slices.utf8Slice("Window frame offset value must not be negative or null")))), BOOLEAN)),
                                                        project(// coerce offset value to calculate frame end values
                                                                ImmutableMap.of("offset", expression(new Cast(new SymbolReference("x"), createDecimalType(10, 0)))),
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x"),
                                                                                ImmutableList.of(
                                                                                        new Row(ImmutableList.of(new Constant(createDecimalType(2, 1), Decimals.valueOfShort(new BigDecimal("1.1"))), new Constant(INTEGER, 1L))),
                                                                                        new Row(ImmutableList.of(new Constant(createDecimalType(2, 1), Decimals.valueOfShort(new BigDecimal("2.2"))), new Constant(INTEGER, 2L))))))))))));

        assertPlan(sql, CREATED, pattern);
    }

    @Test
    public void testFramePrecedingFollowingNoCoercions()
    {
        @Language("SQL") String sql = "SELECT array_agg(key) OVER(ORDER BY key RANGE BETWEEN x PRECEDING AND y FOLLOWING) " +
                "FROM (VALUES (1, 1, 1), (2, 2, 2)) t(key, x, y)";

        PlanMatchPattern pattern =
                anyTree(
                        window(
                                windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("key"),
                                                ImmutableMap.of("key", SortOrder.ASC_NULLS_LAST)))
                                        .addFunction(
                                                "array_agg_result",
                                                windowFunction(
                                                        "array_agg",
                                                        ImmutableList.of("key"),
                                                        new WindowNode.Frame(
                                                                RANGE,
                                                                PRECEDING,
                                                                Optional.of(new Symbol("frame_start_value")),
                                                                Optional.of(new Symbol("key")),
                                                                FOLLOWING,
                                                                Optional.of(new Symbol("frame_end_value")),
                                                                Optional.of(new Symbol("key"))))),
                                project(// calculate frame end value (sort key + frame end offset)
                                        ImmutableMap.of("frame_end_value", expression(new FunctionCall(ADD_INTEGER, ImmutableList.of(new SymbolReference("key"), new SymbolReference("y"))))),
                                        filter(// validate frame end offset values
                                                ifExpression(
                                                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("y"), new Constant(INTEGER, 0L)),
                                                        TRUE_LITERAL,
                                                        new Cast(new FunctionCall(FAIL, ImmutableList.of(new Constant(INTEGER, (long) INVALID_WINDOW_FRAME.toErrorCode().getCode()), new Constant(VARCHAR, Slices.utf8Slice("Window frame offset value must not be negative or null")))), BOOLEAN)),
                                                project(// calculate frame start value (sort key - frame start offset)
                                                        ImmutableMap.of("frame_start_value", expression(new FunctionCall(SUBTRACT_INTEGER, ImmutableList.of(new SymbolReference("key"), new SymbolReference("x"))))),
                                                        filter(// validate frame start offset values
                                                                ifExpression(
                                                                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("x"), new Constant(INTEGER, 0L)),
                                                                        TRUE_LITERAL,
                                                                        new Cast(new FunctionCall(FAIL, ImmutableList.of(new Constant(INTEGER, (long) INVALID_WINDOW_FRAME.toErrorCode().getCode()), new Constant(VARCHAR, Slices.utf8Slice("Window frame offset value must not be negative or null")))), BOOLEAN)),
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x", "y"),
                                                                                ImmutableList.of(
                                                                                        new Row(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 1L), new Constant(INTEGER, 1L))),
                                                                                        new Row(ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 2L), new Constant(INTEGER, 2L))))))))))));

        assertPlan(sql, CREATED, pattern);
    }
}
