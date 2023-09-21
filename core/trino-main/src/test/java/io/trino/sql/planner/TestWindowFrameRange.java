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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFrame;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.PRECEDING;
import static io.trino.sql.tree.WindowFrame.Type.RANGE;
import static java.lang.String.format;

public class TestWindowFrameRange
        extends BasePlanTest
{
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
                                                functionCall("array_agg", ImmutableList.of("key")),
                                                createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of("array_agg"), fromTypes(INTEGER)),
                                                windowFrame(
                                                        RANGE,
                                                        PRECEDING,
                                                        Optional.of("frame_start_value"),
                                                        Optional.of("key_for_frame_start_comparison"),
                                                        CURRENT_ROW,
                                                        Optional.empty(),
                                                        Optional.empty())),
                                project(// coerce sort key to compare sort key values with frame start values
                                        ImmutableMap.of("key_for_frame_start_comparison", expression("CAST(key AS decimal(12, 1))")),
                                        project(// calculate frame start value (sort key - frame offset)
                                                ImmutableMap.of("frame_start_value", expression(new FunctionCall(QualifiedName.of("$operator$subtract"), ImmutableList.of(new SymbolReference("key_for_frame_start_calculation"), new SymbolReference("x"))))),
                                                project(// coerce sort key to calculate frame start values
                                                        ImmutableMap.of("key_for_frame_start_calculation", expression("CAST(key AS decimal(10, 0))")),
                                                        filter(// validate offset values
                                                                "IF((x >= CAST(0 AS DECIMAL(2,1))), " +
                                                                        "true, " +
                                                                        format("CAST(fail(INTEGER '%d', VARCHAR 'Window frame offset value must not be negative or null') AS boolean))",
                                                                                INVALID_WINDOW_FRAME.toErrorCode().getCode()),
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x"),
                                                                                ImmutableList.of(
                                                                                        ImmutableList.of(new LongLiteral("1"), new DecimalLiteral("1.1")),
                                                                                        ImmutableList.of(new LongLiteral("2"), new DecimalLiteral("2.2")))))))))));

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
                                                functionCall("array_agg", ImmutableList.of("key")),
                                                createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of("array_agg"), fromTypes(createDecimalType(2, 1))),
                                                windowFrame(
                                                        RANGE,
                                                        CURRENT_ROW,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        FOLLOWING,
                                                        Optional.of("frame_end_value"),
                                                        Optional.of("key_for_frame_end_comparison"))),
                                project(// coerce sort key to compare sort key values with frame end values
                                        ImmutableMap.of("key_for_frame_end_comparison", expression("CAST(key AS decimal(12, 1))")),
                                        project(// calculate frame end value (sort key + frame offset)
                                                ImmutableMap.of("frame_end_value", expression(new FunctionCall(QualifiedName.of("$operator$add"), ImmutableList.of(new SymbolReference("key"), new SymbolReference("offset"))))),
                                                filter(// validate offset values
                                                        "IF((offset >= CAST(0 AS DECIMAL(10, 0))), " +
                                                                "true, " +
                                                                format("CAST(fail(INTEGER '%d', VARCHAR 'Window frame offset value must not be negative or null') AS boolean))",
                                                                        INVALID_WINDOW_FRAME.toErrorCode().getCode()),
                                                        project(// coerce offset value to calculate frame end values
                                                                ImmutableMap.of("offset", expression("CAST(x AS decimal(10, 0))")),
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x"),
                                                                                ImmutableList.of(
                                                                                        ImmutableList.of(new DecimalLiteral("1.1"), new LongLiteral("1")),
                                                                                        ImmutableList.of(new DecimalLiteral("2.2"), new LongLiteral("2")))))))))));

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
                                                functionCall("array_agg", ImmutableList.of("key")),
                                                createTestMetadataManager().resolveFunction(TEST_SESSION, QualifiedName.of("array_agg"), fromTypes(INTEGER)),
                                                windowFrame(
                                                        RANGE,
                                                        PRECEDING,
                                                        Optional.of("frame_start_value"),
                                                        Optional.of("key"),
                                                        FOLLOWING,
                                                        Optional.of("frame_end_value"),
                                                        Optional.of("key"))),
                                project(// calculate frame end value (sort key + frame end offset)
                                        ImmutableMap.of("frame_end_value", expression(new FunctionCall(QualifiedName.of("$operator$add"), ImmutableList.of(new SymbolReference("key"), new SymbolReference("y"))))),
                                        filter(// validate frame end offset values
                                                "IF((y >= CAST(0 AS INTEGER)), " +
                                                        "true, " +
                                                        format("CAST(fail(INTEGER '%d', VARCHAR 'Window frame offset value must not be negative or null') AS boolean))",
                                                                INVALID_WINDOW_FRAME.toErrorCode().getCode()),
                                                project(// calculate frame start value (sort key - frame start offset)
                                                        ImmutableMap.of("frame_start_value", expression(new FunctionCall(QualifiedName.of("$operator$subtract"), ImmutableList.of(new SymbolReference("key"), new SymbolReference("x"))))),
                                                        filter(// validate frame start offset values
                                                                "IF((x >= CAST(0 AS INTEGER)), " +
                                                                        "true, " +
                                                                        format("CAST(fail(INTEGER '%d', VARCHAR 'Window frame offset value must not be negative or null') AS boolean))",
                                                                                INVALID_WINDOW_FRAME.toErrorCode().getCode()),
                                                                anyTree(
                                                                        values(
                                                                                ImmutableList.of("key", "x", "y"),
                                                                                ImmutableList.of(
                                                                                        ImmutableList.of(new LongLiteral("1"), new LongLiteral("1"), new LongLiteral("1")),
                                                                                        ImmutableList.of(new LongLiteral("2"), new LongLiteral("2"), new LongLiteral("2")))))))))));

        assertPlan(sql, CREATED, pattern);
    }
}
