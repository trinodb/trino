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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.ir.NullLiteral;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.SetOperationOutputMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;

public class TestImplementExceptAll
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        WindowNode.Frame frame = new WindowNode.Frame(
                ROWS,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        tester().assertThat(new ImplementExceptAll(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol a1 = p.symbol("a_1");
                    Symbol a2 = p.symbol("a_2");
                    Symbol b = p.symbol("b");
                    Symbol b1 = p.symbol("b_1");
                    Symbol b2 = p.symbol("b_2");
                    return p.except(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(a, a1)
                                    .put(a, a2)
                                    .put(b, b1)
                                    .put(b, b2)
                                    .build(),
                            ImmutableList.of(
                                    p.values(a1, b1),
                                    p.values(a2, b2)),
                            false);
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "a", expression(new SymbolReference("a")),
                                        "b", expression(new SymbolReference("b"))),
                                filter(
                                        new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("row_number"), new FunctionCall(QualifiedName.of("greatest"), ImmutableList.of(new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("count_1"), new SymbolReference("count_2")), new GenericLiteral(BIGINT, "0")))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "a", expression(new SymbolReference("a")),
                                                        "b", expression(new SymbolReference("b")),
                                                        "count_1", expression(new SymbolReference("count_1")),
                                                        "count_2", expression(new SymbolReference("count_2")),
                                                        "row_number", expression(new SymbolReference("row_number"))),
                                                window(builder -> builder
                                                                .specification(specification(
                                                                        ImmutableList.of("a", "b"),
                                                                        ImmutableList.of(),
                                                                        ImmutableMap.of()))
                                                                .addFunction(
                                                                        "count_1",
                                                                        windowFunction("count", ImmutableList.of("marker_1"), frame))
                                                                .addFunction(
                                                                        "count_2",
                                                                        windowFunction("count", ImmutableList.of("marker_2"), frame))
                                                                .addFunction(
                                                                        "row_number",
                                                                        windowFunction("row_number", ImmutableList.of(), frame)),
                                                        union(
                                                                project(
                                                                        ImmutableMap.of(
                                                                                "a1", expression(new SymbolReference("a_1")),
                                                                                "b1", expression(new SymbolReference("b_1")),
                                                                                "marker_left_1", expression(TRUE_LITERAL),
                                                                                "marker_left_2", expression(new Cast(new NullLiteral(), BOOLEAN))),
                                                                        values("a_1", "b_1")),
                                                                project(
                                                                        ImmutableMap.of(
                                                                                "a2", expression(new SymbolReference("a_2")),
                                                                                "b2", expression(new SymbolReference("b_2")),
                                                                                "marker_right_1", expression(new Cast(new NullLiteral(), BOOLEAN)),
                                                                                "marker_right_2", expression(TRUE_LITERAL)),
                                                                        values("a_2", "b_2")))
                                                                .withAlias("a", new SetOperationOutputMatcher(0))
                                                                .withAlias("b", new SetOperationOutputMatcher(1))
                                                                .withAlias("marker_1", new SetOperationOutputMatcher(2))
                                                                .withAlias("marker_2", new SetOperationOutputMatcher(3)))))));
    }
}
