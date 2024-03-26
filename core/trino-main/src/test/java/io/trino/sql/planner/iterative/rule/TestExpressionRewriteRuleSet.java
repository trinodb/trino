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
import io.trino.spi.type.BigintType;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.rowpattern.Patterns.label;

public class TestExpressionRewriteRuleSet
        extends BaseRuleTest
{
    private final ExpressionRewriteRuleSet zeroRewriter = new ExpressionRewriteRuleSet(
            (expression, context) -> ExpressionTreeRewriter.rewriteWith(new io.trino.sql.ir.ExpressionRewriter<>()
            {
                @Override
                protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    return new Constant(INTEGER, 0L);
                }

                @Override
                public Expression rewriteRow(Row node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    // rewrite Row items to preserve Row structure of ValuesNode
                    return new Row(node.items().stream().map(item -> new Constant(INTEGER, 0L)).collect(toImmutableList()));
                }
            }, expression));

    @Test
    public void testProjectionExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.of(p.symbol("y", INTEGER), new Constant(INTEGER, 0L)),
                        p.values(p.symbol("x"))))
                .doesNotFire();
    }

    @Test
    public void testAggregationExpressionRewrite()
    {
        ExpressionRewriteRuleSet functionCallRewriter = new ExpressionRewriteRuleSet((expression, context) -> new Reference(BIGINT, "y"));
        tester().assertThat(functionCallRewriter.aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.symbol("count_1", BigintType.BIGINT),
                                PlanBuilder.aggregation("count", ImmutableList.of(new Reference(BIGINT, "x"))),
                                ImmutableList.of(BigintType.BIGINT))
                        .source(
                                p.values(p.symbol("x"), p.symbol("y")))))
                .matches(
                        PlanMatchPattern.aggregation(
                                ImmutableMap.of("count_1", PlanMatchPattern.aggregationFunction("count", ImmutableList.of("y"))),
                                values("x", "y")));
    }

    @Test
    public void testFilterExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.filterExpressionRewrite())
                .on(p -> p.filter(new Constant(INTEGER, 1L), p.values()))
                .matches(
                        filter(new Constant(INTEGER, 0L), values()));
    }

    @Test
    public void testFilterExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.filterExpressionRewrite())
                .on(p -> p.filter(new Constant(INTEGER, 0L), p.values()))
                .doesNotFire();
    }

    @Test
    public void testValueExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.valuesExpressionRewrite())
                .on(p -> p.values(
                        ImmutableList.<Symbol>of(p.symbol("a")),
                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L)))))
                .matches(
                        values(ImmutableList.of("a"), ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 0L)))));
    }

    @Test
    public void testValueExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.valuesExpressionRewrite())
                .on(p -> p.values(
                        ImmutableList.<Symbol>of(p.symbol("a")),
                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 0L)))))
                .doesNotFire();
    }

    @Test
    public void testPatternRecognitionExpressionRewrite()
    {
        // NOTE: this tests that the rewrite works on top-level expressions in MEASURES and DEFINE
        // Also, any aggregation arguments in MEASURES and DEFINE are rewritten
        tester().assertThat(zeroRewriter.patternRecognitionExpressionRewrite())
                .on(p -> p.patternRecognition(
                        builder -> builder
                                .addMeasure(p.symbol("measure_1", INTEGER), new Constant(INTEGER, 1L))
                                .pattern(label("X"))
                                .addVariableDefinition(label("X"), TRUE)
                                .source(p.values(p.symbol("a", INTEGER)))))
                .matches(
                        patternRecognition(
                                builder -> builder
                                        .addMeasure("measure_1", new Constant(INTEGER, 0L), INTEGER)
                                        .pattern(label("X"))
                                        .addVariableDefinition(label("X"), new Constant(INTEGER, 0L)),
                                values("a")));
    }

    @Test
    public void testPatternRecognitionExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.patternRecognitionExpressionRewrite())
                .on(p -> p.patternRecognition(
                        builder -> builder
                                .addMeasure(p.symbol("measure_1"), new Constant(INTEGER, 0L))
                                .pattern(label("X"))
                                .addVariableDefinition(label("X"), new Constant(INTEGER, 0L))
                                .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }
}
