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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.rowpattern.Patterns.label;

public class TestExpressionRewriteRuleSet
        extends BaseRuleTest
{
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final ExpressionRewriteRuleSet zeroRewriter = new ExpressionRewriteRuleSet(
            (expression, context) -> ExpressionTreeRewriter.rewriteWith(new io.trino.sql.tree.ExpressionRewriter<>()
            {
                @Override
                protected Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    return new LongLiteral("0");
                }

                @Override
                public Expression rewriteRow(Row node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    // rewrite Row items to preserve Row structure of ValuesNode
                    return new Row(node.getItems().stream().map(item -> new LongLiteral("0")).collect(toImmutableList()));
                }
            }, expression));

    @Test
    public void testProjectionExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.of(p.symbol("y"), PlanBuilder.expression("x IS NOT NULL")),
                        p.values(p.symbol("x"))))
                .matches(
                        project(ImmutableMap.of("y", expression("0")), values("x")));
    }

    @Test
    public void testProjectionExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.of(p.symbol("y"), PlanBuilder.expression("0")),
                        p.values(p.symbol("x"))))
                .doesNotFire();
    }

    @Test
    public void testAggregationExpressionRewrite()
    {
        ExpressionRewriteRuleSet functionCallRewriter = new ExpressionRewriteRuleSet((expression, context) -> functionResolution
                .functionCallBuilder(QualifiedName.of("count"))
                .addArgument(VARCHAR, new SymbolReference("y"))
                .build());
        tester().assertThat(functionCallRewriter.aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.symbol("count_1", BigintType.BIGINT),
                                functionResolution
                                        .functionCallBuilder(QualifiedName.of("count"))
                                        .addArgument(VARCHAR, new SymbolReference("x"))
                                        .build(),
                                ImmutableList.of(BigintType.BIGINT))
                        .source(
                                p.values(p.symbol("x"), p.symbol("y")))))
                .matches(
                        PlanMatchPattern.aggregation(
                                ImmutableMap.of("count_1", aliases -> functionResolution
                                        .functionCallBuilder(QualifiedName.of("count"))
                                        .addArgument(VARCHAR, new SymbolReference("y"))
                                        .build()),
                                values("x", "y")));
    }

    @Test
    public void testAggregationExpressionNotRewritten()
    {
        FunctionCall nowCall = functionResolution
                .functionCallBuilder(QualifiedName.of("now"))
                .build();
        ExpressionRewriteRuleSet functionCallRewriter = new ExpressionRewriteRuleSet((expression, context) -> nowCall);

        tester().assertThat(functionCallRewriter.aggregationExpressionRewrite())
                .on(p -> p.aggregation(a -> a
                        .globalGrouping()
                        .addAggregation(
                                p.symbol("count_1", DateType.DATE),
                                nowCall,
                                ImmutableList.of())
                        .source(
                                p.values())))
                .doesNotFire();
    }

    @Test
    public void testFilterExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.filterExpressionRewrite())
                .on(p -> p.filter(new LongLiteral("1"), p.values()))
                .matches(
                        filter("0", values()));
    }

    @Test
    public void testFilterExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.filterExpressionRewrite())
                .on(p -> p.filter(new LongLiteral("0"), p.values()))
                .doesNotFire();
    }

    @Test
    public void testValueExpressionRewrite()
    {
        tester().assertThat(zeroRewriter.valuesExpressionRewrite())
                .on(p -> p.values(
                        ImmutableList.<Symbol>of(p.symbol("a")),
                        ImmutableList.of((ImmutableList.of(PlanBuilder.expression("1"))))))
                .matches(
                        values(ImmutableList.of("a"), ImmutableList.of(ImmutableList.of(new LongLiteral("0")))));
    }

    @Test
    public void testValueExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.valuesExpressionRewrite())
                .on(p -> p.values(
                        ImmutableList.<Symbol>of(p.symbol("a")),
                        ImmutableList.of((ImmutableList.of(PlanBuilder.expression("0"))))))
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
                                .addMeasure(p.symbol("measure_1"), "1", INTEGER)
                                .pattern(label("X"))
                                .addVariableDefinition(label("X"), "true")
                                .source(p.values(p.symbol("a")))))
                .matches(
                        patternRecognition(
                                builder -> builder
                                        .addMeasure("measure_1", "0", INTEGER)
                                        .pattern(label("X"))
                                        .addVariableDefinition(label("X"), "0"),
                                values("a")));
    }

    @Test
    public void testPatternRecognitionExpressionNotRewritten()
    {
        tester().assertThat(zeroRewriter.patternRecognitionExpressionRewrite())
                .on(p -> p.patternRecognition(
                        builder -> builder
                                .addMeasure(p.symbol("measure_1"), "0", INTEGER)
                                .pattern(label("X"))
                                .addVariableDefinition(label("X"), "0")
                                .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }
}
