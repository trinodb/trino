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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;

public class TestDecorrelateLeftUnnestWithGlobalAggregation
        extends BaseRuleTest
{
    @Test
    public void doesNotFireWithoutGlobalAggregation()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .singleGroupingSet(p.symbol("unnested"))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values())))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireWithoutUnnest()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnSourceDependentUnnest()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr"))),
                                                new UnnestNode.Mapping(p.symbol("a"), ImmutableList.of(p.symbol("unnested_a")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values(p.symbol("a"), p.symbol("b")))))))
                .doesNotFire();
    }

    @Test
    public void testTransformCorrelatedUnnest()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new SymbolReference("unnested_corr"))), ImmutableList.of(BIGINT))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        unnest(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                Optional.empty(),
                                                LEFT,
                                                assignUniqueId("unique", values("corr"))))));
    }

    @Test
    public void testWithMask()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr"), p.symbol("masks")),
                        p.values(p.symbol("corr"), p.symbol("masks")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new SymbolReference("unnested_corr"))), ImmutableList.of(BIGINT), p.symbol("mask"))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr"))),
                                                new UnnestNode.Mapping(p.symbol("masks"), ImmutableList.of(p.symbol("mask")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("corr", "masks", "unique"),
                                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        unnest(
                                                ImmutableList.of("corr", "masks", "unique"),
                                                ImmutableList.of(
                                                        unnestMapping("corr", ImmutableList.of("unnested_corr")),
                                                        unnestMapping("masks", ImmutableList.of("mask"))),
                                                Optional.empty(),
                                                LEFT,
                                                assignUniqueId("unique", values("corr", "masks"))))));
    }

    @Test
    public void testWithOrdinality()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new SymbolReference("unnested_corr"))), ImmutableList.of(BIGINT))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.of(p.symbol("ordinality")),
                                        LEFT,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        unnest(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                Optional.of("ordinality"),
                                                LEFT,
                                                assignUniqueId("unique", values("corr"))))));
    }

    @Test
    public void testMultipleGlobalAggregations()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(outerBuilder -> outerBuilder
                                .globalGrouping()
                                .addAggregation(p.symbol("arbitrary"), PlanBuilder.aggregation("arbitrary", ImmutableList.of(new SymbolReference("sum"))), ImmutableList.of(BIGINT))
                                .source(
                                        p.aggregation(innerBuilder -> innerBuilder
                                                .globalGrouping()
                                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new SymbolReference("unnested_corr"))), ImmutableList.of(BIGINT))
                                                .source(p.unnest(
                                                        ImmutableList.of(),
                                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                        Optional.empty(),
                                                        LEFT,
                                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("any_value"), aggregationFunction("any_value", ImmutableList.of("sum"))),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        aggregation(
                                                singleGroupingSet("unique", "corr"),
                                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                                ImmutableList.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.empty(),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testProjectOverGlobalAggregation()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(
                                Assignments.of(p.symbol("sum_1"), new ArithmeticBinaryExpression(ADD, new SymbolReference("sum"), new LongLiteral("1"))),
                                p.aggregation(innerBuilder -> innerBuilder
                                        .globalGrouping()
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new SymbolReference("unnested_corr"))), ImmutableList.of(BIGINT))
                                        .source(p.unnest(
                                                ImmutableList.of(),
                                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                Optional.empty(),
                                                LEFT,
                                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))
                .matches(
                        project(
                                strictProject(
                                        ImmutableMap.of(
                                                "corr", expression(new SymbolReference("corr")),
                                                "unique", expression(new SymbolReference("unique")),
                                                "sum_1", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("sum"), new LongLiteral("1")))),
                                        aggregation(
                                                singleGroupingSet("unique", "corr"),
                                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                                ImmutableList.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.empty(),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testPreprojectUnnestSymbol()
    {
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> {
                    Symbol corr = p.symbol("corr", VARCHAR);
                    FunctionCall regexpExtractAll = new FunctionCall(
                            tester().getMetadata().resolveBuiltinFunction("regexp_extract_all", fromTypes(VARCHAR, VARCHAR)).toQualifiedName(),
                            ImmutableList.of(corr.toSymbolReference(), new StringLiteral(".")));

                    return p.correlatedJoin(
                            ImmutableList.of(corr),
                            p.values(corr),
                            p.aggregation(builder -> builder
                                    .globalGrouping()
                                    .addAggregation(p.symbol("max"), PlanBuilder.aggregation("max", ImmutableList.of(new SymbolReference("unnested_char"))), ImmutableList.of(BIGINT))
                                    .source(p.unnest(
                                            ImmutableList.of(),
                                            ImmutableList.of(new UnnestNode.Mapping(p.symbol("char_array"), ImmutableList.of(p.symbol("unnested_char")))),
                                            Optional.empty(),
                                            LEFT,
                                            p.project(
                                                    Assignments.of(p.symbol("char_array"), regexpExtractAll),
                                                    p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))));
                })
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("corr", "unique", "char_array"),
                                        ImmutableMap.of(Optional.of("max"), aggregationFunction("max", ImmutableList.of("unnested_char"))),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        unnest(
                                                ImmutableList.of("corr", "unique", "char_array"),
                                                ImmutableList.of(unnestMapping("char_array", ImmutableList.of("unnested_char"))),
                                                Optional.empty(),
                                                LEFT,
                                                project(
                                                        ImmutableMap.of("char_array", expression(new FunctionCall(QualifiedName.of("regexp_extract_all"), ImmutableList.of(new SymbolReference("corr"), new StringLiteral("."))))),
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testMultipleNodesOverUnnestInSubquery()
    {
        // in the following case, the correlated subquery is shaped as follows:
        // project(global_aggregation(project(grouped_aggregation(project(unnest)))))
        tester().assertThat(new DecorrelateLeftUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("groups"), p.symbol("numbers")),
                        p.values(p.symbol("groups"), p.symbol("numbers")),
                        p.project(
                                Assignments.of(p.symbol("sum_1"), new ArithmeticBinaryExpression(ADD, new SymbolReference("sum"), new LongLiteral("1"))),
                                p.aggregation(globalBuilder -> globalBuilder
                                        .globalGrouping()
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new SymbolReference("negate"))), ImmutableList.of(BIGINT))
                                        .source(p.project(
                                                Assignments.builder()
                                                        .put(p.symbol("negate"), new ArithmeticUnaryExpression(MINUS, new SymbolReference("max")))
                                                        .build(),
                                                p.aggregation(groupedBuilder -> groupedBuilder
                                                        .singleGroupingSet(p.symbol("group"))
                                                        .addAggregation(p.symbol("max"), PlanBuilder.aggregation("max", ImmutableList.of(new SymbolReference("modulo"))), ImmutableList.of(BIGINT))
                                                        .source(
                                                                p.project(
                                                                        Assignments.builder()
                                                                                .putIdentities(ImmutableList.of(p.symbol("group"), p.symbol("number")))
                                                                                .put(p.symbol("modulo"), new ArithmeticBinaryExpression(MODULUS, new SymbolReference("number"), new LongLiteral("10")))
                                                                                .build(),
                                                                        p.unnest(
                                                                                ImmutableList.of(),
                                                                                ImmutableList.of(
                                                                                        new UnnestNode.Mapping(p.symbol("groups"), ImmutableList.of(p.symbol("group"))),
                                                                                        new UnnestNode.Mapping(p.symbol("numbers"), ImmutableList.of(p.symbol("number")))),
                                                                                Optional.empty(),
                                                                                LEFT,
                                                                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))))))
                .matches(
                        project(
                                project(
                                        ImmutableMap.of("sum_1", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("sum"), new LongLiteral("1")))),
                                        aggregation(
                                                singleGroupingSet("groups", "numbers", "unique"),
                                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("negated"))),
                                                ImmutableList.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("negated", expression(new ArithmeticUnaryExpression(MINUS, new SymbolReference("max")))),
                                                        aggregation(
                                                                singleGroupingSet("groups", "numbers", "unique", "group"),
                                                                ImmutableMap.of(Optional.of("max"), aggregationFunction("max", ImmutableList.of("modulo"))),
                                                                ImmutableList.of(),
                                                                ImmutableList.of(),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                project(
                                                                        ImmutableMap.of("modulo", expression(new ArithmeticBinaryExpression(MODULUS, new SymbolReference("number"), new LongLiteral("10")))),
                                                                        unnest(
                                                                                ImmutableList.of("groups", "numbers", "unique"),
                                                                                ImmutableList.of(
                                                                                        unnestMapping("groups", ImmutableList.of("group")),
                                                                                        unnestMapping("numbers", ImmutableList.of("number"))),
                                                                                Optional.empty(),
                                                                                LEFT,
                                                                                assignUniqueId("unique", values("groups", "numbers"))))))))));
    }
}
