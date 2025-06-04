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
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.UnnestNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
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
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;

public class TestDecorrelateInnerUnnestWithGlobalAggregation
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction REGEXP_EXTRACT_ALL = FUNCTIONS.resolveFunction("regexp_extract_all", fromTypes(VARCHAR, VARCHAR));
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction NEGATION_BIGINT = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(BIGINT));

    @Test
    public void doesNotFireWithoutGlobalAggregation()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .singleGroupingSet(p.symbol("unnested"))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested")))),
                                        Optional.empty(),
                                        INNER,
                                        p.values())))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireWithoutUnnest()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
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
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
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
                                        INNER,
                                        p.values(p.symbol("a"), p.symbol("b")))))))
                .doesNotFire();
    }

    @Test
    public void testTransformCorrelatedUnnest()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "unnested_corr"))), ImmutableList.of(BIGINT))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        INNER,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("mask", expression(not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BIGINT, "ordinality"))))),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testPreexistingMask()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr"), p.symbol("old_masks", BOOLEAN)),
                        p.values(p.symbol("corr"), p.symbol("old_masks", BOOLEAN)),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "unnested_corr"))), ImmutableList.of(BIGINT), p.symbol("old_mask", BOOLEAN))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr"))),
                                                new UnnestNode.Mapping(p.symbol("old_masks", BOOLEAN), ImmutableList.of(p.symbol("old_mask", BOOLEAN)))),
                                        Optional.empty(),
                                        INNER,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("corr", "old_masks", "unique"),
                                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("new_mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("new_mask", expression(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "old_mask"), new Reference(BOOLEAN, "mask"))))),
                                                project(
                                                        ImmutableMap.of("mask", expression(not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BIGINT, "ordinality"))))),
                                                        unnest(
                                                                ImmutableList.of("corr", "old_masks", "unique"),
                                                                ImmutableList.of(
                                                                        unnestMapping("corr", ImmutableList.of("unnested_corr")),
                                                                        unnestMapping("old_masks", ImmutableList.of("old_mask"))),
                                                                Optional.of("ordinality"),
                                                                LEFT,
                                                                assignUniqueId("unique", values("corr", "old_masks"))))))));
    }

    @Test
    public void testWithPreexistingOrdinality()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "unnested_corr"))), ImmutableList.of(BIGINT))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.of(p.symbol("ordinality")),
                                        INNER,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("mask", expression(not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BIGINT, "ordinality"))))),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testMultipleGlobalAggregations()
    {
        // the innermost aggregation is rewritten with mask
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(outerBuilder -> outerBuilder
                                .globalGrouping()
                                .addAggregation(p.symbol("arbitrary"), PlanBuilder.aggregation("arbitrary", ImmutableList.of(new Reference(BIGINT, "sum"))), ImmutableList.of(BIGINT))
                                .source(
                                        p.aggregation(innerBuilder -> innerBuilder
                                                .globalGrouping()
                                                .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "unnested_corr"))), ImmutableList.of(BIGINT))
                                                .source(p.unnest(
                                                        ImmutableList.of(),
                                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                        Optional.empty(),
                                                        INNER,
                                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("any_value"), aggregationFunction("any_value", ImmutableList.of("sum"))),
                                        ImmutableList.of(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        aggregation(
                                                singleGroupingSet("unique", "corr"),
                                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                                ImmutableList.of(),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("mask", expression(not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BIGINT, "ordinality"))))),
                                                        unnest(
                                                                ImmutableList.of("corr", "unique"),
                                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                                Optional.of("ordinality"),
                                                                LEFT,
                                                                assignUniqueId("unique", values("corr"))))))));
    }

    @Test
    public void testProjectOverGlobalAggregation()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(
                                Assignments.of(p.symbol("sum_1", INTEGER), new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L)))),
                                p.aggregation(innerBuilder -> innerBuilder
                                        .globalGrouping()
                                        .addAggregation(p.symbol("sum"), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "unnested_corr"))), ImmutableList.of(BIGINT))
                                        .source(p.unnest(
                                                ImmutableList.of(),
                                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                Optional.empty(),
                                                INNER,
                                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))
                .matches(
                        project(
                                strictProject(
                                        ImmutableMap.of("corr", expression(new Reference(BIGINT, "corr")), "unique", expression(new Reference(BIGINT, "unique")), "sum_1", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "sum"), new Constant(INTEGER, 1L))))),
                                        aggregation(
                                                singleGroupingSet("unique", "corr"),
                                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("unnested_corr"))),
                                                ImmutableList.of(),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("mask", expression(not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BIGINT, "ordinality"))))),
                                                        unnest(
                                                                ImmutableList.of("corr", "unique"),
                                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                                Optional.of("ordinality"),
                                                                LEFT,
                                                                assignUniqueId("unique", values("corr"))))))));
    }

    @Test
    public void testPreprojectUnnestSymbol()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> {
                    Symbol corr = p.symbol("corr", VARCHAR);
                    Call regexpExtractAll = new Call(
                            tester().getMetadata().resolveBuiltinFunction("regexp_extract_all", fromTypes(VARCHAR, VARCHAR)),
                            ImmutableList.of(corr.toSymbolReference(), new Cast(new Constant(VARCHAR, Slices.utf8Slice(".")), JONI_REGEXP)));

                    return p.correlatedJoin(
                            ImmutableList.of(corr),
                            p.values(corr),
                            p.aggregation(builder -> builder
                                    .globalGrouping()
                                    .addAggregation(p.symbol("max"), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(VARCHAR, "unnested_corr"))), ImmutableList.of(BIGINT))
                                    .source(p.unnest(
                                            ImmutableList.of(),
                                            ImmutableList.of(new UnnestNode.Mapping(p.symbol("char_array", new ArrayType(VARCHAR)), ImmutableList.of(p.symbol("unnested_corr", VARCHAR)))),
                                            Optional.empty(),
                                            INNER,
                                            p.project(
                                                    Assignments.of(p.symbol("char_array", new ArrayType(VARCHAR)), regexpExtractAll),
                                                    p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))));
                })
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("corr", "unique", "char_array"),
                                        ImmutableMap.of(Optional.of("max"), aggregationFunction("max", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("mask", expression(not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BIGINT, "ordinality"))))),
                                                unnest(
                                                        ImmutableList.of("corr", "unique", "char_array"),
                                                        ImmutableList.of(unnestMapping("char_array", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        project(
                                                                ImmutableMap.of("char_array", expression(new Call(REGEXP_EXTRACT_ALL, ImmutableList.of(new Reference(VARCHAR, "corr"), new Cast(new Constant(VARCHAR, Slices.utf8Slice(".")), JONI_REGEXP))))),
                                                                assignUniqueId("unique", values("corr"))))))));
    }

    @Test
    public void testMultipleNodesOverUnnestInSubquery()
    {
        // in the following case, the correlated subquery is shaped as follows:
        // project(global_aggregation(project(grouped_aggregation(project(unnest)))))
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation(FUNCTIONS.getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("groups"), p.symbol("numbers")),
                        p.values(p.symbol("groups"), p.symbol("numbers")),
                        p.project(
                                Assignments.of(p.symbol("sum_1", BIGINT), new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), new Constant(BIGINT, 1L)))),
                                p.aggregation(globalBuilder -> globalBuilder
                                        .globalGrouping()
                                        .addAggregation(p.symbol("sum", BIGINT), PlanBuilder.aggregation("sum", ImmutableList.of(new Reference(BIGINT, "negate"))), ImmutableList.of(BIGINT))
                                        .source(p.project(
                                                Assignments.builder()
                                                        .put(p.symbol("negate", BIGINT), new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "max"))))
                                                        .build(),
                                                p.aggregation(groupedBuilder -> groupedBuilder
                                                        .singleGroupingSet(p.symbol("group"))
                                                        .addAggregation(p.symbol("max", BIGINT), PlanBuilder.aggregation("max", ImmutableList.of(new Reference(BIGINT, "modulo"))), ImmutableList.of(BIGINT))
                                                        .source(
                                                                p.project(
                                                                        Assignments.builder()
                                                                                .putIdentities(ImmutableList.of(p.symbol("group"), p.symbol("number")))
                                                                                .put(p.symbol("modulo", BIGINT), new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "number"), new Constant(BIGINT, 10L))))
                                                                                .build(),
                                                                        p.unnest(
                                                                                ImmutableList.of(),
                                                                                ImmutableList.of(
                                                                                        new UnnestNode.Mapping(p.symbol("groups"), ImmutableList.of(p.symbol("group"))),
                                                                                        new UnnestNode.Mapping(p.symbol("numbers"), ImmutableList.of(p.symbol("number")))),
                                                                                Optional.empty(),
                                                                                INNER,
                                                                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))))))
                .matches(
                        project(
                                project(
                                        ImmutableMap.of("sum_1", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "sum"), new Constant(BIGINT, 1L))))),
                                        aggregation(
                                                singleGroupingSet("groups", "numbers", "unique"),
                                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("negated"))),
                                                ImmutableList.of(),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("negated", expression(new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "max"))))),
                                                        aggregation(
                                                                singleGroupingSet("groups", "numbers", "unique", "mask", "group"),
                                                                ImmutableMap.of(Optional.of("max"), aggregationFunction("max", ImmutableList.of("modulo"))),
                                                                ImmutableList.of(),
                                                                ImmutableList.of(),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                project(
                                                                        ImmutableMap.of("modulo", expression(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "number"), new Constant(BIGINT, 10L))))),
                                                                        project(
                                                                                ImmutableMap.of("mask", expression(not(FUNCTIONS.getMetadata(), new IsNull(new Reference(BIGINT, "ordinality"))))),
                                                                                unnest(
                                                                                        ImmutableList.of("groups", "numbers", "unique"),
                                                                                        ImmutableList.of(
                                                                                                unnestMapping("groups", ImmutableList.of("group")),
                                                                                                unnestMapping("numbers", ImmutableList.of("number"))),
                                                                                        Optional.of("ordinality"),
                                                                                        LEFT,
                                                                                        assignUniqueId("unique", values("groups", "numbers")))))))))));
    }
}
