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
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.UnnestNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;

public class TestDecorrelateUnnest
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction FAIL = FUNCTIONS.resolveFunction("fail", fromTypes(INTEGER, VARCHAR));

    @Test
    public void doesNotFireWithoutUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.limit(5, p.values())))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnSourceDependentUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(
                                        new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr"))),
                                        new UnnestNode.Mapping(p.symbol("a"), ImmutableList.of(p.symbol("unnested_a")))),
                                p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testLeftCorrelatedJoinWithLeftUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                LEFT,
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testInnerCorrelatedJoinWithLeftUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.INNER,
                        TRUE,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                LEFT,
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testInnerCorrelatedJoinWithInnerUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.INNER,
                        TRUE,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                INNER,
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        INNER,
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testLeftCorrelatedJoinWithInnerUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                INNER,
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "corr", expression(new Reference(BIGINT, "corr")),
                                        "unnested_corr", expression(ifExpression(new IsNull(new Reference(BIGINT, "ordinality")), new Constant(BIGINT, null), new Reference(BIGINT, "unnested_corr")))),
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testEnforceSingleRow()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.INNER,
                        TRUE,
                        p.enforceSingleRow(
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        INNER,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(// restore semantics of INNER unnest after it was rewritten to LEFT
                                ImmutableMap.of(
                                        "corr", expression(new Reference(BIGINT, "corr")),
                                        "unnested_corr", expression(ifExpression(new IsNull(new Reference(BIGINT, "ordinality")), new Constant(BIGINT, null), new Reference(BIGINT, "unnested_corr")))),
                                filter(
                                        ifExpression(new Comparison(GREATER_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 1L)), new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, 28L), new Constant(VARCHAR, Slices.utf8Slice("Scalar sub-query has returned multiple rows")))), BOOLEAN), TRUE),
                                        rowNumber(
                                                builder -> builder
                                                        .partitionBy(ImmutableList.of("unique"))
                                                        .maxRowCountPerPartition(Optional.of(2)),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr"))))
                                                .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }

    @Test
    public void testLimit()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.limit(
                                5,
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                filter(
                                        new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)),
                                        rowNumber(
                                                builder -> builder
                                                        .partitionBy(ImmutableList.of("unique"))
                                                        .maxRowCountPerPartition(Optional.empty()),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr"))))
                                                .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }

    @Test
    public void testLimitWithTies()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.limit(
                                5,
                                ImmutableList.of(p.symbol("unnested_corr")),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                filter(
                                        new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "rank_number"), new Constant(BIGINT, 5L)),
                                        window(builder -> builder
                                                        .specification(specification(
                                                                ImmutableList.of("unique"),
                                                                ImmutableList.of("unnested_corr"),
                                                                ImmutableMap.of("unnested_corr", ASC_NULLS_FIRST)))
                                                        .addFunction("rank_number", windowFunction("rank", ImmutableList.of(), DEFAULT_FRAME)),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testTopN()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.topN(
                                5,
                                ImmutableList.of(p.symbol("unnested_corr")),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                filter(
                                        new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)),
                                        window(builder -> builder
                                                        .specification(specification(
                                                                ImmutableList.of("unique"),
                                                                ImmutableList.of("unnested_corr"),
                                                                ImmutableMap.of("unnested_corr", ASC_NULLS_FIRST)))
                                                        .addFunction("row_number", windowFunction("row_number", ImmutableList.of(), DEFAULT_FRAME)),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testProject()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.project(
                                Assignments.of(p.symbol("boolean_result", BOOLEAN), new IsNull(new Reference(BIGINT, "unnested_corr"))),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                project(
                                        ImmutableMap.of(
                                                "corr", expression(new Reference(BIGINT, "corr")),
                                                "unique", expression(new Reference(BIGINT, "unique")),
                                                "ordinality", expression(new Reference(BIGINT, "ordinality")),
                                                "boolean_result", expression(new IsNull(new Reference(BIGINT, "unnested_corr")))),
                                        unnest(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                Optional.of("ordinality"),
                                                LEFT,
                                                assignUniqueId("unique", values("corr"))))));

        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr", BIGINT)),
                        p.values(p.symbol("corr", BIGINT)),
                        JoinType.LEFT,
                        TRUE,
                        p.project(
                                Assignments.of(p.symbol("boolean_result", BOOLEAN), new IsNull(new Reference(BIGINT, "unnested_corr"))),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr", BIGINT), ImmutableList.of(p.symbol("unnested_corr", BIGINT)))),
                                        Optional.empty(),
                                        INNER,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(// restore semantics of INNER unnest after it was rewritten to LEFT
                                ImmutableMap.of(
                                        "corr", expression(new Reference(BIGINT, "corr")),
                                        "boolean_result", expression(ifExpression(new IsNull(new Reference(BIGINT, "ordinality")), new Constant(BOOLEAN, null), new Reference(BOOLEAN, "boolean_result")))),
                                project(// append projection from the subquery
                                        ImmutableMap.of(
                                                "corr", expression(new Reference(BIGINT, "corr")),
                                                "unique", expression(new Reference(BIGINT, "unique")),
                                                "ordinality", expression(new Reference(BIGINT, "ordinality")),
                                                "boolean_result", expression(new IsNull(new Reference(BIGINT, "unnested_corr")))),
                                        unnest(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                Optional.of("ordinality"),
                                                LEFT,
                                                assignUniqueId("unique", values("corr"))))));
    }

    @Test
    public void testDifferentNodesInSubquery()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("integer_result", INTEGER), ifExpression(new Reference(BOOLEAN, "boolean_result"), new Constant(INTEGER, 1L), new Constant(INTEGER, 1L))),
                                        p.limit(
                                                5,
                                                p.project(
                                                        Assignments.of(p.symbol("boolean_result", BOOLEAN), new IsNull(new Reference(BIGINT, "unnested_corr"))),
                                                        p.topN(
                                                                10,
                                                                ImmutableList.of(p.symbol("unnested_corr")),
                                                                p.unnest(
                                                                        ImmutableList.of(),
                                                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                                        Optional.empty(),
                                                                        LEFT,
                                                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))))
                .matches(
                        project(
                                filter(// enforce single row
                                        ifExpression(new Comparison(GREATER_THAN, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 1L)), new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, 28L), new Constant(VARCHAR, Slices.utf8Slice("Scalar sub-query has returned multiple rows")))), BOOLEAN), TRUE),
                                        project(// second projection
                                                ImmutableMap.of(
                                                        "corr", expression(new Reference(BIGINT, "corr")),
                                                        "unique", expression(new Reference(BIGINT, "unique")),
                                                        "ordinality", expression(new Reference(BIGINT, "ordinality")),
                                                        "row_number", expression(new Reference(BIGINT, "row_number")),
                                                        "integer_result", expression(ifExpression(new Reference(BOOLEAN, "boolean_result"), new Constant(INTEGER, 1L), new Constant(INTEGER, 1L)))),
                                                filter(// limit
                                                        new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 5L)),
                                                        project(// first projection
                                                                ImmutableMap.of(
                                                                        "corr", expression(new Reference(BIGINT, "corr")),
                                                                        "unique", expression(new Reference(BIGINT, "unique")),
                                                                        "ordinality", expression(new Reference(BIGINT, "ordinality")),
                                                                        "row_number", expression(new Reference(BIGINT, "row_number")),
                                                                        "boolean_result", expression(new IsNull(new Reference(BIGINT, "unnested_corr")))),
                                                                filter(// topN
                                                                        new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "row_number"), new Constant(BIGINT, 10L)),
                                                                        window(builder -> builder
                                                                                        .specification(specification(
                                                                                                ImmutableList.of("unique"),
                                                                                                ImmutableList.of("unnested_corr"),
                                                                                                ImmutableMap.of("unnested_corr", ASC_NULLS_FIRST)))
                                                                                        .addFunction("row_number", windowFunction("row_number", ImmutableList.of(), DEFAULT_FRAME)),
                                                                                unnest(
                                                                                        ImmutableList.of("corr", "unique"),
                                                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                                                        Optional.of("ordinality"),
                                                                                        LEFT,
                                                                                        assignUniqueId("unique", values("corr")))))))))));
    }

    @Test
    public void testWithPreexistingOrdinality()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.of(p.symbol("ordinality")),
                                INNER,
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "corr", expression(new Reference(BIGINT, "corr")),
                                        "unnested_corr", expression(ifExpression(new IsNull(new Reference(BIGINT, "ordinality")), new Constant(BIGINT, null), new Reference(BIGINT, "unnested_corr")))),
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testPreprojectUnnestSymbol()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> {
                    Symbol corr = p.symbol("corr", VARCHAR);
                    Call regexpExtractAll = new Call(
                            tester().getMetadata().resolveBuiltinFunction("regexp_extract_all", fromTypes(VARCHAR, VARCHAR)),
                            ImmutableList.of(corr.toSymbolReference(), new Cast(new Constant(VARCHAR, Slices.utf8Slice(".")), JONI_REGEXP)));

                    return p.correlatedJoin(
                            ImmutableList.of(corr),
                            p.values(corr),
                            JoinType.LEFT,
                            TRUE,
                            p.unnest(
                                    ImmutableList.of(),
                                    ImmutableList.of(new UnnestNode.Mapping(p.symbol("char_array", new ArrayType(VARCHAR)), ImmutableList.of(p.symbol("unnested_char")))),
                                    Optional.empty(),
                                    LEFT,
                                    p.project(
                                            Assignments.of(p.symbol("char_array", new ArrayType(VARCHAR)), regexpExtractAll),
                                            p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))));
                })
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique", "char_array"),
                                        ImmutableList.of(unnestMapping("char_array", ImmutableList.of("unnested_char"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        project(
                                                ImmutableMap.of("char_array", expression(new Call(tester().getMetadata().resolveBuiltinFunction("regexp_extract_all", fromTypes(VARCHAR, VARCHAR)), ImmutableList.of(new Reference(VARCHAR, "corr"), new Cast(new Constant(VARCHAR, Slices.utf8Slice(".")), JONI_REGEXP))))),
                                                assignUniqueId("unique", values("corr"))))));
    }
}
