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
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;

public class TestDecorrelateUnnest
        extends BaseRuleTest
{
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
                        TRUE_LITERAL,
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
                        TRUE_LITERAL,
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
                        TRUE_LITERAL,
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
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                INNER,
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "corr", expression(new SymbolReference("corr")),
                                        "unnested_corr", expression(new IfExpression(new IsNullPredicate(new SymbolReference("ordinality")), new Cast(new NullLiteral(), dataType("bigint")), new SymbolReference("unnested_corr")))),
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
                        TRUE_LITERAL,
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
                                        "corr", expression(new SymbolReference("corr")),
                                        "unnested_corr", expression(new IfExpression(new IsNullPredicate(new SymbolReference("ordinality")), new Cast(new NullLiteral(), dataType("bigint")), new SymbolReference("unnested_corr")))),
                                filter(
                                        new IfExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("row_number"), new GenericLiteral("BIGINT", "1")), new Cast(new FunctionCall(QualifiedName.of("fail"), ImmutableList.of(new GenericLiteral("INTEGER", "28"), new GenericLiteral("VARCHAR", "Scalar sub-query has returned multiple rows"))), dataType("boolean")), TRUE_LITERAL),
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
                        TRUE_LITERAL,
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
                                        new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("row_number"), new GenericLiteral("BIGINT", "5")),
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
                        TRUE_LITERAL,
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
                                        new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("rank_number"), new GenericLiteral("BIGINT", "5")),
                                        window(builder -> builder
                                                        .specification(specification(
                                                                ImmutableList.of("unique"),
                                                                ImmutableList.of("unnested_corr"),
                                                                ImmutableMap.of("unnested_corr", ASC_NULLS_FIRST)))
                                                        .addFunction("rank_number", functionCall("rank", ImmutableList.of())),
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
                        TRUE_LITERAL,
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
                                        new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("row_number"), new GenericLiteral("BIGINT", "5")),
                                        window(builder -> builder
                                                        .specification(specification(
                                                                ImmutableList.of("unique"),
                                                                ImmutableList.of("unnested_corr"),
                                                                ImmutableMap.of("unnested_corr", ASC_NULLS_FIRST)))
                                                        .addFunction("row_number", functionCall("row_number", ImmutableList.of())),
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
                        TRUE_LITERAL,
                        p.project(
                                Assignments.of(p.symbol("boolean_result"), new IsNullPredicate(new SymbolReference("unnested_corr"))),
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
                                                "corr", expression(new SymbolReference("corr")),
                                                "unique", expression(new SymbolReference("unique")),
                                                "ordinality", expression(new SymbolReference("ordinality")),
                                                "boolean_result", expression(new IsNullPredicate(new SymbolReference("unnested_corr")))),
                                        unnest(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                Optional.of("ordinality"),
                                                LEFT,
                                                assignUniqueId("unique", values("corr"))))));

        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        JoinType.LEFT,
                        TRUE_LITERAL,
                        p.project(
                                Assignments.of(p.symbol("boolean_result"), new IsNullPredicate(new SymbolReference("unnested_corr"))),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        INNER,
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(// restore semantics of INNER unnest after it was rewritten to LEFT
                                ImmutableMap.of(
                                        "corr", expression(new SymbolReference("corr")),
                                        "boolean_result", expression(new IfExpression(new IsNullPredicate(new SymbolReference("ordinality")), new Cast(new NullLiteral(), dataType("bigint")), new SymbolReference("boolean_result")))),
                                project(// append projection from the subquery
                                        ImmutableMap.of(
                                                "corr", expression(new SymbolReference("corr")),
                                                "unique", expression(new SymbolReference("unique")),
                                                "ordinality", expression(new SymbolReference("ordinality")),
                                                "boolean_result", expression(new IsNullPredicate(new SymbolReference("unnested_corr")))),
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
                        TRUE_LITERAL,
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("integer_result"), new IfExpression(new SymbolReference("boolean_result"), new LongLiteral("1"), new LongLiteral("-1"))),
                                        p.limit(
                                                5,
                                                p.project(
                                                        Assignments.of(p.symbol("boolean_result"), new IsNullPredicate(new SymbolReference("unnested_corr"))),
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
                                        new IfExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("row_number"), new GenericLiteral("BIGINT", "1")), new Cast(new FunctionCall(QualifiedName.of("fail"), ImmutableList.of(new GenericLiteral("INTEGER", "28"), new GenericLiteral("VARCHAR", "Scalar sub-query has returned multiple rows"))), dataType("boolean")), TRUE_LITERAL),
                                        project(// second projection
                                                ImmutableMap.of(
                                                        "corr", expression(new SymbolReference("corr")),
                                                        "unique", expression(new SymbolReference("unique")),
                                                        "ordinality", expression(new SymbolReference("ordinality")),
                                                        "row_number", expression(new SymbolReference("row_number")),
                                                        "integer_result", expression(new IfExpression(new SymbolReference("boolean_result"), new LongLiteral("1"), new LongLiteral("-1")))),
                                                filter(// limit
                                                        new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("row_number"), new GenericLiteral("BIGINT", "5")),
                                                        project(// first projection
                                                                ImmutableMap.of(
                                                                        "corr", expression(new SymbolReference("corr")),
                                                                        "unique", expression(new SymbolReference("unique")),
                                                                        "ordinality", expression(new SymbolReference("ordinality")),
                                                                        "row_number", expression(new SymbolReference("row_number")),
                                                                        "boolean_result", expression(new IsNullPredicate(new SymbolReference("unnested_corr")))),
                                                                filter(// topN
                                                                        new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("row_number"), new GenericLiteral("BIGINT", "10")),
                                                                        window(builder -> builder
                                                                                        .specification(specification(
                                                                                                ImmutableList.of("unique"),
                                                                                                ImmutableList.of("unnested_corr"),
                                                                                                ImmutableMap.of("unnested_corr", ASC_NULLS_FIRST)))
                                                                                        .addFunction("row_number", functionCall("row_number", ImmutableList.of())),
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
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.of(p.symbol("ordinality")),
                                INNER,
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "corr", expression(new SymbolReference("corr")),
                                        "unnested_corr", expression(new IfExpression(new IsNullPredicate(new SymbolReference("ordinality")), new Cast(new NullLiteral(), dataType("bigint")), new SymbolReference("unnested_corr")))),
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
                    FunctionCall regexpExtractAll = new FunctionCall(
                            tester().getMetadata().resolveBuiltinFunction("regexp_extract_all", fromTypes(VARCHAR, VARCHAR)).toQualifiedName(),
                            ImmutableList.of(corr.toSymbolReference(), new StringLiteral(".")));

                    return p.correlatedJoin(
                            ImmutableList.of(corr),
                            p.values(corr),
                            JoinType.LEFT,
                            TRUE_LITERAL,
                            p.unnest(
                                    ImmutableList.of(),
                                    ImmutableList.of(new UnnestNode.Mapping(p.symbol("char_array"), ImmutableList.of(p.symbol("unnested_char")))),
                                    Optional.empty(),
                                    LEFT,
                                    p.project(
                                            Assignments.of(p.symbol("char_array"), regexpExtractAll),
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
                                                ImmutableMap.of("char_array", expression(new FunctionCall(QualifiedName.of("regexp_extract_all"), ImmutableList.of(new SymbolReference("corr"), new StringLiteral("."))))),
                                                assignUniqueId("unique", values("corr"))))));
    }
}
