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
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.UnnestNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

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
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                LEFT,
                                Optional.empty(),
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        Optional.empty(),
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testInnerCorrelatedJoinWithLeftUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.INNER,
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                LEFT,
                                Optional.empty(),
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        Optional.empty(),
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testInnerCorrelatedJoinWithInnerUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.INNER,
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                INNER,
                                Optional.empty(),
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        INNER,
                                        Optional.empty(),
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testLeftCorrelatedJoinWithInnerUnnest()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.empty(),
                                INNER,
                                Optional.empty(),
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                ImmutableMap.of("corr", expression("corr"), "unnested_corr", expression("IF((ordinality IS NULL), CAST(null AS bigint), unnested_corr)")),
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        Optional.empty(),
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testEnforceSingleRow()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.INNER,
                        TRUE_LITERAL,
                        p.enforceSingleRow(
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(// restore semantics of INNER unnest after it was rewritten to LEFT
                                ImmutableMap.of("corr", expression("corr"), "unnested_corr", expression("IF((ordinality IS NULL), CAST(null AS bigint), unnested_corr)")),
                                filter(
                                        "IF((row_number > BIGINT '1'), CAST(\"@fail@52QIVV94JMEG607IGOBHL05P1613CN1P9T92HEMH7BITOAOHO6M5DJCDG6AVS0EF51Q4I3398DN9SEQVJ68ED8D9AA82LGAE01OA96R7FI8O05GF5V71FKQKBAP7GSQ55HDD19GI0FESCSJFDP48HV1S2NABNSUSOP897D7E08301TKKLOLOGECE3MO5MF6NVBB4I1GJJ9N18===\"(INTEGER '28', VARCHAR 'Scalar sub-query has returned multiple rows') AS boolean), true)",
                                        rowNumber(
                                                builder -> builder
                                                        .partitionBy(ImmutableList.of("unique"))
                                                        .maxRowCountPerPartition(Optional.of(2)),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        Optional.empty(),
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
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.limit(
                                5,
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                filter(
                                        "row_number <= BIGINT '5'",
                                        rowNumber(
                                                builder -> builder
                                                        .partitionBy(ImmutableList.of("unique"))
                                                        .maxRowCountPerPartition(Optional.empty()),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        Optional.empty(),
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
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.limit(
                                5,
                                ImmutableList.of(p.symbol("unnested_corr")),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                filter(
                                        "rank_number <= BIGINT '5'",
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
                                                        Optional.empty(),
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testTopN()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.topN(
                                5,
                                ImmutableList.of(p.symbol("unnested_corr")),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                filter(
                                        "row_number <= BIGINT '5'",
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
                                                        Optional.empty(),
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testProject()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.project(
                                Assignments.of(p.symbol("boolean_result"), PlanBuilder.expression("unnested_corr IS NULL")),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        LEFT,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                project(
                                        ImmutableMap.of("corr", expression("corr"), "unique", expression("unique"), "ordinality", expression("ordinality"), "boolean_result", expression("unnested_corr IS NULL")),
                                        unnest(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                Optional.of("ordinality"),
                                                LEFT,
                                                Optional.empty(),
                                                assignUniqueId("unique", values("corr"))))));

        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.project(
                                Assignments.of(p.symbol("boolean_result"), PlanBuilder.expression("unnested_corr IS NULL")),
                                p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(// restore semantics of INNER unnest after it was rewritten to LEFT
                                ImmutableMap.of("corr", expression("corr"), "boolean_result", expression("IF((ordinality IS NULL), CAST(null AS bigint), boolean_result)")),
                                project(// append projection from the subquery
                                        ImmutableMap.of("corr", expression("corr"), "unique", expression("unique"), "ordinality", expression("ordinality"), "boolean_result", expression("unnested_corr IS NULL")),
                                        unnest(
                                                ImmutableList.of("corr", "unique"),
                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                Optional.of("ordinality"),
                                                LEFT,
                                                Optional.empty(),
                                                assignUniqueId("unique", values("corr"))))));
    }

    @Test
    public void testDifferentNodesInSubquery()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.enforceSingleRow(
                                p.project(
                                        Assignments.of(p.symbol("integer_result"), PlanBuilder.expression("IF(boolean_result, 1, -1)")),
                                        p.limit(
                                                5,
                                                p.project(
                                                        Assignments.of(p.symbol("boolean_result"), PlanBuilder.expression("unnested_corr IS NULL")),
                                                        p.topN(
                                                                10,
                                                                ImmutableList.of(p.symbol("unnested_corr")),
                                                                p.unnest(
                                                                        ImmutableList.of(),
                                                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                                        Optional.empty(),
                                                                        LEFT,
                                                                        Optional.empty(),
                                                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))))
                .matches(
                        project(
                                filter(// enforce single row
                                        "IF((row_number > BIGINT '1'), CAST(\"@fail@52QIVV94JMEG607IGOBHL05P1613CN1P9T92HEMH7BITOAOHO6M5DJCDG6AVS0EF51Q4I3398DN9SEQVJ68ED8D9AA82LGAE01OA96R7FI8O05GF5V71FKQKBAP7GSQ55HDD19GI0FESCSJFDP48HV1S2NABNSUSOP897D7E08301TKKLOLOGECE3MO5MF6NVBB4I1GJJ9N18===\"(INTEGER '28', VARCHAR 'Scalar sub-query has returned multiple rows') AS boolean), true)",
                                        project(// second projection
                                                ImmutableMap.of("corr", expression("corr"), "unique", expression("unique"), "ordinality", expression("ordinality"), "row_number", expression("row_number"), "integer_result", expression("IF(boolean_result, 1, -1)")),
                                                filter(// limit
                                                        "row_number <= BIGINT '5'",
                                                        project(// first projection
                                                                ImmutableMap.of("corr", expression("corr"), "unique", expression("unique"), "ordinality", expression("ordinality"), "row_number", expression("row_number"), "boolean_result", expression("unnested_corr IS NULL")),
                                                                filter(// topN
                                                                        "row_number <= BIGINT '10'",
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
                                                                                        Optional.empty(),
                                                                                        assignUniqueId("unique", values("corr")))))))))));
    }

    @Test
    public void testWithPreexistingOrdinality()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                Optional.of(p.symbol("ordinality")),
                                INNER,
                                Optional.empty(),
                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))
                .matches(
                        project(
                                ImmutableMap.of("corr", expression("corr"), "unnested_corr", expression("IF((ordinality IS NULL), CAST(null AS bigint), unnested_corr)")),
                                unnest(
                                        ImmutableList.of("corr", "unique"),
                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        Optional.empty(),
                                        assignUniqueId("unique", values("corr")))));
    }

    @Test
    public void testPreprojectUnnestSymbol()
    {
        tester().assertThat(new DecorrelateUnnest(tester().getMetadata()))
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        CorrelatedJoinNode.Type.LEFT,
                        TRUE_LITERAL,
                        p.unnest(
                                ImmutableList.of(),
                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("char_array"), ImmutableList.of(p.symbol("unnested_char")))),
                                Optional.empty(),
                                LEFT,
                                Optional.empty(),
                                p.project(
                                        Assignments.of(p.symbol("char_array"), PlanBuilder.expression("regexp_extract_all(corr, '.')")),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))
                .matches(
                        project(
                                unnest(
                                        ImmutableList.of("corr", "unique", "char_array"),
                                        ImmutableList.of(unnestMapping("char_array", ImmutableList.of("unnested_char"))),
                                        Optional.of("ordinality"),
                                        LEFT,
                                        Optional.empty(),
                                        project(
                                                ImmutableMap.of("char_array", expression("regexp_extract_all(corr, '.')")),
                                                assignUniqueId("unique", values("corr"))))));
    }
}
