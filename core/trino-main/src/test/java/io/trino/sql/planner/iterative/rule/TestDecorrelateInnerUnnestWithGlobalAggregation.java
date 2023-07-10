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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.UnnestNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;

public class TestDecorrelateInnerUnnestWithGlobalAggregation
        extends BaseRuleTest
{
    @Test
    public void doesNotFireWithoutGlobalAggregation()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
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
                                        Optional.empty(),
                                        p.values())))))
                .doesNotFire();
    }

    @Test
    public void doesNotFireWithoutUnnest()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
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
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
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
                                        Optional.empty(),
                                        p.values(p.symbol("a"), p.symbol("b")))))))
                .doesNotFire();
    }

    @Test
    public void testTransformCorrelatedUnnest()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(unnested_corr)"), ImmutableList.of(BIGINT))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("mask", expression("ordinality IS NOT NULL")),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        Optional.empty(),
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testPreexistingMask()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr"), p.symbol("old_masks")),
                        p.values(p.symbol("corr"), p.symbol("old_masks")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(unnested_corr)"), ImmutableList.of(BIGINT), p.symbol("old_mask"))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(
                                                new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr"))),
                                                new UnnestNode.Mapping(p.symbol("old_masks"), ImmutableList.of(p.symbol("old_mask")))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("corr", "old_masks", "unique"),
                                        ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("new_mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("new_mask", expression("old_mask AND mask")),
                                                project(
                                                        ImmutableMap.of("mask", expression("ordinality IS NOT NULL")),
                                                        unnest(
                                                                ImmutableList.of("corr", "old_masks", "unique"),
                                                                ImmutableList.of(
                                                                        unnestMapping("corr", ImmutableList.of("unnested_corr")),
                                                                        unnestMapping("old_masks", ImmutableList.of("old_mask"))),
                                                                Optional.of("ordinality"),
                                                                LEFT,
                                                                Optional.empty(),
                                                                assignUniqueId("unique", values("corr", "old_masks"))))))));
    }

    @Test
    public void testWithPreexistingOrdinality()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(unnested_corr)"), ImmutableList.of(BIGINT))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.of(p.symbol("ordinality")),
                                        INNER,
                                        Optional.empty(),
                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("mask", expression("ordinality IS NOT NULL")),
                                                unnest(
                                                        ImmutableList.of("corr", "unique"),
                                                        ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        Optional.empty(),
                                                        assignUniqueId("unique", values("corr")))))));
    }

    @Test
    public void testMultipleGlobalAggregations()
    {
        // the innermost aggregation is rewritten with mask
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(outerBuilder -> outerBuilder
                                .globalGrouping()
                                .addAggregation(p.symbol("arbitrary"), PlanBuilder.expression("arbitrary(sum)"), ImmutableList.of(BIGINT))
                                .source(
                                        p.aggregation(innerBuilder -> innerBuilder
                                                .globalGrouping()
                                                .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(unnested_corr)"), ImmutableList.of(BIGINT))
                                                .source(p.unnest(
                                                        ImmutableList.of(),
                                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                        Optional.empty(),
                                                        INNER,
                                                        Optional.empty(),
                                                        p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of())))))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("unique", "corr"),
                                        ImmutableMap.of(Optional.of("arbitrary"), functionCall("arbitrary", ImmutableList.of("sum"))),
                                        ImmutableList.of(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        aggregation(
                                                singleGroupingSet("unique", "corr"),
                                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("unnested_corr"))),
                                                ImmutableList.of(),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("mask", expression("ordinality IS NOT NULL")),
                                                        unnest(
                                                                ImmutableList.of("corr", "unique"),
                                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                                Optional.of("ordinality"),
                                                                LEFT,
                                                                Optional.empty(),
                                                                assignUniqueId("unique", values("corr"))))))));
    }

    @Test
    public void testProjectOverGlobalAggregation()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.project(
                                Assignments.of(p.symbol("sum_1"), PlanBuilder.expression("sum + 1")),
                                p.aggregation(innerBuilder -> innerBuilder
                                        .globalGrouping()
                                        .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(unnested_corr)"), ImmutableList.of(BIGINT))
                                        .source(p.unnest(
                                                ImmutableList.of(),
                                                ImmutableList.of(new UnnestNode.Mapping(p.symbol("corr"), ImmutableList.of(p.symbol("unnested_corr")))),
                                                Optional.empty(),
                                                INNER,
                                                Optional.empty(),
                                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))
                .matches(
                        project(
                                strictProject(
                                        ImmutableMap.of("corr", expression("corr"), "unique", expression("unique"), "sum_1", expression("sum + 1")),
                                        aggregation(
                                                singleGroupingSet("unique", "corr"),
                                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("unnested_corr"))),
                                                ImmutableList.of(),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("mask", expression("ordinality IS NOT NULL")),
                                                        unnest(
                                                                ImmutableList.of("corr", "unique"),
                                                                ImmutableList.of(unnestMapping("corr", ImmutableList.of("unnested_corr"))),
                                                                Optional.of("ordinality"),
                                                                LEFT,
                                                                Optional.empty(),
                                                                assignUniqueId("unique", values("corr"))))))));
    }

    @Test
    public void testPreprojectUnnestSymbol()
    {
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("corr")),
                        p.values(p.symbol("corr")),
                        p.aggregation(builder -> builder
                                .globalGrouping()
                                .addAggregation(p.symbol("max"), PlanBuilder.expression("max(unnested_corr)"), ImmutableList.of(BIGINT))
                                .source(p.unnest(
                                        ImmutableList.of(),
                                        ImmutableList.of(new UnnestNode.Mapping(p.symbol("char_array"), ImmutableList.of(p.symbol("unnested_corr")))),
                                        Optional.empty(),
                                        INNER,
                                        Optional.empty(),
                                        p.project(
                                                Assignments.of(p.symbol("char_array"), PlanBuilder.expression("regexp_extract_all(corr, '.')")),
                                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))
                .matches(
                        project(
                                aggregation(
                                        singleGroupingSet("corr", "unique", "char_array"),
                                        ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("unnested_corr"))),
                                        ImmutableList.of(),
                                        ImmutableList.of("mask"),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of("mask", expression("ordinality IS NOT NULL")),
                                                unnest(
                                                        ImmutableList.of("corr", "unique", "char_array"),
                                                        ImmutableList.of(unnestMapping("char_array", ImmutableList.of("unnested_corr"))),
                                                        Optional.of("ordinality"),
                                                        LEFT,
                                                        Optional.empty(),
                                                        project(
                                                                ImmutableMap.of("char_array", expression("regexp_extract_all(corr, '.')")),
                                                                assignUniqueId("unique", values("corr"))))))));
    }

    @Test
    public void testMultipleNodesOverUnnestInSubquery()
    {
        // in the following case, the correlated subquery is shaped as follows:
        // project(global_aggregation(project(grouped_aggregation(project(unnest)))))
        tester().assertThat(new DecorrelateInnerUnnestWithGlobalAggregation())
                .on(p -> p.correlatedJoin(
                        ImmutableList.of(p.symbol("groups"), p.symbol("numbers")),
                        p.values(p.symbol("groups"), p.symbol("numbers")),
                        p.project(
                                Assignments.of(p.symbol("sum_1"), PlanBuilder.expression("sum + 1")),
                                p.aggregation(globalBuilder -> globalBuilder
                                        .globalGrouping()
                                        .addAggregation(p.symbol("sum"), PlanBuilder.expression("sum(negate)"), ImmutableList.of(BIGINT))
                                        .source(p.project(
                                                Assignments.builder()
                                                        .put(p.symbol("negate"), PlanBuilder.expression("-max"))
                                                        .build(),
                                                p.aggregation(groupedBuilder -> groupedBuilder
                                                        .singleGroupingSet(p.symbol("group"))
                                                        .addAggregation(p.symbol("max"), PlanBuilder.expression("max(modulo)"), ImmutableList.of(BIGINT))
                                                        .source(
                                                                p.project(
                                                                        Assignments.builder()
                                                                                .putIdentities(ImmutableList.of(p.symbol("group"), p.symbol("number")))
                                                                                .put(p.symbol("modulo"), PlanBuilder.expression("number % 10"))
                                                                                .build(),
                                                                        p.unnest(
                                                                                ImmutableList.of(),
                                                                                ImmutableList.of(
                                                                                        new UnnestNode.Mapping(p.symbol("groups"), ImmutableList.of(p.symbol("group"))),
                                                                                        new UnnestNode.Mapping(p.symbol("numbers"), ImmutableList.of(p.symbol("number")))),
                                                                                Optional.empty(),
                                                                                INNER,
                                                                                Optional.empty(),
                                                                                p.values(ImmutableList.of(), ImmutableList.of(ImmutableList.of()))))))))))))
                .matches(
                        project(
                                project(
                                        ImmutableMap.of("sum_1", expression("sum + 1")),
                                        aggregation(
                                                singleGroupingSet("groups", "numbers", "unique"),
                                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("negated"))),
                                                ImmutableList.of(),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("negated", expression("-max")),
                                                        aggregation(
                                                                singleGroupingSet("groups", "numbers", "unique", "mask", "group"),
                                                                ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("modulo"))),
                                                                ImmutableList.of(),
                                                                ImmutableList.of(),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                project(
                                                                        ImmutableMap.of("modulo", expression("number % 10")),
                                                                        project(
                                                                                ImmutableMap.of("mask", expression("ordinality IS NOT NULL")),
                                                                                unnest(
                                                                                        ImmutableList.of("groups", "numbers", "unique"),
                                                                                        ImmutableList.of(
                                                                                                unnestMapping("groups", ImmutableList.of("group")),
                                                                                                unnestMapping("numbers", ImmutableList.of("number"))),
                                                                                        Optional.of("ordinality"),
                                                                                        LEFT,
                                                                                        Optional.empty(),
                                                                                        assignUniqueId("unique", values("groups", "numbers")))))))))));
    }
}
