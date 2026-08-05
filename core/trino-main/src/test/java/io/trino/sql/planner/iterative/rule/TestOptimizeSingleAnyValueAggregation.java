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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;

public class TestOptimizeSingleAnyValueAggregation
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, List.of(BIGINT, BIGINT));

    @Test
    public void testDoesNotFireForNonRowInput()
    {
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol result = p.symbol("result");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(value.toSymbolReference())), List.of(BIGINT))
                            .source(p.values(group, value)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForRowTypedInputNotConstructed()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.values(group, row)));
                })
                .doesNotFire();
    }

    @Test
    public void testRewriteForSingleGroupingKey()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "group", expression(new Reference(BIGINT, "group")),
                                "result", expression(new Reference(rowType, "row"))),
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(List.of("group"))
                                        .maxRowCountPerPartition(Optional.of(1))
                                        .orderSensitive(false),
                                strictProject(
                                        ImmutableMap.of(
                                                "group", expression(new Reference(BIGINT, "group")),
                                                "row", expression(new Row(List.of(new Reference(BIGINT, "value"))))),
                                        values("group", "value")))));
    }

    @Test
    public void testRewriteForMultiFieldRowConstructor()
    {
        var rowType = anonymousRow(BIGINT, BOOLEAN);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol flag = p.symbol("flag", BOOLEAN);
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference(), flag.toSymbolReference())))
                                            .build(),
                                    p.values(group, value, flag))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "group", expression(new Reference(BIGINT, "group")),
                                "result", expression(new Reference(rowType, "row"))),
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(List.of("group"))
                                        .maxRowCountPerPartition(Optional.of(1))
                                        .orderSensitive(false),
                                strictProject(
                                        ImmutableMap.of(
                                                "group", expression(new Reference(BIGINT, "group")),
                                                "row", expression(new Row(List.of(new Reference(BIGINT, "value"), new Reference(BOOLEAN, "flag"))))),
                                        values("group", "value", "flag")))));
    }

    @Test
    public void testRewriteForMultipleGroupingKeys()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol subgroup = p.symbol("subgroup");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group, subgroup)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .putIdentity(subgroup)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, subgroup, value))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "group", expression(new Reference(BIGINT, "group")),
                                "subgroup", expression(new Reference(BIGINT, "subgroup")),
                                "result", expression(new Reference(rowType, "row"))),
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(List.of("group", "subgroup"))
                                        .maxRowCountPerPartition(Optional.of(1))
                                        .orderSensitive(false),
                                strictProject(
                                        ImmutableMap.of(
                                                "group", expression(new Reference(BIGINT, "group")),
                                                "subgroup", expression(new Reference(BIGINT, "subgroup")),
                                                "row", expression(new Row(List.of(new Reference(BIGINT, "value"))))),
                                        values("group", "subgroup", "value")))));
    }

    @Test
    public void testRewriteForArbitraryAlias()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("arbitrary", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "group", expression(new Reference(BIGINT, "group")),
                                "result", expression(new Reference(rowType, "row"))),
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(List.of("group"))
                                        .maxRowCountPerPartition(Optional.of(1))
                                        .orderSensitive(false),
                                strictProject(
                                        ImmutableMap.of(
                                                "group", expression(new Reference(BIGINT, "group")),
                                                "row", expression(new Row(List.of(new Reference(BIGINT, "value"))))),
                                        values("group", "value")))));
    }

    @Test
    public void testRewriteWithProjectedGroupingKey()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol projectedGroup = p.symbol("projected_group");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(projectedGroup)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .put(projectedGroup, new Call(ADD_BIGINT, List.of(group.toSymbolReference(), new Constant(BIGINT, 1L))))
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "projected_group", expression(new Reference(BIGINT, "projected_group")),
                                "result", expression(new Reference(rowType, "row"))),
                        rowNumber(
                                pattern -> pattern
                                        .partitionBy(List.of("projected_group"))
                                        .maxRowCountPerPartition(Optional.of(1))
                                        .orderSensitive(false),
                                strictProject(
                                        ImmutableMap.of(
                                                "projected_group", expression(new Call(ADD_BIGINT, List.of(new Reference(BIGINT, "group"), new Constant(BIGINT, 1L)))),
                                                "row", expression(new Row(List.of(new Reference(BIGINT, "value"))))),
                                        values("group", "value")))));
    }

    @Test
    public void testDoesNotFireForMultipleAggregations()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    Symbol count = p.symbol("count");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .addAggregation(count, aggregation("count", List.of()), List.of())
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForGlobalAggregation()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(value))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMultipleGroupingSets()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .groupingSets(groupingSets(List.of(group), 2, ImmutableSet.of()))
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonSingleStep()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .step(FINAL)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForStreamableAggregation()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .preGroupedSymbols(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForUnsupportedAggregationFunction()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol count = p.symbol("count");
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(count, aggregation("count", List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDistinctAggregation()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", true, List.of(row.toSymbolReference())), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForFilteredAggregation()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol filter = p.symbol("filter", BOOLEAN);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference()), filter), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .putIdentity(filter)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value, filter))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMaskedAggregation()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol result = p.symbol("result", rowType);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference())), List.of(rowType), mask)
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .putIdentity(mask)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value, mask))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForOrderedAggregation()
    {
        var rowType = anonymousRow(BIGINT);
        tester().assertThat(new OptimizeSingleAnyValueAggregation())
                .on(p -> {
                    Symbol group = p.symbol("group");
                    Symbol value = p.symbol("value");
                    Symbol row = p.symbol("row", rowType);
                    Symbol result = p.symbol("result", rowType);
                    OrderingScheme orderingScheme = new OrderingScheme(List.of(group), ImmutableMap.of(group, SortOrder.ASC_NULLS_FIRST));
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(group)
                            .addAggregation(result, aggregation("any_value", List.of(row.toSymbolReference()), orderingScheme), List.of(rowType))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(group)
                                            .put(row, new Row(List.of(value.toSymbolReference())))
                                            .build(),
                                    p.values(group, value))));
                })
                .doesNotFire();
    }
}
