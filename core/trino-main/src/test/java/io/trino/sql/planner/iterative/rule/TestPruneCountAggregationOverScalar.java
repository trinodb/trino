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
import io.trino.connector.CatalogName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.type.BigintType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;

public class TestPruneCountAggregationOverScalar
        extends BaseRuleTest
{
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @Test
    public void testDoesNotFireOnNonNestedAggregate()
    {
        tester().assertThat(new PruneCountAggregationOverScalar(tester().getMetadata()))
                .on(p ->
                        p.aggregation((a) -> a
                                .globalGrouping()
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        functionResolution
                                                .functionCallBuilder(QualifiedName.of("count"))
                                                .build(),
                                        ImmutableList.of())
                                .source(
                                        p.tableScan(ImmutableList.of(), ImmutableMap.of())))
                ).doesNotFire();
    }

    @Test
    public void testFiresOnNestedCountAggregate()
    {
        tester().assertThat(new PruneCountAggregationOverScalar(tester().getMetadata()))
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        functionResolution
                                                .functionCallBuilder(QualifiedName.of("count"))
                                                .build(),
                                        ImmutableList.of())
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                        p.aggregation((aggregationBuilder) -> aggregationBuilder
                                                .source(p.tableScan(ImmutableList.of(), ImmutableMap.of()))
                                                .globalGrouping()
                                                .step(AggregationNode.Step.SINGLE)))))
                .matches(values(ImmutableMap.of("count_1", 0)));
    }

    @Test
    public void testFiresOnCountAggregateOverValues()
    {
        tester().assertThat(new PruneCountAggregationOverScalar(tester().getMetadata()))
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        functionResolution
                                                .functionCallBuilder(QualifiedName.of("count"))
                                                .build(),
                                        ImmutableList.of())
                                .step(AggregationNode.Step.SINGLE)
                                .globalGrouping()
                                .source(p.values(ImmutableList.of(p.symbol("orderkey")), ImmutableList.of(expressions("1"))))))
                .matches(values(ImmutableMap.of("count_1", 0)));
    }

    @Test
    public void testFiresOnCountAggregateOverEnforceSingleRow()
    {
        tester().assertThat(new PruneCountAggregationOverScalar(tester().getMetadata()))
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        functionResolution
                                                .functionCallBuilder(QualifiedName.of("count"))
                                                .build(),
                                        ImmutableList.of())
                                .step(AggregationNode.Step.SINGLE)
                                .globalGrouping()
                                .source(p.enforceSingleRow(p.tableScan(ImmutableList.of(), ImmutableMap.of())))))
                .matches(values(ImmutableMap.of("count_1", 0)));
    }

    @Test
    public void testDoesNotFireOnNestedCountAggregateWithNonEmptyGroupBy()
    {
        tester().assertThat(new PruneCountAggregationOverScalar(tester().getMetadata()))
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        functionResolution
                                                .functionCallBuilder(QualifiedName.of("count"))
                                                .build(),
                                        ImmutableList.of())
                                .step(AggregationNode.Step.SINGLE)
                                .globalGrouping()
                                .source(
                                        p.aggregation(aggregationBuilder -> {
                                            aggregationBuilder
                                                    .source(p.tableScan(ImmutableList.of(), ImmutableMap.of()))
                                                    .groupingSets(singleGroupingSet(ImmutableList.of(p.symbol("orderkey"))));
                                        }))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNestedNonCountAggregate()
    {
        tester().assertThat(new PruneCountAggregationOverScalar(tester().getMetadata()))
                .on(p -> {
                    Symbol totalPrice = p.symbol("total_price", DOUBLE);
                    AggregationNode inner = p.aggregation((a) -> a
                            .addAggregation(totalPrice,
                                    functionResolution
                                            .functionCallBuilder(QualifiedName.of("sum"))
                                            .addArgument(DOUBLE, new SymbolReference("totalprice"))
                                            .build(),
                                    ImmutableList.of(DOUBLE))
                            .globalGrouping()
                            .source(
                                    p.project(
                                            Assignments.of(totalPrice, totalPrice.toSymbolReference()),
                                            p.tableScan(
                                                    new TableHandle(
                                                            new CatalogName("local"),
                                                            new TpchTableHandle(TINY_SCHEMA_NAME, "orders", TINY_SCALE_FACTOR),
                                                            TpchTransactionHandle.INSTANCE),
                                                    ImmutableList.of(totalPrice),
                                                    ImmutableMap.of(totalPrice, new TpchColumnHandle(totalPrice.getName(), DOUBLE))))));

                    return p.aggregation((a) -> a
                            .addAggregation(
                                    p.symbol("sum_outer", DOUBLE),
                                    functionResolution
                                            .functionCallBuilder(QualifiedName.of("sum"))
                                            .addArgument(DOUBLE, new SymbolReference("sum_inner"))
                                            .build(),
                                    ImmutableList.of(DOUBLE))
                            .globalGrouping()
                            .source(inner));
                }).doesNotFire();
    }
}
