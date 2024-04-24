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
package io.trino.plugin.geospatial;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteSpatialPartitioningAggregation
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(new GeoPlugin());
    private static final ResolvedFunction ST_ENVELOPE = FUNCTIONS.resolveFunction("st_envelope", fromTypes(GEOMETRY));

    public TestRewriteSpatialPartitioningAggregation()
    {
        super(new GeoPlugin());
    }

    @Test
    public void testDoesNotFire()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(a ->
                        a.globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .addAggregation(p.symbol("sp"), PlanBuilder.aggregation("spatial_partitioning", ImmutableList.of(new Reference(GEOMETRY, "geometry"), new Reference(INTEGER, "n"))), ImmutableList.of(GEOMETRY, INTEGER))
                                .source(p.values(p.symbol("geometry"), p.symbol("n")))))
                .doesNotFire();
    }

    @Test
    public void test()
    {
        assertRuleApplication()
                .on(p -> p.aggregation(a ->
                        a.globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .addAggregation(p.symbol("sp"), PlanBuilder.aggregation("spatial_partitioning", ImmutableList.of(new Reference(GEOMETRY, "geometry"))), ImmutableList.of(GEOMETRY))
                                .source(p.values(p.symbol("geometry")))))
                .matches(
                        aggregation(
                                ImmutableMap.of("sp", aggregationFunction("spatial_partitioning", ImmutableList.of("envelope", "partition_count"))),
                                project(
                                        ImmutableMap.of("partition_count", expression(new Constant(INTEGER, 100L)),
                                                "envelope", expression(new Call(ST_ENVELOPE, ImmutableList.of(new Reference(GEOMETRY, "geometry"))))),
                                        values("geometry"))));

        assertRuleApplication()
                .on(p -> p.aggregation(a ->
                        a.globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .addAggregation(p.symbol("sp"), PlanBuilder.aggregation("spatial_partitioning", ImmutableList.of(new Reference(GEOMETRY, "envelope"))), ImmutableList.of(GEOMETRY))
                                .source(p.values(p.symbol("envelope")))))
                .matches(
                        aggregation(
                                ImmutableMap.of("sp", aggregationFunction("spatial_partitioning", ImmutableList.of("envelope", "partition_count"))),
                                project(
                                        ImmutableMap.of("partition_count", expression(new Constant(INTEGER, 100L)),
                                                "envelope", expression(new Call(ST_ENVELOPE, ImmutableList.of(new Reference(GEOMETRY, "geometry"))))),
                                        values("geometry"))));
    }

    private RuleBuilder assertRuleApplication()
    {
        return tester().assertThat(new RewriteSpatialPartitioningAggregation(tester().getPlannerContext()));
    }
}
