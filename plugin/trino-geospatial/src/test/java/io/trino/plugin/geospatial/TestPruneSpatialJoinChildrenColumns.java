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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.PruneSpatialJoinChildrenColumns;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.SpatialJoinNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneSpatialJoinChildrenColumns
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(new GeoPlugin());
    private static final ResolvedFunction TEST_ST_DISTANCE_FUNCTION = FUNCTIONS.resolveFunction("st_distance", fromTypes(GEOMETRY, GEOMETRY));

    @Test
    public void testPruneOneChild()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a", GEOMETRY);
                    Symbol b = p.symbol("b", GEOMETRY);
                    Symbol r = p.symbol("r", DOUBLE);
                    Symbol unused = p.symbol("unused");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, unused),
                            p.values(b, r),
                            ImmutableList.of(a, b, r),
                            new Comparison(
                                    LESS_THAN_OR_EQUAL,
                                    new Call(TEST_ST_DISTANCE_FUNCTION, ImmutableList.of(a.toSymbolReference(), b.toSymbolReference())),
                                    r.toSymbolReference()));
                })
                .matches(
                        spatialJoin(
                                new Comparison(LESS_THAN_OR_EQUAL, new Call(TEST_ST_DISTANCE_FUNCTION, ImmutableList.of(new Reference(GEOMETRY, "a"), new Reference(GEOMETRY, "b"))), new Reference(DOUBLE, "r")),
                                Optional.empty(),
                                Optional.of(ImmutableList.of("a", "b", "r")),
                                strictProject(
                                        ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(GEOMETRY, "a"))),
                                        values("a", "unused")),
                                values("b", "r")));
    }

    @Test
    public void testPruneBothChildren()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a", GEOMETRY);
                    Symbol b = p.symbol("b", GEOMETRY);
                    Symbol r = p.symbol("r", DOUBLE);
                    Symbol unusedLeft = p.symbol("unused_left", BIGINT);
                    Symbol unusedRight = p.symbol("unused_right", BIGINT);
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, unusedLeft),
                            p.values(b, r, unusedRight),
                            ImmutableList.of(a, b, r),
                            new Comparison(
                                    LESS_THAN_OR_EQUAL,
                                    new Call(TEST_ST_DISTANCE_FUNCTION, ImmutableList.of(a.toSymbolReference(), b.toSymbolReference())),
                                    r.toSymbolReference()));
                })
                .matches(
                        spatialJoin(
                                new Comparison(LESS_THAN_OR_EQUAL, new Call(TEST_ST_DISTANCE_FUNCTION, ImmutableList.of(new Reference(GEOMETRY, "a"), new Reference(GEOMETRY, "b"))), new Reference(DOUBLE, "r")),
                                Optional.empty(),
                                Optional.of(ImmutableList.of("a", "b", "r")),
                                strictProject(
                                        ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(GEOMETRY, "a"))),
                                        values("a", "unused_left")),
                                strictProject(
                                        ImmutableMap.of("b", PlanMatchPattern.expression(new Reference(GEOMETRY, "b")), "r", PlanMatchPattern.expression(new Reference(DOUBLE, "r"))),
                                        values("b", "r", "unused_right"))));
    }

    @Test
    public void testDoNotPruneOneOutputOrFilterSymbols()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a", GEOMETRY);
                    Symbol b = p.symbol("b", GEOMETRY);
                    Symbol r = p.symbol("r", DOUBLE);
                    Symbol output = p.symbol("output", BIGINT);
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a),
                            p.values(b, r, output),
                            ImmutableList.of(output),
                            new Comparison(LESS_THAN_OR_EQUAL, new Call(TEST_ST_DISTANCE_FUNCTION, ImmutableList.of(new Reference(GEOMETRY, "a"), new Reference(GEOMETRY, "b"))), new Reference(DOUBLE, "r")));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPrunePartitionSymbols()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a", GEOMETRY);
                    Symbol b = p.symbol("b", GEOMETRY);
                    Symbol r = p.symbol("r", DOUBLE);
                    Symbol leftPartitionSymbol = p.symbol("left_partition_symbol", BIGINT);
                    Symbol rightPartitionSymbol = p.symbol("right_partition_symbol", BIGINT);
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, leftPartitionSymbol),
                            p.values(b, r, rightPartitionSymbol),
                            ImmutableList.of(a, b, r),
                            new Comparison(LESS_THAN_OR_EQUAL, new Call(TEST_ST_DISTANCE_FUNCTION, ImmutableList.of(new Reference(GEOMETRY, "a"), new Reference(GEOMETRY, "b"))), new Reference(DOUBLE, "r")),
                            Optional.of(leftPartitionSymbol),
                            Optional.of(rightPartitionSymbol),
                            Optional.of("some nice kdb tree"));
                })
                .doesNotFire();
    }
}
