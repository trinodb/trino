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
import com.google.common.collect.ImmutableSet;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionNullability;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.util.SpatialJoinUtils.ST_DISTANCE;

public class TestPruneSpatialJoinChildrenColumns
        extends BaseRuleTest
{
    // normally a test can just resolve the function from metadata, but the geo functions are in a plugin that is not visible to this module
    public static final ResolvedFunction TEST_ST_DISTANCE_FUNCTION = new ResolvedFunction(
            new BoundSignature(builtinFunctionName(ST_DISTANCE), BIGINT, ImmutableList.of(BIGINT, BIGINT)),
            GlobalSystemConnector.CATALOG_HANDLE,
            new FunctionId("st_distance"),
            FunctionKind.SCALAR,
            true,
            new FunctionNullability(false, ImmutableList.of(false, false)),
            ImmutableMap.of(),
            ImmutableSet.of());

    @Test
    public void testPruneOneChild()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol unused = p.symbol("unused");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, unused),
                            p.values(b, r),
                            ImmutableList.of(a, b, r),
                            new ComparisonExpression(
                                    LESS_THAN_OR_EQUAL,
                                    new FunctionCall(TEST_ST_DISTANCE_FUNCTION.toQualifiedName(), ImmutableList.of(a.toSymbolReference(), b.toSymbolReference())),
                                    r.toSymbolReference()));
                })
                .matches(
                        spatialJoin(
                                new ComparisonExpression(LESS_THAN_OR_EQUAL, new FunctionCall(QualifiedName.of("st_distance"), ImmutableList.of(new SymbolReference("a"), new SymbolReference("b"))), new SymbolReference("r")),
                                Optional.empty(),
                                Optional.of(ImmutableList.of("a", "b", "r")),
                                strictProject(
                                        ImmutableMap.of("a", PlanMatchPattern.expression(new SymbolReference("a"))),
                                        values("a", "unused")),
                                values("b", "r")));
    }

    @Test
    public void testPruneBothChildren()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol unusedLeft = p.symbol("unused_left");
                    Symbol unusedRight = p.symbol("unused_right");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, unusedLeft),
                            p.values(b, r, unusedRight),
                            ImmutableList.of(a, b, r),
                            new ComparisonExpression(
                                    LESS_THAN_OR_EQUAL,
                                    new FunctionCall(TEST_ST_DISTANCE_FUNCTION.toQualifiedName(), ImmutableList.of(a.toSymbolReference(), b.toSymbolReference())),
                                    r.toSymbolReference()));
                })
                .matches(
                        spatialJoin(
                                new ComparisonExpression(LESS_THAN_OR_EQUAL, new FunctionCall(QualifiedName.of("st_distance"), ImmutableList.of(new SymbolReference("a"), new SymbolReference("b"))), new SymbolReference("r")),
                                Optional.empty(),
                                Optional.of(ImmutableList.of("a", "b", "r")),
                                strictProject(
                                        ImmutableMap.of("a", PlanMatchPattern.expression(new SymbolReference("a"))),
                                        values("a", "unused_left")),
                                strictProject(
                                        ImmutableMap.of("b", PlanMatchPattern.expression(new SymbolReference("b")), "r", PlanMatchPattern.expression(new SymbolReference("r"))),
                                        values("b", "r", "unused_right"))));
    }

    @Test
    public void testDoNotPruneOneOutputOrFilterSymbols()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol output = p.symbol("output");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a),
                            p.values(b, r, output),
                            ImmutableList.of(output),
                            new ComparisonExpression(LESS_THAN_OR_EQUAL, new FunctionCall(QualifiedName.of("st_distance"), ImmutableList.of(new SymbolReference("a"), new SymbolReference("b"))), new SymbolReference("r")));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPrunePartitionSymbols()
    {
        tester().assertThat(new PruneSpatialJoinChildrenColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol r = p.symbol("r");
                    Symbol leftPartitionSymbol = p.symbol("left_partition_symbol");
                    Symbol rightPartitionSymbol = p.symbol("right_partition_symbol");
                    return p.spatialJoin(
                            SpatialJoinNode.Type.INNER,
                            p.values(a, leftPartitionSymbol),
                            p.values(b, r, rightPartitionSymbol),
                            ImmutableList.of(a, b, r),
                            new ComparisonExpression(LESS_THAN_OR_EQUAL, new FunctionCall(QualifiedName.of("st_distance"), ImmutableList.of(new SymbolReference("a"), new SymbolReference("b"))), new SymbolReference("r")),
                            Optional.of(leftPartitionSymbol),
                            Optional.of(rightPartitionSymbol),
                            Optional.of("some nice kdb tree"));
                })
                .doesNotFire();
    }
}
