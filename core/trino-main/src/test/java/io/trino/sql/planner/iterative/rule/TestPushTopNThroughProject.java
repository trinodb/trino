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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.plugin.tpch.TpchTransactionHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushTopNThroughProject
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    private static final RowType rowType = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("x"), BIGINT),
            new RowType.Field(Optional.of("y"), BIGINT)));

    @Test
    public void testPushdownTopNNonIdentityProjection()
    {
        tester().assertThat(new PushTopNThroughProject())
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedB = p.symbol("projectedB");
                    Symbol b = p.symbol("b");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(projectedA, new Reference(BIGINT, "a"), projectedB, new Reference(BIGINT, "b")),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("projectedA", expression(new Reference(BIGINT, "a")), "projectedB", expression(new Reference(BIGINT, "b"))),
                                topN(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testPushdownTopNNonIdentityProjectionWithExpression()
    {
        tester().assertThat(new PushTopNThroughProject())
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedC = p.symbol("projectedC");
                    Symbol b = p.symbol("b");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(
                                            projectedA, new Reference(BIGINT, "a"),
                                            projectedC, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "projectedA", expression(new Reference(BIGINT, "a")),
                                        "projectedC", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))))),
                                topN(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testDoNotPushdownTopNThroughIdentityProjection()
    {
        tester().assertThat(new PushTopNThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.topN(1,
                            ImmutableList.of(a),
                            p.project(
                                    Assignments.of(a, a.toSymbolReference()),
                                    p.values(a)));
                }).doesNotFire();
    }

    @Test
    public void testPushdownTopNThroughProjectionOverFilterOverTableScan()
    {
        TableHandle nationTableHandle = new TableHandle(
                tester().getCurrentCatalogHandle(),
                new TpchTableHandle("sf1", "nation", 1.0),
                TpchTransactionHandle.INSTANCE);

        ColumnHandle nationkeyColumnHandle = new TpchColumnHandle("nationkey", BIGINT);

        tester().assertThat(new PushTopNThroughProject())
                .on(p -> {
                    Symbol projected = p.symbol("projected");
                    Symbol nationkey = p.symbol("nationkey");
                    return p.topN(
                            1,
                            ImmutableList.of(projected),
                            p.project(
                                    Assignments.of(projected, new Reference(BIGINT, "nationkey")),
                                    p.filter(
                                            Booleans.TRUE,
                                            p.tableScan(
                                                    nationTableHandle,
                                                    ImmutableList.of(nationkey),
                                                    ImmutableMap.of(nationkey, nationkeyColumnHandle)))));
                }).matches(
                        project(
                                ImmutableMap.of("projected", expression(new Reference(BIGINT, "nationkey"))),
                                topN(
                                        1,
                                        ImmutableList.of(sort("nationkey", ASCENDING, FIRST)),
                                        filter(
                                                Booleans.TRUE,
                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey"))))));
    }

    @Test
    public void testPushdownTopNThroughProjectionOverTableScan()
    {
        TableHandle nationTableHandle = new TableHandle(
                tester().getCurrentCatalogHandle(),
                new TpchTableHandle("sf1", "nation", 1.0),
                TpchTransactionHandle.INSTANCE);

        ColumnHandle nationkeyColumnHandle = new TpchColumnHandle("nationkey", BIGINT);

        tester().assertThat(new PushTopNThroughProject())
                .on(p -> {
                    Symbol projected = p.symbol("projected");
                    Symbol nationkey = p.symbol("nationkey");
                    return p.topN(
                            1,
                            ImmutableList.of(projected),
                            p.project(
                                    Assignments.of(projected, new Reference(BIGINT, "nationkey")),
                                    p.tableScan(
                                            nationTableHandle,
                                            ImmutableList.of(nationkey),
                                            ImmutableMap.of(nationkey, nationkeyColumnHandle))));
                }).matches(
                        project(
                                ImmutableMap.of("projected", expression(new Reference(BIGINT, "nationkey"))),
                                topN(
                                        1,
                                        ImmutableList.of(sort("nationkey", ASCENDING, FIRST)),
                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))));
    }

    @Test
    public void testDoesntPushDownTopNThroughExclusiveDereferences()
    {
        tester().assertThat(new PushTopNThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    return p.topN(
                            1,
                            ImmutableList.of(p.symbol("c")),
                            p.project(
                                    Assignments.builder()
                                            .put(p.symbol("b"), new FieldReference(a.toSymbolReference(), 0))
                                            .put(p.symbol("c"), new FieldReference(a.toSymbolReference(), 1))
                                            .build(),
                                    p.values(a)));
                }).doesNotFire();
    }

    @Test
    public void testPushTopNThroughOverlappingDereferences()
    {
        tester().assertThat(new PushTopNThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    Symbol d = p.symbol("d");
                    return p.topN(
                            1,
                            ImmutableList.of(d),
                            p.project(
                                    Assignments.builder()
                                            .put(p.symbol("b"), new FieldReference(a.toSymbolReference(), 0))
                                            .put(p.symbol("c", rowType), a.toSymbolReference())
                                            .putIdentity(d)
                                            .build(),
                                    p.values(a, d)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression(new FieldReference(new Reference(rowType, "a"), 0)), "c", expression(new Reference(BIGINT, "a")), "d", expression(new Reference(BIGINT, "d"))),
                                topN(
                                        1,
                                        ImmutableList.of(sort("d", ASCENDING, FIRST)),
                                        values("a", "d"))));
    }
}
