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
import io.trino.spi.type.RowType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushTopNThroughProject
        extends BaseRuleTest
{
    private static final RowType rowType = RowType.from(ImmutableList.of(
            new RowType.Field(Optional.of("x"), BIGINT),
            new RowType.Field(Optional.of("y"), BIGINT)));

    @Test
    public void testPushdownTopNNonIdentityProjection()
    {
        tester().assertThat(new PushTopNThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedB = p.symbol("projectedB");
                    Symbol b = p.symbol("b");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(projectedA, new SymbolReference("a"), projectedB, new SymbolReference("b")),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("projectedA", expression("a"), "projectedB", expression("b")),
                                topN(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testPushdownTopNNonIdentityProjectionWithExpression()
    {
        tester().assertThat(new PushTopNThroughProject(tester().getTypeAnalyzer()))
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
                                            projectedA, new SymbolReference("a"),
                                            projectedC, new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("b"))),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("projectedA", expression("a"), "projectedC", expression("a + b")),
                                topN(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testDoNotPushdownTopNThroughIdentityProjection()
    {
        tester().assertThat(new PushTopNThroughProject(tester().getTypeAnalyzer()))
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
    public void testDoNotPushdownTopNThroughProjectionOverFilterOverTableScan()
    {
        tester().assertThat(new PushTopNThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(projectedA, new SymbolReference("a")),
                                    p.filter(
                                            BooleanLiteral.TRUE_LITERAL,
                                            p.tableScan(ImmutableList.of(), ImmutableMap.of()))));
                }).doesNotFire();
    }

    @Test
    public void testDoNotPushdownTopNThroughProjectionOverTableScan()
    {
        tester().assertThat(new PushTopNThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    return p.topN(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(projectedA, new SymbolReference("a")),
                                    p.tableScan(
                                            ImmutableList.of(a),
                                            ImmutableMap.of(a, new TestingMetadata.TestingColumnHandle("a")))));
                }).doesNotFire();
    }

    @Test
    public void testDoesntPushDownTopNThroughExclusiveDereferences()
    {
        tester().assertThat(new PushTopNThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    return p.topN(
                            1,
                            ImmutableList.of(p.symbol("c")),
                            p.project(
                                    Assignments.builder()
                                            .put(p.symbol("b"), new SubscriptExpression(a.toSymbolReference(), new LongLiteral("1")))
                                            .put(p.symbol("c"), new SubscriptExpression(a.toSymbolReference(), new LongLiteral("2")))
                                            .build(),
                                    p.values(a)));
                }).doesNotFire();
    }

    @Test
    public void testPushTopNThroughOverlappingDereferences()
    {
        tester().assertThat(new PushTopNThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    Symbol d = p.symbol("d");
                    return p.topN(
                            1,
                            ImmutableList.of(d),
                            p.project(
                                    Assignments.builder()
                                            .put(p.symbol("b"), new SubscriptExpression(a.toSymbolReference(), new LongLiteral("1")))
                                            .put(p.symbol("c", rowType), a.toSymbolReference())
                                            .putIdentity(d)
                                            .build(),
                                    p.values(a, d)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression("a[1]"), "c", expression("a"), "d", expression("d")),
                                topN(
                                        1,
                                        ImmutableList.of(sort("d", ASCENDING, FIRST)),
                                        values("a", "d"))));
    }
}
