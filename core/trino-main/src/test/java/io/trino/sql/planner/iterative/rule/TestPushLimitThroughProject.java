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
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushLimitThroughProject
        extends BaseRuleTest
{
    @Test
    public void testPushdownLimitNonIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(1,
                            p.project(
                                    Assignments.of(a, TRUE_LITERAL),
                                    p.values()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression("true")),
                                limit(1, values())));
    }

    @Test
    public void testPushdownLimitWithTiesNNonIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedB = p.symbol("projectedB");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(projectedA, new SymbolReference("a"), projectedB, new SymbolReference("b")),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("projectedA", expression("a"), "projectedB", expression("b")),
                                limit(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testPushdownLimitWithTiesThroughProjectionWithExpression()
    {
        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedC = p.symbol("projectedC");
                    Symbol b = p.symbol("b");
                    return p.limit(
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
                                limit(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testDoNotPushdownLimitWithTiesThroughProjectionWithExpression()
    {
        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedC = p.symbol("projectedC");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            1,
                            ImmutableList.of(projectedC),
                            p.project(
                                    Assignments.of(
                                            projectedA, new SymbolReference("a"),
                                            projectedC, new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("b"))),
                                    p.values(a, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesntPushdownLimitThroughIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(1,
                            p.project(
                                    Assignments.of(a, a.toSymbolReference()),
                                    p.values(a)));
                }).doesNotFire();
    }

    @Test
    public void testDoesntPushDownLimitThroughExclusiveDereferences()
    {
        RowType rowType = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), BIGINT), new RowType.Field(Optional.of("y"), BIGINT)));

        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    return p.limit(1,
                            p.project(
                                    Assignments.of(
                                            p.symbol("b"), new SubscriptExpression(a.toSymbolReference(), new LongLiteral("1")),
                                            p.symbol("c"), new SubscriptExpression(a.toSymbolReference(), new LongLiteral("2"))),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testLimitWithPreSortedInputs()
    {
        // Do not push down order sensitive Limit if input ordering depends on symbol produced by Project
        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedC = p.symbol("projectedC");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            1,
                            false,
                            ImmutableList.of(projectedC),
                            p.project(
                                    Assignments.of(
                                            projectedA, new SymbolReference("a"),
                                            projectedC, new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("b"))),
                                    p.values(a, b)));
                })
                .doesNotFire();

        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedC = p.symbol("projectedC");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            1,
                            ImmutableList.of(),
                            true,
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
                                limit(1, ImmutableList.of(), true, ImmutableList.of("a"), values("a", "b"))));
    }

    @Test
    public void testPushDownLimitThroughOverlappingDereferences()
    {
        RowType rowType = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), BIGINT), new RowType.Field(Optional.of("y"), BIGINT)));

        tester().assertThat(new PushLimitThroughProject(tester().getTypeAnalyzer()))
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    return p.limit(1,
                            p.project(
                                    Assignments.of(
                                            p.symbol("b"), new SubscriptExpression(a.toSymbolReference(), new LongLiteral("1")),
                                            p.symbol("c", rowType), a.toSymbolReference()),
                                    p.values(a)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression("a[1]"), "c", expression("a")),
                                limit(1,
                                        values("a"))));
    }
}
