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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushLimitThroughProject
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testPushdownLimitNonIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a", BOOLEAN);
                    return p.limit(1,
                            p.project(
                                    Assignments.of(a, TRUE),
                                    p.values()));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression(TRUE)),
                                limit(1, values())));
    }

    @Test
    public void testPushdownLimitWithTiesNNonIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    Symbol projectedA = p.symbol("projectedA");
                    Symbol a = p.symbol("a");
                    Symbol projectedB = p.symbol("projectedB");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            1,
                            ImmutableList.of(projectedA),
                            p.project(
                                    Assignments.of(projectedA, new Reference(BIGINT, "a"), projectedB, new Reference(BIGINT, "b")),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("projectedA", expression(new Reference(BIGINT, "a")), "projectedB", expression(new Reference(BIGINT, "b"))),
                                limit(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testPushdownLimitWithTiesThroughProjectionWithExpression()
    {
        tester().assertThat(new PushLimitThroughProject())
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
                                            projectedA, new Reference(BIGINT, "a"),
                                            projectedC, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of(
                                        "projectedA", expression(new Reference(BIGINT, "a")),
                                        "projectedC", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))))),
                                limit(1, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a", "b"))));
    }

    @Test
    public void testDoNotPushdownLimitWithTiesThroughProjectionWithExpression()
    {
        tester().assertThat(new PushLimitThroughProject())
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
                                            projectedA, new Reference(BIGINT, "a"),
                                            projectedC, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))),
                                    p.values(a, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesntPushdownLimitThroughIdentityProjection()
    {
        tester().assertThat(new PushLimitThroughProject())
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

        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    return p.limit(1,
                            p.project(
                                    Assignments.of(
                                            p.symbol("b"), new FieldReference(a.toSymbolReference(), 0),
                                            p.symbol("c"), new FieldReference(a.toSymbolReference(), 1)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testLimitWithPreSortedInputs()
    {
        // Do not push down order sensitive Limit if input ordering depends on symbol produced by Project
        tester().assertThat(new PushLimitThroughProject())
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
                                            projectedA, new Reference(BIGINT, "a"),
                                            projectedC, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))),
                                    p.values(a, b)));
                })
                .doesNotFire();

        tester().assertThat(new PushLimitThroughProject())
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
                                            projectedA, new Reference(BIGINT, "a"),
                                            projectedC, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))),
                                    p.values(a, b)));
                })
                .matches(
                        project(
                                ImmutableMap.of("projectedA", expression(new Reference(BIGINT, "a")), "projectedC", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))))),
                                limit(1, ImmutableList.of(), true, ImmutableList.of("a"), values("a", "b"))));
    }

    @Test
    public void testPushDownLimitThroughOverlappingDereferences()
    {
        RowType rowType = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), BIGINT), new RowType.Field(Optional.of("y"), BIGINT)));

        tester().assertThat(new PushLimitThroughProject())
                .on(p -> {
                    Symbol a = p.symbol("a", rowType);
                    return p.limit(1,
                            p.project(
                                    Assignments.of(
                                            p.symbol("b"), new FieldReference(a.toSymbolReference(), 0),
                                            p.symbol("c", rowType), a.toSymbolReference()),
                                    p.values(a)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", io.trino.sql.planner.assertions.PlanMatchPattern.expression(new FieldReference(new Reference(rowType, "a"), 0)), "c", expression(new Reference(rowType, "a"))),
                                limit(1,
                                        values("a"))));
    }
}
