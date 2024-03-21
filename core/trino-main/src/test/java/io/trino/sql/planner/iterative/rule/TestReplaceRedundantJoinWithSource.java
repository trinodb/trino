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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static java.util.Collections.nCopies;

public class TestReplaceRedundantJoinWithSource
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnJoinWithEmptySource()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1),
                                p.values(0, p.symbol("a"))))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(0, p.symbol("a")),
                                p.values(1)))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(1),
                                p.values(0, p.symbol("a"))))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(1),
                                p.values(0, p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnJoinWithNoScalarSource()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(10, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(10, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(10, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(10, p.symbol("a")),
                                p.values(10, p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testReplaceCrossJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(10, p.symbol("a", BIGINT)),
                                p.values(1)))
                .matches(
                        values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1),
                                p.values(10, p.symbol("b", BIGINT))))
                .matches(
                        values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));
    }

    @Test
    public void testReplaceInnerJoinWithFilter()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(10, p.symbol("a", BIGINT)),
                                p.values(1),
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L))))
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)),
                                values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null))))));

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(1),
                                p.values(10, p.symbol("b", BIGINT)),
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Constant(BIGINT, 0L))))
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Constant(BIGINT, 0L)),
                                values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null))))));
    }

    @Test
    public void testReplaceLeftJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(10, p.symbol("a", BIGINT)),
                                p.values(1)))
                .matches(
                        values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));

        // in case of outer join, filter does not affect the result
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(10, p.symbol("a", BIGINT)),
                                p.values(1),
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L))))
                .matches(
                        values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));
    }

    @Test
    public void testReplaceRightJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(1),
                                p.values(10, p.symbol("b", BIGINT))))
                .matches(
                        values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));

        // in case of outer join, filter does not affect the result
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(1),
                                p.values(10, p.symbol("b", BIGINT)),
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Constant(BIGINT, 0L))))
                .matches(
                        values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));
    }

    @Test
    public void testReplaceFullJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(10, p.symbol("a", BIGINT)),
                                p.values(1)))
                .matches(
                        values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(1),
                                p.values(10, p.symbol("b", BIGINT))))
                .matches(
                        values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));

        // in case of outer join, filter does not affect the result
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(1),
                                p.values(10, p.symbol("b", BIGINT)),
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "b"), new Constant(BIGINT, 0L))))
                .matches(
                        values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null)))));

        // Right source is scalar with no outputs. Left source cannot be determined to be at least scalar.
        // In such case, FULL join cannot be replaced with left source. The result would be incorrect
        // if left source was empty.
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                FULL,
                                p.filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 5L)),
                                        p.values(10, p.symbol("a", BIGINT))),
                                p.values(1)))
                .doesNotFire();

        // Left source is scalar with no outputs. Right source cannot be determined to be at least scalar.
        // In such case, FULL join cannot be replaced with right source. The result would be incorrect
        // if right source was empty.
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(1),
                                p.filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 5L)),
                                        p.values(10, p.symbol("a", BIGINT)))))
                .doesNotFire();
    }

    @Test
    public void testPruneOutputs()
    {
        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.join(
                            LEFT,
                            p.values(10, a, b),
                            p.values(1),
                            ImmutableList.of(),
                            ImmutableList.of(a),
                            ImmutableList.of(),
                            Optional.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(BIGINT, "a"))),
                                values(ImmutableList.of("a", "b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, null))))));

        tester().assertThat(new ReplaceRedundantJoinWithSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(10, a, b),
                            p.values(1),
                            ImmutableList.of(),
                            ImmutableList.of(a),
                            ImmutableList.of(),
                            Optional.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression(new Reference(BIGINT, "a"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                        values(ImmutableList.of("a", "b"), nCopies(10, ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, null)))))));
    }
}
