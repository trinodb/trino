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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.RowType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.LongLiteral;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushProjectionThroughUnion
        extends BaseRuleTest
{
    private static final RowType ROW_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("x"), BIGINT), new RowType.Field(Optional.of("y"), BIGINT)));

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new PushProjectionThroughUnion())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), new LongLiteral("3")),
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testTrivialProjection()
    {
        tester().assertThat(new PushProjectionThroughUnion())
                .on(p -> {
                    Symbol left = p.symbol("left");
                    Symbol right = p.symbol("right");
                    Symbol unioned = p.symbol("unioned");
                    Symbol renamed = p.symbol("renamed");
                    return p.project(
                            Assignments.of(renamed, unioned.toSymbolReference()),
                            p.union(
                                    ImmutableListMultimap.<Symbol, Symbol>builder()
                                            .put(unioned, left)
                                            .put(unioned, right)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(left),
                                            p.values(right))));
                })
                .doesNotFire();
    }

    @Test
    public void test()
    {
        tester().assertThat(new PushProjectionThroughUnion())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d", ROW_TYPE);
                    Symbol cTimes3 = p.symbol("c_times_3");
                    Symbol dX = p.symbol("d_x");
                    Symbol z = p.symbol("z", ROW_TYPE);
                    Symbol w = p.symbol("w", ROW_TYPE);
                    return p.project(
                            Assignments.of(
                                    cTimes3, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, c.toSymbolReference(), new LongLiteral("3")),
                                    dX, PlanBuilder.expression("d.x")),
                            p.union(
                                    ImmutableListMultimap.<Symbol, Symbol>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .put(d, z)
                                            .put(d, w)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(a, z),
                                            p.values(b, w))));
                })
                .matches(
                        union(
                                project(
                                        ImmutableMap.of("a_times_3", expression("a * 3"), "z_x", expression("z.x")),
                                        values(ImmutableList.of("a", "z"))),
                                project(
                                        ImmutableMap.of("b_times_3", expression("b * 3"), "w_x", expression("w.x")),
                                        values(ImmutableList.of("b", "w"))))
                                .withNumberOfOutputColumns(2)
                                .withAlias("a_times_3")
                                .withAlias("b_times_3")
                                .withAlias("z_x")
                                .withAlias("w_x"));
    }
}
