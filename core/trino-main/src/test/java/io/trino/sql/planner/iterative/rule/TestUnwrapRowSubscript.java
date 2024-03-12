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
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SubscriptExpression;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestUnwrapRowSubscript
        extends BaseRuleTest
{
    @Test
    public void testSimpleSubscript()
    {
        test(new SubscriptExpression(new Row(ImmutableList.of(new LongLiteral("1"))), new LongLiteral("1")), new LongLiteral("1"));
        test(new SubscriptExpression(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), new LongLiteral("1")), new LongLiteral("1"));
        test(new SubscriptExpression(new SubscriptExpression(new Row(ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), new LongLiteral("3"))), new LongLiteral("1")), new LongLiteral("2")), new LongLiteral("2"));
    }

    @Test
    public void testWithCast()
    {
        test(
                new SubscriptExpression(new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), dataType("row(\"a\" bigint,\"b\" bigint)")), new LongLiteral("1")),
                new Cast(new LongLiteral("1"), dataType("bigint")));

        test(
                new SubscriptExpression(new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), dataType("row(bigint,bigint)")), new LongLiteral("1")),
                new Cast(new LongLiteral("1"), dataType("bigint")));

        test(
                new SubscriptExpression(new Cast(new SubscriptExpression(new Cast(new Row(ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), new LongLiteral("3"))), dataType("row(row(smallint,smallint),bigint)")), new LongLiteral("1")), dataType("row(\"x\" bigint,\"y\" bigint)")), new LongLiteral("2")),
                new Cast(new Cast(new LongLiteral("2"), dataType("smallint")), dataType("bigint")));
    }

    @Test
    public void testWithTryCast()
    {
        test(
                new SubscriptExpression(new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), dataType("row(\"a\" bigint,\"b\" bigint)"), true), new LongLiteral("1")),
                new Cast(new LongLiteral("1"), dataType("bigint"), true));

        test(
                new SubscriptExpression(new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), dataType("row(bigint,bigint)"), true), new LongLiteral("1")),
                new Cast(new LongLiteral("1"), dataType("bigint"), true));

        test(
                new SubscriptExpression(new Cast(new SubscriptExpression(new Cast(new Row(ImmutableList.of(new Row(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))), new LongLiteral("3"))), dataType("row(row(smallint,smallint),bigint)"), true), new LongLiteral("1")), dataType("row(\"x\" bigint,\"y\" bigint)"), true), new LongLiteral("2")),
                new Cast(new Cast(new LongLiteral("2"), dataType("smallint"), true), dataType("bigint"), true));
    }

    private void test(Expression original, Expression unwrapped)
    {
        tester().assertThat(new UnwrapRowSubscript().projectExpressionRewrite())
                .on(p -> p.project(
                        Assignments.builder()
                                .put(p.symbol("output"), original)
                                .build(),
                        p.values()))
                .matches(
                        project(Map.of("output", PlanMatchPattern.expression(unwrapped)),
                                values()));
    }
}
