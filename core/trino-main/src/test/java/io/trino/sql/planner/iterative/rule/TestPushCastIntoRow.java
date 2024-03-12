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
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushCastIntoRow
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        // anonymous row type
        test(
                new Cast(new Row(ImmutableList.of(new LongLiteral("1"))), dataType("row(bigint)")),
                new Row(ImmutableList.of(new Cast(new LongLiteral("1"), dataType("bigint")))));
        test(
                new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"))), dataType("row(bigint,varchar)")),
                new Row(ImmutableList.of(new Cast(new LongLiteral("1"), dataType("bigint")), new Cast(new StringLiteral("a"), dataType("varchar")))));
        test(
                new Cast(new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"))), dataType("row(smallint,varchar)")), dataType("row(bigint,varchar)")),
                new Row(ImmutableList.of(new Cast(new Cast(new LongLiteral("1"), dataType("smallint")), dataType("bigint")), new Cast(new Cast(new StringLiteral("a"), dataType("varchar")), dataType("varchar")))));

        // named fields in top-level cast preserved
        test(
                new Cast(new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"))), dataType("row(smallint,varchar)")), dataType("row(\"x\" bigint,varchar)")),
                new Cast(new Row(ImmutableList.of(new Cast(new LongLiteral("1"), dataType("smallint")), new Cast(new StringLiteral("a"), dataType("varchar")))), dataType("row(\"x\" bigint,varchar)")));
        test(
                new Cast(new Cast(new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"))), dataType("row(\"a\" smallint,\"b\" varchar)")), dataType("row(\"x\" bigint,varchar)")),
                new Cast(new Row(ImmutableList.of(new Cast(new LongLiteral("1"), dataType("smallint")), new Cast(new StringLiteral("a"), dataType("varchar")))), dataType("row(\"x\" bigint,varchar)")));

        // expression nested in another unrelated expression
        test(
                new SubscriptExpression(new Cast(new Row(ImmutableList.of(new LongLiteral("1"))), dataType("row(bigint)")), new LongLiteral("1")),
                new SubscriptExpression(new Row(ImmutableList.of(new Cast(new LongLiteral("1"), dataType("bigint")))), new LongLiteral("1")));

        // don't insert CAST(x AS unknown)
        test(
                new Cast(new Row(ImmutableList.of(new NullLiteral())), dataType("row(unknown)")),
                new Row(ImmutableList.of(new NullLiteral())));
    }

    private void test(Expression original, Expression unwrapped)
    {
        tester().assertThat(new PushCastIntoRow().projectExpressionRewrite())
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
