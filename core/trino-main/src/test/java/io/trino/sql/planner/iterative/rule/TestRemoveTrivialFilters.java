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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

public class TestRemoveTrivialFilters
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveTrivialFilters())
                .on(p -> p.filter(
                        new ComparisonExpression(EQUAL, new LongLiteral("1"), new LongLiteral("1")),
                        p.values()))
                .doesNotFire();
    }

    @Test
    public void testRemovesTrueFilter()
    {
        tester().assertThat(new RemoveTrivialFilters())
                .on(p -> p.filter(TRUE_LITERAL, p.values()))
                .matches(values());
    }

    @Test
    public void testRemovesFalseFilter()
    {
        tester().assertThat(new RemoveTrivialFilters())
                .on(p -> p.filter(
                        FALSE_LITERAL,
                        p.values(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(ImmutableList.of(new LongLiteral("1"))))))
                .matches(values("a"));
    }

    @Test
    public void testRemovesNullFilter()
    {
        tester().assertThat(new RemoveTrivialFilters())
                .on(p -> p.filter(
                        new Cast(new NullLiteral(), dataType("boolean")),
                        p.values(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(ImmutableList.of(new LongLiteral("1"))))))
                .matches(values(
                        ImmutableList.of("a"),
                        ImmutableList.of()));
    }
}
