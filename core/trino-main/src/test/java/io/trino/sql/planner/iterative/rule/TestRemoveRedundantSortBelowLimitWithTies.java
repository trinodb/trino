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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestRemoveRedundantSortBelowLimitWithTies
        extends BaseRuleTest
{
    @Test
    public void testRemoveSort()
    {
        tester().assertThat(new RemoveRedundantSortBelowLimitWithTies())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            5,
                            ImmutableList.of(a, b),
                            p.sort(
                                    ImmutableList.of(a, b),
                                    p.values(a, b)));
                })
                .matches(
                        limit(
                                5,
                                ImmutableList.of(sort("a", ASCENDING, FIRST), sort("b", ASCENDING, FIRST)),
                                values("a", "b")));

        tester().assertThat(new RemoveRedundantSortBelowLimitWithTies())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.limit(
                            5,
                            ImmutableList.of(a, b),
                            p.sort(
                                    ImmutableList.of(c, d),
                                    p.values(a, b, c, d)));
                })
                .matches(
                        limit(
                                5,
                                ImmutableList.of(sort("a", ASCENDING, FIRST), sort("b", ASCENDING, FIRST)),
                                values("a", "b", "c", "d")));
    }
}
