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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestMergeLimitOverProjectWithSort
        extends BaseRuleTest
{
    @Test
    public void testMergeLimitOverProjectWithSort()
    {
        tester().assertThat(new MergeLimitOverProjectWithSort())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            1,
                            p.project(
                                    Assignments.identity(b),
                                    p.sort(
                                            ImmutableList.of(a),
                                            p.values(a, b))));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression("b")),
                                topN(
                                        1,
                                        ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                        values("a", "b"))));
    }

    @Test
    public void doNotMergeLimitWithTies()
    {
        tester().assertThat(new MergeLimitOverProjectWithSort())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.limit(
                            1,
                            ImmutableList.of(b),
                            p.project(
                                    Assignments.identity(b),
                                    p.sort(
                                            ImmutableList.of(a),
                                            p.values(a, b))));
                })
                .doesNotFire();
    }

    @Test
    public void testLimitWithPreSortedInputs()
    {
        tester().assertThat(new MergeLimitOverProjectWithSort())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    List<Symbol> orderBy = ImmutableList.of(a);
                    return p.limit(
                            1,
                            false,
                            orderBy,
                            p.project(
                                    Assignments.identity(b),
                                    p.sort(orderBy, p.values(a, b))));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression("b")),
                                topN(
                                        1,
                                        ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                        values("a", "b"))));
    }
}
