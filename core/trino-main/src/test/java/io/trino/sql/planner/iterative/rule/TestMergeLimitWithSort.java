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
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestMergeLimitWithSort
        extends BaseRuleTest
{
    @Test
    public void testMergeLimitWithSort()
    {
        tester().assertThat(new MergeLimitWithSort())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            1,
                            p.sort(
                                    ImmutableList.of(a),
                                    p.values(a)));
                })
                .matches(
                        topN(
                                1,
                                ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                values("a")));
    }

    @Test
    public void doNotMergeLimitWithTies()
    {
        tester().assertThat(new MergeLimitWithSort())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            1,
                            ImmutableList.of(a),
                            p.sort(
                                    ImmutableList.of(a),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testLimitWithPreSortedInputs()
    {
        tester().assertThat(new MergeLimitWithSort())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    List<Symbol> orderBy = ImmutableList.of(a);
                    return p.limit(2, false, orderBy, p.sort(orderBy, p.values(a)));
                })
                .matches(
                        topN(2, ImmutableList.of(sort("a", ASCENDING, FIRST)), values("a")));
    }
}
