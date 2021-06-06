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
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushLimitThroughSemiJoin
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new PushLimitThroughSemiJoin())
                .on(p -> p.limit(1, buildSemiJoin(p)))
                .matches(
                        semiJoin(
                                "leftKey", "rightKey", "match",
                                limit(1, values("leftKey")),
                                values("rightKey")));
    }

    @Test
    public void testPushLimitWithTies()
    {
        tester().assertThat(new PushLimitThroughSemiJoin())
                .on(p ->
                        p.limit(
                                1,
                                ImmutableList.of(p.symbol("leftKey")),
                                buildSemiJoin(p)))
                .matches(
                        semiJoin(
                                "leftKey", "rightKey", "match",
                                limit(1, ImmutableList.of(sort("leftKey", ASCENDING, FIRST)), values("leftKey")),
                                values("rightKey")));
    }

    @Test
    public void testPushLimitWithPreSortedInputs()
    {
        tester().assertThat(new PushLimitThroughSemiJoin())
                .on(p ->
                        p.limit(
                                1,
                                false,
                                ImmutableList.of(p.symbol("leftKey")),
                                buildSemiJoin(p)))
                .matches(
                        semiJoin(
                                "leftKey", "rightKey", "match",
                                limit(
                                        1,
                                        ImmutableList.of(),
                                        false,
                                        ImmutableList.of("leftKey"),
                                        values("leftKey")),
                                values("rightKey")));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new PushLimitThroughSemiJoin())
                .on(p ->
                        p.semiJoin(
                                p.symbol("leftKey"),
                                p.symbol("rightKey"),
                                p.symbol("output"),
                                Optional.empty(),
                                Optional.empty(),
                                p.values(p.symbol("leftKey")),
                                p.limit(1,
                                        p.values(p.symbol("rightKey")))))
                .doesNotFire();

        tester().assertThat(new PushLimitThroughSemiJoin())
                .on(p -> p.limit(
                        1,
                        ImmutableList.of(p.symbol("match")),
                        buildSemiJoin(p)))
                .doesNotFire();

        // Do not push down Limit with pre-sorted inputs if input ordering depends on symbol produced by SemiJoin
        tester().assertThat(new PushLimitThroughSemiJoin())
                .on(p -> p.limit(
                        1,
                        false,
                        ImmutableList.of(p.symbol("match")),
                        buildSemiJoin(p)))
                .doesNotFire();
    }

    private static PlanNode buildSemiJoin(PlanBuilder p)
    {
        Symbol leftKey = p.symbol("leftKey");
        Symbol rightKey = p.symbol("rightKey");
        return p.semiJoin(
                leftKey,
                rightKey,
                p.symbol("match"),
                Optional.empty(),
                Optional.empty(),
                p.values(leftKey),
                p.values(rightKey));
    }
}
