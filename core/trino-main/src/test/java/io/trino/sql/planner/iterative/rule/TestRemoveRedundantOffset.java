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
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestRemoveRedundantOffset
        extends BaseRuleTest
{
    @Test
    public void testOffsetEqualToSubplanCardinality()
    {
        tester().assertThat(new RemoveRedundantOffset())
                .on(p -> p.offset(10, p.values(10)))
                .matches(values(ImmutableList.of(), ImmutableList.of()));
    }

    @Test
    public void testOffsetExceedsSubplanCardinality()
    {
        tester().assertThat(new RemoveRedundantOffset())
                .on(p -> p.offset(10, p.values(5)))
                .matches(values(ImmutableList.of(), ImmutableList.of()));
    }

    @Test
    public void testOffsetEqualToZero()
    {
        tester().assertThat(new RemoveRedundantOffset())
                .on(p -> p.offset(
                        0,
                        p.values(
                                ImmutableList.of(p.symbol("a")),
                                ImmutableList.of(
                                        ImmutableList.of(expression("1")),
                                        ImmutableList.of(expression("2"))))))
                .matches(
                        values(
                                ImmutableList.of("a"),
                                ImmutableList.of(
                                        ImmutableList.of(expression("1")),
                                        ImmutableList.of(expression("2")))));
    }

    @Test
    public void testDoNotFireWhenOffsetLowerThanSubplanCardinality()
    {
        tester().assertThat(new RemoveRedundantOffset())
                .on(p -> p.offset(5, p.values(10)))
                .doesNotFire();
    }
}
