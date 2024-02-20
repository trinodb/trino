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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestEvaluateEmptyIntersect
        extends BaseRuleTest
{
    @Test
    public void testDoesNotApply()
    {
        tester().assertThat(new EvaluateEmptyIntersect())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol input1 = p.symbol("input1");
                    Symbol input2 = p.symbol("input2");

                    return p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(output, input1)
                                    .put(output, input2)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, input1),
                                    p.values(2, input2)));
                })
                .doesNotFire();
    }

    @Test
    public void testRemoveIntersectWhenSetEmpty()
    {
        tester().assertThat(new EvaluateEmptyIntersect())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol input1 = p.symbol("input1");
                    Symbol input2 = p.symbol("input2");
                    Symbol input3 = p.symbol("input3");
                    Symbol input4 = p.symbol("input4");

                    return p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(output, input1)
                                    .put(output, input2)
                                    .put(output, input3)
                                    .put(output, input4)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, input1),
                                    p.values(2, input2),
                                    p.values(0, input3),
                                    p.values(3, input4)));
                })
                .matches(values("output"));
    }
}
