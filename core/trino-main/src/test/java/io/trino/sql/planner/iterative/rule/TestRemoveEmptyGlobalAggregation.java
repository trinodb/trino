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

import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveEmptyGlobalAggregation
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveEmptyGlobalAggregation())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.aggregation(aggregation ->
                            aggregation.singleGroupingSet(a)
                                    .source(p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void test()
    {
        tester().assertThat(new RemoveEmptyGlobalAggregation())
                .on(p -> p.aggregation(aggregation ->
                        aggregation
                                .globalGrouping()
                                .source(p.values(p.symbol("a")))))
                .matches(values(1));
    }
}
