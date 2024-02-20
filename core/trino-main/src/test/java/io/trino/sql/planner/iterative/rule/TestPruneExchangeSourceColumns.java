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
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneExchangeSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneOneChild()
    {
        tester().assertThat(new PruneExchangeSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c1 = p.symbol("c_1");
                    Symbol c2 = p.symbol("c_2");
                    return p.exchange(e -> e
                            .addSource(p.values(b))
                            .addInputsSet(b)
                            .addSource(p.values(c1, c2))
                            .addInputsSet(c1)
                            .singleDistributionPartitioningScheme(a));
                })
                .matches(
                        exchange(
                                values(ImmutableList.of("b")),
                                strictProject(
                                        ImmutableMap.of("c_1", expression("c_1")),
                                        values(ImmutableList.of("c_1", "c_2")))));
    }

    @Test
    public void testPruneAllChildren()
    {
        tester().assertThat(new PruneExchangeSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b1 = p.symbol("b_1");
                    Symbol b2 = p.symbol("b_2");
                    Symbol c1 = p.symbol("c_1");
                    Symbol c2 = p.symbol("c_2");
                    return p.exchange(e -> e
                            .addSource(p.values(b1, b2))
                            .addInputsSet(b1)
                            .addSource(p.values(c1, c2))
                            .addInputsSet(c1)
                            .singleDistributionPartitioningScheme(a));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("b_1", expression("b_1")),
                                        values(ImmutableList.of("b_1", "b_2"))),
                                strictProject(
                                        ImmutableMap.of("c_1", expression("c_1")),
                                        values(ImmutableList.of("c_1", "c_2")))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneExchangeSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.exchange(e -> e
                            .addSource(p.values(b))
                            .addInputsSet(b)
                            .addSource(p.values(c))
                            .addInputsSet(c)
                            .singleDistributionPartitioningScheme(a));
                })
                .doesNotFire();
    }
}
