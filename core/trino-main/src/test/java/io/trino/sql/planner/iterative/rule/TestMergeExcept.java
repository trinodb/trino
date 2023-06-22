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
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.except;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestMergeExcept
        extends BaseRuleTest
{
    @Test
    public void testFlattening()
    {
        tester().assertThat(new MergeExcept())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    ExceptNode u1 = p.except(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(c, a)
                                    .put(c, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, a),
                                    p.values(1, b)));
                    ExceptNode u2 = p.except(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(d, a)
                                    .put(d, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, a),
                                    p.values(1, b)));
                    return p.except(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(e, c)
                                    .put(e, d)
                                    .build(),
                            ImmutableList.of(u1, u2));
                })
                .matches(
                        except(values("a"), values("b"), except(values("a"), values("b"))));
    }

    @Test
    public void testNotFlattening()
    {
        tester().assertThat(new MergeExcept())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    ExceptNode u2 = p.except(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(d, a)
                                    .put(d, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, a),
                                    p.values(1, b)));
                    return p.except(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(e, c)
                                    .put(e, d)
                                    .build(),
                            ImmutableList.of(p.values(1, c), u2));
                })
                .doesNotFire();
    }

    @Test
    public void testQuantifiers()
    {
        tester().assertThat(new MergeExcept())
                .on(p -> buildNestedExcept(p, true, true))
                .matches(except(true, values("v_1"), values("v_2"), values("b")));

        tester().assertThat(new MergeExcept())
                .on(p -> buildNestedExcept(p, true, false))
                .matches(except(true, values("v_1"), values("v_2"), values("b")));

        tester().assertThat(new MergeExcept())
                .on(p -> buildNestedExcept(p, false, true))
                .doesNotFire();

        tester().assertThat(new MergeExcept())
                .on(p -> buildNestedExcept(p, false, false))
                .matches(except(false, values("v_1"), values("v_2"), values("b")));
    }

    private PlanNode buildNestedExcept(PlanBuilder builder, boolean sourceDistinct, boolean parentDistinct)
    {
        Symbol v1 = builder.symbol("v_1");
        Symbol v2 = builder.symbol("v_2");
        Symbol a = builder.symbol("a");
        Symbol b = builder.symbol("b");
        Symbol c = builder.symbol("c");

        ExceptNode child1 = builder.except(
                ImmutableListMultimap.of(a, v1, a, v2),
                ImmutableList.of(builder.values(v1), builder.values(v2)),
                sourceDistinct);

        ValuesNode child2 = builder.values(b);

        return builder.except(
                ImmutableListMultimap.of(c, a, c, b),
                ImmutableList.of(child1, child2),
                parentDistinct);
    }
}
