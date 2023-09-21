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
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.intersect;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestMergeIntersect
        extends BaseRuleTest
{
    @Test
    public void testFlattening()
    {
        tester().assertThat(new MergeIntersect())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    IntersectNode u1 = p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(c, a)
                                    .put(c, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, a),
                                    p.values(1, b)));
                    IntersectNode u2 = p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(d, a)
                                    .put(d, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, a),
                                    p.values(1, b)));
                    return p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(e, c)
                                    .put(e, d)
                                    .build(),
                            ImmutableList.of(u1, u2));
                })
                .matches(
                        intersect(values("a"), values("b"), values("a"), values("b")));
    }

    @Test
    public void testMixedFlattening()
    {
        tester().assertThat(new MergeIntersect())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    IntersectNode u1 = p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(c, a)
                                    .put(c, b)
                                    .build(),
                            ImmutableList.of(
                                    p.values(1, a),
                                    p.values(1, b)));
                    return p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(e, c)
                                    .put(e, d)
                                    .build(),
                            ImmutableList.of(u1, p.values(1, d)));
                })
                .matches(intersect(values("a"), values("b"), values("d")));
    }

    @Test
    public void testNotFlattening()
    {
        tester().assertThat(new MergeIntersect())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.intersect(
                            ImmutableListMultimap.<Symbol, Symbol>builder()
                                    .put(c, a)
                                    .put(c, b)
                                    .build(),
                            ImmutableList.of(p.values(1, a), p.values(1, b)));
                })
                .doesNotFire();
    }

    @Test
    public void testQuantifiers()
    {
        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, true, true, true))
                .matches(intersect(true, values("v_1"), values("v_2"), values("v_3"), values("v_4")));

        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, true, true, false))
                .matches(intersect(true, values("v_1"), values("v_2"), values("v_3"), values("v_4")));

        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, true, false, true))
                .matches(intersect(true, values("v_1"), values("v_2"), values("v_3"), values("v_4")));

        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, false, true, true))
                .matches(intersect(true, values("v_1"), values("v_2"), values("v_3"), values("v_4")));

        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, true, false, false))
                .matches(intersect(true, values("v_1"), values("v_2"), values("v_3"), values("v_4")));

        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, false, false, true))
                .matches(intersect(true, values("v_1"), values("v_2"), values("v_3"), values("v_4")));

        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, false, true, false))
                .matches(intersect(true, values("v_1"), values("v_2"), values("v_3"), values("v_4")));

        tester().assertThat(new MergeIntersect())
                .on(p -> buildNestedIntersect(p, false, false, false))
                .matches(intersect(false, values("v_1"), values("v_2"), values("v_3"), values("v_4")));
    }

    private PlanNode buildNestedIntersect(PlanBuilder builder, boolean lefSourceDistinct, boolean rightSourceDistinct, boolean parentDistinct)
    {
        Symbol v1 = builder.symbol("v_1");
        Symbol v2 = builder.symbol("v_2");
        Symbol v3 = builder.symbol("v_3");
        Symbol v4 = builder.symbol("v_4");
        Symbol a = builder.symbol("a");
        Symbol b = builder.symbol("b");
        Symbol c = builder.symbol("c");

        IntersectNode child1 = builder.intersect(
                ImmutableListMultimap.of(a, v1, a, v2),
                ImmutableList.of(builder.values(v1), builder.values(v2)),
                lefSourceDistinct);

        IntersectNode child2 = builder.intersect(
                ImmutableListMultimap.of(b, v3, b, v4),
                ImmutableList.of(builder.values(v3), builder.values(v4)),
                rightSourceDistinct);

        return builder.intersect(
                ImmutableListMultimap.of(c, a, c, b),
                ImmutableList.of(child1, child2),
                parentDistinct);
    }
}
