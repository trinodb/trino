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
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneUnionSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneOneChild()
    {
        tester().assertThat(new PruneUnionSourceColumns())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.union(
                            ImmutableListMultimap.of(output, a, output, c, output, d),
                            ImmutableList.of(
                                    p.values(a, b),
                                    p.values(c),
                                    p.values(d)));
                })
                .matches(union(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                values("a", "b")),
                        values("c"),
                        values("d")));
    }

    @Test
    public void testPruneAllChildren()
    {
        tester().assertThat(new PruneUnionSourceColumns())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    Symbol f = p.symbol("f");
                    return p.union(
                            ImmutableListMultimap.of(output, a, output, c, output, e),
                            ImmutableList.of(
                                    p.values(a, b),
                                    p.values(c, d),
                                    p.values(e, f)));
                })
                .matches(union(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                values("a", "b")),
                        strictProject(
                                ImmutableMap.of("c", expression("c")),
                                values("c", "d")),
                        strictProject(
                                ImmutableMap.of("e", expression("e")),
                                values("e", "f"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneUnionSourceColumns())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.union(
                            ImmutableListMultimap.of(output, a, output, b, output, c),
                            ImmutableList.of(
                                    p.values(a),
                                    p.values(b),
                                    p.values(c)));
                })
                .doesNotFire();
    }
}
