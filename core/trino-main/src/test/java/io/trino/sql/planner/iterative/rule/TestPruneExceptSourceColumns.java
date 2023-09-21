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

import static io.trino.sql.planner.assertions.PlanMatchPattern.except;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneExceptSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneOneChild()
    {
        tester().assertThat(new PruneExceptSourceColumns())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.except(
                            ImmutableListMultimap.of(output, a, output, c),
                            ImmutableList.of(
                                    p.values(a, b),
                                    p.values(c)));
                })
                .matches(except(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                values("a", "b")),
                        values("c")));
    }

    @Test
    public void testPruneAllChildren()
    {
        tester().assertThat(new PruneExceptSourceColumns())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.except(
                            ImmutableListMultimap.of(output, a, output, c),
                            ImmutableList.of(
                                    p.values(a, b),
                                    p.values(c, d)));
                })
                .matches(except(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                values("a", "b")),
                        strictProject(
                                ImmutableMap.of("c", expression("c")),
                                values("c", "d"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneExceptSourceColumns())
                .on(p -> {
                    Symbol output = p.symbol("output");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.except(
                            ImmutableListMultimap.of(output, a, output, b),
                            ImmutableList.of(
                                    p.values(a),
                                    p.values(b)));
                })
                .doesNotFire();
    }
}
