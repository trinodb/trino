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
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneMarkDistinctColumns
        extends BaseRuleTest
{
    @Test
    public void testMarkerSymbolNotReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p -> {
                    Symbol key = p.symbol("key");
                    Symbol key2 = p.symbol("key2");
                    Symbol mark = p.symbol("mark");
                    Symbol unused = p.symbol("unused");
                    return p.project(
                            Assignments.of(key2, key.toSymbolReference()),
                            p.markDistinct(mark, ImmutableList.of(key), p.values(key, unused)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("key2", expression(new Reference(BIGINT, "key"))),
                                values(ImmutableList.of("key", "unused"))));
    }

    @Test
    public void testSourceSymbolNotReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p -> {
                    Symbol key = p.symbol("key");
                    Symbol mark = p.symbol("mark");
                    Symbol unused = p.symbol("unused");
                    return p.project(
                            Assignments.identity(mark),
                            p.markDistinct(
                                    mark,
                                    ImmutableList.of(key),
                                    p.values(key, unused)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("mark", expression(new Reference(BOOLEAN, "mark"))),
                                markDistinct("mark", ImmutableList.of("key"),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "key", expression(new Reference(BIGINT, "key"))),
                                                values(ImmutableList.of("key", "unused"))))));
    }

    @Test
    public void testKeySymbolNotReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p -> {
                    Symbol key = p.symbol("key");
                    Symbol mark = p.symbol("mark");
                    return p.project(
                            Assignments.identity(mark),
                            p.markDistinct(mark, ImmutableList.of(key), p.values(key)));
                })
                .doesNotFire();
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneMarkDistinctColumns())
                .on(p -> {
                    Symbol key = p.symbol("key");
                    Symbol mark = p.symbol("mark");
                    return p.project(
                            Assignments.identity(key, mark),
                            p.markDistinct(mark, ImmutableList.of(key), p.values(key)));
                })
                .doesNotFire();
    }
}
