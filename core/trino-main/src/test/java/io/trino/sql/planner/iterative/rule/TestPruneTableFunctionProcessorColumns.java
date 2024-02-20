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
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneTableFunctionProcessorColumns
        extends BaseRuleTest
{
    @Test
    public void testDoNotPruneProperOutputs()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.tableFunctionProcessor(
                                builder -> builder
                                        .name("test_function")
                                        .properOutputs(p.symbol("p"))
                                        .source(p.values(p.symbol("x"))))))
                .doesNotFire();
    }

    @Test
    public void testPrunePassThroughOutputs()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.of(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .matches(project(
                        ImmutableMap.of(),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of())),
                                values("a", "b"))));

        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    Symbol proper = p.symbol("proper");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.of(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(proper)
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .matches(project(
                        ImmutableMap.of(),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .properOutputs(ImmutableList.of("proper"))
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of())),
                                values("a", "b"))));
    }

    @Test
    public void testReferencedPassThroughOutputs()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    Symbol x = p.symbol("x");
                    Symbol y = p.symbol("y");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(y, b),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(x, y)
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .matches(project(
                        ImmutableMap.of("y", expression("y"), "b", expression("b")),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .properOutputs(ImmutableList.of("x", "y"))
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of("b"))),
                                values("a", "b"))));
    }

    @Test
    public void testAllPassThroughOutputsReferenced()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a, b),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .doesNotFire();

        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    Symbol proper = p.symbol("proper");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.identity(a, b),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(proper)
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .doesNotFire();
    }

    @Test
    public void testNoSource()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.tableFunctionProcessor(
                                builder -> builder
                                        .name("test_function")
                                        .properOutputs(p.symbol("proper")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleTableArguments()
    {
        // multiple pass-through specifications indicate that the table function has multiple table arguments
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.project(
                            Assignments.identity(b),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(p.symbol("proper"))
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(a, true))),
                                                    new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(b, true))),
                                                    new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(c, false))))
                                            .source(p.values(a, b, c, d))));
                })
                .matches(project(
                        ImmutableMap.of("b", expression("b")),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .properOutputs(ImmutableList.of("proper"))
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of(), ImmutableList.of("b"), ImmutableList.of())),
                                values("a", "b", "c", "d"))));
    }
}
