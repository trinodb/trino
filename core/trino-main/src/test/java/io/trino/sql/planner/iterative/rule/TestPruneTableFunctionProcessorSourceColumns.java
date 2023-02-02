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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneTableFunctionProcessorSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneUnreferencedSymbol()
    {
        // symbols 'a', 'b', 'c', 'd', 'hash', and 'marker' are used by the node.
        // symbol 'unreferenced' is pruned out. Also, the mapping for this symbol is removed from marker mappings
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    Symbol proper = p.symbol("proper");
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol unreferenced = p.symbol("unreferenced");
                    Symbol hash = p.symbol("hash");
                    Symbol marker = p.symbol("marker");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .properOutputs(proper)
                                    .passThroughSpecifications(new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(a, false))))
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(b)))
                                    .markerSymbols(ImmutableMap.of(
                                            a, marker,
                                            b, marker,
                                            c, marker,
                                            d, marker,
                                            unreferenced, marker))
                                    .specification(new DataOrganizationSpecification(ImmutableList.of(c), Optional.of(new OrderingScheme(ImmutableList.of(d), ImmutableMap.of(d, ASC_NULLS_FIRST)))))
                                    .hashSymbol(hash)
                                    .source(p.values(a, b, c, d, unreferenced, hash, marker)));
                })
                .matches(tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("proper"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("a")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("b")))
                                .markerSymbols(ImmutableMap.of(
                                        "a", "marker",
                                        "b", "marker",
                                        "c", "marker",
                                        "d", "marker"))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableMap.of("d", ASC_NULLS_FIRST)))
                                .hashSymbol("hash"),
                        project(
                                ImmutableMap.of(
                                        "a", expression("a"),
                                        "b", expression("b"),
                                        "c", expression("c"),
                                        "d", expression("d"),
                                        "hash", expression("hash"),
                                        "marker", expression("marker")),
                                values("a", "b", "c", "d", "unreferenced", "hash", "marker"))));
    }

    @Test
    public void testPruneUnusedMarkerSymbol()
    {
        // symbol 'unreferenced' is pruned out because the node does not use it.
        // also, the mapping for this symbol is removed from marker mappings.
        // because the marker symbol 'marker' is no longer used, it is pruned out too.
        // note: currently a marker symbol cannot become unused because the function
        // must use at least one symbol from each source. it might change in the future.
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    Symbol unreferenced = p.symbol("unreferenced");
                    Symbol marker = p.symbol("marker");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .markerSymbols(ImmutableMap.of(unreferenced, marker))
                                    .source(p.values(unreferenced, marker)));
                })
                .matches(tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .markerSymbols(ImmutableMap.of()),
                        project(
                                ImmutableMap.of(),
                                values("unreferenced", "marker"))));
    }

    @Test
    public void testMultipleSources()
    {
        // multiple pass-through specifications indicate that the table function has multiple table arguments
        // the third argument provides symbols 'e', 'f', and 'unreferenced'. those symbols are mapped to common marker symbol 'marker3'
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    Symbol f = p.symbol("f");
                    Symbol marker1 = p.symbol("marker1");
                    Symbol marker2 = p.symbol("marker2");
                    Symbol marker3 = p.symbol("marker3");
                    Symbol unreferenced = p.symbol("unreferenced");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .passThroughSpecifications(
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(a, false))),
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(e, true))))
                                    .requiredSymbols(ImmutableList.of(
                                            ImmutableList.of(b),
                                            ImmutableList.of(d),
                                            ImmutableList.of(f)))
                                    .markerSymbols(ImmutableMap.of(
                                            a, marker1,
                                            b, marker1,
                                            c, marker2,
                                            d, marker2,
                                            e, marker3,
                                            f, marker3,
                                            unreferenced, marker3))
                                    .source(p.values(a, b, c, d, e, f, marker1, marker2, marker3, unreferenced)));
                })
                .matches(tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("a"), ImmutableList.of("c"), ImmutableList.of("e")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("b"), ImmutableList.of("d"), ImmutableList.of("f")))
                                .markerSymbols(ImmutableMap.of(
                                        "a", "marker1",
                                        "b", "marker1",
                                        "c", "marker2",
                                        "d", "marker2",
                                        "e", "marker3",
                                        "f", "marker3")),
                        project(
                                ImmutableMap.of(
                                        "a", expression("a"),
                                        "b", expression("b"),
                                        "c", expression("c"),
                                        "d", expression("d"),
                                        "e", expression("e"),
                                        "f", expression("f"),
                                        "marker1", expression("marker1"),
                                        "marker2", expression("marker2"),
                                        "marker3", expression("marker3")),
                                values("a", "b", "c", "d", "e", "f", "marker1", "marker2", "marker3", "unreferenced"))));
    }

    @Test
    public void allSymbolsReferenced()
    {
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol marker = p.symbol("marker");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(a)))
                                    .markerSymbols(ImmutableMap.of(a, marker))
                                    .source(p.values(a, marker)));
                })
                .doesNotFire();
    }
}
