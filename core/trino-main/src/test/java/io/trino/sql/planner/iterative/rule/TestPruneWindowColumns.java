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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.window;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.type.UnknownType.UNKNOWN;

public class TestPruneWindowColumns
        extends BaseRuleTest
{
    private static final ResolvedFunction MIN_FUNCTION = new TestingFunctionResolution().resolveFunction("min", fromTypes(BIGINT));

    private static final List<String> inputSymbolNameList =
            ImmutableList.of("orderKey", "partitionKey", "hash", "startValue1", "startValue2", "endValue1", "endValue2", "input1", "input2", "aggOrderInput1", "aggOrderInput2", "unused");
    private static final Set<String> inputSymbolNameSet = ImmutableSet.copyOf(inputSymbolNameList);

    private static final WindowNode.Frame FRAME1 = new WindowNode.Frame(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.of(new Symbol(UNKNOWN, "startValue1")),
            Optional.of(new Symbol(UNKNOWN, "orderKey")),
            CURRENT_ROW,
            Optional.of(new Symbol(UNKNOWN, "endValue1")),
            Optional.of(new Symbol(UNKNOWN, "orderKey")));

    private static final WindowNode.Frame FRAME2 = new WindowNode.Frame(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.of(new Symbol(UNKNOWN, "startValue2")),
            Optional.of(new Symbol(UNKNOWN, "orderKey")),
            CURRENT_ROW,
            Optional.of(new Symbol(UNKNOWN, "endValue2")),
            Optional.of(new Symbol(UNKNOWN, "orderKey")));

    @Test
    public void testWindowNotNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(p, symbol -> inputSymbolNameSet.contains(symbol.name()), alwaysTrue()))
                .matches(
                        strictProject(
                                Maps.asMap(inputSymbolNameSet, symbol -> expression(new Reference(BIGINT, symbol))),
                                values(inputSymbolNameList)));
    }

    @Test
    public void testOneFunctionNotNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(p,
                        symbol -> symbol.name().equals("output2") || symbol.name().equals("unused"),
                        alwaysTrue()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "output2", expression(new Reference(BIGINT, "output2")),
                                        "unused", expression(new Reference(BIGINT, "unused"))),
                                window(windowBuilder -> windowBuilder
                                                .prePartitionedInputs(ImmutableSet.of())
                                                .specification(
                                                        ImmutableList.of("partitionKey"),
                                                        ImmutableList.of("orderKey"),
                                                        ImmutableMap.of("orderKey", ASC_NULLS_FIRST))
                                                .preSortedOrderPrefix(0)
                                                .addFunction("output2", windowFunction("min", ImmutableList.of("input2"), FRAME2, List.of(sort("aggOrderInput2", ASCENDING, FIRST))))
                                                .hashSymbol("hash"),
                                        strictProject(
                                                Maps.asMap(
                                                        Sets.difference(inputSymbolNameSet, ImmutableSet.of("input1", "startValue1", "endValue1", "aggOrderInput1")),
                                                        symbol -> expression(new Reference(BIGINT, symbol))),
                                                values(inputSymbolNameList)))));
    }

    @Test
    public void testAllColumnsNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(p, alwaysTrue(), alwaysTrue()))
                .doesNotFire();
    }

    @Test
    public void testUsedInputsNotNeeded()
    {
        // If the WindowNode needs all its inputs, we can't discard them from its child.
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(
                        p,
                        // only the window function outputs
                        symbol -> !inputSymbolNameSet.contains(symbol.name()),
                        // only the used input symbols
                        symbol -> !symbol.name().equals("unused")))
                .doesNotFire();
    }

    @Test
    public void testUnusedInputNotNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(
                        p,
                        // only the window function outputs
                        symbol -> !inputSymbolNameSet.contains(symbol.name()),
                        alwaysTrue()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "output1", expression(new Reference(BIGINT, "output1")),
                                        "output2", expression(new Reference(BIGINT, "output2"))),
                                window(windowBuilder -> windowBuilder
                                                .prePartitionedInputs(ImmutableSet.of())
                                                .specification(
                                                        ImmutableList.of("partitionKey"),
                                                        ImmutableList.of("orderKey"),
                                                        ImmutableMap.of("orderKey", ASC_NULLS_FIRST))
                                                .preSortedOrderPrefix(0)
                                                .addFunction("output1", windowFunction("min", ImmutableList.of("input1"), FRAME1, List.of(sort("aggOrderInput1", ASCENDING, FIRST))))
                                                .addFunction("output2", windowFunction("min", ImmutableList.of("input2"), FRAME2, List.of(sort("aggOrderInput2", ASCENDING, FIRST))))
                                                .hashSymbol("hash"),
                                        strictProject(
                                                Maps.asMap(
                                                        Sets.filter(inputSymbolNameSet, symbolName -> !symbolName.equals("unused")),
                                                        symbol -> expression(new Reference(BIGINT, symbol))),
                                                values(inputSymbolNameList)))));
    }

    private static PlanNode buildProjectedWindow(
            PlanBuilder p,
            Predicate<Symbol> projectionFilter,
            Predicate<Symbol> sourceFilter)
    {
        Symbol orderKey = p.symbol("orderKey");
        Symbol partitionKey = p.symbol("partitionKey");
        Symbol hash = p.symbol("hash");
        Symbol startValue1 = p.symbol("startValue1");
        Symbol startValue2 = p.symbol("startValue2");
        Symbol endValue1 = p.symbol("endValue1");
        Symbol endValue2 = p.symbol("endValue2");
        Symbol input1 = p.symbol("input1");
        Symbol input2 = p.symbol("input2");
        Symbol aggOrderInput1 = p.symbol("aggOrderInput1");
        Symbol aggOrderInput2 = p.symbol("aggOrderInput2");
        Symbol unused = p.symbol("unused");
        Symbol output1 = p.symbol("output1");
        Symbol output2 = p.symbol("output2");

        List<Symbol> inputs = ImmutableList.of(orderKey, partitionKey, hash, startValue1, startValue2, endValue1, endValue2, input1, input2, aggOrderInput1, aggOrderInput2, unused);
        List<Symbol> outputs = ImmutableList.<Symbol>builder().addAll(inputs).add(output1, output2).build();

        return p.project(
                Assignments.identity(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.window(
                        new DataOrganizationSpecification(
                                ImmutableList.of(partitionKey),
                                Optional.of(new OrderingScheme(
                                        ImmutableList.of(orderKey),
                                        ImmutableMap.of(orderKey, ASC_NULLS_FIRST)))),
                        ImmutableMap.of(
                                output1,
                                new WindowNode.Function(
                                        MIN_FUNCTION,
                                        ImmutableList.of(input1.toSymbolReference()),
                                        Optional.of(new OrderingScheme(List.of(aggOrderInput1), Map.of(aggOrderInput1, ASC_NULLS_FIRST))),
                                        new WindowNode.Frame(
                                                RANGE,
                                                UNBOUNDED_PRECEDING,
                                                Optional.of(startValue1),
                                                Optional.of(orderKey),
                                                CURRENT_ROW,
                                                Optional.of(endValue1),
                                                Optional.of(orderKey)),
                                        false,
                                        false),
                                output2,
                                new WindowNode.Function(
                                        MIN_FUNCTION,
                                        ImmutableList.of(input2.toSymbolReference()),
                                        Optional.of(new OrderingScheme(List.of(aggOrderInput2), Map.of(aggOrderInput2, ASC_NULLS_FIRST))),
                                        new WindowNode.Frame(
                                                RANGE,
                                                UNBOUNDED_PRECEDING,
                                                Optional.of(startValue2),
                                                Optional.of(orderKey),
                                                CURRENT_ROW,
                                                Optional.of(endValue2),
                                                Optional.of(orderKey)),
                                        false,
                                        false)),
                        hash,
                        p.values(
                                inputs.stream()
                                        .filter(sourceFilter)
                                        .collect(toImmutableList()),
                                ImmutableList.of())));
    }
}
