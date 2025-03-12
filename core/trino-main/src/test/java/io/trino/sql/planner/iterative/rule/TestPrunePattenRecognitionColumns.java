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
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.TestMetadataManager.createTestMetadataManager;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.RowsPerMatch.ALL_SHOW_EMPTY;
import static io.trino.sql.planner.plan.RowsPerMatch.ALL_WITH_UNMATCHED;
import static io.trino.sql.planner.plan.RowsPerMatch.WINDOW;
import static io.trino.sql.planner.plan.SkipToPosition.NEXT;
import static io.trino.sql.planner.plan.SkipToPosition.PAST_LAST;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.type.UnknownType.UNKNOWN;

public class TestPrunePattenRecognitionColumns
        extends BaseRuleTest
{
    @Test
    public void testRemovePatternRecognitionNode()
    {
        ResolvedFunction rank = createTestMetadataManager().resolveBuiltinFunction("rank", ImmutableList.of());

        // MATCH_RECOGNIZE with options: AFTER MATCH SKIP PAST LAST ROW, ALL ROWS WITH UNMATCHED ROW
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("b")),
                        p.patternRecognition(builder -> builder
                                .rowsPerMatch(ALL_WITH_UNMATCHED)
                                .skipTo(PAST_LAST)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                values("a", "b")));

        // pattern recognition in window
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("b")),
                        p.patternRecognition(builder -> builder
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                .skipTo(NEXT)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                values("a", "b")));

        // unreferenced window functions and measures
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("b")),
                        p.patternRecognition(builder -> builder
                                .addWindowFunction(p.symbol("rank"), new WindowNode.Function(rank, ImmutableList.of(), Optional.empty(), DEFAULT_FRAME, false, false))
                                .addMeasure(
                                        p.symbol("measure"),
                                        new Reference(BIGINT, "pointer"),
                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                new Symbol(UNKNOWN, "a"))))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                .skipTo(NEXT)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                values("a", "b")));
    }

    @Test
    public void testPruneUnreferencedWindowFunctionAndSources()
    {
        ResolvedFunction lag = createTestMetadataManager().resolveBuiltinFunction("lag", fromTypes(BIGINT));

        // remove window function "lag" and input symbol "b" used only by that function
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("measure", BIGINT)),
                        p.patternRecognition(builder -> builder
                                .addWindowFunction(p.symbol("lag", BIGINT), new WindowNode.Function(lag, ImmutableList.of(p.symbol("b", BIGINT).toSymbolReference()), Optional.empty(), DEFAULT_FRAME, false, false))
                                .addMeasure(
                                        p.symbol("measure", BIGINT),
                                        new Reference(BIGINT, "pointer"),
                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                new Symbol(BIGINT, "a"))))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                .skipTo(NEXT)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("measure", expression(new Reference(BIGINT, "measure"))),
                                patternRecognition(builder -> builder
                                                .addMeasure(
                                                        "measure",
                                                        new Reference(BIGINT, "pointer"),
                                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                new Symbol(BIGINT, "a"))),
                                                        BIGINT)
                                                .rowsPerMatch(WINDOW)
                                                .frame(new WindowNode.Frame(
                                                        ROWS,
                                                        CURRENT_ROW,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        UNBOUNDED_FOLLOWING,
                                                        Optional.empty(),
                                                        Optional.empty()))
                                                .skipTo(NEXT)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                                values("a", "b")))));
    }

    @Test
    public void testPruneUnreferencedMeasureAndSources()
    {
        ResolvedFunction lag = createTestMetadataManager().resolveBuiltinFunction("lag", fromTypes(BIGINT));
        WindowNode.Frame frame = new WindowNode.Frame(
                ROWS,
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty());

        // remove row pattern measure "measure" and input symbol "a" used only by that measure
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("lag")),
                        p.patternRecognition(builder -> builder
                                .addWindowFunction(p.symbol("lag"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("b").toSymbolReference()), Optional.empty(), frame, false, false))
                                .addMeasure(
                                        p.symbol("measure"),
                                        new Reference(BIGINT, "pointer"),
                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                new Symbol(UNKNOWN, "a"))))
                                .rowsPerMatch(WINDOW)
                                .frame(frame)
                                .skipTo(NEXT)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("lag", expression(new Reference(BIGINT, "lag"))),
                                patternRecognition(builder -> builder
                                                .addFunction("lag", windowFunction("lag", ImmutableList.of("b"), frame))
                                                .rowsPerMatch(WINDOW)
                                                .frame(frame)
                                                .skipTo(NEXT)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        strictProject(
                                                ImmutableMap.of("b", expression(new Reference(BIGINT, "b"))),
                                                values("a", "b")))));
    }

    @Test
    public void testDoNotPruneVariableDefinitionSources()
    {
        // input symbol "a" is used only by the variable definition
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.patternRecognition(builder -> builder
                                .addMeasure(p.symbol("measure", BIGINT), new Constant(BIGINT, 1L))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "pointer"), new Constant(BIGINT, 0L)),
                                        ImmutableMap.of(new Symbol(BIGINT, "pointer"), new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                new Symbol(BIGINT, "a"))))
                                .source(p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))))))
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                patternRecognition(builder -> builder
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(
                                                        new IrLabel("X"),
                                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "pointer"), new Constant(BIGINT, 0L)),
                                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                new Symbol(BIGINT, "a")))),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                                values("a", "b")))));

        // inputs "a", "b" are used as aggregation arguments
        ResolvedFunction maxBy = tester().getMetadata().resolveBuiltinFunction("max_by", fromTypes(BIGINT, BIGINT));
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.patternRecognition(builder -> builder
                                .addMeasure(p.symbol("measure"), new Constant(BIGINT, 1L))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "agg"), new Constant(BIGINT, 5L)),
                                        ImmutableMap.of(new Symbol(BIGINT, "agg"), new AggregationValuePointer(
                                                maxBy,
                                                new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                                Optional.empty(),
                                                Optional.empty())))
                                .source(p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT), p.symbol("c", BIGINT))))))
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                patternRecognition(builder -> builder
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(
                                                        new IrLabel("X"),
                                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "agg"), new Constant(BIGINT, 5L)),
                                                        ImmutableMap.of("agg", new AggregationValuePointer(
                                                                maxBy,
                                                                new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                                ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                                                Optional.empty(),
                                                                Optional.empty()))),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a")), "b", expression(new Reference(BIGINT, "b"))),
                                                values("a", "b", "c")))));
    }

    @Test
    public void testDoNotPruneReferencedInputs()
    {
        // input symbol "a" is not used by the pattern recognition node, but passed to output
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("a")),
                        p.patternRecognition(builder -> builder
                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                patternRecognition(builder -> builder
                                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                                values("a", "b")))));
    }

    @Test
    public void testDoNotPrunePartitionBySymbols()
    {
        // input symbol "a" is used only by PARTITION BY
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.patternRecognition(builder -> builder
                                .partitionBy(ImmutableList.of(p.symbol("a")))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of("a"), ImmutableList.of(), ImmutableMap.of()))
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                                values("a", "b")))));
    }

    @Test
    public void testDoNotPruneOrderBySymbols()
    {
        // input symbol "a" is used only by ORDER BY
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.patternRecognition(builder -> builder
                                .orderBy(new OrderingScheme(ImmutableList.of(p.symbol("a")), ImmutableMap.of(p.symbol("a"), ASC_NULLS_LAST)))
                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of(),
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of(), ImmutableList.of("a"), ImmutableMap.of("a", ASC_NULLS_LAST)))
                                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                                values("a", "b")))));
    }

    @Test
    public void testDoNotPruneCommonBaseFrameSymbols()
    {
        // input symbol "a" is used only by frame end value
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("measure")),
                        p.patternRecognition(builder -> builder
                                .addMeasure(p.symbol("measure"), new Constant(INTEGER, 1L))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), FOLLOWING, Optional.of(p.symbol("a")), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("measure", expression(new Reference(BIGINT, "measure"))),
                                patternRecognition(builder -> builder
                                                .addMeasure("measure", new Constant(INTEGER, 1L), BIGINT)
                                                .rowsPerMatch(WINDOW)
                                                .frame(new WindowNode.Frame(
                                                        ROWS,
                                                        CURRENT_ROW,
                                                        Optional.empty(),
                                                        Optional.empty(),
                                                        FOLLOWING,
                                                        Optional.of(new Symbol(UNKNOWN, "a")),
                                                        Optional.empty()))
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        strictProject(
                                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a"))),
                                                values("a", "b")))));
    }

    @Test
    public void testDoNotPruneUnreferencedUsedOutputs()
    {
        // cannot prune input symbol "a", because it is used by the variable definition.
        // no symbols or pattern recognition inner structures are eligible for pruning
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.patternRecognition(builder -> builder
                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new Comparison(GREATER_THAN, new Reference(INTEGER, "value"), new Constant(INTEGER, 0L)),
                                        ImmutableMap.of(new Symbol(INTEGER, "value"), new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0),
                                                new Symbol(BIGINT, "a"))))
                                .source(p.values(p.symbol("a", BIGINT))))))
                .doesNotFire();
    }

    @Test
    public void testPruneAndMeasures()
    {
        // remove unreferenced measure "measure"
        // source inputs cannot be pruned because they are referenced on output
        tester().assertThat(new PrunePattenRecognitionColumns())
                .on(p -> p.project(
                        Assignments.identity(p.symbol("a"), p.symbol("b")),
                        p.patternRecognition(builder -> builder
                                .addMeasure(p.symbol("measure"), new Constant(INTEGER, 1L))
                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a"), p.symbol("b"))))))
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression(new Reference(BIGINT, "a")), "b", expression(new Reference(BIGINT, "b"))),
                                patternRecognition(builder -> builder
                                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        values("a", "b"))));
    }
}
