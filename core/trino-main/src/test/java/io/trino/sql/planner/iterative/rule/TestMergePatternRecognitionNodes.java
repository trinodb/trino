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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.MergePatternRecognitionNodes.MergePatternRecognitionNodesWithProject;
import io.trino.sql.planner.iterative.rule.MergePatternRecognitionNodes.MergePatternRecognitionNodesWithoutProject;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.MatchNumberValuePointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.RowsPerMatch.ALL_SHOW_EMPTY;
import static io.trino.sql.planner.plan.RowsPerMatch.ONE;
import static io.trino.sql.planner.plan.RowsPerMatch.WINDOW;
import static io.trino.sql.planner.plan.SkipToPosition.LAST;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.type.UnknownType.UNKNOWN;

public class TestMergePatternRecognitionNodes
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));

    private static final WindowNode.Frame ROWS_CURRENT_TO_UNBOUNDED = new WindowNode.Frame(
            ROWS,
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            UNBOUNDED_FOLLOWING,
            Optional.empty(),
            Optional.empty());

    @Test
    public void testSpecificationsDoNotMatch()
    {
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), FALSE)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.identity(p.symbol("a")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), FALSE)
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // aggregations in variable definitions do not match
        ResolvedFunction count = tester().getMetadata().resolveBuiltinFunction("count", fromTypes(BIGINT));
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new Comparison(GREATER_THAN, new Call(count, ImmutableList.of(new Reference(BIGINT, "a"))), new Constant(BIGINT, 5L)))
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new Comparison(GREATER_THAN, new Call(count, ImmutableList.of(new Reference(BIGINT, "b"))), new Constant(BIGINT, 5L)))
                                .source(p.values(p.symbol("a"), p.symbol("b", INTEGER)))))))
                .doesNotFire();
    }

    @Test
    public void testParentDependsOnSourceCreatedOutputs()
    {
        ResolvedFunction lag = createTestMetadataManager().resolveBuiltinFunction("lag", fromTypes(BIGINT));

        // parent node's measure depends on child node's measure output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "measure"))))
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addMeasure(
                                        p.symbol("measure"),
                                        new Reference(BIGINT, "pointer"),
                                        ImmutableMap.of("pointer", new MatchNumberValuePointer()))
                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's measure depends on child node's window function output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "function"))))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addWindowFunction(p.symbol("function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's window function depends on child node's window function output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addWindowFunction(p.symbol("dependent"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("function").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addWindowFunction(p.symbol("function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's window function depends on child node's measure output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addWindowFunction(p.symbol("dependent"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("measure").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addMeasure(
                                        p.symbol("measure"),
                                        new Reference(BIGINT, "pointer"),
                                        ImmutableMap.of("pointer", new MatchNumberValuePointer()))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();
    }

    @Test
    public void testParentDependsOnSourceCreatedOutputsWithProject()
    {
        // identity projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "measure"))))
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.identity(p.symbol("measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("measure"),
                                                new Reference(BIGINT, "pointer"),
                                                ImmutableMap.of("pointer", new MatchNumberValuePointer()))
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE)
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // renaming projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "renamed"))))
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.of(p.symbol("renamed"), new Reference(BIGINT, "measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("measure"),
                                                new Reference(BIGINT, "pointer"),
                                                ImmutableMap.of("pointer", new MatchNumberValuePointer()))
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE)
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // complex projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "projected"))))
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.of(p.symbol("projected"), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "measure")))),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("measure"),
                                                new Reference(BIGINT, "pointer"),
                                                ImmutableMap.of("pointer", new MatchNumberValuePointer()))
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE)
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();
    }

    @Test
    public void testMergeWithoutProject()
    {
        ResolvedFunction lag = createTestMetadataManager().resolveBuiltinFunction("lag", fromTypes(BIGINT));

        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .partitionBy(ImmutableList.of(p.symbol("c")))
                        .orderBy(new OrderingScheme(ImmutableList.of(p.symbol("d")), ImmutableMap.of(p.symbol("d"), ASC_NULLS_LAST)))
                        .addMeasure(
                                p.symbol("parent_measure"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "b"))))
                        .addWindowFunction(p.symbol("parent_function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                        .skipTo(LAST, ImmutableSet.of(new IrLabel("X")))
                        .seek()
                        .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.patternRecognition(childBuilder -> {
                            childBuilder
                                    .partitionBy(ImmutableList.of(p.symbol("c")))
                                    .orderBy(new OrderingScheme(ImmutableList.of(p.symbol("d")), ImmutableMap.of(p.symbol("d"), ASC_NULLS_LAST)))
                                    .addMeasure(
                                            p.symbol("child_measure"),
                                            new Reference(BIGINT, "pointer"),
                                            ImmutableMap.of("pointer", new ScalarValuePointer(
                                                    new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                    new Symbol(BIGINT, "a"))))
                                    .addWindowFunction(p.symbol("child_function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("b").toSymbolReference()), DEFAULT_FRAME, false))
                                    .rowsPerMatch(WINDOW)
                                    .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                    .skipTo(LAST, ImmutableSet.of(new IrLabel("X")))
                                    .seek()
                                    .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                                    .pattern(new IrLabel("X"))
                                    .addVariableDefinition(new IrLabel("X"), TRUE)
                                    .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("d")));
                        }))))
                .matches(
                        patternRecognition(builder -> builder
                                        .specification(specification(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableMap.of("d", ASC_NULLS_LAST)))
                                        .addMeasure(
                                                "parent_measure",
                                                new Reference(BIGINT, "pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                        new Symbol(BIGINT, "b"))),
                                                BIGINT)
                                        .addMeasure(
                                                "child_measure",
                                                new Reference(BIGINT, "pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                        new Symbol(BIGINT, "a"))),
                                                BIGINT)
                                        .addFunction("parent_function", windowFunction("lag", ImmutableList.of("a"), DEFAULT_FRAME))
                                        .addFunction("child_function", windowFunction("lag", ImmutableList.of("b"), DEFAULT_FRAME))
                                        .rowsPerMatch(WINDOW)
                                        .frame(ROWS_CURRENT_TO_UNBOUNDED)
                                        .skipTo(LAST, new IrLabel("X"))
                                        .seek()
                                        .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE),
                                values("a", "b", "c", "d")));
    }

    @Test
    public void testMergeWithoutProjectAndPruneOutputs()
    {
        // with the option ONE ROW PER MATCH, only partitioning symbols and measures are the output of PatternRecognitionNode
        // child_measure is not passed through the parent node, so a pruning projection is added on top of the merged node.
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .partitionBy(ImmutableList.of(p.symbol("c")))
                        .addMeasure(
                                p.symbol("parent_measure"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "b"))))
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.patternRecognition(childBuilder -> {
                            childBuilder
                                    .partitionBy(ImmutableList.of(p.symbol("c")))
                                    .addMeasure(
                                            p.symbol("child_measure"),
                                            new Reference(BIGINT, "pointer"),
                                            ImmutableMap.of("pointer", new ScalarValuePointer(
                                                    new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                    new Symbol(BIGINT, "a"))))
                                    .rowsPerMatch(ONE)
                                    .pattern(new IrLabel("X"))
                                    .addVariableDefinition(new IrLabel("X"), TRUE)
                                    .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c")));
                        }))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "c", expression(new Reference(BIGINT, "c")),
                                        "parent_measure", expression(new Reference(BIGINT, "parent_measure"))),
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                .addMeasure(
                                                        "parent_measure",
                                                        new Reference(BIGINT, "pointer"),
                                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                new Symbol(BIGINT, "b"))),
                                                        BIGINT)
                                                .addMeasure(
                                                        "child_measure",
                                                        new Reference(BIGINT, "pointer"),
                                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                new Symbol(BIGINT, "a"))),
                                                        BIGINT)
                                                .rowsPerMatch(ONE)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE),
                                        values("a", "b", "c"))));
    }

    @Test
    public void testMergeWithProject()
    {
        // project is based on pass-through symbols only
        // it does not produce symbols necessary for parent node, so it is moved on top of merged node
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("parent_measure"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "a"))))
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.of(
                                        p.symbol("a"), new Reference(BIGINT, "a"),
                                        p.symbol("expression"), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))),
                                p.patternRecognition(childBuilder -> {
                                    childBuilder
                                            .addMeasure(
                                                    p.symbol("child_measure"),
                                                    new Reference(BIGINT, "pointer"),
                                                    ImmutableMap.of("pointer", new ScalarValuePointer(
                                                            new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                            new Symbol(BIGINT, "b"))))
                                            .rowsPerMatch(ALL_SHOW_EMPTY)
                                            .pattern(new IrLabel("X"))
                                            .addVariableDefinition(new IrLabel("X"), TRUE)
                                            .source(p.values(p.symbol("a"), p.symbol("b")));
                                })))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new Reference(BIGINT, "a")),
                                        "parent_measure", expression(new Reference(BIGINT, "parent_measure")),
                                        "expression", expression(new Reference(BIGINT, "expression"))),
                                project(
                                        ImmutableMap.of(
                                                "a", expression(new Reference(BIGINT, "a")),
                                                "b", expression(new Reference(BIGINT, "b")),
                                                "parent_measure", expression(new Reference(BIGINT, "parent_measure")),
                                                "child_measure", expression(new Reference(BIGINT, "child_measure")),
                                                "expression", expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))))),
                                        patternRecognition(builder -> builder
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol(BIGINT, "a"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol(BIGINT, "b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE),
                                                values("a", "b")))));

        // project is based on symbols created by the child node
        // it does not produce symbols necessary for parent node, so it is moved on top of merged node
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("parent_measure"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "a"))))
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.of(
                                        p.symbol("a"), new Reference(BIGINT, "a"),
                                        p.symbol("expression"), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))), new Reference(BIGINT, "child_measure")))),
                                p.patternRecognition(childBuilder -> {
                                    childBuilder
                                            .addMeasure(
                                                    p.symbol("child_measure"),
                                                    new Reference(BIGINT, "pointer"),
                                                    ImmutableMap.of("pointer", new ScalarValuePointer(
                                                            new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                            new Symbol(BIGINT, "b"))))
                                            .rowsPerMatch(ALL_SHOW_EMPTY)
                                            .pattern(new IrLabel("X"))
                                            .addVariableDefinition(new IrLabel("X"), TRUE)
                                            .source(p.values(p.symbol("a"), p.symbol("b")));
                                })))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new Reference(BIGINT, "a")),
                                        "parent_measure", expression(new Reference(BIGINT, "parent_measure")),
                                        "expression", expression(new Reference(BIGINT, "expression"))),
                                project(
                                        ImmutableMap.of(
                                                "a", expression(new Reference(BIGINT, "a")),
                                                "b", expression(new Reference(BIGINT, "b")),
                                                "parent_measure", expression(new Reference(BIGINT, "parent_measure")),
                                                "child_measure", expression(new Reference(BIGINT, "child_measure")),
                                                "expression", expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))), new Reference(BIGINT, "child_measure"))))),
                                        patternRecognition(builder -> builder
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol(BIGINT, "a"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol(BIGINT, "b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE),
                                                values("a", "b")))));
    }

    @Test
    public void testMergeWithParentDependingOnProject()
    {
        // project is based on pass-through symbols only
        // it produces a symbol `expression_1` necessary for parent node, so it is partially moved to the source of merged node,
        // and the remaining assignments are moved on top of merged node
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("parent_measure"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "expression_1"))))
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("a"), new Reference(BIGINT, "a"))
                                        .put(p.symbol("expression_1"), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))))
                                        .put(p.symbol("expression_2"), new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))))
                                        .build(),
                                p.patternRecognition(childBuilder -> {
                                    childBuilder
                                            .addMeasure(
                                                    p.symbol("child_measure"),
                                                    new Reference(BIGINT, "pointer"),
                                                    ImmutableMap.of("pointer", new ScalarValuePointer(
                                                            new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                            new Symbol(BIGINT, "b"))))
                                            .rowsPerMatch(ALL_SHOW_EMPTY)
                                            .pattern(new IrLabel("X"))
                                            .addVariableDefinition(new IrLabel("X"), TRUE)
                                            .source(p.values(p.symbol("a"), p.symbol("b")));
                                })))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new Reference(BIGINT, "a")),
                                        "parent_measure", expression(new Reference(BIGINT, "parent_measure")),
                                        "expression_1", expression(new Reference(BIGINT, "expression_1")),
                                        "expression_2", expression(new Reference(BIGINT, "expression_2"))),
                                project(
                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("a", expression(new Reference(BIGINT, "a")))
                                                .put("b", expression(new Reference(BIGINT, "b")))
                                                .put("parent_measure", expression(new Reference(BIGINT, "parent_measure")))
                                                .put("child_measure", expression(new Reference(BIGINT, "child_measure")))
                                                .put("expression_1", expression(new Reference(BIGINT, "expression_1")))
                                                .put("expression_2", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))))
                                                .buildOrThrow(),
                                        patternRecognition(builder -> builder
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol(BIGINT, "expression_1"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol(BIGINT, "b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE),
                                                project(
                                                        ImmutableMap.of(
                                                                "a", expression(new Reference(BIGINT, "a")),
                                                                "b", expression(new Reference(BIGINT, "b")),
                                                                "expression_1", expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))))),
                                                        values("a", "b"))))));
    }

    @Test
    public void testOneRowPerMatchMergeWithParentDependingOnProject()
    {
        // project is based on pass-through symbols only
        // it produces a symbol `expression_1` necessary for parent node, so it is partially moved to the source of merged node,
        // and the remaining assignments are moved on top of merged node.
        // with the option ONE ROW PER MATCH, only partitioning symbols and measures are the output of PatternRecognitionNode
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .partitionBy(ImmutableList.of(p.symbol("a")))
                        .addMeasure(
                                p.symbol("parent_measure"),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "expression_1"))))
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("a"), new Reference(BIGINT, "a"))
                                        .put(p.symbol("child_measure"), new Reference(BIGINT, "child_measure"))
                                        .put(p.symbol("expression_1"), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "a"))))
                                        .put(p.symbol("expression_2"), new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "a"))))
                                        .build(),
                                p.patternRecognition(childBuilder -> {
                                    childBuilder
                                            .partitionBy(ImmutableList.of(p.symbol("a")))
                                            .addMeasure(
                                                    p.symbol("child_measure"),
                                                    new Reference(BIGINT, "pointer"),
                                                    ImmutableMap.of("pointer", new ScalarValuePointer(
                                                            new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                            new Symbol(BIGINT, "b"))))
                                            .rowsPerMatch(ONE)
                                            .pattern(new IrLabel("X"))
                                            .addVariableDefinition(new IrLabel("X"), TRUE)
                                            .source(p.values(p.symbol("a"), p.symbol("b")));
                                })))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new Reference(BIGINT, "a")),
                                        "parent_measure", expression(new Reference(BIGINT, "parent_measure"))),
                                project(
                                        ImmutableMap.of(
                                                "a", expression(new Reference(BIGINT, "a")),
                                                "parent_measure", expression(new Reference(BIGINT, "parent_measure")),
                                                "child_measure", expression(new Reference(BIGINT, "child_measure")),
                                                "expression_2", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "a"))))),
                                        patternRecognition(builder -> builder
                                                        .specification(specification(ImmutableList.of("a"), ImmutableList.of(), ImmutableMap.of()))
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol(UNKNOWN, "expression_1"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new Reference(BIGINT, "pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol(UNKNOWN, "b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ONE)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE),
                                                project(
                                                        ImmutableMap.of(
                                                                "a", expression(new Reference(BIGINT, "a")),
                                                                "b", expression(new Reference(BIGINT, "b")),
                                                                "expression_1", PlanMatchPattern.expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "a"))))),
                                                        values("a", "b"))))));
    }

    @Test
    public void testMergeWithAggregation()
    {
        ResolvedFunction count = tester().getMetadata().resolveBuiltinFunction("count", fromTypes(BIGINT));
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "c"), new Constant(BIGINT, 5L)),
                                ImmutableMap.of(new Symbol(BIGINT, "c"), new AggregationValuePointer(
                                        count,
                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                        ImmutableList.of(new Reference(BIGINT, "a")),
                                        Optional.empty(),
                                        Optional.empty())))
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "c"), new Constant(BIGINT, 5L)),
                                        ImmutableMap.of(new Symbol(BIGINT, "c"), new AggregationValuePointer(
                                                count,
                                                new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                ImmutableList.of(new Reference(BIGINT, "a")),
                                                Optional.empty(),
                                                Optional.empty())))
                                .source(p.values(p.symbol("a", BIGINT)))))))
                .matches(
                        patternRecognition(builder -> builder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(
                                                new IrLabel("X"),
                                                new Comparison(GREATER_THAN, new Reference(BIGINT, "c"), new Constant(BIGINT, 5L)),
                                                ImmutableMap.of("c", new AggregationValuePointer(
                                                        count,
                                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                        ImmutableList.of(new Reference(BIGINT, "a")),
                                                        Optional.empty(),
                                                        Optional.empty()))),
                                values("a")));
    }
}
