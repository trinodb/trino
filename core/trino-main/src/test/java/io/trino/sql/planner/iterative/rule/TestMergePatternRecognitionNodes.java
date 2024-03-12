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
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFrame;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.RowsPerMatch.ALL_SHOW_EMPTY;
import static io.trino.sql.planner.plan.RowsPerMatch.ONE;
import static io.trino.sql.planner.plan.RowsPerMatch.WINDOW;
import static io.trino.sql.planner.plan.SkipToPosition.LAST;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;

public class TestMergePatternRecognitionNodes
        extends BaseRuleTest
{
    @Test
    public void testSpecificationsDoNotMatch()
    {
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), FALSE_LITERAL)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.identity(p.symbol("a")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), FALSE_LITERAL)
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // aggregations in variable definitions do not match
        QualifiedName count = tester().getMetadata().resolveBuiltinFunction("count", fromTypes(BIGINT)).toQualifiedName();
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(count, ImmutableList.of(new SymbolReference("a"))), new LongLiteral("5")))
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new ComparisonExpression(GREATER_THAN, new FunctionCall(count, ImmutableList.of(new SymbolReference("b"))), new LongLiteral("5")))
                                .source(p.values(p.symbol("a"), p.symbol("b")))))))
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
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("measure"))),
                                BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addMeasure(
                                        p.symbol("measure"),
                                        new SymbolReference("pointer"),
                                        ImmutableMap.of("pointer", new MatchNumberValuePointer()),
                                        BIGINT)
                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's measure depends on child node's window function output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("function"))),
                                BIGINT)
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addWindowFunction(p.symbol("function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's window function depends on child node's window function output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addWindowFunction(p.symbol("dependent"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("function").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addWindowFunction(p.symbol("function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's window function depends on child node's measure output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addWindowFunction(p.symbol("dependent"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("measure").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addMeasure(
                                        p.symbol("measure"),
                                        new SymbolReference("pointer"),
                                        ImmutableMap.of("pointer", new MatchNumberValuePointer()),
                                        BIGINT)
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
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
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("measure"))),
                                BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.identity(p.symbol("measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("measure"),
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new MatchNumberValuePointer()),
                                                BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // renaming projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("renamed"))),
                                BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.of(p.symbol("renamed"), new SymbolReference("measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("measure"),
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new MatchNumberValuePointer()),
                                                BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // complex projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("dependent"),
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("projected"))),
                                BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.of(p.symbol("projected"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("measure"))),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("measure"),
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new MatchNumberValuePointer()),
                                                BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
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
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("b"))),
                                BIGINT)
                        .addWindowFunction(p.symbol("parent_function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .skipTo(LAST, ImmutableSet.of(new IrLabel("X")))
                        .seek()
                        .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .partitionBy(ImmutableList.of(p.symbol("c")))
                                .orderBy(new OrderingScheme(ImmutableList.of(p.symbol("d")), ImmutableMap.of(p.symbol("d"), ASC_NULLS_LAST)))
                                .addMeasure(
                                        p.symbol("child_measure"),
                                        new SymbolReference("pointer"),
                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                new Symbol("a"))),
                                        BIGINT)
                                .addWindowFunction(p.symbol("child_function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("b").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .skipTo(LAST, ImmutableSet.of(new IrLabel("X")))
                                .seek()
                                .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("d")))))))
                .matches(
                        patternRecognition(builder -> builder
                                        .specification(specification(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableMap.of("d", ASC_NULLS_LAST)))
                                        .addMeasure(
                                                "parent_measure",
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                        new Symbol("b"))),
                                                BIGINT)
                                        .addMeasure(
                                                "child_measure",
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                        new Symbol("a"))),
                                                BIGINT)
                                        .addFunction("parent_function", functionCall("lag", ImmutableList.of("a")))
                                        .addFunction("child_function", functionCall("lag", ImmutableList.of("b")))
                                        .rowsPerMatch(WINDOW)
                                        .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                        .skipTo(LAST, new IrLabel("X"))
                                        .seek()
                                        .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL),
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
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("b"))),
                                BIGINT)
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .partitionBy(ImmutableList.of(p.symbol("c")))
                                .addMeasure(
                                        p.symbol("child_measure"),
                                        new SymbolReference("pointer"),
                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                new Symbol("a"))),
                                        BIGINT)
                                .rowsPerMatch(ONE)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c")))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "c", expression(new SymbolReference("c")),
                                        "parent_measure", expression(new SymbolReference("parent_measure"))),
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                .addMeasure(
                                                        "parent_measure",
                                                        new SymbolReference("pointer"),
                                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                new Symbol("b"))),
                                                        BIGINT)
                                                .addMeasure(
                                                        "child_measure",
                                                        new SymbolReference("pointer"),
                                                        ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                new Symbol("a"))),
                                                        BIGINT)
                                                .rowsPerMatch(ONE)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL),
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
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("a"))),
                                BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.of(
                                        p.symbol("a"), new SymbolReference("a"),
                                        p.symbol("expression"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("b"))),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("child_measure"),
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                        new Symbol("b"))),
                                                BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new SymbolReference("a")),
                                        "parent_measure", expression(new SymbolReference("parent_measure")),
                                        "expression", expression(new SymbolReference("expression"))),
                                project(
                                        ImmutableMap.of(
                                                "a", expression(new SymbolReference("a")),
                                                "b", expression(new SymbolReference("b")),
                                                "parent_measure", expression(new SymbolReference("parent_measure")),
                                                "child_measure", expression(new SymbolReference("child_measure")),
                                                "expression", expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("b")))),
                                        patternRecognition(builder -> builder
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol("a"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol("b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL),
                                                values("a", "b")))));

        // project is based on symbols created by the child node
        // it does not produce symbols necessary for parent node, so it is moved on top of merged node
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(
                                p.symbol("parent_measure"),
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("a"))),
                                BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.of(
                                        p.symbol("a"), new SymbolReference("a"),
                                        p.symbol("expression"), new ArithmeticBinaryExpression(MULTIPLY, new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("b")), new SymbolReference("child_measure"))),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("child_measure"),
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                        new Symbol("b"))),
                                                BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new SymbolReference("a")),
                                        "parent_measure", expression(new SymbolReference("parent_measure")),
                                        "expression", expression(new SymbolReference("expression"))),
                                project(
                                        ImmutableMap.of(
                                                "a", expression(new SymbolReference("a")),
                                                "b", expression(new SymbolReference("b")),
                                                "parent_measure", expression(new SymbolReference("parent_measure")),
                                                "child_measure", expression(new SymbolReference("child_measure")),
                                                "expression", expression(new ArithmeticBinaryExpression(MULTIPLY, new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("b")), new SymbolReference("child_measure")))),
                                        patternRecognition(builder -> builder
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol("a"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol("b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL),
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
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("expression_1"))),
                                BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("a"), new SymbolReference("a"))
                                        .put(p.symbol("expression_1"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("b")))
                                        .put(p.symbol("expression_2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("b")))
                                        .build(),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(
                                                p.symbol("child_measure"),
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                        new Symbol("b"))),
                                                BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new SymbolReference("a")),
                                        "parent_measure", expression(new SymbolReference("parent_measure")),
                                        "expression_1", expression(new SymbolReference("expression_1")),
                                        "expression_2", expression(new SymbolReference("expression_2"))),
                                project(
                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("a", expression(new SymbolReference("a")))
                                                .put("b", expression(new SymbolReference("b")))
                                                .put("parent_measure", expression(new SymbolReference("parent_measure")))
                                                .put("child_measure", expression(new SymbolReference("child_measure")))
                                                .put("expression_1", expression(new SymbolReference("expression_1")))
                                                .put("expression_2", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("b"))))
                                                .buildOrThrow(),
                                        patternRecognition(builder -> builder
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol("expression_1"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol("b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL),
                                                project(
                                                        ImmutableMap.of(
                                                                "a", expression(new SymbolReference("a")),
                                                                "b", expression(new SymbolReference("b")),
                                                                "expression_1", expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("b")))),
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
                                new SymbolReference("pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol("expression_1"))),
                                BIGINT)
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("a"), new SymbolReference("a"))
                                        .put(p.symbol("child_measure"), new SymbolReference("child_measure"))
                                        .put(p.symbol("expression_1"), new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("a")))
                                        .put(p.symbol("expression_2"), new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("a")))
                                        .build(),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .partitionBy(ImmutableList.of(p.symbol("a")))
                                        .addMeasure(
                                                p.symbol("child_measure"),
                                                new SymbolReference("pointer"),
                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                        new Symbol("b"))),
                                                BIGINT)
                                        .rowsPerMatch(ONE)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL)
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression(new SymbolReference("a")),
                                        "parent_measure", expression(new SymbolReference("parent_measure"))),
                                project(
                                        ImmutableMap.of(
                                                "a", expression(new SymbolReference("a")),
                                                "parent_measure", expression(new SymbolReference("parent_measure")),
                                                "child_measure", expression(new SymbolReference("child_measure")),
                                                "expression_2", expression(new ArithmeticBinaryExpression(ADD, new SymbolReference("a"), new SymbolReference("a")))),
                                        patternRecognition(builder -> builder
                                                        .specification(specification(ImmutableList.of("a"), ImmutableList.of(), ImmutableMap.of()))
                                                        .addMeasure(
                                                                "parent_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                                                        new Symbol("expression_1"))),
                                                                BIGINT)
                                                        .addMeasure(
                                                                "child_measure",
                                                                new SymbolReference("pointer"),
                                                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), false, true, 0, 0),
                                                                        new Symbol("b"))),
                                                                BIGINT)
                                                        .rowsPerMatch(ONE)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), TRUE_LITERAL),
                                                project(
                                                        ImmutableMap.of(
                                                                "a", expression(new SymbolReference("a")),
                                                                "b", expression(new SymbolReference("b")),
                                                                "expression_1", PlanMatchPattern.expression(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("a"), new SymbolReference("a")))),
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
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("5")),
                                ImmutableMap.of("c", new AggregationValuePointer(
                                        count,
                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                        ImmutableList.of(new SymbolReference("a")),
                                        Optional.empty(),
                                        Optional.empty())))
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("5")),
                                        ImmutableMap.of("c", new AggregationValuePointer(
                                                count,
                                                new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                ImmutableList.of(new SymbolReference("a")),
                                                Optional.empty(),
                                                Optional.empty())))
                                .source(p.values(p.symbol("a")))))))
                .matches(
                        patternRecognition(builder -> builder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(
                                                new IrLabel("X"),
                                                new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("5")),
                                                ImmutableMap.of("c", new AggregationValuePointer(
                                                        count,
                                                        new AggregatedSetDescriptor(ImmutableSet.of(), true),
                                                        ImmutableList.of(new SymbolReference("a")),
                                                        Optional.empty(),
                                                        Optional.empty()))),
                                values("a")));
    }
}
