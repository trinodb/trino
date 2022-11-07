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
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.MergePatternRecognitionNodes.MergePatternRecognitionNodesWithProject;
import io.trino.sql.planner.iterative.rule.MergePatternRecognitionNodes.MergePatternRecognitionNodesWithoutProject;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFrame;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ALL_SHOW_EMPTY;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.WINDOW;
import static io.trino.sql.tree.SkipTo.Position.LAST;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;

public class TestMergePatternRecognitionNodes
        extends BaseRuleTest
{
    @Test
    public void testSpecificationsDoNotMatch()
    {
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), "false")
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.identity(p.symbol("a")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "false")
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // aggregations in variable definitions do not match
        QualifiedName count = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("count"), fromTypes(BIGINT)).toQualifiedName();
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(count, ImmutableList.of(expression("a"))), expression("5")))
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new ComparisonExpression(GREATER_THAN, new FunctionCall(count, ImmutableList.of(expression("b"))), expression("5")))
                                .source(p.values(p.symbol("a"), p.symbol("b")))))))
                .doesNotFire();
    }

    @Test
    public void testParentDependsOnSourceCreatedOutputs()
    {
        ResolvedFunction lag = createTestMetadataManager().resolveFunction(tester().getSession(), QualifiedName.of("lag"), fromTypes(BIGINT));

        // parent node's measure depends on child node's measure output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(p.symbol("dependent"), "LAST(X.measure)", BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addMeasure(p.symbol("measure"), "MATCH_NUMBER()", BIGINT)
                                .rowsPerMatch(ALL_SHOW_EMPTY)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), "true")
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's measure depends on child node's window function output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(p.symbol("dependent"), "LAST(X.function)", BIGINT)
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addWindowFunction(p.symbol("function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), "true")
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's window function depends on child node's window function output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addWindowFunction(p.symbol("dependent"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("function").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addWindowFunction(p.symbol("function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), "true")
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();

        // parent node's window function depends on child node's measure output
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addWindowFunction(p.symbol("dependent"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("measure").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .addMeasure(p.symbol("measure"), "MATCH_NUMBER()", BIGINT)
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), "true")
                                .source(p.values(p.symbol("a")))))))
                .doesNotFire();
    }

    @Test
    public void testParentDependsOnSourceCreatedOutputsWithProject()
    {
        // identity projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(p.symbol("dependent"), "LAST(X.measure)", BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.identity(p.symbol("measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(p.symbol("measure"), "MATCH_NUMBER()", BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true")
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // renaming projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(p.symbol("dependent"), "LAST(X.renamed)", BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.of(p.symbol("renamed"), expression("measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(p.symbol("measure"), "MATCH_NUMBER()", BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true")
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();

        // complex projection over created symbol
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(p.symbol("dependent"), "LAST(X.projected)", BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.of(p.symbol("projected"), expression("a * measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(p.symbol("measure"), "MATCH_NUMBER()", BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true")
                                        .source(p.values(p.symbol("a"))))))))
                .doesNotFire();
    }

    @Test
    public void testMergeWithoutProject()
    {
        ResolvedFunction lag = createTestMetadataManager().resolveFunction(tester().getSession(), QualifiedName.of("lag"), fromTypes(BIGINT));

        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .partitionBy(ImmutableList.of(p.symbol("c")))
                        .orderBy(new OrderingScheme(ImmutableList.of(p.symbol("d")), ImmutableMap.of(p.symbol("d"), ASC_NULLS_LAST)))
                        .addMeasure(p.symbol("parent_measure"), "LAST(X.b)", BIGINT)
                        .addWindowFunction(p.symbol("parent_function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("a").toSymbolReference()), DEFAULT_FRAME, false))
                        .rowsPerMatch(WINDOW)
                        .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                        .skipTo(LAST, new IrLabel("X"))
                        .seek()
                        .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .partitionBy(ImmutableList.of(p.symbol("c")))
                                .orderBy(new OrderingScheme(ImmutableList.of(p.symbol("d")), ImmutableMap.of(p.symbol("d"), ASC_NULLS_LAST)))
                                .addMeasure(p.symbol("child_measure"), "FIRST(X.a)", BIGINT)
                                .addWindowFunction(p.symbol("child_function"), new WindowNode.Function(lag, ImmutableList.of(p.symbol("b").toSymbolReference()), DEFAULT_FRAME, false))
                                .rowsPerMatch(WINDOW)
                                .frame(new WindowNode.Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                                .skipTo(LAST, new IrLabel("X"))
                                .seek()
                                .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), "true")
                                .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c"), p.symbol("d")))))))
                .matches(
                        patternRecognition(builder -> builder
                                        .specification(specification(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableMap.of("d", ASC_NULLS_LAST)))
                                        .addMeasure("parent_measure", "LAST(X.b)", BIGINT)
                                        .addMeasure("child_measure", "FIRST(X.a)", BIGINT)
                                        .addFunction("parent_function", functionCall("lag", ImmutableList.of("a")))
                                        .addFunction("child_function", functionCall("lag", ImmutableList.of("b")))
                                        .rowsPerMatch(WINDOW)
                                        .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                        .skipTo(LAST, new IrLabel("X"))
                                        .seek()
                                        .addSubset(new IrLabel("U"), ImmutableSet.of(new IrLabel("X")))
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true"),
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
                        .addMeasure(p.symbol("parent_measure"), "LAST(X.b)", BIGINT)
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .partitionBy(ImmutableList.of(p.symbol("c")))
                                .addMeasure(p.symbol("child_measure"), "FIRST(X.a)", BIGINT)
                                .rowsPerMatch(ONE)
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(new IrLabel("X"), "true")
                                .source(p.values(p.symbol("a"), p.symbol("b"), p.symbol("c")))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "c", PlanMatchPattern.expression("c"),
                                        "parent_measure", PlanMatchPattern.expression("parent_measure")),
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                .addMeasure("parent_measure", "LAST(X.b)", BIGINT)
                                                .addMeasure("child_measure", "FIRST(X.a)", BIGINT)
                                                .rowsPerMatch(ONE)
                                                .pattern(new IrLabel("X"))
                                                .addVariableDefinition(new IrLabel("X"), "true"),
                                        values("a", "b", "c"))));
    }

    @Test
    public void testMergeWithProject()
    {
        // project is based on pass-through symbols only
        // it does not produce symbols necessary for parent node, so it is moved on top of merged node
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(p.symbol("parent_measure"), "LAST(X.a)", BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.of(
                                        p.symbol("a"), expression("a"),
                                        p.symbol("expression"), expression("a * b")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(p.symbol("child_measure"), "FIRST(X.b)", BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true")
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("a"),
                                        "parent_measure", PlanMatchPattern.expression("parent_measure"),
                                        "expression", PlanMatchPattern.expression("expression")),
                                project(
                                        ImmutableMap.of(
                                                "a", PlanMatchPattern.expression("a"),
                                                "b", PlanMatchPattern.expression("b"),
                                                "parent_measure", PlanMatchPattern.expression("parent_measure"),
                                                "child_measure", PlanMatchPattern.expression("child_measure"),
                                                "expression", PlanMatchPattern.expression("a * b")),
                                        patternRecognition(builder -> builder
                                                        .addMeasure("parent_measure", "LAST(X.a)", BIGINT)
                                                        .addMeasure("child_measure", "FIRST(X.b)", BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), "true"),
                                                values("a", "b")))));

        // project is based on symbols created by the child node
        // it does not produce symbols necessary for parent node, so it is moved on top of merged node
        tester().assertThat(new MergePatternRecognitionNodesWithProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .addMeasure(p.symbol("parent_measure"), "LAST(X.a)", BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.of(
                                        p.symbol("a"), expression("a"),
                                        p.symbol("expression"), expression("a * b * child_measure")),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(p.symbol("child_measure"), "FIRST(X.b)", BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true")
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("a"),
                                        "parent_measure", PlanMatchPattern.expression("parent_measure"),
                                        "expression", PlanMatchPattern.expression("expression")),
                                project(
                                        ImmutableMap.of(
                                                "a", PlanMatchPattern.expression("a"),
                                                "b", PlanMatchPattern.expression("b"),
                                                "parent_measure", PlanMatchPattern.expression("parent_measure"),
                                                "child_measure", PlanMatchPattern.expression("child_measure"),
                                                "expression", PlanMatchPattern.expression("a * b * child_measure")),
                                        patternRecognition(builder -> builder
                                                        .addMeasure("parent_measure", "LAST(X.a)", BIGINT)
                                                        .addMeasure("child_measure", "FIRST(X.b)", BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), "true"),
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
                        .addMeasure(p.symbol("parent_measure"), "LAST(X.expression_1)", BIGINT)
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("a"), expression("a"))
                                        .put(p.symbol("expression_1"), expression("a * b"))
                                        .put(p.symbol("expression_2"), expression("a + b"))
                                        .build(),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .addMeasure(p.symbol("child_measure"), "FIRST(X.b)", BIGINT)
                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true")
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("a"),
                                        "parent_measure", PlanMatchPattern.expression("parent_measure"),
                                        "expression_1", PlanMatchPattern.expression("expression_1"),
                                        "expression_2", PlanMatchPattern.expression("expression_2")),
                                project(
                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("a", PlanMatchPattern.expression("a"))
                                                .put("b", PlanMatchPattern.expression("b"))
                                                .put("parent_measure", PlanMatchPattern.expression("parent_measure"))
                                                .put("child_measure", PlanMatchPattern.expression("child_measure"))
                                                .put("expression_1", PlanMatchPattern.expression("expression_1"))
                                                .put("expression_2", PlanMatchPattern.expression("a + b"))
                                                .buildOrThrow(),
                                        patternRecognition(builder -> builder
                                                        .addMeasure("parent_measure", "LAST(X.expression_1)", BIGINT)
                                                        .addMeasure("child_measure", "FIRST(X.b)", BIGINT)
                                                        .rowsPerMatch(ALL_SHOW_EMPTY)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), "true"),
                                                project(
                                                        ImmutableMap.of(
                                                                "a", PlanMatchPattern.expression("a"),
                                                                "b", PlanMatchPattern.expression("b"),
                                                                "expression_1", PlanMatchPattern.expression("a * b")),
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
                        .addMeasure(p.symbol("parent_measure"), "LAST(X.expression_1)", BIGINT)
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("a"), expression("a"))
                                        .put(p.symbol("child_measure"), expression("child_measure"))
                                        .put(p.symbol("expression_1"), expression("a * a"))
                                        .put(p.symbol("expression_2"), expression("a + a"))
                                        .build(),
                                p.patternRecognition(childBuilder -> childBuilder
                                        .partitionBy(ImmutableList.of(p.symbol("a")))
                                        .addMeasure(p.symbol("child_measure"), "FIRST(X.b)", BIGINT)
                                        .rowsPerMatch(ONE)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true")
                                        .source(p.values(p.symbol("a"), p.symbol("b"))))))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", PlanMatchPattern.expression("a"),
                                        "parent_measure", PlanMatchPattern.expression("parent_measure")),
                                project(
                                        ImmutableMap.of(
                                                "a", PlanMatchPattern.expression("a"),
                                                "parent_measure", PlanMatchPattern.expression("parent_measure"),
                                                "child_measure", PlanMatchPattern.expression("child_measure"),
                                                "expression_2", PlanMatchPattern.expression("a + a")),
                                        patternRecognition(builder -> builder
                                                        .specification(specification(ImmutableList.of("a"), ImmutableList.of(), ImmutableMap.of()))
                                                        .addMeasure("parent_measure", "LAST(X.expression_1)", BIGINT)
                                                        .addMeasure("child_measure", "FIRST(X.b)", BIGINT)
                                                        .rowsPerMatch(ONE)
                                                        .pattern(new IrLabel("X"))
                                                        .addVariableDefinition(new IrLabel("X"), "true"),
                                                project(
                                                        ImmutableMap.of(
                                                                "a", PlanMatchPattern.expression("a"),
                                                                "b", PlanMatchPattern.expression("b"),
                                                                "expression_1", PlanMatchPattern.expression("a * a")),
                                                        values("a", "b"))))));
    }

    @Test
    public void testMergeWithAggregation()
    {
        QualifiedName count = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("count"), fromTypes(BIGINT)).toQualifiedName();
        tester().assertThat(new MergePatternRecognitionNodesWithoutProject())
                .on(p -> p.patternRecognition(parentBuilder -> parentBuilder
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new ComparisonExpression(GREATER_THAN, new FunctionCall(count, ImmutableList.of(expression("a"))), expression("5")))
                        .source(p.patternRecognition(childBuilder -> childBuilder
                                .pattern(new IrLabel("X"))
                                .addVariableDefinition(
                                        new IrLabel("X"),
                                        new ComparisonExpression(GREATER_THAN, new FunctionCall(count, ImmutableList.of(expression("a"))), expression("5")))
                                .source(p.values(p.symbol("a")))))))
                .matches(
                        patternRecognition(builder -> builder
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(
                                                new IrLabel("X"),
                                                new ComparisonExpression(GREATER_THAN, new FunctionCall(count, ImmutableList.of(expression("a"))), expression("5"))),
                                values("a")));
    }
}
