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
import com.google.common.collect.Streams;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.WindowNode.Function;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointersEquivalence;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ir.IrLabel;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.patternRecognition;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

public class MergePatternRecognitionNodes
{
    private MergePatternRecognitionNodes() {}

    public static Set<Rule<?>> rules()
    {
        return ImmutableSet.of(new MergePatternRecognitionNodesWithoutProject(), new MergePatternRecognitionNodesWithProject());
    }

    public static final class MergePatternRecognitionNodesWithoutProject
            implements Rule<PatternRecognitionNode>
    {
        private static final Capture<PatternRecognitionNode> CHILD = newCapture();

        private static final Pattern<PatternRecognitionNode> PATTERN = patternRecognition()
                .with(source().matching(patternRecognition()
                        .capturedAs(CHILD)));

        @Override
        public Pattern<PatternRecognitionNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(PatternRecognitionNode node, Captures captures, Context context)
        {
            PatternRecognitionNode child = captures.get(CHILD);

            if (!patternRecognitionSpecificationsMatch(node, child)) {
                return Result.empty();
            }

            if (dependsOnSourceCreatedOutputs(node, child)) {
                return Result.empty();
            }

            PlanNode result = merge(node, child);

            return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(node.getOutputSymbols())).orElse(result));
        }
    }

    public static final class MergePatternRecognitionNodesWithProject
            implements Rule<PatternRecognitionNode>
    {
        private static final Capture<ProjectNode> PROJECT = newCapture();
        private static final Capture<PatternRecognitionNode> CHILD = newCapture();

        private static final Pattern<PatternRecognitionNode> PATTERN = patternRecognition()
                .with(source().matching(project()
                        .capturedAs(PROJECT)
                        .with(source().matching(patternRecognition()
                                .capturedAs(CHILD)))));

        @Override
        public Pattern<PatternRecognitionNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(PatternRecognitionNode node, Captures captures, Context context)
        {
            ProjectNode project = captures.get(PROJECT);
            PatternRecognitionNode child = captures.get(CHILD);

            if (!patternRecognitionSpecificationsMatch(node, child)) {
                return Result.empty();
            }

            if (dependsOnSourceCreatedOutputs(node, project, child)) {
                return Result.empty();
            }

            PatternRecognitionNode merged = merge(node, child);

            Assignments prerequisites = extractPrerequisites(node, project);

            PlanNode result;

            if (prerequisites.isEmpty()) {
                // put projection on top of merged node
                result = new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        merged,
                        Assignments.builder()
                                .putIdentities(merged.getOutputSymbols())
                                .putAll(project.getAssignments())
                                .build());
            }
            else {
                // put prerequisite assignments in the source of merged node,
                // and the remaining assignments on top of merged node
                Assignments remainingAssignments = project.getAssignments()
                        .filter(symbol -> !prerequisites.getSymbols().contains(symbol));

                merged = (PatternRecognitionNode) merged.replaceChildren(ImmutableList.of(new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        merged.getSource(),
                        Assignments.builder()
                                .putIdentities(merged.getSource().getOutputSymbols())
                                .putAll(prerequisites)
                                .build())));

                result = new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        merged,
                        Assignments.builder()
                                .putIdentities(merged.getOutputSymbols())
                                .putAll(remainingAssignments)
                                .build());
            }

            return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(node.getOutputSymbols())).orElse(result));
        }
    }

    private static boolean patternRecognitionSpecificationsMatch(PatternRecognitionNode parent, PatternRecognitionNode child)
    {
        return parent.getSpecification().equals(child.getSpecification()) &&
                parent.getCommonBaseFrame().equals(child.getCommonBaseFrame()) &&
                parent.getRowsPerMatch() == child.getRowsPerMatch() &&
                parent.getSkipToLabel().equals(child.getSkipToLabel()) &&
                parent.getSkipToPosition() == child.getSkipToPosition() &&
                parent.isInitial() == child.isInitial() &&
                parent.getPattern().equals(child.getPattern()) &&
                parent.getSubsets().equals(child.getSubsets()) &&
                equivalent(parent.getVariableDefinitions(), child.getVariableDefinitions());
    }

    private static boolean equivalent(Map<IrLabel, ExpressionAndValuePointers> parentVariableDefinitions, Map<IrLabel, ExpressionAndValuePointers> childVariableDefinitions)
    {
        if (!parentVariableDefinitions.keySet().equals(childVariableDefinitions.keySet())) {
            return false;
        }

        for (Map.Entry<IrLabel, ExpressionAndValuePointers> parentDefinition : parentVariableDefinitions.entrySet()) {
            IrLabel label = parentDefinition.getKey();
            ExpressionAndValuePointers parentExpression = parentDefinition.getValue();
            ExpressionAndValuePointers childExpression = childVariableDefinitions.get(label);
            if (!ExpressionAndValuePointersEquivalence.equivalent(parentExpression, childExpression)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if parent node uses output symbols created by the child node (that is, the output symbols
     * of child node's window functions and measures). Only searches for dependencies in the window
     * functions and measures of the parent node. Other properties of the parent node, such as
     * specification and frame, are supposed to be identical to corresponding properties of the child node,
     * as checked in the `patternRecognitionSpecificationsMatch` call. As such, they cannot use symbols
     * created by the child.
     */
    private static boolean dependsOnSourceCreatedOutputs(PatternRecognitionNode parent, PatternRecognitionNode child)
    {
        Set<Symbol> sourceCreatedOutputs = child.getCreatedSymbols();

        return Streams.concat(
                parent.getWindowFunctions().values().stream()
                        .map(SymbolsExtractor::extractAll)
                        .flatMap(Collection::stream),
                parent.getMeasures().values().stream()
                        .map(Measure::getExpressionAndValuePointers)
                        .map(ExpressionAndValuePointers::getInputSymbols)
                        .flatMap(Collection::stream))
                .anyMatch(sourceCreatedOutputs::contains);
    }

    /**
     * Check if parent node uses output symbols created by the child node (that is, the output symbols
     * of child node's window functions and measures), with an intervening projection between the parent
     * and child nodes. Only searches for dependencies in the window functions and measures of the parent
     * node. Other properties of the parent node, such as specification and frame, are supposed to be
     * identical to corresponding properties of the child node, as checked in the
     * `patternRecognitionSpecificationsMatch` call. As such, they cannot use symbols created by the child.
     */
    private static boolean dependsOnSourceCreatedOutputs(PatternRecognitionNode parent, ProjectNode project, PatternRecognitionNode child)
    {
        Set<Symbol> sourceCreatedOutputs = child.getCreatedSymbols();
        Assignments assignments = project.getAssignments();

        ImmutableSet.Builder<Symbol> parentInputs = ImmutableSet.builder();
        parent.getWindowFunctions().values().stream()
                .map(SymbolsExtractor::extractAll)
                .forEach(parentInputs::addAll);
        parent.getMeasures().values().stream()
                .map(Measure::getExpressionAndValuePointers)
                .map(ExpressionAndValuePointers::getInputSymbols)
                .forEach(parentInputs::addAll);

        return parentInputs.build().stream()
                .map(assignments::get)
                .map(SymbolsExtractor::extractAll)
                .flatMap(Collection::stream)
                .anyMatch(sourceCreatedOutputs::contains);
    }

    /**
     * Extract project assignments producing symbols used by the PatternRecognitionNode's
     * window functions and measures. Exclude identity assignments.
     */
    private static Assignments extractPrerequisites(PatternRecognitionNode node, ProjectNode project)
    {
        Assignments assignments = project.getAssignments();

        ImmutableSet.Builder<Symbol> inputsBuilder = ImmutableSet.builder();
        node.getWindowFunctions().values().stream()
                .map(SymbolsExtractor::extractAll)
                .forEach(inputsBuilder::addAll);
        node.getMeasures().values().stream()
                .map(Measure::getExpressionAndValuePointers)
                .map(ExpressionAndValuePointers::getInputSymbols)
                .forEach(inputsBuilder::addAll);
        Set<Symbol> inputs = inputsBuilder.build();

        return assignments
                .filter(symbol -> !assignments.isIdentity(symbol))
                .filter(inputs::contains);
    }

    private static PatternRecognitionNode merge(PatternRecognitionNode parent, PatternRecognitionNode child)
    {
        ImmutableMap.Builder<Symbol, Function> windowFunctions = ImmutableMap.<Symbol, Function>builder()
                .putAll(parent.getWindowFunctions())
                .putAll(child.getWindowFunctions());

        ImmutableMap.Builder<Symbol, Measure> measures = ImmutableMap.<Symbol, Measure>builder()
                .putAll(parent.getMeasures())
                .putAll(child.getMeasures());

        return new PatternRecognitionNode(
                parent.getId(),
                child.getSource(),
                parent.getSpecification(),
                parent.getHashSymbol(),
                parent.getPrePartitionedInputs(),
                parent.getPreSortedOrderPrefix(),
                windowFunctions.build(),
                measures.build(),
                parent.getCommonBaseFrame(),
                parent.getRowsPerMatch(),
                parent.getSkipToLabel(),
                parent.getSkipToPosition(),
                parent.isInitial(),
                parent.getPattern(),
                parent.getSubsets(),
                parent.getVariableDefinitions());
    }
}
