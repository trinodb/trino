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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode.Frame;
import io.trino.sql.planner.plan.WindowNode.Function;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Maps.filterKeys;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.patternRecognition;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.WINDOW;
import static io.trino.sql.tree.SkipTo.Position.PAST_LAST;

public class PrunePattenRecognitionColumns
        extends ProjectOffPushDownRule<PatternRecognitionNode>
{
    public PrunePattenRecognitionColumns()
    {
        super(patternRecognition());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, PatternRecognitionNode patternRecognitionNode, Set<Symbol> referencedOutputs)
    {
        Map<Symbol, Function> referencedFunctions = filterKeys(
                patternRecognitionNode.getWindowFunctions(),
                referencedOutputs::contains);

        Map<Symbol, Measure> referencedMeasures = filterKeys(
                patternRecognitionNode.getMeasures(),
                referencedOutputs::contains);

        if (referencedFunctions.isEmpty() && referencedMeasures.isEmpty() && passesAllRows(patternRecognitionNode)) {
            return Optional.of(patternRecognitionNode.getSource());
        }

        // TODO prune unreferenced variable definitions when we have pattern transformations that delete parts of the pattern

        ImmutableSet.Builder<Symbol> referencedInputs = ImmutableSet.builder();
        patternRecognitionNode.getSource().getOutputSymbols().stream()
                .filter(referencedOutputs::contains)
                .forEach(referencedInputs::add);
        referencedInputs.addAll(patternRecognitionNode.getPartitionBy());
        patternRecognitionNode.getOrderingScheme().ifPresent(orderingScheme -> referencedInputs.addAll(orderingScheme.getOrderBy()));
        patternRecognitionNode.getHashSymbol().ifPresent(referencedInputs::add);
        referencedFunctions.values().stream()
                .map(SymbolsExtractor::extractUnique)
                .forEach(referencedInputs::addAll);
        referencedMeasures.values().stream()
                .map(Measure::getExpressionAndValuePointers)
                .map(ExpressionAndValuePointers::getInputSymbols)
                .forEach(referencedInputs::addAll);
        patternRecognitionNode.getCommonBaseFrame().flatMap(Frame::getEndValue).ifPresent(referencedInputs::add);
        patternRecognitionNode.getVariableDefinitions().values().stream()
                .map(ExpressionAndValuePointers::getInputSymbols)
                .forEach(referencedInputs::addAll);

        Optional<PlanNode> prunedSource = restrictOutputs(context.getIdAllocator(), patternRecognitionNode.getSource(), referencedInputs.build());

        if (prunedSource.isEmpty() &&
                referencedFunctions.size() == patternRecognitionNode.getWindowFunctions().size() &&
                referencedMeasures.size() == patternRecognitionNode.getMeasures().size()) {
            return Optional.empty();
        }

        return Optional.of(new PatternRecognitionNode(
                patternRecognitionNode.getId(),
                prunedSource.orElse(patternRecognitionNode.getSource()),
                patternRecognitionNode.getSpecification(),
                patternRecognitionNode.getHashSymbol(),
                patternRecognitionNode.getPrePartitionedInputs(),
                patternRecognitionNode.getPreSortedOrderPrefix(),
                referencedFunctions,
                referencedMeasures,
                patternRecognitionNode.getCommonBaseFrame(),
                patternRecognitionNode.getRowsPerMatch(),
                patternRecognitionNode.getSkipToLabel(),
                patternRecognitionNode.getSkipToPosition(),
                patternRecognitionNode.isInitial(),
                patternRecognitionNode.getPattern(),
                patternRecognitionNode.getSubsets(),
                patternRecognitionNode.getVariableDefinitions()));
    }

    private boolean passesAllRows(PatternRecognitionNode node)
    {
        return node.getRowsPerMatch() == WINDOW ||
                (node.getRowsPerMatch().isUnmatchedRows() && node.getSkipToPosition() == PAST_LAST);
    }
}
