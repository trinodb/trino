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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.trino.sql.planner.plan.Patterns.PatternRecognition.rowsPerMatch;
import static io.trino.sql.planner.plan.Patterns.patternRecognition;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;

/**
 * This rule restricts the inputs to PatternRecognitionNode
 * based on which symbols are used by the inner structures
 * of the PatternRecognitionNode. As opposite to
 * PrunePattenRecognitionColumns rule, this rule is not
 * aware of which output symbols of the PatternRecognitionNode
 * are used by the upstream plan. Consequentially, it can only
 * prune the inputs which are not exposed to the output.
 * Such possibility applies only to PatternRecognitionNode
 * with the option `ONE ROW PER MATCH`, where only the partitioning
 * symbols are passed to output.
 * <p>
 * This rule is complementary to PrunePatternRecognitionColumns.
 * It can prune PatternRecognitionNode's inputs in cases when
 * there is no narrowing projection on top of the node.
 */
public class PrunePatternRecognitionSourceColumns
        implements Rule<PatternRecognitionNode>
{
    private static final Pattern<PatternRecognitionNode> PATTERN = patternRecognition()
            .with(rowsPerMatch().matching(rowsPerMatch -> rowsPerMatch == ONE));

    @Override
    public Pattern<PatternRecognitionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(PatternRecognitionNode node, Captures captures, Context context)
    {
        checkState(node.getWindowFunctions().isEmpty(), "invalid node: window functions present with ONE ROW PER MATCH");
        checkState(node.getCommonBaseFrame().isEmpty(), "invalid node: common base frame present with ONE ROW PER MATCH");

        ImmutableSet.Builder<Symbol> referencedInputs = ImmutableSet.builder();

        referencedInputs.addAll(node.getPartitionBy());
        node.getOrderingScheme().ifPresent(orderingScheme -> referencedInputs.addAll(orderingScheme.getOrderBy()));
        node.getHashSymbol().ifPresent(referencedInputs::add);
        node.getMeasures().values().stream()
                .map(PatternRecognitionNode.Measure::getExpressionAndValuePointers)
                .map(LogicalIndexExtractor.ExpressionAndValuePointers::getInputSymbols)
                .forEach(referencedInputs::addAll);
        node.getVariableDefinitions().values().stream()
                .map(LogicalIndexExtractor.ExpressionAndValuePointers::getInputSymbols)
                .forEach(referencedInputs::addAll);

        return restrictChildOutputs(context.getIdAllocator(), node, referencedInputs.build())
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
