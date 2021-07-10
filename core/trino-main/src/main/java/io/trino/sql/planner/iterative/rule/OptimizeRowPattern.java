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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.rowpattern.IrPatternAlternationOptimizer;
import io.trino.sql.planner.rowpattern.IrRowPatternFlattener;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;

import static io.trino.sql.planner.plan.Patterns.patternRecognition;

public class OptimizeRowPattern
        implements Rule<PatternRecognitionNode>
{
    private static final Pattern<PatternRecognitionNode> PATTERN = patternRecognition();

    @Override
    public Pattern<PatternRecognitionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(PatternRecognitionNode node, Captures captures, Context context)
    {
        IrRowPattern optimizedPattern = IrPatternAlternationOptimizer.optimize(IrRowPatternFlattener.optimize(node.getPattern()));

        if (optimizedPattern.equals(node.getPattern())) {
            return Result.empty();
        }

        return Result.ofPlanNode(new PatternRecognitionNode(
                node.getId(),
                node.getSource(),
                node.getSpecification(),
                node.getHashSymbol(),
                node.getPrePartitionedInputs(),
                node.getPreSortedOrderPrefix(),
                node.getWindowFunctions(),
                node.getMeasures(),
                node.getCommonBaseFrame(),
                node.getRowsPerMatch(),
                node.getSkipToLabel(),
                node.getSkipToPosition(),
                node.isInitial(),
                optimizedPattern,
                node.getSubsets(),
                node.getVariableDefinitions()));
    }
}
