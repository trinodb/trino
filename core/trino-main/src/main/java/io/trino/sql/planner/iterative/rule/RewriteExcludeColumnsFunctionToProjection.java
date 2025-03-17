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
import io.trino.operator.table.ExcludeColumnsFunction.ExcludeColumnsFunctionHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.plan.Patterns.tableFunctionProcessor;

public class RewriteExcludeColumnsFunctionToProjection
        implements Rule<TableFunctionProcessorNode>
{
    private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor();

    @Override
    public Pattern<TableFunctionProcessorNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionProcessorNode node, Captures captures, Context context)
    {
        if (!(node.getHandle().functionHandle() instanceof ExcludeColumnsFunctionHandle)) {
            return Result.empty();
        }

        List<Symbol> inputSymbols = getOnlyElement(node.getRequiredSymbols());
        List<Symbol> outputSymbols = node.getOutputSymbols();

        checkState(inputSymbols.size() == outputSymbols.size(), "inputSymbols size differs from outputSymbols size");
        Assignments.Builder assignments = Assignments.builder();
        for (int i = 0; i < outputSymbols.size(); i++) {
            assignments.put(outputSymbols.get(i), inputSymbols.get(i).toSymbolReference());
        }

        return Result.ofPlanNode(new ProjectNode(
                node.getId(),
                node.getSource().orElseThrow(),
                assignments.build()));
    }
}
