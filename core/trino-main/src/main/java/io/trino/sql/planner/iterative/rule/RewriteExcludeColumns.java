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
import io.trino.operator.table.ExcludeColumnsFunction;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;

import java.util.HashMap;
import java.util.Map;

import static io.trino.sql.planner.plan.Patterns.tableFunctionProcessor;

public class RewriteExcludeColumns
        implements Rule<TableFunctionProcessorNode>
{
    private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor().matching(x -> x.getName().equals(ExcludeColumnsFunction.NAME));

    @Override
    public Pattern<TableFunctionProcessorNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionProcessorNode node, Captures captures, Context context)
    {
        PlanNode source = node.getSources().getFirst();
        Map<Symbol, Expression> assignments = new HashMap<>();
        for (int i = 0; i < node.getOutputSymbols().size(); i++) {
            assignments.put(node.getOutputSymbols().get(i), source.getOutputSymbols().get(i).toSymbolReference());
        }
        return Result.ofPlanNode(new ProjectNode(node.getId(), source, new Assignments(assignments)));
    }
}
