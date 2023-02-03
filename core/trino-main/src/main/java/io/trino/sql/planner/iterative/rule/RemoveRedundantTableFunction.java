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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.ValuesNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static io.trino.sql.planner.plan.Patterns.tableFunctionProcessor;

/**
 * Table function can take multiple table arguments. Each argument is either "prune when empty" or "keep when empty".
 * "Prune when empty" means that if this argument has no rows, the function result is empty, so the function can be
 * removed from the plan, and replaced with empty values.
 * "Keep when empty" means that even if the argument has no rows, the function should still be executed, and it can
 * return a non-empty result.
 * All the table arguments are combined into a single source of a TableFunctionProcessorNode. If either argument is
 * "prune when empty", the overall result is "prune when empty". This rule removes a redundant TableFunctionProcessorNode
 * based on the "prune when empty" property.
 */
public class RemoveRedundantTableFunction
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
        if (node.isPruneWhenEmpty() && node.getSource().isPresent()) {
            if (isEmpty(node.getSource().orElseThrow(), context.getLookup())) {
                return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
            }
        }

        return Result.empty();
    }
}
