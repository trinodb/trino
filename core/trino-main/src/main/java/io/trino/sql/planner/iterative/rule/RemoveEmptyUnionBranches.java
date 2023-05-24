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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static io.trino.sql.planner.plan.Patterns.union;

/**
 * Removes branches from a UnionNode that are guaranteed to produce 0 rows.
 *
 * If there's only one branch left, it replaces the UnionNode with a projection
 * to preserve the outputs of the union.
 *
 * If all branches are empty, it replaces the UnionNode with an empty ValuesNode
 */
public class RemoveEmptyUnionBranches
        implements Rule<UnionNode>
{
    private static final Pattern<UnionNode> PATTERN = union();

    @Override
    public Pattern<UnionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(UnionNode node, Captures captures, Context context)
    {
        int emptyBranches = 0;
        ImmutableList.Builder<PlanNode> newSourcesBuilder = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputsBuilder = ImmutableListMultimap.builder();
        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            if (!isEmpty(source, context.getLookup())) {
                newSourcesBuilder.add(source);

                for (Symbol column : node.getOutputSymbols()) {
                    outputsToInputsBuilder.put(column, node.getSymbolMapping().get(column).get(i));
                }
            }
            else {
                emptyBranches++;
            }
        }

        if (emptyBranches == 0) {
            return Result.empty();
        }

        if (emptyBranches == node.getSources().size()) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        List<PlanNode> newSources = newSourcesBuilder.build();
        ListMultimap<Symbol, Symbol> outputsToInputs = outputsToInputsBuilder.build();

        if (newSources.size() == 1) {
            Assignments.Builder assignments = Assignments.builder();

            outputsToInputs.entries()
                    .forEach(entry -> assignments.put(entry.getKey(), entry.getValue().toSymbolReference()));

            return Result.ofPlanNode(
                    new ProjectNode(
                            node.getId(),
                            newSources.get(0),
                            assignments.build()));
        }

        return Result.ofPlanNode(new UnionNode(node.getId(), newSources, outputsToInputs, node.getOutputSymbols()));
    }
}
