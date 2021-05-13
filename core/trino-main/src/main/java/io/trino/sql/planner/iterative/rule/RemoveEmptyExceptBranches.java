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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.except;

/**
 * Removes branches from an ExceptNode that are guaranteed to produce 0 rows.
 *
 * <code>A EXCEPT E1 EXCEPT E2 ... EXCEPT En</code> is equivalent to <code>A - (E1 + ... + En)</code>.
 * If any of the Ei sets is empty, it can be removed. If only A is left and it's empty, it gets replaced with
 * an empty Values node. Otherwise:
 * <li>a projection to preserve the outputs symbols, in the case of EXCEPT ALL.</li>
 * <li>an aggregation to remove duplicates, in case of EXCEPT DISTINCT</li>
 */
public class RemoveEmptyExceptBranches
        implements Rule<ExceptNode>
{
    private static final Pattern<ExceptNode> PATTERN = except();

    @Override
    public Pattern<ExceptNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ExceptNode node, Captures captures, Context context)
    {
        if (isEmpty(node.getSources().get(0), context.getLookup())) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        boolean hasEmptyBranches = node.getSources().stream()
                .skip(1) // first source is the set we're excluding rows from, so ignore it
                .anyMatch(source -> isEmpty(source, context.getLookup()));

        if (!hasEmptyBranches) {
            return Result.empty();
        }

        ImmutableList.Builder<PlanNode> newSourcesBuilder = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputsBuilder = ImmutableListMultimap.builder();

        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            if (i == 0 || !isEmpty(source, context.getLookup())) {
                newSourcesBuilder.add(source);

                for (Symbol column : node.getOutputSymbols()) {
                    outputsToInputsBuilder.put(column, node.getSymbolMapping().get(column).get(i));
                }
            }
        }

        List<PlanNode> newSources = newSourcesBuilder.build();
        ListMultimap<Symbol, Symbol> outputsToInputs = outputsToInputsBuilder.build();

        if (newSources.size() == 1) {
            Assignments.Builder assignments = Assignments.builder();

            outputsToInputs.entries().stream()
                    .forEach(entry -> assignments.put(entry.getKey(), entry.getValue().toSymbolReference()));

            if (node.isDistinct()) {
                return Result.ofPlanNode(
                        new AggregationNode(
                                node.getId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        newSources.get(0),
                                        assignments.build()),
                                ImmutableMap.of(),
                                singleGroupingSet(node.getOutputSymbols()),
                                ImmutableList.of(),
                                Step.SINGLE,
                                Optional.empty(),
                                Optional.empty()));
            }

            return Result.ofPlanNode(
                    new ProjectNode(
                            node.getId(),
                            newSources.get(0),
                            assignments.build()));
        }

        return Result.ofPlanNode(new ExceptNode(node.getId(), newSources, outputsToInputs, node.getOutputSymbols(), node.isDistinct()));
    }
}
