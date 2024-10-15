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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.HashSet;
import java.util.Set;

import static io.trino.sql.planner.plan.Patterns.aggregation;

/**
 * Removes DISTINCT only aggregation, when the input source is already distinct over a subset of
 * the grouping keys as a result of another aggregation.
 *
 * Given:
 * <pre>
 * - Aggregate[keys = [a, max]]
 *   - Aggregate[keys = [a]]
 *     max := max(b)
 * </pre>
 * <p>
 * Produces:
 * <pre>
 *   - Aggregate[keys = [a]]
 *     max := max(b)
 * </pre>
 */
public class RemoveRedundantDistinctAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(RemoveRedundantDistinctAggregation::isDistinctOnlyAggregation);

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        if (isDistinctOverGroupingKeys(lookup.resolve(aggregationNode.getSource()), lookup, new HashSet<>(aggregationNode.getGroupingKeys()))) {
            return Result.ofPlanNode(aggregationNode.getSource());
        }
        else {
            return Result.empty();
        }
    }

    private static boolean isDistinctOnlyAggregation(AggregationNode node)
    {
        return node.producesDistinctRows() && node.getGroupingSetCount() == 1;
    }

    private static boolean isDistinctOverGroupingKeys(PlanNode node, Lookup lookup, Set<Symbol> parentSymbols)
    {
        return switch (node) {
            case AggregationNode aggregationNode ->
                    aggregationNode.getGroupingSets().getGroupingSetCount() == 1 && parentSymbols.containsAll(aggregationNode.getGroupingSets().getGroupingKeys());

            // Project nodes introduce new symbols for computed expressions, and therefore end up preserving distinctness
            // between the distinct aggregation and the child aggregation nodes so long as all child aggregation keys
            // remain present (without transformation by the project) in the distinct aggregation grouping keys
            case ProjectNode projectNode ->
                    isDistinctOverGroupingKeys(lookup.resolve(projectNode.getSource()), lookup, translateProjectReferences(projectNode, parentSymbols));

            // Filter nodes end up preserving distinctness over the input source
            case FilterNode filterNode ->
                    isDistinctOverGroupingKeys(lookup.resolve(filterNode.getSource()), lookup, parentSymbols);
            case null, default -> false;
        };
    }

    private static Set<Symbol> translateProjectReferences(ProjectNode projectNode, Set<Symbol> groupingKeys)
    {
        Set<Symbol> translated = new HashSet<>();
        Assignments assignments = projectNode.getAssignments();
        for (Symbol parentSymbol : groupingKeys) {
            Expression expression = assignments.get(parentSymbol);
            if (expression instanceof Reference reference) {
                translated.add(Symbol.from(reference));
            }
        }
        return translated;
    }
}
