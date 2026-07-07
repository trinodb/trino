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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/// Limit the source of a global `bool_or` aggregation over a constant `true`
/// argument to a single row.
///
/// The result of `bool_or` over rows that are all `true` depends only on whether
/// the source produces any row, so a single arbitrary row is sufficient. This shape
/// is produced by [TransformExistsApplyToCorrelatedJoin] for uncorrelated EXISTS
/// subqueries, where the inserted limit prevents a full scan of the subquery source.
///
/// Transforms:
/// ```
/// - aggregation
///   global grouping
///   bool <- bool_or(s)
///     - project (s := true)
///       - source
/// ```
/// into:
/// ```
/// - aggregation
///   global grouping
///   bool <- bool_or(s)
///     - project (s := true)
///       - limit (1)
///         - source
/// ```
///
/// The rule must run after subquery planning is complete (see `CheckSubqueryNodesAreRewritten`),
/// when the aggregation source can no longer contain correlated references. Inserting the limit
/// earlier could prevent decorrelation of EXISTS subqueries correlated with an outer query.
public class LimitBoolOrAggregationSource
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName BOOL_OR = builtinFunctionName("bool_or");

    private static final Capture<ProjectNode> PROJECT = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(LimitBoolOrAggregationSource::isGlobalBoolOr)
            .with(source().matching(project().capturedAs(PROJECT)));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT);

        Aggregation aggregation = getOnlyElement(node.getAggregations().values());
        Symbol argument = Symbol.from(aggregation.getArguments().getFirst());
        if (!TRUE.equals(projectNode.getAssignments().get(argument))) {
            return Result.empty();
        }

        if (isAtMostScalar(projectNode.getSource(), context.getLookup())) {
            return Result.empty();
        }

        LimitNode limit = new LimitNode(
                context.getIdAllocator().getNextId(),
                projectNode.getSource(),
                1L,
                false);
        return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(projectNode.replaceChildren(ImmutableList.of(limit)))));
    }

    private static boolean isGlobalBoolOr(AggregationNode node)
    {
        if (!node.hasSingleGlobalAggregation() || node.getStep() != SINGLE) {
            return false;
        }

        if (node.getAggregations().size() != 1) {
            return false;
        }

        Aggregation aggregation = getOnlyElement(node.getAggregations().values());
        if (aggregation.isDistinct() ||
                aggregation.getFilter().isPresent() ||
                aggregation.getMask().isPresent() ||
                aggregation.getOrderingScheme().isPresent()) {
            return false;
        }

        if (!aggregation.getResolvedFunction().name().equals(BOOL_OR)) {
            return false;
        }

        List<Expression> arguments = aggregation.getArguments();
        return arguments.size() == 1 && arguments.getFirst() instanceof Reference;
    }
}
