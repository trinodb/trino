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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Replaces a grouped single any_value aggregation with a RowNumberNode that keeps
 * one row per group. This is currently supported only when the argument is a row
 * constructor.
 */
public class OptimizeSingleAnyValueAggregation
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName ANY_VALUE_NAME = builtinFunctionName("any_value");

    private static final Capture<PlanNode> SOURCE = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(OptimizeSingleAnyValueAggregation::isSupportedAggregation)
            .with(source().capturedAs(SOURCE));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Aggregation aggregation = getOnlyElement(node.getAggregations().values());
        Symbol value = Symbol.from(getOnlyElement(aggregation.getArguments()));
        PlanNode source = captures.get(SOURCE);

        // TODO: support any non-null input when we support nullability in the planner
        if (!isRowConstructor(value, source)) {
            return Result.empty();
        }

        Symbol rowNumber = context.getSymbolAllocator().newSymbol("row_number", BIGINT);
        RowNumberNode rowNumberNode = new RowNumberNode(
                context.getIdAllocator().getNextId(),
                node.getSource(),
                node.getGroupingKeys(),
                false,
                rowNumber,
                Optional.of(1));

        Symbol output = getOnlyElement(node.getAggregations().keySet());
        Assignments assignments = Assignments.builder()
                .putIdentities(node.getGroupingKeys())
                .put(output, value.toSymbolReference())
                .build();

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), rowNumberNode, assignments));
    }

    private static boolean isRowConstructor(Symbol value, PlanNode source)
    {
        return source instanceof ProjectNode projectNode &&
                projectNode.getAssignments().get(value) instanceof Row;
    }

    private static boolean isSupportedAggregation(AggregationNode node)
    {
        if (node.getStep() != SINGLE ||
                node.hasSingleGlobalAggregation() ||
                node.isStreamable() ||
                node.getGroupingSetCount() != 1 ||
                node.getAggregations().size() != 1) {
            return false;
        }

        var aggregation = getOnlyElement(node.getAggregations().values());
        return !aggregation.isDistinct() &&
                aggregation.getFilter().isEmpty() &&
                aggregation.getMask().isEmpty() &&
                aggregation.getOrderingScheme().isEmpty() &&
                aggregation.getArguments().size() == 1 &&
                getOnlyElement(aggregation.getArguments()) instanceof Reference &&
                aggregation.getResolvedFunction().signature().getName().equals(ANY_VALUE_NAME);
    }
}
