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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

public class SimplifyCountOverConstant
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName COUNT_NAME = builtinFunctionName("count");
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(CHILD)));

    private final PlannerContext plannerContext;

    public SimplifyCountOverConstant(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        boolean changed = false;
        Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(parent.getAggregations());

        ResolvedFunction countFunction = plannerContext.getMetadata().resolveBuiltinFunction("count", ImmutableList.of());

        for (Entry<Symbol, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            Symbol symbol = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (isCountOverConstant(aggregation, child.getAssignments())) {
                changed = true;
                aggregations.put(symbol, new AggregationNode.Aggregation(
                        countFunction,
                        ImmutableList.of(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        aggregation.getMask()));
            }
        }

        if (!changed) {
            return Result.empty();
        }

        return Result.ofPlanNode(AggregationNode.builderFrom(parent)
                .setSource(child)
                .setAggregations(aggregations)
                .setPreGroupedSymbols(ImmutableList.of())
                .build());
    }

    private boolean isCountOverConstant(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        BoundSignature signature = aggregation.getResolvedFunction().signature();
        if (!signature.getName().equals(COUNT_NAME) || signature.getArgumentTypes().size() != 1) {
            return false;
        }

        if (aggregation.isDistinct()) {
            return false;
        }

        Expression argument = aggregation.getArguments().get(0);
        if (argument instanceof Reference) {
            argument = inputs.get(Symbol.from(argument));
        }

        return argument instanceof Constant constant && constant.value() != null;
    }
}
