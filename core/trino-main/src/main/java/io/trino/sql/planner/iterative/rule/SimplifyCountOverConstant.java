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
import io.trino.metadata.BoundSignature;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

public class SimplifyCountOverConstant
        implements Rule<AggregationNode>
{
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

        ResolvedFunction countFunction = plannerContext.getMetadata().resolveFunction(context.getSession(), QualifiedName.of("count"), ImmutableList.of());

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

        return Result.ofPlanNode(new AggregationNode(
                parent.getId(),
                child,
                aggregations,
                parent.getGroupingSets(),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashSymbol(),
                parent.getGroupIdSymbol()));
    }

    private static boolean isCountOverConstant(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        BoundSignature signature = aggregation.getResolvedFunction().getSignature();
        if (!signature.getName().equals("count") || signature.getArgumentTypes().size() != 1) {
            return false;
        }

        Expression argument = aggregation.getArguments().get(0);
        if (argument instanceof SymbolReference) {
            argument = inputs.get(Symbol.from(argument));
        }

        return argument instanceof Literal && !(argument instanceof NullLiteral);
    }
}
