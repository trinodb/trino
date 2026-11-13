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
import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/// Groups by the collation key of a collated string instead of by the string.
///
/// Hashing and comparing a collated string apply its collation to every value, which costs much
/// more than comparing bytes. The collation key is computed once per row, and equal keys identify
/// equal strings under the collation, so grouping by the key forms the same groups:
/// <pre>
/// - Aggregation
///     GROUP BY (x)
///     F(...)
/// </pre>
/// becomes
/// <pre>
/// - Project
///     x := x_value
///   - Aggregation
///       GROUP BY (x_key)
///       F(...), x_value := any_value(x)
///     - Project
///         x_key := $collation_key(x)
/// </pre>
/// The group's value of `x` is one of its members, as for grouping by `x` itself.
public class GroupByCollationKey
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(GroupByCollationKey::isEligible);

    private final Metadata metadata;

    public GroupByCollationKey(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    private static boolean isEligible(AggregationNode node)
    {
        return node.getStep() == SINGLE
                && node.getGroupingSetCount() == 1
                && node.getGroupIdSymbol().isEmpty()
                && node.getPreGroupedSymbols().isEmpty()
                && CollationKeys.hasCollated(node.getGroupingKeys());
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        CollationKeys keys = new CollationKeys(metadata, context.getSession(), context.getSymbolAllocator());
        ImmutableList.Builder<Symbol> groupingKeys = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.<Symbol, Aggregation>builder()
                .putAll(node.getAggregations());
        Assignments.Builder outputs = Assignments.builder();
        for (Symbol key : node.getGroupingKeys()) {
            if (!CollationKeys.isCollated(key)) {
                groupingKeys.add(key);
                outputs.putIdentity(key);
                continue;
            }
            groupingKeys.add(keys.key(key));

            ResolvedFunction anyValue = metadata.resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), "any_value", List.of(key.type()));
            Symbol valueSymbol = context.getSymbolAllocator().newSymbol(key);
            aggregations.put(valueSymbol, new Aggregation(anyValue, ImmutableList.of(key.toSymbolReference()), false, Optional.empty(), Optional.empty(), Optional.empty()));
            outputs.put(key, valueSymbol.toSymbolReference());
        }
        Map<Symbol, Aggregation> newAggregations = aggregations.buildOrThrow();
        newAggregations.keySet().forEach(outputs::putIdentity);
        outputs = filterToOriginalOutputs(outputs, node);

        AggregationNode aggregation = AggregationNode.builderFrom(node)
                .setSource(new ProjectNode(context.getIdAllocator().getNextId(), node.getSource(), keys.assignments(node.getSource().getOutputSymbols())))
                .setGroupingSets(singleGroupingSet(groupingKeys.build()))
                .setAggregations(newAggregations)
                .setPreGroupedSymbols(ImmutableList.of())
                .build();
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), aggregation, outputs.build()));
    }

    // the outer projection exposes exactly the original aggregation's outputs, in their order
    private static Assignments.Builder filterToOriginalOutputs(Assignments.Builder outputs, AggregationNode node)
    {
        Assignments all = outputs.build();
        Assignments.Builder filtered = Assignments.builder();
        for (Symbol output : node.getOutputSymbols()) {
            filtered.put(output, all.get(output));
        }
        return filtered;
    }
}
