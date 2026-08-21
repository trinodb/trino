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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.isParallelizeChainedAggregations;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static io.trino.sql.planner.plan.Patterns.aggregation;

/**
 * Redistributes rows between chained aggregations with a local round-robin exchange
 * to parallelize the outer partial aggregation.
 * <p>
 * When an outer aggregation's grouping keys are a strict subset of an inner aggregation's
 * grouping keys, both end up in the same fragment and the outer PARTIAL aggregation runs in
 * the same pipeline as the inner aggregation, inheriting a row distribution hashed on the
 * inner grouping keys. When the inner aggregation output is skewed across drivers (e.g. few
 * distinct groups), the outer partial aggregation inherits that skew. Inserting a local
 * round-robin exchange rebalances rows evenly across all drivers:
 * <pre>
 * Aggregation(PARTIAL, keys ⊂ K)          Aggregation(PARTIAL, keys ⊂ K)
 *   [Project]*                              [Project]*
 *     Aggregation(FINAL|SINGLE, K)   =>       LocalExchange(ROUND_ROBIN)
 *                                               Aggregation(FINAL|SINGLE, K)
 * </pre>
 * This rule must run after {@link PushPartialAggregationThroughExchange}: that rule pushes
 * partial aggregations through exchanges without partitioning columns, so running them
 * together would undo this rewrite and loop.
 */
public class ParallelizeChainedAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> aggregation.getStep() == PARTIAL && aggregation.getPreGroupedSymbols().isEmpty());

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        // with a single driver per task there is no parallelism to regain
        return isParallelizeChainedAggregations(session) && getTaskConcurrency(session) > 1;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        PlanNode source = context.getLookup().resolve(node.getSource());

        // descend through projections, translating the outer grouping keys to the symbols they reference
        Set<Symbol> outerKeys = new HashSet<>(node.getGroupingKeys());
        List<ProjectNode> projections = new ArrayList<>();
        while (source instanceof ProjectNode project) {
            Set<Symbol> translated = new HashSet<>();
            for (Symbol key : outerKeys) {
                if (!(project.getAssignments().get(key) instanceof Reference reference)) {
                    return Result.empty();
                }
                translated.add(Symbol.from(reference));
            }
            outerKeys = translated;
            projections.add(project);
            source = context.getLookup().resolve(project.getSource());
        }

        if (!(source instanceof AggregationNode inner) || (inner.getStep() != FINAL && inner.getStep() != SINGLE)) {
            return Result.empty();
        }

        Set<Symbol> innerKeys = ImmutableSet.copyOf(inner.getGroupingKeys());
        if (!innerKeys.containsAll(outerKeys) || outerKeys.size() >= innerKeys.size()) {
            return Result.empty();
        }

        PlanNode result = roundRobinExchange(context.getIdAllocator().getNextId(), LOCAL, inner);
        for (ProjectNode project : projections.reversed()) {
            result = project.replaceChildren(ImmutableList.of(result));
        }
        return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(result)));
    }
}
