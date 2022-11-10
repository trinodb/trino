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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;

import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.matching.Pattern.typeOf;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.SemiJoin.getFilteringSource;
import static io.trino.sql.planner.plan.Patterns.dynamicFilterSource;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.semiJoin;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * For task retry mode, dynamic filters are removed from the JoinNode/SemiJoinNode
 * and rewritten into a DynamicFilterSourceNode below the remote exchange on the
 * build side of the join.
 */
public final class AddDynamicFilterSource
{
    private AddDynamicFilterSource() {}

    public static Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new RewriteJoinDynamicFilter(),
                new RewriteSemiJoinDynamicFilter(),
                new PushOrRemoveDynamicFilterSource());
    }

    private static class RewriteJoinDynamicFilter
            implements Rule<JoinNode>
    {
        private static final Capture<PlanNode> BUILD_SIDE_NODE = Capture.newCapture();
        private static final Pattern<JoinNode> PATTERN = join()
                .matching(node -> !node.getDynamicFilters().isEmpty())
                .with(right().capturedAs(BUILD_SIDE_NODE));

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return getRetryPolicy(session) == RetryPolicy.TASK;
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            PlanNode buildSource = captures.get(BUILD_SIDE_NODE);
            // We try to push down or remove DynamicFilterSourceNode below right child of join here so that
            // PushOrRemoveDynamicFilterSource can just handle cases of DynamicFilterSourceNode below a plan node with single source
            if (!canAddDynamicFilterSource(buildSource, joinNode.getDynamicFilters().values())) {
                return Result.ofPlanNode(joinNode.withoutDynamicFilters());
            }
            return Result.ofPlanNode(
                    joinNode.withoutDynamicFilters().replaceChildren(ImmutableList.of(
                            joinNode.getLeft(),
                            buildSource.replaceChildren(ImmutableList.of(
                                    new DynamicFilterSourceNode(
                                            context.getIdAllocator().getNextId(),
                                            getOnlyElement(buildSource.getSources()),
                                            joinNode.getDynamicFilters()))))));
        }
    }

    private static class RewriteSemiJoinDynamicFilter
            implements Rule<SemiJoinNode>
    {
        private static final Capture<PlanNode> FILTERING_SOURCE_NODE = Capture.newCapture();
        private static final Pattern<SemiJoinNode> PATTERN = semiJoin()
                .matching(node -> node.getDynamicFilterId().isPresent())
                .with(getFilteringSource().capturedAs(FILTERING_SOURCE_NODE));

        @Override
        public Pattern<SemiJoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return getRetryPolicy(session) == RetryPolicy.TASK;
        }

        @Override
        public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
        {
            PlanNode filteringSource = captures.get(FILTERING_SOURCE_NODE);
            // We try to push down or remove DynamicFilterSourceNode below right child of join here so that
            // PushOrRemoveDynamicFilterSource can just handle cases of DynamicFilterSourceNode below a plan node with single source
            if (!canAddDynamicFilterSource(filteringSource, ImmutableList.of(semiJoinNode.getFilteringSourceJoinSymbol()))) {
                return Result.ofPlanNode(semiJoinNode.withoutDynamicFilter());
            }
            return Result.ofPlanNode(
                    semiJoinNode.withoutDynamicFilter().replaceChildren(ImmutableList.of(
                            semiJoinNode.getSource(),
                            filteringSource.replaceChildren(ImmutableList.of(
                                    new DynamicFilterSourceNode(
                                            context.getIdAllocator().getNextId(),
                                            getOnlyElement(filteringSource.getSources()),
                                            ImmutableMap.of(
                                                    semiJoinNode.getDynamicFilterId().orElseThrow(),
                                                    semiJoinNode.getFilteringSourceJoinSymbol())))))));
        }
    }

    private static class PushOrRemoveDynamicFilterSource
            implements Rule<PlanNode>
    {
        private static final Capture<DynamicFilterSourceNode> DYNAMIC_FILTER_SOURCE = Capture.newCapture();
        private static final Pattern<PlanNode> PATTERN = typeOf(PlanNode.class)
                .with(source().matching(dynamicFilterSource().capturedAs(DYNAMIC_FILTER_SOURCE)));

        @Override
        public Pattern<PlanNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return getRetryPolicy(session) == RetryPolicy.TASK;
        }

        @Override
        public Result apply(PlanNode node, Captures captures, Context context)
        {
            if (isRemoteExchange(node)) {
                // If DynamicFilterSourceNode is already below a remote exchange, don't change the plan
                return Result.empty();
            }
            DynamicFilterSourceNode dynamicFilterSourceNode = captures.get(DYNAMIC_FILTER_SOURCE);
            PlanNode dynamicFilterChildNode = context.getLookup().resolve(dynamicFilterSourceNode.getSource());
            if (!canAddDynamicFilterSource(dynamicFilterChildNode, dynamicFilterSourceNode.getDynamicFilters().values())) {
                // DynamicFilterSourceNode is not below a remote exchange and can't be pushed further, so remove it from the plan
                return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(dynamicFilterChildNode)));
            }
            PlanNode dynamicFilterSourceRewritten = dynamicFilterSourceNode.replaceChildren(dynamicFilterChildNode.getSources());
            return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(
                    dynamicFilterChildNode.replaceChildren(ImmutableList.of(dynamicFilterSourceRewritten)))));
        }
    }

    private static boolean canAddDynamicFilterSource(PlanNode node, Collection<Symbol> dynamicFilterSymbols)
    {
        boolean isIdentityProjection = (node instanceof ProjectNode) &&
                dynamicFilterSymbols.stream().allMatch(symbol -> ((ProjectNode) node).getAssignments().isIdentity(symbol));
        // TODO: Add support for cases where the build side has multiple sources, e.g., a UNION ALL for a join
        return isIdentityProjection || (node instanceof ExchangeNode && node.getSources().size() == 1);
    }

    private static boolean isRemoteExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode)) {
            return false;
        }

        return ((ExchangeNode) node).getScope() == REMOTE;
    }
}
