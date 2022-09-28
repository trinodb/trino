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
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.UnionNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.isOptimizeDuplicateInsensitiveJoins;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * For empty aggregations duplicate input rows can be skipped.
 * This rule takes advantage of this fact and sets
 * {@link JoinNode#withMaySkipOutputDuplicates()} for joins below
 * such aggregation.
 */
public class OptimizeDuplicateInsensitiveJoins
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> aggregation.getAggregations().isEmpty());

    private final Metadata metadata;

    public OptimizeDuplicateInsensitiveJoins(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeDuplicateInsensitiveJoins(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        return aggregation.getSource().accept(new Rewriter(metadata, context.getLookup()), null)
                .map(rewrittenSource -> Result.ofPlanNode(aggregation.replaceChildren(ImmutableList.of(rewrittenSource))))
                .orElse(Result.empty());
    }

    private static class Rewriter
            extends PlanVisitor<Optional<PlanNode>, Void>
    {
        private final Metadata metadata;
        private final Lookup lookup;

        private Rewriter(Metadata metadata, Lookup lookup)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        protected Optional<PlanNode> visitPlan(PlanNode node, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<PlanNode> visitFilter(FilterNode node, Void context)
        {
            if (!isDeterministic(node.getPredicate(), metadata)) {
                // non-deterministic expressions could filter duplicate rows probabilistically
                return Optional.empty();
            }

            return node.getSource().accept(this, null)
                    .map(source -> node.replaceChildren(ImmutableList.of(source)));
        }

        @Override
        public Optional<PlanNode> visitProject(ProjectNode node, Void context)
        {
            boolean isDeterministic = node.getAssignments().getExpressions().stream()
                    .allMatch(expression -> isDeterministic(expression, metadata));
            if (!isDeterministic) {
                // non-deterministic projections could be used in downstream filters which could
                // filter duplicate rows probabilistically
                return Optional.empty();
            }

            return node.getSource().accept(this, null)
                    .map(source -> node.replaceChildren(ImmutableList.of(source)));
        }

        @Override
        public Optional<PlanNode> visitUnion(UnionNode node, Void context)
        {
            List<PlanNode> rewrittenSources = node.getSources().stream()
                    .map(source -> source.accept(this, null).orElse(source))
                    .collect(toImmutableList());

            if (rewrittenSources.equals(node.getSources())) {
                return Optional.empty();
            }

            return Optional.of(node.replaceChildren(rewrittenSources));
        }

        @Override
        public Optional<PlanNode> visitJoin(JoinNode node, Void context)
        {
            // LookupJoinOperator will evaluate non-deterministic condition on output rows until one of the
            // rows matches. Therefore it's safe to set maySkipOutputDuplicates for joins with non-deterministic
            // filters.
            if (!isDeterministic(node.getFilter().orElse(TRUE_LITERAL), metadata)) {
                if (node.isMaySkipOutputDuplicates()) {
                    // join node is already set to skip duplicates, return empty to prevent rule from looping forever
                    return Optional.empty();
                }

                return Optional.of(node.withMaySkipOutputDuplicates());
            }

            Optional<PlanNode> rewrittenLeft = node.getLeft().accept(this, null);
            Optional<PlanNode> rewrittenRight = node.getRight().accept(this, null);

            if (node.isMaySkipOutputDuplicates() && rewrittenLeft.isEmpty() && rewrittenRight.isEmpty()) {
                // join node is already set to skip duplicates and children were not modified, return empty
                // to prevent rule from looping forever
                return Optional.empty();
            }

            return Optional.of(node.withMaySkipOutputDuplicates().replaceChildren(ImmutableList.of(
                    rewrittenLeft.orElse(node.getLeft()),
                    rewrittenRight.orElse(node.getRight()))));
        }

        @Override
        public Optional<PlanNode> visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, null);
        }
    }
}
