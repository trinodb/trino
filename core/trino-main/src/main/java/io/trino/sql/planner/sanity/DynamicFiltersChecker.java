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
package io.trino.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.SubExpressionExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;

/**
 * When dynamic filter assignments are present on a Join node, they should be consumed by a Filter node on it's probe side
 */
public class DynamicFiltersChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(
            PlanNode plan,
            Session session,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        plan.accept(new PlanVisitor<Set<DynamicFilterId>, Void>()
        {
            @Override
            protected Set<DynamicFilterId> visitPlan(PlanNode node, Void context)
            {
                Set<DynamicFilterId> consumed = new HashSet<>();
                for (PlanNode source : node.getSources()) {
                    consumed.addAll(source.accept(this, context));
                }
                return consumed;
            }

            @Override
            public Set<DynamicFilterId> visitOutput(OutputNode node, Void context)
            {
                Set<DynamicFilterId> unmatched = visitPlan(node, context);
                verify(unmatched.isEmpty(), "All consumed dynamic filters could not be matched with a join/semi-join.");
                return unmatched;
            }

            @Override
            public Set<DynamicFilterId> visitJoin(JoinNode node, Void context)
            {
                Set<DynamicFilterId> currentJoinDynamicFilters = node.getDynamicFilters().keySet();
                Set<DynamicFilterId> consumedProbeSide = node.getLeft().accept(this, context);
                Set<DynamicFilterId> unconsumedByProbeSide = difference(currentJoinDynamicFilters, consumedProbeSide);
                verify(unconsumedByProbeSide.isEmpty(),
                        "Dynamic filters %s present in join were not fully consumed by it's probe side.", unconsumedByProbeSide);

                Set<DynamicFilterId> consumedBuildSide = node.getRight().accept(this, context);
                Set<DynamicFilterId> unconsumedByBuildSide = intersection(currentJoinDynamicFilters, consumedBuildSide);
                verify(unconsumedByBuildSide.isEmpty(),
                        "Dynamic filters %s present in join were consumed by it's build side.", unconsumedByBuildSide);

                List<DynamicFilters.Descriptor> nonPushedDownFilters = node
                        .getFilter()
                        .map(DynamicFilters::extractDynamicFilters)
                        .map(DynamicFilters.ExtractResult::getDynamicConjuncts)
                        .orElse(ImmutableList.of());
                verify(nonPushedDownFilters.isEmpty(), "Dynamic filters %s present in join filter predicate were not pushed down.", nonPushedDownFilters);

                Set<DynamicFilterId> unmatched = new HashSet<>(consumedBuildSide);
                unmatched.addAll(consumedProbeSide);
                unmatched.removeAll(currentJoinDynamicFilters);
                return ImmutableSet.copyOf(unmatched);
            }

            @Override
            public Set<DynamicFilterId> visitSemiJoin(SemiJoinNode node, Void context)
            {
                Set<DynamicFilterId> consumedSourceSide = node.getSource().accept(this, context);
                Set<DynamicFilterId> consumedFilteringSourceSide = node.getFilteringSource().accept(this, context);

                Set<DynamicFilterId> unmatched = new HashSet<>(consumedSourceSide);
                unmatched.addAll(consumedFilteringSourceSide);

                if (node.getDynamicFilterId().isPresent()) {
                    DynamicFilterId dynamicFilterId = node.getDynamicFilterId().get();
                    verify(consumedSourceSide.contains(dynamicFilterId),
                            "The dynamic filter %s present in semi-join was not consumed by it's source side.", dynamicFilterId);
                    verify(!consumedFilteringSourceSide.contains(dynamicFilterId),
                            "The dynamic filter %s present in semi-join was consumed by it's filtering source side.", dynamicFilterId);
                    unmatched.remove(dynamicFilterId);
                }

                return ImmutableSet.copyOf(unmatched);
            }

            @Override
            public Set<DynamicFilterId> visitFilter(FilterNode node, Void context)
            {
                List<DynamicFilters.Descriptor> dynamicFilters = extractDynamicPredicates(node.getPredicate());
                if (!dynamicFilters.isEmpty()) {
                    verify(node.getSource() instanceof TableScanNode, "Dynamic filters %s present in filter predicate whose source is not a table scan.", dynamicFilters);
                }
                ImmutableSet.Builder<DynamicFilterId> consumed = ImmutableSet.builder();
                dynamicFilters.forEach(descriptor -> {
                    validateDynamicFilterExpression(descriptor.getInput());
                    consumed.add(descriptor.getId());
                });
                consumed.addAll(node.getSource().accept(this, context));
                return consumed.build();
            }
        }, null);
    }

    private static void validateDynamicFilterExpression(Expression expression)
    {
        if (expression instanceof SymbolReference) {
            return;
        }
        verify(expression instanceof Cast,
                "Dynamic filter expression %s must be a SymbolReference or a CAST of SymbolReference.", expression);
        Cast castExpression = (Cast) expression;
        verify(castExpression.getExpression() instanceof SymbolReference,
                "The expression %s within in a CAST in dynamic filter must be a SymbolReference.", castExpression.getExpression());
    }

    private static List<DynamicFilters.Descriptor> extractDynamicPredicates(Expression expression)
    {
        return SubExpressionExtractor.extract(expression)
                .map(DynamicFilters::getDescriptor)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }
}
