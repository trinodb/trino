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
package io.trino.sql.planner.optimizations;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortMergeJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;

public class EliminateSortOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator,
                             WarningCollector warningCollector, PlanOptimizersStatsCollector planOptimizersStatsCollector, TableStatsProvider tableStatsProvider)
    {
        PlanWithProperties result = plan.accept(new Rewriter(), null);
        return result.getNode();
    }

    private static class Rewriter
            extends PlanVisitor<PlanWithProperties, Void>
    {
        @Override
        public PlanWithProperties visitExchange(ExchangeNode node, Void context)
        {
            PlanWithProperties plan = visitPlan(node, context);
            if (node.getOrderingScheme().isPresent()) {
                return new PlanWithProperties(plan.getNode(), ImmutableList.of(node.getOrderingScheme().get()));
            }
            else {
                return new PlanWithProperties(plan.getNode());
            }
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, Void context)
        {
            PlanWithProperties child = node.getSource().accept(this, context);
            return new PlanWithProperties(node.replaceChildren(ImmutableList.of(child.getNode())), ImmutableList.of(node.getOrderingScheme()));
        }

        @Override
        public PlanWithProperties visitSortMergeJoin(SortMergeJoinNode node, Void context)
        {
            PlanWithProperties left = node.getLeft().accept(this, context);
            PlanWithProperties right = node.getRight().accept(this, context);

            OrderingScheme leftOrderingRequire = node.getLeftOrdering();
            OrderingScheme rightOrderingRequire = node.getRightOrdering();

            List<OrderingScheme> ordering = new ArrayList<>();
            switch (node.getType()) {
                case INNER:
                    ordering.add(leftOrderingRequire);
                    ordering.add(rightOrderingRequire);
                    break;
                case LEFT:
                    ordering.add(leftOrderingRequire);
                    break;
                case RIGHT:
                    ordering.add(rightOrderingRequire);
                    break;
                case FULL:
                    break;
            }
            return new PlanWithProperties(node.replaceChildren(ImmutableList.of(left.getNode(), right.getNode()),
                    !left.isOrderingSatisfiedBy(leftOrderingRequire), !right.isOrderingSatisfiedBy(rightOrderingRequire)),
                    ordering);
        }

        @Override
        public PlanWithProperties visitFilter(FilterNode node, Void context)
        {
            return useChildProperties(node, context);
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, Void context)
        {
            return useChildProperties(node, context);
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, Void context)
        {
            PlanWithProperties child = node.getSource().accept(this, context);
            return new PlanWithProperties(node.replaceChildren(ImmutableList.of(child.getNode())), ImmutableList.of(node.getOrderingScheme()));
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, Void context)
        {
            return useChildProperties(node, context);
        }

        @Override
        public PlanWithProperties visitOffset(OffsetNode node, Void context)
        {
            return useChildProperties(node, context);
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, Void context)
        {
            if (node.isWithTies()) {
                return useNoneProperties(node, context);
            }
            else {
                return useChildProperties(node, context);
            }
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, Void context)
        {
            return useNoneProperties(node, context);
        }

        private PlanWithProperties useNoneProperties(PlanNode node, Void context)
        {
            List<PlanNode> sources = node.getSources();
            if (sources.size() == 0) {
                return new PlanWithProperties(node);
            }
            else {
                List<PlanWithProperties> newChildren = new ArrayList<>();
                for (PlanNode planNode : node.getSources()) {
                    PlanWithProperties plan = planNode.accept(this, context);
                    newChildren.add(plan);
                }
                return new PlanWithProperties(node.replaceChildren(newChildren.stream().map(p -> p.getNode()).collect(Collectors.toList())));
            }
        }

        private PlanWithProperties useChildProperties(PlanNode node, Void context)
        {
            PlanWithProperties child = planChild(node, context);
            return new PlanWithProperties(node.replaceChildren(ImmutableList.of(child.getNode())), child.getOrdering());
        }

        private PlanWithProperties planChild(PlanNode node, Void context)
        {
            return getOnlyElement(node.getSources()).accept(this, context);
        }
    }

    @VisibleForTesting
    static class PlanWithProperties
    {
        private final PlanNode node;
        private final List<OrderingScheme> ordering;

        public PlanWithProperties(PlanNode node)
        {
            this(node, Collections.emptyList());
        }

        public PlanWithProperties(PlanNode node, List<OrderingScheme> ordering)
        {
            this.node = node;
            this.ordering = ordering;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public List<OrderingScheme> getOrdering()
        {
            return ordering;
        }

        public boolean isOrderingSatisfiedBy(OrderingScheme require)
        {
            for (OrderingScheme orderingScheme : ordering) {
                if (orderingScheme.equals(require)) {
                    return true;
                }
            }
            return false;
        }
    }
}
