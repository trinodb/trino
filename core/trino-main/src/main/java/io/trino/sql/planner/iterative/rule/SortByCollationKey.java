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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.trino.sql.planner.plan.Patterns.rowNumber;
import static io.trino.sql.planner.plan.Patterns.sort;
import static io.trino.sql.planner.plan.Patterns.topN;
import static io.trino.sql.planner.plan.Patterns.topNRanking;
import static io.trino.sql.planner.plan.Patterns.window;
import static io.trino.sql.planner.plan.TopNNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

/// Sorts and partitions collated strings by their collation keys instead of by the strings.
///
/// Comparing two collated strings applies their collation, while comparing collation keys compares
/// bytes, so computing the key once per row is cheaper than applying the collation in every
/// comparison of a sort. Each rule projects the keys below the node, orders or partitions by them,
/// and projects them away above it. The node still receives and produces the strings, so the
/// rows keep their values.
public class SortByCollationKey
{
    private final Metadata metadata;

    public SortByCollationKey(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new SortRule(),
                new TopNRule(),
                new WindowRule(),
                new TopNRankingRule(),
                new RowNumberRule());
    }

    private abstract static class CollationKeyRule<T extends PlanNode>
            implements Rule<T>
    {
        // projects the keys below the node and removes them above it
        protected Result result(T node, PlanNode source, CollationKeys keys, Function<PlanNode, PlanNode> rebuild, Context context)
        {
            PlanNode keyed = new ProjectNode(context.getIdAllocator().getNextId(), source, keys.assignments(source.getOutputSymbols()));
            PlanNode rewritten = rebuild.apply(keyed);
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), rewritten, Assignments.identity(node.getOutputSymbols())));
        }
    }

    private class SortRule
            extends CollationKeyRule<SortNode>
    {
        @Override
        public Pattern<SortNode> getPattern()
        {
            return sort().matching(node -> !node.isPartial() && CollationKeys.hasCollated(node.getOrderingScheme()));
        }

        @Override
        public Result apply(SortNode node, Captures captures, Context context)
        {
            CollationKeys keys = new CollationKeys(metadata, context.getSession(), context.getSymbolAllocator());
            OrderingScheme orderingScheme = keys.keys(node.getOrderingScheme());
            return result(node, node.getSource(), keys, source -> new SortNode(node.getId(), source, orderingScheme, node.isPartial()), context);
        }
    }

    private class TopNRule
            extends CollationKeyRule<TopNNode>
    {
        @Override
        public Pattern<TopNNode> getPattern()
        {
            return topN().matching(node -> node.getStep() == SINGLE && CollationKeys.hasCollated(node.getOrderingScheme()));
        }

        @Override
        public Result apply(TopNNode node, Captures captures, Context context)
        {
            CollationKeys keys = new CollationKeys(metadata, context.getSession(), context.getSymbolAllocator());
            OrderingScheme orderingScheme = keys.keys(node.getOrderingScheme());
            return result(node, node.getSource(), keys, source -> new TopNNode(node.getId(), source, node.getCount(), orderingScheme, node.getStep()), context);
        }
    }

    private class WindowRule
            extends CollationKeyRule<WindowNode>
    {
        @Override
        public Pattern<WindowNode> getPattern()
        {
            return window().matching(node -> node.getPrePartitionedInputs().isEmpty()
                    && node.getPreSortedOrderPrefix() == 0
                    && hasCollated(node.getSpecification()));
        }

        @Override
        public Result apply(WindowNode node, Captures captures, Context context)
        {
            CollationKeys keys = new CollationKeys(metadata, context.getSession(), context.getSymbolAllocator());
            DataOrganizationSpecification specification = keys(keys, node.getSpecification());
            return result(node, node.getSource(), keys, source -> new WindowNode(
                    node.getId(),
                    source,
                    specification,
                    node.getWindowFunctions(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix()), context);
        }
    }

    private class TopNRankingRule
            extends CollationKeyRule<TopNRankingNode>
    {
        @Override
        public Pattern<TopNRankingNode> getPattern()
        {
            return topNRanking().matching(node -> !node.isPartial() && hasCollated(node.getSpecification()));
        }

        @Override
        public Result apply(TopNRankingNode node, Captures captures, Context context)
        {
            CollationKeys keys = new CollationKeys(metadata, context.getSession(), context.getSymbolAllocator());
            DataOrganizationSpecification specification = keys(keys, node.getSpecification());
            return result(node, node.getSource(), keys, source -> new TopNRankingNode(
                    node.getId(),
                    source,
                    specification,
                    node.getRankingType(),
                    node.getRankingSymbol(),
                    node.getMaxRankingPerPartition(),
                    node.isPartial()), context);
        }
    }

    private class RowNumberRule
            extends CollationKeyRule<RowNumberNode>
    {
        @Override
        public Pattern<RowNumberNode> getPattern()
        {
            return rowNumber().matching(node -> CollationKeys.hasCollated(node.getPartitionBy()));
        }

        @Override
        public Result apply(RowNumberNode node, Captures captures, Context context)
        {
            CollationKeys keys = new CollationKeys(metadata, context.getSession(), context.getSymbolAllocator());
            List<Symbol> partitionBy = keys.keys(node.getPartitionBy());
            return result(node, node.getSource(), keys, source -> new RowNumberNode(
                    node.getId(),
                    source,
                    partitionBy,
                    node.isOrderSensitive(),
                    node.getRowNumberSymbol(),
                    node.getMaxRowCountPerPartition()), context);
        }
    }

    private static boolean hasCollated(DataOrganizationSpecification specification)
    {
        return CollationKeys.hasCollated(specification.partitionBy())
                || specification.orderingScheme().map(CollationKeys::hasCollated).orElse(false);
    }

    private static DataOrganizationSpecification keys(CollationKeys keys, DataOrganizationSpecification specification)
    {
        return new DataOrganizationSpecification(keys.keys(specification.partitionBy()), keys.keys(specification.orderingScheme()));
    }
}
