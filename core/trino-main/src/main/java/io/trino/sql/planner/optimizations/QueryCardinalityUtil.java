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

import com.google.common.collect.Range;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public final class QueryCardinalityUtil
{
    private QueryCardinalityUtil() {}

    public static boolean isScalar(PlanNode node)
    {
        return isScalar(node, noLookup());
    }

    public static boolean isScalar(PlanNode node, Lookup lookup)
    {
        return extractCardinality(node, lookup).isScalar();
    }

    public static boolean isAtMostScalar(PlanNode node)
    {
        return isAtMostScalar(node, noLookup());
    }

    public static boolean isAtMostScalar(PlanNode node, Lookup lookup)
    {
        return isAtMost(node, lookup, 1L);
    }

    public static boolean isAtMost(PlanNode node, Lookup lookup, long maxCardinality)
    {
        return extractCardinality(node, lookup).isAtMost(maxCardinality);
    }

    public static boolean isAtLeastScalar(PlanNode node, Lookup lookup)
    {
        return isAtLeast(node, lookup, 1L);
    }

    public static boolean isAtLeast(PlanNode node, Lookup lookup, long minCardinality)
    {
        return extractCardinality(node, lookup).isAtLeast(minCardinality);
    }

    public static boolean isEmpty(PlanNode node, Lookup lookup)
    {
        return isAtMost(node, lookup, 0);
    }

    public static Cardinality extractCardinality(PlanNode node)
    {
        return extractCardinality(node, noLookup());
    }

    public static Cardinality extractCardinality(PlanNode node, Lookup lookup)
    {
        return new Cardinality(node.accept(new CardinalityExtractorPlanVisitor(lookup), null));
    }

    private static final class CardinalityExtractorPlanVisitor
            extends PlanVisitor<Range<Long>, Void>
    {
        private final Lookup lookup;

        public CardinalityExtractorPlanVisitor(Lookup lookup)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        protected Range<Long> visitPlan(PlanNode node, Void context)
        {
            return Range.atLeast(0L);
        }

        @Override
        public Range<Long> visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public Range<Long> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return Range.singleton(1L);
        }

        @Override
        public Range<Long> visitAggregation(AggregationNode node, Void context)
        {
            if (node.hasSingleGlobalAggregation()) {
                // only single default aggregation which will produce exactly single row
                return Range.singleton(1L);
            }

            Range<Long> sourceCardinalityRange = node.getSource().accept(this, null);

            long lower;
            if (node.hasDefaultOutput() || sourceCardinalityRange.lowerEndpoint() > 0) {
                lower = 1;
            }
            else {
                lower = 0;
            }

            if (sourceCardinalityRange.hasUpperBound()) {
                long upper = Math.max(lower, sourceCardinalityRange.upperEndpoint());
                return Range.closed(lower, upper);
            }

            return Range.atLeast(lower);
        }

        @Override
        public Range<Long> visitExchange(ExchangeNode node, Void context)
        {
            if (node.getSources().size() == 1) {
                return getOnlyElement(node.getSources()).accept(this, null);
            }
            return Range.atLeast(0L);
        }

        @Override
        public Range<Long> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }

        @Override
        public Range<Long> visitFilter(FilterNode node, Void context)
        {
            Range<Long> sourceCardinalityRange = node.getSource().accept(this, null);
            if (sourceCardinalityRange.hasUpperBound()) {
                return Range.closed(0L, sourceCardinalityRange.upperEndpoint());
            }
            return Range.atLeast(0L);
        }

        @Override
        public Range<Long> visitValues(ValuesNode node, Void context)
        {
            return Range.singleton((long) node.getRowCount());
        }

        @Override
        public Range<Long> visitOffset(OffsetNode node, Void context)
        {
            Range<Long> sourceCardinalityRange = node.getSource().accept(this, null);

            long lower = max(sourceCardinalityRange.lowerEndpoint() - node.getCount(), 0L);

            if (sourceCardinalityRange.hasUpperBound()) {
                return Range.closed(lower, max(sourceCardinalityRange.upperEndpoint() - node.getCount(), 0L));
            }
            return Range.atLeast(lower);
        }

        @Override
        public Range<Long> visitLimit(LimitNode node, Void context)
        {
            if (node.isWithTies()) {
                Range<Long> sourceCardinalityRange = node.getSource().accept(this, null);
                long lower = min(node.getCount(), sourceCardinalityRange.lowerEndpoint());
                if (sourceCardinalityRange.hasUpperBound()) {
                    return Range.closed(lower, sourceCardinalityRange.upperEndpoint());
                }
                return Range.atLeast(lower);
            }

            return applyLimit(node.getSource(), node.getCount());
        }

        @Override
        public Range<Long> visitTopN(TopNNode node, Void context)
        {
            return applyLimit(node.getSource(), node.getCount());
        }

        @Override
        public Range<Long> visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }

        private Range<Long> applyLimit(PlanNode source, long limit)
        {
            Range<Long> sourceCardinalityRange = source.accept(this, null);
            if (sourceCardinalityRange.hasUpperBound()) {
                limit = min(sourceCardinalityRange.upperEndpoint(), limit);
            }
            long lower = min(limit, sourceCardinalityRange.lowerEndpoint());
            return Range.closed(lower, limit);
        }
    }
}
