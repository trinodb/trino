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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.function.Consumer;

import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static java.util.Objects.requireNonNull;

public final class ExpressionExtractor
{
    public static List<Expression> extractExpressions(PlanNode plan)
    {
        return extractExpressions(plan, noLookup());
    }

    public static List<Expression> extractExpressions(PlanNode plan, Lookup lookup)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(lookup, "lookup is null");

        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
        plan.accept(new Visitor(expressionsBuilder::add, true, lookup), null);
        return expressionsBuilder.build();
    }

    public static List<Expression> extractExpressionsNonRecursive(PlanNode plan)
    {
        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
        plan.accept(new Visitor(expressionsBuilder::add, false, noLookup()), null);
        return expressionsBuilder.build();
    }

    public static void forEachExpression(PlanNode plan, Consumer<Expression> expressionConsumer)
    {
        plan.accept(new Visitor(expressionConsumer, true, noLookup()), null);
    }

    private ExpressionExtractor()
    {
    }

    private static class Visitor
            extends SimplePlanVisitor<Void>
    {
        private final Consumer<Expression> consumer;
        private final boolean recursive;
        private final Lookup lookup;

        Visitor(Consumer<Expression> consumer, boolean recursive, Lookup lookup)
        {
            this.consumer = requireNonNull(consumer, "consumer is null");
            this.recursive = recursive;
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            if (recursive) {
                return super.visitPlan(node, context);
            }
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            for (Aggregation aggregation : node.getAggregations().values()) {
                aggregation.getArguments().forEach(consumer);
            }
            return super.visitAggregation(node, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            consumer.accept(node.getPredicate());
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            node.getAssignments().getExpressions().forEach(consumer);
            return super.visitProject(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.getFilter().ifPresent(consumer);
            return super.visitJoin(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            node.getRows().ifPresent(list -> list.forEach(consumer));
            return super.visitValues(node, context);
        }
    }
}
