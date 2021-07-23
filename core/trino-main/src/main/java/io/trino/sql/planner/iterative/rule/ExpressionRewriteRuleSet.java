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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.SortItem.NullOrdering;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.values;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static java.util.Objects.requireNonNull;

public class ExpressionRewriteRuleSet
{
    public interface ExpressionRewriter
    {
        Expression rewrite(Expression expression, Rule.Context context);
    }

    private final ExpressionRewriter rewriter;

    public ExpressionRewriteRuleSet(ExpressionRewriter rewriter)
    {
        this.rewriter = requireNonNull(rewriter, "rewriter is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                aggregationExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite());
    }

    public Rule<?> projectExpressionRewrite()
    {
        return new ProjectExpressionRewrite(rewriter);
    }

    public Rule<?> aggregationExpressionRewrite()
    {
        return new AggregationExpressionRewrite(rewriter);
    }

    public Rule<?> filterExpressionRewrite()
    {
        return new FilterExpressionRewrite(rewriter);
    }

    public Rule<?> joinExpressionRewrite()
    {
        return new JoinExpressionRewrite(rewriter);
    }

    public Rule<?> valuesExpressionRewrite()
    {
        return new ValuesExpressionRewrite(rewriter);
    }

    private static final class ProjectExpressionRewrite
            implements Rule<ProjectNode>
    {
        private final ExpressionRewriter rewriter;

        ProjectExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            Assignments assignments = projectNode.getAssignments().rewrite(x -> rewriter.rewrite(x, context));
            if (projectNode.getAssignments().equals(assignments)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ProjectNode(projectNode.getId(), projectNode.getSource(), assignments));
        }
    }

    private static final class AggregationExpressionRewrite
            implements Rule<AggregationNode>
    {
        private final ExpressionRewriter rewriter;

        AggregationExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return aggregation();
        }

        @Override
        public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
                Aggregation aggregation = entry.getValue();
                FunctionCall call = (FunctionCall) rewriter.rewrite(
                        new FunctionCall(
                                Optional.empty(),
                                QualifiedName.of(aggregation.getResolvedFunction().getSignature().getName()),
                                Optional.empty(),
                                aggregation.getFilter().map(symbol -> new SymbolReference(symbol.getName())),
                                aggregation.getOrderingScheme().map(orderBy -> new OrderBy(orderBy.getOrderBy().stream()
                                        .map(symbol -> new SortItem(
                                                new SymbolReference(symbol.getName()),
                                                orderBy.getOrdering(symbol).isAscending() ? ASCENDING : DESCENDING,
                                                orderBy.getOrdering(symbol).isNullsFirst() ? NullOrdering.FIRST : NullOrdering.LAST))
                                        .collect(toImmutableList()))),
                                aggregation.isDistinct(),
                                Optional.empty(),
                                Optional.empty(),
                                aggregation.getArguments()),
                        context);
                verify(
                        QualifiedName.of(extractFunctionName(call.getName())).equals(QualifiedName.of(aggregation.getResolvedFunction().getSignature().getName())),
                        "Aggregation function name changed");
                Aggregation newAggregation = new Aggregation(
                        aggregation.getResolvedFunction(),
                        call.getArguments(),
                        call.isDistinct(),
                        call.getFilter().map(Symbol::from),
                        call.getOrderBy().map(OrderingScheme::fromOrderBy),
                        aggregation.getMask());
                aggregations.put(entry.getKey(), newAggregation);
                if (!aggregation.equals(newAggregation)) {
                    anyRewritten = true;
                }
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new AggregationNode(
                        aggregationNode.getId(),
                        aggregationNode.getSource(),
                        aggregations.build(),
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getPreGroupedSymbols(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashSymbol(),
                        aggregationNode.getGroupIdSymbol()));
            }
            return Result.empty();
        }
    }

    private static final class FilterExpressionRewrite
            implements Rule<FilterNode>
    {
        private final ExpressionRewriter rewriter;

        FilterExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            Expression rewritten = rewriter.rewrite(filterNode.getPredicate(), context);
            if (filterNode.getPredicate().equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private static final class JoinExpressionRewrite
            implements Rule<JoinNode>
    {
        private final ExpressionRewriter rewriter;

        JoinExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return join();
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            Optional<Expression> filter = joinNode.getFilter().map(x -> rewriter.rewrite(x, context));
            if (!joinNode.getFilter().equals(filter)) {
                return Result.ofPlanNode(new JoinNode(
                        joinNode.getId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        joinNode.getLeftOutputSymbols(),
                        joinNode.getRightOutputSymbols(),
                        joinNode.isMaySkipOutputDuplicates(),
                        filter,
                        joinNode.getLeftHashSymbol(),
                        joinNode.getRightHashSymbol(),
                        joinNode.getDistributionType(),
                        joinNode.isSpillable(),
                        joinNode.getDynamicFilters(),
                        joinNode.getReorderJoinStatsAndCost()));
            }
            return Result.empty();
        }
    }

    private static final class ValuesExpressionRewrite
            implements Rule<ValuesNode>
    {
        private final ExpressionRewriter rewriter;

        ValuesExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Result apply(ValuesNode valuesNode, Captures captures, Context context)
        {
            if (valuesNode.getRows().isEmpty()) {
                return Result.empty();
            }

            boolean anyRewritten = false;
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            for (Expression row : valuesNode.getRows().get()) {
                Expression rewritten;
                if (row instanceof Row) {
                    // preserve the structure of row
                    rewritten = new Row(((Row) row).getItems().stream()
                            .map(item -> rewriter.rewrite(item, context))
                            .collect(toImmutableList()));
                }
                else {
                    rewritten = rewriter.rewrite(row, context);
                }
                if (!row.equals(rewritten)) {
                    anyRewritten = true;
                }
                rows.add(rewritten);
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getOutputSymbols(), rows.build()));
            }
            return Result.empty();
        }
    }
}
