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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.Rule.Context;
import static io.trino.sql.planner.iterative.Rule.Result;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;

/**
 * Rule that transform plan like
 * <pre> {@code
 *  filter: expr(l) > expr(r)
 *      join
 *          probe(l)
 *          build(r)
 *       }
 * </pre>
 * to
 * <pre> {@code
 *  filter: expr(l) > expr_r
 *      join
 *          probe(l)
 *          project (expr_r := expr(r))
 *              build (r)
 *       }
 * </pre>
 * This rule allows dynamic filtering to be applied for inequality join node.
 * Additionally, optimized execution of inequality join is performed when equi conditions are also present.
 */
public class PushInequalityFilterExpressionBelowJoinRuleSet
{
    private static final Set<ComparisonExpression.Operator> SUPPORTED_COMPARISONS = ImmutableSet.of(
            GREATER_THAN,
            GREATER_THAN_OR_EQUAL,
            LESS_THAN,
            LESS_THAN_OR_EQUAL);
    private static final Pattern<JoinNode> JOIN_PATTERN = join();
    private static final Capture<JoinNode> JOIN_CAPTURE = newCapture();
    private static final Pattern<FilterNode> FILTER_PATTERN = filter().with(source().matching(
            join().capturedAs(JOIN_CAPTURE)));

    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;

    public PushInequalityFilterExpressionBelowJoinRuleSet(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    public Iterable<Rule<?>> rules()
    {
        return ImmutableList.of(
                pushParentInequalityFilterExpressionBelowJoinRule(),
                pushJoinInequalityFilterExpressionBelowJoinRule());
    }

    public Rule<FilterNode> pushParentInequalityFilterExpressionBelowJoinRule()
    {
        return new PushFilterExpressionBelowJoinFilterRule();
    }

    public Rule<JoinNode> pushJoinInequalityFilterExpressionBelowJoinRule()
    {
        return new PushFilterExpressionBelowJoinJoinRule();
    }

    private Result pushInequalityFilterExpressionBelowJoin(Context context, JoinNode joinNode, Optional<FilterNode> filterNode)
    {
        JoinNodeContext joinNodeContext = new JoinNodeContext(joinNode);

        Expression parentFilterPredicate = filterNode.map(FilterNode::getPredicate).orElse(TRUE_LITERAL);
        Map<Boolean, List<Expression>> parentFilterCandidates;
        if (joinNode.getType() == INNER) {
            parentFilterCandidates = extractPushDownCandidates(joinNodeContext, parentFilterPredicate);
        }
        else {
            // Do not push parent filter predicate for outer joins. Pushing it below join changes
            // filter semantics because such filter depends on null output from outer join side
            // (otherwise outer join would be converted to inner join by predicate push down).
            parentFilterCandidates = ImmutableMap.of(
                    true, ImmutableList.of(),
                    false, extractConjuncts(parentFilterPredicate));
        }

        Map<Boolean, List<Expression>> joinFilterCandidates = extractPushDownCandidates(joinNodeContext, joinNode.getFilter().orElse(TRUE_LITERAL));

        if (parentFilterCandidates.get(true).isEmpty() && joinFilterCandidates.get(true).isEmpty()) {
            // no push-down candidates
            return Result.empty();
        }

        ImmutableList.Builder<Expression> newParentFilterConjuncts = ImmutableList.<Expression>builder().addAll(parentFilterCandidates.get(false));
        Map<Symbol, Expression> newRightProjectionsForParentFilter = pushDownRightComplexExpressions(joinNodeContext, context, parentFilterCandidates.get(true), newParentFilterConjuncts);

        ImmutableList.Builder<Expression> newJoinFilterConjuncts = ImmutableList.<Expression>builder().addAll(joinFilterCandidates.get(false));
        Map<Symbol, Expression> newRightProjectionsForJoinFilter = pushDownRightComplexExpressions(joinNodeContext, context, joinFilterCandidates.get(true), newJoinFilterConjuncts);

        PlanNode newOutput = constructModifiedJoin(
                context,
                joinNode,
                conjunctsToFilter(newJoinFilterConjuncts.build()),
                ImmutableMap.<Symbol, Expression>builder()
                        .putAll(newRightProjectionsForJoinFilter)
                        .putAll(newRightProjectionsForParentFilter)
                        .buildOrThrow(),
                newRightProjectionsForParentFilter.keySet());

        Optional<Expression> filter = conjunctsToFilter(newParentFilterConjuncts.build());
        if (filter.isPresent()) {
            newOutput = new FilterNode(filterNode.get().getId(), newOutput, filter.get());
        }

        if (!joinNode.getOutputSymbols().equals(newOutput.getOutputSymbols())) {
            newOutput = new ProjectNode(context.getIdAllocator().getNextId(), newOutput, Assignments.identity(joinNode.getOutputSymbols()));
        }

        return Result.ofPlanNode(newOutput);
    }

    private Optional<Expression> conjunctsToFilter(List<Expression> conjuncts)
    {
        return Optional.of(combineConjuncts(metadata, conjuncts)).filter(expression -> !TRUE_LITERAL.equals(expression));
    }

    Map<Boolean, List<Expression>> extractPushDownCandidates(JoinNodeContext joinNodeContext, Expression filter)
    {
        return extractConjuncts(filter).stream()
                .collect(partitioningBy(conjunct -> isSupportedExpression(joinNodeContext, conjunct)));
    }

    private boolean isSupportedExpression(JoinNodeContext joinNodeContext, Expression expression)
    {
        if (!(expression instanceof ComparisonExpression comparison && isDeterministic(expression, metadata))) {
            return false;
        }
        if (!SUPPORTED_COMPARISONS.contains(comparison.getOperator())) {
            return false;
        }
        Set<Symbol> leftComparisonSymbols = extractUnique(comparison.getLeft());
        Set<Symbol> rightComparisonSymbols = extractUnique(comparison.getRight());
        if (leftComparisonSymbols.isEmpty() || rightComparisonSymbols.isEmpty()) {
            return false;
        }
        Set<Symbol> leftSymbols = joinNodeContext.getLeftSymbols();
        Set<Symbol> rightSymbols = joinNodeContext.getRightSymbols();
        if (!(leftSymbols.containsAll(leftComparisonSymbols) && rightSymbols.containsAll(rightComparisonSymbols) ||
                (rightSymbols.containsAll(leftComparisonSymbols) && leftSymbols.containsAll(rightComparisonSymbols)))) {
            return false;
        }

        boolean alignedComparison = joinNodeContext.isComparisonAligned(comparison);
        Expression buildExpression = alignedComparison ? comparison.getRight() : comparison.getLeft();

        // if buildExpression is a symbol, and it is available, we don't need to push down anything
        return !(buildExpression instanceof SymbolReference);
    }

    Map<Symbol, Expression> pushDownRightComplexExpressions(
            JoinNodeContext joinNodeContext,
            Context context,
            List<Expression> conjuncts,
            ImmutableList.Builder<Expression> newConjuncts)
    {
        ImmutableMap.Builder<Symbol, Expression> newProjections = ImmutableMap.builder();
        conjuncts.forEach(conjunct -> pushDownRightComplexExpression(joinNodeContext, context, newConjuncts, newProjections, conjunct));
        return newProjections.buildOrThrow();
    }

    private void pushDownRightComplexExpression(
            JoinNodeContext joinNodeContext,
            Context context,
            ImmutableList.Builder<Expression> newConjuncts,
            ImmutableMap.Builder<Symbol, Expression> newProjections,
            Expression conjunct)
    {
        checkArgument(conjunct instanceof ComparisonExpression, "conjunct '%s' is not a comparison", conjunct);
        ComparisonExpression comparison = (ComparisonExpression) conjunct;
        boolean alignedComparison = joinNodeContext.isComparisonAligned(comparison);
        Expression rightExpression = alignedComparison ? comparison.getRight() : comparison.getLeft();
        Expression leftExpression = alignedComparison ? comparison.getLeft() : comparison.getRight();
        Symbol rightSymbol = symbolForExpression(context, rightExpression);
        newConjuncts.add(new ComparisonExpression(
                comparison.getOperator(),
                alignedComparison ? leftExpression : rightSymbol.toSymbolReference(),
                alignedComparison ? rightSymbol.toSymbolReference() : leftExpression));
        newProjections.put(rightSymbol, rightExpression);
    }

    private JoinNode constructModifiedJoin(
            Context context,
            JoinNode originalJoinNode,
            Optional<Expression> newJoinFilter,
            Map<Symbol, Expression> newRightProjections,
            Set<Symbol> newJoinRightOutputSymbols)
    {
        PlanNode rightSource;
        if (newRightProjections.isEmpty()) {
            rightSource = originalJoinNode.getRight();
        }
        else {
            rightSource = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    originalJoinNode.getRight(),
                    buildAssignments(originalJoinNode.getRight(), newRightProjections));
        }

        return new JoinNode(
                originalJoinNode.getId(),
                originalJoinNode.getType(),
                originalJoinNode.getLeft(),
                rightSource,
                originalJoinNode.getCriteria(),
                originalJoinNode.getLeftOutputSymbols(),
                concatToList(originalJoinNode.getRightOutputSymbols(), newJoinRightOutputSymbols),
                originalJoinNode.isMaySkipOutputDuplicates(),
                newJoinFilter,
                originalJoinNode.getLeftHashSymbol(),
                originalJoinNode.getRightHashSymbol(),
                originalJoinNode.getDistributionType(),
                originalJoinNode.isSpillable(),
                originalJoinNode.getDynamicFilters(),
                originalJoinNode.getReorderJoinStatsAndCost());
    }

    private <T> List<T> concatToList(Iterable<T> left, Iterable<T> right)
    {
        return ImmutableList.<T>builder()
                .addAll(left)
                .addAll(right)
                .build();
    }

    private Assignments buildAssignments(PlanNode source, Map<Symbol, Expression> newRightProjections)
    {
        return Assignments.builder()
                .putIdentities(source.getOutputSymbols())
                .putAll(newRightProjections)
                .build();
    }

    private Symbol symbolForExpression(Context context, Expression expression)
    {
        checkArgument(!(expression instanceof SymbolReference), "expression '%s' is a SymbolReference", expression);
        return context.getSymbolAllocator().newSymbol(expression, typeAnalyzer.getType(context.getSession(), context.getSymbolAllocator().getTypes(), expression));
    }

    private class PushFilterExpressionBelowJoinFilterRule
            implements Rule<FilterNode>
    {
        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            return pushInequalityFilterExpressionBelowJoin(context, captures.get(JOIN_CAPTURE), Optional.of(filterNode));
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return FILTER_PATTERN;
        }
    }

    private class PushFilterExpressionBelowJoinJoinRule
            implements Rule<JoinNode>
    {
        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            return pushInequalityFilterExpressionBelowJoin(context, joinNode, Optional.empty());
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return JOIN_PATTERN;
        }
    }

    private static class JoinNodeContext
    {
        private final Set<Symbol> leftSymbols;
        private final Set<Symbol> rightSymbols;

        public JoinNodeContext(JoinNode joinNode)
        {
            requireNonNull(joinNode, "joinNode is null");
            this.leftSymbols = ImmutableSet.copyOf(joinNode.getLeft().getOutputSymbols());
            this.rightSymbols = ImmutableSet.copyOf(joinNode.getRight().getOutputSymbols());
        }

        public Set<Symbol> getLeftSymbols()
        {
            return leftSymbols;
        }

        public Set<Symbol> getRightSymbols()
        {
            return rightSymbols;
        }

        public boolean isComparisonAligned(ComparisonExpression comparison)
        {
            return leftSymbols.containsAll(extractUnique(comparison.getLeft()));
        }
    }
}
