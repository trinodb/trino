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
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.unwrapCasts;
import static io.trino.sql.planner.plan.Patterns.project;

public final class PushFilterThroughProject
        extends FilterPushdownRule<ProjectNode>
{
    public PushFilterThroughProject(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        super(plannerContext, useTableProperties, dynamicFiltering, project());
    }

    @Override
    protected PlanNode pushDown(Pushdown pushdown, ProjectNode node, Expression inheritedPredicate)
    {
        Set<Symbol> deterministicSymbols = node.getAssignments().entrySet().stream()
                .filter(entry -> isDeterministic(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        Predicate<Expression> deterministic = conjunct -> deterministicSymbols.containsAll(extractUnique(conjunct));

        Map<Boolean, List<Expression>> conjuncts = extractConjuncts(inheritedPredicate).stream().collect(Collectors.partitioningBy(deterministic));

        // Push down conjuncts from the inherited predicate that only depend on deterministic assignments with
        // certain limitations.
        List<Expression> deterministicConjuncts = conjuncts.get(true);

        // We partition the expressions in the deterministicConjuncts into two lists, and only inline the
        // expressions that are in the inlining targets list.
        Map<Boolean, List<Expression>> inlineConjuncts = deterministicConjuncts.stream()
                .collect(Collectors.partitioningBy(expression -> isInliningCandidate(expression, node)));

        List<Expression> inlinedDeterministicConjuncts = inlineConjuncts.get(true).stream()
                .map(conjunct -> inlinePredicate(pushdown.session, pushdown.plannerContext, pushdown.symbolAllocator, node, conjunct))
                .collect(Collectors.toList());

        PlanNode rewrittenNode = pushdown.filterChildren(node, combineConjuncts(inlinedDeterministicConjuncts));

        // All deterministic conjuncts that contains non-inlining targets, and non-deterministic conjuncts,
        // if any, will be in the filter node.
        List<Expression> nonInliningConjuncts = inlineConjuncts.get(false);
        nonInliningConjuncts.addAll(conjuncts.get(false));

        if (!nonInliningConjuncts.isEmpty()) {
            rewrittenNode = new FilterNode(pushdown.idAllocator.getNextId(), rewrittenNode, combineConjuncts(nonInliningConjuncts));
        }

        return rewrittenNode;
    }

    static Expression inlinePredicate(Session session, PlannerContext plannerContext, SymbolAllocator symbolAllocator, ProjectNode node, Expression predicate)
    {
        Expression inlined = inlineSymbols(node.getAssignments().assignments(), predicate);
        return unwrapCasts(
                session,
                plannerContext,
                symbolAllocator,
                canonicalizeExpression(inlined, plannerContext, getCharVarcharCoercion(session)));
    }

    static boolean isInliningCandidate(Expression expression, ProjectNode node)
    {
        // candidate symbols for inlining are
        //   1. references to simple constants or symbol references
        //   2. references to complex expressions that appear only once
        // which come from the node, as opposed to an enclosing scope.
        Set<Symbol> childOutputSet = ImmutableSet.copyOf(node.getOutputSymbols());
        Map<Symbol, Long> dependencies = SymbolsExtractor.extractAll(expression).stream()
                .filter(childOutputSet::contains)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        return dependencies.entrySet().stream()
                .allMatch(entry -> entry.getValue() == 1
                        || node.getAssignments().get(entry.getKey()) instanceof Constant
                        || node.getAssignments().get(entry.getKey()) instanceof Reference);
    }
}
