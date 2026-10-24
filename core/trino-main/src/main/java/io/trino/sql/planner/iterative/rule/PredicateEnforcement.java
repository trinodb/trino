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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.planner.iterative.rule.PushFilterThroughProject.inlinePredicate;
import static io.trino.sql.planner.iterative.rule.PushFilterThroughProject.isInliningCandidate;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static java.util.Objects.requireNonNull;

/// Proves that an input already enforces a predicate, even when effective-predicate
/// extraction loses its form through projections or connector pushdown. This is
/// read-only implication analysis; structural matching and rewriting belong to rules.
final class PredicateEnforcement
        extends PlanVisitor<Boolean, Expression>
{
    private final PlannerContext plannerContext;
    private final Session session;
    private final SymbolAllocator symbolAllocator;
    private final Lookup lookup;

    PredicateEnforcement(PlannerContext plannerContext, Session session, SymbolAllocator symbolAllocator, Lookup lookup)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.session = requireNonNull(session, "session is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    boolean isEnforcedBy(PlanNode source, Expression predicate)
    {
        if (predicate.equals(TRUE)) {
            return true;
        }
        return isDeterministic(predicate) && source.accept(this, predicate);
    }

    @Override
    protected Boolean visitPlan(PlanNode node, Expression predicate)
    {
        return false;
    }

    @Override
    public Boolean visitGroupReference(GroupReference node, Expression predicate)
    {
        // The proof can cross an arbitrary number of input mappings. Equivalent
        // alternatives need only supply one proof; do not assume a single member.
        return lookup.resolveGroup(node).anyMatch(alternative -> isEnforcedBy(alternative, predicate));
    }

    @Override
    public Boolean visitFilter(FilterNode node, Expression predicate)
    {
        return isImpliedBy(predicate, node.getPredicate()) || isEnforcedBy(node.getSource(), predicate);
    }

    @Override
    public Boolean visitTableScan(TableScanNode scan, Expression predicate)
    {
        if (!scan.getOutputSymbols().containsAll(extractUnique(predicate))) {
            return false;
        }
        // Effective-predicate extraction can simplify a large discrete domain to a
        // range. Use the exact enforced constraint before reinserting a scan filter.
        DomainTranslator.ExtractionResult domain = DomainTranslator.getExtractionResult(plannerContext, session, predicate);
        return domain.remainingExpression().equals(TRUE) &&
                domain.tupleDomain().transformKeys(scan.getAssignments()::get).contains(scan.getEnforcedConstraint());
    }

    @Override
    public Boolean visitValues(ValuesNode values, Expression predicate)
    {
        // PushFilterIntoValues can consume a predicate that domain inference cannot
        // prove (for example, length(x) < 42). Do not keep reinserting that predicate.
        if (values.getRows().isEmpty() || values.getRowCount() > SystemSessionProperties.getPushFilterIntoValuesMaxRowCount(session) ||
                !values.getOutputSymbols().containsAll(extractUnique(predicate))) {
            return false;
        }
        for (Expression row : values.getRows().orElseThrow()) {
            if (!(row instanceof Row || row instanceof Constant) || !isDeterministic(row) || !extractUnique(row).isEmpty()) {
                return false;
            }
            Map<Symbol, Expression> mapping = new HashMap<>();
            for (int index = 0; index < values.getOutputSymbols().size(); index++) {
                mapping.put(values.getOutputSymbols().get(index), new FieldReference(row, index));
            }
            if (!simplifyExpression(inlineSymbols(mapping, predicate)).equals(TRUE)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitProject(ProjectNode project, Expression predicate)
    {
        if (!isInliningCandidate(predicate, project) || extractUnique(predicate).stream()
                .anyMatch(symbol -> project.getAssignments().get(symbol) != null && !isDeterministic(project.getAssignments().get(symbol)))) {
            return false;
        }
        // Substitution can make the predicate TRUE, leaving no filter to recognize below.
        Expression inlined = simplifyExpression(inlinePredicate(session, plannerContext, symbolAllocator, project, predicate));
        return extractConjuncts(inlined).stream().allMatch(conjunct -> isEnforcedBy(project.getSource(), conjunct));
    }

    @Override
    public Boolean visitUnion(UnionNode union, Expression predicate)
    {
        for (int index = 0; index < union.getSources().size(); index++) {
            if (!isEnforcedBy(union.getSources().get(index), inlineSymbols(union.sourceSymbolMap(index), predicate))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitExchange(ExchangeNode exchange, Expression predicate)
    {
        for (int index = 0; index < exchange.getSources().size(); index++) {
            Map<Symbol, Reference> mapping = new HashMap<>();
            for (int symbol = 0; symbol < exchange.getOutputSymbols().size(); symbol++) {
                mapping.put(exchange.getOutputSymbols().get(symbol), exchange.getInputs().get(index).get(symbol).toSymbolReference());
            }
            if (!isEnforcedBy(exchange.getSources().get(index), inlineSymbols(mapping, predicate))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitGroupId(GroupIdNode groupId, Expression predicate)
    {
        Map<Symbol, Reference> mapping = groupId.getGroupingColumns().entrySet().stream()
                .filter(entry -> groupId.getCommonGroupingColumns().contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));
        return mapping.keySet().containsAll(extractUnique(predicate)) &&
                isEnforcedBy(groupId.getSource(), inlineSymbols(mapping, predicate));
    }

    @Override
    public Boolean visitJoin(JoinNode join, Expression predicate)
    {
        // Only inner-join output rows necessarily satisfy the join filter.
        if (join.getType() == INNER && join.getFilter().isPresent() && isImpliedBy(predicate, join.getFilter().get())) {
            return true;
        }
        Set<Symbol> symbols = extractUnique(predicate);
        // Null extension can invalidate a predicate enforced by an input filter.
        return ((join.getType() == INNER || join.getType() == LEFT) &&
                join.getLeft().getOutputSymbols().containsAll(symbols) && isEnforcedBy(join.getLeft(), predicate)) ||
                ((join.getType() == INNER || join.getType() == RIGHT) &&
                        join.getRight().getOutputSymbols().containsAll(symbols) && isEnforcedBy(join.getRight(), predicate));
    }

    @Override
    public Boolean visitSpatialJoin(SpatialJoinNode join, Expression predicate)
    {
        if (join.getType() == SpatialJoinNode.Type.INNER && isImpliedBy(predicate, join.getFilter())) {
            return true;
        }
        Set<Symbol> symbols = extractUnique(predicate);
        // Spatial joins preserve the left input, but LEFT joins null-extend the right input.
        return (join.getLeft().getOutputSymbols().containsAll(symbols) && isEnforcedBy(join.getLeft(), predicate)) ||
                (join.getType() == SpatialJoinNode.Type.INNER &&
                        join.getRight().getOutputSymbols().containsAll(symbols) && isEnforcedBy(join.getRight(), predicate));
    }

    @Override
    public Boolean visitSemiJoin(SemiJoinNode node, Expression predicate)
    {
        return node.getSource().getOutputSymbols().containsAll(extractUnique(predicate)) && isEnforcedBy(node.getSource(), predicate);
    }

    @Override
    public Boolean visitSort(SortNode node, Expression predicate)
    {
        return isEnforcedBySource(node, predicate);
    }

    @Override
    public Boolean visitSample(SampleNode node, Expression predicate)
    {
        return isEnforcedBySource(node, predicate);
    }

    @Override
    public Boolean visitAssignUniqueId(AssignUniqueId node, Expression predicate)
    {
        return isEnforcedBySource(node, predicate);
    }

    @Override
    public Boolean visitWindow(WindowNode node, Expression predicate)
    {
        return isEnforcedBySource(node, predicate);
    }

    @Override
    public Boolean visitTopNRanking(TopNRankingNode node, Expression predicate)
    {
        return isEnforcedBySource(node, predicate);
    }

    @Override
    public Boolean visitMarkDistinct(MarkDistinctNode node, Expression predicate)
    {
        return node.getDistinctSymbols().containsAll(extractUnique(predicate)) && isEnforcedBySource(node, predicate);
    }

    @Override
    public Boolean visitAggregation(AggregationNode node, Expression predicate)
    {
        return !node.hasEmptyGroupingSet() && node.getGroupingKeys().containsAll(extractUnique(predicate)) && isEnforcedBySource(node, predicate);
    }

    @Override
    public Boolean visitUnnest(UnnestNode node, Expression predicate)
    {
        return (node.getJoinType() == INNER || node.getJoinType() == LEFT) && node.getReplicateSymbols().containsAll(extractUnique(predicate)) && isEnforcedBySource(node, predicate);
    }

    private boolean isEnforcedBySource(PlanNode node, Expression predicate)
    {
        PlanNode source = node.getSources().getFirst();
        return source.getOutputSymbols().containsAll(extractUnique(predicate)) && isEnforcedBy(source, predicate);
    }

    boolean isImpliedBy(Expression predicate, Expression effectivePredicate)
    {
        predicate = simplifyExpression(predicate);
        effectivePredicate = simplifyExpression(filterDeterministicConjuncts(effectivePredicate));
        if (predicate.equals(TRUE) || effectivePredicate.equals(FALSE) ||
                ImmutableSet.copyOf(extractConjuncts(effectivePredicate)).containsAll(extractConjuncts(predicate))) {
            return true;
        }
        // Domain comparison uses SQL value semantics, including for independently
        // allocated array and row constants that are not equal as IR expressions.
        DomainTranslator.ExtractionResult domain = DomainTranslator.getExtractionResult(plannerContext, session, predicate);
        return domain.remainingExpression().equals(TRUE) && domain.tupleDomain().contains(
                DomainTranslator.getExtractionResult(plannerContext, session, effectivePredicate).tupleDomain());
    }

    Expression simplifyExpression(Expression expression)
    {
        Expression simplified = plannerContext.getExpressionOptimizer().process(expression, session, symbolAllocator, ImmutableMap.of()).orElse(expression);
        return canonicalizeExpression(simplified, plannerContext, getCharVarcharCoercion(session));
    }
}
