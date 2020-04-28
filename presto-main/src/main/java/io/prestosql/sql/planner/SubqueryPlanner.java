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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.QuantifiedComparisonExpression.Quantifier;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.util.MorePredicates;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.util.AstUtils.nodeContains;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final Metadata metadata;
    private final Session session;

    SubqueryPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            Metadata metadata,
            Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.metadata = metadata;
        this.session = session;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node);
        }
        return builder;
    }

    public PlanBuilder handleUncorrelatedSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        builder = appendInPredicateApplyNodes(builder, collectInPredicateSubqueries(expression, node), node);
        builder = appendScalarSubqueryCorrelatedJoins(builder, collectScalarSubqueries(expression, node));
        builder = appendExistsSubqueryApplyNodes(builder, collectExistsSubqueries(expression, node));
        builder = appendQuantifiedComparisonApplyNodes(builder, collectQuantifiedComparisonSubqueries(expression, node), node);
        return builder;
    }

    public Set<InPredicate> collectInPredicateSubqueries(Expression expression, Node node)
    {
        return analysis.getInPredicateSubqueries(node)
                .stream()
                .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                .collect(toImmutableSet());
    }

    public Set<SubqueryExpression> collectScalarSubqueries(Expression expression, Node node)
    {
        return analysis.getScalarSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(toImmutableSet());
    }

    public Set<ExistsPredicate> collectExistsSubqueries(Expression expression, Node node)
    {
        return analysis.getExistsSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(toImmutableSet());
    }

    public Set<QuantifiedComparisonExpression> collectQuantifiedComparisonSubqueries(Expression expression, Node node)
    {
        return analysis.getQuantifiedComparisonSubqueries(node)
                .stream()
                .filter(quantifiedComparison -> nodeContains(expression, quantifiedComparison.getSubquery()))
                .collect(toImmutableSet());
    }

    private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan, Set<InPredicate> inPredicates, Node node)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendInPredicateApplyNode(subPlan, inPredicate, node);
        }
        return subPlan;
    }

    private PlanBuilder appendInPredicateApplyNode(PlanBuilder subPlan, InPredicate inPredicate, Node node)
    {
        if (subPlan.canTranslate(inPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        subPlan = handleSubqueries(subPlan, inPredicate.getValue(), node);

        subPlan = subPlan.appendProjections(ImmutableList.of(inPredicate.getValue()), symbolAllocator, idAllocator);

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        SubqueryExpression valueListSubquery = (SubqueryExpression) inPredicate.getValueList();
        SubqueryExpression uncoercedValueListSubquery = uncoercedSubquery(valueListSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedValueListSubquery, subPlan.getTranslations());

        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(valueListSubquery), symbolAllocator, idAllocator);
        SymbolReference valueList = subqueryPlan.translate(valueListSubquery).toSymbolReference();

        Symbol rewrittenValue = subPlan.translate(inPredicate.getValue());
        InPredicate inPredicateSubqueryExpression = new InPredicate(rewrittenValue.toSymbolReference(), valueList);
        Symbol inPredicateSubquerySymbol = symbolAllocator.newSymbol(inPredicateSubqueryExpression, BOOLEAN);

        subPlan.getTranslations().put(inPredicate, inPredicateSubquerySymbol);

        return appendApplyNode(subPlan, inPredicate, subqueryPlan.getRoot(), Assignments.of(inPredicateSubquerySymbol, inPredicateSubqueryExpression));
    }

    private PlanBuilder appendScalarSubqueryCorrelatedJoins(PlanBuilder builder, Set<SubqueryExpression> scalarSubqueries)
    {
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        List<Expression> coercions = coercionsFor(scalarSubquery);

        SubqueryExpression uncoercedScalarSubquery = uncoercedSubquery(scalarSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedScalarSubquery, subPlan.getTranslations());
        subqueryPlan = subqueryPlan.withNewRoot(new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan.getRoot()));
        subqueryPlan = subqueryPlan.appendProjections(coercions, symbolAllocator, idAllocator);

        Symbol uncoercedScalarSubquerySymbol = subqueryPlan.translate(uncoercedScalarSubquery);
        subPlan.getTranslations().put(uncoercedScalarSubquery, uncoercedScalarSubquerySymbol);

        for (Expression coercion : coercions) {
            Symbol coercionSymbol = subqueryPlan.translate(coercion);
            subPlan.getTranslations().put(coercion, coercionSymbol);
        }

        // The subquery's EnforceSingleRowNode always produces a row, so the join is effectively INNER
        return appendCorrelatedJoin(subPlan, subqueryPlan, scalarSubquery.getQuery(), CorrelatedJoinNode.Type.INNER, TRUE_LITERAL);
    }

    public PlanBuilder appendCorrelatedJoin(PlanBuilder subPlan, PlanBuilder subqueryPlan, Query query, CorrelatedJoinNode.Type type, Expression filterCondition)
    {
        PlanNode subqueryNode = subqueryPlan.getRoot();
        return new PlanBuilder(
                subPlan.copyTranslations(),
                new CorrelatedJoinNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subqueryNode,
                        subPlan.getRoot().getOutputSymbols(),
                        type,
                        filterCondition,
                        query));
    }

    private PlanBuilder appendExistsSubqueryApplyNodes(PlanBuilder builder, Set<ExistsPredicate> existsPredicates)
    {
        for (ExistsPredicate existsPredicate : existsPredicates) {
            builder = appendExistSubqueryApplyNode(builder, existsPredicate);
        }
        return builder;
    }

    /**
     * Exists is modeled as:
     * <pre>
     *     - Project($0 > 0)
     *       - Aggregation(COUNT(*))
     *         - Limit(1)
     *           -- subquery
     * </pre>
     */
    private PlanBuilder appendExistSubqueryApplyNode(PlanBuilder subPlan, ExistsPredicate existsPredicate)
    {
        if (subPlan.canTranslate(existsPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        PlanBuilder subqueryPlan = createPlanBuilder(existsPredicate.getSubquery(), subPlan.getTranslations());

        PlanNode subqueryPlanRoot = subqueryPlan.getRoot();
        if (isAggregationWithEmptyGroupBy(subqueryPlanRoot)) {
            subPlan.getTranslations().put(existsPredicate, TRUE_LITERAL);
            return subPlan;
        }

        // add an explicit projection that removes all columns
        PlanNode subqueryNode = new ProjectNode(idAllocator.getNextId(), subqueryPlan.getRoot(), Assignments.of());

        Symbol exists = symbolAllocator.newSymbol("exists", BOOLEAN);
        subPlan.getTranslations().put(existsPredicate, exists);
        ExistsPredicate rewrittenExistsPredicate = new ExistsPredicate(TRUE_LITERAL);
        return appendApplyNode(
                subPlan,
                existsPredicate.getSubquery(),
                subqueryNode,
                Assignments.of(exists, rewrittenExistsPredicate));
    }

    private PlanBuilder appendQuantifiedComparisonApplyNodes(PlanBuilder subPlan, Set<QuantifiedComparisonExpression> quantifiedComparisons, Node node)
    {
        for (QuantifiedComparisonExpression quantifiedComparison : quantifiedComparisons) {
            subPlan = appendQuantifiedComparisonApplyNode(subPlan, quantifiedComparison, node);
        }
        return subPlan;
    }

    private PlanBuilder appendQuantifiedComparisonApplyNode(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, Node node)
    {
        if (subPlan.canTranslate(quantifiedComparison)) {
            // given subquery is already appended
            return subPlan;
        }
        switch (quantifiedComparison.getOperator()) {
            case EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        return planQuantifiedApplyNode(subPlan, quantifiedComparison);
                    case ANY:
                    case SOME:
                        // A = ANY B <=> A IN B
                        InPredicate inPredicate = new InPredicate(quantifiedComparison.getValue(), quantifiedComparison.getSubquery());
                        subPlan = appendInPredicateApplyNode(subPlan, inPredicate, node);
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.translate(inPredicate));
                        return subPlan;
                }
                break;

            case NOT_EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        // A <> ALL B <=> !(A IN B) <=> !(A = ANY B)
                        QuantifiedComparisonExpression rewrittenAny = new QuantifiedComparisonExpression(
                                EQUAL,
                                Quantifier.ANY,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAny = new NotExpression(rewrittenAny);
                        // "A <> ALL B" is equivalent to "NOT (A = ANY B)" so add a rewrite for the initial quantifiedComparison to notAny
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAny));
                        // now plan "A = ANY B" part by calling ourselves for rewrittenAny
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAny, node);
                    case ANY:
                    case SOME:
                        // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
                        QuantifiedComparisonExpression rewrittenAll = new QuantifiedComparisonExpression(
                                EQUAL,
                                QuantifiedComparisonExpression.Quantifier.ALL,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAll = new NotExpression(rewrittenAll);
                        // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAll));
                        // now plan "A = ALL B" part by calling ourselves for rewrittenAll
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAll, node);
                }
                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return planQuantifiedApplyNode(subPlan, quantifiedComparison);
        }
        // all cases are checked, so this exception should never be thrown
        throw new IllegalArgumentException(
                format("Unexpected quantified comparison: '%s %s'", quantifiedComparison.getOperator().getValue(), quantifiedComparison.getQuantifier()));
    }

    private PlanBuilder planQuantifiedApplyNode(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison)
    {
        subPlan = subPlan.appendProjections(ImmutableList.of(quantifiedComparison.getValue()), symbolAllocator, idAllocator);

        checkState(quantifiedComparison.getSubquery() instanceof SubqueryExpression);
        SubqueryExpression quantifiedSubquery = (SubqueryExpression) quantifiedComparison.getSubquery();

        SubqueryExpression uncoercedQuantifiedSubquery = uncoercedSubquery(quantifiedSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedQuantifiedSubquery, subPlan.getTranslations());
        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(quantifiedSubquery), symbolAllocator, idAllocator);

        QuantifiedComparisonExpression coercedQuantifiedComparison = new QuantifiedComparisonExpression(
                quantifiedComparison.getOperator(),
                quantifiedComparison.getQuantifier(),
                subPlan.translate(quantifiedComparison.getValue()).toSymbolReference(),
                subqueryPlan.translate(quantifiedSubquery).toSymbolReference());

        Symbol coercedQuantifiedComparisonSymbol = symbolAllocator.newSymbol(coercedQuantifiedComparison, BOOLEAN);
        subPlan.getTranslations().put(quantifiedComparison, coercedQuantifiedComparisonSymbol);

        return appendApplyNode(
                subPlan,
                quantifiedComparison.getSubquery(),
                subqueryPlan.getRoot(),
                Assignments.of(coercedQuantifiedComparisonSymbol, coercedQuantifiedComparison));
    }

    private static boolean isAggregationWithEmptyGroupBy(PlanNode planNode)
    {
        return searchFrom(planNode)
                .recurseOnlyWhen(MorePredicates.isInstanceOfAny(ProjectNode.class))
                .where(AggregationNode.class::isInstance)
                .findFirst()
                .map(AggregationNode.class::cast)
                .map(aggregation -> aggregation.getGroupingKeys().isEmpty())
                .orElse(false);
    }

    /**
     * Implicit coercions are added when mapping an expression to symbol in {@link TranslationMap}. Coercions
     * for expression are obtained from {@link Analysis} by identity comparison. Create a copy of subquery
     * in order to get a subquery expression that does not have any coercion assigned to it {@link Analysis}.
     */
    private SubqueryExpression uncoercedSubquery(SubqueryExpression subquery)
    {
        return new SubqueryExpression(subquery.getQuery());
    }

    private List<Expression> coercionsFor(Expression expression)
    {
        return analysis.getCoercions().keySet().stream()
                .map(NodeRef::getNode)
                .filter(coercionExpression -> coercionExpression.equals(expression))
                .collect(toImmutableList());
    }

    private PlanBuilder appendApplyNode(
            PlanBuilder subPlan,
            Node subquery,
            PlanNode subqueryNode,
            Assignments subqueryAssignments)
    {
        PlanNode root = subPlan.getRoot();
        return new PlanBuilder(
                subPlan.copyTranslations(),
                new ApplyNode(idAllocator.getNextId(),
                        root,
                        subqueryNode,
                        subqueryAssignments,
                        root.getOutputSymbols(),
                        subquery));
    }

    private PlanBuilder createPlanBuilder(Node node, TranslationMap outerContext)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session, Optional.of(outerContext))
                .process(node, null);
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToSymbolMap);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        if (node instanceof Expression && relationPlan.getFieldMappings().size() == 1) {
            translations.put((Expression) node, getOnlyElement(relationPlan.getFieldMappings()));
        }

        return new PlanBuilder(translations, relationPlan.getRoot());
    }
}
