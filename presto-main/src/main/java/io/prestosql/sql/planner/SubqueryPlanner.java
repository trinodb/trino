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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.ComparisonExpression;
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
import io.prestosql.type.TypeCoercion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.planner.PlanBuilder.newPlanBuilder;
import static io.prestosql.sql.planner.QueryPlanner.coerce;
import static io.prestosql.sql.planner.ScopeAware.scopeAwareKey;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final Metadata metadata;
    private final TypeCoercion typeCoercion;
    private final Session session;
    private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

    SubqueryPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            Metadata metadata,
            TypeCoercion typeCoercion,
            Optional<TranslationMap> outerContext,
            Session session,
            Map<NodeRef<Node>, RelationPlan> recursiveSubqueries)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeCoercion, "typeCoercion is null");
        requireNonNull(outerContext, "outerContext is null");
        requireNonNull(session, "session is null");
        requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.metadata = metadata;
        this.typeCoercion = typeCoercion;
        this.session = session;
        this.recursiveSubqueries = recursiveSubqueries;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        for (Cluster<InPredicate> cluster : cluster(builder.getScope(), selectSubqueries(builder, expression, analysis.getInPredicateSubqueries(node)))) {
            builder = planInPredicate(builder, cluster, node);
        }
        for (Cluster<SubqueryExpression> cluster : cluster(builder.getScope(), selectSubqueries(builder, expression, analysis.getScalarSubqueries(node)))) {
            builder = planScalarSubquery(builder, cluster);
        }
        for (Cluster<ExistsPredicate> cluster : cluster(builder.getScope(), selectSubqueries(builder, expression, analysis.getExistsSubqueries(node)))) {
            builder = planExists(builder, cluster);
        }
        for (Cluster<QuantifiedComparisonExpression> cluster : cluster(builder.getScope(), selectSubqueries(builder, expression, analysis.getQuantifiedComparisonSubqueries(node)))) {
            builder = planQuantifiedComparison(builder, cluster, node);
        }

        return builder;
    }

    /**
     * Find subqueries from the candidate set that are children of the given parent
     * and that have not already been handled in the subplan
     */
    private <T extends Expression> List<T> selectSubqueries(PlanBuilder subPlan, Expression parent, List<T> candidates)
    {
        SuccessorsFunction<Node> recurse = expression -> {
            if (expression instanceof Expression &&
                    !analysis.isColumnReference((Expression) expression) && // no point in following dereference chains
                    !subPlan.canTranslate((Expression) expression)) { // don't consider subqueries under parts of the expression that have already been handled
                return expression.getChildren();
            }

            return ImmutableList.of();
        };

        Iterable<Node> allSubExpressions = Traverser.forTree(recurse).depthFirstPreOrder(parent);

        return candidates
                .stream()
                .filter(candidate -> stream(allSubExpressions).anyMatch(child -> child == candidate))
                .filter(candidate -> !subPlan.canTranslate(candidate))
                .collect(toImmutableList());
    }

    /**
     * Group expressions into clusters such that all entries in a cluster are #equals to each other
     */
    private <T extends Expression> Collection<Cluster<T>> cluster(Scope scope, List<T> expressions)
    {
        Map<ScopeAware<T>, List<T>> sets = new LinkedHashMap<>();

        for (T expression : expressions) {
            sets.computeIfAbsent(ScopeAware.scopeAwareKey(expression, analysis, scope), key -> new ArrayList<>())
                    .add(expression);
        }

        return sets.values().stream()
                .map(cluster -> Cluster.newCluster(cluster, scope, analysis))
                .collect(toImmutableList());
    }

    private PlanBuilder planInPredicate(PlanBuilder subPlan, Cluster<InPredicate> cluster, Node node)
    {
        // Plan one of the predicates from the cluster
        InPredicate predicate = cluster.getRepresentative();

        Expression value = predicate.getValue();
        SubqueryExpression subquery = (SubqueryExpression) predicate.getValueList();
        Symbol output = symbolAllocator.newSymbol(predicate, BOOLEAN);

        subPlan = handleSubqueries(subPlan, value, node);
        subPlan = planInPredicate(subPlan, value, subquery, output, predicate);

        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
                subPlan.getRoot());
    }

    /**
     * Plans a correlated subquery for value IN (subQuery)
     *
     * @param originalExpression the original expression from which the IN predicate was derived. Used for subsequent translations.
     */
    private PlanBuilder planInPredicate(PlanBuilder subPlan, Expression value, SubqueryExpression subquery, Symbol output, Expression originalExpression)
    {
        // Use the current plan's translations as the outer context when planning the subquery
        RelationPlan relationPlan = planSubquery(subquery, subPlan.getTranslations());
        PlanBuilder subqueryPlan = newPlanBuilder(
                relationPlan,
                analysis,
                lambdaDeclarationToSymbolMap,
                ImmutableMap.of(scopeAwareKey(subquery, analysis, relationPlan.getScope()), Iterables.getOnlyElement(relationPlan.getFieldMappings())));

        QueryPlanner.PlanAndMappings subqueryCoercions = coerce(subqueryPlan, ImmutableList.of(subquery), analysis, idAllocator, symbolAllocator, typeCoercion);
        subqueryPlan = subqueryCoercions.getSubPlan();

        subPlan = subPlan.appendProjections(
                ImmutableList.of(value),
                symbolAllocator,
                idAllocator);

        QueryPlanner.PlanAndMappings subplanCoercions = coerce(subPlan, ImmutableList.of(value), analysis, idAllocator, symbolAllocator, typeCoercion);
        subPlan = subplanCoercions.getSubPlan();

        return new PlanBuilder(
                subPlan.getTranslations(),
                new ApplyNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subqueryPlan.getRoot(),
                        Assignments.of(output, new InPredicate(
                                subplanCoercions.get(value).toSymbolReference(),
                                subqueryCoercions.get(subquery).toSymbolReference())),
                        subPlan.getRoot().getOutputSymbols(),
                        originalExpression));
    }

    private PlanBuilder planScalarSubquery(PlanBuilder subPlan, Cluster<SubqueryExpression> cluster)
    {
        // Plan one of the predicates from the cluster
        SubqueryExpression scalarSubquery = cluster.getRepresentative();

        RelationPlan relationPlan = planSubquery(scalarSubquery, subPlan.getTranslations());
        PlanBuilder subqueryPlan = newPlanBuilder(
                relationPlan,
                analysis,
                lambdaDeclarationToSymbolMap);

        subqueryPlan = subqueryPlan.withNewRoot(new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan.getRoot()));

        return appendCorrelatedJoin(
                subPlan,
                subqueryPlan,
                scalarSubquery.getQuery(),
                CorrelatedJoinNode.Type.INNER,
                TRUE_LITERAL,
                mapAll(cluster, subPlan.getScope(), getOnlyElement(relationPlan.getFieldMappings())));
    }

    public PlanBuilder appendCorrelatedJoin(PlanBuilder subPlan, PlanBuilder subqueryPlan, Query query, CorrelatedJoinNode.Type type, Expression filterCondition, Map<ScopeAware<Expression>, Symbol> mappings)
    {
        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(mappings),
                new CorrelatedJoinNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subqueryPlan.getRoot(),
                        subPlan.getRoot().getOutputSymbols(),
                        type,
                        filterCondition,
                        query));
    }

    private PlanBuilder planExists(PlanBuilder subPlan, Cluster<ExistsPredicate> cluster)
    {
        // Plan one of the predicates from the cluster
        ExistsPredicate existsPredicate = cluster.getRepresentative();

        Expression subquery = existsPredicate.getSubquery();
        Symbol exists = symbolAllocator.newSymbol("exists", BOOLEAN);

        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), exists)),
                new ApplyNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        planSubquery(subquery, subPlan.getTranslations()).getRoot(),
                        Assignments.of(exists, new ExistsPredicate(TRUE_LITERAL)),
                        subPlan.getRoot().getOutputSymbols(),
                        subquery));
    }

    private RelationPlan planSubquery(Expression subquery, TranslationMap outerContext)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, Optional.of(outerContext), session, recursiveSubqueries)
                .process(subquery, null);
    }

    private PlanBuilder planQuantifiedComparison(PlanBuilder subPlan, Cluster<QuantifiedComparisonExpression> cluster, Node node)
    {
        // Plan one of the predicates from the cluster
        QuantifiedComparisonExpression quantifiedComparison = cluster.getRepresentative();

        ComparisonExpression.Operator operator = quantifiedComparison.getOperator();
        Quantifier quantifier = quantifiedComparison.getQuantifier();
        Expression value = quantifiedComparison.getValue();
        SubqueryExpression subquery = (SubqueryExpression) quantifiedComparison.getSubquery();

        subPlan = handleSubqueries(subPlan, value, node);

        Symbol output = symbolAllocator.newSymbol(quantifiedComparison, BOOLEAN);

        switch (operator) {
            case EQUAL:
                switch (quantifier) {
                    case ALL:
                        subPlan = planQuantifiedComparison(subPlan, operator, quantifier, value, subquery, output);
                        return new PlanBuilder(
                                subPlan.getTranslations()
                                        .withAdditionalMappings(ImmutableMap.of(scopeAwareKey(quantifiedComparison, analysis, subPlan.getScope()), output)),
                                subPlan.getRoot());
                    case ANY:
                    case SOME:
                        // A = ANY B <=> A IN B
                        subPlan = planInPredicate(subPlan, value, subquery, output, quantifiedComparison);

                        return new PlanBuilder(
                                subPlan.getTranslations()
                                        .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
                                subPlan.getRoot());
                }
                break;

            case NOT_EQUAL:
                switch (quantifier) {
                    case ALL: {
                        // A <> ALL B <=> !(A IN B)
                        subPlan = planInPredicate(subPlan, value, subquery, output, quantifiedComparison);
                        return addNegation(subPlan, cluster, output);
                    }
                    case ANY:
                    case SOME: {
                        // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
                        // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
                        subPlan = planQuantifiedComparison(subPlan, EQUAL, Quantifier.ALL, value, subquery, output);
                        return addNegation(subPlan, cluster, output);
                    }
                }
                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                subPlan = planQuantifiedComparison(subPlan, operator, quantifier, value, subquery, output);
                return new PlanBuilder(
                        subPlan.getTranslations()
                                .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
                        subPlan.getRoot());
        }
        // all cases are checked, so this exception should never be thrown
        throw new IllegalArgumentException(
                format("Unexpected quantified comparison: '%s %s'", operator.getValue(), quantifier));
    }

    /**
     * Adds a negation of the given input and remaps the provided expression to the negated expression
     */
    private PlanBuilder addNegation(PlanBuilder subPlan, Cluster<? extends Expression> cluster, Symbol input)
    {
        Symbol output = symbolAllocator.newSymbol("not", BOOLEAN);

        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
                new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putIdentities(subPlan.getRoot().getOutputSymbols())
                                .put(output, new NotExpression(input.toSymbolReference()))
                                .build()));
    }

    private PlanBuilder planQuantifiedComparison(PlanBuilder subPlan, ComparisonExpression.Operator operator, Quantifier quantifier, Expression value, Expression subquery, Symbol assignment)
    {
        RelationPlan relationPlan = planSubquery(subquery, subPlan.getTranslations());
        PlanBuilder subqueryPlan = newPlanBuilder(
                relationPlan,
                analysis,
                lambdaDeclarationToSymbolMap,
                ImmutableMap.of(scopeAwareKey(subquery, analysis, relationPlan.getScope()), Iterables.getOnlyElement(relationPlan.getFieldMappings())));
        QueryPlanner.PlanAndMappings subqueryCoercions = coerce(subqueryPlan, ImmutableList.of(subquery), analysis, idAllocator, symbolAllocator, typeCoercion);
        subqueryPlan = subqueryCoercions.getSubPlan();

        subPlan = subPlan.appendProjections(
                ImmutableList.of(value),
                symbolAllocator,
                idAllocator);

        QueryPlanner.PlanAndMappings subplanCoercions = coerce(subPlan, ImmutableList.of(value), analysis, idAllocator, symbolAllocator, typeCoercion);
        subPlan = subplanCoercions.getSubPlan();

        return new PlanBuilder(
                subPlan.getTranslations(),
                new ApplyNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subqueryPlan.getRoot(),
                        Assignments.of(assignment, new QuantifiedComparisonExpression(
                                operator,
                                quantifier,
                                subplanCoercions.get(value).toSymbolReference(),
                                subqueryCoercions.get(subquery).toSymbolReference())),
                        subPlan.getRoot().getOutputSymbols(),
                        subquery));
    }

    private <T extends Expression> Map<ScopeAware<Expression>, Symbol> mapAll(Cluster<T> cluster, Scope scope, Symbol output)
    {
        return cluster.getExpressions().stream()
                .collect(toImmutableMap(
                        expression -> ScopeAware.scopeAwareKey(expression, analysis, scope),
                        expression -> output,
                        (first, second) -> first));
    }

    /**
     * A group of expressions that are equivalent to each other according to ScopeAware criteria
     */
    private static class Cluster<T extends Expression>
    {
        private final List<T> expressions;

        private Cluster(List<T> expressions)
        {
            checkArgument(!expressions.isEmpty(), "Cluster is empty");
            this.expressions = ImmutableList.copyOf(expressions);
        }

        public static <T extends Expression> Cluster<T> newCluster(List<T> expressions, Scope scope, Analysis analysis)
        {
            long count = expressions.stream()
                    .map(expression -> scopeAwareKey(expression, analysis, scope))
                    .distinct()
                    .count();

            checkArgument(count == 1, "Cluster contains expressions that are not equivalent to each other");

            return new Cluster<>(expressions);
        }

        public List<T> getExpressions()
        {
            return expressions;
        }

        public T getRepresentative()
        {
            return expressions.get(0);
        }
    }
}
