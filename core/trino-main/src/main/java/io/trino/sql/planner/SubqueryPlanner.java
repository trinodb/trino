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
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.QueryPlanner.PlanAndMappings;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.QuantifiedComparisonExpression.Quantifier;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.SubqueryExpression;

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
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.PlanBuilder.newPlanBuilder;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final PlannerContext plannerContext;
    private final Session session;
    private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

    SubqueryPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            PlannerContext plannerContext,
            Optional<TranslationMap> outerContext,
            Session session,
            Map<NodeRef<Node>, RelationPlan> recursiveSubqueries)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(plannerContext, "plannerContext is null");
        requireNonNull(outerContext, "outerContext is null");
        requireNonNull(session, "session is null");
        requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.plannerContext = plannerContext;
        this.session = session;
        this.recursiveSubqueries = recursiveSubqueries;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<io.trino.sql.tree.Expression> expressions, Analysis.SubqueryAnalysis subqueries)
    {
        for (io.trino.sql.tree.Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, subqueries);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, io.trino.sql.tree.Expression expression, Analysis.SubqueryAnalysis subqueries)
    {
        Iterable<Node> allSubExpressions = Traverser.forTree(recurseExpression(builder)).depthFirstPreOrder(expression);
        for (Cluster<io.trino.sql.tree.InPredicate> cluster : cluster(builder.getScope(), selectSubqueries(builder, allSubExpressions, subqueries.getInPredicatesSubqueries()))) {
            builder = planInPredicate(builder, cluster, subqueries);
        }
        for (Cluster<SubqueryExpression> cluster : cluster(builder.getScope(), selectSubqueries(builder, allSubExpressions, subqueries.getSubqueries()))) {
            builder = planScalarSubquery(builder, cluster);
        }
        for (Cluster<ExistsPredicate> cluster : cluster(builder.getScope(), selectSubqueries(builder, allSubExpressions, subqueries.getExistsSubqueries()))) {
            builder = planExists(builder, cluster);
        }
        for (Cluster<QuantifiedComparisonExpression> cluster : cluster(builder.getScope(), selectSubqueries(builder, allSubExpressions, subqueries.getQuantifiedComparisonSubqueries()))) {
            builder = planQuantifiedComparison(builder, cluster, subqueries);
        }

        return builder;
    }

    /**
     * Find subqueries from the candidate set that are children of the given parent
     * and that have not already been handled in the subplan
     */
    private <T extends io.trino.sql.tree.Expression> List<T> selectSubqueries(PlanBuilder subPlan, Iterable<Node> allSubExpressions, List<T> candidates)
    {
        return candidates
                .stream()
                .filter(candidate -> stream(allSubExpressions).anyMatch(child -> child == candidate))
                .filter(candidate -> !subPlan.canTranslate(candidate))
                .collect(toImmutableList());
    }

    private SuccessorsFunction<Node> recurseExpression(PlanBuilder subPlan)
    {
        return expression -> {
            if (!(expression instanceof io.trino.sql.tree.Expression value) ||
                    (!analysis.isColumnReference(value) && // no point in following dereference chains
                            !subPlan.canTranslate(value))) { // don't consider subqueries under parts of the expression that have already been handled
                return expression.getChildren();
            }
            return ImmutableList.of();
        };
    }

    /**
     * Group expressions into clusters such that all entries in a cluster are #equals to each other
     */
    private <T extends io.trino.sql.tree.Expression> Collection<Cluster<T>> cluster(Scope scope, List<T> expressions)
    {
        Map<ScopeAware<T>, List<T>> sets = new LinkedHashMap<>();

        for (T expression : expressions) {
            sets.computeIfAbsent(scopeAwareKey(expression, analysis, scope), key -> new ArrayList<>())
                    .add(expression);
        }

        return sets.values().stream()
                .map(cluster -> Cluster.newCluster(cluster, scope, analysis))
                .collect(toImmutableList());
    }

    private PlanBuilder planInPredicate(PlanBuilder subPlan, Cluster<io.trino.sql.tree.InPredicate> cluster, Analysis.SubqueryAnalysis subqueries)
    {
        // Plan one of the predicates from the cluster
        io.trino.sql.tree.InPredicate predicate = cluster.getRepresentative();

        io.trino.sql.tree.Expression value = predicate.getValue();
        SubqueryExpression subquery = (SubqueryExpression) predicate.getValueList();
        Symbol output = symbolAllocator.newSymbol("expr", BOOLEAN);

        subPlan = handleSubqueries(subPlan, value, subqueries);
        subPlan = planInPredicate(subPlan, value, subquery, output, predicate, analysis.getPredicateCoercions(predicate));

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
    private PlanBuilder planInPredicate(
            PlanBuilder subPlan,
            io.trino.sql.tree.Expression value,
            SubqueryExpression subquery,
            Symbol output,
            io.trino.sql.tree.Expression originalExpression,
            Analysis.PredicateCoercions predicateCoercions)
    {
        PlanAndMappings subqueryPlan = planSubquery(subquery, predicateCoercions.getSubqueryCoercion(), subPlan.getTranslations());
        PlanAndMappings valuePlan = planValue(subPlan, value, predicateCoercions.getValueType(), predicateCoercions.getValueCoercion());

        return new PlanBuilder(
                valuePlan.getSubPlan().getTranslations(),
                new ApplyNode(
                        idAllocator.getNextId(),
                        valuePlan.getSubPlan().getRoot(),
                        subqueryPlan.getSubPlan().getRoot(),
                        ImmutableMap.of(output, new ApplyNode.In(valuePlan.get(value), subqueryPlan.get(subquery))),
                        valuePlan.getSubPlan().getRoot().getOutputSymbols(),
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
                lambdaDeclarationToSymbolMap,
                session,
                plannerContext);

        PlanNode root = new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan.getRoot());

        Type type = analysis.getType(scalarSubquery);
        RelationType descriptor = relationPlan.getDescriptor();
        List<Symbol> fieldMappings = relationPlan.getFieldMappings();
        Symbol column;
        if (descriptor.getVisibleFieldCount() > 1) {
            column = symbolAllocator.newSymbol("row", type);

            ImmutableList.Builder<Expression> fields = ImmutableList.builder();
            for (int i = 0; i < descriptor.getAllFieldCount(); i++) {
                Field field = descriptor.getFieldByIndex(i);
                if (!field.isHidden()) {
                    fields.add(fieldMappings.get(i).toSymbolReference());
                }
            }

            Expression expression = new Cast(new Row(fields.build()), type);

            root = new ProjectNode(idAllocator.getNextId(), root, Assignments.of(column, expression));
        }
        else {
            column = getOnlyElement(fieldMappings);
        }

        return appendCorrelatedJoin(
                subPlan,
                root,
                scalarSubquery.getQuery(),
                // Scalar subquery always contains EnforceSingleRowNode. Therefore, it's guaranteed
                // that subquery will return single row. Hence, correlated join can be of INNER type.
                JoinType.INNER,
                TRUE,
                mapAll(cluster, subPlan.getScope(), column));
    }

    public PlanBuilder appendCorrelatedJoin(PlanBuilder subPlan, PlanNode subquery, Query query, JoinType type, Expression filterCondition, Map<ScopeAware<io.trino.sql.tree.Expression>, Symbol> mappings)
    {
        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(mappings),
                new CorrelatedJoinNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subquery,
                        subPlan.getRoot().getOutputSymbols(),
                        type,
                        filterCondition,
                        query));
    }

    private PlanBuilder planExists(PlanBuilder subPlan, Cluster<io.trino.sql.tree.ExistsPredicate> cluster)
    {
        // Plan one of the predicates from the cluster
        io.trino.sql.tree.ExistsPredicate existsPredicate = cluster.getRepresentative();

        io.trino.sql.tree.Expression subquery = existsPredicate.getSubquery();
        Symbol exists = symbolAllocator.newSymbol("exists", BOOLEAN);

        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), exists)),
                new ApplyNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        planSubquery(subquery, subPlan.getTranslations()).getRoot(),
                        ImmutableMap.of(exists, new ApplyNode.Exists()),
                        subPlan.getRoot().getOutputSymbols(),
                        subquery));
    }

    private RelationPlan planSubquery(io.trino.sql.tree.Expression subquery, TranslationMap outerContext)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, Optional.of(outerContext), session, recursiveSubqueries)
                .process(subquery, null);
    }

    private PlanBuilder planQuantifiedComparison(PlanBuilder subPlan, Cluster<io.trino.sql.tree.QuantifiedComparisonExpression> cluster, Analysis.SubqueryAnalysis subqueries)
    {
        // Plan one of the predicates from the cluster
        io.trino.sql.tree.QuantifiedComparisonExpression quantifiedComparison = cluster.getRepresentative();

        io.trino.sql.tree.ComparisonExpression.Operator operator = quantifiedComparison.getOperator();
        Quantifier quantifier = quantifiedComparison.getQuantifier();
        io.trino.sql.tree.Expression value = quantifiedComparison.getValue();
        SubqueryExpression subquery = (SubqueryExpression) quantifiedComparison.getSubquery();

        subPlan = handleSubqueries(subPlan, value, subqueries);

        Symbol output = symbolAllocator.newSymbol("expr", BOOLEAN);

        Analysis.PredicateCoercions predicateCoercions = analysis.getPredicateCoercions(quantifiedComparison);

        return switch (operator) {
            case EQUAL -> switch (quantifier) {
                case ALL -> {
                    subPlan = planQuantifiedComparison(subPlan, operator, quantifier, value, subquery, output, predicateCoercions);
                    yield new PlanBuilder(
                            subPlan.getTranslations()
                                    .withAdditionalMappings(ImmutableMap.of(scopeAwareKey(quantifiedComparison, analysis, subPlan.getScope()), output)),
                            subPlan.getRoot());
                }
                case ANY, SOME -> {
                    // A = ANY B <=> A IN B
                    subPlan = planInPredicate(subPlan, value, subquery, output, quantifiedComparison, predicateCoercions);
                    yield new PlanBuilder(
                            subPlan.getTranslations()
                                    .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
                            subPlan.getRoot());
                }
            };
            case NOT_EQUAL -> switch (quantifier) {
                case ALL ->
                    // A <> ALL B <=> !(A IN B)
                        addNegation(
                                planInPredicate(subPlan, value, subquery, output, quantifiedComparison, predicateCoercions),
                                cluster,
                                output);
                case ANY, SOME ->
                    // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
                    // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
                        addNegation(
                                planQuantifiedComparison(subPlan, io.trino.sql.tree.ComparisonExpression.Operator.EQUAL, Quantifier.ALL, value, subquery, output, predicateCoercions),
                                cluster,
                                output);
            };
            case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                subPlan = planQuantifiedComparison(subPlan, operator, quantifier, value, subquery, output, predicateCoercions);
                yield new PlanBuilder(
                        subPlan.getTranslations()
                                .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
                        subPlan.getRoot());
            }
            case IS_DISTINCT_FROM -> // Cannot be used with quantified comparison
                    throw new IllegalArgumentException(format("Unexpected quantified comparison: '%s %s'", operator.getValue(), quantifier));
        };
    }

    /**
     * Adds a negation of the given input and remaps the provided expression to the negated expression
     */
    private PlanBuilder addNegation(PlanBuilder subPlan, Cluster<? extends io.trino.sql.tree.Expression> cluster, Symbol input)
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
                                .put(output, not(plannerContext.getMetadata(), input.toSymbolReference()))
                                .build()));
    }

    private PlanBuilder planQuantifiedComparison(
            PlanBuilder subPlan,
            io.trino.sql.tree.ComparisonExpression.Operator operator,
            Quantifier quantifier,
            io.trino.sql.tree.Expression value,
            io.trino.sql.tree.Expression subquery,
            Symbol assignment,
            Analysis.PredicateCoercions predicateCoercions)
    {
        PlanAndMappings subqueryPlan = planSubquery(subquery, predicateCoercions.getSubqueryCoercion(), subPlan.getTranslations());
        PlanAndMappings valuePlan = planValue(subPlan, value, predicateCoercions.getValueType(), predicateCoercions.getValueCoercion());

        return new PlanBuilder(
                valuePlan.getSubPlan().getTranslations(),
                new ApplyNode(
                        idAllocator.getNextId(),
                        valuePlan.getSubPlan().getRoot(),
                        subqueryPlan.getSubPlan().getRoot(),
                        ImmutableMap.of(assignment, new ApplyNode.QuantifiedComparison(mapOperator(operator), mapQuantifier(quantifier), valuePlan.get(value), subqueryPlan.get(subquery))),
                        valuePlan.getSubPlan().getRoot().getOutputSymbols(),
                        subquery));
    }

    private static ApplyNode.Quantifier mapQuantifier(Quantifier quantifier)
    {
        return switch (quantifier) {
            case ALL -> ApplyNode.Quantifier.ALL;
            case ANY -> ApplyNode.Quantifier.ANY;
            case SOME -> ApplyNode.Quantifier.SOME;
        };
    }

    private static ApplyNode.Operator mapOperator(io.trino.sql.tree.ComparisonExpression.Operator operator)
    {
        return switch (operator) {
            case EQUAL -> ApplyNode.Operator.EQUAL;
            case NOT_EQUAL -> ApplyNode.Operator.NOT_EQUAL;
            case LESS_THAN -> ApplyNode.Operator.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> ApplyNode.Operator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN -> ApplyNode.Operator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> ApplyNode.Operator.GREATER_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM -> throw new IllegalArgumentException();
        };
    }

    private PlanAndMappings planValue(PlanBuilder subPlan, io.trino.sql.tree.Expression value, Type actualType, Optional<Type> coercion)
    {
        subPlan = subPlan.appendProjections(ImmutableList.of(value), symbolAllocator, idAllocator);

        // Adapt implicit row type (in the SQL spec, <row value special case>) by wrapping it with a row constructor
        Symbol column = subPlan.translate(value);
        Type declaredType = analysis.getType(value);
        if (!actualType.equals(declaredType)) {
            Symbol wrapped = symbolAllocator.newSymbol("row", actualType);

            Assignments assignments = Assignments.builder()
                    .putIdentities(subPlan.getRoot().getOutputSymbols())
                    .put(wrapped, new Row(ImmutableList.of(column.toSymbolReference())))
                    .build();

            subPlan = subPlan.withNewRoot(new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments));

            column = wrapped;
        }

        return coerceIfNecessary(subPlan, column, value, coercion);
    }

    private PlanAndMappings planSubquery(io.trino.sql.tree.Expression subquery, Optional<Type> coercion, TranslationMap outerContext)
    {
        Type type = analysis.getType(subquery);
        Symbol column = symbolAllocator.newSymbol("row", type);

        RelationPlan relationPlan = planSubquery(subquery, outerContext);

        PlanBuilder subqueryPlan = newPlanBuilder(
                relationPlan,
                analysis,
                lambdaDeclarationToSymbolMap,
                ImmutableMap.of(scopeAwareKey(subquery, analysis, relationPlan.getScope()), column),
                session,
                plannerContext);

        RelationType descriptor = relationPlan.getDescriptor();
        ImmutableList.Builder<Expression> fields = ImmutableList.builder();
        for (int i = 0; i < descriptor.getAllFieldCount(); i++) {
            Field field = descriptor.getFieldByIndex(i);
            if (!field.isHidden()) {
                fields.add(relationPlan.getFieldMappings().get(i).toSymbolReference());
            }
        }

        subqueryPlan = subqueryPlan.withNewRoot(
                new ProjectNode(
                        idAllocator.getNextId(),
                        relationPlan.getRoot(),
                        Assignments.of(column, new Cast(new Row(fields.build()), type))));

        return coerceIfNecessary(subqueryPlan, column, subquery, coercion);
    }

    private PlanAndMappings coerceIfNecessary(PlanBuilder subPlan, Symbol symbol, io.trino.sql.tree.Expression value, Optional<? extends Type> coercion)
    {
        Symbol coerced = symbol;

        if (coercion.isPresent()) {
            coerced = symbolAllocator.newSymbol("expr", coercion.get());

            Assignments assignments = Assignments.builder()
                    .putIdentities(subPlan.getRoot().getOutputSymbols())
                    .put(coerced, new Cast(symbol.toSymbolReference(), coercion.get()))
                    .build();

            subPlan = subPlan.withNewRoot(new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments));
        }

        return new PlanAndMappings(subPlan, ImmutableMap.of(NodeRef.of(value), coerced));
    }

    private <T extends io.trino.sql.tree.Expression> Map<ScopeAware<io.trino.sql.tree.Expression>, Symbol> mapAll(Cluster<T> cluster, Scope scope, Symbol output)
    {
        return cluster.getExpressions().stream()
                .collect(toImmutableMap(
                        expression -> scopeAwareKey(expression, analysis, scope),
                        expression -> output,
                        (first, second) -> first));
    }

    /**
     * A group of expressions that are equivalent to each other according to ScopeAware criteria
     */
    private static class Cluster<T extends io.trino.sql.tree.Expression>
    {
        private final List<T> expressions;

        private Cluster(List<T> expressions)
        {
            checkArgument(!expressions.isEmpty(), "Cluster is empty");
            this.expressions = ImmutableList.copyOf(expressions);
        }

        public static <T extends io.trino.sql.tree.Expression> Cluster<T> newCluster(List<T> expressions, Scope scope, Analysis analysis)
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
