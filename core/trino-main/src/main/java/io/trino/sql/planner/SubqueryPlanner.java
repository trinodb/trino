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
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analysis.OperandAndPredicate;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.QueryPlanner.PlanAndMappings;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.ComparisonPredicate;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.MatchPredicate;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QuantifiedComparisonPredicate;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.UniquePredicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.PlanBuilder.newPlanBuilder;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
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
        List<Node> allSubExpressions = ImmutableList.copyOf(Traverser.forTree(recurseExpression(builder)).depthFirstPreOrder(expression));

        // Relational subquery predicates - `x IN (subquery)`, `x <op> ANY (subquery)`, and the
        // extended-CASE WHEN fragments over the implicit case operand - are registered by the
        // analyzer as [OperandAndPredicate] pairs. They are planned uniformly here; the planned
        // symbol is registered against the relational predicate node so that TranslationMap
        // recovers it by node identity.
        PlanBuilder inPredicatesBuilder = builder;
        for (Cluster<OperandAndPredicate> cluster : cluster(selectSubqueryPredicates(inPredicatesBuilder, allSubExpressions, subqueries.getInPredicates()), entry -> subqueryPredicateKey(entry, inPredicatesBuilder.getScope()))) {
            builder = planInPredicate(builder, cluster, subqueries);
        }
        PlanBuilder scalarSubqueriesBuilder = builder;
        for (Cluster<SubqueryExpression> cluster : cluster(selectSubqueries(scalarSubqueriesBuilder, allSubExpressions, subqueries.getSubqueries()), expr -> scopeAwareKey(expr, analysis, scalarSubqueriesBuilder.getScope()))) {
            builder = planScalarSubquery(builder, cluster);
        }
        PlanBuilder existsBuilder = builder;
        for (Cluster<ExistsPredicate> cluster : cluster(selectSubqueries(existsBuilder, allSubExpressions, subqueries.getExistsSubqueries()), expr -> scopeAwareKey(expr, analysis, existsBuilder.getScope()))) {
            builder = planExists(builder, cluster);
        }
        PlanBuilder quantifiedComparisonsBuilder = builder;
        for (Cluster<OperandAndPredicate> cluster : cluster(selectSubqueryPredicates(quantifiedComparisonsBuilder, allSubExpressions, subqueries.getQuantifiedComparisons()), entry -> subqueryPredicateKey(entry, quantifiedComparisonsBuilder.getScope()))) {
            builder = planQuantifiedComparison(builder, cluster, subqueries);
        }
        PlanBuilder matchBuilder = builder;
        for (Cluster<OperandAndPredicate> cluster : cluster(selectSubqueryPredicates(matchBuilder, allSubExpressions, subqueries.getMatchPredicates()), entry -> subqueryPredicateKey(entry, matchBuilder.getScope()))) {
            builder = planMatchPredicate(builder, cluster, subqueries);
        }
        PlanBuilder uniqueBuilder = builder;
        for (Cluster<UniquePredicate> cluster : cluster(selectSubqueries(uniqueBuilder, allSubExpressions, subqueries.getUniquePredicates()), expr -> scopeAwareKey(expr, analysis, uniqueBuilder.getScope()))) {
            builder = planUniquePredicate(builder, cluster);
        }

        return builder;
    }

    /// Scope-aware grouping key for an [OperandAndPredicate]: two pairs cluster together only
    /// when both the operand and the relational predicate are scope-aware equivalent. This is
    /// also the key under which the planned symbol is registered for cross-call deduplication.
    private TranslationMap.RelationalPredicateKey subqueryPredicateKey(OperandAndPredicate entry, Scope scope)
    {
        return new TranslationMap.RelationalPredicateKey(
                scopeAwareKey(entry.operand(), analysis, scope),
                scopeAwareKey(entry.predicate(), analysis, scope));
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

    /// Find relational subquery predicates whose predicate node is reachable in the current
    /// expression tree and that have not already been planned in the sub-plan. Reachability is
    /// keyed on predicate node identity; the already-planned check is scope-aware so that a
    /// structurally equal predicate planned by an earlier `handleSubqueries` call is skipped.
    private List<OperandAndPredicate> selectSubqueryPredicates(PlanBuilder subPlan, List<Node> allSubExpressions, List<OperandAndPredicate> candidates)
    {
        return candidates.stream()
                .filter(candidate -> allSubExpressions.stream().anyMatch(node -> node == candidate.predicate()))
                .filter(candidate -> !subPlan.getTranslations().isPlanned(candidate.operand(), candidate.predicate()))
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
    private <T> Collection<Cluster<T>> cluster(List<T> expressions, Function<T, Object> keyFunction)
    {
        Map<Object, List<T>> sets = new LinkedHashMap<>();

        for (T expression : expressions) {
            sets.computeIfAbsent(keyFunction.apply(expression), _ -> new ArrayList<>())
                    .add(expression);
        }

        return sets.values().stream()
                .map(cluster -> Cluster.newCluster(cluster, keyFunction))
                .collect(toImmutableList());
    }

    private PlanBuilder planInPredicate(PlanBuilder subPlan, Cluster<OperandAndPredicate> cluster, Analysis.SubqueryAnalysis subqueries)
    {
        // Plan one of the predicates from the cluster
        OperandAndPredicate representative = cluster.getRepresentative();
        InPredicate inPredicate = (InPredicate) representative.predicate();

        io.trino.sql.tree.Expression value = representative.operand();
        SubqueryExpression subquery = (SubqueryExpression) inPredicate.getValueList();
        Symbol output = symbolAllocator.newSymbol("expr", BOOLEAN);

        subPlan = handleSubqueries(subPlan, value, subqueries);
        subPlan = planInPredicate(subPlan, value, subquery, output, value, analysis.getPredicateCoercions(inPredicate));

        // The in-place NOT (i.e. `x NOT IN (subquery)`) is carried on the predicate; wrap the
        // semi-join's output with NOT and map the predicate to the wrapped symbol so the rest of
        // the planner sees the negated form.
        if (inPredicate.isNegated()) {
            subPlan = addNegation(subPlan, cluster, output);
            return subPlan;
        }
        return new PlanBuilder(
                mapClusterOutput(subPlan.getTranslations(), cluster, output),
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
                plannerContext,
                symbolAllocator);

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

    private PlanBuilder planExists(PlanBuilder subPlan, Cluster<ExistsPredicate> cluster)
    {
        // Plan one of the predicates from the cluster
        ExistsPredicate existsPredicate = cluster.getRepresentative();

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

    /**
     * Plans `UNIQUE (subquery)` per SQL:2003 §8.9. The predicate is TRUE iff no two rows of the
     * subquery are equal under row equality, ignoring any row that contains a NULL value (those
     * rows are not duplicates of anything, including themselves).
     * <p>
     * Reduces to: `NOT EXISTS (
     *   SELECT 1 FROM subquery WHERE noneNull(R) GROUP BY R HAVING COUNT(*) > 1
     * )`.
     * <p>
     * An empty subquery and a subquery containing only NULL-bearing rows both yield TRUE — the
     * grouped + filtered relation is empty so `EXISTS` is FALSE and `NOT EXISTS` is TRUE.
     */
    private PlanBuilder planUniquePredicate(PlanBuilder subPlan, Cluster<UniquePredicate> cluster)
    {
        UniquePredicate predicate = cluster.getRepresentative();
        io.trino.sql.tree.Expression subqueryExpression = predicate.getSubquery();

        RelationPlan relationPlan = planSubquery(subqueryExpression, subPlan.getTranslations());
        List<Symbol> subqueryFields = visibleFieldMappings(relationPlan);

        // Filter out any row that contains a NULL — such rows can't form duplicates per the spec.
        Expression noneNullFilter = combineAnd(subqueryFields.stream()
                .map(symbol -> (Expression) not(plannerContext.getMetadata(), new IsNull(symbol.toSymbolReference())))
                .collect(toImmutableList()));
        PlanNode nonNullRows = new FilterNode(idAllocator.getNextId(), relationPlan.getRoot(), noneNullFilter);

        // Group by the full row and count occurrences.
        ResolvedFunction countFunction = plannerContext.getMetadata().resolveBuiltinFunction("count", ImmutableList.of());
        Symbol countSymbol = symbolAllocator.newSymbol("count", BIGINT);
        AggregationNode countAggregation = singleAggregation(
                idAllocator.getNextId(),
                nonNullRows,
                ImmutableMap.of(countSymbol, new AggregationNode.Aggregation(
                        countFunction,
                        ImmutableList.of(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                singleGroupingSet(subqueryFields));

        // Keep only groups that have more than one row — those are duplicates.
        PlanNode duplicates = new FilterNode(
                idAllocator.getNextId(),
                countAggregation,
                comparison(plannerContext.getMetadata(), ComparisonOperator.GREATER_THAN, countSymbol.toSymbolReference(), new Constant(BIGINT, 1L)));

        // EXISTS over duplicates — TRUE iff there is at least one duplicate group.
        Symbol existsSymbol = symbolAllocator.newSymbol("exists", BOOLEAN);
        PlanNode apply = new ApplyNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                duplicates,
                ImmutableMap.of(existsSymbol, new ApplyNode.Exists()),
                subPlan.getRoot().getOutputSymbols(),
                subqueryExpression);

        // UNIQUE is the negation: no duplicate group → TRUE.
        Symbol uniqueSymbol = symbolAllocator.newSymbol("unique", BOOLEAN);
        PlanNode root = new ProjectNode(
                idAllocator.getNextId(),
                apply,
                Assignments.builder()
                        .putIdentities(apply.getOutputSymbols())
                        .put(uniqueSymbol, not(plannerContext.getMetadata(), existsSymbol.toSymbolReference()))
                        .build());

        return new PlanBuilder(
                subPlan.getTranslations()
                        .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), uniqueSymbol)),
                root);
    }

    private RelationPlan planSubquery(io.trino.sql.tree.Expression subquery, TranslationMap outerContext)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, plannerContext, Optional.of(outerContext), session, recursiveSubqueries)
                .process(subquery, null);
    }

    private PlanBuilder planQuantifiedComparison(PlanBuilder subPlan, Cluster<OperandAndPredicate> cluster, Analysis.SubqueryAnalysis subqueries)
    {
        // Plan one of the predicates from the cluster
        OperandAndPredicate representative = cluster.getRepresentative();
        QuantifiedComparisonPredicate quantifiedComparison = (QuantifiedComparisonPredicate) representative.predicate();

        ComparisonPredicate.Operator operator = quantifiedComparison.getOperator();
        QuantifiedComparisonPredicate.Quantifier quantifier = quantifiedComparison.getQuantifier();
        io.trino.sql.tree.Expression value = representative.operand();
        SubqueryExpression subquery = (SubqueryExpression) quantifiedComparison.getSubquery();

        subPlan = handleSubqueries(subPlan, value, subqueries);

        Symbol output = symbolAllocator.newSymbol("expr", BOOLEAN);

        Analysis.PredicateCoercions predicateCoercions = analysis.getPredicateCoercions(quantifiedComparison);

        return switch (operator) {
            case EQUAL -> switch (quantifier) {
                case ALL -> {
                    subPlan = planQuantifiedComparison(subPlan, operator, quantifier, value, subquery, output, predicateCoercions);
                    yield new PlanBuilder(
                            mapClusterOutput(subPlan.getTranslations(), cluster, output),
                            subPlan.getRoot());
                }
                case ANY, SOME -> {
                    // A = ANY B <=> A IN B
                    subPlan = planInPredicate(subPlan, value, subquery, output, value, predicateCoercions);
                    yield new PlanBuilder(
                            mapClusterOutput(subPlan.getTranslations(), cluster, output),
                            subPlan.getRoot());
                }
            };
            case NOT_EQUAL -> switch (quantifier) {
                // A <> ALL B <=> !(A IN B)
                case ALL -> addNegation(
                        planInPredicate(subPlan, value, subquery, output, value, predicateCoercions),
                        cluster,
                        output);
                // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
                // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
                case ANY, SOME -> addNegation(
                        planQuantifiedComparison(subPlan, ComparisonPredicate.Operator.EQUAL, QuantifiedComparisonPredicate.Quantifier.ALL, value, subquery, output, predicateCoercions),
                        cluster,
                        output);
            };
            case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                subPlan = planQuantifiedComparison(subPlan, operator, quantifier, value, subquery, output, predicateCoercions);
                yield new PlanBuilder(
                        mapClusterOutput(subPlan.getTranslations(), cluster, output),
                        subPlan.getRoot());
            }
        };
    }

    /**
     * Plans `ROW(v1, …, vN) MATCH [UNIQUE] [SIMPLE | PARTIAL | FULL] (subquery)` per SQL:2003 §8.5.
     * The plan is `outerNullWrap(mode, R, indicator)` where:
     * - non-UNIQUE: indicator = EXISTS (subquery WHERE rowMatch)
     * - UNIQUE: indicator = (COUNT(*) over subquery WHERE rowMatch) = 1
     * - rowMatch is AND(R[i] = S[i]) for SIMPLE/FULL, AND(R[i] IS NULL OR R[i] = S[i]) for PARTIAL
     * - outerNullWrap is anyNull(R) OR x (SIMPLE), allNull(R) OR x (PARTIAL),
     *   allNull(R) OR (noneNull(R) AND x) (FULL)
     */
    private PlanBuilder planMatchPredicate(PlanBuilder subPlan, Cluster<OperandAndPredicate> cluster, Analysis.SubqueryAnalysis subqueries)
    {
        OperandAndPredicate representative = cluster.getRepresentative();
        MatchPredicate predicate = (MatchPredicate) representative.predicate();
        io.trino.sql.tree.Expression value = representative.operand();
        io.trino.sql.tree.Expression subqueryExpression = predicate.getSubquery();

        subPlan = handleSubqueries(subPlan, value, subqueries);

        // Plan the value's individual fields as scalar symbols on subPlan so that the row-match
        // predicate is a conjunction of Reference = Reference, which the decorrelator can lift.
        List<io.trino.sql.tree.Expression> valueFieldExpressions = valueFieldExpressions(value);
        subPlan = subPlan.appendProjections(valueFieldExpressions, symbolAllocator, idAllocator);
        List<Symbol> valueFields = valueFieldExpressions.stream()
                .map(subPlan::translate)
                .collect(toImmutableList());

        // Plan the subquery as a relation; its visible field mappings are the per-field S symbols.
        RelationPlan relationPlan = planSubquery(subqueryExpression, subPlan.getTranslations());
        List<Symbol> subqueryFields = visibleFieldMappings(relationPlan);

        checkState(valueFields.size() == subqueryFields.size(),
                "MATCH row arity mismatch: value has %s fields, subquery has %s",
                valueFields.size(),
                subqueryFields.size());

        // If value-field types differ from subquery-field types, cast the value side to match.
        boolean needsCoercion = false;
        for (int i = 0; i < valueFields.size(); i++) {
            if (!valueFields.get(i).type().equals(subqueryFields.get(i).type())) {
                needsCoercion = true;
                break;
            }
        }
        if (needsCoercion) {
            Assignments.Builder assignments = Assignments.builder()
                    .putIdentities(subPlan.getRoot().getOutputSymbols());
            ImmutableList.Builder<Symbol> coerced = ImmutableList.builderWithExpectedSize(valueFields.size());
            for (int i = 0; i < valueFields.size(); i++) {
                Symbol original = valueFields.get(i);
                Type targetType = subqueryFields.get(i).type();
                if (original.type().equals(targetType)) {
                    coerced.add(original);
                }
                else {
                    Symbol castSymbol = symbolAllocator.newSymbol("cast", targetType);
                    assignments.put(castSymbol, new Cast(original.toSymbolReference(), targetType));
                    coerced.add(castSymbol);
                }
            }
            subPlan = subPlan.withNewRoot(new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build()));
            valueFields = coerced.build();
        }

        Expression innerFilter = buildRowMatchFilter(plannerContext.getMetadata(), predicate.getType(), valueFields, subqueryFields);
        PlanNode filteredSubquery = new FilterNode(idAllocator.getNextId(), relationPlan.getRoot(), innerFilter);

        Symbol indicatorSymbol;
        PlanNode withIndicator;
        if (predicate.isUnique()) {
            ResolvedFunction countFunction = plannerContext.getMetadata().resolveBuiltinFunction("count", ImmutableList.of());
            Symbol countSymbol = symbolAllocator.newSymbol("count", BIGINT);
            AggregationNode countAggregation = singleAggregation(
                    idAllocator.getNextId(),
                    filteredSubquery,
                    ImmutableMap.of(countSymbol, new AggregationNode.Aggregation(
                            countFunction,
                            ImmutableList.of(),
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty())),
                    globalAggregation());
            PlanNode joined = new CorrelatedJoinNode(
                    idAllocator.getNextId(),
                    subPlan.getRoot(),
                    countAggregation,
                    subPlan.getRoot().getOutputSymbols(),
                    JoinType.INNER,
                    TRUE,
                    ((SubqueryExpression) subqueryExpression).getQuery());
            indicatorSymbol = symbolAllocator.newSymbol("unique", BOOLEAN);
            withIndicator = new ProjectNode(
                    idAllocator.getNextId(),
                    joined,
                    Assignments.builder()
                            .putIdentities(joined.getOutputSymbols())
                            .put(indicatorSymbol, comparison(plannerContext.getMetadata(), ComparisonOperator.EQUAL, countSymbol.toSymbolReference(), new Constant(BIGINT, 1L)))
                            .build());
        }
        else {
            indicatorSymbol = symbolAllocator.newSymbol("exists", BOOLEAN);
            withIndicator = new ApplyNode(
                    idAllocator.getNextId(),
                    subPlan.getRoot(),
                    filteredSubquery,
                    ImmutableMap.of(indicatorSymbol, new ApplyNode.Exists()),
                    subPlan.getRoot().getOutputSymbols(),
                    subqueryExpression);
        }

        Symbol matchSymbol = symbolAllocator.newSymbol("match", BOOLEAN);
        Expression matchExpression = buildOuterNullWrap(predicate.getType(), valueFields, indicatorSymbol);
        PlanNode root = new ProjectNode(
                idAllocator.getNextId(),
                withIndicator,
                Assignments.builder()
                        .putIdentities(withIndicator.getOutputSymbols())
                        .put(matchSymbol, matchExpression)
                        .build());

        return new PlanBuilder(
                mapClusterOutput(subPlan.getTranslations(), cluster, matchSymbol),
                root);
    }

    private static List<io.trino.sql.tree.Expression> valueFieldExpressions(io.trino.sql.tree.Expression value)
    {
        if (value instanceof io.trino.sql.tree.Row row) {
            return row.getFields().stream()
                    .map(io.trino.sql.tree.Row.Field::getExpression)
                    .collect(toImmutableList());
        }
        return ImmutableList.of(value);
    }

    private static List<Symbol> visibleFieldMappings(RelationPlan relationPlan)
    {
        RelationType descriptor = relationPlan.getDescriptor();
        ImmutableList.Builder<Symbol> fields = ImmutableList.builder();
        for (int i = 0; i < descriptor.getAllFieldCount(); i++) {
            if (!descriptor.getFieldByIndex(i).isHidden()) {
                fields.add(relationPlan.getFieldMappings().get(i));
            }
        }
        return fields.build();
    }

    private static Expression buildRowMatchFilter(Metadata metadata, MatchPredicate.Type type, List<Symbol> valueFields, List<Symbol> subqueryFields)
    {
        ImmutableList.Builder<Expression> conjuncts = ImmutableList.builderWithExpectedSize(valueFields.size());
        for (int i = 0; i < valueFields.size(); i++) {
            Expression r = valueFields.get(i).toSymbolReference();
            Expression s = subqueryFields.get(i).toSymbolReference();
            Expression equality = comparison(metadata, ComparisonOperator.EQUAL, r, s);
            if (type == MatchPredicate.Type.PARTIAL) {
                conjuncts.add(combineOr(new IsNull(r), equality));
            }
            else {
                conjuncts.add(equality);
            }
        }
        return combineAnd(conjuncts.build());
    }

    private Expression buildOuterNullWrap(MatchPredicate.Type type, List<Symbol> valueFields, Symbol indicatorSymbol)
    {
        List<Expression> isNullChecks = valueFields.stream()
                .map(symbol -> (Expression) new IsNull(symbol.toSymbolReference()))
                .collect(toImmutableList());
        Expression indicator = indicatorSymbol.toSymbolReference();
        return switch (type) {
            case SIMPLE -> combineOr(combineOr(isNullChecks), indicator);
            case PARTIAL -> combineOr(combineAnd(isNullChecks), indicator);
            case FULL -> combineOr(
                    combineAnd(isNullChecks),
                    combineAnd(not(plannerContext.getMetadata(), combineOr(isNullChecks)), indicator));
        };
    }

    private static Expression combineAnd(List<Expression> terms)
    {
        return terms.size() == 1 ? terms.get(0) : new Logical(Logical.Operator.AND, ImmutableList.copyOf(terms));
    }

    private static Expression combineAnd(Expression a, Expression b)
    {
        return new Logical(Logical.Operator.AND, ImmutableList.of(a, b));
    }

    private static Expression combineOr(List<Expression> terms)
    {
        return terms.size() == 1 ? terms.get(0) : new Logical(Logical.Operator.OR, ImmutableList.copyOf(terms));
    }

    private static Expression combineOr(Expression a, Expression b)
    {
        return new Logical(Logical.Operator.OR, ImmutableList.of(a, b));
    }

    /**
     * Adds a negation of the given input and remaps the provided expression to the negated expression
     */
    private PlanBuilder addNegation(PlanBuilder subPlan, Cluster<OperandAndPredicate> cluster, Symbol input)
    {
        Symbol output = symbolAllocator.newSymbol("not", BOOLEAN);

        return new PlanBuilder(
                mapClusterOutput(subPlan.getTranslations(), cluster, output),
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
            ComparisonPredicate.Operator operator,
            QuantifiedComparisonPredicate.Quantifier quantifier,
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

    private static ApplyNode.Quantifier mapQuantifier(QuantifiedComparisonPredicate.Quantifier quantifier)
    {
        return switch (quantifier) {
            case ALL -> ApplyNode.Quantifier.ALL;
            case ANY -> ApplyNode.Quantifier.ANY;
            case SOME -> ApplyNode.Quantifier.SOME;
        };
    }

    private static ApplyNode.Operator mapOperator(ComparisonPredicate.Operator operator)
    {
        return switch (operator) {
            case EQUAL -> ApplyNode.Operator.EQUAL;
            case NOT_EQUAL -> ApplyNode.Operator.NOT_EQUAL;
            case LESS_THAN -> ApplyNode.Operator.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> ApplyNode.Operator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN -> ApplyNode.Operator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> ApplyNode.Operator.GREATER_THAN_OR_EQUAL;
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
                plannerContext,
                symbolAllocator);

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
                        _ -> output,
                        (first, _) -> first));
    }

    /// Register the planned `output` for the cluster under its representative's scope-aware
    /// [TranslationMap.RelationalPredicateKey]. All cluster members share that key by
    /// construction, so the single entry covers every member; it also lets structurally equal
    /// `value <predicate>` occurrences planned in a later `handleSubqueries` call be recognized
    /// as already planned.
    private TranslationMap mapClusterOutput(TranslationMap translations, Cluster<OperandAndPredicate> cluster, Symbol output)
    {
        return translations
                .withAdditionalPredicateMappings(ImmutableMap.of(
                        subqueryPredicateKey(cluster.getRepresentative(), translations.getScope()), output));
    }

    /**
     * A group of expressions that are equivalent to each other according to ScopeAware criteria
     */
    private static class Cluster<T>
    {
        private final List<T> expressions;

        private Cluster(List<T> expressions)
        {
            checkArgument(!expressions.isEmpty(), "Cluster is empty");
            this.expressions = ImmutableList.copyOf(expressions);
        }

        public static <T> Cluster<T> newCluster(List<T> expressions, Function<T, Object> keyFunction)
        {
            long count = expressions.stream()
                    .map(keyFunction)
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
