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
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.ExpressionSymbolInliner;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.optimizations.PlanNodeDecorrelator;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.SystemSessionProperties.useQueryFusion;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static java.util.Objects.requireNonNull;

/**
 * This rule decorrelates a correlated subquery of INNER correlated join with:
 * - single global aggregation that returns null on empty inputs
 * - a null-rejecting filter over the global aggregation variable.
 * In this case, we can produce a simpler decorrelation that pushes the aggregation below the join
 * without the need to go through UniqueOperator, Left joins, etc. In principle, improvements on
 * subsequent rules and better positioning of those subsequent rules should result in the same
 * replacement, but currently it is rather complex to refactor the rest of the rules.
 */
public class TransformSimpleCorrelatedGlobalAggregation
        implements PlanOptimizer
{
    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public TransformSimpleCorrelatedGlobalAggregation(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableStatsProvider tableStatsProvider)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(warningCollector, "warningCollector is null");
        requireNonNull(tableStatsProvider, "tableStatsProvider is null");

        // For now we only want this rule to be effective if query fusion is enabled
        if (!useQueryFusion(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new SimpleDecorrelationRewriter(idAllocator, symbolAllocator, plannerContext, typeAnalyzer, session), plan);
    }

    private static class SimpleDecorrelationRewriter
            extends SimplePlanRewriter<Void>
    {
        private final TypeAnalyzer typeAnalyzer;
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final PlannerContext plannerContext;

        private SimpleDecorrelationRewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.session = requireNonNull(session, "session is null");
        }

        private boolean returnsNullOnEmpty(Aggregation aggregation)
        {
            // TODO: Add other aggregation functions that return null on empty inputs.
            BoundSignature signature = aggregation.getResolvedFunction().getSignature();
            String aggName = signature.getName();
            return aggName.equals("sum") ||
                    aggName.equals("max") ||
                    aggName.equals("min") ||
                    aggName.equals("avg");
        }

        private Expression inlineProject(Expression expr, Optional<ProjectNode> project)
        {
            if (project.isEmpty()) {
                return expr;
            }

            Assignments assignments = project.get().getAssignments();
            Function<Symbol, Expression> mapping = symbol -> {
                Expression result = assignments.get(symbol);
                return result != null ? result : symbol.toSymbolReference();
            };

            return ExpressionSymbolInliner.inlineSymbols(mapping, expr);
        }

        private boolean rejectsNulls(Expression expr, Collection<Symbol> nullableSymbols)
        {
            for (Expression conjunct : extractConjuncts(expr)) {
                if (isDeterministic(conjunct, plannerContext.getMetadata())) {
                    Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), conjunct);
                    Object response = new ExpressionInterpreter(conjunct, plannerContext, session, expressionTypes)
                            .optimize(symbol -> nullableSymbols.contains(symbol) ? null : symbol.toSymbolReference());
                    if (response == null || response instanceof NullLiteral || Boolean.FALSE.equals(response)) {
                        return true;
                    }
                }
            }
            return false;
        }

        public Optional<PlanNode> tryDecorrelation(FilterNode filterNode, RewriteContext<Void> context)
        {
            // Find the following pattern:
            // Filter - [Project] - CorrelatedJoin - input
            //                                     \- [Project] - Aggregation - subquery
            // will replace the CorrelatedJoin with an inner join on the decorrelation criteria, and
            // a modified aggregation with the criteria's RHS grouping keys.

            PlanNode filterInput = filterNode.getSource();

            Optional<ProjectNode> outerProject = Optional.empty();
            if (filterInput instanceof ProjectNode) {
                outerProject = Optional.of((ProjectNode) filterInput);
                filterInput = outerProject.get().getSource();
            }

            if (!(filterInput instanceof CorrelatedJoinNode correlatedJoinNode)) {
                return Optional.empty();
            }

            PlanNode input = correlatedJoinNode.getInput();
            if (input instanceof CorrelatedJoinNode) {
                return Optional.empty();
            }

            PlanNode subquery = correlatedJoinNode.getSubquery();
            Optional<ProjectNode> correlatedProject = Optional.empty();
            if (subquery instanceof ProjectNode) {
                correlatedProject = Optional.of((ProjectNode) subquery);
                subquery = correlatedProject.get().getSource();
            }

            if (!(subquery instanceof AggregationNode aggregation)) {
                return Optional.empty();
            }
            subquery = aggregation.getSource();

            // Check requirements for this simple transformation.
            // 1- We currently handle only INNER correlated joins.
            if (correlatedJoinNode.getType() != INNER) {
                return Optional.empty();
            }

            // 2- We handle aggregates with a single aggregation function that returns NULL on empty inputs
            if (aggregation.getAggregations().size() != 1 ||
                    aggregation.getAggregations().values().stream().anyMatch(agg -> !returnsNullOnEmpty(agg))) {
                return Optional.empty();
            }

            // 3- We require the filter to reject null values of the aggregation function.
            Expression predicate = filterNode.getPredicate();
            predicate = inlineProject(predicate, correlatedProject);
            predicate = inlineProject(predicate, outerProject);
            if (!rejectsNulls(predicate, aggregation.getAggregations().keySet())) {
                return Optional.empty();
            }

            // Attempt to decorrelate the subquery.
            PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(plannerContext, symbolAllocator, Lookup.noLookup());
            Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedResult = decorrelator.decorrelateFilters(subquery, correlatedJoinNode.getCorrelation());
            if (decorrelatedResult.isEmpty() || decorrelatedResult.get().getCorrelatedPredicates().isEmpty()) {
                return Optional.empty();
            }

            // 4- Check that the decorrelated predicate is exclusively consists of equi-joins between input and subquery.
            // TODO: We can allow other filters as long as they only mention columns from input or
            //  columns in subquery that are in criteria as well.
            List<Expression> decorrelatedConjuncts = extractConjuncts(decorrelatedResult.get().getCorrelatedPredicates().get());
            if (decorrelatedConjuncts.stream().anyMatch(c -> !ReorderJoins.JoinEnumerator.isJoinEqualityCondition(c))) {
                return Optional.empty();
            }
            Set<Symbol> inputSymbols = new HashSet<>(input.getOutputSymbols());
            List<JoinNode.EquiJoinClause> criteria = decorrelatedConjuncts.stream().map(c -> ReorderJoins.JoinEnumerator.toEquiJoinClause((ComparisonExpression) c, inputSymbols)).collect(Collectors.toList());
            if (criteria.isEmpty() || criteria.stream().anyMatch(c -> inputSymbols.contains(c.getRight()))) {
                return Optional.empty();
            }

            // Recursively visit decorrelatedSource and inputs.
            PlanNode decorrelatedSource = this.visitPlan(decorrelatedResult.get().getNode(), context);
            input = this.visitPlan(input, context);

            // Assemble the result.
            AggregationNode.GroupingSetDescriptor groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                    criteria.stream().map(JoinNode.EquiJoinClause::getRight).collect(Collectors.toList()),
                    1,
                    Set.of());

            PlanNode rightInput = new AggregationNode(
                    idAllocator.getNextId(),
                    decorrelatedSource,
                    aggregation.getAggregations(),
                    groupingSetDescriptor,
                    aggregation.getPreGroupedSymbols(),
                    aggregation.getStep(),
                    aggregation.getHashSymbol(),
                    aggregation.getGroupIdSymbol());

            if (correlatedProject.isPresent()) {
                rightInput = new ProjectNode(
                        idAllocator.getNextId(),
                        rightInput,
                        Assignments.builder()
                                .putAll(correlatedProject.get().getAssignments())
                                .putIdentities(groupingSetDescriptor.getGroupingKeys())
                                .build());
            }

            PlanNode result = new JoinNode(
                    idAllocator.getNextId(),
                    JoinNode.Type.INNER,
                    input,
                    rightInput,
                    criteria,
                    input.getOutputSymbols(),
                    rightInput.getOutputSymbols(),
                    false,
                    Optional.empty(), // no filter allowed
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());

            // Add optional outer project
            if (outerProject.isPresent()) {
                result = outerProject.get().replaceChildren(List.of(result));
            }

            // Add filter
            result = filterNode.replaceChildren(List.of(result));

            // restrict outputs
            return Optional.of(restrictOutputs(idAllocator, result, ImmutableSet.copyOf(filterNode.getOutputSymbols())).orElse(result));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return tryDecorrelation(node, context).orElse(visitPlan(node, context));
        }
    }
}
