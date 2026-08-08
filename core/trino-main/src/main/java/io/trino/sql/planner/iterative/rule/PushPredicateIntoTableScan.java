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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.ConnectorExpressionTranslator.ConnectorExpressionTranslation;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.EngineExpressions;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PushPredicateIntoTableScan
{
    private final PlannerContext plannerContext;
    private final boolean pruneWithPredicateExpression;

    public PushPredicateIntoTableScan(PlannerContext plannerContext, boolean pruneWithPredicateExpression)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.pruneWithPredicateExpression = pruneWithPredicateExpression;
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new PushPredicateIntoTableScanWithoutProject(plannerContext, pruneWithPredicateExpression),
                new PushPredicateIntoTableScanWithProject(plannerContext));
    }

    @VisibleForTesting
    public static final class PushPredicateIntoTableScanWithoutProject
            implements Rule<FilterNode>
    {
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

        private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
                tableScan().capturedAs(TABLE_SCAN)));

        private final PlannerContext plannerContext;
        private final boolean pruneWithPredicateExpression;

        public PushPredicateIntoTableScanWithoutProject(PlannerContext plannerContext, boolean pruneWithPredicateExpression)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.pruneWithPredicateExpression = pruneWithPredicateExpression;
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isAllowPushdownIntoConnectors(session);
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            TableScanNode tableScan = captures.get(TABLE_SCAN);

            Optional<PlanNode> rewritten = pushFilterIntoTableScan(
                    filterNode,
                    tableScan,
                    pruneWithPredicateExpression,
                    context.getSession(),
                    plannerContext,
                    context.getStatsProvider(),
                    context.getSymbolAllocator());

            if (rewritten.isEmpty() || arePlansSame(filterNode, tableScan, rewritten.get())) {
                return Result.empty();
            }

            return Result.ofPlanNode(rewritten.get());
        }

        private boolean arePlansSame(FilterNode filter, TableScanNode tableScan, PlanNode rewritten)
        {
            if (!(rewritten instanceof FilterNode rewrittenFilter)) {
                return false;
            }

            if (!Objects.equals(filter.getPredicate(), rewrittenFilter.getPredicate())) {
                return false;
            }

            if (!(rewrittenFilter.getSource() instanceof TableScanNode rewrittenTableScan)) {
                return false;
            }

            return Objects.equals(tableScan.getEnforcedConstraint(), rewrittenTableScan.getEnforcedConstraint()) &&
                    Objects.equals(tableScan.getTable(), rewrittenTableScan.getTable());
        }

        @VisibleForTesting
        public boolean getPruneWithPredicateExpression()
        {
            return pruneWithPredicateExpression;
        }
    }

    /**
     * Derives a connector constraint from a filter that sits above a projection, without moving the filter.
     * <p>
     * Predicates over projected expressions often cannot be pushed below the projection by
     * {@link io.trino.sql.planner.optimizations.PredicatePushDown}: a conjunct referencing a projected
     * complex expression more than once (e.g. {@code expr BETWEEN a AND b OR expr BETWEEN c AND d}
     * distributed into disjunctive conjuncts) is not an inlining candidate, because inlining would
     * duplicate the computation. The predicate is then stranded above the projection and the table scan
     * receives no constraint at all.
     * <p>
     * This rule inlines the projection's assignments into the predicate <b>only to derive a
     * {@link TupleDomain} constraint</b> for the connector. The inlined expression is never placed in the
     * plan or executed, so the cost concern behind the inlining restriction does not apply, and the
     * original filter and projection are left untouched. Since the inlined predicate is semantically
     * identical to the original filter over the projection's outputs, the extracted domain — a superset
     * of the rows it can match — is a valid scan constraint.
     */
    @VisibleForTesting
    public static final class PushPredicateIntoTableScanWithProject
            implements Rule<FilterNode>
    {
        private static final Capture<ProjectNode> PROJECT = newCapture();

        private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
                project().capturedAs(PROJECT)));

        private final PlannerContext plannerContext;

        public PushPredicateIntoTableScanWithProject(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isAllowPushdownIntoConnectors(session);
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            ProjectNode project = captures.get(PROJECT);
            Session session = context.getSession();

            PlanNode projectSource = context.getLookup().resolve(project.getSource());
            Optional<FilterNode> residualFilter = Optional.empty();
            TableScanNode tableScan;
            if (projectSource instanceof TableScanNode scanNode) {
                tableScan = scanNode;
            }
            else if (projectSource instanceof FilterNode filterBelowProject
                    && context.getLookup().resolve(filterBelowProject.getSource()) instanceof TableScanNode scanNode) {
                // A residual filter the connector could not enforce (e.g. a view's WHERE clause) commonly
                // remains between the projection and the scan. The constraint derived from the outer
                // filter's conjuncts is a valid superset of the rows they can match regardless of the
                // rows the residual filter removes, so it can still be pushed to the scan.
                residualFilter = Optional.of(filterBelowProject);
                tableScan = scanNode;
            }
            else {
                return Result.empty();
            }

            // Substituting an assignment into the predicate is semantics-preserving only for
            // deterministic assignments; conjuncts must also be deterministic and dynamic filters
            // are meaningless to connectors.
            List<Expression> candidateConjuncts = extractConjuncts(filterNode.getPredicate()).stream()
                    .filter(conjunct -> !isDynamicFilter(conjunct) && isDeterministic(conjunct))
                    // Conjuncts that PredicatePushDown can inline below the projection must be left to the
                    // regular pushdown: it both enforces them in the connector and removes them from the
                    // filter. Deriving the constraint here first would leave such a conjunct in the filter
                    // permanently, because the later applyFilter reports no change for an already-enforced
                    // domain. This rule only handles what inlining cannot: conjuncts referencing a projected
                    // complex expression more than once.
                    .filter(conjunct -> !isInliningCandidate(conjunct, project))
                    .filter(conjunct -> SymbolsExtractor.extractUnique(conjunct).stream()
                            .allMatch(symbol -> {
                                Expression assignment = project.getAssignments().get(symbol);
                                return assignment != null && isDeterministic(assignment);
                            }))
                    .collect(toImmutableList());
            if (candidateConjuncts.isEmpty()) {
                return Result.empty();
            }

            Expression inlined = inlineSymbols(project.getAssignments()::get, combineConjuncts(candidateConjuncts));

            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(plannerContext, session, inlined);

            TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                    .transformKeys(tableScan.getAssignments()::get)
                    .intersect(tableScan.getEnforcedConstraint());

            if (newDomain.contains(tableScan.getEnforcedConstraint())) {
                // no narrowing over what the scan already enforces
                return Result.empty();
            }

            if (newDomain.isNone()) {
                // the inlined predicate is equivalent to the original filter over the projection,
                // so an unsatisfiable domain means the filter matches no rows
                return Result.ofPlanNode(new ValuesNode(filterNode.getId(), filterNode.getOutputSymbols()));
            }

            Optional<ConstraintApplicationResult<TableHandle>> result =
                    plannerContext.getMetadata().applyFilter(session, tableScan.getTable(), new Constraint(newDomain));
            if (result.isEmpty()) {
                return Result.empty();
            }

            TableHandle newTable = result.get().getHandle();
            TableProperties newTableProperties = plannerContext.getMetadata().getTableProperties(session, newTable);
            if (newTableProperties.getPredicate().isNone()) {
                return Result.ofPlanNode(new ValuesNode(filterNode.getId(), filterNode.getOutputSymbols()));
            }

            TupleDomain<ColumnHandle> newEnforcedConstraint = computeEnforced(newDomain, result.get().getRemainingFilter());
            if (newTable.equals(tableScan.getTable()) && newEnforcedConstraint.equals(tableScan.getEnforcedConstraint())) {
                return Result.empty();
            }

            verifyTablePartitioning(session, plannerContext.getMetadata(), tableScan, newTableProperties.getTablePartitioning());

            TableScanNode newScan = new TableScanNode(
                    tableScan.getId(),
                    newTable,
                    tableScan.getOutputSymbols(),
                    tableScan.getAssignments(),
                    newEnforcedConstraint,
                    deriveTableStatisticsForPushdown(context.getStatsProvider(), session, result.get().isPrecalculateStatistics(), filterNode),
                    tableScan.isUpdateTarget(),
                    tableScan.getUseConnectorNodePartitioning());

            // The filter and projection are intentionally kept as-is: the constraint is only a superset
            // of the rows the filter matches, and row-level semantics (nulls, errors) must be preserved.
            PlanNode newSource = newScan;
            if (residualFilter.isPresent()) {
                newSource = residualFilter.get().replaceChildren(ImmutableList.of(newScan));
            }
            PlanNode newProject = project.replaceChildren(ImmutableList.of(newSource));
            return Result.ofPlanNode(filterNode.replaceChildren(ImmutableList.of(newProject)));
        }

        // Mirrors PredicatePushDown#isInliningCandidate
        private static boolean isInliningCandidate(Expression conjunct, ProjectNode project)
        {
            Set<Symbol> outputs = ImmutableSet.copyOf(project.getOutputSymbols());
            Map<Symbol, Long> dependencies = SymbolsExtractor.extractAll(conjunct).stream()
                    .filter(outputs::contains)
                    .collect(groupingBy(identity(), counting()));

            return dependencies.entrySet().stream()
                    .allMatch(entry -> entry.getValue() == 1
                            || project.getAssignments().get(entry.getKey()) instanceof io.trino.sql.ir.Constant
                            || project.getAssignments().get(entry.getKey()) instanceof Reference);
        }
    }

    public static Optional<PlanNode> pushFilterIntoTableScan(
            FilterNode filterNode,
            TableScanNode node,
            boolean pruneWithPredicateExpression,
            Session session,
            PlannerContext plannerContext,
            StatsProvider statsProvider,
            SymbolAllocator symbolAllocator)
    {
        if (!isAllowPushdownIntoConnectors(session)) {
            return Optional.empty();
        }

        SplitExpression splitExpression = splitExpression(filterNode.getPredicate());

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                splitExpression.getDeterministicPredicate());

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transformKeys(node.getAssignments()::get)
                .intersect(node.getEnforcedConstraint());

        Map<String, ColumnHandle> connectorExpressionAssignments = node.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().name(), Entry::getValue));
        ConnectorExpressionTranslation expressionTranslation = ConnectorExpressionTranslator.translateConjuncts(
                session,
                decomposedPredicate.getRemainingExpression(),
                connectorExpressionAssignments.keySet());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        ConnectorExpression connectorExpression = expressionTranslation.connectorExpression();

        Constraint constraint;
        // use engine expression only when there is some predicate which could not be translated into tuple domain
        if (pruneWithPredicateExpression && !Booleans.TRUE.equals(decomposedPredicate.getRemainingExpression())) {
            Expression predicate = combineConjuncts(
                    splitExpression.getDeterministicPredicate(),
                    // Simplify the tuple domain to avoid creating an expression with too many nodes,
                    // which would be expensive to evaluate in the call to isCandidate below.
                    new DomainTranslator(plannerContext.getMetadata()).toPredicate(newDomain.simplify().transformKeys(assignments::get)));
            ConnectorExpression expression = ConnectorExpressions.and(
                    connectorExpression,
                    EngineExpressions.buildEngineExpression(predicate, plannerContext.getExpressionCodec()));
            constraint = new Constraint(newDomain, expression, connectorExpressionAssignments);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint(newDomain, connectorExpression, connectorExpressionAssignments);
        }

        // check if new domain is wider than domain already provided by table scan
        // TODO do we need to track enforced ConnectorExpression in TableScanNode?
        if (Constant.TRUE.equals(constraint.getExpression()) && newDomain.contains(node.getEnforcedConstraint())) {
            Expression resultingPredicate = createResultingPredicate(
                    plannerContext,
                    session,
                    symbolAllocator,
                    splitExpression.getDynamicFilter(),
                    Booleans.TRUE,
                    splitExpression.getNonDeterministicPredicate(),
                    decomposedPredicate.getRemainingExpression());

            if (!Booleans.TRUE.equals(resultingPredicate)) {
                return Optional.of(new FilterNode(filterNode.getId(), node, resultingPredicate));
            }

            return Optional.of(node);
        }

        if (newDomain.isNone()) {
            // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
            // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
            // to turn the subtree into a Values node
            return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols()));
        }

        Optional<ConstraintApplicationResult<TableHandle>> result = plannerContext.getMetadata().applyFilter(session, node.getTable(), constraint);

        if (result.isEmpty()) {
            return Optional.empty();
        }

        TableHandle newTable = result.get().getHandle();

        TableProperties newTableProperties = plannerContext.getMetadata().getTableProperties(session, newTable);
        Optional<TablePartitioning> newTablePartitioning = newTableProperties.getTablePartitioning();
        if (newTableProperties.getPredicate().isNone()) {
            return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols()));
        }

        TupleDomain<ColumnHandle> remainingFilter = result.get().getRemainingFilter();
        Optional<ConnectorExpression> remainingConnectorExpression = result.get().getRemainingExpression();
        boolean precalculateStatistics = result.get().isPrecalculateStatistics();

        verifyTablePartitioning(session, plannerContext.getMetadata(), node, newTablePartitioning);

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                newTable,
                node.getOutputSymbols(),
                node.getAssignments(),
                computeEnforced(newDomain, remainingFilter),
                // TODO (https://github.com/trinodb/trino/issues/8144) distinguish between predicate pushed down and remaining
                deriveTableStatisticsForPushdown(statsProvider, session, precalculateStatistics, filterNode),
                node.isUpdateTarget(),
                node.getUseConnectorNodePartitioning());

        Expression remainingDecomposedPredicate;
        if (remainingConnectorExpression.isEmpty() || remainingConnectorExpression.get().equals(constraint.getExpression())) {
            remainingDecomposedPredicate = decomposedPredicate.getRemainingExpression();
        }
        else {
            Map<String, Symbol> variableMappings = assignments.values().stream()
                    .collect(toImmutableMap(Symbol::name, Function.identity()));
            // translate inlines the IR predicate wrapped by any $engine_expression the connector
            // echoed back, regardless of where it appears in the expression tree
            Expression translatedExpression = ConnectorExpressionTranslator.translate(session, remainingConnectorExpression.get(), plannerContext, variableMappings, symbolAllocator);
            translatedExpression = LambdaCaptureDesugaringRewriter.rewrite(translatedExpression, symbolAllocator);
            // ConnectorExpressionTranslator may or may not preserve optimized form of expressions during round-trip. Avoid potential optimizer loop
            // by ensuring expression is optimized.
            translatedExpression = plannerContext.getExpressionOptimizer().process(translatedExpression, session, symbolAllocator, ImmutableMap.of()).orElse(translatedExpression);
            remainingDecomposedPredicate = combineConjuncts(translatedExpression, expressionTranslation.remainingExpression());
        }

        Expression resultingPredicate = createResultingPredicate(
                plannerContext,
                session,
                symbolAllocator,
                splitExpression.getDynamicFilter(),
                new DomainTranslator(plannerContext.getMetadata()).toPredicate(remainingFilter.transformKeys(assignments::get)),
                splitExpression.getNonDeterministicPredicate(),
                remainingDecomposedPredicate);

        if (!Booleans.TRUE.equals(resultingPredicate)) {
            return Optional.of(new FilterNode(filterNode.getId(), tableScan, resultingPredicate));
        }

        return Optional.of(tableScan);
    }

    // PushPredicateIntoTableScan might be executed after AddExchanges and DetermineTableScanNodePartitioning.
    // In that case, table scan node partitioning (if present) was used to fragment plan with ExchangeNodes.
    // Therefore table scan node partitioning should not change after AddExchanges is executed since it would
    // make plan with ExchangeNodes invalid.
    private static void verifyTablePartitioning(
            Session session,
            Metadata metadata,
            TableScanNode oldTableScan,
            Optional<TablePartitioning> newTablePartitioning)
    {
        if (oldTableScan.getUseConnectorNodePartitioning().isEmpty()) {
            return;
        }

        Optional<TablePartitioning> oldTablePartitioning = metadata.getTableProperties(session, oldTableScan.getTable()).getTablePartitioning();
        verify(newTablePartitioning.equals(oldTablePartitioning), "Partitioning must not change after predicate is pushed down");
    }

    private static SplitExpression splitExpression(Expression predicate)
    {
        List<Expression> dynamicFilters = new ArrayList<>();
        List<Expression> deterministicPredicates = new ArrayList<>();
        List<Expression> nonDeterministicPredicate = new ArrayList<>();

        for (Expression conjunct : extractConjuncts(predicate)) {
            if (isDynamicFilter(conjunct)) {
                // dynamic filters have no meaning for connectors, so don't pass them
                dynamicFilters.add(conjunct);
            }
            else if (isDeterministic(conjunct)) {
                deterministicPredicates.add(conjunct);
            }
            else {
                // don't include non-deterministic predicates
                nonDeterministicPredicate.add(conjunct);
            }
        }

        return new SplitExpression(
                combineConjuncts(dynamicFilters),
                combineConjuncts(deterministicPredicates),
                combineConjuncts(nonDeterministicPredicate));
    }

    static Expression createResultingPredicate(
            PlannerContext plannerContext,
            Session session,
            SymbolAllocator symbolAllocator,
            Expression dynamicFilter,
            Expression unenforcedConstraints,
            Expression nonDeterministicPredicate,
            Expression remainingDecomposedPredicate)
    {
        // The order of the arguments to combineConjuncts matters:
        // * Dynamic filters go first because they cannot fail,
        // * Unenforced constraints go next because they can only be simple column references,
        //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
        // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
        //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
        // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
        //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
        //   to failures of previously successful queries.
        Expression expression = combineConjuncts(dynamicFilter, unenforcedConstraints, nonDeterministicPredicate, remainingDecomposedPredicate);

        // Make sure we produce an expression whose terms are consistent with the canonical form used in other optimizations
        // Otherwise, we'll end up ping-ponging among rules
        expression = SimplifyExpressions.rewrite(expression, session, plannerContext.getMetadata(), symbolAllocator, plannerContext.getExpressionOptimizer());

        return expression;
    }

    public static TupleDomain<ColumnHandle> computeEnforced(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> unenforced)
    {
        // The engine requested the connector to apply a filter with a non-none TupleDomain.
        // A TupleDomain is effectively a list of column-Domain pairs.
        // The connector is expected enforce the respective domain entirely on none, some, or all of the columns.
        // 1. When the connector could enforce none of the domains, the unenforced would be equal to predicate;
        // 2. When the connector could enforce some of the domains, the unenforced would contain a subset of the column-Domain pairs;
        // 3. When the connector could enforce all of the domains, the unenforced would be TupleDomain.all().

        // In all 3 cases shown above, the unenforced is not TupleDomain.none().
        checkArgument(!unenforced.isNone(), "Unexpected unenforced none tuple domain");

        Map<ColumnHandle, Domain> predicateDomains = predicate.getDomains().get();
        Map<ColumnHandle, Domain> unenforcedDomains = unenforced.getDomains().get();
        ImmutableMap.Builder<ColumnHandle, Domain> enforcedDomainsBuilder = ImmutableMap.builder();
        for (Entry<ColumnHandle, Domain> entry : predicateDomains.entrySet()) {
            ColumnHandle predicateColumnHandle = entry.getKey();
            Domain predicateDomain = entry.getValue();
            if (unenforcedDomains.containsKey(predicateColumnHandle)) {
                Domain unenforcedDomain = unenforcedDomains.get(predicateColumnHandle);
                checkArgument(
                        predicateDomain.contains(unenforcedDomain),
                        "Unexpected unenforced domain %s on column %s. Expected all, none, or a domain equal to or narrower than %s",
                        unenforcedDomain,
                        predicateColumnHandle,
                        predicateDomain);
            }
            else {
                enforcedDomainsBuilder.put(predicateColumnHandle, predicateDomain);
            }
        }
        return TupleDomain.withColumnDomains(enforcedDomainsBuilder.buildOrThrow());
    }

    private static class SplitExpression
    {
        private final Expression dynamicFilter;
        private final Expression deterministicPredicate;
        private final Expression nonDeterministicPredicate;

        public SplitExpression(Expression dynamicFilter, Expression deterministicPredicate, Expression nonDeterministicPredicate)
        {
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.deterministicPredicate = requireNonNull(deterministicPredicate, "deterministicPredicate is null");
            this.nonDeterministicPredicate = requireNonNull(nonDeterministicPredicate, "nonDeterministicPredicate is null");
        }

        public Expression getDynamicFilter()
        {
            return dynamicFilter;
        }

        public Expression getDeterministicPredicate()
        {
            return deterministicPredicate;
        }

        public Expression getNonDeterministicPredicate()
        {
            return nonDeterministicPredicate;
        }
    }
}
