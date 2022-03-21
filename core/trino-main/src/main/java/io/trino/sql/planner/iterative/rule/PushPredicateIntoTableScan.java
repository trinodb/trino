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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.LayoutConstraintEvaluator;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PushPredicateIntoTableScan
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public PushPredicateIntoTableScan(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
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
                false,
                context.getSession(),
                context.getSymbolAllocator(),
                plannerContext,
                typeAnalyzer,
                context.getStatsProvider(),
                new DomainTranslator(plannerContext));

        if (rewritten.isEmpty() || arePlansSame(filterNode, tableScan, rewritten.get())) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewritten.get());
    }

    private boolean arePlansSame(FilterNode filter, TableScanNode tableScan, PlanNode rewritten)
    {
        if (!(rewritten instanceof FilterNode)) {
            return false;
        }

        FilterNode rewrittenFilter = (FilterNode) rewritten;
        if (!Objects.equals(filter.getPredicate(), rewrittenFilter.getPredicate())) {
            return false;
        }

        if (!(rewrittenFilter.getSource() instanceof TableScanNode)) {
            return false;
        }

        TableScanNode rewrittenTableScan = (TableScanNode) rewrittenFilter.getSource();

        return Objects.equals(tableScan.getEnforcedConstraint(), rewrittenTableScan.getEnforcedConstraint()) &&
                Objects.equals(tableScan.getTable(), rewrittenTableScan.getTable());
    }

    public static Optional<PlanNode> pushFilterIntoTableScan(
            FilterNode filterNode,
            TableScanNode node,
            boolean pruneWithPredicateExpression,
            Session session,
            SymbolAllocator symbolAllocator,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            StatsProvider statsProvider,
            DomainTranslator domainTranslator)
    {
        if (!isAllowPushdownIntoConnectors(session)) {
            return Optional.empty();
        }

        SplitExpression splitExpression = splitExpression(plannerContext, filterNode.getPredicate());

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                splitExpression.getDeterministicPredicate(),
                symbolAllocator.getTypes());

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transformKeys(node.getAssignments()::get)
                .intersect(node.getEnforcedConstraint());

        ConnectorExpressionTranslation expressionTranslation = translateConjunctsToConnectorExpression(
                session,
                plannerContext,
                typeAnalyzer,
                symbolAllocator.getTypes(),
                decomposedPredicate.getRemainingExpression());
        Map<String, ColumnHandle> connectorExpressionAssignments = node.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint constraint;
        // use evaluator only when there is some predicate which could not be translated into tuple domain
        if (pruneWithPredicateExpression && !TRUE_LITERAL.equals(decomposedPredicate.getRemainingExpression())) {
            LayoutConstraintEvaluator evaluator = new LayoutConstraintEvaluator(
                    plannerContext,
                    typeAnalyzer,
                    session,
                    symbolAllocator.getTypes(),
                    node.getAssignments(),
                    combineConjuncts(
                            plannerContext.getMetadata(),
                            splitExpression.getDeterministicPredicate(),
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(session, newDomain.simplify().transformKeys(assignments::get))));
            constraint = new Constraint(newDomain, expressionTranslation.getConnectorExpression(), connectorExpressionAssignments, evaluator::isCandidate, evaluator.getArguments());
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint(newDomain, expressionTranslation.getConnectorExpression(), connectorExpressionAssignments);
        }

        // check if new domain is wider than domain already provided by table scan
        if (constraint.predicate().isEmpty() &&
                // TODO do we need to track enforced ConnectorExpression in TableScanNode?
                TRUE.equals(expressionTranslation.getConnectorExpression()) &&
                newDomain.contains(node.getEnforcedConstraint())) {
            Expression resultingPredicate = createResultingPredicate(
                    plannerContext,
                    session,
                    symbolAllocator,
                    typeAnalyzer,
                    splitExpression.getDynamicFilter(),
                    TRUE_LITERAL,
                    splitExpression.getNonDeterministicPredicate(),
                    decomposedPredicate.getRemainingExpression());

            if (!TRUE_LITERAL.equals(resultingPredicate)) {
                return Optional.of(new FilterNode(filterNode.getId(), node, resultingPredicate));
            }

            return Optional.of(node);
        }

        if (newDomain.isNone()) {
            // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
            // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
            // to turn the subtree into a Values node
            return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        Set<ColumnHandle> remainingPredicateColumns = Stream.concat(
                SymbolsExtractor.extractUnique(splitExpression.getNonDeterministicPredicate()).stream(),
                SymbolsExtractor.extractUnique(expressionTranslation.getRemainingExpression()).stream())
                .map(symbol -> node.getAssignments().get(symbol))
                .collect(Collectors.toSet());
        Optional<ConstraintApplicationResult<TableHandle>> result = plannerContext.getMetadata().applyFilter(session, node.getTable(), constraint, remainingPredicateColumns);

        if (result.isEmpty()) {
            return Optional.empty();
        }

        TableHandle newTable = result.get().getHandle();

        TableProperties newTableProperties = plannerContext.getMetadata().getTableProperties(session, newTable);
        Optional<TablePartitioning> newTablePartitioning = newTableProperties.getTablePartitioning();
        if (newTableProperties.getPredicate().isNone()) {
            return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
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
        if (remainingConnectorExpression.isEmpty() || remainingConnectorExpression.get().equals(expressionTranslation.getConnectorExpression())) {
            remainingDecomposedPredicate = decomposedPredicate.getRemainingExpression();
        }
        else {
            Map<String, Symbol> variableMappings = assignments.values().stream()
                    .collect(toImmutableMap(Symbol::getName, Function.identity()));
            Expression translatedExpression = ConnectorExpressionTranslator.translate(session, remainingConnectorExpression.get(), plannerContext, variableMappings, new LiteralEncoder(plannerContext));
            remainingDecomposedPredicate = combineConjuncts(plannerContext.getMetadata(), translatedExpression, expressionTranslation.getRemainingExpression());
        }

        Expression resultingPredicate = createResultingPredicate(
                plannerContext,
                session,
                symbolAllocator,
                typeAnalyzer,
                splitExpression.getDynamicFilter(),
                domainTranslator.toPredicate(session, remainingFilter.transformKeys(assignments::get)),
                splitExpression.getNonDeterministicPredicate(),
                remainingDecomposedPredicate);

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
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

    private static SplitExpression splitExpression(PlannerContext plannerContext, Expression predicate)
    {
        Metadata metadata = plannerContext.getMetadata();

        List<Expression> dynamicFilters = new ArrayList<>();
        List<Expression> deterministicPredicates = new ArrayList<>();
        List<Expression> nonDeterministicPredicate = new ArrayList<>();

        for (Expression conjunct : extractConjuncts(predicate)) {
            if (isDynamicFilter(conjunct)) {
                // dynamic filters have no meaning for connectors, so don't pass them
                dynamicFilters.add(conjunct);
            }
            else {
                if (isDeterministic(conjunct, metadata)) {
                    deterministicPredicates.add(conjunct);
                }
                else {
                    // don't include non-deterministic predicates
                    nonDeterministicPredicate.add(conjunct);
                }
            }
        }

        return new SplitExpression(
                combineConjuncts(metadata, dynamicFilters),
                combineConjuncts(metadata, deterministicPredicates),
                combineConjuncts(metadata, nonDeterministicPredicate));
    }

    private static ConnectorExpressionTranslation translateConjunctsToConnectorExpression(
            Session session,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            Expression expression)
    {
        Map<NodeRef<Expression>, Type> remainingExpressionTypes = typeAnalyzer.getTypes(session, types, expression);
        ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator translator = new ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator(
                session,
                remainingExpressionTypes,
                plannerContext);

        List<Expression> conjuncts = extractConjuncts(expression);
        List<Expression> remaining = new ArrayList<>();
        List<ConnectorExpression> converted = new ArrayList<>(conjuncts.size());
        for (Expression conjunct : conjuncts) {
            Optional<ConnectorExpression> connectorExpression = translator.process(conjunct);
            if (connectorExpression.isPresent()) {
                converted.add(connectorExpression.get());
            }
            else {
                remaining.add(conjunct);
            }
        }
        return new ConnectorExpressionTranslation(
                combineConjuncts(plannerContext.getMetadata(), remaining),
                ConnectorExpressions.and(converted));
    }

    static Expression createResultingPredicate(
            PlannerContext plannerContext,
            Session session,
            SymbolAllocator symbolAllocator,
            TypeAnalyzer typeAnalyzer,
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
        Expression expression = combineConjuncts(plannerContext.getMetadata(), dynamicFilter, unenforcedConstraints, nonDeterministicPredicate, remainingDecomposedPredicate);

        // Make sure we produce an expression whose terms are consistent with the canonical form used in other optimizations
        // Otherwise, we'll end up ping-ponging among rules
        expression = SimplifyExpressions.rewrite(expression, session, symbolAllocator, plannerContext, typeAnalyzer);

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
        checkArgument(!unenforced.isNone());

        Map<ColumnHandle, Domain> predicateDomains = predicate.getDomains().get();
        Map<ColumnHandle, Domain> unenforcedDomains = unenforced.getDomains().get();
        ImmutableMap.Builder<ColumnHandle, Domain> enforcedDomainsBuilder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : predicateDomains.entrySet()) {
            ColumnHandle predicateColumnHandle = entry.getKey();
            if (unenforcedDomains.containsKey(predicateColumnHandle)) {
                checkArgument(
                        entry.getValue().equals(unenforcedDomains.get(predicateColumnHandle)),
                        "Enforced tuple domain cannot be determined. The connector is expected to enforce the respective domain entirely on none, some, or all of the column.");
            }
            else {
                enforcedDomainsBuilder.put(predicateColumnHandle, entry.getValue());
            }
        }
        Map<ColumnHandle, Domain> enforcedDomains = enforcedDomainsBuilder.buildOrThrow();
        checkArgument(
                enforcedDomains.size() + unenforcedDomains.size() == predicateDomains.size(),
                "Enforced tuple domain cannot be determined. Connector returned an unenforced TupleDomain that contains columns not in predicate.");
        return TupleDomain.withColumnDomains(enforcedDomains);
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

    private static class ConnectorExpressionTranslation
    {
        private final Expression remainingExpression;
        private final ConnectorExpression connectorExpression;

        public ConnectorExpressionTranslation(Expression remainingExpression, ConnectorExpression connectorExpression)
        {
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
            this.connectorExpression = requireNonNull(connectorExpression, "connectorExpression is null");
        }

        public Expression getRemainingExpression()
        {
            return remainingExpression;
        }

        public ConnectorExpression getConnectorExpression()
        {
            return connectorExpression;
        }
    }
}
