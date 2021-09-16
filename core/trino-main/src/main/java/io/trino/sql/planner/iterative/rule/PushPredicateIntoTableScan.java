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
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayoutResult;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.LookupSymbolResolver;
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
import io.trino.sql.tree.NullLiteral;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.TableLayoutResult.computeEnforced;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.trino.sql.ExpressionUtils.filterNonDeterministicConjuncts;
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

    private final Metadata metadata;
    private final TypeOperators typeOperators;
    private final TypeAnalyzer typeAnalyzer;
    private final DomainTranslator domainTranslator;

    public PushPredicateIntoTableScan(Metadata metadata, TypeOperators typeOperators, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.domainTranslator = new DomainTranslator(metadata);
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
                metadata,
                typeOperators,
                typeAnalyzer,
                context.getStatsProvider(),
                domainTranslator);

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
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            StatsProvider statsProvider,
            DomainTranslator domainTranslator)
    {
        if (!isAllowPushdownIntoConnectors(session)) {
            return Optional.empty();
        }

        Expression predicate = filterNode.getPredicate();

        // don't include non-deterministic predicates
        Expression deterministicPredicate = filterDeterministicConjuncts(metadata, predicate);
        Expression nonDeterministicPredicate = filterNonDeterministicConjuncts(metadata, predicate);

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                typeOperators,
                session,
                deterministicPredicate,
                symbolAllocator.getTypes());

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transformKeys(node.getAssignments()::get)
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint constraint;
        // use evaluator only when there is some predicate which could not be translated into tuple domain
        if (pruneWithPredicateExpression && !TRUE_LITERAL.equals(decomposedPredicate.getRemainingExpression())) {
            LayoutConstraintEvaluator evaluator = new LayoutConstraintEvaluator(
                    metadata,
                    typeAnalyzer,
                    session,
                    symbolAllocator.getTypes(),
                    node.getAssignments(),
                    combineConjuncts(
                            metadata,
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transformKeys(assignments::get))));
            constraint = new Constraint(newDomain, evaluator::isCandidate, evaluator.getArguments());
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint(newDomain);
        }

        TableHandle newTable;
        Optional<TablePartitioning> newTablePartitioning;
        TupleDomain<ColumnHandle> remainingFilter;
        boolean precalculateStatistics;
        if (!metadata.usesLegacyTableLayouts(session, node.getTable())) {
            // check if new domain is wider than domain already provided by table scan
            if (constraint.predicate().isEmpty() && newDomain.contains(node.getEnforcedConstraint())) {
                Expression resultingPredicate = createResultingPredicate(
                        metadata,
                        session,
                        symbolAllocator,
                        typeAnalyzer,
                        TRUE_LITERAL,
                        nonDeterministicPredicate,
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

            Optional<ConstraintApplicationResult<TableHandle>> result = metadata.applyFilter(session, node.getTable(), constraint);

            if (result.isEmpty()) {
                return Optional.empty();
            }

            newTable = result.get().getHandle();

            TableProperties newTableProperties = metadata.getTableProperties(session, newTable);
            newTablePartitioning = newTableProperties.getTablePartitioning();
            if (newTableProperties.getPredicate().isNone()) {
                return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            remainingFilter = result.get().getRemainingFilter();
            precalculateStatistics = result.get().isPrecalculateStatistics();
        }
        else {
            Optional<TableLayoutResult> layout = metadata.getLayout(
                    session,
                    node.getTable(),
                    constraint,
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet())));

            if (layout.isEmpty() || layout.get().getTableProperties().getPredicate().isNone()) {
                return Optional.of(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            newTable = layout.get().getNewTableHandle();
            newTablePartitioning = layout.get().getTableProperties().getTablePartitioning();
            remainingFilter = layout.get().getUnenforcedConstraint();
            precalculateStatistics = false;
        }

        verifyTablePartitioning(session, metadata, node, newTablePartitioning);

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

        Expression resultingPredicate = createResultingPredicate(
                metadata,
                session,
                symbolAllocator,
                typeAnalyzer,
                domainTranslator.toPredicate(remainingFilter.transformKeys(assignments::get)),
                nonDeterministicPredicate,
                decomposedPredicate.getRemainingExpression());

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

    static Expression createResultingPredicate(
            Metadata metadata,
            Session session,
            SymbolAllocator symbolAllocator,
            TypeAnalyzer typeAnalyzer,
            Expression unenforcedConstraints,
            Expression nonDeterministicPredicate,
            Expression remainingDecomposedPredicate)
    {
        // The order of the arguments to combineConjuncts matters:
        // * Unenforced constraints go first because they can only be simple column references,
        //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
        // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
        //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
        // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
        //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
        //   to failures of previously successful queries.
        Expression expression = combineConjuncts(metadata, unenforcedConstraints, nonDeterministicPredicate, remainingDecomposedPredicate);

        // Make sure we produce an expression whose terms are consistent with the canonical form used in other optimizations
        // Otherwise, we'll end up ping-ponging among rules
        expression = SimplifyExpressions.rewrite(expression, session, symbolAllocator, metadata, new LiteralEncoder(metadata), typeAnalyzer);

        return expression;
    }

    private static class LayoutConstraintEvaluator
    {
        private final Map<Symbol, ColumnHandle> assignments;
        private final ExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;

        public LayoutConstraintEvaluator(Metadata metadata, TypeAnalyzer typeAnalyzer, Session session, TypeProvider types, Map<Symbol, ColumnHandle> assignments, Expression expression)
        {
            this.assignments = assignments;

            evaluator = new ExpressionInterpreter(expression, metadata, session, typeAnalyzer.getTypes(session, types, expression));
            arguments = SymbolsExtractor.extractUnique(expression).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        public Set<ColumnHandle> getArguments()
        {
            return arguments;
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }
            LookupSymbolResolver inputs = new LookupSymbolResolver(assignments, bindings);

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = TryFunction.evaluate(() -> evaluator.optimize(inputs), true);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !(Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral);
        }
    }
}
