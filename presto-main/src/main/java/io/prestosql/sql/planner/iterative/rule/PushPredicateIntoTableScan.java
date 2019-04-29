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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableLayoutResult;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.DomainTranslator;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.LookupSymbolResolver;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NullLiteral;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.metadata.TableLayoutResult.computeEnforced;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.prestosql.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
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
    private final TypeAnalyzer typeAnalyzer;
    private final DomainTranslator domainTranslator;

    public PushPredicateIntoTableScan(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.domainTranslator = new DomainTranslator(new LiteralEncoder(metadata.getBlockEncodingSerde()));
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        Optional<PlanNode> rewritten = pushFilterIntoTableScan(
                tableScan,
                filterNode.getPredicate(),
                false,
                context.getSession(),
                context.getSymbolAllocator().getTypes(),
                context.getIdAllocator(),
                metadata,
                typeAnalyzer,
                domainTranslator);

        if (!rewritten.isPresent() || arePlansSame(filterNode, tableScan, rewritten.get())) {
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

        return Objects.equals(tableScan.getEnforcedConstraint(), rewrittenTableScan.getEnforcedConstraint());
    }

    public static Optional<PlanNode> pushFilterIntoTableScan(
            TableScanNode node,
            Expression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            TypeProvider types,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            DomainTranslator domainTranslator)
    {
        // don't include non-deterministic predicates
        Expression deterministicPredicate = filterDeterministicConjuncts(predicate);

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                session,
                deterministicPredicate,
                types);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(node.getAssignments()::get)
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint constraint;
        if (pruneWithPredicateExpression) {
            LayoutConstraintEvaluator evaluator = new LayoutConstraintEvaluator(
                    metadata,
                    typeAnalyzer,
                    session,
                    types,
                    node.getAssignments(),
                    combineConjuncts(
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transform(assignments::get))));
            constraint = new Constraint(newDomain, evaluator::isCandidate);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint(newDomain);
        }

        TableHandle newTable;
        TupleDomain<ColumnHandle> remainingFilter;
        if (!metadata.usesLegacyTableLayouts(session, node.getTable())) {
            if (newDomain.isNone()) {
                // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
                // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
                // to turn the subtree into a Values node
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            Optional<ConstraintApplicationResult<TableHandle>> result = metadata.applyFilter(session, node.getTable(), constraint);

            if (!result.isPresent()) {
                return Optional.empty();
            }

            newTable = result.get().getHandle();
            remainingFilter = result.get().getRemainingFilter();
        }
        else {
            Optional<TableLayoutResult> layout = metadata.getLayout(
                    session,
                    node.getTable(),
                    constraint,
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet())));

            if (!layout.isPresent() || layout.get().getTableProperties().getPredicate().isNone()) {
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            newTable = layout.get().getNewTableHandle();
            remainingFilter = layout.get().getUnenforcedConstraint();
        }

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                newTable,
                node.getOutputSymbols(),
                node.getAssignments(),
                computeEnforced(newDomain, remainingFilter));

        // The order of the arguments to combineConjuncts matters:
        // * Unenforced constraints go first because they can only be simple column references,
        //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
        // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
        //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
        // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
        //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
        //   to failures of previously successful queries.
        Expression resultingPredicate = combineConjuncts(
                domainTranslator.toPredicate(remainingFilter.transform(assignments::get)),
                filterNonDeterministicConjuncts(predicate),
                decomposedPredicate.getRemainingExpression());

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return Optional.of(new FilterNode(idAllocator.getNextId(), tableScan, resultingPredicate));
        }

        return Optional.of(tableScan);
    }

    private static class LayoutConstraintEvaluator
    {
        private final Map<Symbol, ColumnHandle> assignments;
        private final ExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;

        public LayoutConstraintEvaluator(Metadata metadata, TypeAnalyzer typeAnalyzer, Session session, TypeProvider types, Map<Symbol, ColumnHandle> assignments, Expression expression)
        {
            this.assignments = assignments;

            evaluator = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, typeAnalyzer.getTypes(session, types, expression));
            arguments = SymbolsExtractor.extractUnique(expression).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
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
            if (Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral) {
                return false;
            }

            return true;
        }
    }
}
