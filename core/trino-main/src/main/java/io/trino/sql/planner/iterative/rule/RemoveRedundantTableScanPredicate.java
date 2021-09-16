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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.trino.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.createResultingPredicate;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

public class RemoveRedundantTableScanPredicate
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<FilterNode> PATTERN =
            filter().with(source().matching(
                    tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;
    private final DomainTranslator domainTranslator;
    private final TypeAnalyzer typeAnalyzer;
    private final TypeOperators typeOperators;

    public RemoveRedundantTableScanPredicate(Metadata metadata, TypeOperators typeOperators, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.domainTranslator = new DomainTranslator(metadata);
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
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

        PlanNode rewritten = removeRedundantTableScanPredicate(
                tableScan,
                filterNode.getPredicate(),
                context.getSession(),
                context.getSymbolAllocator(),
                context.getIdAllocator(),
                typeOperators);

        if (rewritten instanceof FilterNode
                && Objects.equals(((FilterNode) rewritten).getPredicate(), filterNode.getPredicate())) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewritten);
    }

    private PlanNode removeRedundantTableScanPredicate(
            TableScanNode node,
            Expression predicate,
            Session session,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            TypeOperators typeOperators)
    {
        Expression deterministicPredicate = filterDeterministicConjuncts(metadata, predicate);
        Expression nonDeterministicPredicate = filterNonDeterministicConjuncts(metadata, predicate);

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                typeOperators,
                session,
                deterministicPredicate,
                symbolAllocator.getTypes());

        TupleDomain<ColumnHandle> predicateDomain = decomposedPredicate.getTupleDomain()
                .transformKeys(node.getAssignments()::get);

        if (predicateDomain.isNone()) {
            // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
            // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
            // to turn the subtree into a Values node
            return new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of());
        }

        if (node.getEnforcedConstraint().isNone()) {
            // table scans with none domain should be converted to ValuesNode
            return new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of());
        }

        Map<ColumnHandle, Domain> enforcedColumnDomains = node.getEnforcedConstraint().getDomains().orElseThrow(); // is not NONE

        TupleDomain<ColumnHandle> unenforcedDomain = predicateDomain.transformDomains((columnHandle, predicateColumnDomain) -> {
            Type type = predicateColumnDomain.getType();
            Domain enforcedColumnDomain = Optional.ofNullable(enforcedColumnDomains.get(columnHandle)).orElseGet(() -> Domain.all(type));
            if (predicateColumnDomain.contains(enforcedColumnDomain)) {
                // full enforced
                return Domain.all(type);
            }
            return predicateColumnDomain.intersect(enforcedColumnDomain);
        });

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
        Expression resultingPredicate = createResultingPredicate(
                metadata,
                session,
                symbolAllocator,
                typeAnalyzer,
                domainTranslator.toPredicate(unenforcedDomain.transformKeys(assignments::get)),
                nonDeterministicPredicate,
                decomposedPredicate.getRemainingExpression());

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return new FilterNode(idAllocator.getNextId(), node, resultingPredicate);
        }

        return node;
    }
}
