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
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.DomainTranslator;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.Expression;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.prestosql.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static io.prestosql.sql.planner.iterative.rule.PushPredicateIntoTableScan.createResultingPredicate;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
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

    public RemoveRedundantTableScanPredicate(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.domainTranslator = new DomainTranslator(metadata);
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
                context.getSymbolAllocator().getTypes(),
                context.getIdAllocator());

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
            TypeProvider types,
            PlanNodeIdAllocator idAllocator)
    {
        Expression deterministicPredicate = filterDeterministicConjuncts(metadata, predicate);
        Expression nonDeterministicPredicate = filterNonDeterministicConjuncts(metadata, predicate);

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                session,
                deterministicPredicate,
                types);

        TupleDomain<ColumnHandle> predicateDomain = decomposedPredicate.getTupleDomain()
                .transform(node.getAssignments()::get);

        TupleDomain<ColumnHandle> unenforcedDomain;
        if (predicateDomain.getDomains().isPresent()) {
            Map<ColumnHandle, Domain> predicateColumnDomains = predicateDomain.getDomains().get();

            // table scans with none domain should be converted to ValuesNode
            checkState(node.getEnforcedConstraint().getDomains().isPresent());
            Map<ColumnHandle, Domain> enforcedColumnDomains = node.getEnforcedConstraint().getDomains().get();

            ImmutableMap.Builder<ColumnHandle, Domain> unenforcedColumnDomains = ImmutableMap.builder();
            for (Map.Entry<ColumnHandle, Domain> entry : predicateColumnDomains.entrySet()) {
                ColumnHandle columnHandle = entry.getKey();
                Domain predicateColumnDomain = entry.getValue();
                Domain enforcedColumnDomain = enforcedColumnDomains.getOrDefault(columnHandle, Domain.all(predicateColumnDomain.getType()));
                predicateColumnDomain = predicateColumnDomain.intersect(enforcedColumnDomain);
                if (!predicateColumnDomain.contains(enforcedColumnDomain)) {
                    unenforcedColumnDomains.put(columnHandle, predicateColumnDomain);
                }
            }

            unenforcedDomain = TupleDomain.withColumnDomains(unenforcedColumnDomains.build());
        }
        else {
            // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
            // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
            // to turn the subtree into a Values node
            return new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of());
        }

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
        Expression resultingPredicate = createResultingPredicate(
                metadata,
                domainTranslator.toPredicate(unenforcedDomain.transform(assignments::get)),
                nonDeterministicPredicate,
                decomposedPredicate.getRemainingExpression());

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return new FilterNode(idAllocator.getNextId(), node, resultingPredicate);
        }

        return node;
    }
}
