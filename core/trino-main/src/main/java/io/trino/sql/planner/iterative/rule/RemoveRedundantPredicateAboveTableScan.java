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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.TupleDomain.intersect;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.trino.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.createResultingPredicate;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class RemoveRedundantPredicateAboveTableScan
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

    private static final Pattern<FilterNode> PATTERN =
            filter().with(source().matching(
                    tableScan().capturedAs(TABLE_SCAN)
                            // avoid extra computations if table scan doesn't have any enforced predicate
                            .matching(node -> !node.getEnforcedConstraint().isAll())));

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public RemoveRedundantPredicateAboveTableScan(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
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
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        Session session = context.getSession();
        TableScanNode node = captures.get(TABLE_SCAN);
        Expression predicate = filterNode.getPredicate();

        Expression deterministicPredicate = filterDeterministicConjuncts(plannerContext.getMetadata(), predicate);
        Expression nonDeterministicPredicate = filterNonDeterministicConjuncts(plannerContext.getMetadata(), predicate);

        ExtractionResult decomposedPredicate = getFullyExtractedPredicates(
                session,
                deterministicPredicate,
                context.getSymbolAllocator().getTypes());

        if (decomposedPredicate.getTupleDomain().isAll()) {
            // no conjunct could be fully converted to tuple domain
            return Result.empty();
        }

        TupleDomain<ColumnHandle> predicateDomain = decomposedPredicate.getTupleDomain()
                .transformKeys(node.getAssignments()::get);

        if (predicateDomain.isNone()) {
            // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
            // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
            // to turn the subtree into a Values node
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        if (node.getEnforcedConstraint().isNone()) {
            // table scans with none domain should be converted to ValuesNode
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
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

        if (unenforcedDomain.equals(predicateDomain)) {
            // no change in filter predicate
            return Result.empty();
        }

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
        Expression resultingPredicate = createResultingPredicate(
                plannerContext,
                session,
                context.getSymbolAllocator(),
                typeAnalyzer,
                new DomainTranslator(plannerContext).toPredicate(session, unenforcedDomain.transformKeys(assignments::get)),
                nonDeterministicPredicate,
                decomposedPredicate.getRemainingExpression());

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return Result.ofPlanNode(new FilterNode(context.getIdAllocator().getNextId(), node, resultingPredicate));
        }

        return Result.ofPlanNode(node);
    }

    private ExtractionResult getFullyExtractedPredicates(Session session, Expression predicate, TypeProvider types)
    {
        Map<Boolean, List<ExtractionResult>> extractedPredicates = extractConjuncts(predicate).stream()
                .map(conjunct -> DomainTranslator.getExtractionResult(plannerContext, session, conjunct, types))
                .collect(groupingBy(result -> result.getRemainingExpression().equals(TRUE_LITERAL), toList()));
        return new ExtractionResult(
                intersect(extractedPredicates.getOrDefault(TRUE, ImmutableList.of()).stream()
                        .map(ExtractionResult::getTupleDomain)
                        .collect(toImmutableList())),
                combineConjuncts(
                        plannerContext.getMetadata(),
                        extractedPredicates.getOrDefault(FALSE, ImmutableList.of()).stream()
                                .map(ExtractionResult::getRemainingExpression)
                                .collect(toImmutableList())));
    }
}
