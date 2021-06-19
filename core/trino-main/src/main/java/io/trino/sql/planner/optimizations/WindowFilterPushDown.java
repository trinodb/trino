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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionId;
import io.trino.metadata.Metadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isOptimizeTopNRanking;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.DomainTranslator.ExtractionResult;
import static io.trino.sql.planner.DomainTranslator.fromPredicate;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class WindowFilterPushDown
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final TypeOperators typeOperators;
    private final DomainTranslator domainTranslator;

    public WindowFilterPushDown(Metadata metadata, TypeOperators typeOperators)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.domainTranslator = new DomainTranslator(metadata);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, typeOperators, domainTranslator, session, types), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final TypeOperators typeOperators;
        private final DomainTranslator domainTranslator;
        private final Session session;
        private final TypeProvider types;
        private final FunctionId rowNumberFunctionId;
        private final FunctionId rankFunctionId;

        private Rewriter(
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                TypeOperators typeOperators,
                DomainTranslator domainTranslator,
                Session session,
                TypeProvider types)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            rowNumberFunctionId = metadata.resolveFunction(QualifiedName.of("row_number"), ImmutableList.of()).getFunctionId();
            rankFunctionId = metadata.resolveFunction(QualifiedName.of("rank"), ImmutableList.of()).getFunctionId();
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            if (canReplaceWithRowNumber(node)) {
                return new RowNumberNode(idAllocator.getNextId(),
                        rewrittenSource,
                        node.getPartitionBy(),
                        false,
                        getOnlyElement(node.getWindowFunctions().keySet()),
                        Optional.empty(),
                        Optional.empty());
            }
            return replaceChildren(node, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            if (node.isWithTies() || node.requiresPreSortedInputs()) {
                return context.defaultRewrite(node);
            }

            // Limit with count 0 should be removed by RemoveRedundantLimit rule
            if (node.getCount() == 0) {
                return node;
            }

            // Operators can handle MAX_VALUE rows per page, so do not optimize if count is greater than this value
            if (node.getCount() > Integer.MAX_VALUE) {
                return context.defaultRewrite(node);
            }

            PlanNode source = context.rewrite(node.getSource());
            int limit = toIntExact(node.getCount());
            if (source instanceof RowNumberNode) {
                RowNumberNode rowNumberNode = mergeLimit(((RowNumberNode) source), limit);
                if (rowNumberNode.getPartitionBy().isEmpty()) {
                    return rowNumberNode;
                }
                source = rowNumberNode;
            }
            else if (source instanceof WindowNode && isOptimizeTopNRanking(session)) {
                WindowNode windowNode = (WindowNode) source;
                Optional<RankingType> rankingType = toTopNRankingType(windowNode);
                if (rankingType.isPresent()) {
                    TopNRankingNode topNRankingNode = convertToTopNRanking(windowNode, rankingType.get(), limit);
                    if (rankingType.get() == ROW_NUMBER && windowNode.getPartitionBy().isEmpty()) {
                        return topNRankingNode;
                    }
                    source = topNRankingNode;
                }
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            TupleDomain<Symbol> tupleDomain = fromPredicate(metadata, typeOperators, session, node.getPredicate(), types).getTupleDomain();

            if (source instanceof RowNumberNode) {
                Symbol rowNumberSymbol = ((RowNumberNode) source).getRowNumberSymbol();
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);

                if (upperBound.isPresent()) {
                    if (upperBound.getAsInt() <= 0) {
                        return new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of());
                    }
                    source = mergeLimit(((RowNumberNode) source), upperBound.getAsInt());
                    return rewriteFilterSource(node, source, rowNumberSymbol, ((RowNumberNode) source).getMaxRowCountPerPartition().get());
                }
            }
            else if (source instanceof WindowNode && isOptimizeTopNRanking(session)) {
                WindowNode windowNode = (WindowNode) source;
                Optional<RankingType> rankingType = toTopNRankingType(windowNode);
                if (rankingType.isPresent()) {
                    Symbol rankingSymbol = getOnlyElement(windowNode.getWindowFunctions().entrySet()).getKey();
                    OptionalInt upperBound = extractUpperBound(tupleDomain, rankingSymbol);

                    if (upperBound.isPresent()) {
                        if (upperBound.getAsInt() <= 0) {
                            return new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of());
                        }
                        source = convertToTopNRanking(windowNode, rankingType.get(), upperBound.getAsInt());
                        return rewriteFilterSource(node, source, rankingSymbol, upperBound.getAsInt());
                    }
                }
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        private PlanNode rewriteFilterSource(FilterNode filterNode, PlanNode source, Symbol rankingSymbol, int upperBound)
        {
            ExtractionResult extractionResult = fromPredicate(metadata, typeOperators, session, filterNode.getPredicate(), types);
            TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();

            if (!allRankingValuesInDomain(tupleDomain, rankingSymbol, upperBound)) {
                return new FilterNode(filterNode.getId(), source, filterNode.getPredicate());
            }

            // Remove the ranking domain because it is absorbed into the node
            TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(rankingSymbol));
            Expression newPredicate = ExpressionUtils.combineConjuncts(
                    metadata,
                    extractionResult.getRemainingExpression(),
                    domainTranslator.toPredicate(newTupleDomain));

            if (newPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                return source;
            }
            return new FilterNode(filterNode.getId(), source, newPredicate);
        }

        private static boolean allRankingValuesInDomain(TupleDomain<Symbol> tupleDomain, Symbol symbol, long upperBound)
        {
            if (tupleDomain.isNone()) {
                return false;
            }
            Domain domain = tupleDomain.getDomains().get().get(symbol);
            if (domain == null) {
                return true;
            }
            return domain.getValues().contains(ValueSet.ofRanges(range(domain.getType(), 1L, true, upperBound, true)));
        }

        private static OptionalInt extractUpperBound(TupleDomain<Symbol> tupleDomain, Symbol symbol)
        {
            if (tupleDomain.isNone()) {
                return OptionalInt.empty();
            }

            Domain domain = tupleDomain.getDomains().get().get(symbol);
            if (domain == null) {
                return OptionalInt.empty();
            }
            ValueSet values = domain.getValues();
            if (values.isAll() || values.isNone() || values.getRanges().getRangeCount() <= 0) {
                return OptionalInt.empty();
            }

            Range span = values.getRanges().getSpan();

            if (span.isHighUnbounded()) {
                return OptionalInt.empty();
            }

            verify(domain.getType().equals(BIGINT));
            long upperBound = (Long) span.getHighBoundedValue();
            if (!span.isHighInclusive()) {
                upperBound--;
            }

            if (upperBound >= Integer.MIN_VALUE && upperBound <= Integer.MAX_VALUE) {
                return OptionalInt.of(toIntExact(upperBound));
            }
            return OptionalInt.empty();
        }

        private static RowNumberNode mergeLimit(RowNumberNode node, int newRowCountPerPartition)
        {
            if (node.getMaxRowCountPerPartition().isPresent()) {
                newRowCountPerPartition = Math.min(node.getMaxRowCountPerPartition().get(), newRowCountPerPartition);
            }
            return new RowNumberNode(
                    node.getId(),
                    node.getSource(),
                    node.getPartitionBy(),
                    node.isOrderSensitive(),
                    node.getRowNumberSymbol(),
                    Optional.of(newRowCountPerPartition),
                    node.getHashSymbol());
        }

        private TopNRankingNode convertToTopNRanking(WindowNode windowNode, RankingType rankingType, int limit)
        {
            return new TopNRankingNode(idAllocator.getNextId(),
                    windowNode.getSource(),
                    windowNode.getSpecification(),
                    rankingType,
                    getOnlyElement(windowNode.getWindowFunctions().keySet()),
                    limit,
                    false,
                    Optional.empty());
        }

        private boolean canReplaceWithRowNumber(WindowNode node)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            Symbol rankingSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            FunctionId functionId = node.getWindowFunctions().get(rankingSymbol).getResolvedFunction().getFunctionId();
            return functionId.equals(rowNumberFunctionId) && node.getOrderingScheme().isEmpty();
        }

        private Optional<RankingType> toTopNRankingType(WindowNode node)
        {
            if (node.getWindowFunctions().size() != 1 || node.getOrderingScheme().isEmpty()) {
                return Optional.empty();
            }
            Symbol rankingSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            FunctionId functionId = node.getWindowFunctions().get(rankingSymbol).getResolvedFunction().getFunctionId();
            if (functionId.equals(rowNumberFunctionId)) {
                return Optional.of(ROW_NUMBER);
            }
            if (functionId.equals(rankFunctionId)) {
                return Optional.of(RANK);
            }
            return Optional.empty();
        }
    }
}
