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
package io.trino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.cache.CacheController.CacheCandidate;
import io.trino.cache.CanonicalSubplan.AggregationKey;
import io.trino.cache.CanonicalSubplan.FilterProjectKey;
import io.trino.cache.CanonicalSubplan.Key;
import io.trino.cache.CanonicalSubplan.ScanFilterProjectKey;
import io.trino.cache.CanonicalSubplan.TableScan;
import io.trino.cache.CanonicalSubplan.TopNKey;
import io.trino.cache.CanonicalSubplan.TopNRankingKey;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Streams.forEachPair;
import static com.google.common.collect.Streams.zip;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalExpressionToColumnId;
import static io.trino.cache.CanonicalSubplanExtractor.canonicalSymbolToColumnId;
import static io.trino.cache.CanonicalSubplanExtractor.columnIdToSymbol;
import static io.trino.cache.CanonicalSubplanExtractor.extractCanonicalSubplans;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ExpressionFormatter.formatExpression;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.combineDisjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static io.trino.sql.planner.iterative.rule.NormalizeOrExpressionRewriter.normalizeOrExpression;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.pushFilterIntoTableScan;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

/**
 * Identifies common subqueries and provides adaptation to original query plan. Result of common
 * subquery evaluation is cached with {@link CacheManager}. Therefore, IO and computations are
 * performed only once and are reused within query execution.
 * <p>
 * The general idea is that if there are two subqueries, e.g:
 * {@code subquery1: table_scan(table) <- filter(col1 = 1) <- projection(y := col2 + 1)}
 * {@code subquery2: table_scan(table) <- filter(col1 = 2) <- projection(z := col2 * 2)}
 * <p>
 * Then such subqueries can be transformed into:
 * {@code subquery1: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * <- filter(col1 = 1) <- projection(y := y)}
 * {@code subquery2: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * <- filter(col1 = 2) <- projection(z := z)}
 * <p>
 * {@code where: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)}
 * is a common subquery for which the results can be cached and evaluated only once.
 */
public final class CommonSubqueriesExtractor
{
    private final CacheController cacheController;
    private final PlannerContext plannerContext;
    private final Session session;
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final PlanNode root;
    private final DomainTranslator domainTranslator;

    public static Map<PlanNode, CommonPlanAdaptation> extractCommonSubqueries(
            CacheController cacheController,
            PlannerContext plannerContext,
            Session session,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            PlanNode root)
    {
        return new CommonSubqueriesExtractor(cacheController, plannerContext, session, idAllocator, symbolAllocator, root)
                .extractCommonSubqueries();
    }

    public CommonSubqueriesExtractor(
            CacheController cacheController,
            PlannerContext plannerContext,
            Session session,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            PlanNode root)
    {
        this.cacheController = requireNonNull(cacheController, "cacheController is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.session = requireNonNull(session, "session is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.root = requireNonNull(root, "root is null");
        this.domainTranslator = new DomainTranslator(plannerContext.getMetadata());
    }

    public Map<PlanNode, CommonPlanAdaptation> extractCommonSubqueries()
    {
        ImmutableMap.Builder<PlanNode, CommonPlanAdaptation> planAdaptations = ImmutableMap.builder();
        List<CacheCandidate> cacheCandidates = cacheController.getCachingCandidates(
                session,
                extractCanonicalSubplans(plannerContext, session, root));

        // extract common subplan adaptations
        Set<PlanNodeId> processedSubplans = new HashSet<>();
        for (CacheCandidate cacheCandidate : cacheCandidates) {
            List<CanonicalSubplan> subplans = cacheCandidate.subplans().stream()
                    // skip subqueries for which common subplan was already extracted
                    .filter(subplan -> !processedSubplans.contains(subplan.getTableScanId()))
                    .collect(toImmutableList());

            if (subplans.size() < cacheCandidate.minSubplans()) {
                // skip if not enough subplans
                continue;
            }

            subplans.forEach(subplan -> processedSubplans.add(subplan.getTableScanId()));
            List<CommonPlanAdaptation> adaptations = adaptSubplans(subplans);
            checkState(adaptations.size() == subplans.size());
            forEachPair(subplans.stream(), adaptations.stream(), (subplan, adaptation) ->
                    planAdaptations.put(subplan.getOriginalPlanNode(), adaptation));
        }
        return planAdaptations.buildOrThrow();
    }

    private List<CommonPlanAdaptation> adaptSubplans(List<CanonicalSubplan> subplans)
    {
        checkArgument(!subplans.isEmpty());
        checkArgument(subplans.stream().map(CanonicalSubplan::getKeyChain).distinct().count() == 1, "All subplans should have the same keychain");

        Key key = subplans.get(0).getKey();
        if (key instanceof ScanFilterProjectKey) {
            return adaptScanFilterProject(subplans);
        }
        else {
            List<CommonPlanAdaptation> childAdaptations = adaptSubplans(subplans.stream()
                    .map(subplan -> subplan.getChildSubplan().orElseThrow())
                    .collect(toImmutableList()));
            return adaptSubplans(key, childAdaptations, subplans);
        }
    }

    private List<CommonPlanAdaptation> adaptSubplans(Key key, List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        checkArgument(childAdaptations.size() == subplans.size());
        if (key instanceof FilterProjectKey) {
            return adaptFilterProject(childAdaptations, subplans);
        }
        else if (key instanceof AggregationKey) {
            return adaptAggregation(childAdaptations, subplans);
        }
        if (key instanceof TopNKey) {
            return adaptTopN(childAdaptations, subplans);
        }
        if (key instanceof TopNRankingKey) {
            return adaptTopNRanking(childAdaptations, subplans);
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported key: %s", key));
        }
    }

    private List<CommonPlanAdaptation> adaptScanFilterProject(List<CanonicalSubplan> subplans)
    {
        checkArgument(subplans.stream().allMatch(subplan -> subplan.getGroupByColumns().isEmpty()), "Group by columns are not allowed");
        checkArgument(subplans.stream().map(subplan -> subplan.getTableScan().orElseThrow().getTableId()).distinct().count() == 1, "All subplans should have the same table id");
        CacheTableId tableId = subplans.get(0).getTableScan().orElseThrow().getTableId();

        Expression commonPredicate = extractCommonPredicate(subplans);
        Set<Expression> intersectingConjuncts = extractIntersectingConjuncts(subplans);
        Map<CacheColumnId, CacheExpression> commonProjections = extractCommonProjections(subplans, commonPredicate, intersectingConjuncts, Optional.empty(), Optional.empty());
        Map<CacheColumnId, ColumnHandle> commonColumnHandles = extractCommonColumnHandles(subplans);
        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        Expression commonDynamicFilterDisjuncts = extractCommonDynamicFilterDisjuncts(subplans);
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                tableId,
                commonPredicate,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                commonColumnHandles.keySet());

        return subplans.stream()
                .map(subplan -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.empty());
                    SymbolMapper symbolMapper = createSymbolMapper(columnIdMapping);
                    SubplanFilter commonSubplanFilter = createSubplanFilter(
                            subplan,
                            commonPredicate,
                            createSubplanTableScan(subplan.getTableScan().orElseThrow(), commonColumnHandles, columnIdMapping),
                            symbolMapper);
                    PlanNode commonSubplan = createSubplanProjection(commonSubplanFilter.subplan(), commonProjections, columnIdMapping, symbolMapper);
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, commonPredicate, intersectingConjuncts, Optional.empty());
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            new FilteredTableScan(commonSubplanFilter.tableScan(), commonSubplanFilter.predicate()),
                            symbolMapper.map(commonDynamicFilterDisjuncts),
                            commonColumnHandles,
                            createAdaptationPredicate(adaptationConjuncts, symbolMapper),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping),
                            columnIdMapping,
                            adaptationConjuncts);
                })
                .collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptFilterProject(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        checkArgument(subplans.stream().allMatch(subplan -> subplan.getGroupByColumns().isEmpty()), "Group by columns are not allowed");
        checkArgument(subplans.stream().allMatch(subplan -> subplan.getDynamicConjuncts().isEmpty()), "Dynamic filters are only allowed above table scan");

        Expression commonPredicate = extractCommonPredicate(subplans);
        Set<Expression> intersectingConjuncts = extractIntersectingConjuncts(subplans);
        Map<CacheColumnId, CacheExpression> commonProjections = extractCommonProjections(subplans, commonPredicate, intersectingConjuncts, Optional.empty(), Optional.of(childAdaptations));
        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                filterProjectKey(childAdaptations.get(0).getCommonSubplanSignature().signature().getKey()),
                childAdaptations.get(0),
                commonPredicate,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.empty());

        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    SymbolMapper symbolMapper = createSymbolMapper(columnIdMapping);
                    PlanNode commonSubplan = createSubplanProjection(
                            createSubplanFilter(commonPredicate, childAdaptation.getCommonSubplan(), symbolMapper),
                            commonProjections,
                            columnIdMapping,
                            symbolMapper);
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, commonPredicate, intersectingConjuncts, Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, symbolMapper),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping),
                            columnIdMapping,
                            adaptationConjuncts);
                }).collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptTopNRanking(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        CanonicalSubplan canonicalSubplan = subplans.get(0);
        TopNRankingKey topNRankingKey = (TopNRankingKey) canonicalSubplan.getKey();

        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        Map<CacheColumnId, CacheExpression> commonProjections = extractCommonProjections(subplans, TRUE, ImmutableSet.of(), Optional.of(ImmutableSet.of()), Optional.of(childAdaptations));
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                topNRankingKey(childAdaptations.get(0).getCommonSubplanSignature().signature().getKey(), topNRankingKey.partitionBy(), topNRankingKey.orderings(), topNRankingKey.rankingType(), topNRankingKey.maxRankingPerPartition()),
                childAdaptations.get(0),
                TRUE,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.empty());

        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    TopNRankingNode originalPlanNode = (TopNRankingNode) subplan.getOriginalPlanNode();
                    TopNRankingKey key = (TopNRankingKey) subplan.getKey();
                    // recreate specification that matches common partition column order
                    DataOrganizationSpecification specification = new DataOrganizationSpecification(
                            key.partitionBy().stream()
                                    .map(columnIdMapping::get)
                                    .collect(toImmutableList()),
                            Optional.of(originalPlanNode.getOrderingScheme()));
                    PlanNode commonSubplan = new TopNRankingNode(
                            idAllocator.getNextId(),
                            childAdaptation.getCommonSubplan(),
                            specification,
                            originalPlanNode.getRankingType(),
                            originalPlanNode.getRankingSymbol(),
                            originalPlanNode.getMaxRankingPerPartition(),
                            true,
                            Optional.empty());
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, TRUE, ImmutableSet.of(), Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, createSymbolMapper(columnIdMapping)),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping),
                            columnIdMapping,
                            adaptationConjuncts);
                }).collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptTopN(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        CanonicalSubplan canonicalSubplan = subplans.get(0);
        TopNKey topNKey = (TopNKey) canonicalSubplan.getKey();

        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        Map<CacheColumnId, CacheExpression> commonProjections = extractCommonProjections(subplans, TRUE, ImmutableSet.of(), Optional.of(ImmutableSet.of()), Optional.of(childAdaptations));
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                topNKey(childAdaptations.get(0).getCommonSubplanSignature().signature().getKey(), topNKey.orderings(), topNKey.count()),
                childAdaptations.get(0),
                TRUE,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.empty());
        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    TopNNode originalPlanNode = (TopNNode) subplan.getOriginalPlanNode();
                    PlanNode commonSubplan = new TopNNode(
                            idAllocator.getNextId(),
                            childAdaptation.getCommonSubplan(),
                            originalPlanNode.getCount(),
                            originalPlanNode.getOrderingScheme(),
                            TopNNode.Step.PARTIAL);
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, TRUE, ImmutableSet.of(), Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, createSymbolMapper(columnIdMapping)),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping),
                            columnIdMapping,
                            adaptationConjuncts);
                }).collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptAggregation(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        checkArgument(subplans.stream().allMatch(subplan -> subplan.getConjuncts().isEmpty()), "Conjuncts are not allowed in aggregation canonical subplan");
        checkArgument(subplans.get(0).getGroupByColumns().isPresent(), "Group by columns are not present");
        checkArgument(subplans.stream().map(CanonicalSubplan::getGroupByColumns).distinct().count() == 1, "Group by columns must be the same for all subplans");

        Set<CacheColumnId> groupByColumns = subplans.get(0).getGroupByColumns().orElseThrow();
        Map<CacheColumnId, CacheExpression> commonProjections = extractCommonProjections(subplans, TRUE, ImmutableSet.of(), Optional.of(groupByColumns), Optional.of(childAdaptations));
        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                aggregationKey(childAdaptations.get(0).getCommonSubplanSignature().signature().getKey()),
                childAdaptations.get(0),
                TRUE,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.of(groupByColumns));

        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    PlanNode commonSubplan = createSubplanAggregation(
                            childAdaptation.getCommonSubplan(),
                            commonProjections,
                            groupByColumns,
                            columnIdMapping);
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, TRUE, ImmutableSet.of(), Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, createSymbolMapper(columnIdMapping)),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping),
                            columnIdMapping,
                            adaptationConjuncts);
                }).collect(toImmutableList());
    }

    private Expression extractCommonPredicate(List<CanonicalSubplan> subplans)
    {
        // When two similar subqueries have different predicates, e.g: subquery1: col = 1, subquery2: col = 2
        // then common subquery must have predicate "col = 1 OR col = 2". Narrowing adaptation predicate is then
        // created for each subquery on top of common subquery.
        return normalizeOrExpression(
                extractCommonPredicates(
                        or(subplans.stream()
                                .map(subplan -> and(
                                        subplan.getConjuncts()))
                                .collect(toImmutableList()))));
    }

    private static Set<Expression> extractIntersectingConjuncts(List<CanonicalSubplan> subplans)
    {
        return subplans.stream()
                .map(subplan -> (Set<Expression>) ImmutableSet.copyOf(subplan.getConjuncts()))
                .reduce(Sets::intersection)
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
    }

    private static Map<CacheColumnId, CacheExpression> extractCommonProjections(
            List<CanonicalSubplan> subplans,
            Expression commonPredicate,
            Set<Expression> intersectingConjuncts,
            Optional<Set<CacheColumnId>> pullupColumns,
            Optional<List<CommonPlanAdaptation>> childAdaptations)
    {
        // Extract common projections. Common (cached) subquery must contain projections from all subqueries.
        // Pruning adaptation projection is then created for each subquery on top of common subplan.
        Map<CacheColumnId, CacheExpression> commonProjections = subplans.stream()
                .flatMap(subplan -> subplan.getAssignments().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, CommonSubqueriesExtractor::getSmallerExpression));
        // Common subquery must propagate all symbols used in adaptation predicates.
        Map<CacheColumnId, CacheExpression> propagatedSymbols = childAdaptations
                // Append adaptation conjuncts from child adaptations (if present)
                .map(adaptations -> zip(subplans.stream(), adaptations.stream(), CommonSubqueriesExtractor::appendAdaptationConjuncts))
                .orElse(subplans.stream().map(CanonicalSubplan::getConjuncts))
                .filter(conjuncts -> isAdaptationPredicateNeeded(conjuncts, commonPredicate))
                .flatMap(Collection::stream)
                // Use only conjuncts that are not enforced by intersecting predicate
                .filter(conjunct -> !intersectingConjuncts.contains(conjunct))
                .map(SymbolsExtractor::extractAll)
                .flatMap(Collection::stream)
                .distinct()
                .collect(toImmutableMap(CanonicalSubplanExtractor::canonicalSymbolToColumnId, symbol -> CacheExpression.ofProjection(symbol.toSymbolReference())));
        pullupColumns.ifPresent(columns -> checkState(columns.containsAll(propagatedSymbols.keySet()), "pullup columns don't contain all propagated symbols"));
        return Streams.concat(commonProjections.entrySet().stream(), propagatedSymbols.entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, CommonSubqueriesExtractor::getSmallerExpression));
    }

    private static CacheExpression getSmallerExpression(CacheExpression first, CacheExpression second)
    {
        checkArgument(first.projection().isPresent() == second.projection().isPresent(), "One of the projection expressions is missing. first expression: %s, second expression: %s", first.projection(), second.projection());
        if (first.aggregation().isPresent()) {
            // Supported aggregations can only have canonical symbol arguments, hence there should be no evaluation ambiguity.
            checkArgument(first.aggregation().equals(second.aggregation()), "Aggregation expressions are not the same. first expression: %s, second expression: %s", first.aggregation(), second.aggregation());
            return first;
        }
        // Prefer smaller expression trees.
        if (IrUtils.preOrder(first.projection().get()).count() <= IrUtils.preOrder(second.projection().get()).count()) {
            return first;
        }
        return second;
    }

    private static Map<CacheColumnId, ColumnHandle> extractCommonColumnHandles(List<CanonicalSubplan> subplans)
    {
        // Common subquery must select column handles from all subqueries.
        return subplans.stream()
                .flatMap(subplan -> subplan.getTableScan().orElseThrow().getColumnHandles().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<CacheColumnId, Symbol> extractCommonColumnIds(List<CanonicalSubplan> subplans)
    {
        Map<CacheColumnId, Symbol> commonColumnIds = new LinkedHashMap<>();
        subplans.stream()
                .flatMap(subplan -> subplan.getOriginalSymbolMapping().entrySet().stream())
                .forEach(entry -> commonColumnIds.putIfAbsent(entry.getKey(), entry.getValue()));
        return commonColumnIds;
    }

    private Expression extractCommonDynamicFilterDisjuncts(List<CanonicalSubplan> subplans)
    {
        return combineDisjuncts(
                subplans.stream()
                        .map(subplan -> combineConjuncts(subplan.getDynamicConjuncts()))
                        .collect(toImmutableList()));
    }

    private PlanSignatureWithPredicate computePlanSignature(
            Map<CacheColumnId, Symbol> commonColumnIds,
            CacheTableId tableId,
            Expression predicate,
            List<CacheColumnId> projections,
            Set<CacheColumnId> scanColumnIds)
    {
        return computePlanSignature(
                commonColumnIds,
                scanFilterProjectKey(tableId),
                TupleDomain.all(),
                predicate,
                projections,
                Optional.empty(),
                scanColumnIds);
    }

    private PlanSignatureWithPredicate computePlanSignature(
            Map<CacheColumnId, Symbol> commonColumnIds,
            SignatureKey signatureKey,
            CommonPlanAdaptation childAdaptation,
            Expression predicate,
            List<CacheColumnId> projections,
            Optional<Set<CacheColumnId>> groupByColumns)
    {
        return computePlanSignature(
                commonColumnIds,
                // Append child group by columns to signature key (if present)
                combine(signatureKey, childAdaptation.getCommonSubplanSignature().signature().getGroupByColumns().map(columns -> "groupByColumns=" + columns).orElse("")),
                childAdaptation.getCommonSubplanSignature().predicate(),
                predicate,
                projections,
                groupByColumns,
                childAdaptation.getCommonColumnHandles().keySet());
    }

    private PlanSignatureWithPredicate computePlanSignature(
            Map<CacheColumnId, Symbol> commonColumnIds,
            SignatureKey signatureKey,
            TupleDomain<CacheColumnId> tupleDomain,
            Expression predicate,
            List<CacheColumnId> projections,
            Optional<Set<CacheColumnId>> groupByColumns,
            Set<CacheColumnId> scanColumnIds)
    {
        Set<CacheColumnId> projectionSet = ImmutableSet.copyOf(projections);
        checkArgument(groupByColumns.isEmpty() || projectionSet.containsAll(groupByColumns.get()));

        // Order group by columns by name
        Optional<List<CacheColumnId>> orderedGroupByColumns = groupByColumns.map(
                Ordering.from(comparing(CacheColumnId::toString))::immutableSortedCopy);
        List<Type> projectionColumnsTypes = projections.stream()
                .map(cacheColumnId -> commonColumnIds.get(cacheColumnId).type())
                .collect(toImmutableList());
        if (tupleDomain.isAll() && predicate.equals(TRUE)) {
            return new PlanSignatureWithPredicate(
                    new PlanSignature(
                            signatureKey,
                            orderedGroupByColumns,
                            projections,
                            projectionColumnsTypes),
                    TupleDomain.all());
        }

        ExtractionResult extractionResult = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                predicate);
        // Only domains for projected columns can be part of signature predicate
        TupleDomain<CacheColumnId> extractedTupleDomain = extractionResult.getTupleDomain()
                .transformKeys(CanonicalSubplanExtractor::canonicalSymbolToColumnId)
                .intersect(tupleDomain);
        Set<CacheColumnId> retainedColumnIds = ImmutableSet.<CacheColumnId>builder()
                // retain projected and table scan domains for per split simplification and pruning on worker node
                .addAll(projectionSet)
                .addAll(scanColumnIds)
                .build();
        TupleDomain<CacheColumnId> retainedTupleDomain = extractedTupleDomain
                .filter((columnId, domain) -> retainedColumnIds.contains(columnId));
        // Remaining expression and non-projected domains must be part of signature key
        TupleDomain<CacheColumnId> remainingTupleDomain = extractedTupleDomain
                .filter((columnId, domain) -> !retainedColumnIds.contains(columnId));
        if (!remainingTupleDomain.isAll() || !extractionResult.getRemainingExpression().equals(TRUE)) {
            Expression remainingDomainExpression = domainTranslator.toPredicate(
                    remainingTupleDomain.transformKeys(id -> columnIdToSymbol(id, commonColumnIds.get(id).type())));
            signatureKey = combine(
                    signatureKey,
                    "filters=" + formatExpression(combineConjuncts(
                            // Order remaining expressions alphabetically to improve signature generalisation
                            Stream.of(remainingDomainExpression, extractionResult.getRemainingExpression())
                                    .map(IrUtils::extractConjuncts)
                                    .flatMap(Collection::stream)
                                    .sorted(comparing(Expression::toString))
                                    .collect(toImmutableList()))));
        }

        return new PlanSignatureWithPredicate(
                new PlanSignature(
                        signatureKey,
                        orderedGroupByColumns,
                        projections,
                        projectionColumnsTypes),
                retainedTupleDomain);
    }

    private Map<CacheColumnId, Symbol> createSubplanColumnIdMapping(CanonicalSubplan subplan, Map<CacheColumnId, Symbol> commonColumnIds, Optional<CommonPlanAdaptation> childAdaptation)
    {
        // Propagate column id<->symbol mappings from child adaptation
        Map<CacheColumnId, Symbol> columnIdMapping = new LinkedHashMap<>(childAdaptation
                .map(CommonPlanAdaptation::getColumnIdMapping)
                .orElse(ImmutableMap.of()));
        // Propagate original symbol names only if they don't override symbols from child adaptation since
        // child adaptation might use different symbols for column ids than original subplan.
        subplan.getOriginalSymbolMapping().forEach(columnIdMapping::putIfAbsent);
        // Create new symbols for column ids that were not used in original subplan, but are part of common subquery now
        commonColumnIds
                .forEach((key, value) -> columnIdMapping.computeIfAbsent(key, ignored -> symbolAllocator.newSymbol(value)));
        return ImmutableMap.copyOf(columnIdMapping);
    }

    private SymbolMapper createSymbolMapper(Map<CacheColumnId, Symbol> columnIdMapping)
    {
        return new SymbolMapper(symbol -> requireNonNull(columnIdMapping.get(canonicalSymbolToColumnId(symbol))));
    }

    private PlanNode createSubplanAggregation(
            PlanNode subplan,
            Map<CacheColumnId, CacheExpression> projections,
            Set<CacheColumnId> groupByColumns,
            Map<CacheColumnId, Symbol> columnIdMapping)
    {
        ImmutableList.Builder<Symbol> groupByColumnSymbols = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();

        for (Map.Entry<CacheColumnId, CacheExpression> entry : projections.entrySet()) {
            CacheColumnId id = entry.getKey();
            if (groupByColumns.contains(entry.getKey())) {
                groupByColumnSymbols.add(columnIdMapping.get(id));
            }
            else {
                CanonicalAggregation aggregationCall = entry.getValue().aggregation().orElseThrow();

                // Resolve filter expression in terms of subplan symbols
                Optional<Symbol> mask = aggregationCall.mask()
                        .map(filter -> requireNonNull(columnIdMapping.get(canonicalExpressionToColumnId(filter.toSymbolReference()))));

                // Resolve arguments in terms of subplan symbols
                ResolvedFunction resolvedFunction = aggregationCall.resolvedFunction();
                List<Expression> arguments = aggregationCall.arguments().stream()
                        .peek(argument -> checkState(argument instanceof Reference))
                        .map(argument -> columnIdMapping.get(canonicalExpressionToColumnId(argument)).toSymbolReference())
                        .collect(toImmutableList());

                // Re-create aggregation using subquery specific symbols
                aggregations.put(
                        columnIdMapping.get(id),
                        new Aggregation(
                                resolvedFunction,
                                arguments,
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                mask));
            }
        }

        AggregationNode aggregation = new AggregationNode(
                idAllocator.getNextId(),
                subplan,
                aggregations.buildOrThrow(),
                singleGroupingSet(groupByColumnSymbols.build()),
                ImmutableList.of(),
                PARTIAL,
                Optional.empty(),
                Optional.empty());
        List<Symbol> expectedSymbols = projections.keySet().stream()
                .map(columnIdMapping::get)
                .collect(toImmutableList());
        checkState(aggregation.getOutputSymbols().equals(expectedSymbols), "Aggregation symbols (%s) don't match expected symbols (%s)", aggregation.getOutputSymbols(), expectedSymbols);
        return aggregation;
    }

    private TableScanNode createSubplanTableScan(
            TableScan tableScan,
            Map<CacheColumnId, ColumnHandle> columnHandles,
            Map<CacheColumnId, Symbol> columnIdMapping)
    {
        return new TableScanNode(
                idAllocator.getNextId(),
                // use original table handle as it contains information about
                // split enumeration (e.g. enforced partition or bucket filter) for
                // a given subquery
                tableScan.getTable(),
                // Remap column ids into specific subquery symbols
                columnHandles.keySet().stream()
                        .map(columnIdMapping::get)
                        .collect(toImmutableList()),
                columnHandles.entrySet().stream()
                        .collect(toImmutableMap(entry -> columnIdMapping.get(entry.getKey()), Map.Entry::getValue)),
                // Enforced constraint is not important at this stage of planning
                TupleDomain.all(),
                // Stats are not important at this stage of planning
                Optional.empty(),
                false,
                Optional.of(tableScan.isUseConnectorNodePartitioning()));
    }

    private SubplanFilter createSubplanFilter(
            CanonicalSubplan subplan,
            Expression predicate,
            TableScanNode tableScan,
            SymbolMapper symbolMapper)
    {
        if (predicate.equals(TRUE) && subplan.getDynamicConjuncts().isEmpty()) {
            return new SubplanFilter(tableScan, Optional.empty(), tableScan);
        }

        Expression predicateWithDynamicFilters =
                // Subquery specific dynamic filters need to be added back to subplan.
                // Actual dynamic filter domains are accounted for in PlanSignature on worker nodes.
                symbolMapper.map(combineConjuncts(
                        predicate,
                        and(subplan.getDynamicConjuncts())));
        FilterNode filterNode = new FilterNode(
                idAllocator.getNextId(),
                tableScan,
                predicateWithDynamicFilters);

        // Try to push down predicates to table scan
        Optional<PlanNode> rewritten = pushFilterIntoTableScan(
                filterNode,
                tableScan,
                false,
                session,
                plannerContext,
                node -> PlanNodeStatsEstimate.unknown());

        // If ValuesNode was returned as a result of pushing down predicates we fall back
        // to filterNode to avoid introducing significant changes in plan. Changing node from TableScan to ValuesNode
        // potentially interfere with partitioning - note that this step is executed after planning.
        rewritten = rewritten.filter(not(ValuesNode.class::isInstance));

        if (rewritten.isPresent()) {
            PlanNode node = rewritten.get();
            if (node instanceof FilterNode rewrittenFilterNode) {
                checkState(rewrittenFilterNode.getSource() instanceof TableScanNode, "Expected filter source to be TableScanNode");
                return new SubplanFilter(node, Optional.of(rewrittenFilterNode.getPredicate()), (TableScanNode) rewrittenFilterNode.getSource());
            }
            checkState(node instanceof TableScanNode, "Expected rewritten node to be TableScanNode");
            return new SubplanFilter(node, Optional.empty(), (TableScanNode) node);
        }

        return new SubplanFilter(filterNode, Optional.of(predicateWithDynamicFilters), tableScan);
    }

    private PlanNode createSubplanFilter(Expression predicate, PlanNode source, SymbolMapper symbolMapper)
    {
        if (predicate.equals(TRUE)) {
            return source;
        }
        return new FilterNode(idAllocator.getNextId(), source, symbolMapper.map(predicate));
    }

    private record SubplanFilter(PlanNode subplan, Optional<Expression> predicate, TableScanNode tableScan) {}

    private PlanNode createSubplanProjection(
            PlanNode subplan,
            Map<CacheColumnId, CacheExpression> projections,
            Map<CacheColumnId, Symbol> columnIdMapping,
            SymbolMapper symbolMapper)
    {
        return createSubplanAssignments(
                subplan,
                projections.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> columnIdMapping.get(entry.getKey()),
                                entry -> symbolMapper.map(entry.getValue().projection().orElseThrow()))))
                .map(assignments -> (PlanNode) new ProjectNode(idAllocator.getNextId(), subplan, assignments))
                .orElse(subplan);
    }

    private static Optional<Assignments> createAdaptationAssignments(
            PlanNode subplan,
            CanonicalSubplan canonicalSubplan,
            Map<CacheColumnId, Symbol> columnIdMapping)
    {
        // Prune and order common subquery output in order to match original subquery.
        Map<Symbol, Expression> projections = canonicalSubplan.getAssignments().keySet().stream()
                .collect(toImmutableMap(
                        // Use original output symbols for adaptation projection.
                        id -> canonicalSubplan.getOriginalSymbolMapping().get(id),
                        id -> columnIdMapping.get(id).toSymbolReference()));
        return createSubplanAssignments(subplan, projections);
    }

    private static Optional<Assignments> createSubplanAssignments(PlanNode subplan, Map<Symbol, Expression> projections)
    {
        Assignments assignments = Assignments.copyOf(projections);

        // cache is sensitive to output symbols order
        if (subplan.getOutputSymbols().equals(assignments.getOutputs())) {
            return Optional.empty();
        }

        return Optional.of(assignments);
    }

    private static List<Expression> createAdaptationConjuncts(
            CanonicalSubplan subplan,
            Expression commonPredicate,
            Set<Expression> intersectingConjuncts,
            Optional<CommonPlanAdaptation> childAdaptation)
    {
        List<Expression> conjuncts = childAdaptation
                .map(adaptation -> appendAdaptationConjuncts(subplan, adaptation))
                .orElse(subplan.getConjuncts());

        if (!isAdaptationPredicateNeeded(conjuncts, commonPredicate)) {
            return ImmutableList.of();
        }

        return conjuncts.stream()
                // Use only conjuncts that are not enforced by common predicate
                .filter(conjunct -> !intersectingConjuncts.contains(conjunct))
                .collect(toImmutableList());
    }

    private static Optional<Expression> createAdaptationPredicate(List<Expression> conjuncts, SymbolMapper symbolMapper)
    {
        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(symbolMapper.map(and(conjuncts)));
    }

    private static List<Expression> appendAdaptationConjuncts(CanonicalSubplan subplan, CommonPlanAdaptation childAdaptation)
    {
        return ImmutableList.<Expression>builder()
                .addAll(subplan.getConjuncts())
                .addAll(childAdaptation.getCanonicalAdaptationConjuncts())
                .build();
    }

    private static boolean isAdaptationPredicateNeeded(List<Expression> conjuncts, Expression commonPredicate)
    {
        Set<Expression> commonConjuncts = ImmutableSet.copyOf(extractConjuncts(commonPredicate));
        return !conjuncts.isEmpty() && !ImmutableSet.copyOf(conjuncts).equals(commonConjuncts);
    }

    @VisibleForTesting
    public static SignatureKey scanFilterProjectKey(CacheTableId tableId)
    {
        return new SignatureKey(toStringHelper("ScanFilterProject")
                .add("tableId", tableId)
                .toString());
    }

    @VisibleForTesting
    public static SignatureKey filterProjectKey(SignatureKey childKey)
    {
        return new SignatureKey(toStringHelper("FilterProject")
                .add("childKey", childKey)
                .toString());
    }

    @VisibleForTesting
    public static SignatureKey aggregationKey(SignatureKey childKey)
    {
        return new SignatureKey(toStringHelper("Aggregation")
                .add("childKey", childKey)
                .toString());
    }

    @VisibleForTesting
    public static SignatureKey topNRankingKey(
            SignatureKey childKey,
            List<CacheColumnId> partitionBy,
            Map<CacheColumnId, SortOrder> orderBy,
            TopNRankingNode.RankingType rankingType,
            int maxRankingPerPartition)
    {
        return new SignatureKey(toStringHelper("TopNRanking")
                .add("childKey", childKey)
                .add("partitionBy", partitionBy)
                .add("orderBy", orderBy)
                .add("rankingType", rankingType)
                .add("maxRankingPerPartition", maxRankingPerPartition)
                .toString());
    }

    @VisibleForTesting
    public static SignatureKey topNKey(SignatureKey childKey, Map<CacheColumnId, SortOrder> orderBy, long count)
    {
        return new SignatureKey(toStringHelper("TopN")
                .add("childKey", childKey)
                .add("orderBy", orderBy)
                .add("count", count)
                .toString());
    }

    @VisibleForTesting
    public static SignatureKey combine(SignatureKey key, String tail)
    {
        if (tail.isEmpty()) {
            return key;
        }

        return new SignatureKey(key + ":" + tail);
    }
}
