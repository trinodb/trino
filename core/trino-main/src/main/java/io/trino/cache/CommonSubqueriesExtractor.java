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
import io.trino.sql.ExpressionUtils;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.SymbolReference;

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
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.DynamicFilters.extractSourceSymbol;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.combineDisjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.extractDisjuncts;
import static io.trino.sql.ExpressionUtils.or;
import static io.trino.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static io.trino.sql.planner.iterative.rule.NormalizeOrExpressionRewriter.normalizeOrExpression;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.pushFilterIntoTableScan;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;

/**
 * Identifies common subqueries and provides adaptation to original query plan. Result of common
 * subquery evaluation is cached with {@link CacheManager}. Therefore, IO and computations are
 * performed only once and are reused within query execution.
 * <p>
 * The general idea is that if there are two subqueries, e.g:
 * subquery1: table_scan(table) <- filter(col1 = 1) <- projection(y := col2 + 1)
 * subquery2: table_scan(table) <- filter(col1 = 2) <- projection(z := col2 * 2)
 * <p>
 * Then such subqueries can be transformed into:
 * subquery1: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * <- filter(col1 = 1) <- projection(y := y)
 * subquery2: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * <- filter(col1 = 2) <- projection(z := z)
 * <p>
 * where: table_scan(table) <- filter(col1 = 1 OR col1 = 2) <- projection(y := col2 + 1, z := col2 * 2)
 * is a common subquery for which the results can be cached and evaluated only once.
 */
public final class CommonSubqueriesExtractor
{
    private final CacheController cacheController;
    private final PlannerContext plannerContext;
    private final Session session;
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;
    private final TypeAnalyzer typeAnalyzer;
    private final PlanNode root;

    public static Map<PlanNode, CommonPlanAdaptation> extractCommonSubqueries(
            CacheController cacheController,
            PlannerContext plannerContext,
            Session session,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            TypeAnalyzer typeAnalyzer,
            PlanNode root)
    {
        return new CommonSubqueriesExtractor(cacheController, plannerContext, session, idAllocator, symbolAllocator, typeAnalyzer, root)
                .extractCommonSubqueries();
    }

    public CommonSubqueriesExtractor(
            CacheController cacheController,
            PlannerContext plannerContext,
            Session session,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            TypeAnalyzer typeAnalyzer,
            PlanNode root)
    {
        this.cacheController = requireNonNull(cacheController, "cacheController is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.session = requireNonNull(session, "session is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.root = requireNonNull(root, "root is null");
    }

    public Map<PlanNode, CommonPlanAdaptation> extractCommonSubqueries()
    {
        ImmutableMap.Builder<PlanNode, CommonPlanAdaptation> planAdaptations = ImmutableMap.builder();
        List<CacheCandidate> cacheCandidates = cacheController.getCachingCandidates(
                session,
                extractCanonicalSubplans(plannerContext.getMetadata(), plannerContext.getCacheMetadata(), session, root));

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
        Map<CacheColumnId, Expression> commonProjections = extractCommonProjections(subplans, commonPredicate, intersectingConjuncts, Optional.empty(), Optional.empty());
        Map<CacheColumnId, ColumnHandle> commonColumnHandles = extractCommonColumnHandles(subplans);
        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        Expression commonDynamicFilterDisjuncts = extractCommonDynamicFilterDisjuncts(subplans);
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                tableId,
                commonPredicate,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()));

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
                            createDynamicFilterColumnMapping(subplan, commonDynamicFilterDisjuncts, commonColumnHandles),
                            createAdaptationPredicate(adaptationConjuncts, symbolMapper),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping, symbolMapper),
                            columnIdMapping,
                            adaptationConjuncts);
                })
                .collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptFilterProject(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        checkArgument(subplans.stream().allMatch(subplan -> subplan.getGroupByColumns().isEmpty()), "Group by columns are not allowed");
        checkArgument(subplans.stream().allMatch(subplan -> subplan.getDynamicConjuncts().isEmpty()), "Dynamic filters are only allowed above table scan");
        PlanSignatureWithPredicate childPlanSignature = childAdaptations.get(0).getCommonSubplanSignature();

        Expression commonPredicate = extractCommonPredicate(subplans);
        Set<Expression> intersectingConjuncts = extractIntersectingConjuncts(subplans);
        Map<CacheColumnId, Expression> commonProjections = extractCommonProjections(subplans, commonPredicate, intersectingConjuncts, Optional.empty(), Optional.of(childAdaptations));
        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                filterProjectKey(childPlanSignature.signature().getKey()),
                childPlanSignature,
                commonPredicate,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.empty());

        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    SymbolMapper symbolMapper = createSymbolMapper(columnIdMapping);
                    PlanNode commonSubplan = createSubplanProjection(
                            createSubplanFilter(commonPredicate, childAdaptation.getCommonSubplan()),
                            commonProjections,
                            columnIdMapping,
                            symbolMapper);
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, commonPredicate, intersectingConjuncts, Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, symbolMapper),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping, symbolMapper),
                            columnIdMapping,
                            adaptationConjuncts);
                }).collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptTopNRanking(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        CanonicalSubplan canonicalSubplan = subplans.get(0);
        TopNRankingKey topNRankingKey = (TopNRankingKey) canonicalSubplan.getKey();

        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        PlanSignatureWithPredicate childPlanSignature = childAdaptations.get(0).getCommonSubplanSignature();
        Map<CacheColumnId, Expression> commonProjections = extractCommonProjections(subplans, TRUE_LITERAL, ImmutableSet.of(), Optional.of(ImmutableSet.of()), Optional.of(childAdaptations));
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                topNRankingKey(childPlanSignature.signature().getKey(), topNRankingKey.partitionBy(), topNRankingKey.orderings(), topNRankingKey.rankingType(), topNRankingKey.maxRankingPerPartition()),
                childPlanSignature,
                TRUE_LITERAL,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.empty());

        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    SymbolMapper symbolMapper = createSymbolMapper(columnIdMapping);
                    TopNRankingNode originalPlanNode = (TopNRankingNode) subplan.getOriginalPlanNode();
                    TopNRankingKey key = (TopNRankingKey) subplan.getKey();
                    List<Symbol> partitionedBy = key.partitionBy().stream().map(columnIdMapping::get).toList();
                    DataOrganizationSpecification dataOrganizationSpecific = new DataOrganizationSpecification(
                            partitionedBy,
                            Optional.of(originalPlanNode.getOrderingScheme()));
                    PlanNode commonSubplan = new TopNRankingNode(
                            idAllocator.getNextId(),
                            childAdaptation.getCommonSubplan(),
                            dataOrganizationSpecific,
                            originalPlanNode.getRankingType(),
                            originalPlanNode.getRankingSymbol(),
                            originalPlanNode.getMaxRankingPerPartition(),
                            true,
                            Optional.empty());
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, TRUE_LITERAL, ImmutableSet.of(), Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, symbolMapper),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping, symbolMapper),
                            columnIdMapping,
                            adaptationConjuncts);
                }).collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptTopN(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        CanonicalSubplan canonicalSubplan = subplans.get(0);
        TopNKey topNKey = (TopNKey) canonicalSubplan.getKey();

        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        PlanSignatureWithPredicate childPlanSignature = childAdaptations.get(0).getCommonSubplanSignature();
        Map<CacheColumnId, Expression> commonProjections = extractCommonProjections(subplans, TRUE_LITERAL, ImmutableSet.of(), Optional.of(ImmutableSet.of()), Optional.of(childAdaptations));
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                topNKey(childPlanSignature.signature().getKey(), topNKey.orderings(), topNKey.count()),
                childPlanSignature,
                TRUE_LITERAL,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.empty());
        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    SymbolMapper symbolMapper = createSymbolMapper(columnIdMapping);
                    TopNNode originalPlanNode = (TopNNode) subplan.getOriginalPlanNode();
                    PlanNode commonSubplan = new TopNNode(
                            idAllocator.getNextId(),
                            childAdaptation.getCommonSubplan(),
                            topNKey.count(),
                            originalPlanNode.getOrderingScheme(),
                            TopNNode.Step.PARTIAL);
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, TRUE_LITERAL, ImmutableSet.of(), Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, symbolMapper),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping, symbolMapper),
                            columnIdMapping,
                            adaptationConjuncts);
                }).collect(toImmutableList());
    }

    private List<CommonPlanAdaptation> adaptAggregation(List<CommonPlanAdaptation> childAdaptations, List<CanonicalSubplan> subplans)
    {
        checkArgument(subplans.stream().allMatch(subplan -> subplan.getConjuncts().isEmpty()), "Conjuncts are not allowed in aggregation canonical subplan");
        checkArgument(subplans.get(0).getGroupByColumns().isPresent(), "Group by columns are not present");
        checkArgument(subplans.stream().map(CanonicalSubplan::getGroupByColumns).distinct().count() == 1, "Group by columns must be the same for all subplans");
        PlanSignatureWithPredicate childPlanSignature = childAdaptations.get(0).getCommonSubplanSignature();

        Set<CacheColumnId> groupByColumns = subplans.get(0).getGroupByColumns().orElseThrow();
        Map<CacheColumnId, Expression> commonProjections = extractCommonProjections(subplans, TRUE_LITERAL, ImmutableSet.of(), Optional.of(groupByColumns), Optional.of(childAdaptations));
        Map<CacheColumnId, Symbol> commonColumnIds = extractCommonColumnIds(subplans);
        PlanSignatureWithPredicate planSignature = computePlanSignature(
                commonColumnIds,
                aggregationKey(childPlanSignature.signature().getKey()),
                childPlanSignature,
                TRUE_LITERAL,
                commonProjections.keySet().stream()
                        .collect(toImmutableList()),
                Optional.of(groupByColumns));

        return zip(subplans.stream(), childAdaptations.stream(),
                (subplan, childAdaptation) -> {
                    Map<CacheColumnId, Symbol> columnIdMapping = createSubplanColumnIdMapping(subplan, commonColumnIds, Optional.of(childAdaptation));
                    SymbolMapper symbolMapper = createSymbolMapper(columnIdMapping);
                    PlanNode commonSubplan = createSubplanAggregation(
                            childAdaptation.getCommonSubplan(),
                            commonProjections,
                            groupByColumns,
                            columnIdMapping);
                    List<Expression> adaptationConjuncts = createAdaptationConjuncts(subplan, TRUE_LITERAL, ImmutableSet.of(), Optional.of(childAdaptation));
                    return new CommonPlanAdaptation(
                            commonSubplan,
                            planSignature,
                            childAdaptation,
                            createAdaptationPredicate(adaptationConjuncts, symbolMapper),
                            createAdaptationAssignments(commonSubplan, subplan, columnIdMapping, symbolMapper),
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
                        plannerContext.getMetadata(),
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

    private static Map<CacheColumnId, Expression> extractCommonProjections(
            List<CanonicalSubplan> subplans,
            Expression commonPredicate,
            Set<Expression> intersectingConjuncts,
            Optional<Set<CacheColumnId>> groupByColumns,
            Optional<List<CommonPlanAdaptation>> childAdaptations)
    {
        // Extract common projections. Common (cached) subquery must contain projections from all subqueries.
        // Pruning adaptation projection is then created for each subquery on top of common subplan.
        Map<CacheColumnId, Expression> commonProjections = subplans.stream()
                .flatMap(subplan -> subplan.getAssignments().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        // Common subquery must propagate all symbols used in adaptation predicates.
        Map<CacheColumnId, Expression> propagatedSymbols = childAdaptations
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
                .collect(toImmutableMap(CanonicalSubplanExtractor::canonicalSymbolToColumnId, Symbol::toSymbolReference));
        groupByColumns.ifPresent(columns -> checkState(columns.containsAll(propagatedSymbols.keySet()), "group by columns don't contain all propagated symbols"));
        return Streams.concat(commonProjections.entrySet().stream(), propagatedSymbols.entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
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
                plannerContext.getMetadata(),
                subplans.stream()
                        .map(subplan -> combineConjuncts(plannerContext.getMetadata(), subplan.getDynamicConjuncts()))
                        .collect(toImmutableList()));
    }

    private PlanSignatureWithPredicate computePlanSignature(
            Map<CacheColumnId, Symbol> commonColumnIds,
            CacheTableId tableId,
            Expression predicate,
            List<CacheColumnId> projections)
    {
        return computePlanSignature(
                commonColumnIds,
                scanFilterProjectKey(tableId),
                TupleDomain.all(),
                predicate,
                projections,
                Optional.empty());
    }

    private PlanSignatureWithPredicate computePlanSignature(
            Map<CacheColumnId, Symbol> commonColumnIds,
            SignatureKey signatureKey,
            PlanSignatureWithPredicate childPlanSignature,
            Expression predicate,
            List<CacheColumnId> projections,
            Optional<Set<CacheColumnId>> groupByColumns)
    {
        return computePlanSignature(
                commonColumnIds,
                // Append child group by columns to signature key (if present)
                combine(signatureKey, childPlanSignature.signature().getGroupByColumns().map(columns -> "groupByColumns=" + columns).orElse("")),
                childPlanSignature.predicate(),
                predicate,
                projections,
                groupByColumns);
    }

    private PlanSignatureWithPredicate computePlanSignature(
            Map<CacheColumnId, Symbol> commonColumnIds,
            SignatureKey signatureKey,
            TupleDomain<CacheColumnId> tupleDomain,
            Expression predicate,
            List<CacheColumnId> projections,
            Optional<Set<CacheColumnId>> groupByColumns)
    {
        Set<CacheColumnId> projectionSet = ImmutableSet.copyOf(projections);
        checkArgument(groupByColumns.isEmpty() || projectionSet.containsAll(groupByColumns.get()));

        // Order group by columns by name
        Optional<List<CacheColumnId>> orderedGroupByColumns = groupByColumns.map(
                Ordering.from(comparing(CacheColumnId::toString))::immutableSortedCopy);
        TypeProvider typeProvider = symbolAllocator.getTypes();
        List<Type> projectionColumnsTypes = projections.stream()
                .map(cacheColumnId -> typeProvider.get(commonColumnIds.get(cacheColumnId)))
                .collect(toImmutableList());
        if (tupleDomain.isAll() && predicate.equals(TRUE_LITERAL)) {
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
                predicate,
                TypeProvider.viewOf(commonColumnIds.entrySet().stream()
                        .collect(toImmutableMap(entry -> columnIdToSymbol(entry.getKey()), entry -> typeProvider.get(entry.getValue())))));
        // Only domains for projected columns can be part of signature predicate
        TupleDomain<CacheColumnId> extractedTupleDomain = extractionResult.getTupleDomain()
                .transformKeys(CanonicalSubplanExtractor::canonicalSymbolToColumnId)
                .intersect(tupleDomain);
        TupleDomain<CacheColumnId> signatureTupleDomain = extractedTupleDomain
                .filter((columnId, domain) -> projectionSet.contains(columnId));
        // Remaining expression and non-projected domains must be part of signature key
        TupleDomain<CacheColumnId> remainingTupleDomain = extractedTupleDomain
                .filter((columnId, domain) -> !projectionSet.contains(columnId));
        if (!remainingTupleDomain.isAll() || !extractionResult.getRemainingExpression().equals(TRUE_LITERAL)) {
            Expression remainingDomainExpression = new DomainTranslator(plannerContext).toPredicate(
                    remainingTupleDomain.transformKeys(CanonicalSubplanExtractor::columnIdToSymbol));
            signatureKey = combine(
                    signatureKey,
                    "filters=" + formatExpression(combineConjuncts(
                            plannerContext.getMetadata(),
                            // Order remaining expressions alphabetically to improve signature generalisation
                            Stream.of(remainingDomainExpression, extractionResult.getRemainingExpression())
                                    .map(ExpressionUtils::extractConjuncts)
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
                signatureTupleDomain);
    }

    private Map<CacheColumnId, Symbol> createSubplanColumnIdMapping(CanonicalSubplan subplan, Map<CacheColumnId, Symbol> commonColumnIds, Optional<CommonPlanAdaptation> childAdaptation)
    {
        Map<CacheColumnId, Symbol> columnIdMapping = new LinkedHashMap<>(subplan.getOriginalSymbolMapping());
        // Propagate column id<->symbol mappings from child adaptation
        childAdaptation
                .map(CommonPlanAdaptation::getColumnIdMapping)
                .orElse(ImmutableMap.of())
                .forEach((key, value) -> checkState(columnIdMapping.putIfAbsent(key, value) == null || columnIdMapping.get(key).equals(value), "Column id is mapped to a different symbol"));
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
            Map<CacheColumnId, Expression> projections,
            Set<CacheColumnId> groupByColumns,
            Map<CacheColumnId, Symbol> columnIdMapping)
    {
        ImmutableList.Builder<Symbol> groupByColumnSymbols = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();

        for (Map.Entry<CacheColumnId, Expression> entry : projections.entrySet()) {
            CacheColumnId id = entry.getKey();
            if (groupByColumns.contains(entry.getKey())) {
                groupByColumnSymbols.add(columnIdMapping.get(id));
            }
            else {
                FunctionCall aggregationCall = (FunctionCall) entry.getValue();

                // Resolve filter expression in terms of subplan symbols
                aggregationCall.getFilter()
                        .ifPresent(expression -> checkState(expression instanceof SymbolReference));
                Optional<Symbol> mask = aggregationCall.getFilter()
                        .map(filter -> requireNonNull(columnIdMapping.get(canonicalExpressionToColumnId(filter))));

                // Resolve arguments in terms of subplan symbols
                ResolvedFunction resolvedFunction = plannerContext.getMetadata().decodeFunction(aggregationCall.getName());
                List<Expression> arguments = aggregationCall.getArguments().stream()
                        .peek(argument -> checkState(argument instanceof SymbolReference))
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

    private static Map<CacheColumnId, ColumnHandle> createDynamicFilterColumnMapping(CanonicalSubplan subplan, Expression commonDynamicFilterDisjuncts, Map<CacheColumnId, ColumnHandle> commonColumnHandles)
    {
        // Create mapping between dynamic filtering columns and column ids.
        // All dynamic filtering columns are part of planSignature columns
        // because joins are not supported yet.
        return Streams.concat(
                        extractDisjuncts(commonDynamicFilterDisjuncts).stream()
                                .flatMap(conjunct -> extractDynamicFilters(conjunct).getDynamicConjuncts().stream())
                                .map(filter -> canonicalSymbolToColumnId(extractSourceSymbol(filter))),
                        subplan.getDynamicConjuncts().stream()
                                .flatMap(conjunct -> extractDynamicFilters(conjunct).getDynamicConjuncts().stream())
                                .map(filter -> canonicalSymbolToColumnId(extractSourceSymbol(filter))))
                .distinct()
                .collect(toImmutableMap(identity(), commonColumnHandles::get));
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
        if (predicate.equals(TRUE_LITERAL) && subplan.getDynamicConjuncts().isEmpty()) {
            return new SubplanFilter(tableScan, Optional.empty(), tableScan);
        }

        Expression predicateWithDynamicFilters =
                // Subquery specific dynamic filters need to be added back to subplan.
                // Actual dynamic filter domains are accounted for in PlanSignature on worker nodes.
                symbolMapper.map(combineConjuncts(
                        plannerContext.getMetadata(),
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
                idAllocator,
                symbolAllocator,
                plannerContext,
                typeAnalyzer,
                node -> PlanNodeStatsEstimate.unknown(),
                new DomainTranslator(plannerContext))
                .getMainAlternative();

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

    private PlanNode createSubplanFilter(Expression predicate, PlanNode source)
    {
        if (predicate.equals(TRUE_LITERAL)) {
            return source;
        }
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    private record SubplanFilter(PlanNode subplan, Optional<Expression> predicate, TableScanNode tableScan) {}

    private PlanNode createSubplanProjection(
            PlanNode subplan,
            Map<CacheColumnId, Expression> projections,
            Map<CacheColumnId, Symbol> columnIdMapping,
            SymbolMapper symbolMapper)
    {
        return createSubplanAssignments(subplan, projections, columnIdMapping, symbolMapper)
                .map(assignments -> (PlanNode) new ProjectNode(idAllocator.getNextId(), subplan, assignments))
                .orElse(subplan);
    }

    private static Optional<Assignments> createAdaptationAssignments(
            PlanNode subplan,
            CanonicalSubplan canonicalSubplan,
            Map<CacheColumnId, Symbol> columnIdMapping,
            SymbolMapper symbolMapper)
    {
        // Prune and order common subquery output in order to match original subquery.
        Map<CacheColumnId, Expression> projections = canonicalSubplan.getAssignments().keySet().stream()
                .peek(id -> checkState(subplan.getOutputSymbols().contains(requireNonNull(columnIdMapping.get(id))), "No symbol for column id: %s", id))
                .collect(toImmutableMap(id -> id, id -> columnIdToSymbol(id).toSymbolReference()));
        return createSubplanAssignments(
                subplan,
                projections,
                columnIdMapping,
                symbolMapper);
    }

    private static Optional<Assignments> createSubplanAssignments(
            PlanNode subplan,
            Map<CacheColumnId, Expression> projections,
            Map<CacheColumnId, Symbol> columnIdMapping,
            SymbolMapper symbolMapper)
    {
        // Remap CacheColumnIds and symbols into specific subquery symbols
        Assignments assignments = Assignments.copyOf(projections.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> columnIdMapping.get(entry.getKey()),
                        entry -> symbolMapper.map(entry.getValue()))));

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
