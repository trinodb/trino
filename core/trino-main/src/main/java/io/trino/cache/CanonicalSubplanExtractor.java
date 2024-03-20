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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cache.CanonicalSubplan.AggregationKey;
import io.trino.cache.CanonicalSubplan.CanonicalSubplanBuilder;
import io.trino.cache.CanonicalSubplan.FilterProjectKey;
import io.trino.cache.CanonicalSubplan.ScanFilterProjectKey;
import io.trino.cache.CanonicalSubplan.TopNKey;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.ir.CanonicalAggregation;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.type.UnknownType;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cache.CanonicalSubplan.TopNRankingKey;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.ExpressionFormatter.formatExpression;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.partitioningBy;

public final class CanonicalSubplanExtractor
{
    private CanonicalSubplanExtractor() {}

    /**
     * Extracts a list of {@link CanonicalSubplan} for a given plan.
     */
    public static List<CanonicalSubplan> extractCanonicalSubplans(CacheMetadata cacheMetadata, Session session, PlanNode root)
    {
        ImmutableList.Builder<CanonicalSubplan> canonicalSubplans = ImmutableList.builder();
        root.accept(new Visitor(cacheMetadata, session, canonicalSubplans), null).ifPresent(canonicalSubplans::add);
        return canonicalSubplans.build();
    }

    public static CacheColumnId canonicalSymbolToColumnId(Symbol symbol)
    {
        requireNonNull(symbol, "symbol is null");
        return canonicalExpressionToColumnId(symbol.toSymbolReference());
    }

    public static CacheColumnId canonicalExpressionToColumnId(Expression expression)
    {
        requireNonNull(expression, "expression is null");
        if (expression instanceof SymbolReference symbolReference) {
            // symbol -> column id translation should be reversible via columnIdToSymbol method
            return new CacheColumnId(symbolReference.getName());
        }

        // Make CacheColumnIds for complex expressions always wrapped in '()' so they are distinguishable from
        // CacheColumnIds derived from connectors.
        return new CacheColumnId("(" + formatExpression(expression) + ")");
    }

    public static Symbol columnIdToSymbol(CacheColumnId columnId)
    {
        requireNonNull(columnId, "columnId is null");
        // todo[https://github.com/starburstdata/cork/issues/517] use proper type
        return new Symbol(UnknownType.UNKNOWN, columnId.toString());
    }

    private static class Visitor
            extends PlanVisitor<Optional<CanonicalSubplan>, Void>
    {
        private final CacheMetadata cacheMetadata;
        private final Session session;
        private final ImmutableList.Builder<CanonicalSubplan> canonicalSubplans;

        public Visitor(CacheMetadata cacheMetadata, Session session, ImmutableList.Builder<CanonicalSubplan> canonicalSubplans)
        {
            this.cacheMetadata = requireNonNull(cacheMetadata, "cacheMetadata is null");
            this.session = requireNonNull(session, "session is null");
            this.canonicalSubplans = requireNonNull(canonicalSubplans, "canonicalSubplans is null");
        }

        @Override
        protected Optional<CanonicalSubplan> visitPlan(PlanNode node, Void context)
        {
            node.getSources().forEach(this::canonicalizeRecursively);
            return Optional.empty();
        }

        @Override
        public Optional<CanonicalSubplan> visitChooseAlternativeNode(ChooseAlternativeNode node, Void context)
        {
            // do not canonicalize plans that already contain alternative
            return Optional.empty();
        }

        @Override
        public Optional<CanonicalSubplan> visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource();
            // only subset of aggregations is supported
            if (!(node.getGroupingSetCount() == 1
                    && node.getPreGroupedSymbols().isEmpty()
                    && node.getStep() == PARTIAL
                    && node.getGroupIdSymbol().isEmpty()
                    && node.getHashSymbol().isEmpty())) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            // only subset of aggregation functions are supported
            boolean allSupportedAggregations = node.getAggregations().values().stream().allMatch(aggregation ->
                    // only symbol arguments are supported (no lambdas yet)
                    aggregation.getArguments().stream().allMatch(argument -> argument instanceof SymbolReference)
                            && !aggregation.isDistinct()
                            && aggregation.getFilter().isEmpty()
                            && aggregation.getOrderingScheme().isEmpty()
                            && aggregation.getResolvedFunction().isDeterministic());

            if (!allSupportedAggregations) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            // always add non-aggregated canonical subplan so that it can be matched against other
            // non-aggregated subqueries
            Optional<CanonicalSubplan> subplanOptional = canonicalizeRecursively(source);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            // evaluate mapping from subplan symbols to canonical expressions
            CanonicalSubplan subplan = subplanOptional.get();
            SymbolMapper canonicalSymbolMapper = subplan.canonicalSymbolMapper();
            BiMap<CacheColumnId, Symbol> originalSymbolMapping = subplan.getOriginalSymbolMapping();
            Map<CacheColumnId, Expression> assignments = new LinkedHashMap<>();

            // canonicalize grouping columns
            ImmutableSet.Builder<CacheColumnId> groupByColumnsBuilder = ImmutableSet.builder();
            for (Symbol groupingKey : node.getGroupingKeys()) {
                CacheColumnId columnId = requireNonNull(originalSymbolMapping.inverse().get(groupingKey));
                groupByColumnsBuilder.add(columnId);
                if (assignments.put(columnId, columnIdToSymbol(columnId).toSymbolReference()) != null) {
                    // duplicated column ids are not supported
                    return Optional.empty();
                }
            }

            // canonicalize aggregation functions
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(originalSymbolMapping);
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                CanonicalAggregation canonicalAggregation = new CanonicalAggregation(
                        aggregation.getResolvedFunction(),
                        aggregation.getMask().map(canonicalSymbolMapper::map),
                        aggregation.getArguments().stream()
                                .map(canonicalSymbolMapper::map)
                                .collect(toImmutableList()));
                CacheColumnId columnId = canonicalExpressionToColumnId(canonicalAggregation);
                if (assignments.put(columnId, canonicalAggregation) != null) {
                    // duplicated column ids are not supported
                    return Optional.empty();
                }
                if (originalSymbolMapping.containsKey(columnId)) {
                    // might happen if function call is projected by user explicitly
                    return Optional.empty();
                }
                symbolMappingBuilder.put(columnId, symbol);
            }

            // conjuncts that only contain group by symbols are pullable
            Set<CacheColumnId> groupByColumns = groupByColumnsBuilder.build();
            Set<Symbol> groupBySymbols = groupByColumns.stream()
                    .map(CanonicalSubplanExtractor::columnIdToSymbol)
                    .collect(toImmutableSet());
            Map<Boolean, List<Expression>> conjuncts = subplan.getPullableConjuncts().stream()
                    .collect(partitioningBy(expression -> groupBySymbols.containsAll(SymbolsExtractor.extractAll(expression))));
            Set<Expression> pullableConjuncts = ImmutableSet.copyOf(conjuncts.get(true));
            Set<Expression> nonPullableConjuncts = ImmutableSet.copyOf(conjuncts.get(false));

            // validate order of assignments with aggregation output columns
            BiMap<CacheColumnId, Symbol> symbolMapping = symbolMappingBuilder.buildOrThrow();
            verify(ImmutableList.copyOf(assignments.keySet())
                            .equals(node.getOutputSymbols().stream()
                                    .map(symbol -> requireNonNull(symbolMapping.inverse().get(symbol)))
                                    .collect(toImmutableList())),
                    "Assignments order doesn't match aggregation output symbols order");

            return Optional.of(CanonicalSubplan.builderForChildSubplan(new AggregationKey(groupByColumns, nonPullableConjuncts), subplan)
                    .originalPlanNode(node)
                    .originalSymbolMapping(symbolMapping)
                    .groupByColumns(groupByColumns)
                    .assignments(assignments)
                    .pullableConjuncts(pullableConjuncts)
                    .build());
        }

        @Override
        public Optional<CanonicalSubplan> visitTopNRanking(TopNRankingNode node, Void context)
        {
            PlanNode source = node.getSource();

            if (!node.isPartial() || node.getHashSymbol().isPresent() || node.getSpecification().getOrderingScheme().isEmpty()) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = canonicalizeRecursively(source);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();
            BiMap<CacheColumnId, Symbol> originalSymbolMapping = subplan.getOriginalSymbolMapping();

            // Sorting partition columns increases hit ratio and does not affect output rows
            List<CacheColumnId> partitionBy = node.getPartitionBy()
                    .stream().map(partitionKey -> originalSymbolMapping.inverse().get(partitionKey))
                    .sorted(Comparator.comparing(CacheColumnId::toString))
                    .collect(toImmutableList());

            Optional<Map<CacheColumnId, SortOrder>> orderings = canonicalizeOrderingScheme(node.getOrderingScheme(), originalSymbolMapping);
            return orderings.map(orderBy -> CanonicalSubplan.builderForChildSubplan(
                            new TopNRankingKey(
                                    partitionBy,
                                    ImmutableList.copyOf(orderBy.keySet()),
                                    orderBy,
                                    node.getRankingType(),
                                    node.getMaxRankingPerPartition(),
                                    ImmutableSet.copyOf(subplan.getPullableConjuncts())),
                            subplanOptional.get())
                    .originalPlanNode(node)
                    .originalSymbolMapping(originalSymbolMapping)
                    .assignments(subplan.getAssignments())
                    .pullableConjuncts(ImmutableSet.of())
                    .build());
        }

        @Override
        public Optional<CanonicalSubplan> visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource();

            if (node.getStep() != TopNNode.Step.PARTIAL) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = canonicalizeRecursively(source);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();
            BiMap<CacheColumnId, Symbol> originalSymbolMapping = subplan.getOriginalSymbolMapping();
            Optional<Map<CacheColumnId, SortOrder>> orderings = canonicalizeOrderingScheme(node.getOrderingScheme(), originalSymbolMapping);

            return orderings.map(orderBy -> CanonicalSubplan.builderForChildSubplan(
                            new TopNKey(
                                    ImmutableList.copyOf(orderBy.keySet()),
                                    orderBy,
                                    node.getCount(),
                                    ImmutableSet.copyOf(subplan.getPullableConjuncts())),
                            subplanOptional.get())
                    .originalPlanNode(node)
                    .originalSymbolMapping(originalSymbolMapping)
                    .assignments(subplan.getAssignments())
                    .pullableConjuncts(ImmutableSet.of())
                    .build());
        }

        @Override
        public Optional<CanonicalSubplan> visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            if (!node.getAssignments().getExpressions().stream().allMatch(DeterminismEvaluator::isDeterministic)) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional;
            boolean extendSubplan;
            if (source instanceof FilterNode || source instanceof TableScanNode) {
                // subplans consisting of scan <- filter <- project can be represented as one CanonicalSubplan object
                subplanOptional = source.accept(this, null);
                extendSubplan = true;
            }
            else {
                subplanOptional = canonicalizeRecursively(source);
                extendSubplan = false;
            }

            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();
            SymbolMapper canonicalSymbolMapper = subplan.canonicalSymbolMapper();
            // canonicalize projection assignments
            Map<CacheColumnId, Expression> assignments = new LinkedHashMap<>();
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(subplan.getOriginalSymbolMapping());
            for (Symbol symbol : node.getOutputSymbols()) {
                // use formatted canonical expression as column id for non-identity projections
                Expression canonicalExpression = canonicalSymbolMapper.map(node.getAssignments().get(symbol));
                CacheColumnId columnId = canonicalExpressionToColumnId(canonicalExpression);
                if (assignments.put(columnId, canonicalExpression) != null) {
                    // duplicated column ids are not supported
                    if (extendSubplan) {
                        canonicalSubplans.add(subplan);
                    }
                    return Optional.empty();
                }
                // columnId -> symbol could be "identity" and already added by table scan canonicalization
                Symbol originalSymbol = subplan.getOriginalSymbolMapping().get(columnId);
                if (originalSymbol == null) {
                    symbolMappingBuilder.put(columnId, symbol);
                }
                else if (!originalSymbol.equals(symbol)) {
                    // aliasing of column id to multiple symbols is not supported
                    if (extendSubplan) {
                        canonicalSubplans.add(subplan);
                    }
                    return Optional.empty();
                }
            }

            CanonicalSubplanBuilder builder = extendSubplan ?
                    CanonicalSubplan.builderExtending(subplan) :
                    CanonicalSubplan.builderForChildSubplan(new FilterProjectKey(), subplan);
            return Optional.of(builder
                    .originalPlanNode(node)
                    .originalSymbolMapping(symbolMappingBuilder.buildOrThrow())
                    .assignments(assignments)
                    // all symbols (and thus conjuncts) are pullable through projection
                    .pullableConjuncts(subplan.getPullableConjuncts())
                    .build());
        }

        @Override
        public Optional<CanonicalSubplan> visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();
            if (!isDeterministic(node.getPredicate())) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional;
            boolean extendSubplan;
            if (source instanceof TableScanNode) {
                // subplans consisting of scan <- filter <- project can be represented as one CanonicalSubplan object
                subplanOptional = source.accept(this, null);
                extendSubplan = true;
            }
            else {
                subplanOptional = canonicalizeRecursively(source);
                extendSubplan = false;
            }

            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();

            // extract dynamic and static conjuncts
            SymbolMapper canonicalSymbolMapper = subplan.canonicalSymbolMapper();
            ImmutableList.Builder<Expression> conjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> dynamicConjuncts = ImmutableList.builder();
            for (Expression expression : extractConjuncts(node.getPredicate())) {
                if (isDynamicFilter(expression)) {
                    dynamicConjuncts.add(canonicalSymbolMapper.map(expression));
                }
                else {
                    conjuncts.add(canonicalSymbolMapper.map(expression));
                }
            }

            CanonicalSubplanBuilder builder = extendSubplan ?
                    CanonicalSubplan.builderExtending(subplan) :
                    CanonicalSubplan.builderForChildSubplan(new FilterProjectKey(), subplan);
            return Optional.of(builder
                    .originalPlanNode(node)
                    .originalSymbolMapping(subplan.getOriginalSymbolMapping())
                    // assignments from subplan are preserved through filtering
                    .assignments(subplan.getAssignments())
                    .conjuncts(conjuncts.build())
                    .dynamicConjuncts(dynamicConjuncts.build())
                    // all symbols (and thus conjuncts) are projected through filter node
                    .pullableConjuncts(ImmutableSet.<Expression>builder()
                            .addAll(subplan.getPullableConjuncts())
                            .addAll(conjuncts.build())
                            .build())
                    .build());
        }

        @Override
        public Optional<CanonicalSubplan> visitTableScan(TableScanNode node, Void context)
        {
            if (node.isUpdateTarget()) {
                // inserts are not supported
                return Optional.empty();
            }

            if (node.isUseConnectorNodePartitioning()) {
                // TODO: add support for node partitioning
                return Optional.empty();
            }

            TableHandle canonicalTableHandle = cacheMetadata.getCanonicalTableHandle(session, node.getTable());
            Optional<CacheTableId> tableId = cacheMetadata.getCacheTableId(session, canonicalTableHandle)
                    // prepend catalog id
                    .map(id -> new CacheTableId(node.getTable().getCatalogHandle().getId() + ":" + id));
            if (tableId.isEmpty()) {
                return Optional.empty();
            }

            // canonicalize output symbols using column ids
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.builder();
            Map<CacheColumnId, ColumnHandle> columnHandles = new LinkedHashMap<>();
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                ColumnHandle columnHandle = node.getAssignments().get(outputSymbol);
                Optional<CacheColumnId> columnId = cacheMetadata.getCacheColumnId(session, node.getTable(), columnHandle)
                        // Make connector ids always wrapped in '[]' so they are distinguishable from
                        // CacheColumnIds derived from complex expressions.
                        .map(id -> new CacheColumnId("[" + id + "]"));
                if (columnId.isEmpty()) {
                    return Optional.empty();
                }
                symbolMappingBuilder.put(columnId.get(), outputSymbol);
                if (columnHandles.put(columnId.get(), columnHandle) != null) {
                    // duplicated column handles are not supported
                    return Optional.empty();
                }
            }
            BiMap<CacheColumnId, Symbol> symbolMapping = symbolMappingBuilder.build();

            // pass-through canonical output symbols
            Map<CacheColumnId, Expression> assignments = columnHandles.keySet().stream()
                    .collect(toImmutableMap(identity(), id -> columnIdToSymbol(id).toSymbolReference()));

            return Optional.of(CanonicalSubplan.builderForTableScan(
                            new ScanFilterProjectKey(tableId.get()),
                            columnHandles,
                            canonicalTableHandle,
                            tableId.get(),
                            node.isUseConnectorNodePartitioning(),
                            node.getId())
                    .originalPlanNode(node)
                    .originalSymbolMapping(symbolMapping)
                    .assignments(assignments)
                    .pullableConjuncts(ImmutableSet.of())
                    .build());
        }

        private Optional<Map<CacheColumnId, SortOrder>> canonicalizeOrderingScheme(OrderingScheme orderingScheme, BiMap<CacheColumnId, Symbol> originalSymbolMapping)
        {
            Map<CacheColumnId, SortOrder> orderings = new LinkedHashMap<>();
            for (Symbol orderKey : orderingScheme.getOrderBy()) {
                CacheColumnId columnId = requireNonNull(originalSymbolMapping.inverse().get(orderKey));
                if (orderings.put(columnId, orderingScheme.getOrdering(orderKey)) != null) {
                    // duplicated column ids are not supported
                    return Optional.empty();
                }
            }
            return Optional.of(ImmutableMap.copyOf(orderings));
        }

        private Optional<CanonicalSubplan> canonicalizeRecursively(PlanNode node)
        {
            Optional<CanonicalSubplan> subplan = node.accept(this, null);
            subplan.ifPresent(canonicalSubplans::add);
            return subplan;
        }
    }
}
