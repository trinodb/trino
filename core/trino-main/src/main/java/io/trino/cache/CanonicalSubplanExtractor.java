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
import com.google.common.graph.Traverser;
import io.trino.Session;
import io.trino.cache.CanonicalSubplan.AggregationKey;
import io.trino.cache.CanonicalSubplan.CanonicalSubplanBuilder;
import io.trino.cache.CanonicalSubplan.FilterProjectKey;
import io.trino.cache.CanonicalSubplan.Key;
import io.trino.cache.CanonicalSubplan.ScanFilterProjectKey;
import io.trino.cache.CanonicalSubplan.TopNKey;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionFormatter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
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

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static io.trino.cache.CanonicalSubplan.TopNRankingKey;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.ExpressionFormatter.formatExpression;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;

public final class CanonicalSubplanExtractor
{
    private CanonicalSubplanExtractor() {}

    /**
     * Extracts a list of {@link CanonicalSubplan} for a given plan.
     */
    public static List<CanonicalSubplan> extractCanonicalSubplans(PlannerContext plannerContext, Session session, PlanNode root)
    {
        ImmutableList.Builder<CanonicalSubplan> canonicalSubplans = ImmutableList.builder();
        root.accept(new Visitor(plannerContext, session, canonicalSubplans), null).ifPresent(canonicalSubplans::add);
        return canonicalSubplans.build();
    }

    public static CacheColumnId canonicalSymbolToColumnId(Symbol symbol)
    {
        requireNonNull(symbol, "symbol is null");
        return canonicalExpressionToColumnId(symbol.toSymbolReference());
    }

    public static CacheColumnId canonicalAggregationToColumnId(CanonicalAggregation aggregation)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("aggregation ")
                .append(aggregation.resolvedFunction().name().toString())
                .append('(')
                .append(aggregation.arguments().stream()
                        .map(ExpressionFormatter::formatExpression)
                        .collect(joining(", ")))
                .append(')');
        aggregation.mask().ifPresent(mask -> {
            builder.append(" FILTER (WHERE ").append(formatExpression(mask.toSymbolReference())).append(')');
        });
        return new CacheColumnId("(" + builder + ")");
    }

    public static CacheColumnId canonicalExpressionToColumnId(Expression expression)
    {
        requireNonNull(expression, "expression is null");
        if (expression instanceof Reference symbolReference) {
            // symbol -> column id translation should be reversible via columnIdToSymbol method
            return new CacheColumnId(symbolReference.name());
        }

        // Make CacheColumnIds for complex expressions always wrapped in '()' so they are distinguishable from
        // CacheColumnIds derived from connectors.
        return new CacheColumnId("(" + formatExpression(expression) + ")");
    }

    public static Symbol columnIdToSymbol(CacheColumnId columnId, Type type)
    {
        requireNonNull(columnId, "columnId is null");
        return new Symbol(type, columnId.toString());
    }

    private static class Visitor
            extends PlanVisitor<Optional<CanonicalSubplan>, Void>
    {
        private final PlannerContext plannerContext;
        private final Session session;
        private final ImmutableList.Builder<CanonicalSubplan> canonicalSubplans;

        public Visitor(PlannerContext plannerContext, Session session, ImmutableList.Builder<CanonicalSubplan> canonicalSubplans)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
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
                    aggregation.getArguments().stream().allMatch(argument -> argument instanceof Reference)
                            && !aggregation.isDistinct()
                            && aggregation.getFilter().isEmpty()
                            && aggregation.getOrderingScheme().isEmpty()
                            && aggregation.getResolvedFunction().deterministic());

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
            Map<CacheColumnId, CacheExpression> assignments = new LinkedHashMap<>();

            // canonicalize grouping columns
            ImmutableSet.Builder<CacheColumnId> groupByColumnsBuilder = ImmutableSet.builder();
            for (Symbol groupingKey : node.getGroupingKeys()) {
                CacheColumnId columnId = requireNonNull(originalSymbolMapping.inverse().get(groupingKey));
                groupByColumnsBuilder.add(columnId);
                if (assignments.put(columnId, CacheExpression.ofProjection(columnIdToSymbol(columnId, groupingKey.type()).toSymbolReference())) != null) {
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
                CacheColumnId columnId = canonicalAggregationToColumnId(canonicalAggregation);
                if (assignments.put(columnId, CacheExpression.ofAggregation(canonicalAggregation)) != null) {
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
            BiMap<CacheColumnId, Symbol> symbolMapping = symbolMappingBuilder.buildOrThrow();
            Set<CacheColumnId> groupByColumns = groupByColumnsBuilder.build();
            Set<Symbol> groupBySymbols = groupByColumns.stream()
                    .map(id -> columnIdToSymbol(id, symbolMapping.get(id).type()))
                    .collect(toImmutableSet());
            Map<Boolean, List<Expression>> conjuncts = subplan.getPullableConjuncts().stream()
                    .collect(partitioningBy(expression -> groupBySymbols.containsAll(SymbolsExtractor.extractAll(expression))));
            Set<Expression> pullableConjuncts = ImmutableSet.copyOf(conjuncts.get(true));
            Set<Expression> nonPullableConjuncts = ImmutableSet.copyOf(conjuncts.get(false));

            // validate order of assignments with aggregation output columns
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

            if (!node.isPartial() || node.getHashSymbol().isPresent() || node.getSpecification().orderingScheme().isEmpty()) {
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

            if (containsLambdaExpression(node)) {
                // lambda expressions are not supported
                canonicalizeRecursively(source);
                return Optional.empty();
            }

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
            Map<CacheColumnId, CacheExpression> assignments = new LinkedHashMap<>();
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(subplan.getOriginalSymbolMapping());
            for (Symbol symbol : node.getOutputSymbols()) {
                // use formatted canonical expression as column id for non-identity projections
                Expression canonicalExpression = canonicalSymbolMapper.map(node.getAssignments().get(symbol));
                CacheColumnId columnId = canonicalExpressionToColumnId(canonicalExpression);
                if (assignments.put(columnId, CacheExpression.ofProjection(canonicalExpression)) != null) {
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

            // Unsafe expressions that could throw an error should be evaluated only for the rows from
            // original subquery. Therefore, common subplan predicate must match original subplan predicate.
            // If common subplan predicate is wider, then unsafe expressions could fail even though
            // evaluation of the original subplan would be successful.
            boolean safeProjections = node.getAssignments().getExpressions().stream().noneMatch(expression -> mayFail(plannerContext, expression));
            Set<Expression> requiredConjuncts = !safeProjections ? subplan.getPullableConjuncts() : ImmutableSet.of();

            CanonicalSubplanBuilder builder = extendSubplan ?
                    CanonicalSubplan.builderExtending(setRequiredConjuncts(subplan.getKey(), requiredConjuncts), subplan) :
                    CanonicalSubplan.builderForChildSubplan(new FilterProjectKey(requiredConjuncts), subplan);
            return Optional.of(builder
                    .originalPlanNode(node)
                    .originalSymbolMapping(symbolMappingBuilder.buildOrThrow())
                    .assignments(assignments)
                    // all symbols (and thus conjuncts) are pullable through projection
                    .pullableConjuncts(subplan.getPullableConjuncts())
                    .build());
        }

        private Key setRequiredConjuncts(Key key, Set<Expression> requiredConjuncts)
        {
            switch (key) {
                case ScanFilterProjectKey scanFilterProjectKey -> {
                    checkArgument(scanFilterProjectKey.requiredConjuncts().isEmpty());
                    return new ScanFilterProjectKey(scanFilterProjectKey.tableId(), requiredConjuncts);
                }
                case FilterProjectKey filterProjectKey -> {
                    checkArgument(filterProjectKey.requiredConjuncts().isEmpty());
                    return new FilterProjectKey(requiredConjuncts);
                }
                default -> throw new IllegalStateException("Unsupported key type: " + key);
            }
        }

        @Override
        public Optional<CanonicalSubplan> visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();

            if (containsLambdaExpression(node)) {
                // lambda expressions are not supported
                canonicalizeFilterSource(node);
                return Optional.empty();
            }

            if (!isDeterministic(node.getPredicate())) {
                canonicalizeFilterSource(node);
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
                    CanonicalSubplan.builderForChildSubplan(new FilterProjectKey(ImmutableSet.of()), subplan);
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

        private void canonicalizeFilterSource(FilterNode node)
        {
            if (!containsDynamicFilter(node.getPredicate())) {
                // Filter source must be a table scan if filter predicate contains dynamic filter.
                // Hence, such filter nodes cannot be canonicalized separately from the table scan.
                // Otherwise, there is a possibility that "load from cache" alternative is created below the filter node.
                canonicalizeRecursively(node.getSource());
            }
        }

        private boolean containsDynamicFilter(Expression expression)
        {
            return !extractDynamicFilters(expression).getDynamicConjuncts().isEmpty();
        }

        private boolean containsLambdaExpression(PlanNode node)
        {
            return extractExpressions(node).stream().anyMatch(this::containsLambdaExpression);
        }

        private boolean containsLambdaExpression(Expression expression)
        {
            return stream(Traverser.<Expression>forTree(Expression::children).depthFirstPreOrder(expression))
                    .anyMatch(instanceOf(Lambda.class));
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

            CacheMetadata cacheMetadata = plannerContext.getCacheMetadata();
            TableHandle canonicalTableHandle = cacheMetadata.getCanonicalTableHandle(session, node.getTable());
            Optional<CacheTableId> tableId = cacheMetadata.getCacheTableId(session, canonicalTableHandle)
                    // prepend catalog id
                    .map(id -> new CacheTableId(node.getTable().catalogHandle().getId() + ":" + id));
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
            Map<CacheColumnId, CacheExpression> assignments = columnHandles.keySet().stream().collect(toImmutableMap(
                    identity(),
                    id -> CacheExpression.ofProjection(columnIdToSymbol(id, symbolMapping.get(id).type()).toSymbolReference())));

            return Optional.of(CanonicalSubplan.builderForTableScan(
                            new ScanFilterProjectKey(tableId.get(), ImmutableSet.of()),
                            columnHandles,
                            canonicalTableHandle,
                            tableId.get(),
                            canonicalizeEnforcedConstraint(node),
                            node.isUseConnectorNodePartitioning(),
                            node.getId())
                    .originalPlanNode(node)
                    .originalSymbolMapping(symbolMapping)
                    .assignments(assignments)
                    .pullableConjuncts(ImmutableSet.of())
                    .build());
        }

        private TupleDomain<CacheColumnId> canonicalizeEnforcedConstraint(TableScanNode node)
        {
            // table predicate might contain all pushed down predicates to connector whereas TableScanNode#enforcedConstraints are pruned by visibility as output
            TupleDomain<ColumnHandle> tablePredicate = plannerContext.getMetadata().getTableProperties(session, node.getTable()).getPredicate();
            if (tablePredicate.isNone()) {
                return TupleDomain.none();
            }
            if (tablePredicate.isAll()) {
                return TupleDomain.all();
            }

            Map<ColumnHandle, Domain> domains = tablePredicate.getDomains().get();
            HashMap<CacheColumnId, Domain> result = new LinkedHashMap<>(domains.size());
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                Optional<CacheColumnId> columnId = plannerContext.getCacheMetadata().getCacheColumnId(session, node.getTable(), entry.getKey());
                if (columnId.isEmpty()) {
                    return TupleDomain.all();
                }

                Domain domain = entry.getValue();
                checkState(result.put(columnId.get(), domain) == null || result.get(columnId.get()).equals(domain),
                        format("Columns with same ids should have same domains: %s maps to %s and %s", entry.getKey(), entry.getValue(), domain));
            }
            return TupleDomain.withColumnDomains(result);
        }

        private Optional<Map<CacheColumnId, SortOrder>> canonicalizeOrderingScheme(OrderingScheme orderingScheme, BiMap<CacheColumnId, Symbol> originalSymbolMapping)
        {
            Map<CacheColumnId, SortOrder> orderings = new LinkedHashMap<>();
            for (Symbol orderKey : orderingScheme.orderBy()) {
                CacheColumnId columnId = requireNonNull(originalSymbolMapping.inverse().get(orderKey));
                if (orderings.put(columnId, orderingScheme.ordering(orderKey)) != null) {
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
