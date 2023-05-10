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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.SymbolReference;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ExpressionFormatter.formatExpression;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class CanonicalSubplanExtractor
{
    private CanonicalSubplanExtractor() {}

    /**
     * Extracts a list of {@link CanonicalSubplan} for a given plan.
     */
    public static List<CanonicalSubplan> extractCanonicalSubplans(Metadata metadata, CacheMetadata cacheMetadata, Session session, PlanNode root)
    {
        ImmutableList.Builder<CanonicalSubplan> canonicalSubplans = ImmutableList.builder();
        root.accept(new Visitor(metadata, cacheMetadata, session, canonicalSubplans), null).ifPresent(canonicalSubplans::add);
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
        return new Symbol(columnId.toString());
    }

    private static class Visitor
            extends PlanVisitor<Optional<CanonicalSubplan>, Void>
    {
        private final Metadata metadata;
        private final CacheMetadata cacheMetadata;
        private final Session session;
        private final ImmutableList.Builder<CanonicalSubplan> canonicalSubplans;

        public Visitor(Metadata metadata, CacheMetadata cacheMetadata, Session session, ImmutableList.Builder<CanonicalSubplan> canonicalSubplans)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
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
            if (!(source instanceof ProjectNode || source instanceof FilterNode || source instanceof TableScanNode)) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

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
            BiMap<CacheColumnId, Symbol> originalSymbolMapping = subplan.getOriginalSymbolMapping();
            Map<Symbol, Expression> canonicalExpressionMap = originalSymbolMapping.entrySet().stream()
                    .filter(entry -> subplan.getAssignments().containsKey(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getValue, entry -> subplan.getAssignments().get(entry.getKey())));

            Map<CacheColumnId, Expression> assignments = new LinkedHashMap<>();
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(originalSymbolMapping);

            // canonicalize grouping columns
            ImmutableSet.Builder<CacheColumnId> groupByColumns = ImmutableSet.builder();
            for (Symbol groupingKey : node.getGroupingKeys()) {
                Expression groupByExpression = requireNonNull(canonicalExpressionMap.get(groupingKey));
                CacheColumnId columnId = requireNonNull(originalSymbolMapping.inverse().get(groupingKey));
                groupByColumns.add(columnId);
                if (assignments.put(columnId, groupByExpression) != null) {
                    // duplicated column ids are not supported
                    return Optional.empty();
                }
            }

            // canonicalize aggregation functions
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                FunctionCall canonicalAggregation = new FunctionCall(
                        Optional.empty(),
                        aggregation.getResolvedFunction().toQualifiedName(),
                        Optional.empty(),
                        // represent aggregation mask as filter expression
                        aggregation.getMask().map(mask -> requireNonNull(canonicalExpressionMap.get(mask))),
                        Optional.empty(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        aggregation.getArguments().stream()
                                .map(argument -> canonicalExpressionMap.get(new Symbol(((SymbolReference) argument).getName())))
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

            // validate order of assignments with aggregation output columns
            BiMap<CacheColumnId, Symbol> symbolMapping = symbolMappingBuilder.buildOrThrow();
            verify(ImmutableList.copyOf(assignments.keySet())
                            .equals(node.getOutputSymbols().stream()
                                    .map(symbol -> requireNonNull(symbolMapping.inverse().get(symbol)))
                                    .collect(toImmutableList())),
                    "Assignments order doesn't match aggregation output symbols order");

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMapping,
                    Optional.of(groupByColumns.build()),
                    assignments,
                    subplan.getConjuncts(),
                    subplan.getDynamicConjuncts(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning(),
                    subplan.getTableScanId()));
        }

        @Override
        public Optional<CanonicalSubplan> visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            if (!(source instanceof FilterNode || source instanceof TableScanNode)) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            if (!node.getAssignments().getExpressions().stream().allMatch(expression -> isDeterministic(expression, metadata))) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = node.getSource().accept(this, null);
            if (subplanOptional.isEmpty()) {
                return Optional.empty();
            }

            CanonicalSubplan subplan = subplanOptional.get();
            // canonicalize projection assignments
            Map<CacheColumnId, Expression> assignments = new LinkedHashMap<>();
            ImmutableBiMap.Builder<CacheColumnId, Symbol> symbolMappingBuilder = ImmutableBiMap.<CacheColumnId, Symbol>builder()
                    .putAll(subplan.getOriginalSymbolMapping());
            for (Symbol symbol : node.getOutputSymbols()) {
                // use formatted canonical expression as column id for non-identity projections
                Expression canonicalExpression = subplan.canonicalSymbolMapper().map(node.getAssignments().get(symbol));
                CacheColumnId columnId = canonicalExpressionToColumnId(canonicalExpression);
                if (assignments.put(columnId, canonicalExpression) != null) {
                    // duplicated column ids are not supported
                    canonicalizeRecursively(source);
                    return Optional.empty();
                }
                // columnId -> symbol could be "identity" and already added by table scan canonicalization
                Symbol originalSymbol = subplan.getOriginalSymbolMapping().get(columnId);
                if (originalSymbol == null) {
                    symbolMappingBuilder.put(columnId, symbol);
                }
                else if (!originalSymbol.equals(symbol)) {
                    // aliasing of column id to multiple symbols is not supported
                    canonicalizeRecursively(source);
                    return Optional.empty();
                }
            }

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMappingBuilder.buildOrThrow(),
                    Optional.empty(),
                    assignments,
                    subplan.getConjuncts(),
                    subplan.getDynamicConjuncts(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning(),
                    subplan.getTableScanId()));
        }

        @Override
        public Optional<CanonicalSubplan> visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource();
            if (!(source instanceof TableScanNode)) {
                // only scan <- filter <- project or scan <- project plans are supported
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            if (!isDeterministic(node.getPredicate(), metadata)) {
                canonicalizeRecursively(source);
                return Optional.empty();
            }

            Optional<CanonicalSubplan> subplanOptional = source.accept(this, null);
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

            return Optional.of(new CanonicalSubplan(
                    node,
                    subplan.getOriginalSymbolMapping(),
                    Optional.empty(),
                    subplan.getAssignments(),
                    conjuncts.build(),
                    dynamicConjuncts.build(),
                    subplan.getColumnHandles(),
                    subplan.getTable(),
                    subplan.getTableId(),
                    subplan.isUseConnectorNodePartitioning(),
                    subplan.getTableScanId()));
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

            return Optional.of(new CanonicalSubplan(
                    node,
                    symbolMapping,
                    Optional.empty(),
                    assignments,
                    // No filters in table scan. Relevant pushed predicates are
                    // part of CacheTableId, so such subplans won't be considered
                    // as similar.
                    ImmutableList.of(),
                    // no dynamic filters in table scan
                    ImmutableList.of(),
                    columnHandles,
                    canonicalTableHandle,
                    tableId.get(),
                    node.isUseConnectorNodePartitioning(),
                    node.getId()));
        }

        private Optional<CanonicalSubplan> canonicalizeRecursively(PlanNode node)
        {
            Optional<CanonicalSubplan> subplan = node.accept(this, null);
            subplan.ifPresent(canonicalSubplans::add);
            return subplan;
        }
    }
}
