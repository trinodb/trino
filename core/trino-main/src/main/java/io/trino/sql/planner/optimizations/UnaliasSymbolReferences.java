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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.NodeAndMappings;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.UpdateNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SymbolReference;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.DynamicFilters.getDescriptor;
import static io.trino.sql.DynamicFilters.replaceDynamicFilterId;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.SymbolMapper.symbolMapper;
import static io.trino.sql.planner.optimizations.SymbolMapper.symbolReallocator;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * Re-maps symbol references that are just aliases of each other (e.g., due to projections like {@code $0 := $1})
 * <p/>
 * E.g.,
 * <p/>
 * {@code Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 * <p/>
 * gets rewritten as
 * <p/>
 * {@code Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 */
public class UnaliasSymbolReferences
        implements PlanOptimizer
{
    private final Metadata metadata;

    public UnaliasSymbolReferences(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        Visitor visitor = new Visitor(metadata, SymbolMapper::symbolMapper);
        PlanAndMappings result = plan.accept(visitor, UnaliasContext.empty());
        return updateDynamicFilterIds(result.getRoot(), visitor.getDynamicFilterIdMap());
    }

    /**
     * Replace all symbols in the plan with new symbols.
     * The returned plan has different output than the original plan. Also, the order of symbols might change during symbol replacement.
     * Symbols in the list `fields` are replaced maintaining the order so they might be used to match original symbols with their replacements.
     * Replacing symbols helps avoid collisions when symbols or parts of the plan are reused.
     */
    public NodeAndMappings reallocateSymbols(PlanNode plan, List<Symbol> fields, SymbolAllocator symbolAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(fields, "fields is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");

        Visitor visitor = new Visitor(metadata, mapping -> symbolReallocator(mapping, symbolAllocator));
        PlanAndMappings result = plan.accept(visitor, UnaliasContext.empty());
        return new NodeAndMappings(updateDynamicFilterIds(result.getRoot(), visitor.getDynamicFilterIdMap()), symbolMapper(result.getMappings()).map(fields));
    }

    private PlanNode updateDynamicFilterIds(PlanNode resultNode, Map<DynamicFilterId, DynamicFilterId> dynamicFilterIdMap)
    {
        if (!dynamicFilterIdMap.isEmpty()) {
            resultNode = rewriteWith(new DynamicFilterVisitor(metadata, dynamicFilterIdMap), resultNode);
        }
        return resultNode;
    }

    private static class Visitor
            extends PlanVisitor<PlanAndMappings, UnaliasContext>
    {
        private final Metadata metadata;
        private final Function<Map<Symbol, Symbol>, SymbolMapper> mapperProvider;
        private final Map<DynamicFilterId, DynamicFilterId> dynamicFilterIdMap = new HashMap<>();

        public Visitor(Metadata metadata, Function<Map<Symbol, Symbol>, SymbolMapper> mapperProvider)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.mapperProvider = requireNonNull(mapperProvider, "mapperProvider is null");
        }

        private SymbolMapper symbolMapper(Map<Symbol, Symbol> mappings)
        {
            return mapperProvider.apply(mappings);
        }

        public Map<DynamicFilterId, DynamicFilterId> getDynamicFilterIdMap()
        {
            return ImmutableMap.copyOf(dynamicFilterIdMap);
        }

        @Override
        protected PlanAndMappings visitPlan(PlanNode node, UnaliasContext context)
        {
            throw new UnsupportedOperationException("Unsupported plan node " + node.getClass().getSimpleName());
        }

        @Override
        public PlanAndMappings visitAggregation(AggregationNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            AggregationNode rewrittenAggregation = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenAggregation, mapping);
        }

        @Override
        public PlanAndMappings visitGroupId(GroupIdNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            GroupIdNode rewrittenGroupId = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenGroupId, mapping);
        }

        @Override
        public PlanAndMappings visitExplainAnalyze(ExplainAnalyzeNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            Symbol newOutputSymbol = mapper.map(node.getOutputSymbol());
            List<Symbol> actualOutputs = mapper.map(node.getActualOutputs());

            return new PlanAndMappings(
                    new ExplainAnalyzeNode(node.getId(), rewrittenSource.getRoot(), newOutputSymbol, actualOutputs, node.isVerbose()),
                    mapping);
        }

        @Override
        public PlanAndMappings visitMarkDistinct(MarkDistinctNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            Symbol newMarkerSymbol = mapper.map(node.getMarkerSymbol());
            List<Symbol> newDistinctSymbols = mapper.mapAndDistinct(node.getDistinctSymbols());
            Optional<Symbol> newHashSymbol = node.getHashSymbol().map(mapper::map);

            return new PlanAndMappings(
                    new MarkDistinctNode(
                            node.getId(),
                            rewrittenSource.getRoot(),
                            newMarkerSymbol,
                            newDistinctSymbols,
                            newHashSymbol),
                    mapping);
        }

        @Override
        public PlanAndMappings visitUnnest(UnnestNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            List<Symbol> newReplicateSymbols = mapper.mapAndDistinct(node.getReplicateSymbols());

            ImmutableList.Builder<UnnestNode.Mapping> newMappings = ImmutableList.builder();
            for (UnnestNode.Mapping unnestMapping : node.getMappings()) {
                newMappings.add(new UnnestNode.Mapping(mapper.map(unnestMapping.getInput()), mapper.map(unnestMapping.getOutputs())));
            }

            Optional<Symbol> newOrdinalitySymbol = node.getOrdinalitySymbol().map(mapper::map);
            Optional<Expression> newFilter = node.getFilter().map(mapper::map);

            return new PlanAndMappings(
                    new UnnestNode(
                            node.getId(),
                            rewrittenSource.getRoot(),
                            newReplicateSymbols,
                            newMappings.build(),
                            newOrdinalitySymbol,
                            node.getJoinType(),
                            newFilter),
                    mapping);
        }

        @Override
        public PlanAndMappings visitWindow(WindowNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            WindowNode rewrittenWindow = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenWindow, mapping);
        }

        @Override
        public PlanAndMappings visitPatternRecognition(PatternRecognitionNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            PatternRecognitionNode rewrittenPatternRecognition = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenPatternRecognition, mapping);
        }

        @Override
        public PlanAndMappings visitTableScan(TableScanNode node, UnaliasContext context)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper mapper = symbolMapper(mapping);

            List<Symbol> newOutputs = mapper.map(node.getOutputSymbols());

            Optional<PlanNodeStatsEstimate> statistics = node.getStatistics();
            PlanNodeStatsEstimate.Builder newStatistics = PlanNodeStatsEstimate.builder();
            statistics.ifPresent(stats -> newStatistics.setOutputRowCount(stats.getOutputRowCount()));
            Map<Symbol, ColumnHandle> newAssignments = new HashMap<>();
            node.getAssignments().forEach((symbol, handle) -> {
                Symbol newSymbol = mapper.map(symbol);
                newAssignments.put(newSymbol, handle);
                statistics.ifPresent(stats -> newStatistics.addSymbolStatistics(newSymbol, stats.getSymbolStatistics(symbol)));
            });

            return new PlanAndMappings(
                    new TableScanNode(
                            node.getId(),
                            node.getTable(),
                            newOutputs,
                            newAssignments,
                            node.getEnforcedConstraint(),
                            statistics.isPresent() ? Optional.of(newStatistics.build()) : Optional.empty(),
                            node.isUpdateTarget(),
                            node.getUseConnectorNodePartitioning()),
                    mapping);
        }

        @Override
        public PlanAndMappings visitExchange(ExchangeNode node, UnaliasContext context)
        {
            ImmutableList.Builder<PlanNode> rewrittenChildren = ImmutableList.builder();
            ImmutableList.Builder<List<Symbol>> rewrittenInputsBuilder = ImmutableList.builder();

            // rewrite child and map corresponding input list accordingly to the child's mapping
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanAndMappings rewrittenChild = node.getSources().get(i).accept(this, context);
                rewrittenChildren.add(rewrittenChild.getRoot());
                SymbolMapper mapper = symbolMapper(new HashMap<>(rewrittenChild.getMappings()));
                rewrittenInputsBuilder.add(mapper.map(node.getInputs().get(i)));
            }
            List<List<Symbol>> rewrittenInputs = rewrittenInputsBuilder.build();

            // canonicalize ExchangeNode outputs
            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper mapper = symbolMapper(mapping);
            List<Symbol> rewrittenOutputs = mapper.map(node.getOutputSymbols());

            // sanity check: assert that duplicate outputs result from same inputs
            Map<Symbol, List<Symbol>> outputsToInputs = new HashMap<>();
            for (int i = 0; i < rewrittenOutputs.size(); i++) {
                ImmutableList.Builder<Symbol> inputsBuilder = ImmutableList.builder();
                for (List<Symbol> inputs : rewrittenInputs) {
                    inputsBuilder.add(inputs.get(i));
                }
                List<Symbol> inputs = inputsBuilder.build();
                List<Symbol> previous = outputsToInputs.put(rewrittenOutputs.get(i), inputs);
                checkState(previous == null || inputs.equals(previous), "different inputs mapped to the same output symbol");
            }

            // derive new mappings for ExchangeNode output symbols
            Map<Symbol, Symbol> newMapping = new HashMap<>();

            // 1. for a single ExchangeNode source, map outputs to inputs
            if (rewrittenInputs.size() == 1) {
                for (int i = 0; i < rewrittenOutputs.size(); i++) {
                    Symbol output = rewrittenOutputs.get(i);
                    Symbol input = rewrittenInputs.get(0).get(i);
                    if (!output.equals(input)) {
                        newMapping.put(output, input);
                    }
                }
            }
            else {
                // 2. for multiple ExchangeNode sources, if different output symbols result from the same lists of canonical input symbols, map all those outputs to the same symbol
                Map<List<Symbol>, Symbol> inputsToOutputs = new HashMap<>();
                for (int i = 0; i < rewrittenOutputs.size(); i++) {
                    ImmutableList.Builder<Symbol> inputsBuilder = ImmutableList.builder();
                    for (List<Symbol> inputs : rewrittenInputs) {
                        inputsBuilder.add(inputs.get(i));
                    }
                    List<Symbol> inputs = inputsBuilder.build();
                    Symbol previous = inputsToOutputs.get(inputs);
                    if (previous == null || rewrittenOutputs.get(i).equals(previous)) {
                        inputsToOutputs.put(inputs, rewrittenOutputs.get(i));
                    }
                    else {
                        newMapping.put(rewrittenOutputs.get(i), previous);
                    }
                }
            }

            Map<Symbol, Symbol> outputMapping = new HashMap<>();
            outputMapping.putAll(mapping);
            outputMapping.putAll(newMapping);

            mapper = symbolMapper(outputMapping);

            // deduplicate outputs and prune input symbols lists accordingly
            List<List<Symbol>> newInputs = new ArrayList<>();
            for (int i = 0; i < node.getInputs().size(); i++) {
                newInputs.add(new ArrayList<>());
            }
            ImmutableList.Builder<Symbol> newOutputs = ImmutableList.builder();
            Set<Symbol> addedOutputs = new HashSet<>();
            for (int i = 0; i < rewrittenOutputs.size(); i++) {
                Symbol output = mapper.map(rewrittenOutputs.get(i));
                if (addedOutputs.add(output)) {
                    newOutputs.add(output);
                    for (int j = 0; j < rewrittenInputs.size(); j++) {
                        newInputs.get(j).add(rewrittenInputs.get(j).get(i));
                    }
                }
            }

            // rewrite PartitioningScheme
            PartitioningScheme newPartitioningScheme = mapper.map(node.getPartitioningScheme(), newOutputs.build());

            // rewrite OrderingScheme
            Optional<OrderingScheme> newOrderingScheme = node.getOrderingScheme().map(mapper::map);

            return new PlanAndMappings(
                    new ExchangeNode(
                            node.getId(),
                            node.getType(),
                            node.getScope(),
                            newPartitioningScheme,
                            rewrittenChildren.build(),
                            newInputs,
                            newOrderingScheme),
                    outputMapping);
        }

        @Override
        public PlanAndMappings visitRemoteSource(RemoteSourceNode node, UnaliasContext context)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper mapper = symbolMapper(mapping);

            List<Symbol> newOutputs = mapper.mapAndDistinct(node.getOutputSymbols());
            Optional<OrderingScheme> newOrderingScheme = node.getOrderingScheme().map(mapper::map);

            return new PlanAndMappings(
                    new RemoteSourceNode(
                            node.getId(),
                            node.getSourceFragmentIds(),
                            newOutputs,
                            newOrderingScheme,
                            node.getExchangeType(),
                            node.getRetryPolicy()),
                    mapping);
        }

        @Override
        public PlanAndMappings visitOffset(OffsetNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);

            return new PlanAndMappings(
                    node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
                    rewrittenSource.getMappings());
        }

        @Override
        public PlanAndMappings visitLimit(LimitNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            LimitNode rewrittenLimit = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenLimit, mapping);
        }

        @Override
        public PlanAndMappings visitDistinctLimit(DistinctLimitNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            DistinctLimitNode rewrittenDistinctLimit = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenDistinctLimit, mapping);
        }

        @Override
        public PlanAndMappings visitSample(SampleNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);

            return new PlanAndMappings(
                    node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
                    rewrittenSource.getMappings());
        }

        @Override
        public PlanAndMappings visitValues(ValuesNode node, UnaliasContext context)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper mapper = symbolMapper(mapping);

            // nothing to map: no output symbols and no expressions
            if (node.getRows().isEmpty()) {
                return new PlanAndMappings(node, mapping);
            }

            // if any of ValuesNode's rows is specified by expression other than Row, we cannot reason about individual fields
            if (node.getRows().get().stream().anyMatch(row -> !(row instanceof Row))) {
                List<Expression> newRows = node.getRows().get().stream()
                        .map(mapper::map)
                        .collect(toImmutableList());
                List<Symbol> newOutputs = node.getOutputSymbols().stream()
                        .map(mapper::map)
                        .distinct()
                        .collect(toImmutableList());
                checkState(newOutputs.size() == node.getOutputSymbols().size(), "duplicate output symbol in Values");
                return new PlanAndMappings(
                        new ValuesNode(node.getId(), newOutputs, newRows),
                        mapping);
            }

            ImmutableList.Builder<SimpleEntry<Symbol, List<Expression>>> rewrittenAssignmentsBuilder = ImmutableList.builder();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
                for (Expression row : node.getRows().get()) {
                    expressionsBuilder.add(mapper.map(((Row) row).getItems().get(i)));
                }
                rewrittenAssignmentsBuilder.add(new SimpleEntry<>(mapper.map(node.getOutputSymbols().get(i)), expressionsBuilder.build()));
            }
            List<SimpleEntry<Symbol, List<Expression>>> rewrittenAssignments = rewrittenAssignmentsBuilder.build();

            // prune duplicate outputs and corresponding expressions. assert that duplicate outputs result from same input expressions
            Map<Symbol, List<Expression>> deduplicateAssignments = rewrittenAssignments.stream()
                    .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue, (previous, current) -> {
                        checkState(previous.equals(current), "different expressions mapped to the same output symbol");
                        return previous;
                    }));

            List<Symbol> newOutputs = deduplicateAssignments.keySet().stream()
                    .collect(toImmutableList());

            List<ImmutableList.Builder<Expression>> newRows = new ArrayList<>(node.getRowCount());
            for (int i = 0; i < node.getRowCount(); i++) {
                newRows.add(ImmutableList.builder());
            }
            for (List<Expression> expressions : deduplicateAssignments.values()) {
                for (int i = 0; i < expressions.size(); i++) {
                    newRows.get(i).add(expressions.get(i));
                }
            }

            return new PlanAndMappings(
                    new ValuesNode(
                            node.getId(),
                            newOutputs,
                            newRows.stream()
                                    .map(ImmutableList.Builder::build)
                                    .map(Row::new)
                                    .collect(toImmutableList())),
                    mapping);
        }

        @Override
        public PlanAndMappings visitTableDelete(TableDeleteNode node, UnaliasContext context)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper mapper = symbolMapper(mapping);

            Symbol newOutput = mapper.map(node.getOutput());

            return new PlanAndMappings(
                    new TableDeleteNode(node.getId(), node.getTarget(), newOutput),
                    mapping);
        }

        @Override
        public PlanAndMappings visitDelete(DeleteNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            Symbol newRowId = mapper.map(node.getRowId());
            List<Symbol> newOutputs = mapper.map(node.getOutputSymbols());

            return new PlanAndMappings(
                    new DeleteNode(
                            node.getId(),
                            rewrittenSource.getRoot(),
                            node.getTarget(),
                            newRowId,
                            newOutputs),
                    mapping);
        }

        @Override
        public PlanAndMappings visitUpdate(UpdateNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            Symbol newRowId = mapper.map(node.getRowId());
            List<Symbol> newColumnValueSymbols = mapper.map(node.getColumnValueAndRowIdSymbols());
            List<Symbol> newOutputs = mapper.map(node.getOutputSymbols());

            return new PlanAndMappings(
                    new UpdateNode(
                            node.getId(),
                            rewrittenSource.getRoot(),
                            node.getTarget(),
                            newRowId,
                            newColumnValueSymbols,
                            newOutputs),
                    mapping);
        }

        @Override
        public PlanAndMappings visitTableExecute(TableExecuteNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            TableExecuteNode rewrittenTableExecute = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenTableExecute, mapping);
        }

        @Override
        public PlanAndMappings visitSimpleTableExecuteNode(SimpleTableExecuteNode node, UnaliasContext context)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper mapper = symbolMapper(mapping);
            Symbol newOutput = mapper.map(node.getOutput());

            return new PlanAndMappings(
                    new SimpleTableExecuteNode(
                            node.getId(),
                            newOutput,
                            node.getExecuteHandle()),
                    mapping);
        }

        @Override
        public PlanAndMappings visitStatisticsWriterNode(StatisticsWriterNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            StatisticsWriterNode rewrittenStatisticsWriter = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenStatisticsWriter, mapping);
        }

        @Override
        public PlanAndMappings visitRefreshMaterializedView(RefreshMaterializedViewNode node, UnaliasContext context)
        {
            return new PlanAndMappings(node, ImmutableMap.of());
        }

        @Override
        public PlanAndMappings visitTableWriter(TableWriterNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            TableWriterNode rewrittenTableWriter = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenTableWriter, mapping);
        }

        @Override
        public PlanAndMappings visitTableFinish(TableFinishNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            TableFinishNode rewrittenTableFinish = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenTableFinish, mapping);
        }

        @Override
        public PlanAndMappings visitRowNumber(RowNumberNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            RowNumberNode rewrittenRowNumber = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenRowNumber, mapping);
        }

        @Override
        public PlanAndMappings visitTopNRanking(TopNRankingNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            TopNRankingNode rewrittenTopNRanking = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenTopNRanking, mapping);
        }

        @Override
        public PlanAndMappings visitTopN(TopNNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            TopNNode rewrittenTopN = mapper.map(node, rewrittenSource.getRoot());

            return new PlanAndMappings(rewrittenTopN, mapping);
        }

        @Override
        public PlanAndMappings visitSort(SortNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            OrderingScheme newOrderingScheme = mapper.map(node.getOrderingScheme());

            return new PlanAndMappings(
                    new SortNode(node.getId(), rewrittenSource.getRoot(), newOrderingScheme, node.isPartial()),
                    mapping);
        }

        @Override
        public PlanAndMappings visitFilter(FilterNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            Expression newPredicate = mapper.map(node.getPredicate());

            return new PlanAndMappings(
                    new FilterNode(node.getId(), rewrittenSource.getRoot(), newPredicate),
                    mapping);
        }

        @Override
        public PlanAndMappings visitProject(ProjectNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);

            // Assignment of a form `s -> x` establishes new semantics for symbol s.
            // It is possible though that symbol `s` is present in the source plan, and represents the same or different semantics.
            // As a consequence, any symbol mapping derived from the source plan involving symbol `s` becomes potentially invalid,
            // e.g. `s -> y` or `y -> s` refer to the old semantics of symbol `s`.
            // In such case, the underlying mappings are only used to map projection assignments' values to ensure consistency with the source plan.
            // They aren't used to map projection outputs to avoid:
            // - errors from duplicate assignments
            // - incorrect results from mixed semantics of symbols
            // Also, the underlying mappings aren't passed up the plan, and new mappings aren't derived from projection assignments
            // (with the exception for "deduplicating" mappings for repeated assignments).
            // This can be thought of as a "cut-off" at the point of potentially changed semantics.
            // Note: the issue of ambiguous symbols does not apply to symbols involved in context (correlation) mapping.
            // Those symbols are supposed to represent constant semantics throughout the plan.

            Assignments assignments = node.getAssignments();
            Set<Symbol> newlyAssignedSymbols = assignments.filter(output -> !assignments.isIdentity(output)).getSymbols();
            Set<Symbol> symbolsInSourceMapping = ImmutableSet.<Symbol>builder()
                    .addAll(rewrittenSource.getMappings().keySet())
                    .addAll(rewrittenSource.getMappings().values())
                    .build();
            Set<Symbol> symbolsInCorrelationMapping = ImmutableSet.<Symbol>builder()
                    .addAll(context.getCorrelationMapping().keySet())
                    .addAll(context.getCorrelationMapping().values())
                    .build();
            boolean ambiguousSymbolsPresent = !Sets.intersection(newlyAssignedSymbols, Sets.difference(symbolsInSourceMapping, symbolsInCorrelationMapping)).isEmpty();

            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            // canonicalize ProjectNode assignments
            ImmutableList.Builder<Map.Entry<Symbol, Expression>> rewrittenAssignments = ImmutableList.builder();
            for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
                rewrittenAssignments.add(new SimpleEntry<>(
                        ambiguousSymbolsPresent ? assignment.getKey() : mapper.map(assignment.getKey()),
                        mapper.map(assignment.getValue())));
            }

            // deduplicate assignments
            Map<Symbol, Expression> deduplicateAssignments = rewrittenAssignments.build().stream()
                    .distinct()
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            // derive new mappings for ProjectNode output symbols
            Map<Symbol, Symbol> newMapping = mappingFromAssignments(deduplicateAssignments, ambiguousSymbolsPresent);

            Map<Symbol, Symbol> outputMapping = new HashMap<>();
            outputMapping.putAll(ambiguousSymbolsPresent ? context.getCorrelationMapping() : mapping);
            outputMapping.putAll(newMapping);

            mapper = symbolMapper(outputMapping);

            // build new Assignments with canonical outputs
            // duplicate entries will be removed by the Builder
            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<Symbol, Expression> assignment : deduplicateAssignments.entrySet()) {
                newAssignments.put(mapper.map(assignment.getKey()), assignment.getValue());
            }

            return new PlanAndMappings(
                    new ProjectNode(node.getId(), rewrittenSource.getRoot(), newAssignments.build()),
                    outputMapping);
        }

        private Map<Symbol, Symbol> mappingFromAssignments(Map<Symbol, Expression> assignments, boolean ambiguousSymbolsPresent)
        {
            Map<Symbol, Symbol> newMapping = new HashMap<>();
            Map<Expression, Symbol> inputsToOutputs = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                Expression expression = assignment.getValue();
                // 1. for trivial symbol projection, map output symbol to input symbol
                // If the assignment potentially introduces a reused (ambiguous) symbol, do not map output to input
                // to avoid mixing semantics. Input symbols represent semantics as in the source plan,
                // while output symbols represent newly established semantics.
                if (expression instanceof SymbolReference && !ambiguousSymbolsPresent) {
                    Symbol value = Symbol.from(expression);
                    if (!assignment.getKey().equals(value)) {
                        newMapping.put(assignment.getKey(), value);
                    }
                }
                // 2. map same deterministic expressions within a projection into the same symbol
                // omit NullLiterals since those have ambiguous types
                else if (DeterminismEvaluator.isDeterministic(expression, metadata) && !(expression instanceof NullLiteral)) {
                    Symbol previous = inputsToOutputs.get(expression);
                    if (previous == null) {
                        inputsToOutputs.put(expression, assignment.getKey());
                    }
                    else {
                        newMapping.put(assignment.getKey(), previous);
                    }
                }
            }
            return newMapping;
        }

        @Override
        public PlanAndMappings visitOutput(OutputNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            List<Symbol> newOutputs = mapper.map(node.getOutputSymbols());

            return new PlanAndMappings(
                    new OutputNode(node.getId(), rewrittenSource.getRoot(), node.getColumnNames(), newOutputs),
                    mapping);
        }

        @Override
        public PlanAndMappings visitEnforceSingleRow(EnforceSingleRowNode node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);

            return new PlanAndMappings(
                    node.replaceChildren(ImmutableList.of(rewrittenSource.getRoot())),
                    rewrittenSource.getMappings());
        }

        @Override
        public PlanAndMappings visitAssignUniqueId(AssignUniqueId node, UnaliasContext context)
        {
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            Map<Symbol, Symbol> mapping = new HashMap<>(rewrittenSource.getMappings());
            SymbolMapper mapper = symbolMapper(mapping);

            Symbol newUnique = mapper.map(node.getIdColumn());

            return new PlanAndMappings(
                    new AssignUniqueId(node.getId(), rewrittenSource.getRoot(), newUnique),
                    mapping);
        }

        @Override
        public PlanAndMappings visitApply(ApplyNode node, UnaliasContext context)
        {
            // it is assumed that apart from correlation (and possibly outer correlation), symbols are distinct between Input and Subquery
            // rewrite Input
            PlanAndMappings rewrittenInput = node.getInput().accept(this, context);
            Map<Symbol, Symbol> inputMapping = new HashMap<>(rewrittenInput.getMappings());
            SymbolMapper mapper = symbolMapper(inputMapping);

            // rewrite correlation with mapping from Input
            List<Symbol> rewrittenCorrelation = mapper.mapAndDistinct(node.getCorrelation());

            // extract new mappings for correlation symbols to apply in Subquery
            Set<Symbol> correlationSymbols = ImmutableSet.copyOf(node.getCorrelation());
            Map<Symbol, Symbol> correlationMapping = new HashMap<>();
            for (Map.Entry<Symbol, Symbol> entry : inputMapping.entrySet()) {
                if (correlationSymbols.contains(entry.getKey())) {
                    correlationMapping.put(entry.getKey(), mapper.map(entry.getKey()));
                }
            }

            Map<Symbol, Symbol> mappingForSubquery = new HashMap<>();
            mappingForSubquery.putAll(context.getCorrelationMapping());
            mappingForSubquery.putAll(correlationMapping);

            // rewrite Subquery
            PlanAndMappings rewrittenSubquery = node.getSubquery().accept(this, new UnaliasContext(mappingForSubquery));

            // unify mappings from Input and Subquery to rewrite Subquery assignments
            Map<Symbol, Symbol> resultMapping = new HashMap<>();
            resultMapping.putAll(rewrittenInput.getMappings());
            resultMapping.putAll(rewrittenSubquery.getMappings());
            mapper = symbolMapper(resultMapping);

            ImmutableList.Builder<Map.Entry<Symbol, Expression>> rewrittenAssignments = ImmutableList.builder();
            for (Map.Entry<Symbol, Expression> assignment : node.getSubqueryAssignments().entrySet()) {
                rewrittenAssignments.add(new SimpleEntry<>(mapper.map(assignment.getKey()), mapper.map(assignment.getValue())));
            }

            // deduplicate assignments
            Map<Symbol, Expression> deduplicateAssignments = rewrittenAssignments.build().stream()
                    .distinct()
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            // derive new mappings for Subquery assignments outputs
            Map<Symbol, Symbol> newMapping = mappingFromAssignments(deduplicateAssignments, false);

            Map<Symbol, Symbol> assignmentsOutputMapping = new HashMap<>();
            assignmentsOutputMapping.putAll(resultMapping);
            assignmentsOutputMapping.putAll(newMapping);

            mapper = symbolMapper(assignmentsOutputMapping);

            // build new Assignments with canonical outputs
            // duplicate entries will be removed by the Builder
            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<Symbol, Expression> assignment : deduplicateAssignments.entrySet()) {
                newAssignments.put(mapper.map(assignment.getKey()), assignment.getValue());
            }

            return new PlanAndMappings(
                    new ApplyNode(node.getId(), rewrittenInput.getRoot(), rewrittenSubquery.getRoot(), newAssignments.build(), rewrittenCorrelation, node.getOriginSubquery()),
                    assignmentsOutputMapping);
        }

        @Override
        public PlanAndMappings visitCorrelatedJoin(CorrelatedJoinNode node, UnaliasContext context)
        {
            // it is assumed that apart from correlation (and possibly outer correlation), symbols are distinct between left and right CorrelatedJoin source
            // rewrite Input
            PlanAndMappings rewrittenInput = node.getInput().accept(this, context);
            Map<Symbol, Symbol> inputMapping = new HashMap<>(rewrittenInput.getMappings());
            SymbolMapper mapper = symbolMapper(inputMapping);

            // rewrite correlation with mapping from Input
            List<Symbol> rewrittenCorrelation = mapper.mapAndDistinct(node.getCorrelation());

            // extract new mappings for correlation symbols to apply in Subquery
            Set<Symbol> correlationSymbols = ImmutableSet.copyOf(node.getCorrelation());
            Map<Symbol, Symbol> correlationMapping = new HashMap<>();
            for (Map.Entry<Symbol, Symbol> entry : inputMapping.entrySet()) {
                if (correlationSymbols.contains(entry.getKey())) {
                    correlationMapping.put(entry.getKey(), mapper.map(entry.getKey()));
                }
            }

            Map<Symbol, Symbol> mappingForSubquery = new HashMap<>();
            mappingForSubquery.putAll(context.getCorrelationMapping());
            mappingForSubquery.putAll(correlationMapping);

            // rewrite Subquery
            PlanAndMappings rewrittenSubquery = node.getSubquery().accept(this, new UnaliasContext(mappingForSubquery));

            // unify mappings from Input and Subquery
            Map<Symbol, Symbol> resultMapping = new HashMap<>();
            resultMapping.putAll(rewrittenInput.getMappings());
            resultMapping.putAll(rewrittenSubquery.getMappings());

            // rewrite filter with unified mapping
            mapper = symbolMapper(resultMapping);
            Expression newFilter = mapper.map(node.getFilter());

            return new PlanAndMappings(
                    new CorrelatedJoinNode(node.getId(), rewrittenInput.getRoot(), rewrittenSubquery.getRoot(), rewrittenCorrelation, node.getType(), newFilter, node.getOriginSubquery()),
                    resultMapping);
        }

        @Override
        public PlanAndMappings visitJoin(JoinNode node, UnaliasContext context)
        {
            // it is assumed that symbols are distinct between left and right join source. Only symbols from outer correlation might be the exception
            PlanAndMappings rewrittenLeft = node.getLeft().accept(this, context);
            PlanAndMappings rewrittenRight = node.getRight().accept(this, context);

            // unify mappings from left and right join source
            Map<Symbol, Symbol> unifiedMapping = new HashMap<>();
            unifiedMapping.putAll(rewrittenLeft.getMappings());
            unifiedMapping.putAll(rewrittenRight.getMappings());

            SymbolMapper mapper = symbolMapper(unifiedMapping);

            ImmutableList.Builder<JoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                builder.add(new JoinNode.EquiJoinClause(mapper.map(clause.getLeft()), mapper.map(clause.getRight())));
            }
            List<JoinNode.EquiJoinClause> newCriteria = builder.build();

            Optional<Expression> newFilter = node.getFilter().map(mapper::map);
            Optional<Symbol> newLeftHashSymbol = node.getLeftHashSymbol().map(mapper::map);
            Optional<Symbol> newRightHashSymbol = node.getRightHashSymbol().map(mapper::map);

            // rewrite dynamic filters
            Map<Symbol, DynamicFilterId> canonicalDynamicFilters = new HashMap<>();
            ImmutableMap.Builder<DynamicFilterId, Symbol> filtersBuilder = ImmutableMap.builder();
            for (Map.Entry<DynamicFilterId, Symbol> entry : node.getDynamicFilters().entrySet()) {
                Symbol canonical = mapper.map(entry.getValue());
                DynamicFilterId canonicalDynamicFilterId = canonicalDynamicFilters.putIfAbsent(canonical, entry.getKey());
                if (canonicalDynamicFilterId == null) {
                    filtersBuilder.put(entry.getKey(), canonical);
                }
                else {
                    dynamicFilterIdMap.put(entry.getKey(), canonicalDynamicFilterId);
                }
            }
            Map<DynamicFilterId, Symbol> newDynamicFilters = filtersBuilder.buildOrThrow();

            // derive new mappings from inner join equi criteria
            Map<Symbol, Symbol> newMapping = new HashMap<>();
            if (node.getType() == INNER) {
                newCriteria.stream()
                        // Map right equi-condition symbol to left symbol. This helps to
                        // reuse join node partitioning better as partitioning properties are
                        // only derived from probe side symbols
                        .forEach(clause -> newMapping.put(clause.getRight(), clause.getLeft()));
            }

            Map<Symbol, Symbol> outputMapping = new HashMap<>();
            outputMapping.putAll(unifiedMapping);
            outputMapping.putAll(newMapping);

            mapper = symbolMapper(outputMapping);
            List<Symbol> canonicalOutputs = mapper.mapAndDistinct(node.getOutputSymbols());
            List<Symbol> newLeftOutputSymbols = canonicalOutputs.stream()
                    .filter(rewrittenLeft.getRoot().getOutputSymbols()::contains)
                    .collect(toImmutableList());
            List<Symbol> newRightOutputSymbols = canonicalOutputs.stream()
                    .filter(rewrittenRight.getRoot().getOutputSymbols()::contains)
                    .collect(toImmutableList());

            return new PlanAndMappings(
                    new JoinNode(
                            node.getId(),
                            node.getType(),
                            rewrittenLeft.getRoot(),
                            rewrittenRight.getRoot(),
                            newCriteria,
                            newLeftOutputSymbols,
                            newRightOutputSymbols,
                            node.isMaySkipOutputDuplicates(),
                            newFilter,
                            newLeftHashSymbol,
                            newRightHashSymbol,
                            node.getDistributionType(),
                            node.isSpillable(),
                            newDynamicFilters,
                            node.getReorderJoinStatsAndCost()),
                    outputMapping);
        }

        @Override
        public PlanAndMappings visitSemiJoin(SemiJoinNode node, UnaliasContext context)
        {
            // it is assumed that symbols are distinct between SemiJoin source and filtering source. Only symbols from outer correlation might be the exception
            PlanAndMappings rewrittenSource = node.getSource().accept(this, context);
            PlanAndMappings rewrittenFilteringSource = node.getFilteringSource().accept(this, context);

            Map<Symbol, Symbol> outputMapping = new HashMap<>();
            outputMapping.putAll(rewrittenSource.getMappings());
            outputMapping.putAll(rewrittenFilteringSource.getMappings());

            SymbolMapper mapper = symbolMapper(outputMapping);

            Symbol newSourceJoinSymbol = mapper.map(node.getSourceJoinSymbol());
            Symbol newFilteringSourceJoinSymbol = mapper.map(node.getFilteringSourceJoinSymbol());
            Symbol newSemiJoinOutput = mapper.map(node.getSemiJoinOutput());
            Optional<Symbol> newSourceHashSymbol = node.getSourceHashSymbol().map(mapper::map);
            Optional<Symbol> newFilteringSourceHashSymbol = node.getFilteringSourceHashSymbol().map(mapper::map);

            return new PlanAndMappings(
                    new SemiJoinNode(
                            node.getId(),
                            rewrittenSource.getRoot(),
                            rewrittenFilteringSource.getRoot(),
                            newSourceJoinSymbol,
                            newFilteringSourceJoinSymbol,
                            newSemiJoinOutput,
                            newSourceHashSymbol,
                            newFilteringSourceHashSymbol,
                            node.getDistributionType(),
                            node.getDynamicFilterId()),
                    outputMapping);
        }

        @Override
        public PlanAndMappings visitSpatialJoin(SpatialJoinNode node, UnaliasContext context)
        {
            // it is assumed that symbols are distinct between left and right SpatialJoin source. Only symbols from outer correlation might be the exception
            PlanAndMappings rewrittenLeft = node.getLeft().accept(this, context);
            PlanAndMappings rewrittenRight = node.getRight().accept(this, context);

            Map<Symbol, Symbol> outputMapping = new HashMap<>();
            outputMapping.putAll(rewrittenLeft.getMappings());
            outputMapping.putAll(rewrittenRight.getMappings());

            SymbolMapper mapper = symbolMapper(outputMapping);

            List<Symbol> newOutputSymbols = mapper.mapAndDistinct(node.getOutputSymbols());
            Expression newFilter = mapper.map(node.getFilter());
            Optional<Symbol> newLeftPartitionSymbol = node.getLeftPartitionSymbol().map(mapper::map);
            Optional<Symbol> newRightPartitionSymbol = node.getRightPartitionSymbol().map(mapper::map);

            return new PlanAndMappings(
                    new SpatialJoinNode(node.getId(), node.getType(), rewrittenLeft.getRoot(), rewrittenRight.getRoot(), newOutputSymbols, newFilter, newLeftPartitionSymbol, newRightPartitionSymbol, node.getKdbTree()),
                    outputMapping);
        }

        @Override
        public PlanAndMappings visitIndexJoin(IndexJoinNode node, UnaliasContext context)
        {
            // it is assumed that symbols are distinct between probeSource and indexSource. Only symbols from outer correlation might be the exception
            PlanAndMappings rewrittenProbe = node.getProbeSource().accept(this, context);
            PlanAndMappings rewrittenIndex = node.getIndexSource().accept(this, context);

            Map<Symbol, Symbol> outputMapping = new HashMap<>();
            outputMapping.putAll(rewrittenProbe.getMappings());
            outputMapping.putAll(rewrittenIndex.getMappings());

            SymbolMapper mapper = symbolMapper(outputMapping);

            // canonicalize index join criteria
            ImmutableList.Builder<IndexJoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                builder.add(new IndexJoinNode.EquiJoinClause(mapper.map(clause.getProbe()), mapper.map(clause.getIndex())));
            }
            List<IndexJoinNode.EquiJoinClause> newEquiCriteria = builder.build();

            Optional<Symbol> newProbeHashSymbol = node.getProbeHashSymbol().map(mapper::map);
            Optional<Symbol> newIndexHashSymbol = node.getIndexHashSymbol().map(mapper::map);

            return new PlanAndMappings(
                    new IndexJoinNode(node.getId(), node.getType(), rewrittenProbe.getRoot(), rewrittenIndex.getRoot(), newEquiCriteria, newProbeHashSymbol, newIndexHashSymbol),
                    outputMapping);
        }

        @Override
        public PlanAndMappings visitIndexSource(IndexSourceNode node, UnaliasContext context)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper mapper = symbolMapper(mapping);

            Set<Symbol> newLookupSymbols = node.getLookupSymbols().stream()
                    .map(mapper::map)
                    .collect(toImmutableSet());
            List<Symbol> newOutputSymbols = mapper.mapAndDistinct(node.getOutputSymbols());

            Map<Symbol, ColumnHandle> newAssignments = new HashMap<>();
            node.getAssignments().entrySet().stream()
                    .forEach(assignment -> newAssignments.put(mapper.map(assignment.getKey()), assignment.getValue()));

            return new PlanAndMappings(
                    new IndexSourceNode(node.getId(), node.getIndexHandle(), node.getTableHandle(), newLookupSymbols, newOutputSymbols, newAssignments),
                    mapping);
        }

        @Override
        public PlanAndMappings visitUnion(UnionNode node, UnaliasContext context)
        {
            List<PlanAndMappings> rewrittenSources = node.getSources().stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());

            List<SymbolMapper> inputMappers = rewrittenSources.stream()
                    .map(source -> symbolMapper(new HashMap<>(source.getMappings())))
                    .collect(toImmutableList());

            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper outputMapper = symbolMapper(mapping);

            ListMultimap<Symbol, Symbol> newOutputToInputs = rewriteOutputToInputsMap(node.getSymbolMapping(), outputMapper, inputMappers);
            List<Symbol> newOutputs = outputMapper.mapAndDistinct(node.getOutputSymbols());

            return new PlanAndMappings(
                    new UnionNode(
                            node.getId(),
                            rewrittenSources.stream()
                                    .map(PlanAndMappings::getRoot)
                                    .collect(toImmutableList()),
                            newOutputToInputs,
                            newOutputs),
                    mapping);
        }

        @Override
        public PlanAndMappings visitIntersect(IntersectNode node, UnaliasContext context)
        {
            List<PlanAndMappings> rewrittenSources = node.getSources().stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());

            List<SymbolMapper> inputMappers = rewrittenSources.stream()
                    .map(source -> symbolMapper(new HashMap<>(source.getMappings())))
                    .collect(toImmutableList());

            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper outputMapper = symbolMapper(mapping);

            ListMultimap<Symbol, Symbol> newOutputToInputs = rewriteOutputToInputsMap(node.getSymbolMapping(), outputMapper, inputMappers);
            List<Symbol> newOutputs = outputMapper.mapAndDistinct(node.getOutputSymbols());

            return new PlanAndMappings(
                    new IntersectNode(
                            node.getId(),
                            rewrittenSources.stream()
                                    .map(PlanAndMappings::getRoot)
                                    .collect(toImmutableList()),
                            newOutputToInputs,
                            newOutputs,
                            node.isDistinct()),
                    mapping);
        }

        @Override
        public PlanAndMappings visitExcept(ExceptNode node, UnaliasContext context)
        {
            List<PlanAndMappings> rewrittenSources = node.getSources().stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());

            List<SymbolMapper> inputMappers = rewrittenSources.stream()
                    .map(source -> symbolMapper(new HashMap<>(source.getMappings())))
                    .collect(toImmutableList());

            Map<Symbol, Symbol> mapping = new HashMap<>(context.getCorrelationMapping());
            SymbolMapper outputMapper = symbolMapper(mapping);

            ListMultimap<Symbol, Symbol> newOutputToInputs = rewriteOutputToInputsMap(node.getSymbolMapping(), outputMapper, inputMappers);
            List<Symbol> newOutputs = outputMapper.mapAndDistinct(node.getOutputSymbols());

            return new PlanAndMappings(
                    new ExceptNode(
                            node.getId(),
                            rewrittenSources.stream()
                                    .map(PlanAndMappings::getRoot)
                                    .collect(toImmutableList()),
                            newOutputToInputs,
                            newOutputs,
                            node.isDistinct()),
                    mapping);
        }

        private ListMultimap<Symbol, Symbol> rewriteOutputToInputsMap(ListMultimap<Symbol, Symbol> oldMapping, SymbolMapper outputMapper, List<SymbolMapper> inputMappers)
        {
            ImmutableListMultimap.Builder<Symbol, Symbol> newMappingBuilder = ImmutableListMultimap.builder();
            Set<Symbol> addedSymbols = new HashSet<>();
            for (Map.Entry<Symbol, Collection<Symbol>> entry : oldMapping.asMap().entrySet()) {
                Symbol rewrittenOutput = outputMapper.map(entry.getKey());
                if (addedSymbols.add(rewrittenOutput)) {
                    List<Symbol> inputs = ImmutableList.copyOf(entry.getValue());
                    ImmutableList.Builder<Symbol> rewrittenInputs = ImmutableList.builder();
                    for (int i = 0; i < inputs.size(); i++) {
                        rewrittenInputs.add(inputMappers.get(i).map(inputs.get(i)));
                    }
                    newMappingBuilder.putAll(rewrittenOutput, rewrittenInputs.build());
                }
            }
            return newMappingBuilder.build();
        }
    }

    private static class UnaliasContext
    {
        // Correlation mapping is a record of how correlation symbols have been mapped in the subplan which provides them.
        // All occurrences of correlation symbols within the correlated subquery must be remapped accordingly.
        // In case of nested correlation, correlationMappings has required mappings for correlation symbols from all levels of nesting.
        private final Map<Symbol, Symbol> correlationMapping;

        public UnaliasContext(Map<Symbol, Symbol> correlationMapping)
        {
            this.correlationMapping = requireNonNull(correlationMapping, "correlationMapping is null");
        }

        public static UnaliasContext empty()
        {
            return new UnaliasContext(ImmutableMap.of());
        }

        public Map<Symbol, Symbol> getCorrelationMapping()
        {
            return correlationMapping;
        }
    }

    private static class PlanAndMappings
    {
        private final PlanNode root;
        private final Map<Symbol, Symbol> mappings;

        public PlanAndMappings(PlanNode root, Map<Symbol, Symbol> mappings)
        {
            this.root = requireNonNull(root, "root is null");
            this.mappings = ImmutableMap.copyOf(requireNonNull(mappings, "mappings is null"));
        }

        public PlanNode getRoot()
        {
            return root;
        }

        public Map<Symbol, Symbol> getMappings()
        {
            return mappings;
        }
    }

    private static class DynamicFilterVisitor
            extends SimplePlanRewriter<Void>
    {
        private final Metadata metadata;
        private final Map<DynamicFilterId, DynamicFilterId> dynamicFilterIdMap;

        private DynamicFilterVisitor(Metadata metadata, Map<DynamicFilterId, DynamicFilterId> dynamicFilterIdMap)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.dynamicFilterIdMap = requireNonNull(dynamicFilterIdMap, "dynamicFilterIdMap is null");
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource());
            Expression rewrittenPredicate = updateDynamicFilterIds(dynamicFilterIdMap, node.getPredicate());

            if (rewrittenSource == node.getSource() && rewrittenPredicate == node.getPredicate()) {
                return node;
            }
            return new FilterNode(
                    node.getId(),
                    rewrittenSource,
                    rewrittenPredicate);
        }

        private Expression updateDynamicFilterIds(Map<DynamicFilterId, DynamicFilterId> dynamicFilterIdMap, Expression predicate)
        {
            List<Expression> conjuncts = extractConjuncts(predicate);
            boolean updated = false;
            ImmutableList.Builder<Expression> newConjuncts = ImmutableList.builder();
            for (Expression conjunct : conjuncts) {
                Optional<DynamicFilters.Descriptor> descriptor = getDescriptor(conjunct);
                if (descriptor.isEmpty()) {
                    // not DF
                    newConjuncts.add(conjunct);
                    continue;
                }
                DynamicFilterId mappedId = dynamicFilterIdMap.get(descriptor.get().getId());
                Expression newConjunct = conjunct;
                if (mappedId != null) {
                    // DF was remapped
                    newConjunct = replaceDynamicFilterId((FunctionCall) conjunct, mappedId);
                    updated = true;
                }
                newConjuncts.add(newConjunct);
            }
            if (updated) {
                return combineConjuncts(metadata, newConjuncts.build());
            }
            return predicate;
        }
    }
}
