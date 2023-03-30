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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.plan.AggregationNode.groupingSets;
import static java.util.Objects.requireNonNull;

public class SymbolMapper
{
    private final Function<Symbol, Symbol> mappingFunction;

    public SymbolMapper(Function<Symbol, Symbol> mappingFunction)
    {
        this.mappingFunction = requireNonNull(mappingFunction, "mappingFunction is null");
    }

    public static SymbolMapper symbolMapper(Map<Symbol, Symbol> mapping)
    {
        return new SymbolMapper(symbol -> {
            while (mapping.containsKey(symbol) && !mapping.get(symbol).equals(symbol)) {
                symbol = mapping.get(symbol);
            }
            return symbol;
        });
    }

    public static SymbolMapper symbolReallocator(Map<Symbol, Symbol> mapping, SymbolAllocator symbolAllocator)
    {
        return new SymbolMapper(symbol -> {
            if (mapping.containsKey(symbol)) {
                while (mapping.containsKey(symbol) && !mapping.get(symbol).equals(symbol)) {
                    symbol = mapping.get(symbol);
                }
                // do not remap the symbol further
                mapping.put(symbol, symbol);
                return symbol;
            }
            Symbol newSymbol = symbolAllocator.newSymbol(symbol);
            mapping.put(symbol, newSymbol);
            // do not remap the symbol further
            mapping.put(newSymbol, newSymbol);
            return newSymbol;
        });
    }

    // Return the canonical mapping for the symbol.
    public Symbol map(Symbol symbol)
    {
        return mappingFunction.apply(symbol);
    }

    public List<Symbol> map(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(this::map)
                .collect(toImmutableList());
    }

    public List<Symbol> mapAndDistinct(List<Symbol> symbols)
    {
        return symbols.stream()
                .map(this::map)
                .distinct()
                .collect(toImmutableList());
    }

    public Expression map(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Symbol canonical = map(Symbol.from(node));
                return canonical.toSymbolReference();
            }
        }, expression);
    }

    public AggregationNode map(AggregationNode node, PlanNode source)
    {
        return map(node, source, node.getId());
    }

    public AggregationNode map(AggregationNode node, PlanNode source, PlanNodeId newNodeId)
    {
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            aggregations.put(map(entry.getKey()), map(entry.getValue()));
        }

        return new AggregationNode(
                newNodeId,
                source,
                aggregations.buildOrThrow(),
                groupingSets(
                        mapAndDistinct(node.getGroupingKeys()),
                        node.getGroupingSetCount(),
                        node.getGlobalGroupingSets()),
                ImmutableList.of(),
                node.getStep(),
                node.getHashSymbol().map(this::map),
                node.getGroupIdSymbol().map(this::map));
    }

    public Aggregation map(Aggregation aggregation)
    {
        return new Aggregation(
                aggregation.getResolvedFunction(),
                aggregation.getArguments().stream()
                        .map(this::map)
                        .collect(toImmutableList()),
                aggregation.isDistinct(),
                aggregation.getFilter().map(this::map),
                aggregation.getOrderingScheme().map(this::map),
                aggregation.getMask().map(this::map));
    }

    public GroupIdNode map(GroupIdNode node, PlanNode source)
    {
        Map<Symbol, Symbol> newGroupingMappings = new HashMap<>();
        ImmutableList.Builder<List<Symbol>> newGroupingSets = ImmutableList.builder();

        for (List<Symbol> groupingSet : node.getGroupingSets()) {
            ImmutableList.Builder<Symbol> newGroupingSet = ImmutableList.builder();
            for (Symbol output : groupingSet) {
                Symbol newOutput = map(output);
                newGroupingMappings.putIfAbsent(
                        newOutput,
                        map(node.getGroupingColumns().get(output)));
                newGroupingSet.add(newOutput);
            }
            newGroupingSets.add(newGroupingSet.build());
        }

        return new GroupIdNode(
                node.getId(),
                source,
                newGroupingSets.build(),
                newGroupingMappings,
                mapAndDistinct(node.getAggregationArguments()),
                map(node.getGroupIdSymbol()));
    }

    public WindowNode map(WindowNode node, PlanNode source)
    {
        ImmutableMap.Builder<Symbol, WindowNode.Function> newFunctions = ImmutableMap.builder();
        node.getWindowFunctions().forEach((symbol, function) -> {
            List<Expression> newArguments = function.getArguments().stream()
                    .map(this::map)
                    .collect(toImmutableList());
            WindowNode.Frame newFrame = map(function.getFrame());

            newFunctions.put(map(symbol), new WindowNode.Function(function.getResolvedFunction(), newArguments, newFrame, function.isIgnoreNulls()));
        });

        SpecificationWithPreSortedPrefix newSpecification = mapAndDistinct(node.getSpecification(), node.getPreSortedOrderPrefix());

        return new WindowNode(
                node.getId(),
                source,
                newSpecification.specification(),
                newFunctions.buildOrThrow(),
                node.getHashSymbol().map(this::map),
                node.getPrePartitionedInputs().stream()
                        .map(this::map)
                        .collect(toImmutableSet()),
                newSpecification.preSorted());
    }

    private WindowNode.Frame map(WindowNode.Frame frame)
    {
        return new WindowNode.Frame(
                frame.getType(),
                frame.getStartType(),
                frame.getStartValue().map(this::map),
                frame.getSortKeyCoercedForFrameStartComparison().map(this::map),
                frame.getEndType(),
                frame.getEndValue().map(this::map),
                frame.getSortKeyCoercedForFrameEndComparison().map(this::map),
                frame.getOriginalStartValue(),
                frame.getOriginalEndValue());
    }

    private SpecificationWithPreSortedPrefix mapAndDistinct(DataOrganizationSpecification specification, int preSorted)
    {
        Optional<OrderingSchemeWithPreSortedPrefix> newOrderingScheme = specification.getOrderingScheme()
                .map(orderingScheme -> map(orderingScheme, preSorted));

        return new SpecificationWithPreSortedPrefix(
                new DataOrganizationSpecification(
                        mapAndDistinct(specification.getPartitionBy()),
                        newOrderingScheme.map(OrderingSchemeWithPreSortedPrefix::orderingScheme)),
                newOrderingScheme.map(OrderingSchemeWithPreSortedPrefix::preSorted).orElse(preSorted));
    }

    public DataOrganizationSpecification mapAndDistinct(DataOrganizationSpecification specification)
    {
        return new DataOrganizationSpecification(
                mapAndDistinct(specification.getPartitionBy()),
                specification.getOrderingScheme().map(this::map));
    }

    public PatternRecognitionNode map(PatternRecognitionNode node, PlanNode source)
    {
        SpecificationWithPreSortedPrefix newSpecification = mapAndDistinct(node.getSpecification(), node.getPreSortedOrderPrefix());

        ImmutableMap.Builder<Symbol, WindowNode.Function> newFunctions = ImmutableMap.builder();
        node.getWindowFunctions().forEach((symbol, function) -> {
            List<Expression> newArguments = function.getArguments().stream()
                    .map(this::map)
                    .collect(toImmutableList());
            WindowNode.Frame newFrame = map(function.getFrame());

            newFunctions.put(map(symbol), new WindowNode.Function(function.getResolvedFunction(), newArguments, newFrame, function.isIgnoreNulls()));
        });

        ImmutableMap.Builder<Symbol, Measure> newMeasures = ImmutableMap.builder();
        node.getMeasures().forEach((symbol, measure) -> {
            ExpressionAndValuePointers newExpression = map(measure.getExpressionAndValuePointers());
            newMeasures.put(map(symbol), new Measure(newExpression, measure.getType()));
        });

        ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> newVariableDefinitions = ImmutableMap.builder();
        node.getVariableDefinitions().forEach((label, expression) -> newVariableDefinitions.put(label, map(expression)));

        return new PatternRecognitionNode(
                node.getId(),
                source,
                newSpecification.specification(),
                node.getHashSymbol().map(this::map),
                node.getPrePartitionedInputs().stream()
                        .map(this::map)
                        .collect(toImmutableSet()),
                newSpecification.preSorted(),
                newFunctions.buildOrThrow(),
                newMeasures.buildOrThrow(),
                node.getCommonBaseFrame().map(this::map),
                node.getRowsPerMatch(),
                node.getSkipToLabel(),
                node.getSkipToPosition(),
                node.isInitial(),
                node.getPattern(),
                node.getSubsets(),
                newVariableDefinitions.buildOrThrow());
    }

    private ExpressionAndValuePointers map(ExpressionAndValuePointers expressionAndValuePointers)
    {
        // Map only the input symbols of ValuePointers. These are the symbols produced by the source node.
        // Other symbols present in the ExpressionAndValuePointers structure are synthetic unique symbols
        // with no outer usage or dependencies.
        ImmutableList.Builder<ValuePointer> newValuePointers = ImmutableList.builder();
        for (ValuePointer valuePointer : expressionAndValuePointers.getValuePointers()) {
            if (valuePointer instanceof ScalarValuePointer scalarValuePointer) {
                Symbol inputSymbol = scalarValuePointer.getInputSymbol();
                if (expressionAndValuePointers.getClassifierSymbols().contains(inputSymbol) || expressionAndValuePointers.getMatchNumberSymbols().contains(inputSymbol)) {
                    newValuePointers.add(scalarValuePointer);
                }
                else {
                    newValuePointers.add(new ScalarValuePointer(scalarValuePointer.getLogicalIndexPointer(), map(inputSymbol)));
                }
            }
            else {
                AggregationValuePointer aggregationValuePointer = (AggregationValuePointer) valuePointer;

                List<Expression> newArguments = aggregationValuePointer.getArguments().stream()
                        .map(expression -> ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
                        {
                            @Override
                            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                            {
                                if (Symbol.from(node).equals(aggregationValuePointer.getClassifierSymbol()) || Symbol.from(node).equals(aggregationValuePointer.getMatchNumberSymbol())) {
                                    return node;
                                }
                                return map(node);
                            }
                        }, expression))
                        .collect(toImmutableList());

                newValuePointers.add(new AggregationValuePointer(
                        aggregationValuePointer.getFunction(),
                        aggregationValuePointer.getSetDescriptor(),
                        newArguments,
                        aggregationValuePointer.getClassifierSymbol(),
                        aggregationValuePointer.getMatchNumberSymbol()));
            }
        }

        return new ExpressionAndValuePointers(
                expressionAndValuePointers.getExpression(),
                expressionAndValuePointers.getLayout(),
                newValuePointers.build(),
                expressionAndValuePointers.getClassifierSymbols(),
                expressionAndValuePointers.getMatchNumberSymbols());
    }

    public TableFunctionProcessorNode map(TableFunctionProcessorNode node, PlanNode source)
    {
        // rewrite and deduplicate pass-through specifications
        // note: Potentially, pass-through symbols from different sources might be recognized as semantically identical, and rewritten
        // to the same symbol. Currently, we retrieve the first occurrence of a symbol, and skip all the following occurrences.
        // For better performance, we could pick the occurrence with "isPartitioningColumn" property, since the pass-through mechanism
        // is more efficient for partitioning columns which are guaranteed to be constant within partition.
        // TODO choose a partitioning column to be retrieved while deduplicating
        ImmutableList.Builder<PassThroughSpecification> newPassThroughSpecifications = ImmutableList.builder();
        Set<Symbol> newPassThroughSymbols = new HashSet<>();
        for (PassThroughSpecification specification : node.getPassThroughSpecifications()) {
            ImmutableList.Builder<PassThroughColumn> newColumns = ImmutableList.builder();
            for (PassThroughColumn column : specification.columns()) {
                Symbol newSymbol = map(column.symbol());
                if (newPassThroughSymbols.add(newSymbol)) {
                    newColumns.add(new PassThroughColumn(newSymbol, column.isPartitioningColumn()));
                }
            }
            newPassThroughSpecifications.add(new PassThroughSpecification(specification.declaredAsPassThrough(), newColumns.build()));
        }

        // rewrite required symbols without deduplication. the table function expects specific input layout
        List<List<Symbol>> newRequiredSymbols = node.getRequiredSymbols().stream()
                .map(this::map)
                .collect(toImmutableList());

        // rewrite and deduplicate marker mapping
        Optional<Map<Symbol, Symbol>> newMarkerSymbols = node.getMarkerSymbols()
                .map(mapping -> mapping.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> map(entry.getKey()),
                                entry -> map(entry.getValue()),
                                (first, second) -> {
                                    checkState(first.equals(second), "Ambiguous marker symbols: %s and %s", first, second);
                                    return first;
                                })));

        // rewrite and deduplicate specification
        Optional<SpecificationWithPreSortedPrefix> newSpecification = node.getSpecification().map(specification -> mapAndDistinct(specification, node.getPreSorted()));

        return new TableFunctionProcessorNode(
                node.getId(),
                node.getName(),
                node.getFunctionCatalog(),
                map(node.getProperOutputs()),
                Optional.of(source),
                node.isPruneWhenEmpty(),
                newPassThroughSpecifications.build(),
                newRequiredSymbols,
                newMarkerSymbols,
                newSpecification.map(SpecificationWithPreSortedPrefix::specification),
                node.getPrePartitioned().stream()
                        .map(this::map)
                        .collect(toImmutableSet()),
                newSpecification.map(SpecificationWithPreSortedPrefix::preSorted).orElse(node.getPreSorted()),
                node.getHashSymbol().map(this::map),
                node.getHandle());
    }

    public LimitNode map(LimitNode node, PlanNode source)
    {
        return new LimitNode(
                node.getId(),
                source,
                node.getCount(),
                node.getTiesResolvingScheme().map(this::map),
                node.isPartial(),
                node.getPreSortedInputs().stream()
                        .map(this::map)
                        .collect(toImmutableList()));
    }

    public OrderingSchemeWithPreSortedPrefix map(OrderingScheme orderingScheme, int preSorted)
    {
        ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SortOrder> newOrderings = ImmutableMap.builder();
        int newPreSorted = preSorted;

        Set<Symbol> added = new HashSet<>(orderingScheme.getOrderBy().size());

        for (int i = 0; i < orderingScheme.getOrderBy().size(); i++) {
            Symbol symbol = orderingScheme.getOrderBy().get(i);
            Symbol canonical = map(symbol);
            if (added.add(canonical)) {
                newSymbols.add(canonical);
                newOrderings.put(canonical, orderingScheme.getOrdering(symbol));
            }
            else if (i < preSorted) {
                newPreSorted--;
            }
        }

        return new OrderingSchemeWithPreSortedPrefix(new OrderingScheme(newSymbols.build(), newOrderings.buildOrThrow()), newPreSorted);
    }

    public OrderingScheme map(OrderingScheme orderingScheme)
    {
        ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SortOrder> newOrderings = ImmutableMap.builder();
        Set<Symbol> added = new HashSet<>(orderingScheme.getOrderBy().size());
        for (Symbol symbol : orderingScheme.getOrderBy()) {
            Symbol canonical = map(symbol);
            if (added.add(canonical)) {
                newSymbols.add(canonical);
                newOrderings.put(canonical, orderingScheme.getOrdering(symbol));
            }
        }
        return new OrderingScheme(newSymbols.build(), newOrderings.buildOrThrow());
    }

    public DistinctLimitNode map(DistinctLimitNode node, PlanNode source)
    {
        return new DistinctLimitNode(
                node.getId(),
                source,
                node.getLimit(),
                node.isPartial(),
                mapAndDistinct(node.getDistinctSymbols()),
                node.getHashSymbol().map(this::map));
    }

    public StatisticsWriterNode map(StatisticsWriterNode node, PlanNode source)
    {
        return new StatisticsWriterNode(
                node.getId(),
                source,
                node.getTarget(),
                map(node.getRowCountSymbol()),
                node.isRowCountEnabled(),
                node.getDescriptor().map(this::map));
    }

    public TableWriterNode map(TableWriterNode node, PlanNode source)
    {
        return map(node, source, node.getId());
    }

    public TableWriterNode map(TableWriterNode node, PlanNode source, PlanNodeId newId)
    {
        // Intentionally does not use mapAndDistinct on columns as that would remove columns
        return new TableWriterNode(
                newId,
                source,
                node.getTarget(),
                map(node.getRowCountSymbol()),
                map(node.getFragmentSymbol()),
                map(node.getColumns()),
                node.getColumnNames(),
                node.getPartitioningScheme().map(partitioningScheme -> map(partitioningScheme, source.getOutputSymbols())),
                node.getPreferredPartitioningScheme().map(partitioningScheme -> map(partitioningScheme, source.getOutputSymbols())),
                node.getStatisticsAggregation().map(this::map),
                node.getStatisticsAggregationDescriptor().map(descriptor -> descriptor.map(this::map)));
    }

    public TableExecuteNode map(TableExecuteNode node, PlanNode source)
    {
        return map(node, source, node.getId());
    }

    public TableExecuteNode map(TableExecuteNode node, PlanNode source, PlanNodeId newId)
    {
        // Intentionally does not use mapAndDistinct on columns as that would remove columns
        return new TableExecuteNode(
                newId,
                source,
                node.getTarget(),
                map(node.getRowCountSymbol()),
                map(node.getFragmentSymbol()),
                map(node.getColumns()),
                node.getColumnNames(),
                node.getPartitioningScheme().map(partitioningScheme -> map(partitioningScheme, source.getOutputSymbols())),
                node.getPreferredPartitioningScheme().map(partitioningScheme -> map(partitioningScheme, source.getOutputSymbols())));
    }

    public MergeWriterNode map(MergeWriterNode node, PlanNode source)
    {
        // Intentionally does not use mapAndDistinct on columns as that would remove columns
        List<Symbol> newOutputs = map(node.getOutputSymbols());

        return new MergeWriterNode(
                node.getId(),
                source,
                node.getTarget(),
                map(node.getProjectedSymbols()),
                node.getPartitioningScheme().map(partitioningScheme -> map(partitioningScheme, source.getOutputSymbols())),
                newOutputs);
    }

    public MergeWriterNode map(MergeWriterNode node, PlanNode source, PlanNodeId newId)
    {
        // Intentionally does not use mapAndDistinct on columns as that would remove columns
        List<Symbol> newOutputs = map(node.getOutputSymbols());

        return new MergeWriterNode(
                newId,
                source,
                node.getTarget(),
                map(node.getProjectedSymbols()),
                node.getPartitioningScheme().map(partitioningScheme -> map(partitioningScheme, source.getOutputSymbols())),
                newOutputs);
    }

    public MergeProcessorNode map(MergeProcessorNode node, PlanNode source)
    {
        List<Symbol> newOutputs = map(node.getOutputSymbols());

        return new MergeProcessorNode(
                node.getId(),
                source,
                node.getTarget(),
                map(node.getRowIdSymbol()),
                map(node.getMergeRowSymbol()),
                map(node.getDataColumnSymbols()),
                map(node.getRedistributionColumnSymbols()),
                newOutputs);
    }

    public PartitioningScheme map(PartitioningScheme scheme, List<Symbol> sourceLayout)
    {
        return new PartitioningScheme(
                scheme.getPartitioning().translate(this::map),
                mapAndDistinct(sourceLayout),
                scheme.getHashColumn().map(this::map),
                scheme.isReplicateNullsAndAny(),
                scheme.getBucketToPartition(),
                scheme.getPartitionCount());
    }

    public TableFinishNode map(TableFinishNode node, PlanNode source)
    {
        return new TableFinishNode(
                node.getId(),
                source,
                node.getTarget(),
                map(node.getRowCountSymbol()),
                node.getStatisticsAggregation().map(this::map),
                node.getStatisticsAggregationDescriptor().map(descriptor -> descriptor.map(this::map)));
    }

    private StatisticAggregations map(StatisticAggregations statisticAggregations)
    {
        Map<Symbol, Aggregation> aggregations = statisticAggregations.getAggregations().entrySet().stream()
                .collect(toImmutableMap(entry -> map(entry.getKey()), entry -> map(entry.getValue())));
        return new StatisticAggregations(aggregations, mapAndDistinct(statisticAggregations.getGroupingSymbols()));
    }

    public RowNumberNode map(RowNumberNode node, PlanNode source)
    {
        return new RowNumberNode(
                node.getId(),
                source,
                mapAndDistinct(node.getPartitionBy()),
                node.isOrderSensitive(),
                map(node.getRowNumberSymbol()),
                node.getMaxRowCountPerPartition(),
                node.getHashSymbol().map(this::map));
    }

    public TopNRankingNode map(TopNRankingNode node, PlanNode source)
    {
        return new TopNRankingNode(
                node.getId(),
                source,
                mapAndDistinct(node.getSpecification()),
                node.getRankingType(),
                map(node.getRankingSymbol()),
                node.getMaxRankingPerPartition(),
                node.isPartial(),
                node.getHashSymbol().map(this::map));
    }

    public TopNNode map(TopNNode node, PlanNode source)
    {
        return map(node, source, node.getId());
    }

    public TopNNode map(TopNNode node, PlanNode source, PlanNodeId nodeId)
    {
        return new TopNNode(
                nodeId,
                source,
                node.getCount(),
                map(node.getOrderingScheme()),
                node.getStep());
    }

    private record OrderingSchemeWithPreSortedPrefix(OrderingScheme orderingScheme, int preSorted)
    {
        private OrderingSchemeWithPreSortedPrefix(OrderingScheme orderingScheme, int preSorted)
        {
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
            this.preSorted = preSorted;
        }
    }

    private record SpecificationWithPreSortedPrefix(DataOrganizationSpecification specification, int preSorted)
    {
        private SpecificationWithPreSortedPrefix(DataOrganizationSpecification specification, int preSorted)
        {
            this.specification = requireNonNull(specification, "specification is null");
            this.preSorted = preSorted;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();

        public void put(Symbol from, Symbol to)
        {
            mappings.put(from, to);
        }

        public SymbolMapper build()
        {
            return SymbolMapper.symbolMapper(mappings.buildOrThrow());
        }
    }
}
