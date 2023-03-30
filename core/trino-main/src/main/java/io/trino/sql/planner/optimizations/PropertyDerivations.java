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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConstantProperty;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.ActualProperties.Global;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.predicate.TupleDomain.extractFixedValues;
import static io.trino.sql.planner.SystemPartitioningHandle.ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.ActualProperties.Global.arbitraryPartition;
import static io.trino.sql.planner.optimizations.ActualProperties.Global.coordinatorSingleStreamPartition;
import static io.trino.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static io.trino.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static io.trino.sql.planner.optimizations.ActualProperties.Global.streamPartitionedOn;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.WINDOW;
import static io.trino.sql.tree.SkipTo.Position.PAST_LAST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class PropertyDerivations
{
    private PropertyDerivations() {}

    public static ActualProperties derivePropertiesRecursively(
            PlanNode node,
            PlannerContext plannerContext,
            Session session,
            TypeProvider types,
            TypeAnalyzer typeAnalyzer)
    {
        List<ActualProperties> inputProperties = node.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, plannerContext, session, types, typeAnalyzer))
                .collect(toImmutableList());
        return deriveProperties(node, inputProperties, plannerContext, session, types, typeAnalyzer);
    }

    public static ActualProperties deriveProperties(
            PlanNode node,
            List<ActualProperties> inputProperties,
            PlannerContext plannerContext,
            Session session,
            TypeProvider types,
            TypeAnalyzer typeAnalyzer)
    {
        ActualProperties output = node.accept(new Visitor(plannerContext, session, types, typeAnalyzer), inputProperties);

        output.getNodePartitioning().ifPresent(partitioning ->
                verify(node.getOutputSymbols().containsAll(partitioning.getColumns()), "Node-level partitioning properties contain columns not present in node's output"));

        verify(node.getOutputSymbols().containsAll(output.getConstants().keySet()), "Node-level constant properties contain columns not present in node's output");

        Set<Symbol> localPropertyColumns = output.getLocalProperties().stream()
                .flatMap(property -> property.getColumns().stream())
                .collect(Collectors.toSet());

        verify(node.getOutputSymbols().containsAll(localPropertyColumns), "Node-level local properties contain columns not present in node's output");
        return output;
    }

    public static ActualProperties streamBackdoorDeriveProperties(
            PlanNode node,
            List<ActualProperties> inputProperties,
            PlannerContext plannerContext,
            Session session,
            TypeProvider types,
            TypeAnalyzer typeAnalyzer)
    {
        return node.accept(new Visitor(plannerContext, session, types, typeAnalyzer), inputProperties);
    }

    private static class Visitor
            extends PlanVisitor<ActualProperties, List<ActualProperties>>
    {
        private final PlannerContext plannerContext;
        private final Session session;
        private final TypeProvider types;
        private final TypeAnalyzer typeAnalyzer;

        public Visitor(PlannerContext plannerContext, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
        {
            this.plannerContext = plannerContext;
            this.session = session;
            this.types = types;
            this.typeAnalyzer = typeAnalyzer;
        }

        @Override
        protected ActualProperties visitPlan(PlanNode node, List<ActualProperties> inputProperties)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitExplainAnalyze(ExplainAnalyzeNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitOutput(OutputNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties)
                    .translate(column -> PropertyDerivations.filterIfMissing(node.getOutputSymbols(), column));
        }

        @Override
        public ActualProperties visitEnforceSingleRow(EnforceSingleRowNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitAssignUniqueId(AssignUniqueId node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ImmutableList.Builder<LocalProperty<Symbol>> newLocalProperties = ImmutableList.builder();
            newLocalProperties.addAll(properties.getLocalProperties());
            newLocalProperties.add(new GroupingProperty<>(ImmutableList.of(node.getIdColumn())));
            node.getSource().getOutputSymbols()
                    .forEach(column -> newLocalProperties.add(new ConstantProperty<>(column)));

            if (properties.getNodePartitioning().isPresent()) {
                // preserve input (possibly preferred) partitioning
                return ActualProperties.builderFrom(properties)
                        .local(newLocalProperties.build())
                        .build();
            }

            return ActualProperties.builderFrom(properties)
                    .global(partitionedOn(ARBITRARY_DISTRIBUTION, ImmutableList.of(node.getIdColumn()), Optional.of(ImmutableList.of(node.getIdColumn()))))
                    .local(newLocalProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitApply(ApplyNode node, List<ActualProperties> inputProperties)
        {
            throw new IllegalArgumentException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitCorrelatedJoin(CorrelatedJoinNode node, List<ActualProperties> inputProperties)
        {
            throw new IllegalArgumentException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitMarkDistinct(MarkDistinctNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitWindow(WindowNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            // If the input is completely pre-partitioned and sorted, then the original input properties will be respected
            Optional<OrderingScheme> orderingScheme = node.getOrderingScheme();
            if (ImmutableSet.copyOf(node.getPartitionBy()).equals(node.getPrePartitionedInputs())
                    && (orderingScheme.isEmpty() || node.getPreSortedOrderPrefix() == orderingScheme.get().getOrderBy().size())) {
                return properties;
            }

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();

            // If the WindowNode has pre-partitioned inputs, then it will not change the order of those inputs at output,
            // so we should just propagate those underlying local properties that guarantee the pre-partitioning.
            // TODO: come up with a more general form of this operation for other streaming operators
            if (!node.getPrePartitionedInputs().isEmpty()) {
                GroupingProperty<Symbol> prePartitionedProperty = new GroupingProperty<>(node.getPrePartitionedInputs());
                for (LocalProperty<Symbol> localProperty : properties.getLocalProperties()) {
                    if (!prePartitionedProperty.isSimplifiedBy(localProperty)) {
                        break;
                    }
                    localProperties.add(localProperty);
                }
            }

            if (!node.getPartitionBy().isEmpty()) {
                localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }

            orderingScheme.ifPresent(ordering -> localProperties.addAll(ordering.toLocalProperties()));

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.normalizeAndPrune(localProperties.build()))
                    .build();
        }

        @Override
        public ActualProperties visitPatternRecognition(PatternRecognitionNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);
            // Crop properties to output columns.
            ActualProperties translatedProperties = properties.translate(symbol -> node.getOutputSymbols().contains(symbol) ? Optional.of(symbol) : Optional.empty());

            // If the input is completely pre-partitioned and sorted, then the original input properties will be respected is some cases.
            // ALL ROW PER MATCH with overlapping matches might shuffle rows and break the order.
            // Otherwise, partitioning and sorting will be respected.
            Optional<OrderingScheme> orderingScheme = node.getOrderingScheme();
            if (ImmutableSet.copyOf(node.getPartitionBy()).equals(node.getPrePartitionedInputs())
                    && (orderingScheme.isEmpty() || node.getPreSortedOrderPrefix() == orderingScheme.get().getOrderBy().size())) {
                if (node.getRowsPerMatch() == WINDOW ||
                        node.getRowsPerMatch() == ONE ||
                        node.getSkipToPosition() == PAST_LAST) {
                    return translatedProperties;
                }
            }

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();

            // If the PatternRecognitionNode has pre-partitioned inputs, then it will not change the order of those inputs at output,
            // so we should just propagate those underlying local properties that guarantee the pre-partitioning.
            // TODO: come up with a more general form of this operation for other streaming operators
            if (!node.getPrePartitionedInputs().isEmpty()) {
                GroupingProperty<Symbol> prePartitionedProperty = new GroupingProperty<>(node.getPrePartitionedInputs());
                for (LocalProperty<Symbol> localProperty : translatedProperties.getLocalProperties()) {
                    if (!prePartitionedProperty.isSimplifiedBy(localProperty)) {
                        break;
                    }
                    localProperties.add(localProperty);
                }
            }

            if (!node.getPartitionBy().isEmpty()) {
                localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }

            // Sorted properties should be propagated only in certain cases.
            // ALL ROW PER MATCH with overlapping matches might shuffle rows.
            if (node.getRowsPerMatch().isOneRow() || node.getSkipToPosition() == PAST_LAST) {
                Set<Symbol> outputs = ImmutableSet.copyOf(node.getOutputSymbols());
                orderingScheme.ifPresent(ordering ->
                        ordering.toLocalProperties().stream()
                                .filter(property -> outputs.containsAll(property.getColumns()))
                                .forEach(localProperties::add));
            }

            return ActualProperties.builderFrom(translatedProperties)
                    .local(localProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitTableFunction(TableFunctionNode node, List<ActualProperties> inputProperties)
        {
            throw new IllegalStateException(format("Unexpected node: TableFunctionNode (%s)", node.getName()));
        }

        @Override
        public ActualProperties visitTableFunctionProcessor(TableFunctionProcessorNode node, List<ActualProperties> inputProperties)
        {
            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();

            if (node.getSource().isPresent()) {
                ActualProperties properties = Iterables.getOnlyElement(inputProperties);

                // Only the partitioning properties of the source are passed-through, because the pass-through mechanism preserves the partitioning values.
                // Sorting properties might be broken because input rows can be shuffled or nulls can be inserted as the result of pass-through.
                // Constant properties might be broken because nulls can be inserted as the result of pass-through.
                if (!node.getPrePartitioned().isEmpty()) {
                    GroupingProperty<Symbol> prePartitionedProperty = new GroupingProperty<>(node.getPrePartitioned());
                    for (LocalProperty<Symbol> localProperty : properties.getLocalProperties()) {
                        if (!prePartitionedProperty.isSimplifiedBy(localProperty)) {
                            break;
                        }
                        localProperties.add(localProperty);
                    }
                }
            }

            List<Symbol> partitionBy = node.getSpecification()
                    .map(DataOrganizationSpecification::getPartitionBy)
                    .orElse(ImmutableList.of());
            if (!partitionBy.isEmpty()) {
                localProperties.add(new GroupingProperty<>(partitionBy));
            }

            // TODO add global single stream property when there's Specification present with no partitioning columns

            return ActualProperties.builder()
                    .local(localProperties.build())
                    .build()
                    // Crop properties to output columns.
                    .translate(symbol -> node.getOutputSymbols().contains(symbol) ? Optional.of(symbol) : Optional.empty());
        }

        @Override
        public ActualProperties visitGroupId(GroupIdNode node, List<ActualProperties> inputProperties)
        {
            Map<Symbol, Symbol> inputToOutputMappings = new HashMap<>();
            for (Map.Entry<Symbol, Symbol> setMapping : node.getGroupingColumns().entrySet()) {
                if (node.getCommonGroupingColumns().contains(setMapping.getKey())) {
                    // TODO: Add support for translating a property on a single column to multiple columns
                    // when GroupIdNode is copying a single input grouping column into multiple output grouping columns (i.e. aliases), this is basically picking one arbitrarily
                    inputToOutputMappings.putIfAbsent(setMapping.getValue(), setMapping.getKey());
                }
            }

            // TODO: Add support for translating a property on a single column to multiple columns
            // this is deliberately placed after the grouping columns, because preserving properties has a bigger perf impact
            for (Symbol argument : node.getAggregationArguments()) {
                inputToOutputMappings.putIfAbsent(argument, argument);
            }

            return Iterables.getOnlyElement(inputProperties).translate(column -> Optional.ofNullable(inputToOutputMappings.get(column)));
        }

        @Override
        public ActualProperties visitAggregation(AggregationNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties translated = properties.translate(symbol -> node.getGroupingKeys().contains(symbol) ? Optional.of(symbol) : Optional.empty());

            return ActualProperties.builderFrom(translated)
                    .local(LocalProperties.grouped(node.getGroupingKeys()))
                    .build();
        }

        @Override
        public ActualProperties visitRowNumber(RowNumberNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitTopNRanking(TopNRankingNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();
            localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            localProperties.addAll(node.getOrderingScheme().toLocalProperties());

            return ActualProperties.builderFrom(properties)
                    .local(localProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitTopN(TopNNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return ActualProperties.builderFrom(properties)
                    .local(node.getOrderingScheme().toLocalProperties())
                    .build();
        }

        @Override
        public ActualProperties visitSort(SortNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return ActualProperties.builderFrom(properties)
                    .local(node.getOrderingScheme().toLocalProperties())
                    .build();
        }

        @Override
        public ActualProperties visitLimit(LimitNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitDistinctLimit(DistinctLimitNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.grouped(node.getDistinctSymbols()))
                    .build();
        }

        @Override
        public ActualProperties visitStatisticsWriterNode(StatisticsWriterNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableFinish(TableFinishNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableDelete(TableDeleteNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableExecute(TableExecuteNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            if (properties.isCoordinatorOnly()) {
                return ActualProperties.builder()
                        .global(coordinatorSingleStreamPartition())
                        .build();
            }
            return ActualProperties.builder()
                    .global(properties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                    .build();
        }

        @Override
        public ActualProperties visitSimpleTableExecuteNode(SimpleTableExecuteNode node, List<ActualProperties> inputProperties)
        {
            // metadata operations always run on the coordinator
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitMergeWriter(MergeWriterNode node, List<ActualProperties> inputProperties)
        {
            return visitPartitionedWriter(inputProperties);
        }

        @Override
        public ActualProperties visitMergeProcessor(MergeProcessorNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties).translate(symbol -> Optional.empty());
        }

        @Override
        public ActualProperties visitJoin(JoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);

            boolean unordered = spillPossible(session, node.getType());

            return switch (node.getType()) {
                case INNER -> {
                    probeProperties = probeProperties.translate(column -> filterOrRewrite(node.getOutputSymbols(), node.getCriteria(), column));
                    buildProperties = buildProperties.translate(column -> filterOrRewrite(node.getOutputSymbols(), node.getCriteria(), column));

                    Map<Symbol, NullableValue> constants = new HashMap<>();
                    constants.putAll(probeProperties.getConstants());
                    constants.putAll(buildProperties.getConstants());

                    if (node.isCrossJoin()) {
                        // Cross join preserves only constants from probe and build sides.
                        // Cross join doesn't preserve sorting or grouping local properties on either side.
                        yield ActualProperties.builder()
                                .global(probeProperties)
                                .local(ImmutableList.of())
                                .constants(constants)
                                .build();
                    }

                    yield ActualProperties.builderFrom(probeProperties)
                            .constants(constants)
                            .unordered(unordered)
                            .build();
                }
                case LEFT -> ActualProperties.builderFrom(probeProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                        .unordered(unordered)
                        .build();
                case RIGHT -> ActualProperties.builderFrom(buildProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                        .local(ImmutableList.of())
                        .unordered(true)
                        .build();
                case FULL ->
                        // We can't say anything about the partitioning scheme because any partition of
                        // a hash-partitioned join can produce nulls in case of a lack of matches
                        ActualProperties.builder()
                                .global(probeProperties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                                .build();
            };
        }

        @Override
        public ActualProperties visitSemiJoin(SemiJoinNode node, List<ActualProperties> inputProperties)
        {
            return inputProperties.get(0);
        }

        @Override
        public ActualProperties visitSpatialJoin(SpatialJoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);

            return switch (node.getType()) {
                case INNER -> {
                    probeProperties = probeProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column));
                    buildProperties = buildProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column));

                    Map<Symbol, NullableValue> constants = new HashMap<>();
                    constants.putAll(probeProperties.getConstants());
                    constants.putAll(buildProperties.getConstants());

                    yield ActualProperties.builderFrom(probeProperties)
                            .constants(constants)
                            .build();
                }
                case LEFT -> ActualProperties.builderFrom(probeProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                        .build();
            };
        }

        @Override
        public ActualProperties visitIndexJoin(IndexJoinNode node, List<ActualProperties> inputProperties)
        {
            // TODO: include all equivalent columns in partitioning properties
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties indexProperties = inputProperties.get(1);

            return switch (node.getType()) {
                case INNER -> ActualProperties.builderFrom(probeProperties)
                        .constants(ImmutableMap.<Symbol, NullableValue>builder()
                                .putAll(probeProperties.getConstants())
                                .putAll(indexProperties.getConstants())
                                .buildOrThrow())
                        .build();
                case SOURCE_OUTER -> ActualProperties.builderFrom(probeProperties)
                        .constants(probeProperties.getConstants())
                        .build();
            };
        }

        @Override
        public ActualProperties visitDynamicFilterSource(DynamicFilterSourceNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitIndexSource(IndexSourceNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(singleStreamPartition())
                    .build();
        }

        public static Map<Symbol, Symbol> exchangeInputToOutput(ExchangeNode node, int sourceIndex)
        {
            List<Symbol> inputSymbols = node.getInputs().get(sourceIndex);
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                inputToOutput.put(inputSymbols.get(i), node.getOutputSymbols().get(i));
            }
            return inputToOutput;
        }

        @Override
        public ActualProperties visitExchange(ExchangeNode node, List<ActualProperties> inputProperties)
        {
            checkArgument(node.getScope() != REMOTE || inputProperties.stream().noneMatch(ActualProperties::isNullsAndAnyReplicated), "Null-and-any replicated inputs should not be remotely exchanged");

            Set<Map.Entry<Symbol, NullableValue>> entries = null;
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                Map<Symbol, Symbol> inputToOutput = exchangeInputToOutput(node, sourceIndex);
                ActualProperties translated = inputProperties.get(sourceIndex).translate(symbol -> Optional.ofNullable(inputToOutput.get(symbol)));

                entries = (entries == null) ? translated.getConstants().entrySet() : Sets.intersection(entries, translated.getConstants().entrySet());
            }
            checkState(entries != null);

            Map<Symbol, NullableValue> constants = entries.stream()
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();
            node.getOrderingScheme().ifPresent(orderingScheme -> localProperties.addAll(orderingScheme.toLocalProperties()));

            // Local exchanges are only created in AddLocalExchanges, at the end of optimization, and
            // local exchanges do not produce all global properties as represented by ActualProperties.
            // This is acceptable because AddLocalExchanges does not use global properties and is only
            // interested in the local properties.
            // However, for the purpose of validation, some global properties (single-node vs distributed)
            // need to be propagated through local exchanges.
            // TODO: implement full properties for local exchanges
            if (node.getScope() == LOCAL) {
                if (inputProperties.size() == 1) {
                    ActualProperties inputProperty = inputProperties.get(0);
                    if (inputProperty.isEffectivelySingleStream() && node.getOrderingScheme().isEmpty()) {
                        verify(node.getInputs().size() == 1);
                        Map<Symbol, Symbol> inputToOutput = exchangeInputToOutput(node, 0);
                        // Single stream input's local sorting and grouping properties are preserved
                        // In case of merging exchange, it's orderingScheme takes precedence
                        localProperties.addAll(LocalProperties.translate(
                                inputProperty.getLocalProperties(),
                                symbol -> Optional.ofNullable(inputToOutput.get(symbol))));
                    }
                }

                ActualProperties.Builder builder = ActualProperties.builder();
                builder.local(localProperties.build());
                builder.constants(constants);

                if (inputProperties.stream().anyMatch(ActualProperties::isCoordinatorOnly)) {
                    builder.global(partitionedOn(
                            COORDINATOR_DISTRIBUTION,
                            ImmutableList.of(),
                            // only gathering local exchange preserves single stream property
                            node.getType() == GATHER ? Optional.of(ImmutableList.of()) : Optional.empty()));
                }
                else if (inputProperties.stream().anyMatch(ActualProperties::isSingleNode)) {
                    builder.global(partitionedOn(
                            SINGLE_DISTRIBUTION,
                            ImmutableList.of(),
                            // only gathering local exchange preserves single stream property
                            node.getType() == GATHER ? Optional.of(ImmutableList.of()) : Optional.empty()));
                }

                return builder.build();
            }

            return switch (node.getType()) {
                case GATHER -> ActualProperties.builder()
                        .global(node.getPartitioningScheme().getPartitioning().getHandle().isCoordinatorOnly() ? coordinatorSingleStreamPartition() : singleStreamPartition())
                        .local(localProperties.build())
                        .constants(constants)
                        .build();
                case REPARTITION -> ActualProperties.builder()
                        .global(partitionedOn(
                                node.getPartitioningScheme().getPartitioning(),
                                Optional.of(node.getPartitioningScheme().getPartitioning()))
                                .withReplicatedNulls(node.getPartitioningScheme().isReplicateNullsAndAny()))
                        .constants(constants)
                        .build();
                case REPLICATE ->
                        // TODO: this should have the same global properties as the stream taking the replicated data
                        ActualProperties.builder()
                                .global(arbitraryPartition())
                                .constants(constants)
                                .build();
            };
        }

        @Override
        public ActualProperties visitFilter(FilterNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                    plannerContext,
                    session,
                    node.getPredicate(),
                    types);

            Map<Symbol, NullableValue> constants = new HashMap<>(properties.getConstants());
            constants.putAll(extractFixedValues(decomposedPredicate.getTupleDomain()).orElse(ImmutableMap.of()));

            return ActualProperties.builderFrom(properties)
                    .constants(constants)
                    .build();
        }

        @Override
        public ActualProperties visitProject(ProjectNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments().getMap());

            ActualProperties translatedProperties = properties.translate(column -> Optional.ofNullable(identities.get(column)), expression -> rewriteExpression(node.getAssignments().getMap(), expression));

            // Extract additional constants
            Map<Symbol, NullableValue> constants = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
                Expression expression = assignment.getValue();

                Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, expression);
                Type type = requireNonNull(expressionTypes.get(NodeRef.of(expression)));
                ExpressionInterpreter optimizer = new ExpressionInterpreter(expression, plannerContext, session, expressionTypes);
                // TODO:
                // We want to use a symbol resolver that looks up in the constants from the input subplan
                // to take advantage of constant-folding for complex expressions
                // However, that currently causes errors when those expressions operate on arrays or row types
                // ("ROW comparison not supported for fields with null elements", etc)
                Object value = optimizer.optimize(NoOpSymbolResolver.INSTANCE);

                if (value instanceof SymbolReference) {
                    Symbol symbol = Symbol.from((SymbolReference) value);
                    NullableValue existingConstantValue = constants.get(symbol);
                    if (existingConstantValue != null) {
                        constants.put(assignment.getKey(), new NullableValue(type, value));
                    }
                }
                else if (!(value instanceof Expression)) {
                    constants.put(assignment.getKey(), new NullableValue(type, value));
                }
            }
            constants.putAll(translatedProperties.getConstants());

            return ActualProperties.builderFrom(translatedProperties)
                    .constants(constants)
                    .build();
        }

        @Override
        public ActualProperties visitRefreshMaterializedView(RefreshMaterializedViewNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableWriter(TableWriterNode node, List<ActualProperties> inputProperties)
        {
            return visitPartitionedWriter(inputProperties);
        }

        private ActualProperties visitPartitionedWriter(List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            if (properties.isCoordinatorOnly()) {
                return ActualProperties.builder()
                        .global(coordinatorSingleStreamPartition())
                        .build();
            }
            return ActualProperties.builder()
                    .global(properties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                    .build();
        }

        @Override
        public ActualProperties visitSample(SampleNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitUnnest(UnnestNode node, List<ActualProperties> inputProperties)
        {
            Set<Symbol> passThroughInputs = ImmutableSet.copyOf(node.getReplicateSymbols());

            ActualProperties translatedProperties = Iterables.getOnlyElement(inputProperties).translate(column -> {
                if (passThroughInputs.contains(column)) {
                    return Optional.of(column);
                }
                return Optional.empty();
            });

            return switch (node.getJoinType()) {
                case INNER, LEFT -> translatedProperties;
                case RIGHT, FULL -> ActualProperties.builderFrom(translatedProperties)
                        .local(ImmutableList.of())
                        .build();
            };
        }

        @Override
        public ActualProperties visitValues(ValuesNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(singleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableScan(TableScanNode node, List<ActualProperties> inputProperties)
        {
            TableProperties layout = plannerContext.getMetadata().getTableProperties(session, node.getTable());
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            ActualProperties.Builder properties = ActualProperties.builder();

            // Globally constant assignments
            Map<ColumnHandle, NullableValue> globalConstants = new HashMap<>();

            extractFixedValues(layout.getPredicate())
                    .orElse(ImmutableMap.of())
                    .entrySet().stream()
                    .filter(entry -> !entry.getValue().isNull())
                    .forEach(entry -> globalConstants.put(entry.getKey(), entry.getValue()));

            Map<Symbol, NullableValue> symbolConstants = globalConstants.entrySet().stream()
                    .filter(entry -> assignments.containsKey(entry.getKey()))
                    .collect(toMap(entry -> assignments.get(entry.getKey()), Map.Entry::getValue));
            properties.constants(symbolConstants);

            // Partitioning properties
            properties.global(deriveGlobalProperties(node, layout, assignments, globalConstants));

            // Append the global constants onto the local properties to maximize their translation potential
            List<LocalProperty<ColumnHandle>> constantAppendedLocalProperties = ImmutableList.<LocalProperty<ColumnHandle>>builder()
                    .addAll(globalConstants.keySet().stream().map(ConstantProperty::new).iterator())
                    .addAll(layout.getLocalProperties())
                    .build();
            properties.local(LocalProperties.translate(constantAppendedLocalProperties, column -> Optional.ofNullable(assignments.get(column))));

            return properties.build();
        }

        private Global deriveGlobalProperties(TableScanNode node, TableProperties layout, Map<ColumnHandle, Symbol> assignments, Map<ColumnHandle, NullableValue> constants)
        {
            Optional<List<Symbol>> streamPartitioning = layout.getStreamPartitioningColumns()
                    .flatMap(columns -> translateToNonConstantSymbols(columns, assignments, constants));

            if (layout.getTablePartitioning().isPresent() && node.isUseConnectorNodePartitioning()) {
                TablePartitioning tablePartitioning = layout.getTablePartitioning().get();
                if (assignments.keySet().containsAll(tablePartitioning.getPartitioningColumns())) {
                    List<Symbol> arguments = tablePartitioning.getPartitioningColumns().stream()
                            .map(assignments::get)
                            .collect(toImmutableList());

                    return partitionedOn(tablePartitioning.getPartitioningHandle(), arguments, streamPartitioning);
                }
            }

            if (streamPartitioning.isPresent()) {
                return streamPartitionedOn(streamPartitioning.get());
            }
            return arbitraryPartition();
        }

        private static Optional<List<Symbol>> translateToNonConstantSymbols(
                Set<ColumnHandle> columnHandles,
                Map<ColumnHandle, Symbol> assignments,
                Map<ColumnHandle, NullableValue> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.containsKey(column))
                    .collect(toImmutableSet());

            ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();
            for (ColumnHandle column : constantsStrippedColumns) {
                Symbol translated = assignments.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(translated);
            }

            return Optional.of(ImmutableList.copyOf(builder.build()));
        }

        private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
        {
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                if (assignment.getValue() instanceof SymbolReference) {
                    inputToOutput.put(Symbol.from(assignment.getValue()), assignment.getKey());
                }
            }
            return inputToOutput;
        }
    }

    static boolean spillPossible(Session session, JoinNode.Type joinType)
    {
        if (!SystemSessionProperties.isSpillEnabled(session)) {
            return false;
        }
        return switch (joinType) {
            case INNER, LEFT -> true;
            // Even though join might not have "spillable" property set yet
            // it might still be set as spillable later on by AddLocalExchanges.
            case RIGHT, FULL -> false; // Currently there is no spill support for outer on the build side.
        };
    }

    public static Optional<Symbol> filterIfMissing(Collection<Symbol> columns, Symbol column)
    {
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        return Optional.empty();
    }

    // Used to filter columns that are not exposed by join node
    // Or, if they are part of the equalities, to translate them
    // to the other symbol if that's exposed, instead.
    public static Optional<Symbol> filterOrRewrite(Collection<Symbol> columns, Collection<JoinNode.EquiJoinClause> equalities, Symbol column)
    {
        // symbol is exposed directly, so no translation needed
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        // if the column is part of the equality conditions and its counterpart
        // is exposed, use that, instead
        for (JoinNode.EquiJoinClause equality : equalities) {
            if (equality.getLeft().equals(column) && columns.contains(equality.getRight())) {
                return Optional.of(equality.getRight());
            }
            if (equality.getRight().equals(column) && columns.contains(equality.getLeft())) {
                return Optional.of(equality.getLeft());
            }
        }

        return Optional.empty();
    }

    private static Optional<Symbol> rewriteExpression(Map<Symbol, Expression> assignments, Expression expression)
    {
        // Only simple coalesce expressions supported currently
        if (!(expression instanceof CoalesceExpression)) {
            return Optional.empty();
        }

        Set<Expression> arguments = ImmutableSet.copyOf(((CoalesceExpression) expression).getOperands());
        if (!arguments.stream().allMatch(SymbolReference.class::isInstance)) {
            return Optional.empty();
        }

        // We are using the property that the result of coalesce from full outer join keys would not be null despite of the order
        // of the arguments. Thus we extract and compare the symbols of the CoalesceExpression as a set rather than compare the
        // CoalesceExpression directly.
        for (Map.Entry<Symbol, Expression> entry : assignments.entrySet()) {
            if (entry.getValue() instanceof CoalesceExpression) {
                Set<Expression> candidateArguments = ImmutableSet.copyOf(((CoalesceExpression) entry.getValue()).getOperands());
                if (!candidateArguments.stream().allMatch(SymbolReference.class::isInstance)) {
                    return Optional.empty();
                }

                if (candidateArguments.equals(arguments)) {
                    return Optional.of(entry.getKey());
                }
            }
        }
        return Optional.empty();
    }
}
