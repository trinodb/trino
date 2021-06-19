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
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
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
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticsWriterNode;
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
import io.trino.sql.tree.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.intersection;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.INNER;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.CorrelatedJoinNode.Type.RIGHT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * Removes all computation that does is not referenced transitively from the root of the plan
 * <p>
 * E.g.,
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2, $3 = $4 / $5] -> ...}
 * <p>
 * gets rewritten as
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2] -> ...}
 */
public class PruneUnreferencedOutputs
        implements PlanOptimizer
{
    private final Metadata metadata;

    public PruneUnreferencedOutputs(Metadata metadata)
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

        return SimplePlanRewriter.rewriteWith(new Rewriter(metadata, types, session), plan, ImmutableSet.of());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Set<Symbol>>
    {
        private final Metadata metadata;
        private final TypeProvider types;
        private final Session session;

        public Rewriter(Metadata metadata, TypeProvider types, Session session)
        {
            this.metadata = metadata;
            this.types = types;
            this.session = session;
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<Set<Symbol>> context)
        {
            return context.defaultRewrite(node, ImmutableSet.copyOf(node.getSource().getOutputSymbols()));
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedOutputSymbols = Sets.newHashSet(context.get());
            node.getPartitioningScheme().getHashColumn().ifPresent(expectedOutputSymbols::add);
            expectedOutputSymbols.addAll(node.getPartitioningScheme().getPartitioning().getColumns());
            node.getOrderingScheme().ifPresent(orderingScheme -> expectedOutputSymbols.addAll(orderingScheme.getOrderBy()));

            List<List<Symbol>> inputsBySource = new ArrayList<>(node.getInputs().size());
            for (int i = 0; i < node.getInputs().size(); i++) {
                inputsBySource.add(new ArrayList<>());
            }

            List<Symbol> newOutputSymbols = new ArrayList<>(node.getOutputSymbols().size());
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol outputSymbol = node.getOutputSymbols().get(i);
                if (expectedOutputSymbols.contains(outputSymbol)) {
                    newOutputSymbols.add(outputSymbol);
                    for (int source = 0; source < node.getInputs().size(); source++) {
                        inputsBySource.get(source).add(node.getInputs().get(source).get(i));
                    }
                }
            }

            // newOutputSymbols contains all partition, sort and hash symbols so simply swap the output layout
            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    node.getPartitioningScheme().getPartitioning(),
                    newOutputSymbols,
                    node.getPartitioningScheme().getHashColumn(),
                    node.getPartitioningScheme().isReplicateNullsAndAny(),
                    node.getPartitioningScheme().getBucketToPartition());

            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                        .addAll(inputsBySource.get(i));

                rewrittenSources.add(context.rewrite(
                        node.getSources().get(i),
                        expectedInputs.build()));
            }

            return new ExchangeNode(
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    partitioningScheme,
                    rewrittenSources.build(),
                    inputsBySource,
                    node.getOrderingScheme());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedFilterInputs = node.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of());

            ImmutableSet.Builder<Symbol> leftInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(expectedFilterInputs)
                    .addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft));
            node.getLeftHashSymbol().ifPresent(leftInputs::add);

            ImmutableSet.Builder<Symbol> rightInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(expectedFilterInputs)
                    .addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight));
            node.getRightHashSymbol().ifPresent(rightInputs::add);

            PlanNode left = context.rewrite(node.getLeft(), leftInputs.build());
            PlanNode right = context.rewrite(node.getRight(), rightInputs.build());

            List<Symbol> leftOutputSymbols = node.getLeftOutputSymbols().stream()
                    .filter(context.get()::contains)
                    .distinct()
                    .collect(toImmutableList());
            List<Symbol> rightOutputSymbols = node.getRightOutputSymbols().stream()
                    .filter(context.get()::contains)
                    .distinct()
                    .collect(toImmutableList());

            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    leftOutputSymbols,
                    rightOutputSymbols,
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Set<Symbol>> context)
        {
            if (!context.get().contains(node.getSemiJoinOutput())) {
                return context.rewrite(node.getSource(), context.get());
            }

            ImmutableSet.Builder<Symbol> sourceInputsBuilder = ImmutableSet.builder();
            sourceInputsBuilder.addAll(context.get()).add(node.getSourceJoinSymbol());
            if (node.getSourceHashSymbol().isPresent()) {
                sourceInputsBuilder.add(node.getSourceHashSymbol().get());
            }
            Set<Symbol> sourceInputs = sourceInputsBuilder.build();

            ImmutableSet.Builder<Symbol> filteringSourceInputBuilder = ImmutableSet.builder();
            filteringSourceInputBuilder.add(node.getFilteringSourceJoinSymbol());
            if (node.getFilteringSourceHashSymbol().isPresent()) {
                filteringSourceInputBuilder.add(node.getFilteringSourceHashSymbol().get());
            }
            Set<Symbol> filteringSourceInputs = filteringSourceInputBuilder.build();

            PlanNode source = context.rewrite(node.getSource(), sourceInputs);
            PlanNode filteringSource = context.rewrite(node.getFilteringSource(), filteringSourceInputs);

            return new SemiJoinNode(
                    node.getId(),
                    source,
                    filteringSource,
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashSymbol(),
                    node.getFilteringSourceHashSymbol(),
                    node.getDistributionType(),
                    node.getDynamicFilterId());
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> requiredInputs = ImmutableSet.<Symbol>builder()
                    .addAll(SymbolsExtractor.extractUnique(node.getFilter()))
                    .addAll(context.get())
                    .build();

            ImmutableSet.Builder<Symbol> leftInputs = ImmutableSet.builder();
            node.getLeftPartitionSymbol().ifPresent(leftInputs::add);

            ImmutableSet.Builder<Symbol> rightInputs = ImmutableSet.builder();
            node.getRightPartitionSymbol().ifPresent(rightInputs::add);

            PlanNode left = context.rewrite(node.getLeft(), leftInputs.addAll(requiredInputs).build());
            PlanNode right = context.rewrite(node.getRight(), rightInputs.addAll(requiredInputs).build());

            List<Symbol> outputSymbols = node.getOutputSymbols().stream()
                    .filter(context.get()::contains)
                    .distinct()
                    .collect(toImmutableList());

            return new SpatialJoinNode(node.getId(), node.getType(), left, right, outputSymbols, node.getFilter(), node.getLeftPartitionSymbol(), node.getRightPartitionSymbol(), node.getKdbTree());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> probeInputsBuilder = ImmutableSet.builder();
            probeInputsBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getProbe));
            if (node.getProbeHashSymbol().isPresent()) {
                probeInputsBuilder.add(node.getProbeHashSymbol().get());
            }
            Set<Symbol> probeInputs = probeInputsBuilder.build();

            ImmutableSet.Builder<Symbol> indexInputBuilder = ImmutableSet.builder();
            indexInputBuilder.addAll(context.get())
                    .addAll(Iterables.transform(node.getCriteria(), IndexJoinNode.EquiJoinClause::getIndex));
            if (node.getIndexHashSymbol().isPresent()) {
                indexInputBuilder.add(node.getIndexHashSymbol().get());
            }
            Set<Symbol> indexInputs = indexInputBuilder.build();

            PlanNode probeSource = context.rewrite(node.getProbeSource(), probeInputs);
            PlanNode indexSource = context.rewrite(node.getIndexSource(), indexInputs);

            return new IndexJoinNode(node.getId(), node.getType(), probeSource, indexSource, node.getCriteria(), node.getProbeHashSymbol(), node.getIndexHashSymbol());
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Set<Symbol>> context)
        {
            List<Symbol> newOutputSymbols = node.getOutputSymbols().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableList());

            Set<Symbol> newLookupSymbols = node.getLookupSymbols().stream()
                    .filter(context.get()::contains)
                    .collect(toImmutableSet());

            Map<Symbol, ColumnHandle> newAssignments = newOutputSymbols.stream()
                    .collect(Collectors.toMap(Function.identity(), node.getAssignments()::get));

            return new IndexSourceNode(node.getId(), node.getIndexHandle(), node.getTableHandle(), newLookupSymbols, newOutputSymbols, newAssignments);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getGroupingKeys());
            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }

            ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                if (context.get().contains(symbol)) {
                    Aggregation aggregation = entry.getValue();
                    expectedInputs.addAll(SymbolsExtractor.extractUnique(aggregation));
                    aggregations.put(symbol, aggregation);
                }
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new AggregationNode(
                    node.getId(),
                    source,
                    aggregations.build(),
                    node.getGroupingSets(),
                    ImmutableList.of(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Set<Symbol>> context)
        {
            Map<Symbol, WindowNode.Function> prunedFunctions = node.getWindowFunctions().entrySet().stream()
                    .filter(entry -> context.get().contains(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            if (prunedFunctions.isEmpty()) {
                return context.rewrite(node.getSource(), context.get());
            }

            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(node.getPartitionBy());

            node.getOrderingScheme().ifPresent(orderingScheme ->
                    orderingScheme.getOrderBy()
                            .forEach(expectedInputs::add));

            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }

            prunedFunctions.values().stream()
                    .map(SymbolsExtractor::extractUnique)
                    .forEach(expectedInputs::addAll);

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new WindowNode(
                    node.getId(),
                    source,
                    node.getSpecification(),
                    prunedFunctions,
                    node.getHashSymbol(),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Set<Symbol>> context)
        {
            return PruneTableScanColumns.pruneColumns(metadata, types, session, node, context.get())
                    .orElse(node);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(SymbolsExtractor.extractUnique(node.getPredicate()))
                    .addAll(context.get())
                    .build();

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.builder();

            List<Symbol> newAggregationArguments = node.getAggregationArguments().stream()
                    .filter(context.get()::contains)
                    .collect(Collectors.toList());
            expectedInputs.addAll(newAggregationArguments);

            ImmutableList.Builder<List<Symbol>> newGroupingSets = ImmutableList.builder();
            Map<Symbol, Symbol> newGroupingMapping = new HashMap<>();

            for (List<Symbol> groupingSet : node.getGroupingSets()) {
                ImmutableList.Builder<Symbol> newGroupingSet = ImmutableList.builder();

                for (Symbol output : groupingSet) {
                    if (context.get().contains(output)) {
                        newGroupingSet.add(output);
                        newGroupingMapping.putIfAbsent(output, node.getGroupingColumns().get(output));
                        expectedInputs.add(node.getGroupingColumns().get(output));
                    }
                }
                newGroupingSets.add(newGroupingSet.build());
            }

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new GroupIdNode(node.getId(), source, newGroupingSets.build(), newGroupingMapping, newAggregationArguments, node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Set<Symbol>> context)
        {
            if (!context.get().contains(node.getMarkerSymbol())) {
                return context.rewrite(node.getSource(), context.get());
            }

            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getDistinctSymbols())
                    .addAll(context.get().stream()
                            .filter(symbol -> !symbol.equals(node.getMarkerSymbol()))
                            .collect(toImmutableList()));

            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new MarkDistinctNode(node.getId(), source, node.getMarkerSymbol(), node.getDistinctSymbols(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> contextAndFilterSymbolsBuilder = ImmutableSet.<Symbol>builder()
                    .addAll(context.get());
            node.getFilter().ifPresent(expression -> contextAndFilterSymbolsBuilder.addAll(SymbolsExtractor.extractUnique(expression)));
            Set<Symbol> contextAndFilterSymbols = contextAndFilterSymbolsBuilder.build();

            List<Symbol> prunedReplicateSymbols = node.getReplicateSymbols().stream()
                    .filter(contextAndFilterSymbols::contains)
                    .collect(toImmutableList());

            Optional<Symbol> prunedOrdinalitySymbol = node.getOrdinalitySymbol()
                    .filter(contextAndFilterSymbols::contains);

            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(prunedReplicateSymbols);
            node.getMappings().stream()
                    .map(UnnestNode.Mapping::getInput)
                    .forEach(expectedInputs::add);

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new UnnestNode(node.getId(), source, prunedReplicateSymbols, node.getMappings(), prunedOrdinalitySymbol, node.getJoinType(), node.getFilter());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.builder();

            Assignments.Builder builder = Assignments.builder();
            node.getAssignments().forEach((symbol, expression) -> {
                if (context.get().contains(symbol)) {
                    expectedInputs.addAll(SymbolsExtractor.extractUnique(expression));
                    builder.put(symbol, expression);
                }
            });

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new ProjectNode(node.getId(), source, builder.build());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(node.getOutputSymbols());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new OutputNode(node.getId(), source, node.getColumnNames(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitOffset(OffsetNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new OffsetNode(node.getId(), source, node.getCount());
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(node.getTiesResolvingScheme().map(OrderingScheme::getOrderBy).orElse(ImmutableList.of()))
                    .addAll(node.getPreSortedInputs());
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new LimitNode(node.getId(), source, node.getCount(), node.getTiesResolvingScheme(), node.isPartial(), node.getPreSortedInputs());
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs;
            if (node.getHashSymbol().isPresent()) {
                expectedInputs = ImmutableSet.copyOf(concat(node.getDistinctSymbols(), ImmutableList.of(node.getHashSymbol().get())));
            }
            else {
                expectedInputs = ImmutableSet.copyOf(node.getDistinctSymbols());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new DistinctLimitNode(node.getId(), source, node.getLimit(), node.isPartial(), node.getDistinctSymbols(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(node.getOrderingScheme().getOrderBy());

            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNNode(node.getId(), source, node.getCount(), node.getOrderingScheme(), node.getStep());
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Set<Symbol>> context)
        {
            // Remove unused RowNumberNode
            if (!context.get().contains(node.getRowNumberSymbol())) {
                PlanNode source = context.rewrite(node.getSource(), context.get());
                if (node.getMaxRowCountPerPartition().isEmpty()) {
                    return source;
                }
                if (node.getPartitionBy().isEmpty()) {
                    return new LimitNode(node.getId(), source, node.getMaxRowCountPerPartition().get(), false);
                }
            }

            ImmutableSet.Builder<Symbol> inputsBuilder = ImmutableSet.builder();
            ImmutableSet.Builder<Symbol> expectedInputs = inputsBuilder
                    .addAll(context.get())
                    .addAll(node.getPartitionBy());

            if (node.getHashSymbol().isPresent()) {
                inputsBuilder.add(node.getHashSymbol().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new RowNumberNode(node.getId(), source, node.getPartitionBy(), node.isOrderSensitive(), node.getRowNumberSymbol(), node.getMaxRowCountPerPartition(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitTopNRanking(TopNRankingNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(node.getPartitionBy())
                    .addAll(node.getOrderingScheme().getOrderBy());

            if (node.getHashSymbol().isPresent()) {
                expectedInputs.add(node.getHashSymbol().get());
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNRankingNode(
                    node.getId(),
                    source,
                    node.getSpecification(),
                    node.getRankingType(),
                    node.getRankingSymbol(),
                    node.getMaxRankingPerPartition(),
                    node.isPartial(),
                    node.getHashSymbol());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedInputs = ImmutableSet.copyOf(concat(context.get(), node.getOrderingScheme().getOrderBy()));

            PlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new SortNode(node.getId(), source, node.getOrderingScheme(), node.isPartial());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableSet.Builder<Symbol> expectedInputs = ImmutableSet.<Symbol>builder()
                    .addAll(node.getColumns());
            if (node.getPartitioningScheme().isPresent()) {
                PartitioningScheme partitioningScheme = node.getPartitioningScheme().get();
                partitioningScheme.getPartitioning().getColumns().forEach(expectedInputs::add);
                partitioningScheme.getHashColumn().ifPresent(expectedInputs::add);
            }
            if (node.getStatisticsAggregation().isPresent()) {
                StatisticAggregations aggregations = node.getStatisticsAggregation().get();
                expectedInputs.addAll(aggregations.getGroupingSymbols());
                aggregations.getAggregations().values().forEach(aggregation -> expectedInputs.addAll(SymbolsExtractor.extractUnique(aggregation)));
            }
            PlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new TableWriterNode(
                    node.getId(),
                    source,
                    node.getTarget(),
                    node.getRowCountSymbol(),
                    node.getFragmentSymbol(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getNotNullColumnSymbols(),
                    node.getPartitioningScheme(),
                    node.getPreferredPartitioningScheme(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<Set<Symbol>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputSymbols()));
            return new StatisticsWriterNode(
                    node.getId(),
                    source,
                    node.getTarget(),
                    node.getRowCountSymbol(),
                    node.isRowCountEnabled(),
                    node.getDescriptor());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Set<Symbol>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getSource().getOutputSymbols()));
            return new TableFinishNode(
                    node.getId(),
                    source,
                    node.getTarget(),
                    node.getRowCountSymbol(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Set<Symbol>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.of(node.getRowId()));
            return new DeleteNode(node.getId(), source, node.getTarget(), node.getRowId(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitUpdate(UpdateNode node, RewriteContext<Set<Symbol>> context)
        {
            PlanNode source = context.rewrite(node.getSource(), ImmutableSet.copyOf(node.getColumnValueAndRowIdSymbols()));
            return new UpdateNode(node.getId(), source, node.getTarget(), node.getRowId(), node.getColumnValueAndRowIdSymbols(), node.getOutputSymbols());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Set<Symbol>> context)
        {
            // Find out which output symbols we need to keep
            ImmutableListMultimap.Builder<Symbol, Symbol> prunedMappingBuilder = ImmutableListMultimap.builder();
            for (Symbol symbol : node.getOutputSymbols()) {
                if (context.get().contains(symbol)) {
                    prunedMappingBuilder.putAll(symbol, node.getSymbolMapping().get(symbol));
                }
            }
            ListMultimap<Symbol, Symbol> prunedSymbolMapping = prunedMappingBuilder.build();

            // Find the corresponding input symbols to the remaining output symbols and prune the children
            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                ImmutableSet.Builder<Symbol> expectedSourceSymbols = ImmutableSet.builder();
                for (Collection<Symbol> symbols : prunedSymbolMapping.asMap().values()) {
                    expectedSourceSymbols.add(Iterables.get(symbols, i));
                }
                rewrittenSources.add(context.rewrite(node.getSources().get(i), expectedSourceSymbols.build()));
            }

            return new UnionNode(node.getId(), rewrittenSources.build(), prunedSymbolMapping, ImmutableList.copyOf(prunedSymbolMapping.keySet()));
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Set<Symbol>> context)
        {
            return rewriteSetOperationChildren(node, context);
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<Set<Symbol>> context)
        {
            return rewriteSetOperationChildren(node, context);
        }

        private PlanNode rewriteSetOperationChildren(SetOperationNode node, RewriteContext<Set<Symbol>> context)
        {
            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                rewrittenSources.add(context.rewrite(node.getSources().get(i), ImmutableSet.copyOf(node.sourceOutputLayout(i))));
            }
            return node.replaceChildren(rewrittenSources.build());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Set<Symbol>> context)
        {
            // nothing to prune: no output symbols and no expressions
            if (node.getRows().isEmpty()) {
                return node;
            }

            // handle the case of all output symbols pruned
            if (node.getOutputSymbols().stream().noneMatch(context.get()::contains)) {
                return new ValuesNode(node.getId(), node.getRowCount());
            }

            // if any of ValuesNode's rows is specified by expression other than Row, the redundant piece cannot be extracted and pruned
            if (!node.getRows().get().stream().allMatch(Row.class::isInstance)) {
                return node;
            }

            ImmutableList.Builder<Symbol> rewrittenOutputSymbolsBuilder = ImmutableList.builder();
            ImmutableList.Builder<ImmutableList.Builder<Expression>> rowBuildersBuilder = ImmutableList.builder();
            // Initialize builder for each row
            for (int i = 0; i < node.getRowCount(); i++) {
                rowBuildersBuilder.add(ImmutableList.builder());
            }
            ImmutableList<ImmutableList.Builder<Expression>> rowBuilders = rowBuildersBuilder.build();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol outputSymbol = node.getOutputSymbols().get(i);
                // If output symbol is used
                if (context.get().contains(outputSymbol)) {
                    rewrittenOutputSymbolsBuilder.add(outputSymbol);
                    // Add the value of the output symbol for each row
                    for (int j = 0; j < node.getRowCount(); j++) {
                        rowBuilders.get(j).add(((Row) node.getRows().get().get(j)).getItems().get(i));
                    }
                }
            }
            List<Expression> rewrittenRows = rowBuilders.stream()
                    .map(ImmutableList.Builder::build)
                    .map(Row::new)
                    .collect(toImmutableList());
            return new ValuesNode(node.getId(), rewrittenOutputSymbolsBuilder.build(), rewrittenRows);
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Set<Symbol>> context)
        {
            // remove unused apply nodes
            if (intersection(node.getSubqueryAssignments().getSymbols(), context.get()).isEmpty()) {
                return context.rewrite(node.getInput(), context.get());
            }

            // extract symbols required subquery plan
            ImmutableSet.Builder<Symbol> subqueryAssignmentsSymbolsBuilder = ImmutableSet.builder();
            Assignments.Builder subqueryAssignments = Assignments.builder();
            for (Map.Entry<Symbol, Expression> entry : node.getSubqueryAssignments().getMap().entrySet()) {
                Symbol output = entry.getKey();
                Expression expression = entry.getValue();
                if (context.get().contains(output)) {
                    subqueryAssignmentsSymbolsBuilder.addAll(SymbolsExtractor.extractUnique(expression));
                    subqueryAssignments.put(output, expression);
                }
            }

            Set<Symbol> subqueryAssignmentsSymbols = subqueryAssignmentsSymbolsBuilder.build();
            PlanNode subquery = context.rewrite(node.getSubquery(), subqueryAssignmentsSymbols);

            // prune not used correlation symbols
            Set<Symbol> subquerySymbols = SymbolsExtractor.extractUnique(subquery);
            List<Symbol> newCorrelation = node.getCorrelation().stream()
                    .filter(subquerySymbols::contains)
                    .collect(toImmutableList());

            Set<Symbol> inputContext = ImmutableSet.<Symbol>builder()
                    .addAll(context.get())
                    .addAll(newCorrelation)
                    .addAll(subqueryAssignmentsSymbols) // need to include those: e.g: "expr" from "expr IN (SELECT 1)"
                    .build();
            PlanNode input = context.rewrite(node.getInput(), inputContext);
            return new ApplyNode(node.getId(), input, subquery, subqueryAssignments.build(), newCorrelation, node.getOriginSubquery());
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Set<Symbol>> context)
        {
            if (!context.get().contains(node.getIdColumn())) {
                return context.rewrite(node.getSource(), context.get());
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitCorrelatedJoin(CorrelatedJoinNode node, RewriteContext<Set<Symbol>> context)
        {
            Set<Symbol> expectedFilterSymbols = SymbolsExtractor.extractUnique(node.getFilter());

            Set<Symbol> expectedFilterAndContextSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(expectedFilterSymbols)
                    .addAll(context.get())
                    .build();

            PlanNode subquery = context.rewrite(node.getSubquery(), expectedFilterAndContextSymbols);

            // remove unused correlated join nodes
            if (intersection(ImmutableSet.copyOf(subquery.getOutputSymbols()), context.get()).isEmpty()) {
                // remove unused subquery of inner join
                if (node.getType() == INNER && isScalar(subquery) && node.getFilter().equals(TRUE_LITERAL)) {
                    return context.rewrite(node.getInput(), context.get());
                }
                // remove unused subquery of left join
                if (node.getType() == LEFT && isAtMostScalar(subquery)) {
                    return context.rewrite(node.getInput(), context.get());
                }
            }

            // prune not used correlation symbols
            Set<Symbol> subquerySymbols = SymbolsExtractor.extractUnique(subquery);
            List<Symbol> newCorrelation = node.getCorrelation().stream()
                    .filter(subquerySymbols::contains)
                    .collect(toImmutableList());

            Set<Symbol> expectedCorrelationAndContextSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(newCorrelation)
                    .addAll(context.get())
                    .build();
            Set<Symbol> inputContext = ImmutableSet.<Symbol>builder()
                    .addAll(expectedCorrelationAndContextSymbols)
                    .addAll(expectedFilterSymbols)
                    .build();
            PlanNode input = context.rewrite(node.getInput(), inputContext);

            // remove unused input nodes
            if (intersection(ImmutableSet.copyOf(input.getOutputSymbols()), expectedCorrelationAndContextSymbols).isEmpty()) {
                // remove unused input of inner join
                if (node.getType() == INNER && isScalar(input) && node.getFilter().equals(TRUE_LITERAL)) {
                    return subquery;
                }
                // remove unused input of right join
                if (node.getType() == RIGHT && isAtMostScalar(input)) {
                    return subquery;
                }
            }

            return new CorrelatedJoinNode(node.getId(), input, subquery, newCorrelation, node.getType(), node.getFilter(), node.getOriginSubquery());
        }
    }
}
