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
package io.trino.sql.planner.planprinter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.json.JsonCodec;
import io.airlift.stats.TDigest;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.execution.StageStats;
import io.trino.execution.TableInfo;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.DescriptorArgument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExchangeNode.Scope;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.planprinter.NodeRepresentation.TypedSymbol;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SkipTo.Position;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctNanos;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.metadata.ResolvedFunction.extractFunctionName;
import static io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import static io.trino.spi.function.table.DescriptorArgument.NULL_DESCRIPTOR;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ExpressionUtils.combineConjunctsWithDuplicates;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;
import static io.trino.sql.planner.planprinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static io.trino.sql.planner.planprinter.TextRenderer.formatDouble;
import static io.trino.sql.planner.planprinter.TextRenderer.formatPositions;
import static io.trino.sql.planner.planprinter.TextRenderer.indentString;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.WINDOW;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class PlanPrinter
{
    private static final JsonCodec<Map<PlanFragmentId, JsonRenderedNode>> DISTRIBUTED_PLAN_CODEC =
            mapJsonCodec(PlanFragmentId.class, JsonRenderedNode.class);

    private final PlanRepresentation representation;
    private final Function<TableScanNode, TableInfo> tableInfoSupplier;
    private final Map<DynamicFilterId, DynamicFilterDomainStats> dynamicFilterDomainStats;
    private final ValuePrinter valuePrinter;
    private final Anonymizer anonymizer;

    // NOTE: do NOT add Metadata or Session to this class.  The plan printer must be usable outside of a transaction.
    @VisibleForTesting
    PlanPrinter(
            PlanNode planRoot,
            TypeProvider types,
            Function<TableScanNode, TableInfo> tableInfoSupplier,
            Map<DynamicFilterId, DynamicFilterDomainStats> dynamicFilterDomainStats,
            ValuePrinter valuePrinter,
            StatsAndCosts estimatedStatsAndCosts,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats,
            Anonymizer anonymizer)
    {
        requireNonNull(planRoot, "planRoot is null");
        requireNonNull(types, "types is null");
        requireNonNull(tableInfoSupplier, "tableInfoSupplier is null");
        requireNonNull(dynamicFilterDomainStats, "dynamicFilterDomainStats is null");
        requireNonNull(valuePrinter, "valuePrinter is null");
        requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(anonymizer, "anonymizer is null");

        this.tableInfoSupplier = tableInfoSupplier;
        this.dynamicFilterDomainStats = ImmutableMap.copyOf(dynamicFilterDomainStats);
        this.valuePrinter = valuePrinter;
        this.anonymizer = anonymizer;

        Optional<Duration> totalScheduledTime = stats.map(s -> new Duration(s.values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeScheduledTime().toMillis())
                .sum(), MILLISECONDS));

        Optional<Duration> totalCpuTime = stats.map(s -> new Duration(s.values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeCpuTime().toMillis())
                .sum(), MILLISECONDS));

        Optional<Duration> totalBlockedTime = stats.map(s -> new Duration(s.values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeBlockedTime().toMillis())
                .sum(), MILLISECONDS));

        this.representation = new PlanRepresentation(planRoot, types, totalCpuTime, totalScheduledTime, totalBlockedTime);

        Visitor visitor = new Visitor(types, estimatedStatsAndCosts, stats);
        planRoot.accept(visitor, new Context());
    }

    private String toText(boolean verbose, int level)
    {
        return new TextRenderer(verbose, level).render(representation);
    }

    @VisibleForTesting
    String toJson()
    {
        return new JsonRenderer().render(representation);
    }

    JsonRenderedNode toJsonRenderedNode()
    {
        return new JsonRenderer().renderJson(representation, representation.getRoot());
    }

    public static String jsonFragmentPlan(PlanNode root, Map<Symbol, Type> symbols, Metadata metadata, FunctionManager functionManager, Session session)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(symbols.entrySet().stream()
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

        TableInfoSupplier tableInfoSupplier = new TableInfoSupplier(metadata, session);
        ValuePrinter valuePrinter = new ValuePrinter(metadata, functionManager, session);
        return new PlanPrinter(
                root,
                typeProvider,
                tableInfoSupplier,
                ImmutableMap.of(),
                valuePrinter,
                StatsAndCosts.empty(),
                Optional.empty(),
                new NoOpAnonymizer())
                .toJson();
    }

    public static String jsonLogicalPlan(
            PlanNode plan,
            Session session,
            TypeProvider types,
            Metadata metadata,
            FunctionManager functionManager,
            StatsAndCosts estimatedStatsAndCosts)
    {
        TableInfoSupplier tableInfoSupplier = new TableInfoSupplier(metadata, session);
        ValuePrinter valuePrinter = new ValuePrinter(metadata, functionManager, session);
        return new PlanPrinter(
                plan,
                types,
                tableInfoSupplier,
                ImmutableMap.of(),
                valuePrinter,
                estimatedStatsAndCosts,
                Optional.empty(),
                new NoOpAnonymizer())
                .toJson();
    }

    public static String jsonDistributedPlan(
            StageInfo outputStageInfo,
            Session session,
            Metadata metadata,
            FunctionManager functionManager,
            Anonymizer anonymizer)
    {
        List<StageInfo> allStages = getAllStages(Optional.of(outputStageInfo));
        TypeProvider types = getTypeProvider(allStages.stream()
                .map(StageInfo::getPlan)
                .collect(toImmutableList()));
        Map<PlanNodeId, TableInfo> tableInfos = allStages.stream()
                .map(StageInfo::getTables)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        ValuePrinter valuePrinter = new ValuePrinter(metadata, functionManager, session);
        List<PlanFragment> planFragments = allStages.stream()
                .map(StageInfo::getPlan)
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        return jsonDistributedPlan(
                planFragments,
                tableScanNode -> tableInfos.get(tableScanNode.getId()),
                valuePrinter,
                types,
                anonymizer);
    }

    public static String jsonDistributedPlan(SubPlan plan, Metadata metadata, FunctionManager functionManager, Session session)
    {
        TableInfoSupplier tableInfoSupplier = new TableInfoSupplier(metadata, session);
        ValuePrinter valuePrinter = new ValuePrinter(metadata, functionManager, session);
        TypeProvider typeProvider = getTypeProvider(plan.getAllFragments());
        return jsonDistributedPlan(
                plan.getAllFragments(),
                tableInfoSupplier,
                valuePrinter,
                typeProvider,
                new NoOpAnonymizer());
    }

    private static String jsonDistributedPlan(
            List<PlanFragment> fragments,
            Function<TableScanNode, TableInfo> tableInfoSupplier,
            ValuePrinter valuePrinter,
            TypeProvider typeProvider,
            Anonymizer anonymizer)
    {
        Map<PlanFragmentId, JsonRenderedNode> anonymizedPlan = fragments.stream()
                .collect(toImmutableMap(
                        PlanFragment::getId,
                        planFragment -> new PlanPrinter(
                                planFragment.getRoot(),
                                typeProvider,
                                tableInfoSupplier,
                                ImmutableMap.of(),
                                valuePrinter,
                                planFragment.getStatsAndCosts(),
                                Optional.empty(),
                                anonymizer)
                                .toJsonRenderedNode()));
        return DISTRIBUTED_PLAN_CODEC.toJson(anonymizedPlan);
    }

    public static String textLogicalPlan(
            PlanNode plan,
            TypeProvider types,
            Metadata metadata,
            FunctionManager functionManager,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            int level,
            boolean verbose)
    {
        return textLogicalPlan(plan, types, metadata, functionManager, estimatedStatsAndCosts, session, level, verbose, Optional.empty());
    }

    public static String textLogicalPlan(
            PlanNode plan,
            TypeProvider types,
            Metadata metadata,
            FunctionManager functionManager,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            int level,
            boolean verbose,
            Optional<NodeVersion> version)
    {
        TableInfoSupplier tableInfoSupplier = new TableInfoSupplier(metadata, session);
        ValuePrinter valuePrinter = new ValuePrinter(metadata, functionManager, session);
        StringBuilder builder = new StringBuilder();
        version.ifPresent(v -> builder.append(format("Trino version: %s\n", v)));
        builder.append(new PlanPrinter(
                plan,
                types,
                tableInfoSupplier,
                ImmutableMap.of(),
                valuePrinter,
                estimatedStatsAndCosts,
                Optional.empty(),
                new NoOpAnonymizer())
                .toText(verbose, level));
        return builder.toString();
    }

    public static String textDistributedPlan(
            StageInfo outputStageInfo,
            QueryStats queryStats,
            Metadata metadata,
            FunctionManager functionManager,
            Session session,
            boolean verbose,
            NodeVersion version)
    {
        return textDistributedPlan(
                outputStageInfo,
                queryStats,
                new ValuePrinter(metadata, functionManager, session),
                verbose,
                new NoOpAnonymizer(),
                version);
    }

    public static String textDistributedPlan(
            StageInfo outputStageInfo,
            QueryStats queryStats,
            ValuePrinter valuePrinter,
            boolean verbose,
            Anonymizer anonymizer,
            NodeVersion version)
    {
        List<StageInfo> allStages = getAllStages(Optional.of(outputStageInfo));
        Map<PlanNodeId, TableInfo> tableInfos = allStages.stream()
                .map(StageInfo::getTables)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        StringBuilder builder = new StringBuilder();
        List<PlanFragment> allFragments = allStages.stream()
                .map(StageInfo::getPlan)
                .collect(toImmutableList());
        Map<PlanNodeId, PlanNodeStats> aggregatedStats = aggregateStageStats(allStages);

        Map<DynamicFilterId, DynamicFilterDomainStats> dynamicFilterDomainStats = queryStats.getDynamicFiltersStats()
                .getDynamicFilterDomainStats().stream()
                .collect(toImmutableMap(DynamicFilterDomainStats::getDynamicFilterId, identity()));
        TypeProvider typeProvider = getTypeProvider(allFragments);

        builder.append(format("Trino version: %s\n", version));
        builder.append(format("Queued: %s, Analysis: %s, Planning: %s, Execution: %s\n",
                queryStats.getQueuedTime().convertToMostSuccinctTimeUnit(),
                queryStats.getAnalysisTime().convertToMostSuccinctTimeUnit(),
                queryStats.getPlanningTime().convertToMostSuccinctTimeUnit(),
                queryStats.getExecutionTime().convertToMostSuccinctTimeUnit()));

        for (StageInfo stageInfo : allStages) {
            builder.append(formatFragment(
                    tableScanNode -> tableInfos.get(tableScanNode.getId()),
                    dynamicFilterDomainStats,
                    valuePrinter,
                    stageInfo.getPlan(),
                    Optional.of(stageInfo),
                    Optional.of(aggregatedStats),
                    verbose,
                    typeProvider,
                    anonymizer));
        }

        return builder.toString();
    }

    public static String textDistributedPlan(SubPlan plan, Metadata metadata, FunctionManager functionManager, Session session, boolean verbose, NodeVersion version)
    {
        TableInfoSupplier tableInfoSupplier = new TableInfoSupplier(metadata, session);
        ValuePrinter valuePrinter = new ValuePrinter(metadata, functionManager, session);
        StringBuilder builder = new StringBuilder();
        TypeProvider typeProvider = getTypeProvider(plan.getAllFragments());
        builder.append(format("Trino version: %s\n", version));
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(
                    tableInfoSupplier,
                    ImmutableMap.of(), valuePrinter, fragment, Optional.empty(), Optional.empty(), verbose, typeProvider, new NoOpAnonymizer()));
        }

        return builder.toString();
    }

    private static String formatFragment(
            Function<TableScanNode, TableInfo> tableInfoSupplier,
            Map<DynamicFilterId, DynamicFilterDomainStats> dynamicFilterDomainStats,
            ValuePrinter valuePrinter,
            PlanFragment fragment,
            Optional<StageInfo> stageInfo,
            Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats,
            boolean verbose,
            TypeProvider typeProvider,
            Anonymizer anonymizer)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s [%s]\n",
                fragment.getId(),
                anonymizer.anonymize(fragment.getPartitioning())));

        if (stageInfo.isPresent()) {
            StageStats stageStats = stageInfo.get().getStageStats();

            double avgPositionsPerTask = stageInfo.get().getTasks().stream().mapToLong(task -> task.getStats().getProcessedInputPositions()).average().orElse(Double.NaN);
            double squaredDifferences = stageInfo.get().getTasks().stream().mapToDouble(task -> Math.pow(task.getStats().getProcessedInputPositions() - avgPositionsPerTask, 2)).sum();
            double sdAmongTasks = Math.sqrt(squaredDifferences / stageInfo.get().getTasks().size());

            builder.append(indentString(1))
                    .append(format("CPU: %s, Scheduled: %s, Blocked %s (Input: %s, Output: %s), Input: %s (%s); per task: avg.: %s std.dev.: %s, Output: %s (%s)\n",
                            stageStats.getTotalCpuTime().convertToMostSuccinctTimeUnit(),
                            stageStats.getTotalScheduledTime().convertToMostSuccinctTimeUnit(),
                            stageStats.getTotalBlockedTime().convertToMostSuccinctTimeUnit(),
                            stageStats.getInputBlockedTime().convertToMostSuccinctTimeUnit(),
                            stageStats.getOutputBlockedTime().convertToMostSuccinctTimeUnit(),
                            formatPositions(stageStats.getProcessedInputPositions()),
                            stageStats.getProcessedInputDataSize(),
                            formatDouble(avgPositionsPerTask),
                            formatDouble(sdAmongTasks),
                            formatPositions(stageStats.getOutputPositions()),
                            stageStats.getOutputDataSize()));
            Optional<TDigestHistogram> outputBufferUtilization = stageInfo.get().getStageStats().getOutputBufferUtilization();
            if (verbose && outputBufferUtilization.isPresent()) {
                builder.append(indentString(1))
                        .append(format("Output buffer active time: %s, buffer utilization distribution (%%): {p01=%s, p05=%s, p10=%s, p25=%s, p50=%s, p75=%s, p90=%s, p95=%s, p99=%s, max=%s}\n",
                                succinctNanos(outputBufferUtilization.get().getTotal()),
                                // scale ratio to percentages
                                formatDouble(outputBufferUtilization.get().getP01() * 100),
                                formatDouble(outputBufferUtilization.get().getP05() * 100),
                                formatDouble(outputBufferUtilization.get().getP10() * 100),
                                formatDouble(outputBufferUtilization.get().getP25() * 100),
                                formatDouble(outputBufferUtilization.get().getP50() * 100),
                                formatDouble(outputBufferUtilization.get().getP75() * 100),
                                formatDouble(outputBufferUtilization.get().getP90() * 100),
                                formatDouble(outputBufferUtilization.get().getP95() * 100),
                                formatDouble(outputBufferUtilization.get().getP99() * 100),
                                formatDouble(outputBufferUtilization.get().getMax() * 100)));
            }

            TDigest taskOutputDistribution = new TDigest();
            stageInfo.get().getTasks().forEach(task -> taskOutputDistribution.add(task.getStats().getOutputDataSize().toBytes()));
            TDigest taskInputDistribution = new TDigest();
            stageInfo.get().getTasks().forEach(task -> taskInputDistribution.add(task.getStats().getProcessedInputDataSize().toBytes()));

            if (verbose) {
                builder.append(indentString(1))
                        .append(format("Task output distribution: %s\n", formatSizeDistribution(taskOutputDistribution)));
                builder.append(indentString(1))
                        .append(format("Task input distribution: %s\n", formatSizeDistribution(taskInputDistribution)));
            }

            if (taskInputDistribution.valueAt(0.99) > taskInputDistribution.valueAt(0.49) * 2) {
                builder.append(indentString(1))
                        .append("Amount of input data processed by the workers for this stage might be skewed\n");
            }
        }

        PartitioningScheme partitioningScheme = fragment.getOutputPartitioningScheme();
        List<String> layout = partitioningScheme.getOutputLayout().stream()
                .map(anonymizer::anonymize)
                .collect(toImmutableList());
        builder.append(indentString(1))
                .append(format("Output layout: [%s]\n",
                        Joiner.on(", ").join(layout)));

        boolean replicateNullsAndAny = partitioningScheme.isReplicateNullsAndAny();
        List<String> arguments = partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        NullableValue constant = argument.getConstant();
                        String printableValue = valuePrinter.castToVarchar(constant.getType(), constant.getValue());
                        return constant.getType().getDisplayName() + "(" + anonymizer.anonymize(constant.getType(), printableValue) + ")";
                    }
                    return anonymizer.anonymize(argument.getColumn());
                })
                .collect(toImmutableList());
        builder.append(indentString(1));
        String hashColumn = partitioningScheme.getHashColumn().map(anonymizer::anonymize).map(column -> "[" + column + "]").orElse("");
        if (replicateNullsAndAny) {
            builder.append(format("Output partitioning: %s (replicate nulls and any) [%s]%s\n",
                    anonymizer.anonymize(partitioningScheme.getPartitioning().getHandle()),
                    Joiner.on(", ").join(arguments),
                    hashColumn));
        }
        else {
            builder.append(format("Output partitioning: %s [%s]%s\n",
                    anonymizer.anonymize(partitioningScheme.getPartitioning().getHandle()),
                    Joiner.on(", ").join(arguments),
                    hashColumn));
        }

        fragment.getPartitionCount().ifPresent(partitionCount -> builder.append(format("%sPartition count: %s\n", indentString(1), partitionCount)));

        builder.append(
                        new PlanPrinter(
                                fragment.getRoot(),
                                typeProvider,
                                tableInfoSupplier,
                                dynamicFilterDomainStats,
                                valuePrinter,
                                fragment.getStatsAndCosts(),
                                planNodeStats,
                                anonymizer).toText(verbose, 1))
                .append("\n");

        return builder.toString();
    }

    private static String formatSizeDistribution(TDigest digest)
    {
        return format("{count=%s, p01=%s, p05=%s, p10=%s, p25=%s, p50=%s, p75=%s, p90=%s, p95=%s, p99=%s, max=%s}",
                formatDouble(digest.getCount()),
                succinctBytes((long) digest.valueAt(0.01)),
                succinctBytes((long) digest.valueAt(0.05)),
                succinctBytes((long) digest.valueAt(0.10)),
                succinctBytes((long) digest.valueAt(0.25)),
                succinctBytes((long) digest.valueAt(0.50)),
                succinctBytes((long) digest.valueAt(0.75)),
                succinctBytes((long) digest.valueAt(0.90)),
                succinctBytes((long) digest.valueAt(0.95)),
                succinctBytes((long) digest.valueAt(0.99)),
                succinctBytes((long) digest.getMax()));
    }

    private static TypeProvider getTypeProvider(List<PlanFragment> fragments)
    {
        return TypeProvider.copyOf(fragments.stream()
                .flatMap(f -> f.getSymbols().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public static String graphvizLogicalPlan(PlanNode plan, TypeProvider types)
    {
        // TODO: This should move to something like GraphvizRenderer
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId("graphviz_plan"),
                plan,
                types.allTypes(),
                SINGLE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(plan.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputSymbols()),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                Optional.empty());
        return GraphvizPrinter.printLogical(ImmutableList.of(fragment));
    }

    public static String graphvizDistributedPlan(SubPlan plan)
    {
        return GraphvizPrinter.printDistributed(plan);
    }

    private class Visitor
            extends PlanVisitor<Void, Context>
    {
        private final TypeProvider types;
        private final StatsAndCosts estimatedStatsAndCosts;
        private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;

        public Visitor(TypeProvider types, StatsAndCosts estimatedStatsAndCosts, Optional<Map<PlanNodeId, PlanNodeStats>> stats)
        {
            this.types = requireNonNull(types, "types is null");
            this.estimatedStatsAndCosts = requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Context context)
        {
            addNode(node, "ExplainAnalyze", context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitJoin(JoinNode node, Context context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(unresolveFunctions(clause.toExpression()));
            }
            node.getFilter()
                    .map(PlanPrinter::unresolveFunctions)
                    .ifPresent(joinExpressions::add);

            NodeRepresentation nodeOutput;
            if (node.isCrossJoin()) {
                checkState(joinExpressions.isEmpty());
                nodeOutput = addNode(node, "CrossJoin", context.tag());
            }
            else {
                ImmutableMap.Builder<String, String> descriptor = ImmutableMap.<String, String>builder()
                        .put("criteria", Joiner.on(" AND ").join(anonymizeExpressions(joinExpressions)))
                        .put("hash", formatHash(node.getLeftHashSymbol(), node.getRightHashSymbol()));
                node.getDistributionType().ifPresent(distribution -> descriptor.put("distribution", distribution.name()));
                nodeOutput = addNode(node, node.getType().getJoinLabel(), descriptor.buildOrThrow(), node.getReorderJoinStatsAndCost(), context.tag());
            }

            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetails("Distribution: %s", distributionType));
            if (node.isMaySkipOutputDuplicates()) {
                nodeOutput.appendDetails("maySkipOutputDuplicates = %s", node.isMaySkipOutputDuplicates());
            }
            if (!node.getDynamicFilters().isEmpty()) {
                nodeOutput.appendDetails("dynamicFilterAssignments = %s", printDynamicFilterAssignments(node.getDynamicFilters()));
            }
            node.getLeft().accept(this, new Context());
            node.getRight().accept(this, new Context());

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    node.getType().getJoinLabel(),
                    ImmutableMap.of("filter", formatFilter(node.getFilter())),
                    context.tag());

            nodeOutput.appendDetails("Distribution: %s", node.getDistributionType());
            node.getLeft().accept(this, new Context());
            node.getRight().accept(this, new Context());

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    "SemiJoin",
                    ImmutableMap.of(
                            "criteria", anonymizer.anonymize(node.getSourceJoinSymbol()) + " = " + anonymizer.anonymize(node.getFilteringSourceJoinSymbol()),
                            "hash", formatHash(node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol())),
                    context.tag());
            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetails("Distribution: %s", distributionType));
            node.getDynamicFilterId().ifPresent(dynamicFilterId -> nodeOutput.appendDetails("dynamicFilterId: %s", dynamicFilterId));
            node.getSource().accept(this, new Context());
            node.getFilteringSource().accept(this, new Context());

            return null;
        }

        @Override
        public Void visitDynamicFilterSource(DynamicFilterSourceNode node, Context context)
        {
            addNode(
                    node,
                    "DynamicFilterSource",
                    ImmutableMap.of("dynamicFilterAssignments", printDynamicFilterAssignments(node.getDynamicFilters())),
                    context.tag());
            node.getSource().accept(this, new Context());
            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    "IndexSource",
                    ImmutableMap.of(
                            "indexedTable", anonymizer.anonymize(node.getIndexHandle()),
                            "lookup", formatSymbols(node.getLookupSymbols())),
                    context.tag());

            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputSymbols().contains(entry.getKey())) {
                    nodeOutput.appendDetails("%s := %s", anonymizer.anonymize(entry.getKey()), anonymizer.anonymize(entry.getValue()));
                }
            }
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Context context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        clause.getProbe().toSymbolReference(),
                        clause.getIndex().toSymbolReference()));
            }

            addNode(node,
                    format("%sIndexJoin", node.getType().getJoinLabel()),
                    ImmutableMap.of(
                            "criteria", Joiner.on(" AND ").join(anonymizeExpressions(joinExpressions)),
                            "hash", formatHash(node.getProbeHashSymbol(), node.getIndexHashSymbol())),
                    context.tag());
            node.getProbeSource().accept(this, new Context());
            node.getIndexSource().accept(this, new Context());

            return null;
        }

        @Override
        public Void visitOffset(OffsetNode node, Context context)
        {
            addNode(node,
                    "Offset",
                    ImmutableMap.of("count", String.valueOf(node.getCount())),
                    context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitLimit(LimitNode node, Context context)
        {
            addNode(node,
                    format("Limit%s", node.isPartial() ? "Partial" : ""),
                    ImmutableMap.of(
                            "count", String.valueOf(node.getCount()),
                            "withTies", formatBoolean(node.isWithTies()),
                            "inputPreSortedBy", formatSymbols(node.getPreSortedInputs())),
                    context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Context context)
        {
            addNode(node,
                    format("DistinctLimit%s", node.isPartial() ? "Partial" : ""),
                    ImmutableMap.of(
                            "limit", String.valueOf(node.getLimit()),
                            "hash", formatHash(node.getHashSymbol())),
                    context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitAggregation(AggregationNode node, Context context)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = node.getStep().name();
            }
            if (node.isStreamable()) {
                type = format("%s (STREAMING)", type);
            }
            String keys = "";
            if (!node.getGroupingKeys().isEmpty()) {
                keys = formatSymbols(node.getGroupingKeys());
            }

            NodeRepresentation nodeOutput = addNode(
                    node,
                    "Aggregate",
                    ImmutableMap.of("type", type, "keys", keys, "hash", formatHash(node.getHashSymbol())),
                    context.tag());

            node.getAggregations().forEach((symbol, aggregation) ->
                    nodeOutput.appendDetails("%s := %s", anonymizer.anonymize(symbol), formatAggregation(anonymizer, aggregation)));

            return processChildren(node, new Context());
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Context context)
        {
            // grouping sets are easier to understand in terms of inputs
            List<String> anonymizedInputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> set.stream()
                            .map(symbol -> node.getGroupingColumns().get(symbol))
                            .collect(toImmutableList()))
                    .map(this::formatSymbols)
                    .collect(toImmutableList());

            NodeRepresentation nodeOutput = addNode(
                    node,
                    "GroupId",
                    ImmutableMap.of("symbols", formatCollection(anonymizedInputGroupingSetSymbols, Objects::toString)),
                    context.tag());

            for (Map.Entry<Symbol, Symbol> mapping : node.getGroupingColumns().entrySet()) {
                nodeOutput.appendDetails("%s := %s", anonymizer.anonymize(mapping.getKey()), anonymizer.anonymize(mapping.getValue()));
            }

            return processChildren(node, new Context());
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Context context)
        {
            addNode(node,
                    "MarkDistinct",
                    ImmutableMap.of(
                            "distinct", formatOutputs(types, node.getDistinctSymbols()),
                            "marker", anonymizer.anonymize(node.getMarkerSymbol()),
                            "hash", formatHash(node.getHashSymbol())),
                    context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitWindow(WindowNode node, Context context)
        {
            ImmutableMap.Builder<String, String> descriptor = ImmutableMap.builder();
            if (!node.getPartitionBy().isEmpty()) {
                List<Symbol> prePartitioned = node.getPartitionBy().stream()
                        .filter(node.getPrePartitionedInputs()::contains)
                        .collect(toImmutableList());

                List<Symbol> notPrePartitioned = node.getPartitionBy().stream()
                        .filter(column -> !node.getPrePartitionedInputs().contains(column))
                        .collect(toImmutableList());

                StringBuilder builder = new StringBuilder();
                if (!prePartitioned.isEmpty()) {
                    builder.append("<")
                            .append(Joiner.on(", ").join(anonymize(prePartitioned)))
                            .append(">");
                    if (!notPrePartitioned.isEmpty()) {
                        builder.append(", ");
                    }
                }
                if (!notPrePartitioned.isEmpty()) {
                    builder.append(Joiner.on(", ").join(anonymize(notPrePartitioned)));
                }
                descriptor.put("partitionBy", format("[%s]", builder));
            }
            if (node.getOrderingScheme().isPresent()) {
                descriptor.put("orderBy", formatOrderingScheme(node.getOrderingScheme().get(), node.getPreSortedOrderPrefix()));
            }

            NodeRepresentation nodeOutput = addNode(
                    node,
                    "Window",
                    descriptor.put("hash", formatHash(node.getHashSymbol())).buildOrThrow(),
                    context.tag());

            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                WindowNode.Function function = entry.getValue();
                String frameInfo = formatFrame(function.getFrame());

                nodeOutput.appendDetails(
                        "%s := %s(%s) %s",
                        anonymizer.anonymize(entry.getKey()),
                        function.getResolvedFunction().getSignature().getName(),
                        Joiner.on(", ").join(anonymizeExpressions(function.getArguments())),
                        frameInfo);
            }
            return processChildren(node, new Context());
        }

        @Override
        public Void visitPatternRecognition(PatternRecognitionNode node, Context context)
        {
            ImmutableMap.Builder<String, String> descriptor = ImmutableMap.builder();
            if (!node.getPartitionBy().isEmpty()) {
                List<Symbol> prePartitioned = node.getPartitionBy().stream()
                        .filter(node.getPrePartitionedInputs()::contains)
                        .collect(toImmutableList());

                List<Symbol> notPrePartitioned = node.getPartitionBy().stream()
                        .filter(column -> !node.getPrePartitionedInputs().contains(column))
                        .collect(toImmutableList());

                StringBuilder builder = new StringBuilder();
                if (!prePartitioned.isEmpty()) {
                    builder.append("<")
                            .append(Joiner.on(", ").join(anonymize(prePartitioned)))
                            .append(">");
                    if (!notPrePartitioned.isEmpty()) {
                        builder.append(", ");
                    }
                }
                if (!notPrePartitioned.isEmpty()) {
                    builder.append(Joiner.on(", ").join(anonymize(notPrePartitioned)));
                }
                descriptor.put("partitionBy", format("[%s]", builder));
            }
            if (node.getOrderingScheme().isPresent()) {
                descriptor.put("orderBy", formatOrderingScheme(node.getOrderingScheme().get(), node.getPreSortedOrderPrefix()));
            }

            NodeRepresentation nodeOutput = addNode(
                    node,
                    "PatterRecognition",
                    descriptor.put("hash", formatHash(node.getHashSymbol())).buildOrThrow(),
                    context.tag());

            if (node.getCommonBaseFrame().isPresent()) {
                nodeOutput.appendDetails("base frame: %s", formatFrame(node.getCommonBaseFrame().get()));
            }
            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                WindowNode.Function function = entry.getValue();
                nodeOutput.appendDetails(
                        "%s := %s(%s)",
                        anonymizer.anonymize(entry.getKey()),
                        function.getResolvedFunction().getSignature().getName(),
                        Joiner.on(", ").join(anonymizeExpressions(function.getArguments())));
            }

            for (Map.Entry<Symbol, Measure> entry : node.getMeasures().entrySet()) {
                nodeOutput.appendDetails(
                        "%s := %s",
                        anonymizer.anonymize(entry.getKey()),
                        anonymizer.anonymize(unresolveFunctions(entry.getValue().getExpressionAndValuePointers().getExpression())));
                appendValuePointers(nodeOutput, entry.getValue().getExpressionAndValuePointers());
            }
            if (node.getRowsPerMatch() != WINDOW) {
                nodeOutput.appendDetails("%s", formatRowsPerMatch(node.getRowsPerMatch()));
            }
            nodeOutput.appendDetails("%s", formatSkipTo(node.getSkipToPosition(), node.getSkipToLabel()));
            nodeOutput.appendDetails("pattern[%s] (%s)", node.getPattern(), node.isInitial() ? "INITIAL" : "SEEK");
            nodeOutput.appendDetails("subsets[%s]", node.getSubsets().entrySet().stream()
                    .map(subset -> subset.getKey().getName() +
                            " := " +
                            subset.getValue().stream()
                                    .map(IrLabel::getName)
                                    .collect(Collectors.joining(", ", "{", "}")))
                    .collect(joining(", ")));
            for (Map.Entry<IrLabel, ExpressionAndValuePointers> entry : node.getVariableDefinitions().entrySet()) {
                nodeOutput.appendDetails("%s := %s", entry.getKey().getName(), anonymizer.anonymize(unresolveFunctions(entry.getValue().getExpression())));
                appendValuePointers(nodeOutput, entry.getValue());
            }

            return processChildren(node, new Context());
        }

        private void appendValuePointers(NodeRepresentation nodeOutput, ExpressionAndValuePointers expressionAndPointers)
        {
            for (int i = 0; i < expressionAndPointers.getLayout().size(); i++) {
                Symbol symbol = expressionAndPointers.getLayout().get(i);
                if (expressionAndPointers.getMatchNumberSymbols().contains(symbol)) {
                    // match_number does not use the value pointer. It is constant per match.
                    continue;
                }
                ValuePointer pointer = expressionAndPointers.getValuePointers().get(i);

                if (pointer instanceof ScalarValuePointer scalarPointer) {
                    String sourceSymbolName = expressionAndPointers.getClassifierSymbols().contains(symbol)
                            ? "classifier"
                            : anonymizer.anonymize(scalarPointer.getInputSymbol());
                    nodeOutput.appendDetails("%s%s := %s[%s]", indentString(1), anonymizer.anonymize(symbol), sourceSymbolName, formatLogicalIndexPointer(scalarPointer.getLogicalIndexPointer()));
                }
                else if (pointer instanceof AggregationValuePointer aggregationPointer) {
                    String processingMode = aggregationPointer.getSetDescriptor().isRunning() ? "RUNNING " : "FINAL ";
                    String name = aggregationPointer.getFunction().getSignature().getName();
                    String arguments = Joiner.on(", ").join(anonymizeExpressions(aggregationPointer.getArguments()));
                    String labels = aggregationPointer.getSetDescriptor().getLabels().stream()
                            .map(IrLabel::getName)
                            .collect(joining(", ", "{", "}"));
                    nodeOutput.appendDetails("%s%s := %s%s(%s)%s", indentString(1), anonymizer.anonymize(symbol), processingMode, name, arguments, labels);
                }
                else {
                    throw new UnsupportedOperationException("unexpected ValuePointer type: " + pointer.getClass().getSimpleName());
                }
            }
        }

        private String formatFrame(WindowNode.Frame frame)
        {
            StringBuilder builder = new StringBuilder(frame.getType().toString());

            frame.getStartValue()
                    .map(anonymizer::anonymize)
                    .ifPresent(value -> builder.append(" ").append(value));
            builder.append(" ").append(frame.getStartType());

            frame.getEndValue()
                    .map(anonymizer::anonymize)
                    .ifPresent(value -> builder.append(" ").append(value));
            builder.append(" ").append(frame.getEndType());

            return builder.toString();
        }

        private String formatLogicalIndexPointer(LogicalIndexPointer pointer)
        {
            StringBuilder builder = new StringBuilder();
            int physicalOffset = pointer.getPhysicalOffset();
            if (physicalOffset > 0) {
                builder.append("NEXT(");
            }
            else if (physicalOffset < 0) {
                builder.append("PREV(");
            }
            builder.append(pointer.isRunning() ? "RUNNING " : "FINAL ");
            builder.append(pointer.isLast() ? "LAST(" : "FIRST(");
            builder.append(pointer.getLabels().stream()
                    .map(IrLabel::getName)
                    .collect(joining(", ", "{", "}")));
            if (pointer.getLogicalOffset() > 0) {
                builder
                        .append(", ")
                        .append(pointer.getLogicalOffset());
            }
            builder.append(")");
            if (physicalOffset != 0) {
                builder
                        .append(", ")
                        .append(abs(physicalOffset))
                        .append(")");
            }
            return builder.toString();
        }

        private String formatRowsPerMatch(RowsPerMatch rowsPerMatch)
        {
            return switch (rowsPerMatch) {
                case ONE -> "ONE ROW PER MATCH";
                case ALL_SHOW_EMPTY -> "ALL ROWS PER MATCH SHOW EMPTY MATCHES";
                case ALL_OMIT_EMPTY -> "ALL ROWS PER MATCH OMIT EMPTY MATCHES";
                case ALL_WITH_UNMATCHED -> "ALL ROWS PER MATCH WITH UNMATCHED ROWS";
                default -> throw new IllegalArgumentException("unexpected rowsPer match value: " + rowsPerMatch.name());
            };
        }

        private String formatSkipTo(Position position, Optional<IrLabel> label)
        {
            return switch (position) {
                case PAST_LAST -> "AFTER MATCH SKIP PAST LAST ROW";
                case NEXT -> "AFTER MATCH SKIP TO NEXT ROW";
                case FIRST -> "AFTER MATCH SKIP TO FIRST " + label.get().getName();
                case LAST -> "AFTER MATCH SKIP TO LAST " + label.get().getName();
            };
        }

        @Override
        public Void visitTopNRanking(TopNRankingNode node, Context context)
        {
            ImmutableMap.Builder<String, String> descriptor = ImmutableMap.builder();
            descriptor.put("partitionBy", formatSymbols(node.getPartitionBy()));
            descriptor.put("orderBy", formatOrderingScheme(node.getOrderingScheme()));

            NodeRepresentation nodeOutput = addNode(
                    node,
                    "TopNRanking",
                    descriptor
                            .put("limit", String.valueOf(node.getMaxRankingPerPartition()))
                            .put("hash", formatHash(node.getHashSymbol()))
                            .buildOrThrow(),
                    context.tag());

            nodeOutput.appendDetails("%s := %s", anonymizer.anonymize(node.getRankingSymbol()), node.getRankingType());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Context context)
        {
            ImmutableMap.Builder<String, String> descriptor = ImmutableMap.builder();
            if (!node.getPartitionBy().isEmpty()) {
                descriptor.put("partitionBy", formatSymbols(node.getPartitionBy()));
            }

            if (node.getMaxRowCountPerPartition().isPresent()) {
                descriptor.put("limit", String.valueOf(node.getMaxRowCountPerPartition().get()));
            }

            NodeRepresentation nodeOutput = addNode(
                    node,
                    "RowNumber",
                    descriptor.put("hash", formatHash(node.getHashSymbol())).buildOrThrow(),
                    context.tag());
            nodeOutput.appendDetails("%s := %s", anonymizer.anonymize(node.getRowNumberSymbol()), "row_number()");

            return processChildren(node, new Context());
        }

        @Override
        public Void visitTableScan(TableScanNode node, Context context)
        {
            TableHandle table = node.getTable();
            TableInfo tableInfo = tableInfoSupplier.apply(node);
            NodeRepresentation nodeOutput;
            nodeOutput = addNode(node, "TableScan", ImmutableMap.of("table", anonymizer.anonymize(table, tableInfo)), context.tag());
            printTableScanInfo(nodeOutput, node, tableInfo);
            PlanNodeStats nodeStats = stats.map(s -> s.get(node.getId())).orElse(null);
            if (nodeStats != null) {
                StringBuilder inputDetailBuilder = new StringBuilder();
                ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
                buildFormatString(
                        inputDetailBuilder,
                        argsBuilder,
                        "Input: %s (%s)",
                        formatPositions(nodeStats.getPlanNodeInputPositions()),
                        nodeStats.getPlanNodeInputDataSize().toString());
                addPhysicalInputStats(nodeStats, inputDetailBuilder, argsBuilder);
                appendDetailsFromBuilder(nodeOutput, inputDetailBuilder, argsBuilder);
            }
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Values", context.tag());
            if (node.getRows().isEmpty()) {
                for (int i = 0; i < node.getRowCount(); i++) {
                    nodeOutput.appendDetails("()");
                }
                return null;
            }
            List<String> rows = node.getRows().get().stream()
                    .map(row -> {
                        if (row instanceof Row) {
                            return ((Row) row).getItems().stream()
                                    .map(PlanPrinter::unresolveFunctions)
                                    .map(anonymizer::anonymize)
                                    .collect(joining(", ", "(", ")"));
                        }
                        return anonymizer.anonymize(unresolveFunctions(row));
                    })
                    .collect(toImmutableList());
            for (String row : rows) {
                nodeOutput.appendDetails("%s", row);
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Context context)
        {
            return visitScanFilterAndProjectInfo(node, Optional.of(node), Optional.empty(), context);
        }

        @Override
        public Void visitProject(ProjectNode node, Context context)
        {
            if (node.getSource() instanceof FilterNode) {
                return visitScanFilterAndProjectInfo(node, Optional.of((FilterNode) node.getSource()), Optional.of(node), context);
            }

            return visitScanFilterAndProjectInfo(node, Optional.empty(), Optional.of(node), context);
        }

        private Void visitScanFilterAndProjectInfo(
                PlanNode node,
                Optional<FilterNode> filterNode,
                Optional<ProjectNode> projectNode,
                Context context)
        {
            checkState(projectNode.isPresent() || filterNode.isPresent());

            PlanNode sourceNode;
            if (filterNode.isPresent()) {
                sourceNode = filterNode.get().getSource();
            }
            else {
                sourceNode = projectNode.get().getSource();
            }

            Optional<TableScanNode> scanNode;
            if (sourceNode instanceof TableScanNode) {
                scanNode = Optional.of((TableScanNode) sourceNode);
            }
            else {
                scanNode = Optional.empty();
            }

            String operatorName = "";
            ImmutableMap.Builder<String, String> descriptor = ImmutableMap.builder();

            if (scanNode.isPresent()) {
                operatorName += "Scan";
                descriptor.put("table", anonymizer.anonymize(scanNode.get().getTable(), tableInfoSupplier.apply(scanNode.get())));
            }

            List<DynamicFilters.Descriptor> dynamicFilters = ImmutableList.of();
            if (filterNode.isPresent()) {
                operatorName += "Filter";
                Expression predicate = filterNode.get().getPredicate();
                DynamicFilters.ExtractResult extractResult = extractDynamicFilters(predicate);
                descriptor.put("filterPredicate", formatFilter(unresolveFunctions(combineConjunctsWithDuplicates(extractResult.getStaticConjuncts()))));
                if (!extractResult.getDynamicConjuncts().isEmpty()) {
                    dynamicFilters = extractResult.getDynamicConjuncts();
                    descriptor.put("dynamicFilters", printDynamicFilters(dynamicFilters));
                }
            }

            if (projectNode.isPresent()) {
                operatorName += "Project";
            }

            List<PlanNodeId> allNodes = Stream.of(scanNode, filterNode, projectNode)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(PlanNode::getId)
                    .collect(toList());

            NodeRepresentation nodeOutput = addNode(
                    node,
                    operatorName,
                    descriptor.buildOrThrow(),
                    allNodes,
                    ImmutableList.of(sourceNode),
                    ImmutableList.of(),
                    Optional.empty(),
                    context.tag());

            projectNode.ifPresent(value -> printAssignments(nodeOutput, value.getAssignments()));

            if (scanNode.isPresent()) {
                printTableScanInfo(nodeOutput, scanNode.get(), tableInfoSupplier.apply(scanNode.get()));
                PlanNodeStats nodeStats = stats.map(s -> s.get(node.getId())).orElse(null);
                if (nodeStats != null) {
                    // Add to 'details' rather than 'statistics', since these stats are node-specific
                    double filtered = 100.0d * (nodeStats.getPlanNodeInputPositions() - nodeStats.getPlanNodeOutputPositions()) / nodeStats.getPlanNodeInputPositions();
                    StringBuilder inputDetailBuilder = new StringBuilder();
                    ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
                    buildFormatString(
                            inputDetailBuilder,
                            argsBuilder,
                            "Input: %s (%s), Filtered: %s%%",
                            formatPositions(nodeStats.getPlanNodeInputPositions()),
                            nodeStats.getPlanNodeInputDataSize().toString(),
                            formatDouble(filtered));
                    addPhysicalInputStats(nodeStats, inputDetailBuilder, argsBuilder);
                    appendDetailsFromBuilder(nodeOutput, inputDetailBuilder, argsBuilder);
                }
                List<DynamicFilterDomainStats> collectedDomainStats = dynamicFilters.stream()
                        .map(DynamicFilters.Descriptor::getId)
                        .map(dynamicFilterDomainStats::get)
                        .filter(Objects::nonNull)
                        .collect(toImmutableList());
                if (!collectedDomainStats.isEmpty()) {
                    nodeOutput.appendDetails("Dynamic filters: ");
                    if (anonymizer instanceof NoOpAnonymizer) {
                        collectedDomainStats.forEach(stats -> nodeOutput.appendDetails(
                                "    - %s, %s, collection time=%s",
                                stats.getDynamicFilterId(),
                                stats.getSimplifiedDomain(),
                                stats.getCollectionDuration().map(Duration::toString).orElse("uncollected")));
                    }
                    else {
                        collectedDomainStats.forEach(stats -> nodeOutput.appendDetails(
                                "    - %s, collection time=%s",
                                stats.getDynamicFilterId(),
                                stats.getCollectionDuration().map(Duration::toString).orElse("uncollected")));
                    }
                }
                return null;
            }

            sourceNode.accept(this, new Context());
            return null;
        }

        private static void addPhysicalInputStats(PlanNodeStats nodeStats, StringBuilder inputDetailBuilder, ImmutableList.Builder<String> argsBuilder)
        {
            if (nodeStats.getPlanNodePhysicalInputDataSize().toBytes() > 0) {
                buildFormatString(inputDetailBuilder, argsBuilder, ", Physical input: %s", nodeStats.getPlanNodePhysicalInputDataSize().toString());
                buildFormatString(inputDetailBuilder, argsBuilder, ", Physical input time: %s", nodeStats.getPlanNodePhysicalInputReadTime().toString());
            }
            // Some connectors may report physical input time but not physical input data size
            else if (nodeStats.getPlanNodePhysicalInputReadTime().getValue() > 0) {
                buildFormatString(inputDetailBuilder, argsBuilder, ", Physical input time: %s", nodeStats.getPlanNodePhysicalInputReadTime().toString());
            }
        }

        @FormatMethod
        private static void buildFormatString(StringBuilder formatBuilder, ImmutableList.Builder<String> argsBuilder, String formatFragment, String... fragmentArgs)
        {
            formatBuilder.append(formatFragment);
            argsBuilder.add(fragmentArgs);
        }

        @SuppressWarnings("FormatStringAnnotation") // verified by building the format and args using #buildFormatString
        private void appendDetailsFromBuilder(NodeRepresentation nodeOutput, StringBuilder inputDetailBuilder, ImmutableList.Builder<String> argsBuilder)
        {
            nodeOutput.appendDetails(inputDetailBuilder.toString(), argsBuilder.build().toArray());
        }

        private String printDynamicFilters(Collection<DynamicFilters.Descriptor> filters)
        {
            return filters.stream()
                    .map(filter -> anonymizer.anonymize(filter.getInput()) + " " + filter.getOperator().getValue() + " #" + filter.getId())
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        private String printDynamicFilterAssignments(Map<DynamicFilterId, Symbol> filters)
        {
            return filters.entrySet().stream()
                    .map(filter -> anonymizer.anonymize(filter.getValue()) + " -> #" + filter.getKey())
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        private void printTableScanInfo(NodeRepresentation nodeOutput, TableScanNode node, TableInfo tableInfo)
        {
            TupleDomain<ColumnHandle> predicate = tableInfo.getPredicate();

            if (predicate.isNone()) {
                nodeOutput.appendDetails(":: NONE");
            }
            else {
                // first, print output columns and their constraints
                for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    nodeOutput.appendDetails("%s := %s", anonymizer.anonymize(assignment.getKey()), anonymizer.anonymize(column));
                    printConstraint(nodeOutput, column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    predicate.getDomains().get()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                nodeOutput.appendDetails("%s", anonymizer.anonymize(column));
                                printConstraint(nodeOutput, column, predicate);
                            });
                }
            }
        }

        @Override
        public Void visitUnnest(UnnestNode node, Context context)
        {
            String name;
            if (node.getFilter().isPresent()) {
                name = node.getJoinType().getJoinLabel() + " Unnest";
            }
            else if (!node.getReplicateSymbols().isEmpty()) {
                if (node.getJoinType() == INNER) {
                    name = "CrossJoin Unnest";
                }
                else {
                    name = node.getJoinType().getJoinLabel() + " Unnest";
                }
            }
            else {
                name = "Unnest";
            }

            List<Symbol> unnestInputs = node.getMappings().stream()
                    .map(UnnestNode.Mapping::getInput)
                    .collect(toImmutableList());

            ImmutableMap.Builder<String, String> descriptor = ImmutableMap.builder();
            if (!node.getReplicateSymbols().isEmpty()) {
                descriptor.put("replicate", formatOutputs(types, node.getReplicateSymbols()));
            }
            descriptor.put("unnest", formatOutputs(types, unnestInputs));
            node.getFilter().ifPresent(filter -> descriptor.put("filter", formatFilter(filter)));
            addNode(node, name, descriptor.buildOrThrow(), context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitOutput(OutputNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(
                    node,
                    "Output",
                    ImmutableMap.of("columnNames", formatCollection(node.getColumnNames(), anonymizer::anonymizeColumn)),
                    context.tag());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getOutputSymbols().get(i);
                if (!name.equals(symbol.toString())) {
                    nodeOutput.appendDetails("%s := %s", anonymizer.anonymizeColumn(name), anonymizer.anonymize(symbol));
                }
            }
            return processChildren(node, new Context());
        }

        @Override
        public Void visitTopN(TopNNode node, Context context)
        {
            addNode(node,
                    format("TopN%s", node.getStep() == TopNNode.Step.PARTIAL ? "Partial" : ""),
                    ImmutableMap.of(
                            "count", String.valueOf(node.getCount()),
                            "orderBy", formatOrderingScheme(node.getOrderingScheme())),
                    context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitSort(SortNode node, Context context)
        {
            addNode(node,
                    format("%sSort", node.isPartial() ? "Partial" : ""),
                    ImmutableMap.of("orderBy", formatOrderingScheme(node.getOrderingScheme())),
                    context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Context context)
        {
            addNode(node,
                    format("Remote%s", node.getOrderingScheme().isPresent() ? "Merge" : "Source"),
                    ImmutableMap.of("sourceFragmentIds", formatCollection(node.getSourceFragmentIds(), Objects::toString)),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    node.getSourceFragmentIds(),
                    Optional.empty(),
                    context.tag());

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Context context)
        {
            addNode(node, "Union", context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitIntersect(IntersectNode node, Context context)
        {
            addNode(node,
                    "Intersect",
                    ImmutableMap.of("isDistinct", formatBoolean(node.isDistinct())),
                    context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitExcept(ExceptNode node, Context context)
        {
            addNode(node,
                    "Except",
                    ImmutableMap.of("isDistinct", formatBoolean(node.isDistinct())),
                    context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitRefreshMaterializedView(RefreshMaterializedViewNode node, Context context)
        {
            addNode(node,
                    "RefreshMaterializedView",
                    ImmutableMap.of("viewName", anonymizer.anonymize(node.getViewName())),
                    context.tag());
            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(node, "TableWriter", context.tag());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                nodeOutput.appendDetails("%s := %s", anonymizer.anonymizeColumn(name), anonymizer.anonymize(symbol));
            }

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(nodeOutput, node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get());
            }

            return processChildren(node, new Context());
        }

        @Override
        public Void visitStatisticsWriterNode(StatisticsWriterNode node, Context context)
        {
            addNode(node,
                    "StatisticsWriter",
                    ImmutableMap.of("target", anonymizer.anonymize(node.getTarget())),
                    context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(
                    node,
                    "TableCommit",
                    ImmutableMap.of("target", anonymizer.anonymize(node.getTarget())),
                    context.tag());

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(nodeOutput, node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get());
            }

            return processChildren(node, new Context());
        }

        private void printStatisticAggregations(NodeRepresentation nodeOutput, StatisticAggregations aggregations, StatisticAggregationsDescriptor<Symbol> descriptor)
        {
            nodeOutput.appendDetails("Collected statistics:");
            printStatisticAggregationsInfo(nodeOutput, descriptor.getTableStatistics(), descriptor.getColumnStatistics(), aggregations.getAggregations());
            nodeOutput.appendDetails("%sgrouped by => [%s]", indentString(1), getStatisticGroupingSetsInfo(descriptor.getGrouping()));
        }

        private String getStatisticGroupingSetsInfo(Map<String, Symbol> columnMappings)
        {
            return columnMappings.entrySet().stream()
                    .map(entry -> format("%s := %s", anonymizer.anonymize(entry.getValue()), anonymizer.anonymizeColumn(entry.getKey())))
                    .collect(joining(", "));
        }

        private void printStatisticAggregationsInfo(
                NodeRepresentation nodeOutput,
                Map<TableStatisticType, Symbol> tableStatistics,
                Map<ColumnStatisticMetadata, Symbol> columnStatistics,
                Map<Symbol, AggregationNode.Aggregation> aggregations)
        {
            nodeOutput.appendDetails("aggregations =>");
            for (Map.Entry<TableStatisticType, Symbol> tableStatistic : tableStatistics.entrySet()) {
                nodeOutput.appendDetails("%s%s => [%s := %s]",
                        indentString(1),
                        anonymizer.anonymize(tableStatistic.getValue()),
                        tableStatistic.getKey(),
                        formatAggregation(anonymizer, aggregations.get(tableStatistic.getValue())));
            }

            for (Map.Entry<ColumnStatisticMetadata, Symbol> columnStatistic : columnStatistics.entrySet()) {
                String aggregationName;
                if (columnStatistic.getKey().getStatisticTypeIfPresent().isPresent()) {
                    aggregationName = columnStatistic.getKey().getStatisticType().name();
                }
                else {
                    FunctionName aggregation = columnStatistic.getKey().getAggregation();
                    if (aggregation.getCatalogSchema().isPresent()) {
                        aggregationName = aggregation.getCatalogSchema().get() + "." + aggregation.getName();
                    }
                    else {
                        aggregationName = aggregation.getName();
                    }
                }
                nodeOutput.appendDetails(
                        "%s%s[%s] => [%s := %s]",
                        indentString(1),
                        aggregationName,
                        anonymizer.anonymizeColumn(columnStatistic.getKey().getColumnName()),
                        anonymizer.anonymize(columnStatistic.getValue()),
                        formatAggregation(anonymizer, aggregations.get(columnStatistic.getValue())));
            }
        }

        @Override
        public Void visitSample(SampleNode node, Context context)
        {
            addNode(node,
                    "Sample",
                    ImmutableMap.of(
                            "type", node.getSampleType().name(),
                            "ratio", String.valueOf(node.getSampleRatio())),
                    context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitExchange(ExchangeNode node, Context context)
        {
            if (node.getOrderingScheme().isPresent()) {
                addNode(node,
                        format("%sMerge", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        ImmutableMap.of("orderBy", formatOrderingScheme(node.getOrderingScheme().get())),
                        context.tag());
            }
            else if (node.getScope() == Scope.LOCAL) {
                addNode(node,
                        "LocalExchange",
                        ImmutableMap.of(
                                "partitioning", anonymizer.anonymize(node.getPartitioningScheme().getPartitioning().getHandle()),
                                "isReplicateNullsAndAny", formatBoolean(node.getPartitioningScheme().isReplicateNullsAndAny()),
                                "hashColumn", formatHash(node.getPartitioningScheme().getHashColumn()),
                                "arguments", formatCollection(node.getPartitioningScheme().getPartitioning().getArguments(), anonymizer::anonymize)),
                        context.tag());
            }
            else {
                addNode(node,
                        format("%sExchange", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        ImmutableMap.of(
                                "partitionCount", node.getPartitioningScheme().getPartitionCount().map(String::valueOf).orElse(""),
                                "type", node.getType().name(),
                                "isReplicateNullsAndAny", formatBoolean(node.getPartitioningScheme().isReplicateNullsAndAny()),
                                "hashColumn", formatHash(node.getPartitioningScheme().getHashColumn())),
                        context.tag());
            }
            return processChildren(node, new Context());
        }

        @Override
        public Void visitTableExecute(TableExecuteNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(node, "TableExecute", context.tag());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                nodeOutput.appendDetails("%s := %s", anonymizer.anonymizeColumn(name), anonymizer.anonymize(symbol));
            }

            return processChildren(node, new Context());
        }

        @Override
        public Void visitSimpleTableExecuteNode(SimpleTableExecuteNode node, Context context)
        {
            addNode(node,
                    "SimpleTableExecute",
                    ImmutableMap.of("table", anonymizer.anonymize(node.getExecuteHandle())),
                    context.tag());
            return null;
        }

        @Override
        public Void visitMergeWriter(MergeWriterNode node, Context context)
        {
            addNode(node,
                    "MergeWriter",
                    ImmutableMap.of("table", anonymizer.anonymize(node.getTarget())),
                    context.tag());
            return processChildren(node, new Context());
        }

        @Override
        public Void visitMergeProcessor(MergeProcessorNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(node, "MergeProcessor", context.tag());
            nodeOutput.appendDetails("target: %s", anonymizer.anonymize(node.getTarget()));
            nodeOutput.appendDetails("merge row column: %s", anonymizer.anonymize(node.getMergeRowSymbol()));
            nodeOutput.appendDetails("row id column: %s", anonymizer.anonymize(node.getRowIdSymbol()));
            nodeOutput.appendDetails("redistribution columns: %s", anonymize(node.getRedistributionColumnSymbols()));
            nodeOutput.appendDetails("data columns: %s", anonymize(node.getDataColumnSymbols()));

            return processChildren(node, new Context());
        }

        @Override
        public Void visitTableDelete(TableDeleteNode node, Context context)
        {
            addNode(node,
                    "TableDelete",
                    ImmutableMap.of("target", anonymizer.anonymize(node.getTarget())),
                    context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Context context)
        {
            addNode(node, "EnforceSingleRow", context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Context context)
        {
            addNode(node, "AssignUniqueId", context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitGroupReference(GroupReference node, Context context)
        {
            addNode(node,
                    "GroupReference",
                    ImmutableMap.of("groupId", String.valueOf(node.getGroupId())),
                    ImmutableList.of(),
                    Optional.empty(),
                    context.tag());

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(
                    node,
                    "Apply",
                    ImmutableMap.of("correlation", formatSymbols(node.getCorrelation())),
                    context.tag());
            printAssignments(nodeOutput, node.getSubqueryAssignments());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitCorrelatedJoin(CorrelatedJoinNode node, Context context)
        {
            addNode(node,
                    "CorrelatedJoin",
                    ImmutableMap.of(
                            "correlation", formatSymbols(node.getCorrelation()),
                            "filter", formatFilter(node.getFilter())),
                    context.tag());

            return processChildren(node, new Context());
        }

        @Override
        public Void visitTableFunction(TableFunctionNode node, Context context)
        {
            NodeRepresentation nodeOutput = addNode(
                    node,
                    "TableFunction",
                    ImmutableMap.of("name", node.getName()),
                    context.tag());

            if (!node.getArguments().isEmpty()) {
                nodeOutput.appendDetails("Arguments:");

                Map<String, TableArgumentProperties> tableArguments = node.getTableArgumentProperties().stream()
                        .collect(toImmutableMap(TableArgumentProperties::getArgumentName, identity()));

                node.getArguments().entrySet()
                        .forEach(entry -> nodeOutput.appendDetails("%s", formatArgument(entry.getKey(), entry.getValue(), tableArguments)));

                if (!node.getCopartitioningLists().isEmpty()) {
                    nodeOutput.appendDetails("%s", node.getCopartitioningLists().stream()
                            .map(list -> list.stream().collect(Collectors.joining(", ", "(", ")")))
                            .collect(joining(", ", "Co-partition: [", "]")));
                }
            }

            for (int i = 0; i < node.getSources().size(); i++) {
                node.getSources().get(i).accept(this, new Context(node.getTableArgumentProperties().get(i).getArgumentName()));
            }

            return null;
        }

        private String formatArgument(String argumentName, Argument argument, Map<String, TableArgumentProperties> tableArguments)
        {
            if (argument instanceof ScalarArgument scalarArgument) {
                return formatScalarArgument(argumentName, scalarArgument);
            }
            if (argument instanceof DescriptorArgument descriptorArgument) {
                return formatDescriptorArgument(argumentName, descriptorArgument);
            }
            else {
                TableArgumentProperties argumentProperties = tableArguments.get(argumentName);
                return formatTableArgument(argumentName, argumentProperties);
            }
        }

        private String formatScalarArgument(String argumentName, ScalarArgument argument)
        {
            return format(
                    "%s => ScalarArgument{type=%s, value=%s}",
                    argumentName,
                    argument.getType().getDisplayName(),
                    anonymizer.anonymize(
                            argument.getType(),
                            valuePrinter.castToVarchar(argument.getType(), argument.getValue())));
        }

        private String formatDescriptorArgument(String argumentName, DescriptorArgument argument)
        {
            String descriptor;
            if (argument.equals(NULL_DESCRIPTOR)) {
                descriptor = "NULL";
            }
            else {
                descriptor = argument.getDescriptor().orElseThrow().getFields().stream()
                        .map(field -> anonymizer.anonymizeColumn(field.getName().orElseThrow()) + field.getType().map(type -> " " + type.getDisplayName()).orElse(""))
                        .collect(joining(", ", "(", ")"));
            }
            return format("%s => DescriptorArgument{%s}", argumentName, descriptor);
        }

        private String formatTableArgument(String argumentName, TableArgumentProperties argumentProperties)
        {
            StringBuilder properties = new StringBuilder();
            if (argumentProperties.isRowSemantics()) {
                properties.append("row semantics");
            }
            argumentProperties.getSpecification().ifPresent(specification -> {
                properties
                        .append("partition by: [")
                        .append(Joiner.on(", ").join(anonymize(specification.getPartitionBy())))
                        .append("]");
                specification.getOrderingScheme().ifPresent(orderingScheme -> {
                    properties
                            .append(", order by: ")
                            .append(formatOrderingScheme(orderingScheme));
                });
            });
            properties.append("required columns: [")
                    .append(Joiner.on(", ").join(anonymize(argumentProperties.getRequiredColumns())))
                    .append("]");
            if (argumentProperties.isPruneWhenEmpty()) {
                properties.append(", prune when empty");
            }
            if (argumentProperties.getPassThroughSpecification().declaredAsPassThrough()) {
                properties.append(", pass through columns");
            }
            return format("%s => TableArgument{%s}", argumentName, properties);
        }

        @Override
        public Void visitTableFunctionProcessor(TableFunctionProcessorNode node, Context context)
        {
            ImmutableMap.Builder<String, String> descriptor = ImmutableMap.builder();

            descriptor.put("name", node.getName());

            descriptor.put("properOutputs", format("[%s]", Joiner.on(", ").join(anonymize(node.getProperOutputs()))));

            node.getSpecification().ifPresent(specification -> {
                if (!specification.getPartitionBy().isEmpty()) {
                    List<Symbol> prePartitioned = specification.getPartitionBy().stream()
                            .filter(node.getPrePartitioned()::contains)
                            .collect(toImmutableList());

                    List<Symbol> notPrePartitioned = specification.getPartitionBy().stream()
                            .filter(column -> !node.getPrePartitioned().contains(column))
                            .collect(toImmutableList());

                    StringBuilder builder = new StringBuilder();
                    if (!prePartitioned.isEmpty()) {
                        builder.append(anonymize(prePartitioned).stream()
                                .collect(joining(", ", "<", ">")));
                        if (!notPrePartitioned.isEmpty()) {
                            builder.append(", ");
                        }
                    }
                    if (!notPrePartitioned.isEmpty()) {
                        builder.append(Joiner.on(", ").join(anonymize(notPrePartitioned)));
                    }
                    descriptor.put("partitionBy", format("[%s]", builder));
                }
                specification.getOrderingScheme().ifPresent(orderingScheme -> descriptor.put("orderBy", formatOrderingScheme(orderingScheme, node.getPreSorted())));
            });

            addNode(node, "TableFunctionProcessor", descriptor.put("hash", formatHash(node.getHashSymbol())).buildOrThrow(), context.tag());

            return processChildren(node, new Context());
        }

        @Override
        protected Void visitPlan(PlanNode node, Context context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, Context context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }

            return null;
        }

        private void printAssignments(NodeRepresentation nodeOutput, Assignments assignments)
        {
            for (Map.Entry<Symbol, Expression> entry : assignments.getMap().entrySet()) {
                if (entry.getValue() instanceof SymbolReference && ((SymbolReference) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                nodeOutput.appendDetails("%s := %s", anonymizer.anonymize(entry.getKey()), anonymizer.anonymize(unresolveFunctions(entry.getValue())));
            }
        }

        private void printConstraint(NodeRepresentation nodeOutput, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
            if (domains.containsKey(column)) {
                nodeOutput.appendDetails("    :: %s", formatDomain(domains.get(column).simplify()));
            }
        }

        private String formatDomain(Domain domain)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            if (domain.isNullAllowed()) {
                parts.add("NULL");
            }

            Type type = domain.getType();

            domain.getValues().getValuesProcessor().consume(
                    ranges -> {
                        for (Range range : ranges.getOrderedRanges()) {
                            StringBuilder builder = new StringBuilder();
                            if (range.isSingleValue()) {
                                String value = anonymizer.anonymize(type, valuePrinter.castToVarchar(type, range.getSingleValue()));
                                builder.append('[').append(value).append(']');
                            }
                            else {
                                builder.append(range.isLowInclusive() ? '[' : '(');

                                if (range.isLowUnbounded()) {
                                    builder.append("<min>");
                                }
                                else {
                                    builder.append(anonymizer.anonymize(type, valuePrinter.castToVarchar(type, range.getLowBoundedValue())));
                                }

                                builder.append(", ");

                                if (range.isHighUnbounded()) {
                                    builder.append("<max>");
                                }
                                else {
                                    builder.append(anonymizer.anonymize(type, valuePrinter.castToVarchar(type, range.getHighBoundedValue())));
                                }

                                builder.append(range.isHighInclusive() ? ']' : ')');
                            }
                            parts.add(builder.toString());
                        }
                    },
                    discreteValues -> discreteValues.getValues().stream()
                            .map(value -> anonymizer.anonymize(type, valuePrinter.castToVarchar(type, value)))
                            .sorted() // Sort so the values will be printed in predictable order
                            .forEach(parts::add),
                    allOrNone -> {
                        if (allOrNone.isAll()) {
                            parts.add("ALL VALUES");
                        }
                    });

            return "[" + Joiner.on(", ").join(parts.build()) + "]";
        }

        private String formatFilter(Expression filter)
        {
            return filter.equals(TRUE_LITERAL) ? "" : anonymizer.anonymize(filter);
        }

        private String formatBoolean(boolean value)
        {
            return value ? "true" : "";
        }

        private String formatOrderingScheme(OrderingScheme orderingScheme, int preSortedOrderPrefix)
        {
            List<String> orderBy = Stream.concat(
                            orderingScheme.getOrderBy().stream()
                                    .limit(preSortedOrderPrefix)
                                    .map(symbol -> "<" + anonymizer.anonymize(symbol) + " " + orderingScheme.getOrdering(symbol) + ">"),
                            orderingScheme.getOrderBy().stream()
                                    .skip(preSortedOrderPrefix)
                                    .map(symbol -> anonymizer.anonymize(symbol) + " " + orderingScheme.getOrdering(symbol)))
                    .collect(toImmutableList());
            return formatCollection(orderBy, Objects::toString);
        }

        private String formatOrderingScheme(OrderingScheme orderingScheme)
        {
            return formatCollection(orderingScheme.getOrderBy(), input -> anonymizer.anonymize(input) + " " + orderingScheme.getOrdering(input));
        }

        @SafeVarargs
        private String formatHash(Optional<Symbol>... hashes)
        {
            List<Symbol> symbols = stream(hashes)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());
            return formatSymbols(symbols);
        }

        private String formatSymbols(Collection<Symbol> symbols)
        {
            return formatCollection(symbols, anonymizer::anonymize);
        }

        private List<String> anonymize(Collection<Symbol> symbols)
        {
            return symbols.stream()
                    .map(anonymizer::anonymize)
                    .collect(toImmutableList());
        }

        private List<String> anonymizeExpressions(List<Expression> expressions)
        {
            return expressions.stream()
                    .map(anonymizer::anonymize)
                    .collect(toImmutableList());
        }

        private String formatOutputs(TypeProvider types, Iterable<Symbol> outputs)
        {
            return Streams.stream(outputs)
                    .map(input -> anonymizer.anonymize(input) + ":" + types.get(input).getDisplayName())
                    .collect(joining(", ", "[", "]"));
        }

        public NodeRepresentation addNode(PlanNode node, String name, Optional<String> tag)
        {
            return addNode(node, name, ImmutableMap.of(), tag);
        }

        public NodeRepresentation addNode(PlanNode node, String name, Map<String, String> descriptor, Optional<String> tag)
        {
            return addNode(node, name, descriptor, node.getSources(), Optional.empty(), tag);
        }

        public NodeRepresentation addNode(PlanNode node, String name, Map<String, String> descriptor, Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost, Optional<String> tag)
        {
            return addNode(node, name, descriptor, node.getSources(), reorderJoinStatsAndCost, tag);
        }

        public NodeRepresentation addNode(PlanNode node, String name, Map<String, String> descriptor, List<PlanNode> children, Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost, Optional<String> tag)
        {
            return addNode(node, name, descriptor, ImmutableList.of(node.getId()), children, ImmutableList.of(), reorderJoinStatsAndCost, tag);
        }

        public NodeRepresentation addNode(
                PlanNode rootNode,
                String name,
                Map<String, String> descriptor,
                List<PlanNodeId> allNodes,
                List<PlanNode> children,
                List<PlanFragmentId> remoteSources,
                Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost,
                Optional<String> tag)
        {
            List<PlanNodeId> childrenIds = children.stream().map(PlanNode::getId).collect(toImmutableList());
            List<PlanNodeStatsEstimate> estimatedStats = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getStats().getOrDefault(nodeId, PlanNodeStatsEstimate.unknown()))
                    .collect(toList());
            List<PlanCostEstimate> estimatedCosts = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getCosts().getOrDefault(nodeId, PlanCostEstimate.unknown()))
                    .collect(toList());
            name = tag
                    .map(tagName -> format("[%s] ", tagName))
                    .orElse("") + name;

            NodeRepresentation nodeOutput = new NodeRepresentation(
                    rootNode.getId(),
                    name,
                    rootNode.getClass().getSimpleName(),
                    descriptor,
                    rootNode.getOutputSymbols().stream()
                            .map(s -> new TypedSymbol(new Symbol(anonymizer.anonymize(s)), types.get(s).getDisplayName()))
                            .collect(toImmutableList()),
                    stats.map(s -> s.get(rootNode.getId())),
                    estimatedStats,
                    estimatedCosts,
                    reorderJoinStatsAndCost,
                    childrenIds,
                    remoteSources);

            representation.addNode(nodeOutput);
            return nodeOutput;
        }
    }

    private static <T> String formatCollection(Collection<T> collection, Function<T, String> formatter)
    {
        return collection.stream()
                .map(formatter)
                .collect(joining(", ", "[", "]"));
    }

    public static String formatAggregation(Anonymizer anonymizer, Aggregation aggregation)
    {
        StringBuilder builder = new StringBuilder();
        List<String> anonymizedArguments = aggregation.getArguments().stream()
                .map(anonymizer::anonymize)
                .collect(toImmutableList());
        String arguments = Joiner.on(", ").join(anonymizedArguments);
        if (aggregation.getArguments().isEmpty() && "count".equalsIgnoreCase(aggregation.getResolvedFunction().getSignature().getName())) {
            arguments = "*";
        }
        if (aggregation.isDistinct()) {
            arguments = "DISTINCT " + arguments;
        }

        builder.append(aggregation.getResolvedFunction().getSignature().getName())
                .append('(').append(arguments);

        aggregation.getOrderingScheme().ifPresent(orderingScheme -> builder.append(' ').append(orderingScheme.getOrderBy().stream()
                .map(input -> anonymizer.anonymize(input) + " " + orderingScheme.getOrdering(input))
                .collect(joining(", "))));

        builder.append(')');

        aggregation.getFilter()
                .map(anonymizer::anonymize)
                .ifPresent(expression -> builder.append(" FILTER (WHERE ").append(expression).append(")"));

        aggregation.getMask()
                .map(anonymizer::anonymize)
                .ifPresent(symbol -> builder.append(" (mask = ").append(symbol).append(")"));
        return builder.toString();
    }

    private static Expression unresolveFunctions(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                FunctionCall rewritten = treeRewriter.defaultRewrite(node, context);

                return new FunctionCall(
                        rewritten.getLocation(),
                        QualifiedName.of(extractFunctionName(node.getName())),
                        rewritten.getWindow(),
                        rewritten.getFilter(),
                        rewritten.getOrderBy(),
                        rewritten.isDistinct(),
                        rewritten.getNullTreatment(),
                        rewritten.getProcessingMode(),
                        rewritten.getArguments());
            }
        }, expression);
    }

    private record Context(Optional<String> tag)
    {
        public Context()
        {
            this(Optional.empty());
        }

        public Context(String tag)
        {
            this(Optional.of(tag));
        }

        private Context
        {
            requireNonNull(tag, "tag is null");
        }
    }
}
