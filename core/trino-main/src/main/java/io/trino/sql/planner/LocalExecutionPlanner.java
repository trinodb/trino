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
package io.trino.sql.planner;

import com.google.common.base.VerifyException;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.ExplainAnalyzeContext;
import io.trino.execution.StageId;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.index.IndexManager;
import io.trino.metadata.FunctionKind;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.AssignUniqueIdOperator;
import io.trino.operator.DeleteOperator.DeleteOperatorFactory;
import io.trino.operator.DevNullOperator.DevNullOperatorFactory;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.DriverFactory;
import io.trino.operator.DynamicFilterSourceOperator;
import io.trino.operator.DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory;
import io.trino.operator.EnforceSingleRowOperator;
import io.trino.operator.ExchangeOperator.ExchangeOperatorFactory;
import io.trino.operator.ExplainAnalyzeOperator.ExplainAnalyzeOperatorFactory;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.GroupIdOperator;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.HashSemiJoinOperator;
import io.trino.operator.LimitOperator.LimitOperatorFactory;
import io.trino.operator.LocalPlannerAware;
import io.trino.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.trino.operator.MergeOperator.MergeOperatorFactory;
import io.trino.operator.OperatorFactories;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OrderByOperator.OrderByOperatorFactory;
import io.trino.operator.OutputFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesSpatialIndexFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.PipelineExecutionStrategy;
import io.trino.operator.RefreshMaterializedViewOperator.RefreshMaterializedViewOperatorFactory;
import io.trino.operator.RetryPolicy;
import io.trino.operator.RowNumberOperator;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetSupplier;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialIndexBuilderOperatorFactory;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.trino.operator.SpatialJoinOperator.SpatialJoinOperatorFactory;
import io.trino.operator.StageExecutionDescriptor;
import io.trino.operator.StatisticsWriterOperator.StatisticsWriterOperatorFactory;
import io.trino.operator.StreamingAggregationOperator;
import io.trino.operator.TableDeleteOperator.TableDeleteOperatorFactory;
import io.trino.operator.TableScanOperator.TableScanOperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.TopNOperator;
import io.trino.operator.TopNRankingOperator;
import io.trino.operator.UpdateOperator.UpdateOperatorFactory;
import io.trino.operator.ValuesOperator.ValuesOperatorFactory;
import io.trino.operator.WindowFunctionDefinition;
import io.trino.operator.WindowOperator.WindowOperatorFactory;
import io.trino.operator.WorkProcessorPipelineSourceOperator;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.AggregationMetadata;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.DistinctAccumulatorFactory;
import io.trino.operator.aggregation.LambdaProvider;
import io.trino.operator.aggregation.OrderedAccumulatorFactory;
import io.trino.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.trino.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import io.trino.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import io.trino.operator.exchange.LocalMergeSourceOperator.LocalMergeSourceOperatorFactory;
import io.trino.operator.exchange.PageChannelSelector;
import io.trino.operator.index.DynamicTupleFilterFactory;
import io.trino.operator.index.FieldSetFilteringRecordSet;
import io.trino.operator.index.IndexBuildDriverFactoryProvider;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.index.IndexLookupSourceFactory;
import io.trino.operator.index.IndexSourceOperator;
import io.trino.operator.join.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorFactory;
import io.trino.operator.join.JoinOperatorFactory.OuterOperatorFactoryResult;
import io.trino.operator.join.LookupOuterOperator.LookupOuterOperatorFactory;
import io.trino.operator.join.LookupSourceFactory;
import io.trino.operator.join.NestedLoopJoinBridge;
import io.trino.operator.join.NestedLoopJoinPagesSupplier;
import io.trino.operator.join.PartitionedLookupSourceFactory;
import io.trino.operator.output.TaskOutputOperator.TaskOutputFactory;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.operator.window.AggregationWindowFunctionSupplier;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.PartitionerSupplier;
import io.trino.operator.window.PatternRecognitionPartitionerSupplier;
import io.trino.operator.window.RegularPartitionerSupplier;
import io.trino.operator.window.WindowFunctionSupplier;
import io.trino.operator.window.matcher.IrRowPatternToProgramRewriter;
import io.trino.operator.window.matcher.Matcher;
import io.trino.operator.window.matcher.Program;
import io.trino.operator.window.pattern.ArgumentComputation.ArgumentComputationSupplier;
import io.trino.operator.window.pattern.LabelEvaluator.EvaluationSupplier;
import io.trino.operator.window.pattern.LogicalIndexNavigation;
import io.trino.operator.window.pattern.MatchAggregation.MatchAggregationInstantiator;
import io.trino.operator.window.pattern.MatchAggregationPointer;
import io.trino.operator.window.pattern.MeasureComputation.MeasureComputationSupplier;
import io.trino.operator.window.pattern.PhysicalValueAccessor;
import io.trino.operator.window.pattern.PhysicalValuePointer;
import io.trino.operator.window.pattern.SetEvaluator.SetEvaluatorSupplier;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.spiller.SpillerFactory;
import io.trino.split.MappedRecordSet;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceProvider;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.optimizations.IndexJoinOptimizer;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterId;
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
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import io.trino.sql.planner.plan.TableWriterNode.UpdateTarget;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.UpdateNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Frame;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.sql.tree.SymbolReference;
import io.trino.type.BlockTypeOperators;
import io.trino.type.FunctionType;

import javax.inject.Inject;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Range.closedOpen;
import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.trino.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.getTaskWriterCount;
import static io.trino.SystemSessionProperties.isEnableCoordinatorDynamicFiltersDistribution;
import static io.trino.SystemSessionProperties.isEnableLargeDynamicFilters;
import static io.trino.SystemSessionProperties.isExchangeCompressionEnabled;
import static io.trino.SystemSessionProperties.isLateMaterializationEnabled;
import static io.trino.SystemSessionProperties.isSpillEnabled;
import static io.trino.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static io.trino.operator.HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier;
import static io.trino.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static io.trino.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.trino.operator.TableFinishOperator.TableFinishOperatorFactory;
import static io.trino.operator.TableFinishOperator.TableFinisher;
import static io.trino.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static io.trino.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static io.trino.operator.TableWriterOperator.STATS_START_CHANNEL;
import static io.trino.operator.TableWriterOperator.TableWriterOperatorFactory;
import static io.trino.operator.WindowFunctionDefinition.window;
import static io.trino.operator.WorkProcessorPipelineSourceOperator.toOperatorFactories;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactory;
import static io.trino.operator.join.JoinUtils.isBuildSideReplicated;
import static io.trino.operator.join.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import static io.trino.operator.join.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import static io.trino.operator.unnest.UnnestOperator.UnnestOperatorFactory;
import static io.trino.operator.window.pattern.PhysicalValuePointer.CLASSIFIER;
import static io.trino.operator.window.pattern.PhysicalValuePointer.MATCH_NUMBER;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.gen.LambdaBytecodeGenerator.compileLambdaProvider;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.SortExpressionExtractor.extractSortExpression;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.SkipTo.Position.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.SpatialJoinUtils.ST_CONTAINS;
import static io.trino.util.SpatialJoinUtils.ST_DISTANCE;
import static io.trino.util.SpatialJoinUtils.ST_INTERSECTS;
import static io.trino.util.SpatialJoinUtils.ST_WITHIN;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.IntStream.range;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;
    private final Optional<ExplainAnalyzeContext> explainAnalyzeContext;
    private final PageSourceProvider pageSourceProvider;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final ExpressionCompiler expressionCompiler;
    private final PageFunctionCompiler pageFunctionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final DataSize maxIndexMemorySize;
    private final IndexJoinLookupStats indexJoinLookupStats;
    private final DataSize maxPartialAggregationMemorySize;
    private final DataSize maxPagePartitioningBufferSize;
    private final DataSize maxLocalExchangeBufferSize;
    private final SpillerFactory spillerFactory;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final PagesIndex.Factory pagesIndexFactory;
    private final JoinCompiler joinCompiler;
    private final OperatorFactories operatorFactories;
    private final OrderingCompiler orderingCompiler;
    private final DynamicFilterConfig dynamicFilterConfig;
    private final BlockTypeOperators blockTypeOperators;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

    @Inject
    public LocalExecutionPlanner(
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            ExpressionCompiler expressionCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            JoinFilterFunctionCompiler joinFilterFunctionCompiler,
            IndexJoinLookupStats indexJoinLookupStats,
            TaskManagerConfig taskManagerConfig,
            SpillerFactory spillerFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            OperatorFactories operatorFactories,
            OrderingCompiler orderingCompiler,
            DynamicFilterConfig dynamicFilterConfig,
            BlockTypeOperators blockTypeOperators,
            TableExecuteContextManager tableExecuteContextManager,
            ExchangeManagerRegistry exchangeManagerRegistry)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.explainAnalyzeContext = requireNonNull(explainAnalyzeContext, "explainAnalyzeContext is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.directExchangeClientSupplier = directExchangeClientSupplier;
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.expressionCompiler = requireNonNull(expressionCompiler, "expressionCompiler is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.joinFilterFunctionCompiler = requireNonNull(joinFilterFunctionCompiler, "joinFilterFunctionCompiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.maxIndexMemorySize = requireNonNull(taskManagerConfig, "taskManagerConfig is null").getMaxIndexMemoryUsage();
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.maxPagePartitioningBufferSize = taskManagerConfig.getMaxPagePartitioningBufferSize();
        this.maxLocalExchangeBufferSize = taskManagerConfig.getMaxLocalExchangeBufferSize();
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.operatorFactories = requireNonNull(operatorFactories, "operatorFactories is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.dynamicFilterConfig = requireNonNull(dynamicFilterConfig, "dynamicFilterConfig is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            TypeProvider types,
            PartitioningScheme partitioningScheme,
            StageExecutionDescriptor stageExecutionDescriptor,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer)
    {
        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, stageExecutionDescriptor, plan, outputLayout, types, partitionedSourceOrder, new TaskOutputFactory(outputBuffer));
        }

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<NullableValue>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        }
        else {
            partitionChannels = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return -1;
                        }
                        return outputLayout.indexOf(argument.getColumn());
                    })
                    .collect(toImmutableList());
            partitionConstants = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return Optional.of(argument.getConstant());
                        }
                        return Optional.<NullableValue>empty();
                    })
                    .collect(toImmutableList());
            partitionChannelTypes = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return argument.getConstant().getType();
                        }
                        return types.get(argument.getColumn());
                    })
                    .collect(toImmutableList());
        }

        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(), partitioningScheme, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        return plan(
                taskContext,
                stageExecutionDescriptor,
                plan,
                outputLayout,
                types,
                partitionedSourceOrder,
                operatorFactories.partitionedOutput(
                        taskContext,
                        partitionFunction,
                        partitionChannels,
                        partitionConstants,
                        partitioningScheme.isReplicateNullsAndAny(),
                        nullChannel,
                        outputBuffer,
                        maxPagePartitioningBufferSize));
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            StageExecutionDescriptor stageExecutionDescriptor,
            PlanNode plan,
            List<Symbol> outputLayout,
            TypeProvider types,
            List<PlanNodeId> partitionedSourceOrder,
            OutputFactory outputOperatorFactory)
    {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, types);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session, stageExecutionDescriptor), context);

        Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(outputLayout, physicalOperation.getLayout());

        List<Type> outputTypes = outputLayout.stream()
                .map(types::get)
                .collect(toImmutableList());

        context.addDriverFactory(
                context.isInputDriver(),
                true,
                new PhysicalOperation(
                        outputOperatorFactory.createOutputOperator(
                                context.getNextOperatorId(),
                                plan.getId(),
                                outputTypes,
                                pagePreprocessor,
                                new PagesSerdeFactory(plannerContext.getBlockEncodingSerde(), isExchangeCompressionEnabled(session))),
                        physicalOperation),
                context.getDriverInstanceCount());

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder, stageExecutionDescriptor);
    }

    private static class LocalExecutionPlanContext
    {
        private final TaskContext taskContext;
        private final TypeProvider types;
        private final List<DriverFactory> driverFactories;
        private final Optional<IndexSourceContext> indexSourceContext;

        // this is shared with all subContexts
        private final AtomicInteger nextPipelineId;

        private int nextOperatorId;
        private boolean inputDriver = true;
        private OptionalInt driverInstanceCount = OptionalInt.empty();

        public LocalExecutionPlanContext(TaskContext taskContext, TypeProvider types)
        {
            this(
                    taskContext,
                    types,
                    new ArrayList<>(),
                    Optional.empty(),
                    new AtomicInteger(0));
        }

        private LocalExecutionPlanContext(
                TaskContext taskContext,
                TypeProvider types,
                List<DriverFactory> driverFactories,
                Optional<IndexSourceContext> indexSourceContext,
                AtomicInteger nextPipelineId)
        {
            this.taskContext = taskContext;
            this.types = types;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
            this.nextPipelineId = nextPipelineId;
        }

        public void addDriverFactory(boolean inputDriver, boolean outputDriver, PhysicalOperation physicalOperation, OptionalInt driverInstances)
        {
            List<OperatorFactoryWithTypes> operatorFactoriesWithTypes = physicalOperation.getOperatorFactoriesWithTypes();
            validateFirstOperatorFactory(inputDriver, operatorFactoriesWithTypes.get(0).getOperatorFactory(), physicalOperation.getPipelineExecutionStrategy());
            addLookupOuterDrivers(outputDriver, toOperatorFactories(operatorFactoriesWithTypes));
            List<OperatorFactory> operatorFactories;
            if (isLateMaterializationEnabled(taskContext.getSession())) {
                operatorFactories = handleLateMaterialization(operatorFactoriesWithTypes);
            }
            else {
                operatorFactories = toOperatorFactories(operatorFactoriesWithTypes);
            }
            driverFactories.add(new DriverFactory(getNextPipelineId(), inputDriver, outputDriver, operatorFactories, driverInstances, physicalOperation.getPipelineExecutionStrategy()));
        }

        private List<OperatorFactory> handleLateMaterialization(List<OperatorFactoryWithTypes> operatorFactories)
        {
            return WorkProcessorPipelineSourceOperator.convertOperators(
                    operatorFactories,
                    getFilterAndProjectMinOutputPageSize(taskContext.getSession()),
                    getFilterAndProjectMinOutputPageRowCount(taskContext.getSession()));
        }

        private void addLookupOuterDrivers(boolean isOutputDriver, List<OperatorFactory> operatorFactories)
        {
            // For an outer join on the lookup side (RIGHT or FULL) add an additional
            // driver to output the unused rows in the lookup source
            for (int i = 0; i < operatorFactories.size(); i++) {
                OperatorFactory operatorFactory = operatorFactories.get(i);
                if (!(operatorFactory instanceof JoinOperatorFactory)) {
                    continue;
                }

                JoinOperatorFactory lookupJoin = (JoinOperatorFactory) operatorFactory;
                Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult = lookupJoin.createOuterOperatorFactory();
                if (outerOperatorFactoryResult.isPresent()) {
                    // Add a new driver to output the unmatched rows in an outer join.
                    // We duplicate all of the factories above the JoinOperator (the ones reading from the joins),
                    // and replace the JoinOperator with the OuterOperator (the one that produces unmatched rows).
                    ImmutableList.Builder<OperatorFactory> newOperators = ImmutableList.builder();
                    newOperators.add(outerOperatorFactoryResult.get().getOuterOperatorFactory());
                    operatorFactories.subList(i + 1, operatorFactories.size()).stream()
                            .map(OperatorFactory::duplicate)
                            .forEach(newOperators::add);

                    addDriverFactory(false, isOutputDriver, newOperators.build(), OptionalInt.of(1), outerOperatorFactoryResult.get().getBuildExecutionStrategy());
                }
            }
        }

        private void addDriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            validateFirstOperatorFactory(inputDriver, operatorFactories.get(0), pipelineExecutionStrategy);
            driverFactories.add(new DriverFactory(getNextPipelineId(), inputDriver, outputDriver, operatorFactories, driverInstances, pipelineExecutionStrategy));
        }

        private void validateFirstOperatorFactory(boolean inputDriver, OperatorFactory firstOperatorFactory, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            if (pipelineExecutionStrategy == GROUPED_EXECUTION) {
                if (inputDriver) {
                    checkArgument(firstOperatorFactory instanceof ScanFilterAndProjectOperatorFactory || firstOperatorFactory instanceof TableScanOperatorFactory);
                }
                else {
                    checkArgument(firstOperatorFactory instanceof LocalExchangeSourceOperatorFactory || firstOperatorFactory instanceof LookupOuterOperatorFactory);
                }
            }
        }

        private List<DriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        public StageId getStageId()
        {
            return taskContext.getTaskId().getStageId();
        }

        public TaskId getTaskId()
        {
            return taskContext.getTaskId();
        }

        public TypeProvider getTypes()
        {
            return types;
        }

        public LocalDynamicFiltersCollector getDynamicFiltersCollector()
        {
            return taskContext.getLocalDynamicFiltersCollector();
        }

        private void addLocalDynamicFilters(Map<DynamicFilterId, Domain> dynamicTupleDomain)
        {
            taskContext.addDynamicFilter(dynamicTupleDomain);
        }

        private void registerCoordinatorDynamicFilters(List<DynamicFilters.Descriptor> dynamicFilters)
        {
            if (!isEnableCoordinatorDynamicFiltersDistribution(taskContext.getSession())) {
                return;
            }
            Set<DynamicFilterId> consumedFilterIds = dynamicFilters.stream()
                    .map(DynamicFilters.Descriptor::getId)
                    .collect(toImmutableSet());
            LocalDynamicFiltersCollector dynamicFiltersCollector = getDynamicFiltersCollector();
            // Don't repeat registration of node-local filters or those already registered by another scan (e.g. co-located joins)
            dynamicFiltersCollector.register(
                    difference(consumedFilterIds, dynamicFiltersCollector.getRegisteredDynamicFilterIds()));
        }

        private void addCoordinatorDynamicFilters(Map<DynamicFilterId, Domain> dynamicTupleDomain)
        {
            taskContext.updateDomains(dynamicTupleDomain);
        }

        public Optional<IndexSourceContext> getIndexSourceContext()
        {
            return indexSourceContext;
        }

        private int getNextPipelineId()
        {
            return nextPipelineId.getAndIncrement();
        }

        private int getNextOperatorId()
        {
            return nextOperatorId++;
        }

        private boolean isInputDriver()
        {
            return inputDriver;
        }

        private void setInputDriver(boolean inputDriver)
        {
            this.inputDriver = inputDriver;
        }

        public LocalExecutionPlanContext createSubContext()
        {
            checkState(indexSourceContext.isEmpty(), "index build plan cannot have sub-contexts");
            return new LocalExecutionPlanContext(taskContext, types, driverFactories, indexSourceContext, nextPipelineId);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(taskContext, types, driverFactories, Optional.of(indexSourceContext), nextPipelineId);
        }

        public OptionalInt getDriverInstanceCount()
        {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount)
        {
            checkArgument(driverInstanceCount > 0, "driverInstanceCount must be > 0");
            if (this.driverInstanceCount.isPresent()) {
                checkState(this.driverInstanceCount.getAsInt() == driverInstanceCount, "driverInstance count already set to " + this.driverInstanceCount.getAsInt());
            }
            this.driverInstanceCount = OptionalInt.of(driverInstanceCount);
        }
    }

    private static class IndexSourceContext
    {
        private final SetMultimap<Symbol, Integer> indexLookupToProbeInput;

        public IndexSourceContext(SetMultimap<Symbol, Integer> indexLookupToProbeInput)
        {
            this.indexLookupToProbeInput = ImmutableSetMultimap.copyOf(requireNonNull(indexLookupToProbeInput, "indexLookupToProbeInput is null"));
        }

        private SetMultimap<Symbol, Integer> getIndexLookupToProbeInput()
        {
            return indexLookupToProbeInput;
        }
    }

    public static class LocalExecutionPlan
    {
        private final List<DriverFactory> driverFactories;
        private final List<PlanNodeId> partitionedSourceOrder;
        private final StageExecutionDescriptor stageExecutionDescriptor;

        public LocalExecutionPlan(List<DriverFactory> driverFactories, List<PlanNodeId> partitionedSourceOrder, StageExecutionDescriptor stageExecutionDescriptor)
        {
            this.driverFactories = ImmutableList.copyOf(requireNonNull(driverFactories, "driverFactories is null"));
            this.partitionedSourceOrder = ImmutableList.copyOf(requireNonNull(partitionedSourceOrder, "partitionedSourceOrder is null"));
            this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
        }

        public List<DriverFactory> getDriverFactories()
        {
            return driverFactories;
        }

        public List<PlanNodeId> getPartitionedSourceOrder()
        {
            return partitionedSourceOrder;
        }

        public StageExecutionDescriptor getStageExecutionDescriptor()
        {
            return stageExecutionDescriptor;
        }
    }

    public static class OperatorFactoryWithTypes
    {
        private final OperatorFactory operatorFactory;
        private final List<Type> types;

        public OperatorFactoryWithTypes(OperatorFactory operatorFactory, List<Type> types)
        {
            this.operatorFactory = requireNonNull(operatorFactory, "operatorFactory is null");
            this.types = requireNonNull(types, "types is null");
        }

        public OperatorFactory getOperatorFactory()
        {
            return operatorFactory;
        }

        public List<Type> getTypes()
        {
            return types;
        }
    }

    private class Visitor
            extends PlanVisitor<PhysicalOperation, LocalExecutionPlanContext>
    {
        private final Session session;
        private final StageExecutionDescriptor stageExecutionDescriptor;

        private Visitor(Session session, StageExecutionDescriptor stageExecutionDescriptor)
        {
            this.session = session;
            this.stageExecutionDescriptor = stageExecutionDescriptor;
        }

        @Override
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (node.getOrderingScheme().isPresent()) {
                return createMergeSource(node, context);
            }

            return createRemoteSource(node, context);
        }

        private PhysicalOperation createMergeSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkArgument(node.getRetryPolicy() == RetryPolicy.NONE, "unexpected retry policy: " + node.getRetryPolicy());

            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.getOrderingList();

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            ImmutableList<Integer> outputChannels = IntStream.range(0, types.size())
                    .boxed()
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new MergeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    directExchangeClientSupplier,
                    new PagesSerdeFactory(plannerContext.getBlockEncodingSerde(), isExchangeCompressionEnabled(session)),
                    orderingCompiler,
                    types,
                    outputChannels,
                    sortChannels,
                    sortOrder);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (context.getDriverInstanceCount().isEmpty()) {
                context.setDriverInstanceCount(getTaskConcurrency(session));
            }

            OperatorFactory operatorFactory = new ExchangeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    directExchangeClientSupplier,
                    new PagesSerdeFactory(plannerContext.getBlockEncodingSerde(), isExchangeCompressionEnabled(session)),
                    node.getRetryPolicy(),
                    exchangeManagerRegistry);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context)
        {
            ExplainAnalyzeContext analyzeContext = explainAnalyzeContext
                    .orElseThrow(() -> new IllegalStateException("ExplainAnalyze can only run on coordinator"));
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new ExplainAnalyzeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    analyzeContext.getQueryPerformanceFetcher(),
                    metadata,
                    node.isVerbose());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public PhysicalOperation visitRowNumber(RowNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());

            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            // row number function goes in the last channel
            int channel = source.getTypes().size();
            outputMappings.put(node.getRowNumberSymbol(), channel);

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            OperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    node.getMaxRowCountPerPartition(),
                    hashChannel,
                    10_000,
                    joinCompiler,
                    blockTypeOperators);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), context, source);
        }

        @Override
        public PhysicalOperation visitTopNRanking(TopNRankingNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());
            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();
            List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());
            List<SortOrder> sortOrder = orderBySymbols.stream()
                    .map(symbol -> node.getOrderingScheme().getOrdering(symbol))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            if (!node.isPartial() || !partitionChannels.isEmpty()) {
                // ranking function goes in the last channel
                int channel = source.getTypes().size();
                outputMappings.put(node.getRankingSymbol(), channel);
            }

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            boolean isPartial = node.isPartial();
            Optional<DataSize> maxPartialTopNMemorySize = isPartial ? Optional.of(SystemSessionProperties.getMaxPartialTopNMemory(session)).filter(
                    maxSize -> maxSize.compareTo(DataSize.ofBytes(0)) > 0) : Optional.empty();
            OperatorFactory operatorFactory = new TopNRankingOperator.TopNRankingOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getRankingType(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    node.getMaxRankingPerPartition(),
                    isPartial,
                    hashChannel,
                    1000,
                    maxPartialTopNMemorySize,
                    joinCompiler,
                    plannerContext.getTypeOperators(),
                    blockTypeOperators);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), source.getLayout());
                sortOrder = orderingScheme.getOrderingList();
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Symbol> windowFunctionOutputSymbolsBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                Optional<Integer> frameStartChannel = Optional.empty();
                Optional<Integer> sortKeyChannelForStartComparison = Optional.empty();
                Optional<Integer> frameEndChannel = Optional.empty();
                Optional<Integer> sortKeyChannelForEndComparison = Optional.empty();
                Optional<Integer> sortKeyChannel = Optional.empty();
                Optional<Ordering> ordering = Optional.empty();

                Frame frame = entry.getValue().getFrame();
                if (frame.getStartValue().isPresent()) {
                    frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
                }
                if (frame.getSortKeyCoercedForFrameStartComparison().isPresent()) {
                    sortKeyChannelForStartComparison = Optional.of(source.getLayout().get(frame.getSortKeyCoercedForFrameStartComparison().get()));
                }
                if (frame.getEndValue().isPresent()) {
                    frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
                }
                if (frame.getSortKeyCoercedForFrameEndComparison().isPresent()) {
                    sortKeyChannelForEndComparison = Optional.of(source.getLayout().get(frame.getSortKeyCoercedForFrameEndComparison().get()));
                }
                if (node.getOrderingScheme().isPresent()) {
                    sortKeyChannel = Optional.of(sortChannels.get(0));
                    ordering = Optional.of(sortOrder.get(0).isAscending() ? ASCENDING : DESCENDING);
                }
                FrameInfo frameInfo = new FrameInfo(
                        frame.getType(),
                        frame.getStartType(),
                        frameStartChannel,
                        sortKeyChannelForStartComparison,
                        frame.getEndType(),
                        frameEndChannel,
                        sortKeyChannelForEndComparison,
                        sortKeyChannel,
                        ordering);

                WindowNode.Function function = entry.getValue();
                ResolvedFunction resolvedFunction = function.getResolvedFunction();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (Expression argument : function.getArguments()) {
                    if (!(argument instanceof LambdaExpression)) {
                        Symbol argumentSymbol = Symbol.from(argument);
                        arguments.add(source.getLayout().get(argumentSymbol));
                    }
                }
                Symbol symbol = entry.getKey();
                WindowFunctionSupplier windowFunctionSupplier = getWindowFunctionImplementation(resolvedFunction);
                Type type = resolvedFunction.getSignature().getReturnType();

                List<LambdaExpression> lambdaExpressions = function.getArguments().stream()
                        .filter(LambdaExpression.class::isInstance)
                        .map(LambdaExpression.class::cast)
                        .collect(toImmutableList());
                List<FunctionType> functionTypes = resolvedFunction.getSignature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .map(FunctionType.class::cast)
                        .collect(toImmutableList());

                List<LambdaProvider> lambdaProviders = makeLambdaProviders(lambdaExpressions, windowFunctionSupplier.getLambdaInterfaces(), functionTypes);
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, frameInfo, function.isIgnoreNulls(), lambdaProviders, arguments.build()));
                windowFunctionOutputSymbolsBuilder.add(symbol);
            }

            List<Symbol> windowFunctionOutputSymbols = windowFunctionOutputSymbolsBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTypes().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctionsBuilder.build(),
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    10_000,
                    pagesIndexFactory,
                    isSpillEnabled(session),
                    spillerFactory,
                    orderingCompiler,
                    ImmutableList.of(),
                    new RegularPartitionerSupplier());

            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), context, source);
        }

        private WindowFunctionSupplier getWindowFunctionImplementation(ResolvedFunction resolvedFunction)
        {
            if (resolvedFunction.getFunctionKind() == FunctionKind.AGGREGATE) {
                AggregationMetadata aggregationMetadata = metadata.getAggregateFunctionImplementation(resolvedFunction);
                return new AggregationWindowFunctionSupplier(
                        resolvedFunction.getSignature(),
                        aggregationMetadata,
                        resolvedFunction.getFunctionNullability());
            }
            return metadata.getWindowFunctionImplementation(resolvedFunction);
        }

        @Override
        public PhysicalOperation visitPatternRecognition(PatternRecognitionNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), source.getLayout());
                sortOrder = orderingScheme.getOrderingList();
            }

            // The output order for pattern recognition operation is defined as follows:
            // - for ONE ROW PER MATCH: partition by symbols, then measures,
            // - for ALL ROWS PER MATCH: partition by symbols, order by symbols, measures, remaining input symbols,
            // - for WINDOW: all input symbols, then window functions (including measures).
            // The operator produces output in the following order:
            // - for ONE ROW PER MATCH: partition by symbols, then measures,
            // - otherwise all input symbols, then window functions and measures.
            // There is no need to shuffle channels for output. Any upstream operator will pick them in preferred order using output mappings.

            // input channels to be passed directly to output
            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();

            // all output symbols mapped to output channels
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();

            int nextOutputChannel;

            if (node.getRowsPerMatch() == ONE) {
                outputChannels.addAll(partitionChannels);
                nextOutputChannel = partitionBySymbols.size();
                for (int i = 0; i < partitionBySymbols.size(); i++) {
                    outputMappings.put(partitionBySymbols.get(i), i);
                }
            }
            else {
                outputChannels.addAll(IntStream.range(0, source.getTypes().size())
                        .boxed()
                        .collect(toImmutableList()));
                nextOutputChannel = source.getTypes().size();
                outputMappings.putAll(source.getLayout());
            }

            // measures go in remaining channels starting after the last channel from the source operator, one per channel
            for (Map.Entry<Symbol, Measure> measure : node.getMeasures().entrySet()) {
                outputMappings.put(measure.getKey(), nextOutputChannel);
                nextOutputChannel++;
            }

            // process window functions
            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                // window functions outputs go in remaining channels starting after the last measure channel
                outputMappings.put(entry.getKey(), nextOutputChannel);
                nextOutputChannel++;

                WindowNode.Function function = entry.getValue();
                ResolvedFunction resolvedFunction = function.getResolvedFunction();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (Expression argument : function.getArguments()) {
                    if (!(argument instanceof LambdaExpression)) {
                        Symbol argumentSymbol = Symbol.from(argument);
                        arguments.add(source.getLayout().get(argumentSymbol));
                    }
                }
                WindowFunctionSupplier windowFunctionSupplier = getWindowFunctionImplementation(resolvedFunction);
                Type type = resolvedFunction.getSignature().getReturnType();

                List<LambdaExpression> lambdaExpressions = function.getArguments().stream()
                        .filter(LambdaExpression.class::isInstance)
                        .map(LambdaExpression.class::cast)
                        .collect(toImmutableList());
                List<FunctionType> functionTypes = resolvedFunction.getSignature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .map(FunctionType.class::cast)
                        .collect(toImmutableList());

                List<LambdaProvider> lambdaProviders = makeLambdaProviders(lambdaExpressions, windowFunctionSupplier.getLambdaInterfaces(), functionTypes);
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, function.isIgnoreNulls(), lambdaProviders, arguments.build()));
            }

            // prepare structures specific to PatternRecognitionNode
            // 1. establish a two-way mapping of IrLabels to `int`
            List<IrLabel> primaryLabels = ImmutableList.copyOf(node.getVariableDefinitions().keySet());
            ImmutableList.Builder<String> labelNamesBuilder = ImmutableList.builder();
            ImmutableMap.Builder<IrLabel, Integer> mappingBuilder = ImmutableMap.builder();
            for (int i = 0; i < primaryLabels.size(); i++) {
                IrLabel label = primaryLabels.get(i);
                labelNamesBuilder.add(label.getName());
                mappingBuilder.put(label, i);
            }
            Map<IrLabel, Integer> mapping = mappingBuilder.buildOrThrow();
            List<String> labelNames = labelNamesBuilder.build();

            // 2. rewrite pattern to program
            Program program = IrRowPatternToProgramRewriter.rewrite(node.getPattern(), mapping);

            // 3. prepare common base frame for pattern matching in window
            Optional<FrameInfo> frame = node.getCommonBaseFrame()
                    .map(baseFrame -> {
                        checkArgument(
                                baseFrame.getType() == ROWS &&
                                        baseFrame.getStartType() == CURRENT_ROW,
                                "invalid base frame");
                        return new FrameInfo(
                                baseFrame.getType(),
                                baseFrame.getStartType(),
                                Optional.empty(),
                                Optional.empty(),
                                baseFrame.getEndType(),
                                baseFrame.getEndValue().map(source.getLayout()::get),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty());
                    });

            ConnectorSession connectorSession = session.toConnectorSession();
            // 4. prepare label evaluations (LabelEvaluator is to be instantiated once per Partition)

            // during pattern matching, each thread will have a list of aggregations necessary for label evaluations.
            // the list of aggregations for a thread will be produced at thread creation time from this supplier list, respecting the order.
            // pointers in LabelEvaluator and ThreadEquivalence will access aggregations by position in list.
            int matchAggregationIndex = 0;
            ImmutableList.Builder<MatchAggregationInstantiator> labelEvaluationsAggregations = ImmutableList.builder();
            // runtime-evaluated aggregation arguments will appear in additional channels after all source channels
            int firstUnusedChannel = source.getLayout().values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            ImmutableList.Builder<ArgumentComputationSupplier> labelEvaluationsAggregationArguments = ImmutableList.builder();
            ImmutableList.Builder<List<PhysicalValueAccessor>> evaluationsValuePointers = ImmutableList.builder();

            ImmutableList.Builder<MatchAggregationLabelDependency> aggregationsLabelDependencies = ImmutableList.builder();

            ImmutableList.Builder<EvaluationSupplier> evaluationsBuilder = ImmutableList.builder();
            for (ExpressionAndValuePointers expressionAndValuePointers : node.getVariableDefinitions().values()) {
                // compile the rewritten expression
                Supplier<PageProjection> pageProjectionSupplier = prepareProjection(expressionAndValuePointers, context);

                // prepare physical value accessors to provide input for the expression
                ValueAccessors valueAccessors = preparePhysicalValuePointers(expressionAndValuePointers, mapping, source, connectorSession, context, firstUnusedChannel, matchAggregationIndex);

                firstUnusedChannel = valueAccessors.getFirstUnusedChannel();
                matchAggregationIndex = valueAccessors.getAggregationIndex();

                // record aggregations
                labelEvaluationsAggregations.addAll(valueAccessors.getAggregations());

                // record aggregation argument computations
                labelEvaluationsAggregationArguments.addAll(valueAccessors.getAggregationArguments());

                // record aggregation label dependencies and value accessors for ThreadEquivalence
                aggregationsLabelDependencies.addAll(valueAccessors.getLabelDependencies());
                evaluationsValuePointers.add(valueAccessors.getValueAccessors());

                // build label evaluation
                evaluationsBuilder.add(new EvaluationSupplier(pageProjectionSupplier, valueAccessors.getValueAccessors(), labelNames, connectorSession));
            }
            List<EvaluationSupplier> labelEvaluations = evaluationsBuilder.build();

            // 5. prepare measures computations

            matchAggregationIndex = 0;
            ImmutableList.Builder<MatchAggregationInstantiator> measureComputationsAggregations = ImmutableList.builder();
            // runtime-evaluated aggregation arguments will appear in additional channels after all source channels
            // measure computations will use a different instance of WindowIndex than the label evaluations
            firstUnusedChannel = source.getLayout().values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            ImmutableList.Builder<ArgumentComputationSupplier> measureComputationsAggregationArguments = ImmutableList.builder();

            ImmutableList.Builder<MeasureComputationSupplier> measuresBuilder = ImmutableList.builder();
            for (Measure measure : node.getMeasures().values()) {
                ExpressionAndValuePointers expressionAndValuePointers = measure.getExpressionAndValuePointers();

                // compile the rewritten expression
                Supplier<PageProjection> pageProjectionSupplier = prepareProjection(expressionAndValuePointers, context);

                // prepare physical value accessors to provide input for the expression
                ValueAccessors valueAccessors = preparePhysicalValuePointers(expressionAndValuePointers, mapping, source, connectorSession, context, firstUnusedChannel, matchAggregationIndex);

                firstUnusedChannel = valueAccessors.getFirstUnusedChannel();
                matchAggregationIndex = valueAccessors.getAggregationIndex();

                // record aggregations
                measureComputationsAggregations.addAll(valueAccessors.getAggregations());

                // record aggregation argument computations
                measureComputationsAggregationArguments.addAll(valueAccessors.getAggregationArguments());

                // build measure computation
                measuresBuilder.add(new MeasureComputationSupplier(pageProjectionSupplier, valueAccessors.getValueAccessors(), measure.getType(), labelNames, connectorSession));
            }
            List<MeasureComputationSupplier> measureComputations = measuresBuilder.build();

            // 6. prepare SKIP TO navigation
            Optional<LogicalIndexNavigation> skipToNavigation = node.getSkipToLabel().map(label -> {
                Set<IrLabel> labels = node.getSubsets().get(label);
                if (labels == null) {
                    labels = ImmutableSet.of(label);
                }
                boolean last = node.getSkipToPosition().equals(LAST);
                return new LogicalIndexPointer(labels, last, false, 0, 0).toLogicalIndexNavigation(mapping);
            });

            // 7. pass additional info like: rowsPerMatch, skipToPosition, initial to the WindowPartition factory supplier
            PartitionerSupplier partitionerSupplier = new PatternRecognitionPartitionerSupplier(
                    measureComputations,
                    measureComputationsAggregations.build(),
                    measureComputationsAggregationArguments.build(),
                    frame,
                    node.getRowsPerMatch(),
                    skipToNavigation,
                    node.getSkipToPosition(),
                    node.isInitial(),
                    new Matcher(program, evaluationsValuePointers.build(), aggregationsLabelDependencies.build(), labelEvaluationsAggregations.build()),
                    labelEvaluations,
                    labelEvaluationsAggregationArguments.build(),
                    labelNames);

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctionsBuilder.build(),
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    10_000,
                    pagesIndexFactory,
                    isSpillEnabled(session),
                    spillerFactory,
                    orderingCompiler,
                    node.getMeasures().values().stream()
                            .map(Measure::getType)
                            .collect(toImmutableList()),
                    partitionerSupplier);

            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), context, source);
        }

        private Supplier<PageProjection> prepareProjection(ExpressionAndValuePointers expressionAndValuePointers, LocalExecutionPlanContext context)
        {
            Expression rewritten = expressionAndValuePointers.getExpression();
            List<Symbol> inputSymbols = expressionAndValuePointers.getLayout();
            List<ValuePointer> valuePointers = expressionAndValuePointers.getValuePointers();
            Set<Symbol> classifierSymbols = expressionAndValuePointers.getClassifierSymbols();
            Set<Symbol> matchNumberSymbols = expressionAndValuePointers.getMatchNumberSymbols();

            // prepare input layout and type provider for compilation
            ImmutableMap.Builder<Symbol, Type> inputTypes = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Integer> inputLayout = ImmutableMap.builder();
            for (int i = 0; i < inputSymbols.size(); i++) {
                if (classifierSymbols.contains(inputSymbols.get(i))) {
                    inputTypes.put(inputSymbols.get(i), VARCHAR);
                }
                else if (matchNumberSymbols.contains(inputSymbols.get(i))) {
                    inputTypes.put(inputSymbols.get(i), BIGINT);
                }
                else {
                    ValuePointer pointer = valuePointers.get(i);
                    if (pointer instanceof ScalarValuePointer) {
                        ScalarValuePointer scalar = (ScalarValuePointer) pointer;
                        inputTypes.put(inputSymbols.get(i), context.getTypes().get(scalar.getInputSymbol()));
                    }
                    else {
                        AggregationValuePointer aggregation = (AggregationValuePointer) pointer;
                        inputTypes.put(inputSymbols.get(i), aggregation.getFunction().getSignature().getReturnType());
                    }
                }
                inputLayout.put(inputSymbols.get(i), i);
            }

            // compile expression using input layout and input types
            RowExpression rowExpression = toRowExpression(rewritten, typeAnalyzer.getTypes(session, TypeProvider.viewOf(inputTypes.buildOrThrow()), rewritten), inputLayout.buildOrThrow());
            return pageFunctionCompiler.compileProjection(rowExpression, Optional.empty());
        }

        private ValueAccessors preparePhysicalValuePointers(
                ExpressionAndValuePointers expressionAndValuePointers,
                Map<IrLabel, Integer> mapping,
                PhysicalOperation source,
                ConnectorSession connectorSession,
                LocalExecutionPlanContext context,
                int firstUnusedChannel,
                int matchAggregationIndex)
        {
            Map<Symbol, Integer> sourceLayout = source.getLayout();

            ImmutableList.Builder<MatchAggregationInstantiator> matchAggregations = ImmutableList.builder();

            // runtime-evaluated aggregation arguments mapped to free channel slots
            ImmutableList.Builder<ArgumentComputationSupplier> aggregationArguments = ImmutableList.builder();

            // for thread equivalence
            ImmutableList.Builder<MatchAggregationLabelDependency> labelDependencies = ImmutableList.builder();

            List<ValuePointer> valuePointers = expressionAndValuePointers.getValuePointers();
            Set<Symbol> classifierSymbols = expressionAndValuePointers.getClassifierSymbols();
            Set<Symbol> matchNumberSymbols = expressionAndValuePointers.getMatchNumberSymbols();

            ImmutableList.Builder<PhysicalValueAccessor> valueAccessors = ImmutableList.builder();
            for (ValuePointer valuePointer : valuePointers) {
                if (valuePointer instanceof ScalarValuePointer) {
                    ScalarValuePointer pointer = (ScalarValuePointer) valuePointer;
                    if (classifierSymbols.contains(pointer.getInputSymbol())) {
                        valueAccessors.add(new PhysicalValuePointer(
                                CLASSIFIER,
                                VARCHAR,
                                pointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
                    }
                    else if (matchNumberSymbols.contains(pointer.getInputSymbol())) {
                        valueAccessors.add(new PhysicalValuePointer(
                                MATCH_NUMBER,
                                BIGINT,
                                pointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
                    }
                    else {
                        valueAccessors.add(new PhysicalValuePointer(
                                getOnlyElement(getChannelsForSymbols(ImmutableList.of(pointer.getInputSymbol()), sourceLayout)),
                                context.getTypes().get(pointer.getInputSymbol()),
                                pointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
                    }
                }
                else {
                    AggregationValuePointer pointer = (AggregationValuePointer) valuePointer;

                    boolean classifierInvolved = false;

                    AggregationMetadata aggregationMetadata = metadata.getAggregateFunctionImplementation(pointer.getFunction());

                    ImmutableList.Builder<Map.Entry<Expression, Type>> builder = ImmutableList.builder();
                    List<Type> signatureTypes = pointer.getFunction().getSignature().getArgumentTypes();
                    for (int i = 0; i < pointer.getArguments().size(); i++) {
                        builder.add(new SimpleEntry<>(pointer.getArguments().get(i), signatureTypes.get(i)));
                    }
                    Map<Boolean, List<Map.Entry<Expression, Type>>> arguments = builder.build().stream()
                            .collect(partitioningBy(entry -> entry.getKey() instanceof LambdaExpression));

                    // handle lambda arguments
                    List<LambdaExpression> lambdaExpressions = arguments.get(true).stream()
                            .map(Map.Entry::getKey)
                            .map(LambdaExpression.class::cast)
                            .collect(toImmutableList());

                    List<FunctionType> functionTypes = pointer.getFunction().getSignature().getArgumentTypes().stream()
                            .filter(FunctionType.class::isInstance)
                            .map(FunctionType.class::cast)
                            .collect(toImmutableList());

                    // TODO when we support lambda arguments: lambda cannot have runtime-evaluated symbols -- add check in the Analyzer
                    List<LambdaProvider> lambdaProviders = makeLambdaProviders(lambdaExpressions, aggregationMetadata.getLambdaInterfaces(), functionTypes);

                    // handle non-lambda arguments
                    List<Integer> valueChannels = new ArrayList<>();

                    Symbol classifierArgumentSymbol = pointer.getClassifierSymbol();
                    Symbol matchNumberArgumentSymbol = pointer.getMatchNumberSymbol();
                    Set<Symbol> runtimeEvaluatedSymbols = ImmutableSet.of(classifierArgumentSymbol, matchNumberArgumentSymbol);

                    for (Map.Entry<Expression, Type> argumentWithType : arguments.get(false)) {
                        Expression argument = argumentWithType.getKey();
                        boolean isRuntimeEvaluated = !(argument instanceof SymbolReference) || runtimeEvaluatedSymbols.contains(Symbol.from(argument));
                        if (isRuntimeEvaluated) {
                            List<Symbol> argumentInputSymbols = ImmutableList.copyOf(SymbolsExtractor.extractUnique(argument));
                            Supplier<PageProjection> argumentProjectionSupplier = prepareArgumentProjection(argument, argumentInputSymbols, classifierArgumentSymbol, matchNumberArgumentSymbol, context);

                            List<Integer> argumentInputChannels = new ArrayList<>();
                            for (Symbol symbol : argumentInputSymbols) {
                                if (symbol.equals(classifierArgumentSymbol)) {
                                    classifierInvolved = true;
                                    argumentInputChannels.add(CLASSIFIER);
                                }
                                else if (symbol.equals(matchNumberArgumentSymbol)) {
                                    argumentInputChannels.add(MATCH_NUMBER);
                                }
                                else {
                                    argumentInputChannels.add(sourceLayout.get(symbol));
                                }
                            }

                            Type argumentType = argumentWithType.getValue();
                            ArgumentComputationSupplier argumentComputationSupplier = new ArgumentComputationSupplier(argumentProjectionSupplier, argumentType, argumentInputChannels, connectorSession);
                            aggregationArguments.add(argumentComputationSupplier);

                            // the runtime-evaluated argument will appear in an extra channel after all input channels
                            valueChannels.add(firstUnusedChannel);
                            firstUnusedChannel++;
                        }
                        else {
                            valueChannels.add(sourceLayout.get(Symbol.from(argument)));
                        }
                    }

                    matchAggregations.add(new MatchAggregationInstantiator(
                            pointer.getFunction().getSignature(),
                            aggregationMetadata,
                            pointer.getFunction().getFunctionNullability(),
                            valueChannels,
                            lambdaProviders,
                            new SetEvaluatorSupplier(pointer.getSetDescriptor(), mapping)));
                    labelDependencies.add(new MatchAggregationLabelDependency(
                            pointer.getSetDescriptor().getLabels().stream()
                                    .map(mapping::get)
                                    .collect(toImmutableSet()),
                            classifierInvolved));
                    valueAccessors.add(new MatchAggregationPointer(matchAggregationIndex));
                    matchAggregationIndex++;
                }
            }

            return new ValueAccessors(valueAccessors.build(), matchAggregations.build(), matchAggregationIndex, aggregationArguments.build(), firstUnusedChannel, labelDependencies.build());
        }

        private Supplier<PageProjection> prepareArgumentProjection(Expression argument, List<Symbol> inputSymbols, Symbol classifierSymbol, Symbol matchNumberSymbol, LocalExecutionPlanContext context)
        {
            // prepare input layout and type provider for compilation
            ImmutableMap.Builder<Symbol, Type> inputTypes = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, Integer> inputLayout = ImmutableMap.builder();
            for (int i = 0; i < inputSymbols.size(); i++) {
                if (inputSymbols.get(i).equals(classifierSymbol)) {
                    inputTypes.put(inputSymbols.get(i), VARCHAR);
                }
                else if (inputSymbols.get(i).equals(matchNumberSymbol)) {
                    inputTypes.put(inputSymbols.get(i), BIGINT);
                }
                else {
                    inputTypes.put(inputSymbols.get(i), context.getTypes().get(inputSymbols.get(i)));
                }
                inputLayout.put(inputSymbols.get(i), i);
            }

            // compile expression using input layout and input types
            RowExpression rowExpression = toRowExpression(argument, typeAnalyzer.getTypes(session, TypeProvider.viewOf(inputTypes.buildOrThrow()), argument), inputLayout.buildOrThrow());
            return pageFunctionCompiler.compileProjection(rowExpression, Optional.empty());
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortChannels.add(source.getLayout().get(symbol));
                sortOrders.add(node.getOrderingScheme().getOrdering(symbol));
            }

            OperatorFactory operator = TopNOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    (int) node.getCount(),
                    sortChannels,
                    sortOrders,
                    plannerContext.getTypeOperators());

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().getOrderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderingScheme().getOrdering(symbol));
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            boolean spillEnabled = isSpillEnabled(session);

            OperatorFactory operator = new OrderByOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    10_000,
                    orderByChannels,
                    sortOrder.build(),
                    pagesIndexFactory,
                    spillEnabled,
                    Optional.of(spillerFactory),
                    orderingCompiler);

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            // Limit with ties should be rewritten at this point
            checkState(node.getTiesResolvingScheme().isEmpty(), "Limit with ties not supported");

            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            List<Integer> distinctChannels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());

            OperatorFactory operatorFactory = new DistinctLimitOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    distinctChannels,
                    node.getLimit(),
                    hashChannel,
                    joinCompiler,
                    blockTypeOperators);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitGroupId(GroupIdNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Symbol, Integer> newLayout = new HashMap<>();
            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();

            int outputChannel = 0;

            for (Symbol output : node.getGroupingSets().stream().flatMap(Collection::stream).collect(Collectors.toSet())) {
                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(source.getLayout().get(node.getGroupingColumns().get(output))));
            }

            Map<Symbol, Integer> argumentMappings = new HashMap<>();
            for (Symbol output : node.getAggregationArguments()) {
                int inputChannel = source.getLayout().get(output);

                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(inputChannel));
                argumentMappings.put(output, inputChannel);
            }

            // for every grouping set, create a mapping of all output to input channels (including arguments)
            ImmutableList.Builder<Map<Integer, Integer>> mappings = ImmutableList.builder();
            for (List<Symbol> groupingSet : node.getGroupingSets()) {
                ImmutableMap.Builder<Integer, Integer> setMapping = ImmutableMap.builder();

                for (Symbol output : groupingSet) {
                    setMapping.put(newLayout.get(output), source.getLayout().get(node.getGroupingColumns().get(output)));
                }

                for (Symbol output : argumentMappings.keySet()) {
                    setMapping.put(newLayout.get(output), argumentMappings.get(output));
                }

                mappings.add(setMapping.buildOrThrow());
            }

            newLayout.put(node.getGroupIdSymbol(), outputChannel);
            outputTypes.add(BIGINT);

            OperatorFactory groupIdOperatorFactory = new GroupIdOperator.GroupIdOperatorFactory(context.getNextOperatorId(),
                    node.getId(),
                    outputTypes.build(),
                    mappings.build());

            return new PhysicalOperation(groupIdOperatorFactory, newLayout, context, source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupingKeys().isEmpty()) {
                return planGlobalAggregation(node, source, context);
            }

            boolean spillEnabled = isSpillEnabled(session);
            DataSize unspillMemoryLimit = getAggregationOperatorUnspillMemoryLimit(session);

            return planGroupByAggregation(node, source, spillEnabled, unspillMemoryLimit, context);
        }

        @Override
        public PhysicalOperation visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> channels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());
            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), channels, hashChannel, joinCompiler, blockTypeOperators);
            return new PhysicalOperation(operator, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitSample(SampleNode node, LocalExecutionPlanContext context)
        {
            // For system sample, the splits are already filtered out, so no specific action needs to be taken here
            if (node.getSampleType() == SampleNode.Type.SYSTEM) {
                return node.getSource().accept(this, context);
            }

            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode = node.getSource();

            if (node.getSource() instanceof TableScanNode && getStaticFilter(node.getPredicate()).isEmpty()) {
                // filter node contains only dynamic filter, fallback to normal table scan
                return visitTableScan((TableScanNode) node.getSource(), node.getPredicate(), context);
            }

            Expression filterExpression = node.getPredicate();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, Optional.of(filterExpression), Assignments.identity(outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Optional<Expression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                sourceNode = filterNode.getSource();
                filterExpression = Optional.of(filterNode.getPredicate());
            }
            else {
                sourceNode = node.getSource();
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(), outputSymbols);
        }

        // TODO: This should be refactored, so that there's an optimizer that merges scan-filter-project into a single PlanNode
        private PhysicalOperation visitScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Optional<Expression> filterExpression,
                Assignments assignments,
                List<Symbol> outputSymbols)
        {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;
                table = tableScanNode.getTable();

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    channel++;
                }
            }
            //TODO: This is a simple hack, it will be replaced when we add ability to push down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode) {
                SampleNode sampleNode = (SampleNode) sourceNode;
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported", sampleNode.getSampleType());
                return visitScanFilterAndProject(context,
                        planNodeId,
                        sampleNode.getSource(),
                        filterExpression,
                        assignments,
                        outputSymbols);
            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.buildOrThrow();

            Optional<Expression> staticFilters = filterExpression.flatMap(this::getStaticFilter);
            DynamicFilter dynamicFilter = filterExpression
                    .filter(expression -> sourceNode instanceof TableScanNode)
                    .map(expression -> getDynamicFilter((TableScanNode) sourceNode, expression, context))
                    .orElse(DynamicFilter.EMPTY);

            List<Expression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(
                    session,
                    context.getTypes(),
                    concat(staticFilters.map(ImmutableList::of).orElse(ImmutableList.of()), assignments.getExpressions()));

            Optional<RowExpression> translatedFilter = staticFilters.map(filter -> toRowExpression(filter, expressionTypes, sourceLayout));
            List<RowExpression> translatedProjections = projections.stream()
                    .map(expression -> toRowExpression(expression, expressionTypes, sourceLayout))
                    .collect(toImmutableList());

            try {
                if (columns != null) {
                    Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter, translatedProjections, sourceNode.getId());
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode.getId(),
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            table,
                            columns,
                            dynamicFilter,
                            getTypes(projections, expressionTypes),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
                }
                else {
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(translatedFilter, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    OperatorFactory operatorFactory = FilterAndProjectOperator.createOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            pageProcessor,
                            getTypes(projections, expressionTypes),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, source);
                }
            }
            catch (TrinoException e) {
                throw e;
            }
            catch (RuntimeException e) {
                throw new TrinoException(COMPILER_ERROR, "Compiler failed", e);
            }
        }

        private RowExpression toRowExpression(Expression expression, Map<NodeRef<Expression>, Type> types, Map<Symbol, Integer> layout)
        {
            return SqlToRowExpressionTranslator.translate(expression, types, layout, metadata, session, true);
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            return visitTableScan(node, TRUE_LITERAL, context);
        }

        private PhysicalOperation visitTableScan(TableScanNode node, Expression filterExpression, LocalExecutionPlanContext context)
        {
            List<ColumnHandle> columns = new ArrayList<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
            }

            DynamicFilter dynamicFilter = getDynamicFilter(node, filterExpression, context);
            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getNextOperatorId(), node.getId(), pageSourceProvider, node.getTable(), columns, dynamicFilter);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
        }

        private Optional<Expression> getStaticFilter(Expression filterExpression)
        {
            DynamicFilters.ExtractResult extractDynamicFilterResult = extractDynamicFilters(filterExpression);
            Expression staticFilter = combineConjuncts(metadata, extractDynamicFilterResult.getStaticConjuncts());
            if (staticFilter.equals(TRUE_LITERAL)) {
                return Optional.empty();
            }
            return Optional.of(staticFilter);
        }

        private DynamicFilter getDynamicFilter(
                TableScanNode tableScanNode,
                Expression filterExpression,
                LocalExecutionPlanContext context)
        {
            DynamicFilters.ExtractResult extractDynamicFilterResult = extractDynamicFilters(filterExpression);
            List<DynamicFilters.Descriptor> dynamicFilters = extractDynamicFilterResult.getDynamicConjuncts();
            if (dynamicFilters.isEmpty()) {
                return DynamicFilter.EMPTY;
            }

            log.debug("[TableScan] Dynamic filters: %s", dynamicFilters);
            context.registerCoordinatorDynamicFilters(dynamicFilters);
            return context.getDynamicFiltersCollector().createDynamicFilter(
                    dynamicFilters,
                    tableScanNode.getAssignments(),
                    context.getTypes(),
                    metadata,
                    plannerContext.getTypeOperators());
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            // a values node must have a single driver
            context.setDriverInstanceCount(1);

            if (node.getRowCount() == 0) {
                OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of());
                return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
            }

            List<Type> outputTypes = getSymbolTypes(node.getOutputSymbols(), context.getTypes());
            PageBuilder pageBuilder = new PageBuilder(node.getRowCount(), outputTypes);
            for (int i = 0; i < node.getRowCount(); i++) {
                // declare position for every row
                pageBuilder.declarePosition();
                // evaluate values for non-empty rows
                if (node.getRows().isPresent()) {
                    Expression row = node.getRows().get().get(i);
                    Map<NodeRef<Expression>, Type> types = typeAnalyzer.getTypes(session, TypeProvider.empty(), row);
                    checkState(types.get(NodeRef.of(row)) instanceof RowType, "unexpected type of Values row: %s", types);
                    // evaluate the literal value
                    Object result = new ExpressionInterpreter(row, plannerContext, session, types).evaluate();
                    for (int j = 0; j < outputTypes.size(); j++) {
                        // divide row into fields
                        writeNativeValue(outputTypes.get(j), pageBuilder.getBlockBuilder(j), readNativeValue(outputTypes.get(j), (SingleRowBlock) result, j));
                    }
                }
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of(pageBuilder.build()));
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitUnnest(UnnestNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableList.Builder<Type> replicateTypes = ImmutableList.builder();
            for (Symbol symbol : node.getReplicateSymbols()) {
                replicateTypes.add(context.getTypes().get(symbol));
            }

            List<Symbol> unnestSymbols = node.getMappings().stream()
                    .map(UnnestNode.Mapping::getInput)
                    .collect(toImmutableList());

            ImmutableList.Builder<Type> unnestTypes = ImmutableList.builder();
            for (Symbol symbol : unnestSymbols) {
                unnestTypes.add(context.getTypes().get(symbol));
            }
            Optional<Symbol> ordinalitySymbol = node.getOrdinalitySymbol();
            Optional<Type> ordinalityType = ordinalitySymbol.map(context.getTypes()::get);
            ordinalityType.ifPresent(type -> checkState(type.equals(BIGINT), "Type of ordinalitySymbol must always be BIGINT."));

            List<Integer> replicateChannels = getChannelsForSymbols(node.getReplicateSymbols(), source.getLayout());
            List<Integer> unnestChannels = getChannelsForSymbols(unnestSymbols, source.getLayout());

            // Source channels are always laid out first, followed by the unnested symbols
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getReplicateSymbols()) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            for (UnnestNode.Mapping mapping : node.getMappings()) {
                for (Symbol unnestedSymbol : mapping.getOutputs()) {
                    outputMappings.put(unnestedSymbol, channel);
                    channel++;
                }
            }

            if (ordinalitySymbol.isPresent()) {
                outputMappings.put(ordinalitySymbol.get(), channel);
                channel++;
            }
            boolean outer = node.getJoinType() == LEFT || node.getJoinType() == FULL;
            OperatorFactory operatorFactory = new UnnestOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    replicateChannels,
                    replicateTypes.build(),
                    unnestChannels,
                    unnestTypes.build(),
                    ordinalityType.isPresent(),
                    outer);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), context, source);
        }

        private ImmutableMap<Symbol, Integer> makeLayout(PlanNode node)
        {
            return makeLayoutFromOutputSymbols(node.getOutputSymbols());
        }

        private ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : outputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            return outputMappings.buildOrThrow();
        }

        @Override
        public PhysicalOperation visitIndexSource(IndexSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(context.getIndexSourceContext().isPresent(), "Must be in an index source context");
            IndexSourceContext indexSourceContext = context.getIndexSourceContext().get();

            SetMultimap<Symbol, Integer> indexLookupToProbeInput = indexSourceContext.getIndexLookupToProbeInput();
            checkState(indexLookupToProbeInput.keySet().equals(node.getLookupSymbols()));

            // Finalize the symbol lookup layout for the index source
            List<Symbol> lookupSymbolSchema = ImmutableList.copyOf(node.getLookupSymbols());

            // Identify how to remap the probe key Input to match the source index lookup layout
            ImmutableList.Builder<Integer> remappedProbeKeyChannelsBuilder = ImmutableList.builder();
            // Identify overlapping fields that can produce the same lookup symbol.
            // We will filter incoming keys to ensure that overlapping fields will have the same value.
            ImmutableList.Builder<Set<Integer>> overlappingFieldSetsBuilder = ImmutableList.builder();
            for (Symbol lookupSymbol : lookupSymbolSchema) {
                Set<Integer> potentialProbeInputs = indexLookupToProbeInput.get(lookupSymbol);
                checkState(!potentialProbeInputs.isEmpty(), "Must have at least one source from the probe input");
                if (potentialProbeInputs.size() > 1) {
                    overlappingFieldSetsBuilder.add(potentialProbeInputs.stream().collect(toImmutableSet()));
                }
                remappedProbeKeyChannelsBuilder.add(Iterables.getFirst(potentialProbeInputs, null));
            }
            List<Set<Integer>> overlappingFieldSets = overlappingFieldSetsBuilder.build();
            List<Integer> remappedProbeKeyChannels = remappedProbeKeyChannelsBuilder.build();
            Function<RecordSet, RecordSet> probeKeyNormalizer = recordSet -> {
                if (!overlappingFieldSets.isEmpty()) {
                    recordSet = new FieldSetFilteringRecordSet(plannerContext.getTypeOperators(), recordSet, overlappingFieldSets);
                }
                return new MappedRecordSet(recordSet, remappedProbeKeyChannels);
            };

            // Declare the input and output schemas for the index and acquire the actual Index
            List<ColumnHandle> lookupSchema = Lists.transform(lookupSymbolSchema, forMap(node.getAssignments()));
            List<ColumnHandle> outputSchema = Lists.transform(node.getOutputSymbols(), forMap(node.getAssignments()));
            ConnectorIndex index = indexManager.getIndex(session, node.getIndexHandle(), lookupSchema, outputSchema);

            OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(context.getNextOperatorId(), node.getId(), index, probeKeyNormalizer);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        /**
         * This method creates a mapping from each index source lookup symbol (directly applied to the index)
         * to the corresponding probe key Input
         */
        private SetMultimap<Symbol, Integer> mapIndexSourceLookupSymbolToProbeKeyInput(IndexJoinNode node, Map<Symbol, Integer> probeKeyLayout)
        {
            Set<Symbol> indexJoinSymbols = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());

            // Trace the index join symbols to the index source lookup symbols
            // Map: Index join symbol => Index source lookup symbol
            Map<Symbol, Symbol> indexKeyTrace = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), indexJoinSymbols);

            // Map the index join symbols to the probe key Input
            Multimap<Symbol, Integer> indexToProbeKeyInput = HashMultimap.create();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                indexToProbeKeyInput.put(clause.getIndex(), probeKeyLayout.get(clause.getProbe()));
            }

            // Create the mapping from index source look up symbol to probe key Input
            ImmutableSetMultimap.Builder<Symbol, Integer> builder = ImmutableSetMultimap.builder();
            for (Map.Entry<Symbol, Symbol> entry : indexKeyTrace.entrySet()) {
                Symbol indexJoinSymbol = entry.getKey();
                Symbol indexLookupSymbol = entry.getValue();
                builder.putAll(indexLookupSymbol, indexToProbeKeyInput.get(indexJoinSymbol));
            }
            return builder.build();
        }

        @Override
        public PhysicalOperation visitIndexJoin(IndexJoinNode node, LocalExecutionPlanContext context)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> probeSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe);
            List<Symbol> indexSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex);

            // Plan probe side
            PhysicalOperation probeSource = node.getProbeSource().accept(this, context);
            List<Integer> probeChannels = getChannelsForSymbols(probeSymbols, probeSource.getLayout());
            OptionalInt probeHashChannel = node.getProbeHashSymbol().map(channelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // The probe key channels will be handed to the index according to probeSymbol order
            Map<Symbol, Integer> probeKeyLayout = new HashMap<>();
            for (int i = 0; i < probeSymbols.size(); i++) {
                // Duplicate symbols can appear and we only need to take one of the Inputs
                probeKeyLayout.put(probeSymbols.get(i), i);
            }

            // Plan the index source side
            SetMultimap<Symbol, Integer> indexLookupToProbeInput = mapIndexSourceLookupSymbolToProbeKeyInput(node, probeKeyLayout);
            LocalExecutionPlanContext indexContext = context.createIndexSourceSubContext(new IndexSourceContext(indexLookupToProbeInput));
            PhysicalOperation indexSource = node.getIndexSource().accept(this, indexContext);
            List<Integer> indexOutputChannels = getChannelsForSymbols(indexSymbols, indexSource.getLayout());
            OptionalInt indexHashChannel = node.getIndexHashSymbol().map(channelGetter(indexSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // Identify just the join keys/channels needed for lookup by the index source (does not have to use all of them).
            Set<Symbol> indexSymbolsNeededBySource = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), ImmutableSet.copyOf(indexSymbols)).keySet();

            Set<Integer> lookupSourceInputChannels = node.getCriteria().stream()
                    .filter(equiJoinClause -> indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .map(probeKeyLayout::get)
                    .collect(toImmutableSet());

            Optional<DynamicTupleFilterFactory> dynamicTupleFilterFactory = Optional.empty();
            if (lookupSourceInputChannels.size() < probeKeyLayout.values().size()) {
                int[] nonLookupInputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getProbe)
                        .map(probeKeyLayout::get)
                        .collect(toImmutableList()));
                int[] nonLookupOutputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getIndex)
                        .map(indexSource.getLayout()::get)
                        .collect(toImmutableList()));

                int filterOperatorId = indexContext.getNextOperatorId();
                dynamicTupleFilterFactory = Optional.of(new DynamicTupleFilterFactory(
                        filterOperatorId,
                        node.getId(),
                        nonLookupInputChannels,
                        nonLookupOutputChannels,
                        indexSource.getTypes(),
                        pageFunctionCompiler,
                        blockTypeOperators));
            }

            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                    indexContext.getNextPipelineId(),
                    indexContext.getNextOperatorId(),
                    node.getId(),
                    indexContext.isInputDriver(),
                    indexSource.getTypes(),
                    indexSource.getOperatorFactories(),
                    dynamicTupleFilterFactory);

            IndexLookupSourceFactory indexLookupSourceFactory = new IndexLookupSourceFactory(
                    lookupSourceInputChannels,
                    indexOutputChannels,
                    indexHashChannel,
                    indexSource.getTypes(),
                    indexBuildDriverFactoryProvider,
                    maxIndexMemorySize,
                    indexJoinLookupStats,
                    SystemSessionProperties.isShareIndexLoading(session),
                    pagesIndexFactory,
                    joinCompiler,
                    blockTypeOperators);

            verify(probeSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            verify(indexSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            JoinBridgeManager<LookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    false,
                    UNGROUPED_EXECUTION,
                    UNGROUPED_EXECUTION,
                    lifespan -> {
                        indexLookupSourceFactory.setTaskContext(context.taskContext);
                        return indexLookupSourceFactory;
                    },
                    indexLookupSourceFactory.getOutputTypes());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from index side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : indexSource.getLayout().entrySet()) {
                Integer input = entry.getValue();
                outputMappings.put(entry.getKey(), offset + input);
            }

            OperatorFactory lookupJoinOperatorFactory;
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            switch (node.getType()) {
                case INNER:
                    lookupJoinOperatorFactory = operatorFactories.innerJoin(
                            context.getNextOperatorId(),
                            node.getId(),
                            lookupSourceFactoryManager,
                            false,
                            false,
                            false,
                            probeSource.getTypes(),
                            probeChannels,
                            probeHashChannel,
                            Optional.empty(),
                            totalOperatorsCount,
                            unsupportedPartitioningSpillerFactory(),
                            blockTypeOperators);
                    break;
                case SOURCE_OUTER:
                    lookupJoinOperatorFactory = operatorFactories.probeOuterJoin(
                            context.getNextOperatorId(),
                            node.getId(),
                            lookupSourceFactoryManager,
                            false,
                            false,
                            probeSource.getTypes(),
                            probeChannels,
                            probeHashChannel,
                            Optional.empty(),
                            totalOperatorsCount,
                            unsupportedPartitioningSpillerFactory(),
                            blockTypeOperators);
                    break;
                default:
                    throw new AssertionError("Unknown type: " + node.getType());
            }
            return new PhysicalOperation(lookupJoinOperatorFactory, outputMappings.buildOrThrow(), context, probeSource);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            // Register dynamic filters, allowing the scan operators to wait for the collection completion.
            // Skip dynamic filters that are not used locally (e.g. in case of distributed joins).
            Set<DynamicFilterId> localDynamicFilters = node.getDynamicFilters().keySet().stream()
                    .filter(getConsumedDynamicFilterIds(node.getLeft())::contains)
                    .collect(toImmutableSet());
            context.getDynamicFiltersCollector().register(localDynamicFilters);

            if (node.isCrossJoin()) {
                return createNestedLoopJoin(node, localDynamicFilters, context);
            }

            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                case RIGHT:
                case FULL:
                    return createLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(), node.getRight(), rightSymbols, node.getRightHashSymbol(), localDynamicFilters, context);
            }
            throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
        }

        @Override
        public PhysicalOperation visitSpatialJoin(SpatialJoinNode node, LocalExecutionPlanContext context)
        {
            Expression filterExpression = node.getFilter();
            List<FunctionCall> spatialFunctions = extractSupportedSpatialFunctions(filterExpression);
            for (FunctionCall spatialFunction : spatialFunctions) {
                Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialFunction), spatialFunction, Optional.empty(), Optional.empty());
                if (operation.isPresent()) {
                    return operation.get();
                }
            }

            List<ComparisonExpression> spatialComparisons = extractSupportedSpatialComparisons(filterExpression);
            for (ComparisonExpression spatialComparison : spatialComparisons) {
                if (spatialComparison.getOperator() == LESS_THAN || spatialComparison.getOperator() == LESS_THAN_OR_EQUAL) {
                    // ST_Distance(a, b) <= r
                    Expression radius = spatialComparison.getRight();
                    if (radius instanceof SymbolReference && getSymbolReferences(node.getRight().getOutputSymbols()).contains(radius)) {
                        FunctionCall spatialFunction = (FunctionCall) spatialComparison.getLeft();
                        Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialComparison), spatialFunction, Optional.of(radius), Optional.of(spatialComparison.getOperator()));
                        if (operation.isPresent()) {
                            return operation.get();
                        }
                    }
                }
            }

            throw new VerifyException("No valid spatial relationship found for spatial join");
        }

        private Optional<PhysicalOperation> tryCreateSpatialJoin(
                LocalExecutionPlanContext context,
                SpatialJoinNode node,
                Optional<Expression> filterExpression,
                FunctionCall spatialFunction,
                Optional<Expression> radius,
                Optional<ComparisonExpression.Operator> comparisonOperator)
        {
            List<Expression> arguments = spatialFunction.getArguments();
            verify(arguments.size() == 2);

            if (!(arguments.get(0) instanceof SymbolReference) || !(arguments.get(1) instanceof SymbolReference)) {
                return Optional.empty();
            }

            SymbolReference firstSymbol = (SymbolReference) arguments.get(0);
            SymbolReference secondSymbol = (SymbolReference) arguments.get(1);

            PlanNode probeNode = node.getLeft();
            Set<SymbolReference> probeSymbols = getSymbolReferences(probeNode.getOutputSymbols());

            PlanNode buildNode = node.getRight();
            Set<SymbolReference> buildSymbols = getSymbolReferences(buildNode.getOutputSymbols());

            if (probeSymbols.contains(firstSymbol) && buildSymbols.contains(secondSymbol)) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        Symbol.from(firstSymbol),
                        buildNode,
                        Symbol.from(secondSymbol),
                        radius.map(Symbol::from),
                        spatialTest(spatialFunction, true, comparisonOperator),
                        filterExpression,
                        context));
            }
            if (probeSymbols.contains(secondSymbol) && buildSymbols.contains(firstSymbol)) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        Symbol.from(secondSymbol),
                        buildNode,
                        Symbol.from(firstSymbol),
                        radius.map(Symbol::from),
                        spatialTest(spatialFunction, false, comparisonOperator),
                        filterExpression,
                        context));
            }
            return Optional.empty();
        }

        private Optional<Expression> removeExpressionFromFilter(Expression filter, Expression expression)
        {
            Expression updatedJoinFilter = replaceExpression(filter, ImmutableMap.of(expression, TRUE_LITERAL));
            return updatedJoinFilter == TRUE_LITERAL ? Optional.empty() : Optional.of(updatedJoinFilter);
        }

        private SpatialPredicate spatialTest(FunctionCall functionCall, boolean probeFirst, Optional<ComparisonExpression.Operator> comparisonOperator)
        {
            String functionName = ResolvedFunction.extractFunctionName(functionCall.getName()).toLowerCase(Locale.ENGLISH);
            switch (functionName) {
                case ST_CONTAINS:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.contains(buildGeometry);
                    }
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.contains(probeGeometry);
                case ST_WITHIN:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.within(buildGeometry);
                    }
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.within(probeGeometry);
                case ST_INTERSECTS:
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.intersects(probeGeometry);
                case ST_DISTANCE:
                    if (comparisonOperator.get() == LESS_THAN) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) < radius.getAsDouble();
                    }
                    if (comparisonOperator.get() == LESS_THAN_OR_EQUAL) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) <= radius.getAsDouble();
                    }
                    throw new UnsupportedOperationException("Unsupported comparison operator: " + comparisonOperator.get());
                default:
                    throw new UnsupportedOperationException("Unsupported spatial function: " + functionName);
            }
        }

        private Set<SymbolReference> getSymbolReferences(Collection<Symbol> symbols)
        {
            return symbols.stream().map(Symbol::toSymbolReference).collect(toImmutableSet());
        }

        private PhysicalOperation createNestedLoopJoin(JoinNode node, Set<DynamicFilterId> localDynamicFilters, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getLeft().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getRight().accept(this, buildContext);

            checkState(
                    buildSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION,
                    "Build source of a nested loop join is expected to be UNGROUPED_EXECUTION.");
            checkArgument(node.getType() == INNER, "NestedLoopJoin is only used for inner join");

            JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                    false,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new NestedLoopJoinPagesSupplier(),
                    buildSource.getTypes());
            NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    nestedLoopJoinBridgeManager);

            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int operatorId = buildContext.getNextOperatorId();
            Optional<LocalDynamicFilterConsumer> localDynamicFilter = createDynamicFilter(buildSource, node, context, partitionCount, localDynamicFilters);
            if (localDynamicFilter.isPresent()) {
                buildSource = createDynamicFilterSourceOperatorFactory(operatorId, localDynamicFilter.get(), node, buildSource, buildContext);
            }

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    new PhysicalOperation(nestedLoopBuildOperatorFactory, buildSource),
                    buildContext.getDriverInstanceCount());

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            List<Integer> probeChannels = getChannelsForSymbols(node.getLeftOutputSymbols(), probeSource.getLayout());
            List<Integer> buildChannels = getChannelsForSymbols(node.getRightOutputSymbols(), buildSource.getLayout());

            OperatorFactory operatorFactory = new NestedLoopJoinOperatorFactory(context.getNextOperatorId(), node.getId(), nestedLoopJoinBridgeManager, probeChannels, buildChannels);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), context, probeSource);
        }

        private PhysicalOperation createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                Symbol probeSymbol,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                SpatialPredicate spatialRelationshipTest,
                Optional<Expression> joinFilter,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            PagesSpatialIndexFactory pagesSpatialIndexFactory = createPagesSpatialIndexFactory(node,
                    buildNode,
                    buildSymbol,
                    radiusSymbol,
                    probeSource.getLayout(),
                    spatialRelationshipTest,
                    joinFilter,
                    context);

            OperatorFactory operator = createSpatialLookupJoin(node, probeNode, probeSource, probeSymbol, pagesSpatialIndexFactory, context);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), context, probeSource);
        }

        private OperatorFactory createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                PhysicalOperation probeSource,
                Symbol probeSymbol,
                PagesSpatialIndexFactory pagesSpatialIndexFactory,
                LocalExecutionPlanContext context)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> probeNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            Function<Symbol, Integer> probeChannelGetter = channelGetter(probeSource);
            int probeChannel = probeChannelGetter.apply(probeSymbol);

            Optional<Integer> partitionChannel = node.getLeftPartitionSymbol().map(probeChannelGetter);

            return new SpatialJoinOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getType(),
                    probeTypes,
                    probeOutputChannels,
                    probeChannel,
                    partitionChannel,
                    pagesSpatialIndexFactory);
        }

        private PagesSpatialIndexFactory createPagesSpatialIndexFactory(
                SpatialJoinNode node,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                Map<Symbol, Integer> probeLayout,
                SpatialPredicate spatialRelationshipTest,
                Optional<Expression> joinFilter,
                LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);
            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> buildNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildLayout));
            Function<Symbol, Integer> buildChannelGetter = channelGetter(buildSource);
            Integer buildChannel = buildChannelGetter.apply(buildSymbol);
            Optional<Integer> radiusChannel = radiusSymbol.map(buildChannelGetter);

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = joinFilter
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeLayout,
                            buildLayout,
                            context.getTypes(),
                            session));

            Optional<Integer> partitionChannel = node.getRightPartitionSymbol().map(buildChannelGetter);

            SpatialIndexBuilderOperatorFactory builderOperatorFactory = new SpatialIndexBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes(),
                    buildOutputChannels,
                    buildChannel,
                    radiusChannel,
                    partitionChannel,
                    spatialRelationshipTest,
                    node.getKdbTree(),
                    filterFunctionFactory,
                    10_000,
                    pagesIndexFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    new PhysicalOperation(builderOperatorFactory, buildSource),
                    buildContext.getDriverInstanceCount());

            return builderOperatorFactory.getPagesSpatialIndexFactory();
        }

        private PhysicalOperation createLookupJoin(
                JoinNode node,
                PlanNode probeNode,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                Set<DynamicFilterId> localDynamicFilters,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            boolean spillEnabled = isSpillEnabled(session)
                    && node.isSpillable().orElseThrow(() -> new IllegalArgumentException("spillable not yet set"))
                    && probeSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION
                    && !buildOuter;
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory =
                    createLookupSourceFactory(node, buildNode, buildSymbols, buildHashSymbol, probeSource, context, spillEnabled, localDynamicFilters);

            OperatorFactory operator = createLookupJoin(
                    node,
                    probeSource,
                    probeSymbols,
                    probeHashSymbol,
                    lookupSourceFactory,
                    context,
                    spillEnabled,
                    !localDynamicFilters.isEmpty());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), context, probeSource);
        }

        private JoinBridgeManager<PartitionedLookupSourceFactory> createLookupSourceFactory(
                JoinNode node,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                PhysicalOperation probeSource,
                LocalExecutionPlanContext context,
                boolean spillEnabled,
                Set<DynamicFilterId> localDynamicFilters)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            if (buildSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION) {
                checkState(
                        probeSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        "Build execution is GROUPED_EXECUTION. Probe execution is expected be GROUPED_EXECUTION, but is UNGROUPED_EXECUTION.");
            }

            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(node.getRightOutputSymbols(), buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashSymbol.map(channelGetter(buildSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);

            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildLayout,
                            context.getTypes(),
                            session));

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter()
                    .flatMap(filter -> extractSortExpression(metadata, ImmutableSet.copyOf(node.getRight().getOutputSymbols()), filter));

            Optional<Integer> sortChannel = sortExpressionContext
                    .map(SortExpressionContext::getSortExpression)
                    .map(Symbol::from)
                    .map(sortSymbol -> createJoinSourcesLayout(buildLayout, probeSource.getLayout()).get(sortSymbol));

            List<JoinFilterFunctionFactory> searchFunctionFactories = sortExpressionContext
                    .map(SortExpressionContext::getSearchExpressions)
                    .map(searchExpressions -> searchExpressions.stream()
                            .map(searchExpression -> compileJoinFilterFunction(
                                    searchExpression,
                                    probeSource.getLayout(),
                                    buildLayout,
                                    context.getTypes(),
                                    session))
                            .collect(toImmutableList()))
                    .orElse(ImmutableList.of());

            ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            List<Type> buildTypes = buildSource.getTypes();
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    buildOuter,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new PartitionedLookupSourceFactory(
                            buildTypes,
                            buildOutputTypes,
                            buildChannels.stream()
                                    .map(buildTypes::get)
                                    .collect(toImmutableList()),
                            partitionCount,
                            buildOuter,
                            blockTypeOperators),
                    buildOutputTypes);

            int operatorId = buildContext.getNextOperatorId();
            Optional<LocalDynamicFilterConsumer> localDynamicFilter = createDynamicFilter(buildSource, node, context, partitionCount, localDynamicFilters);
            if (localDynamicFilter.isPresent()) {
                buildSource = createDynamicFilterSourceOperatorFactory(operatorId, localDynamicFilter.get(), node, buildSource, buildContext);
            }

            int taskConcurrency = getTaskConcurrency(session);
            HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    lookupSourceFactoryManager,
                    buildOutputChannels,
                    buildChannels,
                    buildHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    10_000,
                    pagesIndexFactory,
                    spillEnabled && partitionCount > 1,
                    singleStreamSpillerFactory,
                    incrementalLoadFactorHashArraySizeSupplier(
                            session,
                            // scale load factor in case partition count (and number of hash build operators)
                            // is reduced (e.g. by plan rule) with respect to default task concurrency
                            taskConcurrency / partitionCount));

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    new PhysicalOperation(hashBuilderOperatorFactory, buildSource),
                    buildContext.getDriverInstanceCount());

            return lookupSourceFactoryManager;
        }

        private PhysicalOperation createDynamicFilterSourceOperatorFactory(
                int operatorId,
                LocalDynamicFilterConsumer dynamicFilter,
                JoinNode node,
                PhysicalOperation buildSource,
                LocalExecutionPlanContext context)
        {
            List<DynamicFilterSourceOperator.Channel> filterBuildChannels = dynamicFilter.getBuildChannels().entrySet().stream()
                    .map(entry -> {
                        DynamicFilterId filterId = entry.getKey();
                        int index = entry.getValue();
                        Type type = buildSource.getTypes().get(index);
                        return new DynamicFilterSourceOperator.Channel(filterId, type, index);
                    })
                    .collect(Collectors.toList());
            boolean isReplicatedJoin = isBuildSideReplicated(node);
            boolean isBuildSideSingle = context.getDriverInstanceCount().orElse(1) == 1;
            int taskConcurrency = getTaskConcurrency(session);
            return new PhysicalOperation(
                    new DynamicFilterSourceOperatorFactory(
                            operatorId,
                            node.getId(),
                            dynamicFilter.getTupleDomainConsumer(),
                            filterBuildChannels,
                            multipleIf(getDynamicFilteringMaxDistinctValuesPerDriver(session, isReplicatedJoin), taskConcurrency, isBuildSideSingle),
                            multipleIf(getDynamicFilteringMaxSizePerDriver(session, isReplicatedJoin), taskConcurrency, isBuildSideSingle),
                            multipleIf(getDynamicFilteringRangeRowLimitPerDriver(session, isReplicatedJoin), taskConcurrency, isBuildSideSingle),
                            blockTypeOperators),
                    buildSource.getLayout(),
                    context,
                    buildSource);
        }

        private int multipleIf(int value, int multiplier, boolean shouldMultiply)
        {
            return shouldMultiply ? value * multiplier : value;
        }

        private DataSize multipleIf(DataSize value, int multiplier, boolean shouldMultiply)
        {
            return shouldMultiply ? DataSize.ofBytes(value.toBytes() * multiplier) : value;
        }

        private Optional<LocalDynamicFilterConsumer> createDynamicFilter(
                PhysicalOperation buildSource,
                JoinNode node,
                LocalExecutionPlanContext context,
                int partitionCount,
                Set<DynamicFilterId> localDynamicFilters)
        {
            Set<DynamicFilterId> coordinatorDynamicFilters = getCoordinatorDynamicFilters(node.getDynamicFilters().keySet(), node, context.getTaskId());
            Set<DynamicFilterId> collectedDynamicFilters = ImmutableSet.<DynamicFilterId>builder()
                    .addAll(localDynamicFilters)
                    .addAll(coordinatorDynamicFilters)
                    .build();
            if (collectedDynamicFilters.isEmpty()) {
                return Optional.empty();
            }
            checkState(
                    buildSource.getPipelineExecutionStrategy() != GROUPED_EXECUTION,
                    "Dynamic filtering cannot be used with grouped execution");
            log.debug("[Join] Dynamic filters: %s", node.getDynamicFilters());
            LocalDynamicFilterConsumer filterConsumer = LocalDynamicFilterConsumer.create(node, buildSource.getTypes(), partitionCount, collectedDynamicFilters);
            ListenableFuture<Map<DynamicFilterId, Domain>> domainsFuture = filterConsumer.getDynamicFilterDomains();
            if (!localDynamicFilters.isEmpty()) {
                addSuccessCallback(domainsFuture, context::addLocalDynamicFilters);
            }
            if (!coordinatorDynamicFilters.isEmpty()) {
                addSuccessCallback(
                        domainsFuture,
                        domains -> context.addCoordinatorDynamicFilters(domains.entrySet().stream()
                                .filter(entry -> coordinatorDynamicFilters.contains(entry.getKey()))
                                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue))));
            }
            return Optional.of(filterConsumer);
        }

        private JoinFilterFunctionFactory compileJoinFilterFunction(
                Expression filterExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout,
                TypeProvider types,
                Session session)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);

            RowExpression translatedFilter = toRowExpression(filterExpression, typeAnalyzer.getTypes(session, types, filterExpression), joinSourcesLayout);
            return joinFilterFunctionCompiler.compileJoinFilterFunction(translatedFilter, buildLayout.size());
        }

        private OperatorFactory createLookupJoin(
                JoinNode node,
                PhysicalOperation probeSource,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                LocalExecutionPlanContext context,
                boolean spillEnabled,
                boolean consumedLocalDynamicFilters)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(node.getLeftOutputSymbols(), probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList.copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            OptionalInt probeHashChannel = probeHashSymbol.map(channelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());
            OptionalInt totalOperatorsCount = OptionalInt.empty();
            if (spillEnabled) {
                totalOperatorsCount = context.getDriverInstanceCount();
                checkState(totalOperatorsCount.isPresent(), "A fixed distribution is required for JOIN when spilling is enabled");
            }

            // Implementation of hash join operator may only take advantage of output duplicates insensitive joins when:
            // 1. Join is of INNER or LEFT type. For right or full joins all matching build rows must be tagged as visited.
            // 2. Right (build) output symbols are subset of equi-clauses right symbols. If additional build symbols
            //    are produced, then skipping build rows could skip some distinct rows.
            boolean outputSingleMatch = node.isMaySkipOutputDuplicates() &&
                    node.getCriteria().stream()
                            .map(JoinNode.EquiJoinClause::getRight)
                            .collect(toImmutableSet())
                            .containsAll(node.getRightOutputSymbols());
            // Wait for build side to be collected before local dynamic filters are
            // consumed by table scan. This way table scan can filter data more efficiently.
            boolean waitForBuild = consumedLocalDynamicFilters;
            switch (node.getType()) {
                case INNER:
                    return operatorFactories.innerJoin(
                            context.getNextOperatorId(),
                            node.getId(),
                            lookupSourceFactoryManager,
                            outputSingleMatch,
                            waitForBuild,
                            node.getFilter().isPresent(),
                            probeTypes,
                            probeJoinChannels,
                            probeHashChannel,
                            Optional.of(probeOutputChannels),
                            totalOperatorsCount,
                            partitioningSpillerFactory,
                            blockTypeOperators);
                case LEFT:
                    return operatorFactories.probeOuterJoin(
                            context.getNextOperatorId(),
                            node.getId(),
                            lookupSourceFactoryManager,
                            outputSingleMatch,
                            node.getFilter().isPresent(),
                            probeTypes,
                            probeJoinChannels,
                            probeHashChannel,
                            Optional.of(probeOutputChannels),
                            totalOperatorsCount,
                            partitioningSpillerFactory,
                            blockTypeOperators);
                case RIGHT:
                    return operatorFactories.lookupOuterJoin(
                            context.getNextOperatorId(),
                            node.getId(),
                            lookupSourceFactoryManager,
                            waitForBuild,
                            node.getFilter().isPresent(),
                            probeTypes,
                            probeJoinChannels,
                            probeHashChannel,
                            Optional.of(probeOutputChannels),
                            totalOperatorsCount,
                            partitioningSpillerFactory,
                            blockTypeOperators);
                case FULL:
                    return operatorFactories.fullOuterJoin(
                            context.getNextOperatorId(),
                            node.getId(),
                            lookupSourceFactoryManager,
                            node.getFilter().isPresent(),
                            probeTypes,
                            probeJoinChannels,
                            probeHashChannel,
                            Optional.of(probeOutputChannels),
                            totalOperatorsCount,
                            partitioningSpillerFactory,
                            blockTypeOperators);
            }
            throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
        }

        private Map<Symbol, Integer> createJoinSourcesLayout(Map<Symbol, Integer> lookupSourceLayout, Map<Symbol, Integer> probeSourceLayout)
        {
            ImmutableMap.Builder<Symbol, Integer> joinSourcesLayout = ImmutableMap.builder();
            joinSourcesLayout.putAll(lookupSourceLayout);
            for (Map.Entry<Symbol, Integer> probeLayoutEntry : probeSourceLayout.entrySet()) {
                joinSourcesLayout.put(probeLayoutEntry.getKey(), probeLayoutEntry.getValue() + lookupSourceLayout.size());
            }
            return joinSourcesLayout.buildOrThrow();
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            boolean isLocalDynamicFilter = node.getDynamicFilterId()
                    .map(filterId -> getConsumedDynamicFilterIds(node.getSource()).contains(filterId))
                    .orElse(false);
            boolean isCoordinatorDynamicFilter = node.getDynamicFilterId()
                    .map(filterId -> !getCoordinatorDynamicFilters(ImmutableSet.of(filterId), node, context.getTaskId()).isEmpty())
                    .orElse(false);
            if (isLocalDynamicFilter) {
                // Register locally if the table scan is on the same node (e.g., in case of broadcast semi-joins)
                context.getDynamicFiltersCollector().register(ImmutableSet.of(node.getDynamicFilterId().get()));
            }

            // Plan probe
            PhysicalOperation probeSource = node.getSource().accept(this, context);

            // Plan build
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);
            checkState(buildSource.getPipelineExecutionStrategy() == probeSource.getPipelineExecutionStrategy(), "build and probe have different pipelineExecutionStrategy");
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            int operatorId = buildContext.getNextOperatorId();
            if (isLocalDynamicFilter || isCoordinatorDynamicFilter) {
                // Add a DynamicFilterSourceOperatorFactory to build operator factories
                DynamicFilterId filterId = node.getDynamicFilterId().get();
                log.debug("[Semi-join] Dynamic filter: %s", filterId);
                LocalDynamicFilterConsumer filterConsumer = new LocalDynamicFilterConsumer(
                        ImmutableMap.of(filterId, buildChannel),
                        ImmutableMap.of(filterId, buildSource.getTypes().get(buildChannel)),
                        partitionCount);
                ListenableFuture<Map<DynamicFilterId, Domain>> domainsFuture = filterConsumer.getDynamicFilterDomains();
                if (isLocalDynamicFilter) {
                    addSuccessCallback(domainsFuture, context::addLocalDynamicFilters);
                }
                if (isCoordinatorDynamicFilter) {
                    addSuccessCallback(domainsFuture, context::addCoordinatorDynamicFilters);
                }
                boolean isReplicatedJoin = isBuildSideReplicated(node);
                buildSource = new PhysicalOperation(
                        new DynamicFilterSourceOperatorFactory(
                                operatorId,
                                node.getId(),
                                filterConsumer.getTupleDomainConsumer(),
                                ImmutableList.of(new DynamicFilterSourceOperator.Channel(filterId, buildSource.getTypes().get(buildChannel), buildChannel)),
                                getDynamicFilteringMaxDistinctValuesPerDriver(session, isReplicatedJoin),
                                getDynamicFilteringMaxSizePerDriver(session, isReplicatedJoin),
                                getDynamicFilteringRangeRowLimitPerDriver(session, isReplicatedJoin),
                                blockTypeOperators),
                        buildSource.getLayout(),
                        buildContext,
                        buildSource);
            }

            Optional<Integer> buildHashChannel = node.getFilteringSourceHashSymbol().map(channelGetter(buildSource));
            Optional<Integer> probeHashChannel = node.getSourceHashSymbol().map(channelGetter(probeSource));

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes().get(buildChannel),
                    buildChannel,
                    buildHashChannel,
                    10_000,
                    joinCompiler,
                    blockTypeOperators);
            SetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    new PhysicalOperation(setBuilderOperatorFactory, buildSource),
                    buildContext.getDriverInstanceCount());

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .buildOrThrow();

            OperatorFactory operator = HashSemiJoinOperator.createOperatorFactory(context.getNextOperatorId(), node.getId(), setProvider, probeSource.getTypes(), probeChannel, probeHashChannel);
            return new PhysicalOperation(operator, outputMappings, context, probeSource);
        }

        private Set<DynamicFilterId> getCoordinatorDynamicFilters(Set<DynamicFilterId> dynamicFilters, PlanNode node, TaskId taskId)
        {
            if (!isBuildSideReplicated(node) || taskId.getPartitionId() == 0) {
                // replicated dynamic filters are collected by single stage task only
                return dynamicFilters;
            }

            return ImmutableSet.of();
        }

        @Override
        public PhysicalOperation visitRefreshMaterializedView(RefreshMaterializedViewNode node, LocalExecutionPlanContext context)
        {
            context.setDriverInstanceCount(1);
            OperatorFactory operatorFactory = new RefreshMaterializedViewOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, node.getViewName());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            context.setDriverInstanceCount(getTaskWriterCount(session));

            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            PARTIAL,
                            STATS_START_CHANNEL,
                            outputMapping,
                            source,
                            context);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        PARTIAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        DataSize.ofBytes(0),
                        context,
                        STATS_START_CHANNEL,
                        outputMapping,
                        200,
                        // This aggregation must behave as INTERMEDIATE.
                        // Using INTERMEDIATE aggregation directly
                        // is not possible, as it doesn't accept raw input data.
                        // Disabling partial pre-aggregation memory limit effectively
                        // turns PARTIAL aggregation into INTERMEDIATE.
                        Optional.empty());
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            List<String> notNullChannelColumnNames = node.getColumns().stream()
                    .map(symbol -> node.getNotNullColumnSymbols().contains(symbol) ? node.getColumnNames().get(source.symbolToChannel(symbol)) : null)
                    .collect(Collectors.toList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    notNullChannelColumnNames,
                    session,
                    statisticsAggregation,
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()));

            return new PhysicalOperation(operatorFactory, outputMapping.buildOrThrow(), context, source);
        }

        @Override
        public PhysicalOperation visitStatisticsWriterNode(StatisticsWriterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            StatisticAggregationsDescriptor<Integer> descriptor = node.getDescriptor().map(symbol -> source.getLayout().get(symbol));

            OperatorFactory operatorFactory = new StatisticsWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    computedStatistics -> metadata.finishStatisticsCollection(session, ((StatisticsWriterNode.WriteStatisticsHandle) node.getTarget()).getHandle(), computedStatistics),
                    node.isRowCountEnabled(),
                    descriptor);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitTableFinish(TableFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            FINAL,
                            0,
                            outputMapping,
                            source,
                            context);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        FINAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        DataSize.ofBytes(0),
                        context,
                        0,
                        outputMapping,
                        200,
                        // final aggregation ignores partial pre-aggregation memory limit
                        Optional.empty());
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            Map<Symbol, Integer> aggregationOutput = outputMapping.buildOrThrow();
            StatisticAggregationsDescriptor<Integer> descriptor = node.getStatisticsAggregationDescriptor()
                    .map(desc -> desc.map(aggregationOutput::get))
                    .orElse(StatisticAggregationsDescriptor.empty());

            OperatorFactory operatorFactory = new TableFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    createTableFinisher(session, node, metadata),
                    statisticsAggregation,
                    descriptor,
                    tableExecuteContextManager,
                    shouldOutputRowCount(node),
                    session);
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitDelete(DeleteNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new DeleteOperatorFactory(context.getNextOperatorId(), node.getId(), source.getLayout().get(node.getRowId()));

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .buildOrThrow();

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitUpdate(UpdateNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            List<Integer> channelNumbers = createColumnValueAndRowIdChannels(node.getSource().getOutputSymbols(), node.getColumnValueAndRowIdSymbols());
            OperatorFactory operatorFactory = new UpdateOperatorFactory(context.getNextOperatorId(), node.getId(), channelNumbers);

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .buildOrThrow();

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        private List<Integer> createColumnValueAndRowIdChannels(List<Symbol> outputSymbols, List<Symbol> columnValueAndRowIdSymbols)
        {
            Integer[] columnValueAndRowIdChannels = new Integer[columnValueAndRowIdSymbols.size()];
            int symbolCounter = 0;
            for (Symbol symbol : columnValueAndRowIdSymbols) {
                int index = outputSymbols.indexOf(symbol);
                verify(index >= 0, "Could not find symbol %s in the outputSymbols %s", symbol, outputSymbols);
                columnValueAndRowIdChannels[symbolCounter] = index;
                symbolCounter++;
            }
            return Arrays.asList(columnValueAndRowIdChannels);
        }

        @Override
        public PhysicalOperation visitTableExecute(TableExecuteNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            context.setDriverInstanceCount(getTaskWriterCount(session));

            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    nCopies(inputChannels.size(), null), // N x null means no not-null checking will be performed. This is ok as in TableExecute flow we are not changing any table data.
                    session,
                    new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()), // statistics are not calculated
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()));

            return new PhysicalOperation(operatorFactory, outputMapping.buildOrThrow(), context, source);
        }

        @Override
        public PhysicalOperation visitTableDelete(TableDeleteNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new TableDeleteOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, session, node.getTarget());

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("Union node should not be present in a local execution plan");
        }

        @Override
        public PhysicalOperation visitEnforceSingleRow(EnforceSingleRowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitAssignUniqueId(AssignUniqueId node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = AssignUniqueIdOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the local planner");

            if (node.getOrderingScheme().isPresent()) {
                return createLocalMerge(node, context);
            }

            return createLocalExchange(node, context);
        }

        private PhysicalOperation createLocalMerge(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkState(node.getSources().size() == 1, "single source is expected");

            // local merge source must have a single driver
            context.setDriverInstanceCount(1);

            PlanNode sourceNode = getOnlyElement(node.getSources());
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = sourceNode.accept(this, subContext);

            int operatorsCount = subContext.getDriverInstanceCount().orElse(1);
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            LocalExchangeFactory exchangeFactory = new LocalExchangeFactory(
                    nodePartitioningManager,
                    session,
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    operatorsCount,
                    types,
                    ImmutableList.of(),
                    Optional.empty(),
                    source.getPipelineExecutionStrategy(),
                    maxLocalExchangeBufferSize,
                    blockTypeOperators);

            List<Symbol> expectedLayout = node.getInputs().get(0);
            Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());
            context.addDriverFactory(
                    subContext.isInputDriver(),
                    false,
                    new PhysicalOperation(
                            new LocalExchangeSinkOperatorFactory(
                                    exchangeFactory,
                                    subContext.getNextOperatorId(),
                                    node.getId(),
                                    exchangeFactory.newSinkFactoryId(),
                                    pagePreprocessor),
                            source),
                    subContext.getDriverInstanceCount());
            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), layout);
            List<SortOrder> orderings = orderingScheme.getOrderingList();
            OperatorFactory operatorFactory = new LocalMergeSourceOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeFactory,
                    types,
                    orderingCompiler,
                    sortChannels,
                    orderings);
            return new PhysicalOperation(operatorFactory, layout, context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createLocalExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            int driverInstanceCount;
            if (node.getType() == ExchangeNode.Type.GATHER) {
                driverInstanceCount = 1;
                context.setDriverInstanceCount(1);
            }
            else if (context.getDriverInstanceCount().isPresent()) {
                driverInstanceCount = context.getDriverInstanceCount().getAsInt();
            }
            else {
                driverInstanceCount = getTaskConcurrency(session);
                context.setDriverInstanceCount(driverInstanceCount);
            }

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            List<Integer> channels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                    .collect(toImmutableList());
            Optional<Integer> hashChannel = node.getPartitioningScheme().getHashColumn()
                    .map(symbol -> node.getOutputSymbols().indexOf(symbol));

            PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = GROUPED_EXECUTION;
            List<DriverFactoryParameters> driverFactoryParametersList = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode sourceNode = node.getSources().get(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = sourceNode.accept(this, subContext);
                driverFactoryParametersList.add(new DriverFactoryParameters(subContext, source));

                if (source.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION) {
                    exchangeSourcePipelineExecutionStrategy = UNGROUPED_EXECUTION;
                }
            }

            LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                    nodePartitioningManager,
                    session,
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    driverInstanceCount,
                    types,
                    channels,
                    hashChannel,
                    exchangeSourcePipelineExecutionStrategy,
                    maxLocalExchangeBufferSize,
                    blockTypeOperators);
            for (int i = 0; i < node.getSources().size(); i++) {
                DriverFactoryParameters driverFactoryParameters = driverFactoryParametersList.get(i);
                PhysicalOperation source = driverFactoryParameters.getSource();
                LocalExecutionPlanContext subContext = driverFactoryParameters.getSubContext();

                List<Symbol> expectedLayout = node.getInputs().get(i);
                Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());

                context.addDriverFactory(
                        subContext.isInputDriver(),
                        false,
                        new PhysicalOperation(
                                new LocalExchangeSinkOperatorFactory(
                                        localExchangeFactory,
                                        subContext.getNextOperatorId(),
                                        node.getId(),
                                        localExchangeFactory.newSinkFactoryId(),
                                        pagePreprocessor),
                                source),
                        subContext.getDriverInstanceCount());
            }

            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            // instance count must match the number of partitions in the exchange
            verify(context.getDriverInstanceCount().getAsInt() == localExchangeFactory.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            return new PhysicalOperation(new LocalExchangeSourceOperatorFactory(context.getNextOperatorId(), node.getId(), localExchangeFactory), makeLayout(node), context, exchangeSourcePipelineExecutionStrategy);
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<Type> getSourceOperatorTypes(PlanNode node, TypeProvider types)
        {
            return getSymbolTypes(node.getOutputSymbols(), types);
        }

        private List<Type> getSymbolTypes(List<Symbol> symbols, TypeProvider types)
        {
            return symbols.stream()
                    .map(types::get)
                    .collect(toImmutableList());
        }

        private AggregatorFactory buildAggregatorFactory(
                PhysicalOperation source,
                Aggregation aggregation,
                Step step)
        {
            List<Integer> argumentChannels = new ArrayList<>();
            for (Expression argument : aggregation.getArguments()) {
                if (!(argument instanceof LambdaExpression)) {
                    Symbol argumentSymbol = Symbol.from(argument);
                    argumentChannels.add(source.getLayout().get(argumentSymbol));
                }
            }

            OptionalInt maskChannel = aggregation.getMask().stream()
                    .mapToInt(value -> source.getLayout().get(value))
                    .findAny();
            AggregationMetadata aggregationMetadata = metadata.getAggregateFunctionImplementation(aggregation.getResolvedFunction());
            List<LambdaExpression> lambdaExpressions = aggregation.getArguments().stream()
                    .filter(LambdaExpression.class::isInstance)
                    .map(LambdaExpression.class::cast)
                    .collect(toImmutableList());
            List<FunctionType> functionTypes = aggregation.getResolvedFunction().getSignature().getArgumentTypes().stream()
                    .filter(FunctionType.class::isInstance)
                    .map(FunctionType.class::cast)
                    .collect(toImmutableList());
            List<LambdaProvider> lambdaProviders = makeLambdaProviders(lambdaExpressions, aggregationMetadata.getLambdaInterfaces(), functionTypes);

            AccumulatorFactory accumulatorFactory = generateAccumulatorFactory(
                    aggregation.getResolvedFunction().getSignature(),
                    aggregationMetadata,
                    aggregation.getResolvedFunction().getFunctionNullability(),
                    lambdaProviders);

            if (aggregation.isDistinct()) {
                accumulatorFactory = new DistinctAccumulatorFactory(
                        accumulatorFactory,
                        argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList()),
                        joinCompiler,
                        blockTypeOperators,
                        session);
            }

            if (aggregation.getOrderingScheme().isPresent()) {
                List<Integer> inputArgumentChannels = range(0, argumentChannels.size())
                        .boxed()
                        .collect(toImmutableList());

                OrderingScheme orderingScheme = aggregation.getOrderingScheme().get();
                List<Symbol> sortKeys = orderingScheme.getOrderBy();

                List<SortOrder> sortOrders = sortKeys.stream()
                        .map(orderingScheme::getOrdering)
                        .collect(toImmutableList());

                List<Integer> inputOrderByChannels = new ArrayList<>();
                for (int orderByChannel : getChannelsForSymbols(sortKeys, source.getLayout())) {
                    int inputChannel = argumentChannels.indexOf(orderByChannel);
                    if (inputChannel < 0) {
                        inputChannel = argumentChannels.size();
                        argumentChannels.add(orderByChannel);
                    }
                    inputOrderByChannels.add(inputChannel);
                }

                accumulatorFactory = new OrderedAccumulatorFactory(
                        accumulatorFactory,
                        argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList()),
                        inputArgumentChannels,
                        inputOrderByChannels,
                        sortOrders,
                        pagesIndexFactory);
            }

            ImmutableList<Type> intermediateTypes = aggregationMetadata.getAccumulatorStateDescriptors().stream()
                    .map(stateDescriptor -> stateDescriptor.getSerializer().getSerializedType())
                    .collect(toImmutableList());
            Type intermediateType = (intermediateTypes.size() == 1) ? getOnlyElement(intermediateTypes) : RowType.anonymous(intermediateTypes);
            Type finalType = aggregation.getResolvedFunction().getSignature().getReturnType();

            return new AggregatorFactory(
                    accumulatorFactory,
                    step,
                    intermediateType,
                    finalType,
                    argumentChannels,
                    maskChannel,
                    !aggregation.isDistinct() && aggregation.getOrderingScheme().isEmpty());
        }

        private List<LambdaProvider> makeLambdaProviders(List<LambdaExpression> lambdaExpressions, List<Class<?>> lambdaInterfaces, List<FunctionType> functionTypes)
        {
            List<LambdaProvider> lambdaProviders = new ArrayList<>();
            if (!lambdaExpressions.isEmpty()) {
                verify(lambdaExpressions.size() == functionTypes.size());
                verify(lambdaExpressions.size() == lambdaInterfaces.size());

                for (int i = 0; i < lambdaExpressions.size(); i++) {
                    LambdaExpression lambdaExpression = lambdaExpressions.get(i);
                    FunctionType functionType = functionTypes.get(i);

                    // To compile lambda, LambdaDefinitionExpression needs to be generated from LambdaExpression,
                    // which requires the types of all sub-expressions.
                    //
                    // In project and filter expression compilation, ExpressionAnalyzer.getExpressionTypesFromInput
                    // is used to generate the types of all sub-expressions. (see visitScanFilterAndProject and visitFilter)
                    //
                    // This does not work here since the function call representation in final aggregation node
                    // is currently a hack: it takes intermediate type as input, and may not be a valid
                    // function call in Trino.
                    //
                    // TODO: Once the final aggregation function call representation is fixed,
                    // the same mechanism in project and filter expression should be used here.
                    verify(lambdaExpression.getArguments().size() == functionType.getArgumentTypes().size());
                    Map<NodeRef<Expression>, Type> lambdaArgumentExpressionTypes = new HashMap<>();
                    Map<Symbol, Type> lambdaArgumentSymbolTypes = new HashMap<>();
                    for (int j = 0; j < lambdaExpression.getArguments().size(); j++) {
                        LambdaArgumentDeclaration argument = lambdaExpression.getArguments().get(j);
                        Type type = functionType.getArgumentTypes().get(j);
                        lambdaArgumentExpressionTypes.put(NodeRef.of(argument), type);
                        lambdaArgumentSymbolTypes.put(new Symbol(argument.getName().getValue()), type);
                    }
                    Map<NodeRef<Expression>, Type> expressionTypes = ImmutableMap.<NodeRef<Expression>, Type>builder()
                            // the lambda expression itself
                            .put(NodeRef.of(lambdaExpression), functionType)
                            // expressions from lambda arguments
                            .putAll(lambdaArgumentExpressionTypes)
                            // expressions from lambda body
                            .putAll(typeAnalyzer.getTypes(session, TypeProvider.copyOf(lambdaArgumentSymbolTypes), lambdaExpression.getBody()))
                            .buildOrThrow();

                    LambdaDefinitionExpression lambda = (LambdaDefinitionExpression) toRowExpression(lambdaExpression, expressionTypes, ImmutableMap.of());
                    Class<? extends LambdaProvider> lambdaProviderClass = compileLambdaProvider(lambda, metadata, lambdaInterfaces.get(i));
                    try {
                        lambdaProviders.add((LambdaProvider) constructorMethodHandle(lambdaProviderClass, ConnectorSession.class).invoke(session.toConnectorSession()));
                    }
                    catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            }
            return lambdaProviders;
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            AggregationOperatorFactory operatorFactory = createAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getStep(),
                    0,
                    outputMappings,
                    source,
                    context);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), context, source);
        }

        private AggregationOperatorFactory createAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Step step,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                PhysicalOperation source,
                LocalExecutionPlanContext context)
        {
            int outputChannel = startOutputChannel;
            ImmutableList.Builder<AggregatorFactory> aggregatorFactories = ImmutableList.builder();
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                aggregatorFactories.add(buildAggregatorFactory(source, aggregation, step));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                outputChannel++;
            }
            return new AggregationOperatorFactory(context.getNextOperatorId(), planNodeId, aggregatorFactories.build());
        }

        private PhysicalOperation planGroupByAggregation(
                AggregationNode node,
                PhysicalOperation source,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> mappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createHashAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getGlobalGroupingSets(),
                    node.getGroupingKeys(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol(),
                    source,
                    node.hasDefaultOutput(),
                    spillEnabled,
                    node.isStreamable(),
                    unspillMemoryLimit,
                    context,
                    0,
                    mappings,
                    10_000,
                    Optional.of(maxPartialAggregationMemorySize));
            return new PhysicalOperation(operatorFactory, mappings.buildOrThrow(), context, source);
        }

        private OperatorFactory createHashAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Set<Integer> globalGroupingSets,
                List<Symbol> groupBySymbols,
                Step step,
                Optional<Symbol> hashSymbol,
                Optional<Symbol> groupIdSymbol,
                PhysicalOperation source,
                boolean hasDefaultOutput,
                boolean spillEnabled,
                boolean isStreamable,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                int expectedGroups,
                Optional<DataSize> maxPartialAggregationMemorySize)
        {
            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AggregatorFactory> aggregatorFactories = new ArrayList<>();
            for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();

                aggregatorFactories.add(buildAggregatorFactory(source, aggregation, step));
                aggregationOutputSymbols.add(symbol);
            }

            // add group-by key fields each in a separate channel
            int channel = startOutputChannel;
            Optional<Integer> groupIdChannel = Optional.empty();
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                if (groupIdSymbol.isPresent() && groupIdSymbol.get().equals(symbol)) {
                    groupIdChannel = Optional.of(channel);
                }
                channel++;
            }

            // hashChannel follows the group by channels
            if (hashSymbol.isPresent()) {
                outputMappings.put(hashSymbol.get(), channel++);
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                    .map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());

            if (isStreamable) {
                return StreamingAggregationOperator.createOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        source.getTypes(),
                        groupByTypes,
                        groupByChannels,
                        aggregatorFactories,
                        joinCompiler);
            }
            else {
                Optional<Integer> hashChannel = hashSymbol.map(channelGetter(source));
                return new HashAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        groupByTypes,
                        groupByChannels,
                        ImmutableList.copyOf(globalGroupingSets),
                        step,
                        hasDefaultOutput,
                        aggregatorFactories,
                        hashChannel,
                        groupIdChannel,
                        expectedGroups,
                        maxPartialAggregationMemorySize,
                        spillEnabled,
                        unspillMemoryLimit,
                        spillerFactory,
                        joinCompiler,
                        blockTypeOperators);
            }
        }
    }

    private int getDynamicFilteringMaxDistinctValuesPerDriver(Session session, boolean isReplicatedJoin)
    {
        if (isEnableLargeDynamicFilters(session)) {
            if (isReplicatedJoin) {
                return dynamicFilterConfig.getLargeBroadcastMaxDistinctValuesPerDriver();
            }
            return dynamicFilterConfig.getLargePartitionedMaxDistinctValuesPerDriver();
        }
        if (isReplicatedJoin) {
            return dynamicFilterConfig.getSmallBroadcastMaxDistinctValuesPerDriver();
        }
        return dynamicFilterConfig.getSmallPartitionedMaxDistinctValuesPerDriver();
    }

    private DataSize getDynamicFilteringMaxSizePerDriver(Session session, boolean isReplicatedJoin)
    {
        if (isEnableLargeDynamicFilters(session)) {
            if (isReplicatedJoin) {
                return dynamicFilterConfig.getLargeBroadcastMaxSizePerDriver();
            }
            return dynamicFilterConfig.getLargePartitionedMaxSizePerDriver();
        }
        if (isReplicatedJoin) {
            return dynamicFilterConfig.getSmallBroadcastMaxSizePerDriver();
        }
        return dynamicFilterConfig.getSmallPartitionedMaxSizePerDriver();
    }

    private int getDynamicFilteringRangeRowLimitPerDriver(Session session, boolean isReplicatedJoin)
    {
        if (isEnableLargeDynamicFilters(session)) {
            if (isReplicatedJoin) {
                return dynamicFilterConfig.getLargeBroadcastRangeRowLimitPerDriver();
            }
            return dynamicFilterConfig.getLargePartitionedRangeRowLimitPerDriver();
        }
        if (isReplicatedJoin) {
            return dynamicFilterConfig.getSmallBroadcastRangeRowLimitPerDriver();
        }
        return dynamicFilterConfig.getSmallPartitionedRangeRowLimitPerDriver();
    }

    private static List<Type> getTypes(List<Expression> expressions, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return expressions.stream()
                .map(NodeRef::of)
                .map(expressionTypes::get)
                .collect(toImmutableList());
    }

    private static TableFinisher createTableFinisher(Session session, TableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return (fragments, statistics, tableExecuteContext) -> {
            if (target instanceof CreateTarget) {
                return metadata.finishCreateTable(session, ((CreateTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof InsertTarget) {
                return metadata.finishInsert(session, ((InsertTarget) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof TableWriterNode.RefreshMaterializedViewTarget) {
                TableWriterNode.RefreshMaterializedViewTarget refreshTarget = (TableWriterNode.RefreshMaterializedViewTarget) target;
                return metadata.finishRefreshMaterializedView(
                        session,
                        refreshTarget.getTableHandle(),
                        refreshTarget.getInsertHandle(),
                        fragments,
                        statistics,
                        refreshTarget.getSourceTableHandles());
            }
            else if (target instanceof DeleteTarget) {
                metadata.finishDelete(session, ((DeleteTarget) target).getHandleOrElseThrow(), fragments);
                return Optional.empty();
            }
            else if (target instanceof UpdateTarget) {
                metadata.finishUpdate(session, ((UpdateTarget) target).getHandleOrElseThrow(), fragments);
                return Optional.empty();
            }
            else if (target instanceof TableExecuteTarget) {
                TableExecuteHandle tableExecuteHandle = ((TableExecuteTarget) target).getExecuteHandle();
                metadata.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteContext.getSplitsInfo());
                return Optional.empty();
            }
            else {
                throw new AssertionError("Unhandled target type: " + target.getClass().getName());
            }
        };
    }

    private static boolean shouldOutputRowCount(TableFinishNode node)
    {
        WriterTarget target = node.getTarget();
        return !(target instanceof TableExecuteTarget);
    }

    private static Function<Page, Page> enforceLoadedLayoutProcessor(List<Symbol> expectedLayout, Map<Symbol, Integer> inputLayout)
    {
        int[] channels = expectedLayout.stream()
                .peek(symbol -> checkArgument(inputLayout.containsKey(symbol), "channel not found for symbol: %s", symbol))
                .mapToInt(inputLayout::get)
                .toArray();

        if (Arrays.equals(channels, range(0, inputLayout.size()).toArray())) {
            // this is an identity mapping, simply ensuring that the page is fully loaded is sufficient
            return PageChannelSelector.identitySelection();
        }

        return new PageChannelSelector(channels);
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol));
        }
        return builder.build();
    }

    private static Function<Symbol, Integer> channelGetter(PhysicalOperation source)
    {
        return input -> {
            checkArgument(source.getLayout().containsKey(input));
            return source.getLayout().get(input);
        };
    }

    private static Set<DynamicFilterId> getConsumedDynamicFilterIds(PlanNode node)
    {
        return extractExpressions(node)
                .stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .map(DynamicFilters.Descriptor::getId)
                .collect(toImmutableSet());
    }

    /**
     * Encapsulates an physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private final List<OperatorFactoryWithTypes> operatorFactoriesWithTypes;
        private final Map<Symbol, Integer> layout;
        private final List<Type> types;

        private final PipelineExecutionStrategy pipelineExecutionStrategy;

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            this(operatorFactory, layout, context.getTypes(), Optional.empty(), pipelineExecutionStrategy);
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PhysicalOperation source)
        {
            this(operatorFactory, layout, context.getTypes(), Optional.of(requireNonNull(source, "source is null")), source.getPipelineExecutionStrategy());
        }

        public PhysicalOperation(OperatorFactory outputOperatorFactory, PhysicalOperation source)
        {
            this(outputOperatorFactory, ImmutableMap.of(), TypeProvider.empty(), Optional.of(requireNonNull(source, "source is null")), source.getPipelineExecutionStrategy());
        }

        private PhysicalOperation(
                OperatorFactory operatorFactory,
                Map<Symbol, Integer> layout,
                TypeProvider typeProvider,
                Optional<PhysicalOperation> source,
                PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");
            requireNonNull(typeProvider, "typeProvider is null");
            requireNonNull(source, "source is null");
            requireNonNull(pipelineExecutionStrategy, "pipelineExecutionStrategy is null");

            this.types = toTypes(layout, typeProvider);
            this.operatorFactoriesWithTypes = ImmutableList.<OperatorFactoryWithTypes>builder()
                    .addAll(source.map(PhysicalOperation::getOperatorFactoriesWithTypes).orElse(ImmutableList.of()))
                    .add(new OperatorFactoryWithTypes(operatorFactory, types))
                    .build();
            this.layout = ImmutableMap.copyOf(layout);
            this.pipelineExecutionStrategy = pipelineExecutionStrategy;
        }

        private static List<Type> toTypes(Map<Symbol, Integer> layout, TypeProvider typeProvider)
        {
            // verify layout covers all values
            int channelCount = layout.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            checkArgument(
                    layout.size() == channelCount && ImmutableSet.copyOf(layout.values()).containsAll(ContiguousSet.create(closedOpen(0, channelCount), integers())),
                    "Layout does not have a symbol for every output channel: %s", layout);
            Map<Integer, Symbol> channelLayout = ImmutableBiMap.copyOf(layout).inverse();

            return range(0, channelCount)
                    .mapToObj(channelLayout::get)
                    .map(typeProvider::get)
                    .collect(toImmutableList());
        }

        public int symbolToChannel(Symbol input)
        {
            checkArgument(layout.containsKey(input));
            return layout.get(input);
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Map<Symbol, Integer> getLayout()
        {
            return layout;
        }

        private List<OperatorFactory> getOperatorFactories()
        {
            return toOperatorFactories(operatorFactoriesWithTypes);
        }

        private List<OperatorFactoryWithTypes> getOperatorFactoriesWithTypes()
        {
            return operatorFactoriesWithTypes;
        }

        public PipelineExecutionStrategy getPipelineExecutionStrategy()
        {
            return pipelineExecutionStrategy;
        }
    }

    private static class DriverFactoryParameters
    {
        private final LocalExecutionPlanContext subContext;
        private final PhysicalOperation source;

        public DriverFactoryParameters(LocalExecutionPlanContext subContext, PhysicalOperation source)
        {
            this.subContext = subContext;
            this.source = source;
        }

        public LocalExecutionPlanContext getSubContext()
        {
            return subContext;
        }

        public PhysicalOperation getSource()
        {
            return source;
        }
    }

    private static class ValueAccessors
    {
        private final List<PhysicalValueAccessor> valueAccessors;
        private final List<MatchAggregationInstantiator> aggregations;
        private final int aggregationIndex;
        private final List<ArgumentComputationSupplier> aggregationArguments;
        private final int firstUnusedChannel;
        private final List<MatchAggregationLabelDependency> labelDependencies;

        public ValueAccessors(List<PhysicalValueAccessor> valueAccessors, List<MatchAggregationInstantiator> aggregations, int aggregationIndex, List<ArgumentComputationSupplier> aggregationArguments, int firstUnusedChannel, List<MatchAggregationLabelDependency> labelDependencies)
        {
            this.valueAccessors = valueAccessors;
            this.aggregations = aggregations;
            this.aggregationIndex = aggregationIndex;
            this.aggregationArguments = aggregationArguments;
            this.firstUnusedChannel = firstUnusedChannel;
            this.labelDependencies = labelDependencies;
        }

        public List<PhysicalValueAccessor> getValueAccessors()
        {
            return valueAccessors;
        }

        public List<MatchAggregationInstantiator> getAggregations()
        {
            return aggregations;
        }

        public int getAggregationIndex()
        {
            return aggregationIndex;
        }

        public List<ArgumentComputationSupplier> getAggregationArguments()
        {
            return aggregationArguments;
        }

        public int getFirstUnusedChannel()
        {
            return firstUnusedChannel;
        }

        public List<MatchAggregationLabelDependency> getLabelDependencies()
        {
            return labelDependencies;
        }
    }

    public static class MatchAggregationLabelDependency
    {
        private final Set<Integer> labels;
        private final boolean classifierInvolved;

        public MatchAggregationLabelDependency(Set<Integer> labels, boolean classifierInvolved)
        {
            this.labels = labels;
            this.classifierInvolved = classifierInvolved;
        }

        public Set<Integer> getLabels()
        {
            return labels;
        }

        public boolean isClassifierInvolved()
        {
            return classifierInvolved;
        }
    }
}
