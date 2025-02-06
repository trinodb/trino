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

import com.google.common.base.Throwables;
import com.google.common.base.VerifyException;
import com.google.common.cache.CacheBuilder;
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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cache.NonEvictableCache;
import io.trino.client.NodeVersion;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.ExplainAnalyzeContext;
import io.trino.execution.StageId;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.AssignUniqueIdOperator;
import io.trino.operator.DevNullOperator.DevNullOperatorFactory;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.DriverFactory;
import io.trino.operator.DynamicFilterSourceOperator;
import io.trino.operator.DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory;
import io.trino.operator.EnforceSingleRowOperator;
import io.trino.operator.ExchangeOperator.ExchangeOperatorFactory;
import io.trino.operator.ExplainAnalyzeOperator.ExplainAnalyzeOperatorFactory;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupIdOperator;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.HashSemiJoinOperator;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.LeafTableFunctionOperator.LeafTableFunctionOperatorFactory;
import io.trino.operator.LimitOperator.LimitOperatorFactory;
import io.trino.operator.LocalPlannerAware;
import io.trino.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.trino.operator.MergeOperator.MergeOperatorFactory;
import io.trino.operator.MergeProcessorOperator;
import io.trino.operator.MergeWriterOperator.MergeWriterOperatorFactory;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OrderByOperator.OrderByOperatorFactory;
import io.trino.operator.OutputFactory;
import io.trino.operator.OutputSpoolingOperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesSpatialIndexFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.RefreshMaterializedViewOperator.RefreshMaterializedViewOperatorFactory;
import io.trino.operator.RowNumberOperator;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetSupplier;
import io.trino.operator.SimpleTableExecuteOperator.SimpleTableExecuteOperatorOperatorFactory;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialIndexBuilderOperatorFactory;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.trino.operator.SpatialJoinOperator.SpatialJoinOperatorFactory;
import io.trino.operator.StatisticsWriterOperator.StatisticsWriterOperatorFactory;
import io.trino.operator.StreamingAggregationOperator;
import io.trino.operator.TableMutationOperator.TableMutationOperatorFactory;
import io.trino.operator.TableScanOperator.TableScanOperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.TopNOperator;
import io.trino.operator.TopNRankingOperator;
import io.trino.operator.ValuesOperator.ValuesOperatorFactory;
import io.trino.operator.WindowFunctionDefinition;
import io.trino.operator.WindowOperator.WindowOperatorFactory;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.DistinctAccumulatorFactory;
import io.trino.operator.aggregation.DistinctWindowAccumulator;
import io.trino.operator.aggregation.OrderedAccumulatorFactory;
import io.trino.operator.aggregation.OrderedWindowAccumulator;
import io.trino.operator.aggregation.partial.PartialAggregationController;
import io.trino.operator.exchange.LocalExchange;
import io.trino.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import io.trino.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import io.trino.operator.exchange.LocalMergeSourceOperator.LocalMergeSourceOperatorFactory;
import io.trino.operator.exchange.PageChannelSelector;
import io.trino.operator.function.RegularTableFunctionPartition.PassThroughColumnSpecification;
import io.trino.operator.function.TableFunctionOperator.TableFunctionOperatorFactory;
import io.trino.operator.index.DynamicTupleFilterFactory;
import io.trino.operator.index.FieldSetFilteringRecordSet;
import io.trino.operator.index.IndexBuildDriverFactoryProvider;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.index.IndexLookupSourceFactory;
import io.trino.operator.index.IndexManager;
import io.trino.operator.index.IndexSourceOperator;
import io.trino.operator.join.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorFactory;
import io.trino.operator.join.LookupSourceFactory;
import io.trino.operator.join.NestedLoopJoinBridge;
import io.trino.operator.join.NestedLoopJoinPagesSupplier;
import io.trino.operator.join.PartitionedLookupSourceFactory;
import io.trino.operator.join.unspilled.HashBuilderOperator;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputFactory;
import io.trino.operator.output.PositionsAppenderFactory;
import io.trino.operator.output.SkewedPartitionRebalancer;
import io.trino.operator.output.TaskOutputOperator.TaskOutputFactory;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.operator.unnest.UnnestOperator;
import io.trino.operator.window.AggregateWindowFunction;
import io.trino.operator.window.AggregationWindowFunctionSupplier;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.PartitionerSupplier;
import io.trino.operator.window.PatternRecognitionPartitionerSupplier;
import io.trino.operator.window.RegularPartitionerSupplier;
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
import io.trino.plugin.base.MappedRecordSet;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoders;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.spiller.SpillerFactory;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.DynamicPageFilter;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.planner.optimizations.IndexJoinOptimizer;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterId;
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
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Frame;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ClassifierValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.MatchNumberValuePointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.type.BlockTypeOperators;
import io.trino.type.FunctionType;
import org.objectweb.asm.MethodTooLargeException;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Range.closedOpen;
import static com.google.common.collect.Sets.difference;
import static io.trino.SystemSessionProperties.getAdaptivePartialAggregationUniqueRowsRatioThreshold;
import static io.trino.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static io.trino.SystemSessionProperties.getDynamicRowFilterSelectivityThreshold;
import static io.trino.SystemSessionProperties.getExchangeCompressionCodec;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.trino.SystemSessionProperties.getPagePartitioningBufferPoolSize;
import static io.trino.SystemSessionProperties.getSkewedPartitionMinDataProcessedRebalanceThreshold;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.getTaskMaxWriterCount;
import static io.trino.SystemSessionProperties.getTaskMinWriterCount;
import static io.trino.SystemSessionProperties.getWriterScalingMinDataProcessed;
import static io.trino.SystemSessionProperties.isAdaptivePartialAggregationEnabled;
import static io.trino.SystemSessionProperties.isColumnarFilterEvaluationEnabled;
import static io.trino.SystemSessionProperties.isEnableDynamicRowFiltering;
import static io.trino.SystemSessionProperties.isEnableLargeDynamicFilters;
import static io.trino.SystemSessionProperties.isForceSpillingOperator;
import static io.trino.SystemSessionProperties.isSpillEnabled;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static io.trino.operator.HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier;
import static io.trino.operator.OperatorFactories.join;
import static io.trino.operator.OperatorFactories.spillingJoin;
import static io.trino.operator.OutputSpoolingOperatorFactory.layoutUnionWithSpooledMetadata;
import static io.trino.operator.OutputSpoolingOperatorFactory.spooledOutputLayout;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.TableFinishOperator.TableFinishOperatorFactory;
import static io.trino.operator.TableFinishOperator.TableFinisher;
import static io.trino.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static io.trino.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static io.trino.operator.TableWriterOperator.STATS_START_CHANNEL;
import static io.trino.operator.TableWriterOperator.TableWriterOperatorFactory;
import static io.trino.operator.WindowFunctionDefinition.window;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactory;
import static io.trino.operator.join.JoinUtils.isBuildSideReplicated;
import static io.trino.operator.join.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import static io.trino.operator.join.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import static io.trino.operator.output.SkewedPartitionRebalancer.checkCanScalePartitionsRemotely;
import static io.trino.operator.output.SkewedPartitionRebalancer.createPartitionFunction;
import static io.trino.operator.output.SkewedPartitionRebalancer.getMaxWritersBasedOnMemory;
import static io.trino.operator.output.SkewedPartitionRebalancer.getTaskCount;
import static io.trino.operator.window.FrameInfo.Ordering.ASCENDING;
import static io.trino.operator.window.FrameInfo.Ordering.DESCENDING;
import static io.trino.operator.window.pattern.PhysicalValuePointer.CLASSIFIER;
import static io.trino.operator.window.pattern.PhysicalValuePointer.MATCH_NUMBER;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_EXCEEDED_COMPILER_LIMIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.gen.LambdaBytecodeGenerator.compileLambdaProvider;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.SortExpressionExtractor.extractSortExpression;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.RowsPerMatch.ONE;
import static io.trino.sql.planner.plan.SkipToPosition.LAST;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.util.MoreMath.previousPowerOfTwo;
import static io.trino.util.SpatialJoinUtils.ST_CONTAINS;
import static io.trino.util.SpatialJoinUtils.ST_DISTANCE;
import static io.trino.util.SpatialJoinUtils.ST_INTERSECTS;
import static io.trino.util.SpatialJoinUtils.ST_WITHIN;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.IntStream.range;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final Optional<ExplainAnalyzeContext> explainAnalyzeContext;
    private final PageSourceManager pageSourceManager;
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
    private final QueryDataEncoders encoders;
    private final Optional<SpoolingManager> spoolingManager;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final PagesIndex.Factory pagesIndexFactory;
    private final JoinCompiler joinCompiler;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final OrderingCompiler orderingCompiler;
    private final int largeMaxDistinctValuesPerDriver;
    private final int largePartitionedMaxDistinctValuesPerDriver;
    private final int smallMaxDistinctValuesPerDriver;
    private final int smallPartitionedMaxDistinctValuesPerDriver;
    private final DataSize largeMaxSizePerDriver;
    private final DataSize largePartitionedMaxSizePerDriver;
    private final DataSize smallMaxSizePerDriver;
    private final DataSize smallPartitionedMaxSizePerDriver;
    private final int largeRangeRowLimitPerDriver;
    private final int largePartitionedRangeRowLimitPerDriver;
    private final int smallRangeRowLimitPerDriver;
    private final int smallPartitionedRangeRowLimitPerDriver;
    private final DataSize largeMaxSizePerOperator;
    private final DataSize largePartitionedMaxSizePerOperator;
    private final DataSize smallMaxSizePerOperator;
    private final DataSize smallPartitionedMaxSizePerOperator;
    private final BlockTypeOperators blockTypeOperators;
    private final TypeOperators typeOperators;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final PositionsAppenderFactory positionsAppenderFactory;
    private final NodeVersion version;
    private final boolean specializeAggregationLoops;

    private final NonEvictableCache<FunctionKey, AccumulatorFactory> accumulatorFactoryCache = buildNonEvictableCache(CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(1, HOURS));
    private final NonEvictableCache<FunctionKey, AggregationWindowFunctionSupplier> aggregationWindowFunctionSupplierCache = buildNonEvictableCache(CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(1, HOURS));

    @Inject
    public LocalExecutionPlanner(
            PlannerContext plannerContext,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceManager pageSourceManager,
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
            QueryDataEncoders encoders,
            Optional<SpoolingManager> spoolingManager,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            FlatHashStrategyCompiler hashStrategyCompiler,
            OrderingCompiler orderingCompiler,
            DynamicFilterConfig dynamicFilterConfig,
            BlockTypeOperators blockTypeOperators,
            TypeOperators typeOperators,
            TableExecuteContextManager tableExecuteContextManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            NodeVersion version,
            CompilerConfig compilerConfig)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.explainAnalyzeContext = requireNonNull(explainAnalyzeContext, "explainAnalyzeContext is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.expressionCompiler = requireNonNull(expressionCompiler, "expressionCompiler is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.joinFilterFunctionCompiler = requireNonNull(joinFilterFunctionCompiler, "joinFilterFunctionCompiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.maxIndexMemorySize = taskManagerConfig.getMaxIndexMemoryUsage();
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.encoders = requireNonNull(encoders, "encoders is null");
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.maxPagePartitioningBufferSize = taskManagerConfig.getMaxPagePartitioningBufferSize();
        this.maxLocalExchangeBufferSize = taskManagerConfig.getMaxLocalExchangeBufferSize();
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.largeMaxDistinctValuesPerDriver = dynamicFilterConfig.getLargeMaxDistinctValuesPerDriver();
        this.smallMaxDistinctValuesPerDriver = dynamicFilterConfig.getSmallMaxDistinctValuesPerDriver();
        this.smallPartitionedMaxDistinctValuesPerDriver = dynamicFilterConfig.getSmallPartitionedMaxDistinctValuesPerDriver();
        this.largeMaxSizePerDriver = dynamicFilterConfig.getLargeMaxSizePerDriver();
        this.largePartitionedMaxSizePerDriver = dynamicFilterConfig.getLargePartitionedMaxSizePerDriver();
        this.smallMaxSizePerDriver = dynamicFilterConfig.getSmallMaxSizePerDriver();
        this.smallPartitionedMaxSizePerDriver = dynamicFilterConfig.getSmallPartitionedMaxSizePerDriver();
        this.largeRangeRowLimitPerDriver = dynamicFilterConfig.getLargeRangeRowLimitPerDriver();
        this.largePartitionedRangeRowLimitPerDriver = dynamicFilterConfig.getLargePartitionedRangeRowLimitPerDriver();
        this.smallRangeRowLimitPerDriver = dynamicFilterConfig.getSmallRangeRowLimitPerDriver();
        this.smallPartitionedRangeRowLimitPerDriver = dynamicFilterConfig.getSmallPartitionedRangeRowLimitPerDriver();
        this.largeMaxSizePerOperator = dynamicFilterConfig.getLargeMaxSizePerOperator();
        this.largePartitionedMaxSizePerOperator = dynamicFilterConfig.getLargePartitionedMaxSizePerOperator();
        this.smallMaxSizePerOperator = dynamicFilterConfig.getSmallMaxSizePerOperator();
        this.smallPartitionedMaxSizePerOperator = dynamicFilterConfig.getSmallPartitionedMaxSizePerOperator();
        this.largePartitionedMaxDistinctValuesPerDriver = dynamicFilterConfig.getLargePartitionedMaxDistinctValuesPerDriver();
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.positionsAppenderFactory = new PositionsAppenderFactory(blockTypeOperators);
        this.version = requireNonNull(version, "version is null");
        this.specializeAggregationLoops = compilerConfig.isSpecializeAggregationLoops();
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            PartitioningScheme partitioningScheme,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer)
    {
        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, plan, outputLayout, partitionedSourceOrder, new TaskOutputFactory(outputBuffer));
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
                        return argument.getColumn().type();
                    })
                    .collect(toImmutableList());
        }

        PartitionFunction partitionFunction;
        Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer = Optional.empty();
        int taskCount = getTaskCount(partitioningScheme);
        if (checkCanScalePartitionsRemotely(taskContext.getSession(), taskCount, partitioningScheme.getPartitioning().getHandle(), nodePartitioningManager)) {
            partitionFunction = createPartitionFunction(taskContext.getSession(), nodePartitioningManager, partitioningScheme, partitionChannelTypes);
            int partitionedWriterCount = getPartitionedWriterCountBasedOnMemory(taskContext.getSession());
            // Keep the task bucket count to 50% of total local writers
            int taskBucketCount = (int) ceil(0.5 * partitionedWriterCount);
            skewedPartitionRebalancer = Optional.of(new SkewedPartitionRebalancer(
                    partitionFunction.partitionCount(),
                    taskCount,
                    taskBucketCount,
                    getWriterScalingMinDataProcessed(taskContext.getSession()).toBytes(),
                    getSkewedPartitionMinDataProcessedRebalanceThreshold(taskContext.getSession()).toBytes()));
        }
        else {
            partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(), partitioningScheme, partitionChannelTypes);
        }
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        return plan(
                taskContext,
                plan,
                outputLayout,
                partitionedSourceOrder,
                new PartitionedOutputFactory(
                        partitionFunction,
                        partitionChannels,
                        partitionConstants,
                        partitioningScheme.isReplicateNullsAndAny(),
                        nullChannel,
                        outputBuffer,
                        maxPagePartitioningBufferSize,
                        positionsAppenderFactory,
                        taskContext.getSession().getExchangeEncryptionKey(),
                        taskContext.newAggregateMemoryContext(),
                        getPagePartitioningBufferPoolSize(taskContext.getSession()),
                        skewedPartitionRebalancer));
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            List<Symbol> outputLayout,
            List<PlanNodeId> partitionedSourceOrder,
            OutputFactory outputOperatorFactory)
    {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session), context);

        Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(outputLayout, physicalOperation.getLayout());

        List<Type> outputTypes = outputLayout.stream()
                .map(Symbol::type)
                .collect(toImmutableList());

        context.addDriverFactory(
                true,
                new PhysicalOperation(
                        outputOperatorFactory.createOutputOperator(
                                context.getNextOperatorId(),
                                plan.getId(),
                                outputTypes,
                                pagePreprocessor,
                                new PagesSerdeFactory(plannerContext.getBlockEncodingSerde(), getExchangeCompressionCodec(session))),
                        physicalOperation),
                context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder);
    }

    private static class LocalExecutionPlanContext
    {
        private final TaskContext taskContext;
        private final List<DriverFactory> driverFactories;
        private final Optional<IndexSourceContext> indexSourceContext;

        // this is shared with all subContexts
        private final AtomicInteger nextPipelineId;

        private int nextOperatorId;
        private boolean inputDriver = true;
        private OptionalInt driverInstanceCount = OptionalInt.empty();

        public LocalExecutionPlanContext(TaskContext taskContext)
        {
            this(
                    taskContext,
                    new ArrayList<>(),
                    Optional.empty(),
                    new AtomicInteger(0));
        }

        private LocalExecutionPlanContext(
                TaskContext taskContext,
                List<DriverFactory> driverFactories,
                Optional<IndexSourceContext> indexSourceContext,
                AtomicInteger nextPipelineId)
        {
            this.taskContext = taskContext;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
            this.nextPipelineId = nextPipelineId;
        }

        public void addDriverFactory(boolean outputDriver, PhysicalOperation physicalOperation, LocalExecutionPlanContext context)
        {
            boolean inputDriver = context.isInputDriver();
            OptionalInt driverInstances = context.getDriverInstanceCount();
            List<OperatorFactory> operatorFactories = physicalOperation.getOperatorFactories();
            addLookupOuterDrivers(outputDriver, operatorFactories);
            addDriverFactory(inputDriver, outputDriver, operatorFactories, driverInstances);
        }

        private void addLookupOuterDrivers(boolean isOutputDriver, List<OperatorFactory> operatorFactories)
        {
            // For an outer join on the lookup side (RIGHT or FULL) add an additional
            // driver to output the unused rows in the lookup source
            for (int i = 0; i < operatorFactories.size(); i++) {
                OperatorFactory operatorFactory = operatorFactories.get(i);
                if (!(operatorFactory instanceof JoinOperatorFactory lookupJoin)) {
                    continue;
                }

                Optional<OperatorFactory> outerOperatorFactoryResult = lookupJoin.createOuterOperatorFactory();
                if (outerOperatorFactoryResult.isPresent()) {
                    // Add a new driver to output the unmatched rows in an outer join.
                    // We duplicate all of the factories above the JoinOperator (the ones reading from the joins),
                    // and replace the JoinOperator with the OuterOperator (the one that produces unmatched rows).
                    ImmutableList.Builder<OperatorFactory> newOperators = ImmutableList.builder();
                    newOperators.add(outerOperatorFactoryResult.get());
                    operatorFactories.subList(i + 1, operatorFactories.size()).stream()
                            .map(OperatorFactory::duplicate)
                            .forEach(newOperators::add);

                    addDriverFactory(false, isOutputDriver, newOperators.build(), OptionalInt.of(1));
                }
            }
        }

        private void addDriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances)
        {
            driverFactories.add(new DriverFactory(getNextPipelineId(), inputDriver, outputDriver, operatorFactories, driverInstances));
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

        public LocalDynamicFiltersCollector getDynamicFiltersCollector()
        {
            return taskContext.getLocalDynamicFiltersCollector();
        }

        private void registerCoordinatorDynamicFilters(List<DynamicFilters.Descriptor> dynamicFilters)
        {
            Set<DynamicFilterId> consumedFilterIds = dynamicFilters.stream()
                    .map(DynamicFilters.Descriptor::getId)
                    .collect(toImmutableSet());
            LocalDynamicFiltersCollector dynamicFiltersCollector = getDynamicFiltersCollector();
            // Don't repeat registration of node-local filters or those already registered by another scan (e.g. co-located joins)
            dynamicFiltersCollector.register(
                    difference(consumedFilterIds, dynamicFiltersCollector.getRegisteredDynamicFilterIds()));
        }

        private TaskContext getTaskContext()
        {
            return taskContext;
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
            return new LocalExecutionPlanContext(taskContext, driverFactories, indexSourceContext, nextPipelineId);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(taskContext, driverFactories, Optional.of(indexSourceContext), nextPipelineId);
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

        public LocalExecutionPlan(List<DriverFactory> driverFactories, List<PlanNodeId> partitionedSourceOrder)
        {
            this.driverFactories = ImmutableList.copyOf(requireNonNull(driverFactories, "driverFactories is null"));
            this.partitionedSourceOrder = ImmutableList.copyOf(requireNonNull(partitionedSourceOrder, "partitionedSourceOrder is null"));
        }

        public List<DriverFactory> getDriverFactories()
        {
            return driverFactories;
        }

        public List<PlanNodeId> getPartitionedSourceOrder()
        {
            return partitionedSourceOrder;
        }
    }

    private class Visitor
            extends PlanVisitor<PhysicalOperation, LocalExecutionPlanContext>
    {
        private final Session session;
        private final IrExpressionEvaluator evaluator;

        private Visitor(Session session)
        {
            this.session = session;
            evaluator = new IrExpressionEvaluator(plannerContext);
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
            checkArgument(node.getRetryPolicy() == NONE, "unexpected retry policy: %s", node.getRetryPolicy());

            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.orderingList();

            List<Type> types = getSourceOperatorTypes(node);
            ImmutableList<Integer> outputChannels = IntStream.range(0, types.size())
                    .boxed()
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new MergeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    directExchangeClientSupplier,
                    new PagesSerdeFactory(plannerContext.getBlockEncodingSerde(), getExchangeCompressionCodec(session)),
                    orderingCompiler,
                    types,
                    outputChannels,
                    sortChannels,
                    sortOrder);

            return new PhysicalOperation(operatorFactory, makeLayout(node));
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
                    new PagesSerdeFactory(plannerContext.getBlockEncodingSerde(), getExchangeCompressionCodec(session)),
                    node.getRetryPolicy(),
                    exchangeManagerRegistry);

            return new PhysicalOperation(operatorFactory, makeLayout(node));
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
                    plannerContext.getFunctionManager(),
                    node.isVerbose(),
                    version);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            Session session = context.taskContext.getSession();
            PhysicalOperation operation = node.getSource().accept(this, context);

            if (session.getQueryDataEncoding().isEmpty()) {
                return operation;
            }

            QueryDataEncoder.Factory encoderFactory = session
                    .getQueryDataEncoding()
                    .map(encoders::get)
                    .orElseThrow(() -> new IllegalStateException("Spooled query encoding was not found"));

            Map<Symbol, Integer> spooledLayout = layoutUnionWithSpooledMetadata(operation.layout);
            QueryDataEncoder queryDataEncoder = encoderFactory.create(session, spooledOutputLayout(node, operation.layout));
            OutputSpoolingOperatorFactory outputSpoolingOperatorFactory = new OutputSpoolingOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    spooledLayout,
                    queryDataEncoder,
                    spoolingManager.orElseThrow());

            return new PhysicalOperation(outputSpoolingOperatorFactory, spooledLayout, operation);
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
                    hashStrategyCompiler);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
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

            List<Symbol> orderBySymbols = node.getOrderingScheme().orderBy();
            List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());
            List<SortOrder> sortOrder = orderBySymbols.stream()
                    .map(symbol -> node.getOrderingScheme().ordering(symbol))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
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
                    hashStrategyCompiler,
                    plannerContext.getTypeOperators(),
                    blockTypeOperators);

            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
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
                sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), source.getLayout());
                sortOrder = orderingScheme.orderingList();
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
                Optional<FrameInfo.Ordering> ordering = Optional.empty();

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
                    // the following fields are only used for frame type RANGE
                    // in such case, there is a single sort channel
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
                ArrayList<Integer> argumentChannels = new ArrayList<>();
                for (Expression argument : function.getArguments()) {
                    if (!(argument instanceof Lambda)) {
                        Symbol argumentSymbol = Symbol.from(argument);
                        argumentChannels.add(source.getLayout().get(argumentSymbol));
                    }
                }
                Symbol symbol = entry.getKey();

                Type type = resolvedFunction.signature().getReturnType();

                List<Lambda> lambdas = function.getArguments().stream()
                        .filter(Lambda.class::isInstance)
                        .map(Lambda.class::cast)
                        .collect(toImmutableList());
                List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .map(FunctionType.class::cast)
                        .collect(toImmutableList());

                WindowFunctionSupplier windowFunctionSupplier;
                if (resolvedFunction.functionKind() == FunctionKind.AGGREGATE) {
                    AggregationWindowFunctionSupplier targetFunction = getAggregationWindowFunctionSupplier(resolvedFunction);
                    List<Class<?>> lambdaInterfaces = targetFunction.getLambdaInterfaces();
                    Function<List<Supplier<Object>>, WindowAccumulator> accumulatorSupplier = targetFunction::createWindowAccumulator;

                    if (function.getOrderingScheme().isPresent()) {
                        OrderingScheme orderingScheme = function.getOrderingScheme().orElseThrow();
                        List<Symbol> sortKeys = orderingScheme.orderBy();
                        List<SortOrder> sortOrders = sortKeys.stream()
                                .map(orderingScheme::ordering)
                                .collect(toImmutableList());
                        ImmutableList.Builder<Integer> sortKeysArgumentsBuilder = ImmutableList.builder();
                        sortKeys.forEach(orderingArgumentSymbol -> {
                            argumentChannels.add(source.getLayout().get(orderingArgumentSymbol));
                            sortKeysArgumentsBuilder.add(argumentChannels.size() - 1); // last added argument
                        });

                        List<Type> argumentTypes = argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList());

                        List<Integer> sortKeysArguments = sortKeysArgumentsBuilder.build();
                        Function<List<Supplier<Object>>, WindowAccumulator> finalAccumulatorSupplier = accumulatorSupplier;
                        accumulatorSupplier = (lambdaProviders) ->
                                new OrderedWindowAccumulator(
                                        pagesIndexFactory,
                                        finalAccumulatorSupplier.apply(lambdaProviders),
                                        argumentTypes,
                                        argumentChannels,
                                        sortKeysArguments,
                                        sortOrders);
                    }

                    if (function.isDistinct()) {
                        List<Type> argumentTypes = argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList());

                        Function<List<Supplier<Object>>, WindowAccumulator> finalAccumulatorSupplier = accumulatorSupplier;
                        List<Integer> argumentChannelsFinal = ImmutableList.copyOf(argumentChannels);
                        accumulatorSupplier = (lambdaProviders) -> new DistinctWindowAccumulator(
                                finalAccumulatorSupplier.apply(lambdaProviders),
                                argumentTypes,
                                argumentChannelsFinal,
                                hashStrategyCompiler,
                                session,
                                pagesIndexFactory);
                    }

                    windowFunctionSupplier = windowAggregationFunctionSupplier(resolvedFunction, lambdaInterfaces, accumulatorSupplier);
                }
                else {
                    windowFunctionSupplier = plannerContext.getFunctionManager().getWindowFunctionSupplier(resolvedFunction);
                }

                List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, windowFunctionSupplier.getLambdaInterfaces(), functionTypes);
                WindowFunctionDefinition windowFunction = window(windowFunctionSupplier, type, frameInfo, function.isIgnoreNulls(), lambdaProviders, ImmutableList.copyOf(argumentChannels));

                windowFunctionsBuilder.add(windowFunction);
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

            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
        }

        private WindowFunctionSupplier windowAggregationFunctionSupplier(ResolvedFunction resolvedFunction, List<Class<?>> lambdaInterfaces, Function<List<Supplier<Object>>, WindowAccumulator> accumulatorSupplier)
        {
            return new WindowFunctionSupplier()
            {
                @Override
                public WindowFunction createWindowFunction(boolean ignoreNulls, List<Supplier<Object>> lambdaProviders)
                {
                    AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(resolvedFunction);
                    boolean hasRemoveInput = aggregationImplementation.getWindowAccumulator().isPresent();
                    return new AggregateWindowFunction(
                            () -> accumulatorSupplier.apply(lambdaProviders),
                            hasRemoveInput);
                }

                @Override
                public List<Class<?>> getLambdaInterfaces()
                {
                    return lambdaInterfaces;
                }
            };
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
                sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), source.getLayout());
                sortOrder = orderingScheme.orderingList();
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
                    if (!(argument instanceof Lambda)) {
                        Symbol argumentSymbol = Symbol.from(argument);
                        arguments.add(source.getLayout().get(argumentSymbol));
                    }
                }
                WindowFunctionSupplier windowFunctionSupplier = getWindowFunctionImplementation(resolvedFunction);
                Type type = resolvedFunction.signature().getReturnType();

                List<Lambda> lambdas = function.getArguments().stream()
                        .filter(Lambda.class::isInstance)
                        .map(Lambda.class::cast)
                        .collect(toImmutableList());
                List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .map(FunctionType.class::cast)
                        .collect(toImmutableList());

                List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, windowFunctionSupplier.getLambdaInterfaces(), functionTypes);
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
                Supplier<PageProjection> pageProjectionSupplier = prepareProjection(expressionAndValuePointers);

                // prepare physical value accessors to provide input for the expression
                ValueAccessors valueAccessors = preparePhysicalValuePointers(expressionAndValuePointers, mapping, source, connectorSession, firstUnusedChannel, matchAggregationIndex);

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
                Supplier<PageProjection> pageProjectionSupplier = prepareProjection(expressionAndValuePointers);

                // prepare physical value accessors to provide input for the expression
                ValueAccessors valueAccessors = preparePhysicalValuePointers(expressionAndValuePointers, mapping, source, connectorSession, firstUnusedChannel, matchAggregationIndex);

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
            Optional<LogicalIndexNavigation> skipToNavigation = Optional.empty();
            if (!node.getSkipToLabels().isEmpty()) {
                boolean last = node.getSkipToPosition().equals(LAST);
                skipToNavigation = Optional.of(new LogicalIndexPointer(node.getSkipToLabels(), last, false, 0, 0).toLogicalIndexNavigation(mapping));
            }

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

            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
        }

        private WindowFunctionSupplier getWindowFunctionImplementation(ResolvedFunction resolvedFunction)
        {
            if (resolvedFunction.functionKind() == FunctionKind.AGGREGATE) {
                return getAggregationWindowFunctionSupplier(resolvedFunction);
            }
            return plannerContext.getFunctionManager().getWindowFunctionSupplier(resolvedFunction);
        }

        private AggregationWindowFunctionSupplier getAggregationWindowFunctionSupplier(ResolvedFunction resolvedFunction)
        {
            checkArgument(
                    resolvedFunction.functionKind() == FunctionKind.AGGREGATE,
                    "Expected %s to be AGGREGATE function, but got %s",
                    resolvedFunction.functionId(),
                    resolvedFunction.functionKind());
            return uncheckedCacheGet(aggregationWindowFunctionSupplierCache, new FunctionKey(resolvedFunction.functionId(), resolvedFunction.signature()), () -> {
                AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(resolvedFunction);
                return new AggregationWindowFunctionSupplier(
                        resolvedFunction.signature(),
                        aggregationImplementation,
                        resolvedFunction.functionNullability());
            });
        }

        private Supplier<PageProjection> prepareProjection(ExpressionAndValuePointers expressionAndValuePointers)
        {
            Expression rewritten = expressionAndValuePointers.getExpression();

            // prepare input layout for compilation
            ImmutableMap.Builder<Symbol, Integer> inputLayout = ImmutableMap.builder();

            List<ExpressionAndValuePointers.Assignment> assignments = expressionAndValuePointers.getAssignments();
            for (int i = 0; i < assignments.size(); i++) {
                ExpressionAndValuePointers.Assignment assignment = assignments.get(i);
                inputLayout.put(assignment.symbol(), i);
            }

            // compile expression using input layout and input types
            RowExpression rowExpression = toRowExpression(rewritten, inputLayout.buildOrThrow());
            return pageFunctionCompiler.compileProjection(rowExpression, Optional.empty());
        }

        private ValueAccessors preparePhysicalValuePointers(
                ExpressionAndValuePointers expressionAndValuePointers,
                Map<IrLabel, Integer> mapping,
                PhysicalOperation source,
                ConnectorSession connectorSession,
                int firstUnusedChannel,
                int matchAggregationIndex)
        {
            Map<Symbol, Integer> sourceLayout = source.getLayout();

            ImmutableList.Builder<MatchAggregationInstantiator> matchAggregations = ImmutableList.builder();

            // runtime-evaluated aggregation arguments mapped to free channel slots
            ImmutableList.Builder<ArgumentComputationSupplier> aggregationArguments = ImmutableList.builder();

            // for thread equivalence
            ImmutableList.Builder<MatchAggregationLabelDependency> labelDependencies = ImmutableList.builder();

            ImmutableList.Builder<PhysicalValueAccessor> valueAccessors = ImmutableList.builder();
            for (ExpressionAndValuePointers.Assignment assignment : expressionAndValuePointers.getAssignments()) {
                switch (assignment.valuePointer()) {
                    case ClassifierValuePointer pointer -> {
                        valueAccessors.add(new PhysicalValuePointer(
                                CLASSIFIER,
                                VARCHAR,
                                pointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
                    }
                    case MatchNumberValuePointer pointer -> {
                        valueAccessors.add(new PhysicalValuePointer(MATCH_NUMBER, BIGINT, LogicalIndexNavigation.NO_OP));
                    }
                    case ScalarValuePointer pointer -> {
                        valueAccessors.add(new PhysicalValuePointer(
                                getOnlyElement(getChannelsForSymbols(ImmutableList.of(pointer.getInputSymbol()), sourceLayout)),
                                pointer.getInputSymbol().type(),
                                pointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
                    }
                    case AggregationValuePointer pointer -> {
                        boolean classifierInvolved = false;

                        ResolvedFunction resolvedFunction = pointer.getFunction();
                        AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(pointer.getFunction());

                        ImmutableList.Builder<Map.Entry<Expression, Type>> builder = ImmutableList.builder();
                        List<Type> signatureTypes = resolvedFunction.signature().getArgumentTypes();
                        for (int i = 0; i < pointer.getArguments().size(); i++) {
                            builder.add(new SimpleEntry<>(pointer.getArguments().get(i), signatureTypes.get(i)));
                        }
                        Map<Boolean, List<Map.Entry<Expression, Type>>> arguments = builder.build().stream()
                                .collect(partitioningBy(entry -> entry.getKey() instanceof Lambda));

                        // handle lambda arguments
                        List<Lambda> lambdas = arguments.get(true).stream()
                                .map(Map.Entry::getKey)
                                .map(Lambda.class::cast)
                                .collect(toImmutableList());

                        List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                                .filter(FunctionType.class::isInstance)
                                .map(FunctionType.class::cast)
                                .collect(toImmutableList());

                        // TODO when we support lambda arguments: lambda cannot have runtime-evaluated symbols -- add check in the Analyzer
                        List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, aggregationImplementation.getLambdaInterfaces(), functionTypes);

                        // handle non-lambda arguments
                        List<Integer> valueChannels = new ArrayList<>();

                        Optional<Symbol> classifierArgumentSymbol = pointer.getClassifierSymbol();
                        Optional<Symbol> matchNumberArgumentSymbol = pointer.getMatchNumberSymbol();
                        Set<Symbol> runtimeEvaluatedSymbols = ImmutableSet.of(classifierArgumentSymbol, matchNumberArgumentSymbol).stream()
                                .flatMap(Optional::stream)
                                .collect(toImmutableSet());

                        for (Map.Entry<Expression, Type> argumentWithType : arguments.get(false)) {
                            Expression argument = argumentWithType.getKey();
                            boolean isRuntimeEvaluated = !(argument instanceof Reference) || runtimeEvaluatedSymbols.contains(Symbol.from(argument));
                            if (isRuntimeEvaluated) {
                                List<Symbol> argumentInputSymbols = ImmutableList.copyOf(SymbolsExtractor.extractUnique(argument));
                                Supplier<PageProjection> argumentProjectionSupplier = prepareArgumentProjection(argument, argumentInputSymbols);

                                List<Integer> argumentInputChannels = new ArrayList<>();
                                for (Symbol symbol : argumentInputSymbols) {
                                    if (classifierArgumentSymbol.isPresent() && symbol.equals(classifierArgumentSymbol.get())) {
                                        classifierInvolved = true;
                                        argumentInputChannels.add(CLASSIFIER);
                                    }
                                    else if (matchNumberArgumentSymbol.isPresent() && symbol.equals(matchNumberArgumentSymbol.get())) {
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

                        AggregationWindowFunctionSupplier aggregationWindowFunctionSupplier = uncheckedCacheGet(
                                aggregationWindowFunctionSupplierCache,
                                new FunctionKey(resolvedFunction.functionId(), resolvedFunction.signature()),
                                () -> new AggregationWindowFunctionSupplier(
                                        resolvedFunction.signature(),
                                        aggregationImplementation,
                                        resolvedFunction.functionNullability()));
                        matchAggregations.add(new MatchAggregationInstantiator(
                                resolvedFunction.signature(),
                                aggregationWindowFunctionSupplier,
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
            }

            return new ValueAccessors(valueAccessors.build(), matchAggregations.build(), matchAggregationIndex, aggregationArguments.build(), firstUnusedChannel, labelDependencies.build());
        }

        private Supplier<PageProjection> prepareArgumentProjection(Expression argument, List<Symbol> inputSymbols)
        {
            // prepare input layout and type provider for compilation
            ImmutableMap.Builder<Symbol, Integer> inputLayout = ImmutableMap.builder();
            for (int i = 0; i < inputSymbols.size(); i++) {
                inputLayout.put(inputSymbols.get(i), i);
            }

            // compile expression using input layout and input types
            RowExpression rowExpression = toRowExpression(argument, inputLayout.buildOrThrow());
            return pageFunctionCompiler.compileProjection(rowExpression, Optional.empty());
        }

        @Override
        public PhysicalOperation visitTableFunction(TableFunctionNode node, LocalExecutionPlanContext context)
        {
            throw new IllegalStateException(format("Unexpected node: TableFunctionNode (%s)", node.getName()));
        }

        @Override
        public PhysicalOperation visitTableFunctionProcessor(TableFunctionProcessorNode node, LocalExecutionPlanContext context)
        {
            TableFunctionProcessorProvider processorProvider = plannerContext.getFunctionManager().getTableFunctionProcessorProvider(node.getHandle());

            if (node.getSource().isEmpty()) {
                OperatorFactory operatorFactory = new LeafTableFunctionOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        node.getHandle().catalogHandle(),
                        processorProvider,
                        node.getHandle().functionHandle());
                return new PhysicalOperation(operatorFactory, makeLayout(node));
            }

            PhysicalOperation source = node.getSource().orElseThrow().accept(this, context);

            int properChannelsCount = node.getProperOutputs().size();

            long passThroughSourcesCount = node.getPassThroughSpecifications().stream()
                    .filter(PassThroughSpecification::declaredAsPassThrough)
                    .count();

            List<List<Integer>> requiredChannels = node.getRequiredSymbols().stream()
                    .map(list -> getChannelsForSymbols(list, source.getLayout()))
                    .collect(toImmutableList());

            Optional<Map<Integer, Integer>> markerChannels = node.getMarkerSymbols()
                    .map(map -> map.entrySet().stream()
                            .collect(toImmutableMap(entry -> source.getLayout().get(entry.getKey()), entry -> source.getLayout().get(entry.getValue()))));

            int channel = properChannelsCount;
            ImmutableList.Builder<PassThroughColumnSpecification> passThroughColumnSpecifications = ImmutableList.builder();
            for (PassThroughSpecification specification : node.getPassThroughSpecifications()) {
                // the table function produces one index channel for each source declared as pass-through. They are laid out after the proper channels.
                int indexChannel = specification.declaredAsPassThrough() ? channel++ : -1;
                for (PassThroughColumn column : specification.columns()) {
                    passThroughColumnSpecifications.add(new PassThroughColumnSpecification(column.isPartitioningColumn(), source.getLayout().get(column.symbol()), indexChannel));
                }
            }

            List<Integer> partitionChannels = node.getSpecification()
                    .map(DataOrganizationSpecification::partitionBy)
                    .map(list -> getChannelsForSymbols(list, source.getLayout()))
                    .orElse(ImmutableList.of());

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrders = ImmutableList.of();
            if (node.getSpecification().flatMap(DataOrganizationSpecification::orderingScheme).isPresent()) {
                OrderingScheme orderingScheme = node.getSpecification().flatMap(DataOrganizationSpecification::orderingScheme).orElseThrow();
                sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), source.getLayout());
                sortOrders = orderingScheme.orderingList();
            }

            OperatorFactory operator = new TableFunctionOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    processorProvider,
                    node.getHandle().catalogHandle(),
                    node.getHandle().functionHandle(),
                    properChannelsCount,
                    toIntExact(passThroughSourcesCount),
                    requiredChannels,
                    markerChannels,
                    passThroughColumnSpecifications.build(),
                    node.isPruneWhenEmpty(),
                    partitionChannels,
                    getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitioned()), source.getLayout()),
                    sortChannels,
                    sortOrders,
                    node.getPreSorted(),
                    source.getTypes(),
                    10_000,
                    pagesIndexFactory);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (int i = 0; i < node.getProperOutputs().size(); i++) {
                outputMappings.put(node.getProperOutputs().get(i), i);
            }
            List<Symbol> passThroughSymbols = node.getPassThroughSpecifications().stream()
                    .map(PassThroughSpecification::columns)
                    .flatMap(Collection::stream)
                    .map(PassThroughColumn::symbol)
                    .collect(toImmutableList());
            int outputChannel = properChannelsCount;
            for (Symbol passThroughSymbol : passThroughSymbols) {
                outputMappings.put(passThroughSymbol, outputChannel++);
            }

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), source);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().orderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortChannels.add(source.getLayout().get(symbol));
                sortOrders.add(node.getOrderingScheme().ordering(symbol));
            }

            OperatorFactory operator = TopNOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    (int) node.getCount(),
                    sortChannels,
                    sortOrders,
                    plannerContext.getTypeOperators());

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().orderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderingScheme().ordering(symbol));
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

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            // Limit with ties should be rewritten at this point
            checkState(node.getTiesResolvingScheme().isEmpty(), "Limit with ties not supported");

            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
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
                    hashStrategyCompiler);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitGroupId(GroupIdNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Symbol, Integer> newLayout = new HashMap<>();
            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();

            int outputChannel = 0;

            for (Symbol output : node.getDistinctGroupingSetSymbols()) {
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

            return new PhysicalOperation(groupIdOperatorFactory, newLayout, source);
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
            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), channels, hashChannel, hashStrategyCompiler);
            return new PhysicalOperation(operator, makeLayout(node), source);
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
            Expression filterExpression = node.getPredicate();

            if (node.getSource() instanceof TableScanNode tableScanNode) {
                DynamicFilters.ExtractResult extractDynamicFilterResult = extractDynamicFilters(filterExpression);
                Expression staticFilter = combineConjuncts(extractDynamicFilterResult.getStaticConjuncts());
                if (staticFilter.equals(TRUE) && extractDynamicFilterResult.getDynamicConjuncts().isEmpty()) {
                    // filter node contains only empty dynamic filter, fallback to normal table scan
                    return visitTableScan(node.getId(), tableScanNode, filterExpression, context);
                }
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();
            return visitScanFilterAndProject(context, node.getId(), sourceNode, Optional.of(filterExpression), Assignments.identity(outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Optional<Expression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode filterNode) {
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
            if (sourceNode instanceof TableScanNode tableScanNode) {
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
            else if (sourceNode instanceof SampleNode sampleNode) {
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

            Optional<RowExpression> translatedFilter = staticFilters.map(filter -> toRowExpression(filter, sourceLayout));
            List<RowExpression> translatedProjections = projections.stream()
                    .map(expression -> toRowExpression(expression, sourceLayout))
                    .collect(toImmutableList());

            try {
                boolean columnarFilterEvaluationEnabled = isColumnarFilterEvaluationEnabled(session);
                Optional<DynamicPageFilter> dynamicPageFilterFactory = Optional.empty();
                if (dynamicFilter != DynamicFilter.EMPTY && isEnableDynamicRowFiltering(session)) {
                    dynamicPageFilterFactory = Optional.of(new DynamicPageFilter(
                            plannerContext,
                            session,
                            ((TableScanNode) sourceNode).getAssignments(),
                            sourceLayout,
                            getDynamicRowFilterSelectivityThreshold(session)));
                }
                Function<DynamicFilter, PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(
                        columnarFilterEvaluationEnabled,
                        translatedFilter,
                        dynamicPageFilterFactory,
                        translatedProjections,
                        Optional.of(context.getStageId() + "_" + planNodeId),
                        OptionalInt.empty());

                if (columns != null) {
                    Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(translatedFilter, translatedProjections, sourceNode.getId());

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode.getId(),
                            pageSourceManager,
                            cursorProcessor,
                            pageProcessor,
                            table,
                            columns,
                            dynamicFilter,
                            getTypes(projections),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings);
                }

                OperatorFactory operatorFactory = FilterAndProjectOperator.createOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        () -> pageProcessor.apply(dynamicFilter),
                        getTypes(projections),
                        getFilterAndProjectMinOutputPageSize(session),
                        getFilterAndProjectMinOutputPageRowCount(session));

                return new PhysicalOperation(operatorFactory, outputMappings, source);
            }
            catch (TrinoException e) {
                throw e;
            }
            catch (RuntimeException e) {
                if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                    throw new TrinoException(QUERY_EXCEEDED_COMPILER_LIMIT,
                            "Compiler failed. Possible reasons include: the query may have too many or too complex expressions, " +
                                    "or the underlying tables may have too many columns", e);
                }
                throw new TrinoException(COMPILER_ERROR, e);
            }
        }

        private RowExpression toRowExpression(Expression expression, Map<Symbol, Integer> layout)
        {
            return SqlToRowExpressionTranslator.translate(expression, layout, metadata, plannerContext.getTypeManager());
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            return visitTableScan(node.getId(), node, TRUE, context);
        }

        private PhysicalOperation visitTableScan(PlanNodeId planNodeId, TableScanNode node, Expression filterExpression, LocalExecutionPlanContext context)
        {
            List<ColumnHandle> columns = new ArrayList<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
            }

            DynamicFilter dynamicFilter = getDynamicFilter(node, filterExpression, context);
            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getNextOperatorId(), planNodeId, node.getId(), pageSourceManager, node.getTable(), columns, dynamicFilter);
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        private Optional<Expression> getStaticFilter(Expression filterExpression)
        {
            DynamicFilters.ExtractResult extractDynamicFilterResult = extractDynamicFilters(filterExpression);
            Expression staticFilter = combineConjuncts(extractDynamicFilterResult.getStaticConjuncts());
            if (staticFilter.equals(TRUE)) {
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
                    plannerContext);
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            // a values node must have a single driver
            context.setDriverInstanceCount(1);

            if (node.getRowCount() == 0) {
                OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of());
                return new PhysicalOperation(operatorFactory, makeLayout(node));
            }

            List<Type> outputTypes = getSymbolTypes(node.getOutputSymbols());
            PageBuilder pageBuilder = new PageBuilder(node.getRowCount(), outputTypes);
            for (int i = 0; i < node.getRowCount(); i++) {
                // declare position for every row
                pageBuilder.declarePosition();
                // evaluate values for non-empty rows
                if (node.getRows().isPresent()) {
                    Expression row = node.getRows().get().get(i);
                    checkState(row.type() instanceof RowType, "unexpected type of Values row: %s", row.type());
                    // evaluate the literal value
                    SqlRow result = (SqlRow) evaluator.evaluate(row, session, ImmutableMap.of());
                    int rawIndex = result.getRawIndex();
                    for (int j = 0; j < outputTypes.size(); j++) {
                        // divide row into fields
                        Block fieldBlock = result.getRawFieldBlock(j);
                        writeNativeValue(outputTypes.get(j), pageBuilder.getBlockBuilder(j), readNativeValue(outputTypes.get(j), fieldBlock, rawIndex));
                    }
                }
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of(pageBuilder.build()));
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitUnnest(UnnestNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableList.Builder<Type> replicateTypes = ImmutableList.builder();
            for (Symbol symbol : node.getReplicateSymbols()) {
                replicateTypes.add(symbol.type());
            }

            List<Symbol> unnestSymbols = node.getMappings().stream()
                    .map(UnnestNode.Mapping::getInput)
                    .collect(toImmutableList());

            ImmutableList.Builder<Type> unnestTypes = ImmutableList.builder();
            for (Symbol symbol : unnestSymbols) {
                unnestTypes.add(symbol.type());
            }
            Optional<Symbol> ordinalitySymbol = node.getOrdinalitySymbol();
            Optional<Type> ordinalityType = ordinalitySymbol.map(Symbol::type);
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
            OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    replicateChannels,
                    replicateTypes.build(),
                    unnestChannels,
                    unnestTypes.build(),
                    ordinalityType.isPresent(),
                    outer);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
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
                    overlappingFieldSetsBuilder.add(ImmutableSet.copyOf(potentialProbeInputs));
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
            return new PhysicalOperation(operatorFactory, makeLayout(node));
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
                    hashStrategyCompiler,
                    blockTypeOperators);

            indexLookupSourceFactory.setTaskContext(context.taskContext);
            JoinBridgeManager<LookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    false,
                    indexLookupSourceFactory,
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
            // We use spilling operator since Non-spilling one does not support index lookup sources
            lookupJoinOperatorFactory = switch (node.getType()) {
                case INNER -> spillingJoin(
                        JoinOperatorType.innerJoin(false, false),
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactoryManager,
                        probeSource.getTypes(),
                        probeChannels,
                        probeHashChannel,
                        Optional.empty(),
                        totalOperatorsCount,
                        unsupportedPartitioningSpillerFactory(),
                        typeOperators);
                case SOURCE_OUTER -> spillingJoin(
                        JoinOperatorType.probeOuterJoin(false),
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactoryManager,
                        probeSource.getTypes(),
                        probeChannels,
                        probeHashChannel,
                        Optional.empty(),
                        totalOperatorsCount,
                        unsupportedPartitioningSpillerFactory(),
                        typeOperators);
            };
            return new PhysicalOperation(lookupJoinOperatorFactory, outputMappings.buildOrThrow(), probeSource);
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

            return switch (node.getType()) {
                case INNER, LEFT, RIGHT, FULL ->
                        createLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(), node.getRight(), rightSymbols, node.getRightHashSymbol(), localDynamicFilters, context);
            };
        }

        @Override
        public PhysicalOperation visitSpatialJoin(SpatialJoinNode node, LocalExecutionPlanContext context)
        {
            Expression filterExpression = node.getFilter();
            List<Call> spatialFunctions = extractSupportedSpatialFunctions(filterExpression);
            for (Call spatialFunction : spatialFunctions) {
                Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialFunction), spatialFunction, Optional.empty(), Optional.empty());
                if (operation.isPresent()) {
                    return operation.get();
                }
            }

            List<Comparison> spatialComparisons = extractSupportedSpatialComparisons(filterExpression);
            for (Comparison spatialComparison : spatialComparisons) {
                if (spatialComparison.operator() == LESS_THAN || spatialComparison.operator() == LESS_THAN_OR_EQUAL) {
                    // ST_Distance(a, b) <= r
                    Expression radius = spatialComparison.right();
                    if (radius instanceof Reference && getSymbolReferences(node.getRight().getOutputSymbols()).contains(radius) || radius instanceof Constant) {
                        Call spatialFunction = (Call) spatialComparison.left();
                        Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialComparison), spatialFunction, Optional.of(radius), Optional.of(spatialComparison.operator()));
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
                Call spatialFunction,
                Optional<Expression> radius,
                Optional<Comparison.Operator> comparisonOperator)
        {
            List<Expression> arguments = spatialFunction.arguments();
            verify(arguments.size() == 2);

            if (!(arguments.get(0) instanceof Reference firstSymbol) || !(arguments.get(1) instanceof Reference secondSymbol)) {
                return Optional.empty();
            }

            PlanNode probeNode = node.getLeft();
            Set<Reference> probeSymbols = getSymbolReferences(probeNode.getOutputSymbols());

            PlanNode buildNode = node.getRight();
            Set<Reference> buildSymbols = getSymbolReferences(buildNode.getOutputSymbols());

            Optional<Symbol> radiusSymbol = Optional.empty();
            OptionalDouble constantRadius = OptionalDouble.empty();
            if (radius.isPresent()) {
                Expression expression = radius.get();
                if (expression instanceof Reference reference) {
                    radiusSymbol = Optional.of(Symbol.from(reference));
                }
                else if (expression instanceof Constant constant) {
                    constantRadius = OptionalDouble.of((Double) constant.value());
                }
                else {
                    throw new IllegalArgumentException("Unexpected expression for radius: " + expression);
                }
            }

            if (probeSymbols.contains(firstSymbol) && buildSymbols.contains(secondSymbol)) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        Symbol.from(firstSymbol),
                        buildNode,
                        Symbol.from(secondSymbol),
                        radiusSymbol,
                        constantRadius,
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
                        radiusSymbol,
                        constantRadius,
                        spatialTest(spatialFunction, false, comparisonOperator),
                        filterExpression,
                        context));
            }
            return Optional.empty();
        }

        private Optional<Expression> removeExpressionFromFilter(Expression filter, Expression expression)
        {
            Expression updatedJoinFilter = replaceExpression(filter, ImmutableMap.of(expression, TRUE));
            return updatedJoinFilter.equals(TRUE) ? Optional.empty() : Optional.of(updatedJoinFilter);
        }

        private SpatialPredicate spatialTest(Call call, boolean probeFirst, Optional<Comparison.Operator> comparisonOperator)
        {
            CatalogSchemaFunctionName functionName = call.function().name();
            if (functionName.equals(builtinFunctionName(ST_CONTAINS))) {
                if (probeFirst) {
                    return (buildGeometry, probeGeometry, radius) -> probeGeometry.contains(buildGeometry);
                }
                return (buildGeometry, probeGeometry, radius) -> buildGeometry.contains(probeGeometry);
            }
            if (functionName.equals(builtinFunctionName(ST_WITHIN))) {
                if (probeFirst) {
                    return (buildGeometry, probeGeometry, radius) -> probeGeometry.within(buildGeometry);
                }
                return (buildGeometry, probeGeometry, radius) -> buildGeometry.within(probeGeometry);
            }
            if (functionName.equals(builtinFunctionName(ST_INTERSECTS))) {
                return (buildGeometry, probeGeometry, radius) -> buildGeometry.intersects(probeGeometry);
            }
            if (functionName.equals(builtinFunctionName(ST_DISTANCE))) {
                if (comparisonOperator.orElseThrow() == LESS_THAN) {
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) < radius.getAsDouble();
                }
                if (comparisonOperator.get() == LESS_THAN_OR_EQUAL) {
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) <= radius.getAsDouble();
                }
                throw new UnsupportedOperationException("Unsupported comparison operator: " + comparisonOperator.get());
            }
            throw new UnsupportedOperationException("Unsupported spatial function: " + functionName);
        }

        private Set<Reference> getSymbolReferences(Collection<Symbol> symbols)
        {
            return symbols.stream().map(Symbol::toSymbolReference).collect(toImmutableSet());
        }

        private PhysicalOperation createNestedLoopJoin(JoinNode node, Set<DynamicFilterId> localDynamicFilters, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getLeft().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getRight().accept(this, buildContext);

            checkArgument(node.getType() == INNER, "NestedLoopJoin is only used for inner join");

            JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                    false,
                    new NestedLoopJoinPagesSupplier(),
                    buildSource.getTypes());
            NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    nestedLoopJoinBridgeManager);

            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int operatorId = buildContext.getNextOperatorId();
            boolean partitioned = !isBuildSideReplicated(node);
            Optional<LocalDynamicFilterConsumer> localDynamicFilter = createDynamicFilter(buildSource, node, context, localDynamicFilters, partitioned);
            if (localDynamicFilter.isPresent()) {
                buildSource = createDynamicFilterSourceOperatorFactory(
                        operatorId,
                        localDynamicFilter.get(),
                        node,
                        partitioned,
                        buildContext.getDriverInstanceCount().orElse(1) == 1,
                        buildSource);
            }

            context.addDriverFactory(
                    false,
                    new PhysicalOperation(nestedLoopBuildOperatorFactory, buildSource),
                    buildContext);

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
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), probeSource);
        }

        private PhysicalOperation createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                Symbol probeSymbol,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                OptionalDouble constantRadius,
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
                    constantRadius,
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

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), probeSource);
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
                OptionalDouble constantRadius,
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
                            buildLayout));

            Optional<Integer> partitionChannel = node.getRightPartitionSymbol().map(buildChannelGetter);

            SpatialIndexBuilderOperatorFactory builderOperatorFactory = new SpatialIndexBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes(),
                    buildOutputChannels,
                    buildChannel,
                    radiusChannel,
                    constantRadius,
                    partitionChannel,
                    spatialRelationshipTest,
                    node.getKdbTree(),
                    filterFunctionFactory,
                    10_000,
                    pagesIndexFactory);

            context.addDriverFactory(
                    false,
                    new PhysicalOperation(builderOperatorFactory, buildSource),
                    buildContext);

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
                    && !buildOuter;

            boolean consumedLocalDynamicFilters = !localDynamicFilters.isEmpty();
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

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(node.getRightOutputSymbols(), buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashSymbol.map(channelGetter(buildSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);

            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildLayout));

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter()
                    .flatMap(filter -> extractSortExpression(ImmutableSet.copyOf(node.getRight().getOutputSymbols()), filter));

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
                                    buildLayout))
                            .collect(toImmutableList()))
                    .orElse(ImmutableList.of());

            ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            List<Type> buildTypes = buildSource.getTypes();
            int operatorId = buildContext.getNextOperatorId();
            boolean partitioned = !isBuildSideReplicated(node);
            Optional<LocalDynamicFilterConsumer> localDynamicFilter = createDynamicFilter(buildSource, node, context, localDynamicFilters, partitioned);
            if (localDynamicFilter.isPresent()) {
                buildSource = createDynamicFilterSourceOperatorFactory(
                        operatorId,
                        localDynamicFilter.get(),
                        node,
                        partitioned,
                        buildContext.getDriverInstanceCount().orElse(1) == 1,
                        buildSource);
            }

            int taskConcurrency = getTaskConcurrency(session);

            // Wait for build side to be collected before local dynamic filters are
            // consumed by table scan. This way table scan can filter data more efficiently.
            boolean waitForBuild = consumedLocalDynamicFilters;
            OperatorFactory operator;
            if (useSpillingJoinOperator(spillEnabled, session)) {
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = new JoinBridgeManager<>(
                        buildOuter,
                        new PartitionedLookupSourceFactory(
                                buildTypes,
                                buildOutputTypes,
                                buildChannels.stream()
                                        .map(buildTypes::get)
                                        .collect(toImmutableList()),
                                partitionCount,
                                buildOuter,
                                typeOperators),
                        buildOutputTypes);

                OperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                        buildContext.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
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
                        false,
                        new PhysicalOperation(hashBuilderOperatorFactory, buildSource),
                        buildContext);

                JoinOperatorType joinType = JoinOperatorType.ofJoinNodeType(node.getType(), outputSingleMatch, waitForBuild);
                operator = spillingJoin(
                        joinType,
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
                        probeTypes,
                        probeJoinChannels,
                        probeHashChannel,
                        Optional.of(probeOutputChannels),
                        totalOperatorsCount,
                        partitioningSpillerFactory,
                        typeOperators);
            }
            else {
                JoinBridgeManager<io.trino.operator.join.unspilled.PartitionedLookupSourceFactory> lookupSourceFactory = new JoinBridgeManager<>(
                        buildOuter,
                        new io.trino.operator.join.unspilled.PartitionedLookupSourceFactory(
                                buildTypes,
                                buildOutputTypes,
                                buildChannels.stream()
                                        .map(buildTypes::get)
                                        .collect(toImmutableList()),
                                partitionCount,
                                buildOuter,
                                typeOperators),
                        buildOutputTypes);

                OperatorFactory hashBuilderOperatorFactory = new HashBuilderOperator.HashBuilderOperatorFactory(
                        buildContext.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
                        buildOutputChannels,
                        buildChannels,
                        buildHashChannel,
                        filterFunctionFactory,
                        sortChannel,
                        searchFunctionFactories,
                        10_000,
                        pagesIndexFactory,
                        incrementalLoadFactorHashArraySizeSupplier(
                                session,
                                // scale load factor in case partition count (and number of hash build operators)
                                // is reduced (e.g. by plan rule) with respect to default task concurrency
                                taskConcurrency / partitionCount));

                context.addDriverFactory(
                        false,
                        new PhysicalOperation(hashBuilderOperatorFactory, buildSource),
                        buildContext);

                JoinOperatorType joinType = JoinOperatorType.ofJoinNodeType(node.getType(), outputSingleMatch, waitForBuild);
                operator = join(
                        joinType,
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
                        node.getFilter().isPresent(),
                        probeTypes,
                        probeJoinChannels,
                        probeHashChannel,
                        Optional.of(probeOutputChannels),
                        typeOperators);
            }

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), probeSource);
        }

        @Override
        public PhysicalOperation visitDynamicFilterSource(DynamicFilterSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(
                    !node.getDynamicFilters().isEmpty(),
                    "Dynamic filters cannot be empty in DynamicFilterSourceNode");
            log.debug("[DynamicFilterSource] Dynamic filters: %s", node.getDynamicFilters());
            PhysicalOperation source = node.getSource().accept(this, context);

            Map<DynamicFilterId, Integer> dynamicFilterChannels = node.getDynamicFilters().entrySet().stream()
                    .collect(toImmutableMap(
                            // Dynamic filter ID
                            Map.Entry::getKey,
                            // Build-side channel index
                            entry -> {
                                Symbol buildSymbol = entry.getValue();
                                int buildChannelIndex = node.getOutputSymbols().indexOf(buildSymbol);
                                verify(buildChannelIndex >= 0);
                                return buildChannelIndex;
                            }));
            Map<DynamicFilterId, Type> dynamicFilterChannelTypes = dynamicFilterChannels.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            entry -> source.getTypes().get(entry.getValue())));

            TaskContext taskContext = context.getTaskContext();
            LocalDynamicFilterConsumer dynamicFilterSourceConsumer = new LocalDynamicFilterConsumer(
                    dynamicFilterChannels,
                    dynamicFilterChannelTypes,
                    // In fault-tolerant execution, all tasks need to collect dynamic filters even if the join has
                    // broadcast distribution type because the collection takes place before the remote exchange
                    ImmutableList.of(taskContext::updateDomains),
                    getDynamicFilteringMaxSizePerOperator(session, false));
            return createDynamicFilterSourceOperatorFactory(
                    context.getNextOperatorId(),
                    dynamicFilterSourceConsumer,
                    node,
                    false,
                    false,
                    source);
        }

        private PhysicalOperation createDynamicFilterSourceOperatorFactory(
                int operatorId,
                LocalDynamicFilterConsumer dynamicFilter,
                PlanNode node,
                boolean partitioned,
                boolean isBuildSideSingle,
                PhysicalOperation buildSource)
        {
            List<DynamicFilterSourceOperator.Channel> filterBuildChannels = dynamicFilter.getBuildChannels().entrySet().stream()
                    .map(entry -> {
                        DynamicFilterId filterId = entry.getKey();
                        int index = entry.getValue();
                        Type type = buildSource.getTypes().get(index);
                        return new DynamicFilterSourceOperator.Channel(filterId, type, index);
                    })
                    .collect(toImmutableList());
            int taskConcurrency = getTaskConcurrency(session);
            return new PhysicalOperation(
                    new DynamicFilterSourceOperatorFactory(
                            operatorId,
                            node.getId(),
                            dynamicFilter,
                            filterBuildChannels,
                            multipleIf(getDynamicFilteringMaxDistinctValuesPerDriver(session, partitioned), taskConcurrency, isBuildSideSingle),
                            multipleIf(getDynamicFilteringMaxSizePerDriver(session, partitioned), taskConcurrency, isBuildSideSingle),
                            multipleIf(getDynamicFilteringRangeRowLimitPerDriver(session, partitioned), taskConcurrency, isBuildSideSingle),
                            typeOperators),
                    buildSource.getLayout(),
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
                Set<DynamicFilterId> localDynamicFilters,
                boolean partitioned)
        {
            Set<DynamicFilterId> coordinatorDynamicFilters = getCoordinatorDynamicFilters(node.getDynamicFilters().keySet(), node, context.getTaskId());
            Set<DynamicFilterId> collectedDynamicFilters = ImmutableSet.<DynamicFilterId>builder()
                    .addAll(localDynamicFilters)
                    .addAll(coordinatorDynamicFilters)
                    .build();
            if (collectedDynamicFilters.isEmpty()) {
                return Optional.empty();
            }
            log.debug("[Join] Dynamic filters: %s", node.getDynamicFilters());
            ImmutableList.Builder<Consumer<Map<DynamicFilterId, Domain>>> collectors = ImmutableList.builder();
            TaskContext taskContext = context.getTaskContext();
            if (!localDynamicFilters.isEmpty()) {
                collectors.add(taskContext::addDynamicFilter);
            }
            if (!coordinatorDynamicFilters.isEmpty()) {
                collectors.add(getCoordinatorDynamicFilterDomainsCollector(taskContext, coordinatorDynamicFilters));
            }
            LocalDynamicFilterConsumer filterConsumer = LocalDynamicFilterConsumer.create(
                    node,
                    buildSource.getTypes(),
                    collectedDynamicFilters,
                    collectors.build(),
                    getDynamicFilteringMaxSizePerOperator(session, partitioned));

            return Optional.of(filterConsumer);
        }

        private JoinFilterFunctionFactory compileJoinFilterFunction(
                Expression filterExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);

            RowExpression translatedFilter = toRowExpression(filterExpression, joinSourcesLayout);
            return joinFilterFunctionCompiler.compileJoinFilterFunction(translatedFilter, buildLayout.size());
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
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            int operatorId = buildContext.getNextOperatorId();
            if (isLocalDynamicFilter || isCoordinatorDynamicFilter) {
                // Add a DynamicFilterSourceOperatorFactory to build operator factories
                DynamicFilterId filterId = node.getDynamicFilterId().get();
                log.debug("[Semi-join] Dynamic filter: %s", filterId);
                ImmutableList.Builder<Consumer<Map<DynamicFilterId, Domain>>> collectors = ImmutableList.builder();
                TaskContext taskContext = context.getTaskContext();
                if (isLocalDynamicFilter) {
                    collectors.add(taskContext::addDynamicFilter);
                }
                if (isCoordinatorDynamicFilter) {
                    collectors.add(getCoordinatorDynamicFilterDomainsCollector(taskContext, ImmutableSet.of(filterId)));
                }
                boolean partitioned = !isBuildSideReplicated(node);
                LocalDynamicFilterConsumer filterConsumer = new LocalDynamicFilterConsumer(
                        ImmutableMap.of(filterId, buildChannel),
                        ImmutableMap.of(filterId, buildSource.getTypes().get(buildChannel)),
                        collectors.build(),
                        getDynamicFilteringMaxSizePerOperator(session, partitioned));
                buildSource = new PhysicalOperation(
                        new DynamicFilterSourceOperatorFactory(
                                operatorId,
                                node.getId(),
                                filterConsumer,
                                ImmutableList.of(new DynamicFilterSourceOperator.Channel(filterId, buildSource.getTypes().get(buildChannel), buildChannel)),
                                getDynamicFilteringMaxDistinctValuesPerDriver(session, partitioned),
                                getDynamicFilteringMaxSizePerDriver(session, partitioned),
                                getDynamicFilteringRangeRowLimitPerDriver(session, partitioned),
                                typeOperators),
                        buildSource.getLayout(),
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
                    typeOperators);
            SetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            context.addDriverFactory(
                    false,
                    new PhysicalOperation(setBuilderOperatorFactory, buildSource),
                    buildContext);

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .buildOrThrow();

            OperatorFactory operator = HashSemiJoinOperator.createOperatorFactory(context.getNextOperatorId(), node.getId(), setProvider, probeSource.getTypes(), probeChannel, probeHashChannel);
            return new PhysicalOperation(operator, outputMappings, probeSource);
        }

        private static Set<DynamicFilterId> getCoordinatorDynamicFilters(Set<DynamicFilterId> dynamicFilters, PlanNode node, TaskId taskId)
        {
            if (!isBuildSideReplicated(node) || taskId.getPartitionId() == 0) {
                // replicated dynamic filters are collected by single stage task only
                return dynamicFilters;
            }

            return ImmutableSet.of();
        }

        private static Consumer<Map<DynamicFilterId, Domain>> getCoordinatorDynamicFilterDomainsCollector(TaskContext taskContext, Set<DynamicFilterId> coordinatorDynamicFilters)
        {
            return domains -> taskContext.updateDomains(
                    domains.entrySet().stream()
                            .filter(entry -> coordinatorDynamicFilters.contains(entry.getKey()))
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
        }

        @Override
        public PhysicalOperation visitRefreshMaterializedView(RefreshMaterializedViewNode node, LocalExecutionPlanContext context)
        {
            context.setDriverInstanceCount(1);
            OperatorFactory operatorFactory = new RefreshMaterializedViewOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, node.getViewName());
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            int maxWriterCount = getWriterCount(
                    session,
                    node.getTarget().getWriterScalingOptions(metadata, session),
                    node.getPartitioningScheme(),
                    node.getSource());
            context.setDriverInstanceCount(maxWriterCount);
            context.taskContext.setMaxWriterCount(maxWriterCount);

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
            }).orElseGet(() -> new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    session,
                    statisticsAggregation,
                    getSymbolTypes(node.getOutputSymbols()));

            return new PhysicalOperation(operatorFactory, outputMapping.buildOrThrow(), source);
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
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
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
            }).orElseGet(() -> new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            Map<Symbol, Integer> aggregationOutput = outputMapping.buildOrThrow();
            StatisticAggregationsDescriptor<Integer> descriptor = node.getStatisticsAggregationDescriptor()
                    .map(desc -> desc.map(aggregationOutput::get))
                    .orElseGet(StatisticAggregationsDescriptor::empty);

            OperatorFactory operatorFactory = new TableFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    createTableFinisher(session, node, metadata),
                    statisticsAggregation,
                    descriptor,
                    tableExecuteContextManager,
                    shouldOutputRowCount(node),
                    session);
            Map<Symbol, Integer> layout = ImmutableMap.of(getOnlyElement(node.getOutputSymbols()), 0);

            return new PhysicalOperation(operatorFactory, layout, source);
        }

        @Override
        public PhysicalOperation visitSimpleTableExecuteNode(SimpleTableExecuteNode node, LocalExecutionPlanContext context)
        {
            context.setDriverInstanceCount(1);
            SimpleTableExecuteOperatorOperatorFactory operatorFactory =
                    new SimpleTableExecuteOperatorOperatorFactory(
                            context.getNextOperatorId(),
                            node.getId(),
                            metadata,
                            session,
                            node.getExecuteHandle());

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitTableExecute(TableExecuteNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            int maxWriterCount = getWriterCount(
                    session,
                    node.getTarget().getWriterScalingOptions(metadata, session),
                    node.getPartitioningScheme(),
                    node.getSource());
            context.setDriverInstanceCount(maxWriterCount);
            context.taskContext.setMaxWriterCount(maxWriterCount);

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
                    session,
                    new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()), // statistics are not calculated
                    getSymbolTypes(node.getOutputSymbols()));

            return new PhysicalOperation(operatorFactory, outputMapping.buildOrThrow(), source);
        }

        private int getWriterCount(Session session, WriterScalingOptions connectorScalingOptions, Optional<PartitioningScheme> partitioningScheme, PlanNode source)
        {
            // This check is required because we don't know which writer count to use when exchange is
            // single distribution. It could be possible that when scaling is enabled, a single distribution is
            // selected for partitioned write using "task_max_writer_count". However, we can't say for sure
            // whether this single distribution comes from unpartitioned or partitioned writer count.
            if (isSingleGatheringExchange(source)) {
                return 1;
            }

            if (partitioningScheme.isPresent()) {
                // The default value of partitioned writer count is 2 * number_of_cores (capped to 64) which is high
                // enough to use it for cases with or without scaling enabled. Additionally, it doesn't lead
                // to too many small files when scaling is disabled because single partition will be written by
                // a single writer only.
                int partitionedWriterCount = getTaskMaxWriterCount(session);
                if (isLocalScaledWriterExchange(source)) {
                    partitionedWriterCount = connectorScalingOptions.perTaskMaxScaledWriterCount()
                            .map(writerCount -> min(writerCount, getTaskMaxWriterCount(session)))
                            .orElse(getTaskMaxWriterCount(session));
                }
                return getPartitionedWriterCountBasedOnMemory(partitionedWriterCount, session);
            }

            int unpartitionedWriterCount = getTaskMinWriterCount(session);
            if (isLocalScaledWriterExchange(source)) {
                unpartitionedWriterCount = connectorScalingOptions.perTaskMaxScaledWriterCount()
                        .map(writerCount -> min(writerCount, getTaskMaxWriterCount(session)))
                        .orElse(getTaskMaxWriterCount(session));
            }
            // Consider memory while calculating writer count.
            return min(unpartitionedWriterCount, getMaxWritersBasedOnMemory(session));
        }

        private boolean isSingleGatheringExchange(PlanNode node)
        {
            Optional<PlanNode> result = searchFrom(node)
                    .where(planNode -> planNode instanceof ExchangeNode)
                    .findFirst();

            return result.isPresent()
                    && result.get() instanceof ExchangeNode
                    && ((ExchangeNode) result.get()).getPartitioningScheme().getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION);
        }

        @Override
        public PhysicalOperation visitMergeWriter(MergeWriterNode node, LocalExecutionPlanContext context)
        {
            // Todo: Implement writer scaling for merge. https://github.com/trinodb/trino/issues/14622
            int writerCount = node.getPartitioningScheme()
                    .map(scheme -> getTaskMaxWriterCount(session))
                    .orElseGet(() -> getTaskMinWriterCount(session));
            context.setDriverInstanceCount(writerCount);

            PhysicalOperation source = node.getSource().accept(this, context);

            Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(node.getProjectedSymbols(), source.getLayout());

            OperatorFactory operatorFactory = new MergeWriterOperatorFactory(context.getNextOperatorId(), node.getId(), pageSinkManager, node.getTarget(), session, pagePreprocessor);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitMergeProcessor(MergeProcessorNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Map<Symbol, Integer> nodeLayout = makeLayout(node);
            Map<Symbol, Integer> sourceLayout = makeLayout(node.getSource());
            int rowIdChannel = sourceLayout.get(node.getRowIdSymbol());
            int mergeRowChannel = sourceLayout.get(node.getMergeRowSymbol());

            List<Integer> redistributionColumns = node.getRedistributionColumnSymbols().stream()
                    .map(nodeLayout::get)
                    .collect(toImmutableList());
            List<Integer> dataColumnChannels = node.getDataColumnSymbols().stream()
                    .map(nodeLayout::get)
                    .collect(toImmutableList());

            List<Symbol> expectedLayout = node.getSource().getOutputSymbols();
            Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());

            OperatorFactory operatorFactory = MergeProcessorOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getTarget().getMergeParadigmAndTypes(),
                    rowIdChannel,
                    mergeRowChannel,
                    redistributionColumns,
                    dataColumnChannels,
                    pagePreprocessor);
            return new PhysicalOperation(operatorFactory, nodeLayout, source);
        }

        @Override
        public PhysicalOperation visitTableDelete(TableDeleteNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new TableMutationOperatorFactory(context.getNextOperatorId(), node.getId(), () -> metadata.executeDelete(session, node.getTarget()));

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitTableUpdate(TableUpdateNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new TableMutationOperatorFactory(context.getNextOperatorId(), node.getId(), () -> metadata.executeUpdate(session, node.getTarget()));

            return new PhysicalOperation(operatorFactory, makeLayout(node));
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
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitAssignUniqueId(AssignUniqueId node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = AssignUniqueIdOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
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

        private boolean isLocalScaledWriterExchange(PlanNode node)
        {
            Optional<PlanNode> result = searchFrom(node)
                    .where(planNode -> planNode instanceof ExchangeNode && ((ExchangeNode) planNode).getScope() == LOCAL)
                    .findFirst();

            return result.isPresent()
                    && result.get() instanceof ExchangeNode
                    && ((ExchangeNode) result.get()).getPartitioningScheme().getPartitioning().getHandle().isScaleWriters();
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
            List<Type> types = getSourceOperatorTypes(node);
            LocalExchange localExchange = new LocalExchange(
                    nodePartitioningManager,
                    session,
                    operatorsCount,
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.empty(),
                    maxLocalExchangeBufferSize,
                    typeOperators,
                    getWriterScalingMinDataProcessed(session),
                    () -> context.getTaskContext().getQueryMemoryReservation().toBytes());

            List<Symbol> expectedLayout = getOnlyElement(node.getInputs());
            Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());
            context.addDriverFactory(
                    false,
                    new PhysicalOperation(
                            new LocalExchangeSinkOperatorFactory(
                                    localExchange.createSinkFactory(),
                                    subContext.getNextOperatorId(),
                                    node.getId(),
                                    pagePreprocessor),
                            source),
                    subContext);
            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), layout);
            List<SortOrder> orderings = orderingScheme.orderingList();
            OperatorFactory operatorFactory = new LocalMergeSourceOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    localExchange,
                    types,
                    orderingCompiler,
                    sortChannels,
                    orderings);
            return new PhysicalOperation(operatorFactory, layout);
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

            List<Type> types = getSourceOperatorTypes(node);
            List<Integer> partitionChannels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                    .collect(toImmutableList());
            Optional<Integer> hashChannel = node.getPartitioningScheme().getHashColumn()
                    .map(symbol -> node.getOutputSymbols().indexOf(symbol));
            List<Type> partitionChannelTypes = partitionChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());

            List<DriverFactoryParameters> driverFactoryParametersList = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode sourceNode = node.getSources().get(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = sourceNode.accept(this, subContext);
                driverFactoryParametersList.add(new DriverFactoryParameters(subContext, source));
            }

            LocalExchange localExchange = new LocalExchange(
                    nodePartitioningManager,
                    session,
                    driverInstanceCount,
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    partitionChannels,
                    partitionChannelTypes,
                    hashChannel,
                    maxLocalExchangeBufferSize,
                    typeOperators,
                    getWriterScalingMinDataProcessed(session),
                    () -> context.getTaskContext().getQueryMemoryReservation().toBytes());
            for (int i = 0; i < node.getSources().size(); i++) {
                DriverFactoryParameters driverFactoryParameters = driverFactoryParametersList.get(i);
                PhysicalOperation source = driverFactoryParameters.getSource();
                LocalExecutionPlanContext subContext = driverFactoryParameters.getSubContext();

                List<Symbol> expectedLayout = node.getInputs().get(i);
                Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());

                context.addDriverFactory(
                        false,
                        new PhysicalOperation(
                                new LocalExchangeSinkOperatorFactory(
                                        localExchange.createSinkFactory(),
                                        subContext.getNextOperatorId(),
                                        node.getId(),
                                        pagePreprocessor),
                                source),
                        subContext);
            }

            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            // instance count must match the number of partitions in the exchange
            verify(context.getDriverInstanceCount().getAsInt() == localExchange.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            return new PhysicalOperation(new LocalExchangeSourceOperatorFactory(context.getNextOperatorId(), node.getId(), localExchange), makeLayout(node));
        }

        @Override
        public PhysicalOperation visitAdaptivePlanNode(AdaptivePlanNode node, LocalExecutionPlanContext context)
        {
            return node.getCurrentPlan().accept(this, context);
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<Type> getSourceOperatorTypes(PlanNode node)
        {
            return getSymbolTypes(node.getOutputSymbols());
        }

        private List<Type> getSymbolTypes(List<Symbol> symbols)
        {
            return symbols.stream()
                    .map(Symbol::type)
                    .collect(toImmutableList());
        }

        private AggregatorFactory buildAggregatorFactory(
                PhysicalOperation source,
                Aggregation aggregation,
                Step step)
        {
            List<Integer> argumentChannels = new ArrayList<>();
            for (Expression argument : aggregation.getArguments()) {
                if (!(argument instanceof Lambda)) {
                    Symbol argumentSymbol = Symbol.from(argument);
                    argumentChannels.add(source.getLayout().get(argumentSymbol));
                }
            }

            ResolvedFunction resolvedFunction = aggregation.getResolvedFunction();
            AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(aggregation.getResolvedFunction());
            AccumulatorFactory accumulatorFactory = uncheckedCacheGet(
                    accumulatorFactoryCache,
                    new FunctionKey(resolvedFunction.functionId(), resolvedFunction.signature()),
                    () -> generateAccumulatorFactory(
                            resolvedFunction.signature(),
                            aggregationImplementation,
                            resolvedFunction.functionNullability(),
                            specializeAggregationLoops));

            if (aggregation.isDistinct()) {
                accumulatorFactory = new DistinctAccumulatorFactory(
                        accumulatorFactory,
                        argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList()),
                        hashStrategyCompiler,
                        session);
            }

            if (aggregation.getOrderingScheme().isPresent()) {
                List<Integer> inputArgumentChannels = range(0, argumentChannels.size())
                        .boxed()
                        .collect(toImmutableList());

                OrderingScheme orderingScheme = aggregation.getOrderingScheme().get();
                List<Symbol> sortKeys = orderingScheme.orderBy();

                List<SortOrder> sortOrders = sortKeys.stream()
                        .map(orderingScheme::ordering)
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

            ImmutableList<Type> intermediateTypes = aggregationImplementation.getAccumulatorStateDescriptors().stream()
                    .map(stateDescriptor -> stateDescriptor.getSerializer().getSerializedType())
                    .collect(toImmutableList());
            Type intermediateType = (intermediateTypes.size() == 1) ? getOnlyElement(intermediateTypes) : RowType.anonymous(intermediateTypes);
            Type finalType = resolvedFunction.signature().getReturnType();

            OptionalInt maskChannel = aggregation.getMask().stream()
                    .mapToInt(value -> source.getLayout().get(value))
                    .findAny();

            List<Lambda> lambdas = aggregation.getArguments().stream()
                    .filter(Lambda.class::isInstance)
                    .map(Lambda.class::cast)
                    .collect(toImmutableList());
            List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                    .filter(FunctionType.class::isInstance)
                    .map(FunctionType.class::cast)
                    .collect(toImmutableList());
            List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, aggregationImplementation.getLambdaInterfaces(), functionTypes);

            return new AggregatorFactory(
                    accumulatorFactory,
                    step,
                    intermediateType,
                    finalType,
                    argumentChannels,
                    maskChannel,
                    !aggregation.isDistinct() && aggregation.getOrderingScheme().isEmpty(),
                    lambdaProviders);
        }

        private List<Supplier<Object>> makeLambdaProviders(List<Lambda> lambdas, List<Class<?>> lambdaInterfaces, List<FunctionType> functionTypes)
        {
            List<Supplier<Object>> lambdaProviders = new ArrayList<>();
            if (!lambdas.isEmpty()) {
                verify(lambdas.size() == functionTypes.size());
                verify(lambdas.size() == lambdaInterfaces.size());

                for (int i = 0; i < lambdas.size(); i++) {
                    Lambda lambdaExpression = lambdas.get(i);
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
                    verify(lambdaExpression.arguments().size() == functionType.getArgumentTypes().size());

                    LambdaDefinitionExpression lambda = (LambdaDefinitionExpression) toRowExpression(lambdaExpression, ImmutableMap.of());
                    Class<? extends Supplier<Object>> lambdaProviderClass = compileLambdaProvider(lambda, plannerContext.getFunctionManager(), lambdaInterfaces.get(i));
                    try {
                        lambdaProviders.add(lambdaProviderClass.getConstructor(ConnectorSession.class).newInstance(session.toConnectorSession()));
                    }
                    catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
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
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
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
            return new PhysicalOperation(operatorFactory, mappings.buildOrThrow(), source);
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
                    hashStrategyCompiler,
                    typeOperators,
                    createPartialAggregationController(maxPartialAggregationMemorySize, step, session));
        }
    }

    private int getPartitionedWriterCountBasedOnMemory(Session session)
    {
        return getPartitionedWriterCountBasedOnMemory(getTaskMaxWriterCount(session), session);
    }

    private int getPartitionedWriterCountBasedOnMemory(int partitionedWriterCount, Session session)
    {
        return min(partitionedWriterCount, previousPowerOfTwo(getMaxWritersBasedOnMemory(session)));
    }

    private static Optional<PartialAggregationController> createPartialAggregationController(Optional<DataSize> maxPartialAggregationMemorySize, AggregationNode.Step step, Session session)
    {
        return maxPartialAggregationMemorySize.isPresent() && step.isOutputPartial() && isAdaptivePartialAggregationEnabled(session) ?
                Optional.of(new PartialAggregationController(
                        maxPartialAggregationMemorySize.get(),
                        getAdaptivePartialAggregationUniqueRowsRatioThreshold(session))) :
                Optional.empty();
    }

    private int getDynamicFilteringMaxDistinctValuesPerDriver(Session session, boolean partitioned)
    {
        if (isEnableLargeDynamicFilters(session)) {
            if (partitioned) {
                return largePartitionedMaxDistinctValuesPerDriver;
            }
            return largeMaxDistinctValuesPerDriver;
        }
        if (partitioned) {
            return smallPartitionedMaxDistinctValuesPerDriver;
        }
        return smallMaxDistinctValuesPerDriver;
    }

    private DataSize getDynamicFilteringMaxSizePerDriver(Session session, boolean partitioned)
    {
        if (isEnableLargeDynamicFilters(session)) {
            if (partitioned) {
                return largePartitionedMaxSizePerDriver;
            }
            return largeMaxSizePerDriver;
        }
        if (partitioned) {
            return smallPartitionedMaxSizePerDriver;
        }
        return smallMaxSizePerDriver;
    }

    private int getDynamicFilteringRangeRowLimitPerDriver(Session session, boolean partitioned)
    {
        if (isEnableLargeDynamicFilters(session)) {
            if (partitioned) {
                return largePartitionedRangeRowLimitPerDriver;
            }
            return largeRangeRowLimitPerDriver;
        }
        if (partitioned) {
            return smallPartitionedRangeRowLimitPerDriver;
        }
        return smallRangeRowLimitPerDriver;
    }

    private DataSize getDynamicFilteringMaxSizePerOperator(Session session, boolean partitioned)
    {
        if (isEnableLargeDynamicFilters(session)) {
            if (partitioned) {
                return largePartitionedMaxSizePerOperator;
            }
            return largeMaxSizePerOperator;
        }
        if (partitioned) {
            return smallPartitionedMaxSizePerOperator;
        }
        return smallMaxSizePerOperator;
    }

    private static List<Type> getTypes(List<Expression> expressions)
    {
        return expressions.stream()
                .map(Expression::type)
                .collect(toImmutableList());
    }

    private static TableFinisher createTableFinisher(Session session, TableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return (fragments, statistics, tableExecuteContext) -> {
            if (target instanceof CreateTarget) {
                return metadata.finishCreateTable(session, ((CreateTarget) target).getHandle(), fragments, statistics);
            }
            if (target instanceof InsertTarget insertTarget) {
                return metadata.finishInsert(session, insertTarget.getHandle(), insertTarget.getSourceTableHandles(), fragments, statistics);
            }
            if (target instanceof TableWriterNode.RefreshMaterializedViewTarget refreshTarget) {
                return metadata.finishRefreshMaterializedView(
                        session,
                        refreshTarget.getTableHandle(),
                        refreshTarget.getInsertHandle(),
                        fragments,
                        statistics,
                        refreshTarget.getSourceTableHandles(),
                        refreshTarget.getSourceTableFunctions());
            }
            if (target instanceof TableExecuteTarget) {
                TableExecuteHandle tableExecuteHandle = ((TableExecuteTarget) target).getExecuteHandle();
                metadata.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteContext.getSplitsInfo());
                return Optional.empty();
            }
            if (target instanceof MergeTarget mergeTarget) {
                MergeHandle mergeHandle = mergeTarget.getMergeHandle().orElseThrow(() -> new IllegalArgumentException("mergeHandle not present"));
                metadata.finishMerge(session, mergeHandle, mergeTarget.getSourceTableHandles(), fragments, statistics);
                return Optional.empty();
            }
            throw new AssertionError("Unhandled target type: " + target.getClass().getName());
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
     * Encapsulates a physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private final List<OperatorFactory> operatorFactories;
        private final Map<Symbol, Integer> layout;
        private final List<Type> types;

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout)
        {
            this(operatorFactory, layout, Optional.empty());
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, PhysicalOperation source)
        {
            this(operatorFactory, layout, Optional.of(requireNonNull(source, "source is null")));
        }

        public PhysicalOperation(OperatorFactory outputOperatorFactory, PhysicalOperation source)
        {
            this(outputOperatorFactory, ImmutableMap.of(), Optional.of(requireNonNull(source, "source is null")));
        }

        private PhysicalOperation(
                OperatorFactory operatorFactory,
                Map<Symbol, Integer> layout,
                Optional<PhysicalOperation> source)
        {
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");
            requireNonNull(source, "source is null");

            this.types = toTypes(layout);
            this.operatorFactories = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.map(PhysicalOperation::getOperatorFactories).orElse(ImmutableList.of()))
                    .add(operatorFactory)
                    .build();
            this.layout = ImmutableMap.copyOf(layout);
        }

        private static List<Type> toTypes(Map<Symbol, Integer> layout)
        {
            // verify layout covers all values
            int channelCount = layout.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            checkArgument(
                    layout.size() == channelCount && ImmutableSet.copyOf(layout.values()).containsAll(ContiguousSet.create(closedOpen(0, channelCount), integers())),
                    "Layout does not have a symbol for every output channel: %s", layout);
            Map<Integer, Symbol> channelLayout = ImmutableBiMap.copyOf(layout).inverse();

            return range(0, channelCount)
                    .mapToObj(channelLayout::get)
                    .map(symbol -> symbol.type())
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
            return operatorFactories;
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

    private static class FunctionKey
    {
        private final FunctionId functionId;
        private final BoundSignature boundSignature;

        public FunctionKey(FunctionId functionId, BoundSignature boundSignature)
        {
            this.functionId = requireNonNull(functionId, "functionId is null");
            this.boundSignature = requireNonNull(boundSignature, "boundSignature is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionKey that = (FunctionKey) o;
            return functionId.equals(that.functionId) &&
                    boundSignature.equals(that.boundSignature);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionId, boundSignature);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("functionId", functionId)
                    .add("boundSignature", boundSignature)
                    .toString();
        }
    }

    private boolean useSpillingJoinOperator(boolean spillEnabled, Session session)
    {
        return spillEnabled || isForceSpillingOperator(session);
    }
}
