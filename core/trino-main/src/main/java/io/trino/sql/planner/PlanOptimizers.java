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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.SystemSessionProperties;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculator.EstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.TaskManagerConfig;
import io.trino.metadata.Metadata;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.RuleStats;
import io.trino.sql.planner.iterative.rule.AddDynamicFilterSource;
import io.trino.sql.planner.iterative.rule.AddExchangesBelowPartialAggregationOverGroupIdRuleSet;
import io.trino.sql.planner.iterative.rule.AddIntermediateAggregations;
import io.trino.sql.planner.iterative.rule.ApplyTableScanRedirection;
import io.trino.sql.planner.iterative.rule.ArraySortAfterArrayDistinct;
import io.trino.sql.planner.iterative.rule.CanonicalizeExpressions;
import io.trino.sql.planner.iterative.rule.CreatePartialTopN;
import io.trino.sql.planner.iterative.rule.DecorrelateInnerUnnestWithGlobalAggregation;
import io.trino.sql.planner.iterative.rule.DecorrelateLeftUnnestWithGlobalAggregation;
import io.trino.sql.planner.iterative.rule.DecorrelateUnnest;
import io.trino.sql.planner.iterative.rule.DesugarLambdaExpression;
import io.trino.sql.planner.iterative.rule.DetermineJoinDistributionType;
import io.trino.sql.planner.iterative.rule.DetermineSemiJoinDistributionType;
import io.trino.sql.planner.iterative.rule.DetermineTableScanNodePartitioning;
import io.trino.sql.planner.iterative.rule.EliminateCrossJoins;
import io.trino.sql.planner.iterative.rule.EvaluateEmptyIntersect;
import io.trino.sql.planner.iterative.rule.EvaluateZeroSample;
import io.trino.sql.planner.iterative.rule.ExtractDereferencesFromFilterAboveScan;
import io.trino.sql.planner.iterative.rule.ExtractSpatialJoins;
import io.trino.sql.planner.iterative.rule.GatherAndMergeWindows;
import io.trino.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
import io.trino.sql.planner.iterative.rule.ImplementExceptAll;
import io.trino.sql.planner.iterative.rule.ImplementExceptDistinctAsUnion;
import io.trino.sql.planner.iterative.rule.ImplementFilteredAggregations;
import io.trino.sql.planner.iterative.rule.ImplementIntersectAll;
import io.trino.sql.planner.iterative.rule.ImplementIntersectDistinctAsUnion;
import io.trino.sql.planner.iterative.rule.ImplementLimitWithTies;
import io.trino.sql.planner.iterative.rule.ImplementOffset;
import io.trino.sql.planner.iterative.rule.ImplementTableFunctionSource;
import io.trino.sql.planner.iterative.rule.InlineProjectIntoFilter;
import io.trino.sql.planner.iterative.rule.InlineProjections;
import io.trino.sql.planner.iterative.rule.MergeExcept;
import io.trino.sql.planner.iterative.rule.MergeFilters;
import io.trino.sql.planner.iterative.rule.MergeIntersect;
import io.trino.sql.planner.iterative.rule.MergeLimitOverProjectWithSort;
import io.trino.sql.planner.iterative.rule.MergeLimitWithDistinct;
import io.trino.sql.planner.iterative.rule.MergeLimitWithSort;
import io.trino.sql.planner.iterative.rule.MergeLimitWithTopN;
import io.trino.sql.planner.iterative.rule.MergeLimits;
import io.trino.sql.planner.iterative.rule.MergePatternRecognitionNodes;
import io.trino.sql.planner.iterative.rule.MergeProjectWithValues;
import io.trino.sql.planner.iterative.rule.MergeUnion;
import io.trino.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import io.trino.sql.planner.iterative.rule.OptimizeDuplicateInsensitiveJoins;
import io.trino.sql.planner.iterative.rule.OptimizeRowPattern;
import io.trino.sql.planner.iterative.rule.PreAggregateCaseAggregations;
import io.trino.sql.planner.iterative.rule.PruneAggregationColumns;
import io.trino.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneApplyColumns;
import io.trino.sql.planner.iterative.rule.PruneApplyCorrelation;
import io.trino.sql.planner.iterative.rule.PruneApplySourceColumns;
import io.trino.sql.planner.iterative.rule.PruneAssignUniqueIdColumns;
import io.trino.sql.planner.iterative.rule.PruneCorrelatedJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneCorrelatedJoinCorrelation;
import io.trino.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import io.trino.sql.planner.iterative.rule.PruneDistinctAggregation;
import io.trino.sql.planner.iterative.rule.PruneDistinctLimitSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneEnforceSingleRowColumns;
import io.trino.sql.planner.iterative.rule.PruneExceptSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneExchangeColumns;
import io.trino.sql.planner.iterative.rule.PruneExchangeSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneExplainAnalyzeSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneFilterColumns;
import io.trino.sql.planner.iterative.rule.PruneGroupIdColumns;
import io.trino.sql.planner.iterative.rule.PruneGroupIdSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneIndexJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneIndexSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneIntersectSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import io.trino.sql.planner.iterative.rule.PruneJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneLimitColumns;
import io.trino.sql.planner.iterative.rule.PruneMarkDistinctColumns;
import io.trino.sql.planner.iterative.rule.PruneMergeSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneOffsetColumns;
import io.trino.sql.planner.iterative.rule.PruneOrderByInAggregation;
import io.trino.sql.planner.iterative.rule.PruneOutputSourceColumns;
import io.trino.sql.planner.iterative.rule.PrunePattenRecognitionColumns;
import io.trino.sql.planner.iterative.rule.PrunePatternRecognitionSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneProjectColumns;
import io.trino.sql.planner.iterative.rule.PruneRowNumberColumns;
import io.trino.sql.planner.iterative.rule.PruneSampleColumns;
import io.trino.sql.planner.iterative.rule.PruneSemiJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneSortColumns;
import io.trino.sql.planner.iterative.rule.PruneSpatialJoinChildrenColumns;
import io.trino.sql.planner.iterative.rule.PruneSpatialJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneTableExecuteSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneTableFunctionProcessorColumns;
import io.trino.sql.planner.iterative.rule.PruneTableFunctionProcessorSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PruneTableWriterSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneTopNColumns;
import io.trino.sql.planner.iterative.rule.PruneTopNRankingColumns;
import io.trino.sql.planner.iterative.rule.PruneUnionColumns;
import io.trino.sql.planner.iterative.rule.PruneUnionSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneUnnestColumns;
import io.trino.sql.planner.iterative.rule.PruneUnnestSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneValuesColumns;
import io.trino.sql.planner.iterative.rule.PruneWindowColumns;
import io.trino.sql.planner.iterative.rule.PushAggregationIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushCastIntoRow;
import io.trino.sql.planner.iterative.rule.PushDistinctLimitIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughFilter;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughJoin;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughProject;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughSemiJoin;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughUnnest;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughAssignUniqueId;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughLimit;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughMarkDistinct;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughRowNumber;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughSort;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughTopN;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughTopNRanking;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughWindow;
import io.trino.sql.planner.iterative.rule.PushDownProjectionsFromPatternRecognition;
import io.trino.sql.planner.iterative.rule.PushFilterThroughCountAggregation;
import io.trino.sql.planner.iterative.rule.PushInequalityFilterExpressionBelowJoinRuleSet;
import io.trino.sql.planner.iterative.rule.PushJoinIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushLimitIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import io.trino.sql.planner.iterative.rule.PushLimitThroughOffset;
import io.trino.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushLimitThroughProject;
import io.trino.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import io.trino.sql.planner.iterative.rule.PushLimitThroughUnion;
import io.trino.sql.planner.iterative.rule.PushMergeWriterDeleteIntoConnector;
import io.trino.sql.planner.iterative.rule.PushOffsetThroughProject;
import io.trino.sql.planner.iterative.rule.PushPartialAggregationThroughExchange;
import io.trino.sql.planner.iterative.rule.PushPartialAggregationThroughJoin;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushPredicateThroughProjectIntoRowNumber;
import io.trino.sql.planner.iterative.rule.PushPredicateThroughProjectIntoWindow;
import io.trino.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushProjectionThroughExchange;
import io.trino.sql.planner.iterative.rule.PushProjectionThroughUnion;
import io.trino.sql.planner.iterative.rule.PushRemoteExchangeThroughAssignUniqueId;
import io.trino.sql.planner.iterative.rule.PushSampleIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushTableWriteThroughUnion;
import io.trino.sql.planner.iterative.rule.PushTopNIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushTopNThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushTopNThroughProject;
import io.trino.sql.planner.iterative.rule.PushTopNThroughUnion;
import io.trino.sql.planner.iterative.rule.PushdownFilterIntoRowNumber;
import io.trino.sql.planner.iterative.rule.PushdownFilterIntoWindow;
import io.trino.sql.planner.iterative.rule.PushdownLimitIntoRowNumber;
import io.trino.sql.planner.iterative.rule.PushdownLimitIntoWindow;
import io.trino.sql.planner.iterative.rule.RemoveAggregationInSemiJoin;
import io.trino.sql.planner.iterative.rule.RemoveDuplicateConditions;
import io.trino.sql.planner.iterative.rule.RemoveEmptyExceptBranches;
import io.trino.sql.planner.iterative.rule.RemoveEmptyGlobalAggregation;
import io.trino.sql.planner.iterative.rule.RemoveEmptyMergeWriterRuleSet;
import io.trino.sql.planner.iterative.rule.RemoveEmptyTableExecute;
import io.trino.sql.planner.iterative.rule.RemoveEmptyUnionBranches;
import io.trino.sql.planner.iterative.rule.RemoveFullSample;
import io.trino.sql.planner.iterative.rule.RemoveRedundantDateTrunc;
import io.trino.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import io.trino.sql.planner.iterative.rule.RemoveRedundantEnforceSingleRowNode;
import io.trino.sql.planner.iterative.rule.RemoveRedundantExists;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.iterative.rule.RemoveRedundantJoin;
import io.trino.sql.planner.iterative.rule.RemoveRedundantLimit;
import io.trino.sql.planner.iterative.rule.RemoveRedundantOffset;
import io.trino.sql.planner.iterative.rule.RemoveRedundantPredicateAboveTableScan;
import io.trino.sql.planner.iterative.rule.RemoveRedundantSort;
import io.trino.sql.planner.iterative.rule.RemoveRedundantSortBelowLimitWithTies;
import io.trino.sql.planner.iterative.rule.RemoveRedundantTableFunction;
import io.trino.sql.planner.iterative.rule.RemoveRedundantTopN;
import io.trino.sql.planner.iterative.rule.RemoveRedundantWindow;
import io.trino.sql.planner.iterative.rule.RemoveTrivialFilters;
import io.trino.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import io.trino.sql.planner.iterative.rule.RemoveUnreferencedScalarSubqueries;
import io.trino.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import io.trino.sql.planner.iterative.rule.ReorderJoins;
import io.trino.sql.planner.iterative.rule.ReplaceJoinOverConstantWithProject;
import io.trino.sql.planner.iterative.rule.ReplaceRedundantJoinWithProject;
import io.trino.sql.planner.iterative.rule.ReplaceRedundantJoinWithSource;
import io.trino.sql.planner.iterative.rule.ReplaceWindowWithRowNumber;
import io.trino.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import io.trino.sql.planner.iterative.rule.RewriteTableFunctionToTableScan;
import io.trino.sql.planner.iterative.rule.SimplifyCountOverConstant;
import io.trino.sql.planner.iterative.rule.SimplifyExpressions;
import io.trino.sql.planner.iterative.rule.SimplifyFilterPredicate;
import io.trino.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedDistinctAggregationWithProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedDistinctAggregationWithoutProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGlobalAggregationWithProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGlobalAggregationWithoutProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGroupedAggregationWithProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedGroupedAggregationWithoutProjection;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedJoinToJoin;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import io.trino.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import io.trino.sql.planner.iterative.rule.TransformExistsApplyToCorrelatedJoin;
import io.trino.sql.planner.iterative.rule.TransformFilteringSemiJoinToInnerJoin;
import io.trino.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import io.trino.sql.planner.iterative.rule.TransformUncorrelatedSubqueryToJoin;
import io.trino.sql.planner.iterative.rule.UnwrapCastInComparison;
import io.trino.sql.planner.iterative.rule.UnwrapDateTruncInComparison;
import io.trino.sql.planner.iterative.rule.UnwrapRowSubscript;
import io.trino.sql.planner.iterative.rule.UnwrapSingleColumnRowInApply;
import io.trino.sql.planner.iterative.rule.UnwrapYearInComparison;
import io.trino.sql.planner.iterative.rule.UseNonPartitionedJoinLookupSource;
import io.trino.sql.planner.optimizations.AddExchanges;
import io.trino.sql.planner.optimizations.AddLocalExchanges;
import io.trino.sql.planner.optimizations.BeginTableWrite;
import io.trino.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.trino.sql.planner.optimizations.DeterminePartitionCount;
import io.trino.sql.planner.optimizations.HashGenerationOptimizer;
import io.trino.sql.planner.optimizations.IndexJoinOptimizer;
import io.trino.sql.planner.optimizations.LimitPushDown;
import io.trino.sql.planner.optimizations.MetadataQueryOptimizer;
import io.trino.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import io.trino.sql.planner.optimizations.OptimizerStats;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.PredicatePushDown;
import io.trino.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import io.trino.sql.planner.optimizations.TransformQuantifiedComparisonApplyToCorrelatedJoin;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.optimizations.WindowFilterPushDown;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PlanOptimizers
        implements PlanOptimizersFactory
{
    private final List<PlanOptimizer> optimizers;
    private final RuleStatsRecorder ruleStats;
    private final OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();

    @Inject
    public PlanOptimizers(
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            ScalarStatsCalculator scalarStatsCalculator,
            CostCalculator costCalculatorWithoutEstimatedExchanges,
            @EstimatedExchanges CostCalculator costCalculatorWithEstimatedExchanges,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            NodePartitioningManager nodePartitioningManager,
            RuleStatsRecorder ruleStats)
    {
        this(plannerContext,
                typeAnalyzer,
                taskManagerConfig,
                false,
                splitManager,
                pageSourceManager,
                statsCalculator,
                scalarStatsCalculator,
                costCalculatorWithoutEstimatedExchanges,
                costCalculatorWithEstimatedExchanges,
                costComparator,
                taskCountEstimator,
                nodePartitioningManager,
                ruleStats);
    }

    public PlanOptimizers(
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            boolean forceSingleNode,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            ScalarStatsCalculator scalarStatsCalculator,
            CostCalculator costCalculatorWithoutEstimatedExchanges,
            CostCalculator costCalculatorWithEstimatedExchanges,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            NodePartitioningManager nodePartitioningManager,
            RuleStatsRecorder ruleStats)
    {
        CostCalculator costCalculator = costCalculatorWithEstimatedExchanges;

        this.ruleStats = requireNonNull(ruleStats, "ruleStats is null");
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        Metadata metadata = plannerContext.getMetadata();
        Set<Rule<?>> columnPruningRules = columnPruningRules(metadata);

        Set<Rule<?>> projectionPushdownRules = ImmutableSet.of(
                new PushProjectionThroughUnion(),
                new PushProjectionThroughExchange(),
                // Dereference pushdown rules
                new PushDownDereferencesThroughMarkDistinct(typeAnalyzer),
                new PushDownDereferenceThroughProject(typeAnalyzer),
                new PushDownDereferenceThroughUnnest(typeAnalyzer),
                new PushDownDereferenceThroughSemiJoin(typeAnalyzer),
                new PushDownDereferenceThroughJoin(typeAnalyzer),
                new PushDownDereferenceThroughFilter(typeAnalyzer),
                new ExtractDereferencesFromFilterAboveScan(typeAnalyzer),
                new PushDownDereferencesThroughLimit(typeAnalyzer),
                new PushDownDereferencesThroughSort(typeAnalyzer),
                new PushDownDereferencesThroughAssignUniqueId(typeAnalyzer),
                new PushDownDereferencesThroughWindow(typeAnalyzer),
                new PushDownDereferencesThroughTopN(typeAnalyzer),
                new PushDownDereferencesThroughRowNumber(typeAnalyzer),
                new PushDownDereferencesThroughTopNRanking(typeAnalyzer));

        Set<Rule<?>> limitPushdownRules = ImmutableSet.of(
                new PushLimitThroughOffset(),
                new PushLimitThroughProject(typeAnalyzer),
                new PushLimitThroughMarkDistinct(),
                new PushLimitThroughOuterJoin(),
                new PushLimitThroughSemiJoin(),
                new PushLimitThroughUnion());

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new InlineProjections(plannerContext, typeAnalyzer),
                        new RemoveRedundantIdentityProjections()));

        Set<Rule<?>> simplifyOptimizerRules = ImmutableSet.<Rule<?>>builder()
                .addAll(new SimplifyExpressions(plannerContext, typeAnalyzer).rules())
                .addAll(new UnwrapRowSubscript().rules())
                .addAll(new PushCastIntoRow().rules())
                .addAll(new UnwrapCastInComparison(plannerContext, typeAnalyzer).rules())
                .addAll(new UnwrapDateTruncInComparison(plannerContext, typeAnalyzer).rules())
                .addAll(new UnwrapYearInComparison(plannerContext, typeAnalyzer).rules())
                .addAll(new RemoveDuplicateConditions(metadata).rules())
                .addAll(new CanonicalizeExpressions(plannerContext, typeAnalyzer).rules())
                .addAll(new RemoveRedundantDateTrunc(plannerContext, typeAnalyzer).rules())
                .addAll(new ArraySortAfterArrayDistinct(plannerContext).rules())
                .add(new RemoveTrivialFilters())
                .build();
        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                simplifyOptimizerRules);

        IterativeOptimizer columnPruningOptimizer = new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                columnPruningRules);

        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .build()),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new CanonicalizeExpressions(plannerContext, typeAnalyzer).rules())
                                .add(new OptimizeRowPattern())
                                .build()),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(columnPruningRules)
                                .addAll(projectionPushdownRules)
                                .addAll(limitPushdownRules)
                                .addAll(new UnwrapRowSubscript().rules())
                                .addAll(new PushCastIntoRow().rules())
                                .addAll(ImmutableSet.of(
                                        new ImplementTableFunctionSource(metadata),
                                        new UnwrapSingleColumnRowInApply(typeAnalyzer),
                                        new RemoveEmptyUnionBranches(),
                                        new EvaluateEmptyIntersect(),
                                        new RemoveEmptyExceptBranches(),
                                        new MergeFilters(metadata),
                                        new InlineProjections(plannerContext, typeAnalyzer),
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroSample(),
                                        new PushOffsetThroughProject(),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitOverProjectWithSort(),
                                        new MergeLimitWithTopN(),
                                        new RemoveTrivialFilters(),
                                        new RemoveRedundantLimit(),
                                        new RemoveRedundantOffset(),
                                        new RemoveRedundantSort(),
                                        new RemoveRedundantSortBelowLimitWithTies(),
                                        new RemoveRedundantTableFunction(),
                                        new RemoveRedundantTopN(),
                                        new RemoveRedundantDistinctLimit(),
                                        new ReplaceRedundantJoinWithSource(),
                                        new RemoveRedundantJoin(),
                                        new ReplaceRedundantJoinWithProject(),
                                        new RemoveRedundantEnforceSingleRowNode(),
                                        new RemoveRedundantExists(),
                                        new RemoveRedundantWindow(),
                                        new ImplementFilteredAggregations(metadata),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata),
                                        new PruneOrderByInAggregation(metadata),
                                        new RewriteSpatialPartitioningAggregation(plannerContext),
                                        new SimplifyCountOverConstant(plannerContext),
                                        new PreAggregateCaseAggregations(plannerContext, typeAnalyzer)))
                                .build()),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new ImplementOffset())),
                simplifyOptimizer,
                new UnaliasSymbolReferences(metadata),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new MergeUnion(),
                                new MergeIntersect(),
                                new MergeExcept(),
                                new PruneDistinctAggregation())),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new ImplementIntersectDistinctAsUnion(metadata),
                                new ImplementExceptDistinctAsUnion(metadata),
                                new ImplementIntersectAll(metadata),
                                new ImplementExceptAll(metadata))),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                columnPruningOptimizer,
                inlineProjections,
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        columnPruningRules),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new TransformExistsApplyToCorrelatedJoin(plannerContext))),
                new TransformQuantifiedComparisonApplyToCorrelatedJoin(metadata),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantEnforceSingleRowNode(),
                                new RemoveUnreferencedScalarSubqueries(),
                                new TransformUncorrelatedSubqueryToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedJoinToJoin(plannerContext),
                                new DecorrelateInnerUnnestWithGlobalAggregation(),
                                new DecorrelateLeftUnnestWithGlobalAggregation(),
                                new DecorrelateUnnest(metadata),
                                new TransformCorrelatedGlobalAggregationWithProjection(plannerContext),
                                new TransformCorrelatedGlobalAggregationWithoutProjection(plannerContext),
                                new TransformCorrelatedDistinctAggregationWithProjection(plannerContext),
                                new TransformCorrelatedDistinctAggregationWithoutProjection(plannerContext),
                                new TransformCorrelatedGroupedAggregationWithProjection(plannerContext),
                                new TransformCorrelatedGroupedAggregationWithoutProjection(plannerContext))),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new ImplementLimitWithTies(metadata), // must be run after DecorrelateUnnest
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(metadata), // must be run after columnPruningOptimizer
                                new TransformCorrelatedScalarSubquery(metadata), // must be run after TransformCorrelatedAggregation rules
                                new TransformCorrelatedJoinToJoin(plannerContext),
                                new ImplementFilteredAggregations(metadata))),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new InlineProjections(plannerContext, typeAnalyzer),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject(),
                                new RemoveAggregationInSemiJoin(),
                                new MergeProjectWithValues(metadata),
                                new ReplaceJoinOverConstantWithProject(metadata))),
                new CheckSubqueryNodesAreRewritten(),
                simplifyOptimizer, // Should run after MergeProjectWithValues
                new StatsRecordingPlanOptimizer(
                        optimizerStats,
                        new PredicatePushDown(plannerContext, typeAnalyzer, false, false)),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new RemoveEmptyUnionBranches(),
                                new EvaluateEmptyIntersect(),
                                new RemoveEmptyExceptBranches(),
                                new TransformFilteringSemiJoinToInnerJoin())), // must run after PredicatePushDown
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .add(new InlineProjectIntoFilter(metadata))
                                .add(new SimplifyFilterPredicate(metadata))
                                .addAll(columnPruningRules)
                                .add(new InlineProjections(plannerContext, typeAnalyzer))
                                .addAll(new PushFilterThroughCountAggregation(plannerContext).rules()) // must run after PredicatePushDown and after TransformFilteringSemiJoinToInnerJoin
                                .build()));

        // Perform redirection before CBO rules to ensure stats from destination connector are used
        // Perform redirection before agg, topN, limit, sample etc. push down into table scan as the destination connector may support a different set of push downs
        // Perform redirection before push down of dereferences into table scan via PushProjectionIntoTableScan
        // Perform redirection after at least one PredicatePushDown and PushPredicateIntoTableScan to allow connector to use pushed down predicates in redirection decision
        // Perform redirection after at least table scan pruning rules because redirected table might have fewer columns
        // PushPredicateIntoTableScan needs to be run again after redirection to ensure predicate push down into destination table scan
        // Column pruning rules need to be run after redirection
        builder.add(
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new ApplyTableScanRedirection(plannerContext),
                                new PruneTableScanColumns(metadata),
                                new PushPredicateIntoTableScan(plannerContext, typeAnalyzer, false))));

        Set<Rule<?>> pushIntoTableScanRulesExceptJoins = ImmutableSet.<Rule<?>>builder()
                .addAll(columnPruningRules)
                .addAll(projectionPushdownRules)
                .add(new PushProjectionIntoTableScan(plannerContext, typeAnalyzer, scalarStatsCalculator))
                .add(new RemoveRedundantIdentityProjections())
                .add(new PushLimitIntoTableScan(metadata))
                .add(new PushPredicateIntoTableScan(plannerContext, typeAnalyzer, false))
                .add(new PushSampleIntoTableScan(metadata))
                .add(new PushAggregationIntoTableScan(plannerContext, typeAnalyzer))
                .add(new PushDistinctLimitIntoTableScan(plannerContext, typeAnalyzer))
                .add(new PushTopNIntoTableScan(metadata))
                .add(new RewriteTableFunctionToTableScan(plannerContext)) // must run after ImplementTableFunctionSource
                .build();
        IterativeOptimizer pushIntoTableScanOptimizer = new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                pushIntoTableScanRulesExceptJoins);
        builder.add(pushIntoTableScanOptimizer);
        builder.add(new UnaliasSymbolReferences(metadata));
        builder.add(pushIntoTableScanOptimizer); // TODO (https://github.com/trinodb/trino/issues/811) merge with the above after migrating UnaliasSymbolReferences to rules

        IterativeOptimizer pushProjectionIntoTableScanOptimizer = new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(projectionPushdownRules)
                        .add(new PushProjectionIntoTableScan(plannerContext, typeAnalyzer, scalarStatsCalculator))
                        .build());

        builder.add(
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        // Temporary hack: separate optimizer step to avoid the sample node being replaced by filter before pushing
                        // it to table scan node
                        ImmutableSet.of(new ImplementBernoulliSampleAsFilter(metadata))),
                columnPruningOptimizer,
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new RemoveEmptyUnionBranches(),
                                new EvaluateEmptyIntersect(),
                                new RemoveEmptyExceptBranches(),
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin(),
                                new ReplaceRedundantJoinWithSource(), // Run this after PredicatePushDown optimizer as it inlines filter constants
                                new MultipleDistinctAggregationToMarkDistinct(taskCountEstimator))), // Run this after aggregation pushdown so that multiple distinct aggregations can be pushed into a connector
                inlineProjections,
                simplifyOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                pushProjectionIntoTableScanOptimizer,
                // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
                // pushdown into the connectors. We invoke PredicatePushdown and PushPredicateIntoTableScan after this
                // to leverage predicate pushdown on projected columns.
                new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(plannerContext, typeAnalyzer, true, false)),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                                .add(new PushPredicateIntoTableScan(plannerContext, typeAnalyzer, false))
                                .build()),
                new UnaliasSymbolReferences(metadata), // Run again because predicate pushdown and projection pushdown might add more projections
                columnPruningOptimizer, // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(plannerContext), // Run this after projections and filters have been fully simplified and pushed down
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        SystemSessionProperties::useLegacyWindowFilterPushdown,
                        ImmutableList.of(new WindowFilterPushDown(plannerContext)),
                        ImmutableSet.of(// should run after DecorrelateUnnest and ImplementLimitWithTies
                                new RemoveEmptyUnionBranches(),
                                new EvaluateEmptyIntersect(),
                                new RemoveEmptyExceptBranches(),
                                new PushdownLimitIntoRowNumber(),
                                new PushdownLimitIntoWindow(),
                                new PushdownFilterIntoRowNumber(plannerContext),
                                new PushdownFilterIntoWindow(plannerContext),
                                new ReplaceWindowWithRowNumber(metadata))),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .addAll(MergePatternRecognitionNodes.rules())
                                .add(new PushPredicateThroughProjectIntoRowNumber(plannerContext))
                                .add(new PushPredicateThroughProjectIntoWindow(plannerContext))
                                .build()),
                inlineProjections,
                columnPruningOptimizer, // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new PushDownProjectionsFromPatternRecognition())),
                new MetadataQueryOptimizer(plannerContext),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new EliminateCrossJoins(plannerContext, typeAnalyzer), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                                new RemoveRedundantJoin())),
                new StatsRecordingPlanOptimizer(
                        optimizerStats,
                        new PredicatePushDown(plannerContext, typeAnalyzer, true, false)),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                                .add(new PushPredicateIntoTableScan(plannerContext, typeAnalyzer, false))
                                .build()),
                pushProjectionIntoTableScanOptimizer,
                // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
                // pushdown into the connectors. Invoke PredicatePushdown and PushPredicateIntoTableScan after this
                // to leverage predicate pushdown on projected columns.
                new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(plannerContext, typeAnalyzer, true, false)),
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                                .add(new PushPredicateIntoTableScan(plannerContext, typeAnalyzer, false))
                                .build()),
                columnPruningOptimizer,
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                // Because ReorderJoins runs only once,
                // PredicatePushDown, columnPruningOptimizer and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new ReorderJoins(plannerContext, costComparator, typeAnalyzer))));

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughProject(typeAnalyzer),
                        new PushTopNThroughOuterJoin(),
                        new PushTopNThroughUnion(),
                        new PushTopNIntoTableScan(metadata))));
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .addAll(new ExtractSpatialJoins(plannerContext, splitManager, pageSourceManager, typeAnalyzer).rules())
                        .add(new InlineProjections(plannerContext, typeAnalyzer))
                        .build()));

        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        // Must run before AddExchanges
                        new PushMergeWriterDeleteIntoConnector(metadata),
                        new DetermineTableScanNodePartitioning(metadata, nodePartitioningManager, taskCountEstimator),
                        // Must run after join reordering because join reordering creates
                        // new join nodes without JoinNode.maySkipOutputDuplicates flag set
                        new OptimizeDuplicateInsensitiveJoins(metadata))));

        // Previous invocations of PushPredicateIntoTableScan do not prune using predicate expression. The invocation in AddExchanges
        // does this pruning - and we may end up with empty union branches after that. We invoke PushPredicateIntoTableScan
        // and rules to remove empty branches here to get empty values node through pushdown and then prune them.
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new PushPredicateIntoTableScan(plannerContext, typeAnalyzer, true),
                        new RemoveEmptyUnionBranches(),
                        new EvaluateEmptyIntersect(),
                        new RemoveEmptyExceptBranches(),
                        new TransformFilteringSemiJoinToInnerJoin())));

        if (!forceSingleNode) {
            builder.add(new IterativeOptimizer(
                    plannerContext,
                    ruleStats,
                    statsCalculator,
                    costCalculator,
                    ImmutableSet.of(
                            new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                            // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                            // to avoid temporarily having an invalid plan
                            new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator))));

            builder.add(new IterativeOptimizer(
                    plannerContext,
                    ruleStats,
                    statsCalculator,
                    costCalculator,
                    ImmutableSet.<Rule<?>>builder()
                            .addAll(pushIntoTableScanRulesExceptJoins)
                            // PushJoinIntoTableScan must run after ReorderJoins (and DetermineJoinDistributionType)
                            // otherwise too early pushdown could prevent optimal plan from being selected.
                            .add(new PushJoinIntoTableScan(plannerContext, typeAnalyzer))
                            // DetermineTableScanNodePartitioning is needed to needs to ensure all table handles have proper partitioning determined
                            // Must run before AddExchanges
                            .add(new DetermineTableScanNodePartitioning(metadata, nodePartitioningManager, taskCountEstimator))
                            .build()));

            builder.add(
                    new IterativeOptimizer(
                            plannerContext,
                            ruleStats,
                            statsCalculator,
                            costCalculator,
                            ImmutableSet.of(new PushTableWriteThroughUnion()))); // Must run before AddExchanges
            // unalias symbols before adding exchanges to use same partitioning symbols in joins, aggregations and other
            // operators that require node partitioning
            builder.add(new UnaliasSymbolReferences(metadata));
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new AddExchanges(plannerContext, typeAnalyzer, statsCalculator, taskCountEstimator)));
            // It can only run after AddExchanges since it estimates the hash partition count for all remote exchanges
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new DeterminePartitionCount(statsCalculator)));
        }

        // use cost calculator without estimated exchanges after AddExchanges
        costCalculator = costCalculatorWithoutEstimatedExchanges;

        builder.add(
                new IterativeOptimizer(
                        plannerContext,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // Run these after table scan is removed by AddExchanges
                                .addAll(RemoveEmptyMergeWriterRuleSet.rules())
                                .add(new RemoveEmptyTableExecute())
                                .build()));

        // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(new StatsRecordingPlanOptimizer(
                optimizerStats,
                new PredicatePushDown(plannerContext, typeAnalyzer, true, false)));
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                        .add(new RemoveRedundantPredicateAboveTableScan(plannerContext, typeAnalyzer))
                        .build()));
        builder.add(pushProjectionIntoTableScanOptimizer);
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.copyOf(new PushInequalityFilterExpressionBelowJoinRuleSet(metadata, typeAnalyzer).rules())));
        // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
        // pushdown into the connectors. Invoke PredicatePushdown and PushPredicateIntoTableScan after this
        // to leverage predicate pushdown on projected columns and to pushdown dynamic filters.
        builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(plannerContext, typeAnalyzer, true, true)));
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                        .add(new PushPredicateIntoTableScan(plannerContext, typeAnalyzer, false))
                        .add(new RemoveRedundantPredicateAboveTableScan(plannerContext, typeAnalyzer))
                        .build()));
        // Remove unsupported dynamic filters introduced by PredicatePushdown. Also, cleanup dynamic filters removed by
        // PushPredicateIntoTableScan and RemoveRedundantPredicateAboveTableScan due to those rules replacing table scans with empty ValuesNode
        builder.add(new RemoveUnsupportedDynamicFilters(plannerContext));
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences(metadata)); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(columnPruningOptimizer);

        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushRemoteExchangeThroughAssignUniqueId())
                        .add(new InlineProjections(plannerContext, typeAnalyzer))
                        .build()));

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(plannerContext, typeAnalyzer));
        // UseNonPartitionedJoinLookupSource needs to run after AddLocalExchanges since it operates on ExchangeNodes added by this optimizer.
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new UseNonPartitionedJoinLookupSource())));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new PushPartialAggregationThroughJoin(),
                        new PushPartialAggregationThroughExchange(plannerContext),
                        new PruneJoinColumns(),
                        new PruneJoinChildrenColumns())));
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(plannerContext, typeAnalyzer, taskCountEstimator, taskManagerConfig).rules()));
        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections())));
        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer(metadata));

        builder.add(new IterativeOptimizer(
                plannerContext,
                ruleStats,
                statsCalculator,
                costCalculator,
                AddDynamicFilterSource.rules()));
        // If AddDynamicFilterSource fails to rewrite dynamic filter from JoinNode/SemiJoinNode to DynamicFilterSourceNode,
        // RemoveUnsupportedDynamicFilters needs to be run again to clean up the corresponding dynamic filter expression left over the table scan.
        builder.add(new RemoveUnsupportedDynamicFilters(plannerContext));

        builder.add(new BeginTableWrite(metadata, plannerContext.getFunctionManager())); // HACK! see comments in BeginTableWrite

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    @VisibleForTesting
    public static Set<Rule<?>> columnPruningRules(Metadata metadata)
    {
        return ImmutableSet.of(
                new PruneAggregationColumns(),
                new RemoveEmptyGlobalAggregation(), // aggregation can become empty after pruning its output columns
                new PruneAggregationSourceColumns(),
                new PruneApplyColumns(),
                new PruneApplyCorrelation(),
                new PruneApplySourceColumns(),
                new PruneAssignUniqueIdColumns(),
                new PruneCorrelatedJoinColumns(),
                new PruneCorrelatedJoinCorrelation(),
                new PruneDistinctLimitSourceColumns(),
                new PruneEnforceSingleRowColumns(),
                new PruneExceptSourceColumns(),
                new PruneExchangeColumns(),
                new PruneExchangeSourceColumns(),
                new PruneExplainAnalyzeSourceColumns(),
                new PruneFilterColumns(),
                new PruneGroupIdColumns(),
                new PruneGroupIdSourceColumns(),
                new PruneIndexJoinColumns(),
                new PruneIndexSourceColumns(),
                new PruneIntersectSourceColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                new PruneLimitColumns(),
                new PruneMarkDistinctColumns(),
                new PruneMergeSourceColumns(),
                new PruneOffsetColumns(),
                new PruneOutputSourceColumns(),
                new PrunePattenRecognitionColumns(),
                new PrunePatternRecognitionSourceColumns(),
                new PruneProjectColumns(),
                new PruneRowNumberColumns(),
                new PruneSampleColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneSortColumns(),
                new PruneSpatialJoinChildrenColumns(),
                new PruneSpatialJoinColumns(),
                new PruneTableExecuteSourceColumns(),
                new PruneTableFunctionProcessorColumns(),
                new PruneTableFunctionProcessorSourceColumns(),
                new PruneTableScanColumns(metadata),
                new PruneTableWriterSourceColumns(),
                new PruneTopNColumns(),
                new PruneTopNRankingColumns(),
                new PruneUnionColumns(),
                new PruneUnionSourceColumns(),
                new PruneUnnestColumns(),
                new PruneUnnestSourceColumns(),
                new PruneValuesColumns(),
                new PruneWindowColumns());
    }

    @Override
    public List<PlanOptimizer> get()
    {
        return optimizers;
    }

    @Override
    public Map<Class<?>, OptimizerStats> getOptimizerStats()
    {
        return optimizerStats.getStats();
    }

    @Override
    public Map<Class<?>, RuleStats> getRuleStats()
    {
        return ruleStats.getStats();
    }
}
