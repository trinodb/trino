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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.SystemSessionProperties;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculator.EstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.TaskManagerConfig;
import io.trino.metadata.Metadata;
import io.trino.spi.type.TypeOperators;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.RuleStats;
import io.trino.sql.planner.iterative.rule.AddExchangesBelowPartialAggregationOverGroupIdRuleSet;
import io.trino.sql.planner.iterative.rule.AddIntermediateAggregations;
import io.trino.sql.planner.iterative.rule.ApplyTableScanRedirection;
import io.trino.sql.planner.iterative.rule.CanonicalizeExpressions;
import io.trino.sql.planner.iterative.rule.CreatePartialTopN;
import io.trino.sql.planner.iterative.rule.DecorrelateInnerUnnestWithGlobalAggregation;
import io.trino.sql.planner.iterative.rule.DecorrelateLeftUnnestWithGlobalAggregation;
import io.trino.sql.planner.iterative.rule.DecorrelateUnnest;
import io.trino.sql.planner.iterative.rule.DesugarArrayConstructor;
import io.trino.sql.planner.iterative.rule.DesugarAtTimeZone;
import io.trino.sql.planner.iterative.rule.DesugarCurrentCatalog;
import io.trino.sql.planner.iterative.rule.DesugarCurrentPath;
import io.trino.sql.planner.iterative.rule.DesugarCurrentSchema;
import io.trino.sql.planner.iterative.rule.DesugarCurrentUser;
import io.trino.sql.planner.iterative.rule.DesugarLambdaExpression;
import io.trino.sql.planner.iterative.rule.DesugarLike;
import io.trino.sql.planner.iterative.rule.DesugarTryExpression;
import io.trino.sql.planner.iterative.rule.DetermineJoinDistributionType;
import io.trino.sql.planner.iterative.rule.DeterminePreferredWritePartitioning;
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
import io.trino.sql.planner.iterative.rule.PruneAggregationColumns;
import io.trino.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneApplyColumns;
import io.trino.sql.planner.iterative.rule.PruneApplyCorrelation;
import io.trino.sql.planner.iterative.rule.PruneApplySourceColumns;
import io.trino.sql.planner.iterative.rule.PruneAssignUniqueIdColumns;
import io.trino.sql.planner.iterative.rule.PruneCorrelatedJoinColumns;
import io.trino.sql.planner.iterative.rule.PruneCorrelatedJoinCorrelation;
import io.trino.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import io.trino.sql.planner.iterative.rule.PruneDeleteSourceColumns;
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
import io.trino.sql.planner.iterative.rule.PruneTableScanColumns;
import io.trino.sql.planner.iterative.rule.PruneTableWriterSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneTopNColumns;
import io.trino.sql.planner.iterative.rule.PruneTopNRankingColumns;
import io.trino.sql.planner.iterative.rule.PruneUnionColumns;
import io.trino.sql.planner.iterative.rule.PruneUnionSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneUnnestColumns;
import io.trino.sql.planner.iterative.rule.PruneUnnestSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneUpdateSourceColumns;
import io.trino.sql.planner.iterative.rule.PruneValuesColumns;
import io.trino.sql.planner.iterative.rule.PruneWindowColumns;
import io.trino.sql.planner.iterative.rule.PushAggregationIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushCastIntoRow;
import io.trino.sql.planner.iterative.rule.PushDeleteIntoConnector;
import io.trino.sql.planner.iterative.rule.PushDistinctLimitIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughFilter;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughJoin;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughProject;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughSemiJoin;
import io.trino.sql.planner.iterative.rule.PushDownDereferenceThroughUnnest;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughAssignUniqueId;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughLimit;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughRowNumber;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughSort;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughTopN;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughTopNRanking;
import io.trino.sql.planner.iterative.rule.PushDownDereferencesThroughWindow;
import io.trino.sql.planner.iterative.rule.PushJoinIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushLimitIntoTableScan;
import io.trino.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import io.trino.sql.planner.iterative.rule.PushLimitThroughOffset;
import io.trino.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.trino.sql.planner.iterative.rule.PushLimitThroughProject;
import io.trino.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import io.trino.sql.planner.iterative.rule.PushLimitThroughUnion;
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
import io.trino.sql.planner.iterative.rule.RemoveEmptyDelete;
import io.trino.sql.planner.iterative.rule.RemoveEmptyExceptBranches;
import io.trino.sql.planner.iterative.rule.RemoveEmptyUnionBranches;
import io.trino.sql.planner.iterative.rule.RemoveFullSample;
import io.trino.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import io.trino.sql.planner.iterative.rule.RemoveRedundantEnforceSingleRowNode;
import io.trino.sql.planner.iterative.rule.RemoveRedundantExists;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.iterative.rule.RemoveRedundantJoin;
import io.trino.sql.planner.iterative.rule.RemoveRedundantLimit;
import io.trino.sql.planner.iterative.rule.RemoveRedundantOffset;
import io.trino.sql.planner.iterative.rule.RemoveRedundantSort;
import io.trino.sql.planner.iterative.rule.RemoveRedundantSortBelowLimitWithTies;
import io.trino.sql.planner.iterative.rule.RemoveRedundantTableScanPredicate;
import io.trino.sql.planner.iterative.rule.RemoveRedundantTopN;
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
import io.trino.sql.planner.iterative.rule.UnwrapRowSubscript;
import io.trino.sql.planner.iterative.rule.UnwrapSingleColumnRowInApply;
import io.trino.sql.planner.optimizations.AddExchanges;
import io.trino.sql.planner.optimizations.AddLocalExchanges;
import io.trino.sql.planner.optimizations.BeginTableWrite;
import io.trino.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.trino.sql.planner.optimizations.HashGenerationOptimizer;
import io.trino.sql.planner.optimizations.IndexJoinOptimizer;
import io.trino.sql.planner.optimizations.LimitPushDown;
import io.trino.sql.planner.optimizations.MetadataQueryOptimizer;
import io.trino.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import io.trino.sql.planner.optimizations.OptimizerStats;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.PredicatePushDown;
import io.trino.sql.planner.optimizations.PruneUnreferencedOutputs;
import io.trino.sql.planner.optimizations.ReplicateSemiJoinInDelete;
import io.trino.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import io.trino.sql.planner.optimizations.TableDeleteOptimizer;
import io.trino.sql.planner.optimizations.TransformQuantifiedComparisonApplyToCorrelatedJoin;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.optimizations.WindowFilterPushDown;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.SystemSessionProperties.isIterativeRuleBasedColumnPruning;

public class PlanOptimizers
        implements PlanOptimizersFactory
{
    private final List<PlanOptimizer> optimizers;
    private final RuleStatsRecorder ruleStats = new RuleStatsRecorder();
    private final OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();

    @Inject
    public PlanOptimizers(
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            ScalarStatsCalculator scalarStatsCalculator,
            CostCalculator costCalculator,
            @EstimatedExchanges CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            NodePartitioningManager nodePartitioningManager)
    {
        this(metadata,
                typeOperators,
                typeAnalyzer,
                taskManagerConfig,
                false,
                splitManager,
                pageSourceManager,
                statsCalculator,
                scalarStatsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                costComparator,
                taskCountEstimator,
                nodePartitioningManager);
    }

    public PlanOptimizers(
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            boolean forceSingleNode,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            ScalarStatsCalculator scalarStatsCalculator,
            CostCalculator costCalculator,
            CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            NodePartitioningManager nodePartitioningManager)
    {
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneApplyColumns(),
                new PruneApplyCorrelation(),
                new PruneApplySourceColumns(),
                new PruneAssignUniqueIdColumns(),
                new PruneCorrelatedJoinColumns(),
                new PruneCorrelatedJoinCorrelation(),
                new PruneDeleteSourceColumns(),
                new PruneUpdateSourceColumns(),
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

        Set<Rule<?>> projectionPushdownRules = ImmutableSet.of(
                new PushProjectionThroughUnion(),
                new PushProjectionThroughExchange(),
                // Dereference pushdown rules
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

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(typeAnalyzer),
                        new RemoveRedundantIdentityProjections()));

        Set<Rule<?>> simplifyOptimizerRules = ImmutableSet.<Rule<?>>builder()
                .addAll(new SimplifyExpressions(metadata, typeAnalyzer).rules())
                .addAll(new UnwrapRowSubscript().rules())
                .addAll(new PushCastIntoRow().rules())
                .addAll(new UnwrapCastInComparison(metadata, typeOperators, typeAnalyzer).rules())
                .addAll(new RemoveDuplicateConditions(metadata).rules())
                .addAll(new CanonicalizeExpressions(metadata, typeAnalyzer).rules())
                .add(new RemoveTrivialFilters())
                .build();
        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                simplifyOptimizerRules);

        IterativeOptimizer columnPruningOptimizer = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                session -> !isIterativeRuleBasedColumnPruning(session),
                ImmutableList.of(new PruneUnreferencedOutputs(metadata)),
                columnPruningRules);

        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new DesugarAtTimeZone(metadata, typeAnalyzer).rules())
                                .addAll(new DesugarCurrentCatalog(metadata).rules())
                                .addAll(new DesugarCurrentSchema(metadata).rules())
                                .addAll(new DesugarCurrentUser(metadata).rules())
                                .addAll(new DesugarCurrentPath(metadata).rules())
                                .addAll(new DesugarTryExpression(metadata, typeAnalyzer).rules())
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new CanonicalizeExpressions(metadata, typeAnalyzer).rules())
                                .add(new OptimizeRowPattern())
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(columnPruningRules)
                                .addAll(projectionPushdownRules)
                                .addAll(new UnwrapRowSubscript().rules())
                                .addAll(new PushCastIntoRow().rules())
                                .addAll(ImmutableSet.of(
                                        new UnwrapSingleColumnRowInApply(typeAnalyzer),
                                        new RemoveEmptyUnionBranches(),
                                        new EvaluateEmptyIntersect(),
                                        new RemoveEmptyExceptBranches(),
                                        new MergeFilters(metadata),
                                        new InlineProjections(typeAnalyzer),
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroSample(),
                                        new PushOffsetThroughProject(),
                                        new PushLimitThroughOffset(),
                                        new PushLimitThroughProject(typeAnalyzer),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitOverProjectWithSort(),
                                        new MergeLimitWithTopN(),
                                        new PushLimitThroughMarkDistinct(),
                                        new PushLimitThroughOuterJoin(),
                                        new PushLimitThroughSemiJoin(),
                                        new PushLimitThroughUnion(),
                                        new RemoveTrivialFilters(),
                                        new RemoveRedundantLimit(),
                                        new RemoveRedundantOffset(),
                                        new RemoveRedundantSort(),
                                        new RemoveRedundantSortBelowLimitWithTies(),
                                        new RemoveRedundantTopN(),
                                        new RemoveRedundantDistinctLimit(),
                                        new ReplaceRedundantJoinWithSource(),
                                        new RemoveRedundantJoin(),
                                        new ReplaceRedundantJoinWithProject(),
                                        new RemoveRedundantEnforceSingleRowNode(),
                                        new RemoveRedundantExists(),
                                        new ImplementFilteredAggregations(metadata),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MultipleDistinctAggregationToMarkDistinct(),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata),
                                        new PruneOrderByInAggregation(metadata),
                                        new RewriteSpatialPartitioningAggregation(metadata),
                                        new SimplifyCountOverConstant(metadata)))
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new ImplementOffset())),
                simplifyOptimizer,
                new UnaliasSymbolReferences(metadata),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new MergeUnion(),
                                new MergeIntersect(),
                                new MergeExcept(),
                                new PruneDistinctAggregation())),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementIntersectDistinctAsUnion(metadata),
                                new ImplementExceptDistinctAsUnion(metadata),
                                new ImplementIntersectAll(metadata),
                                new ImplementExceptAll(metadata))),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                columnPruningOptimizer,
                inlineProjections,
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        columnPruningRules),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformExistsApplyToCorrelatedJoin(metadata))),
                new TransformQuantifiedComparisonApplyToCorrelatedJoin(metadata),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantEnforceSingleRowNode(),
                                new RemoveUnreferencedScalarSubqueries(),
                                new TransformUncorrelatedSubqueryToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedJoinToJoin(metadata),
                                new DecorrelateInnerUnnestWithGlobalAggregation(),
                                new DecorrelateLeftUnnestWithGlobalAggregation(),
                                new DecorrelateUnnest(metadata),
                                new TransformCorrelatedGlobalAggregationWithProjection(metadata),
                                new TransformCorrelatedGlobalAggregationWithoutProjection(metadata),
                                new TransformCorrelatedDistinctAggregationWithProjection(metadata),
                                new TransformCorrelatedDistinctAggregationWithoutProjection(metadata),
                                new TransformCorrelatedGroupedAggregationWithProjection(metadata),
                                new TransformCorrelatedGroupedAggregationWithoutProjection(metadata))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementLimitWithTies(metadata), // must be run after DecorrelateUnnest
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(metadata), // must be run after columnPruningOptimizer
                                new TransformCorrelatedScalarSubquery(metadata), // must be run after TransformCorrelatedAggregation rules
                                new TransformCorrelatedJoinToJoin(metadata),
                                new ImplementFilteredAggregations(metadata))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new InlineProjections(typeAnalyzer),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject(),
                                new RemoveAggregationInSemiJoin(),
                                new MergeProjectWithValues(metadata),
                                new ReplaceJoinOverConstantWithProject())),
                new CheckSubqueryNodesAreRewritten(),
                simplifyOptimizer, // Should run after MergeProjectWithValues
                new StatsRecordingPlanOptimizer(
                        optimizerStats,
                        new PredicatePushDown(metadata, typeOperators, typeAnalyzer, false, false)),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveEmptyUnionBranches(),
                                new EvaluateEmptyIntersect(),
                                new RemoveEmptyExceptBranches(),
                                new TransformFilteringSemiJoinToInnerJoin(),
                                new InlineProjectIntoFilter(metadata),
                                new SimplifyFilterPredicate(metadata)))); // must run after PredicatePushDown

        // Perform redirection before CBO rules to ensure stats from destination connector are used
        // Perform redirection before agg, topN, limit, sample etc. push down into table scan as the destination connector may support a different set of push downs
        // Perform redirection before push down of dereferences into table scan via PushProjectionIntoTableScan
        // Perform redirection after at least one PredicatePushDown and PushPredicateIntoTableScan to allow connector to use pushed down predicates in redirection decision
        // Perform redirection after at least table scan pruning rules because redirected table might have fewer columns
        // PushPredicateIntoTableScan needs to be run again after redirection to ensure predicate push down into destination table scan
        // Column pruning rules need to be run after redirection
        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ApplyTableScanRedirection(metadata),
                                new PruneTableScanColumns(metadata),
                                new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer))));

        Set<Rule<?>> pushIntoTableScanRulesExceptJoins = ImmutableSet.<Rule<?>>builder()
                .addAll(columnPruningRules)
                .addAll(projectionPushdownRules)
                .add(new PushProjectionIntoTableScan(metadata, typeAnalyzer, scalarStatsCalculator))
                .add(new RemoveRedundantIdentityProjections())
                .add(new PushLimitIntoTableScan(metadata))
                .add(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer))
                .add(new PushSampleIntoTableScan(metadata))
                .add(new PushAggregationIntoTableScan(metadata))
                .add(new PushDistinctLimitIntoTableScan(metadata))
                .add(new PushTopNIntoTableScan(metadata))
                .build();
        IterativeOptimizer pushIntoTableScanOptimizer = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                pushIntoTableScanRulesExceptJoins);
        builder.add(pushIntoTableScanOptimizer);
        builder.add(new UnaliasSymbolReferences(metadata));
        builder.add(pushIntoTableScanOptimizer); // TODO (https://github.com/trinodb/trino/issues/811) merge with the above after migrating UnaliasSymbolReferences to rules

        IterativeOptimizer pushProjectionIntoTableScanOptimizer = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(projectionPushdownRules)
                        .add(new PushProjectionIntoTableScan(metadata, typeAnalyzer, scalarStatsCalculator))
                        .build());

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        // Temporary hack: separate optimizer step to avoid the sample node being replaced by filter before pushing
                        // it to table scan node
                        ImmutableSet.of(new ImplementBernoulliSampleAsFilter(metadata))),
                columnPruningOptimizer,
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveEmptyUnionBranches(),
                                new EvaluateEmptyIntersect(),
                                new RemoveEmptyExceptBranches(),
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin(),
                                new ReplaceRedundantJoinWithSource())), // Run this after PredicatePushDown optimizer as it inlines filter constants
                inlineProjections,
                simplifyOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                pushProjectionIntoTableScanOptimizer,
                // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
                // pushdown into the connectors. We invoke PredicatePushdown and PushPredicateIntoTableScan after this
                // to leverage predicate pushdown on projected columns.
                new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false)),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                                .add(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer))
                                .build()),
                new UnaliasSymbolReferences(metadata), // Run again because predicate pushdown and projection pushdown might add more projections
                columnPruningOptimizer, // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata, typeOperators), // Run this after projections and filters have been fully simplified and pushed down
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        SystemSessionProperties::useLegacyWindowFilterPushdown,
                        ImmutableList.of(new WindowFilterPushDown(metadata, typeOperators)),
                        ImmutableSet.of(// should run after DecorrelateUnnest and ImplementLimitWithTies
                                new RemoveEmptyUnionBranches(),
                                new EvaluateEmptyIntersect(),
                                new RemoveEmptyExceptBranches(),
                                new PushdownLimitIntoRowNumber(),
                                new PushdownLimitIntoWindow(metadata),
                                new PushdownFilterIntoRowNumber(metadata, typeOperators),
                                new PushdownFilterIntoWindow(metadata, typeOperators),
                                new ReplaceWindowWithRowNumber(metadata))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .addAll(MergePatternRecognitionNodes.rules())
                                .add(new PushPredicateThroughProjectIntoRowNumber(metadata, typeOperators))
                                .add(new PushPredicateThroughProjectIntoWindow(metadata, typeOperators))
                                .build()),
                inlineProjections,
                columnPruningOptimizer, // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new MetadataQueryOptimizer(metadata),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins(metadata, typeAnalyzer))), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                new StatsRecordingPlanOptimizer(
                        optimizerStats,
                        new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false)),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                                .add(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer))
                                .build()),
                pushProjectionIntoTableScanOptimizer,
                // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
                // pushdown into the connectors. Invoke PredicatePushdown and PushPredicateIntoTableScan after this
                // to leverage predicate pushdown on projected columns.
                new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false)),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                                .add(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer))
                                .build()),
                columnPruningOptimizer,
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                // Prefer write partitioning rule requires accurate stats.
                // Run it before reorder joins which also depends on accurate stats.
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new DeterminePreferredWritePartitioning())),
                // Because ReorderJoins runs only once,
                // PredicatePushDown, columnPruningOptimizer and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new ReorderJoins(metadata, costComparator, typeAnalyzer))));

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughProject(typeAnalyzer),
                        new PushTopNThroughOuterJoin(),
                        new PushTopNThroughUnion(),
                        new PushTopNIntoTableScan(metadata))));
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .addAll(new ExtractSpatialJoins(metadata, splitManager, pageSourceManager, typeAnalyzer).rules())
                        .add(new InlineProjections(typeAnalyzer))
                        .build()));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        // Must run before AddExchanges
                        new PushDeleteIntoConnector(metadata),
                        new DetermineTableScanNodePartitioning(metadata, nodePartitioningManager, taskCountEstimator),
                        // Must run after join reordering because join reordering creates
                        // new join nodes without JoinNode.maySkipOutputDuplicates flag set
                        new OptimizeDuplicateInsensitiveJoins(metadata))));

        if (!forceSingleNode) {
            builder.add(new ReplicateSemiJoinInDelete()); // Must run before AddExchanges
            builder.add(new IterativeOptimizer(
                    metadata,
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    ImmutableSet.of(
                            new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                            // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                            // to avoid temporarily having an invalid plan
                            new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator))));

            builder.add(new IterativeOptimizer(
                    metadata,
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    ImmutableSet.<Rule<?>>builder()
                            .addAll(pushIntoTableScanRulesExceptJoins)
                            // PushJoinIntoTableScan must run after ReorderJoins (and DetermineJoinDistributionType)
                            // otherwise too early pushdown could prevent optimal plan from being selected.
                            .add(new PushJoinIntoTableScan(metadata))
                            // DetermineTableScanNodePartitioning is needed to needs to ensure all table handles have proper partitioning determined
                            // Must run before AddExchanges
                            .add(new DetermineTableScanNodePartitioning(metadata, nodePartitioningManager, taskCountEstimator))
                            .build()));

            builder.add(
                    new IterativeOptimizer(
                            metadata,
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(new PushTableWriteThroughUnion()))); // Must run before AddExchanges
            // unalias symbols before adding exchanges to use same partitioning symbols in joins, aggregations and other
            // operators that require node partitioning
            builder.add(new UnaliasSymbolReferences(metadata));
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new AddExchanges(metadata, typeOperators, typeAnalyzer, statsCalculator)));
        }
        //noinspection UnusedAssignment
        estimatedExchangesCostCalculator = null; // Prevent accidental use after AddExchanges

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveEmptyDelete()))); // Run RemoveEmptyDelete after table scan is removed by PickTableLayout/AddExchanges

        // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        // and to pushdown dynamic filters
        builder.add(new StatsRecordingPlanOptimizer(
                optimizerStats,
                new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, false)));
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                        .add(new RemoveRedundantTableScanPredicate(metadata, typeOperators))
                        .build()));
        builder.add(pushProjectionIntoTableScanOptimizer);
        // Projection pushdown rules may push reducing projections (e.g. dereferences) below filters for potential
        // pushdown into the connectors. Invoke PredicatePushdown and PushPredicateIntoTableScan after this
        // to leverage predicate pushdown on projected columns.
        builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, typeOperators, typeAnalyzer, true, true)));
        builder.add(new RemoveUnsupportedDynamicFilters(metadata)); // Remove unsupported dynamic filters introduced by PredicatePushdown
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(simplifyOptimizerRules) // Should be always run after PredicatePushDown
                        .add(new PushPredicateIntoTableScan(metadata, typeOperators, typeAnalyzer))
                        .build()));
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences(metadata)); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(columnPruningOptimizer);

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushRemoteExchangeThroughAssignUniqueId())
                        .add(new InlineProjections(typeAnalyzer))
                        .build()));

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, typeOperators, typeAnalyzer));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new PushPartialAggregationThroughJoin(),
                        new PushPartialAggregationThroughExchange(metadata),
                        new PruneJoinColumns(),
                        new PruneJoinChildrenColumns())));
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(metadata, typeOperators, typeAnalyzer, taskCountEstimator, taskManagerConfig).rules()));
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections())));
        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Remove any remaining sugar
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(new DesugarLike(metadata, typeAnalyzer).rules())
                        .addAll(new DesugarArrayConstructor(metadata, typeAnalyzer).rules())
                        .build()));

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer(metadata));

        builder.add(new TableDeleteOptimizer(metadata));
        builder.add(new BeginTableWrite(metadata)); // HACK! see comments in BeginTableWrite

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
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
