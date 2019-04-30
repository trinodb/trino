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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostCalculator.EstimatedExchanges;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.TaskCountEstimator;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.metadata.Metadata;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.rule.AddExchangesBelowPartialAggregationOverGroupIdRuleSet;
import io.prestosql.sql.planner.iterative.rule.AddIntermediateAggregations;
import io.prestosql.sql.planner.iterative.rule.CanonicalizeExpressions;
import io.prestosql.sql.planner.iterative.rule.CreatePartialTopN;
import io.prestosql.sql.planner.iterative.rule.DesugarAtTimeZone;
import io.prestosql.sql.planner.iterative.rule.DesugarCurrentPath;
import io.prestosql.sql.planner.iterative.rule.DesugarCurrentUser;
import io.prestosql.sql.planner.iterative.rule.DesugarLambdaExpression;
import io.prestosql.sql.planner.iterative.rule.DesugarRowSubscript;
import io.prestosql.sql.planner.iterative.rule.DesugarTryExpression;
import io.prestosql.sql.planner.iterative.rule.DetermineJoinDistributionType;
import io.prestosql.sql.planner.iterative.rule.DetermineSemiJoinDistributionType;
import io.prestosql.sql.planner.iterative.rule.EliminateCrossJoins;
import io.prestosql.sql.planner.iterative.rule.EvaluateZeroSample;
import io.prestosql.sql.planner.iterative.rule.ExtractSpatialJoins;
import io.prestosql.sql.planner.iterative.rule.GatherAndMergeWindows;
import io.prestosql.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
import io.prestosql.sql.planner.iterative.rule.ImplementExceptAsUnion;
import io.prestosql.sql.planner.iterative.rule.ImplementFilteredAggregations;
import io.prestosql.sql.planner.iterative.rule.ImplementIntersectAsUnion;
import io.prestosql.sql.planner.iterative.rule.ImplementLimitWithTies;
import io.prestosql.sql.planner.iterative.rule.ImplementOffset;
import io.prestosql.sql.planner.iterative.rule.InlineProjections;
import io.prestosql.sql.planner.iterative.rule.MergeFilters;
import io.prestosql.sql.planner.iterative.rule.MergeLimitOverProjectWithSort;
import io.prestosql.sql.planner.iterative.rule.MergeLimitWithDistinct;
import io.prestosql.sql.planner.iterative.rule.MergeLimitWithSort;
import io.prestosql.sql.planner.iterative.rule.MergeLimitWithTopN;
import io.prestosql.sql.planner.iterative.rule.MergeLimits;
import io.prestosql.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import io.prestosql.sql.planner.iterative.rule.PruneAggregationColumns;
import io.prestosql.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import io.prestosql.sql.planner.iterative.rule.PruneCrossJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneFilterColumns;
import io.prestosql.sql.planner.iterative.rule.PruneIndexSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import io.prestosql.sql.planner.iterative.rule.PruneJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneLimitColumns;
import io.prestosql.sql.planner.iterative.rule.PruneMarkDistinctColumns;
import io.prestosql.sql.planner.iterative.rule.PruneOffsetColumns;
import io.prestosql.sql.planner.iterative.rule.PruneOrderByInAggregation;
import io.prestosql.sql.planner.iterative.rule.PruneOutputColumns;
import io.prestosql.sql.planner.iterative.rule.PruneProjectColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSemiJoinColumns;
import io.prestosql.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTableScanColumns;
import io.prestosql.sql.planner.iterative.rule.PruneTopNColumns;
import io.prestosql.sql.planner.iterative.rule.PruneValuesColumns;
import io.prestosql.sql.planner.iterative.rule.PruneWindowColumns;
import io.prestosql.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushDeleteIntoConnector;
import io.prestosql.sql.planner.iterative.rule.PushLimitIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOffset;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughProject;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import io.prestosql.sql.planner.iterative.rule.PushLimitThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushOffsetThroughProject;
import io.prestosql.sql.planner.iterative.rule.PushPartialAggregationThroughExchange;
import io.prestosql.sql.planner.iterative.rule.PushPartialAggregationThroughJoin;
import io.prestosql.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushProjectionIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushProjectionThroughExchange;
import io.prestosql.sql.planner.iterative.rule.PushProjectionThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushRemoteExchangeThroughAssignUniqueId;
import io.prestosql.sql.planner.iterative.rule.PushSampleIntoTableScan;
import io.prestosql.sql.planner.iterative.rule.PushTableWriteThroughUnion;
import io.prestosql.sql.planner.iterative.rule.PushTopNThroughOuterJoin;
import io.prestosql.sql.planner.iterative.rule.PushTopNThroughProject;
import io.prestosql.sql.planner.iterative.rule.PushTopNThroughUnion;
import io.prestosql.sql.planner.iterative.rule.RemoveAggregationInSemiJoin;
import io.prestosql.sql.planner.iterative.rule.RemoveDuplicateConditions;
import io.prestosql.sql.planner.iterative.rule.RemoveEmptyDelete;
import io.prestosql.sql.planner.iterative.rule.RemoveFullSample;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantLimit;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantSort;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantTableScanPredicate;
import io.prestosql.sql.planner.iterative.rule.RemoveRedundantTopN;
import io.prestosql.sql.planner.iterative.rule.RemoveTrivialFilters;
import io.prestosql.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import io.prestosql.sql.planner.iterative.rule.RemoveUnreferencedScalarSubqueries;
import io.prestosql.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import io.prestosql.sql.planner.iterative.rule.ReorderJoins;
import io.prestosql.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import io.prestosql.sql.planner.iterative.rule.SimplifyCountOverConstant;
import io.prestosql.sql.planner.iterative.rule.SimplifyExpressions;
import io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedJoinToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedScalarAggregationToJoin;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import io.prestosql.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import io.prestosql.sql.planner.iterative.rule.TransformExistsApplyToCorrelatedJoin;
import io.prestosql.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import io.prestosql.sql.planner.iterative.rule.TransformUncorrelatedSubqueryToJoin;
import io.prestosql.sql.planner.iterative.rule.UnwrapCastInComparison;
import io.prestosql.sql.planner.optimizations.AddExchanges;
import io.prestosql.sql.planner.optimizations.AddLocalExchanges;
import io.prestosql.sql.planner.optimizations.BeginTableWrite;
import io.prestosql.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.prestosql.sql.planner.optimizations.HashGenerationOptimizer;
import io.prestosql.sql.planner.optimizations.ImplementIntersectAndExceptAsUnion;
import io.prestosql.sql.planner.optimizations.IndexJoinOptimizer;
import io.prestosql.sql.planner.optimizations.LimitPushDown;
import io.prestosql.sql.planner.optimizations.MetadataQueryOptimizer;
import io.prestosql.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.optimizations.PredicatePushDown;
import io.prestosql.sql.planner.optimizations.PruneUnreferencedOutputs;
import io.prestosql.sql.planner.optimizations.ReplicateSemiJoinInDelete;
import io.prestosql.sql.planner.optimizations.SetFlatteningOptimizer;
import io.prestosql.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import io.prestosql.sql.planner.optimizations.TableDeleteOptimizer;
import io.prestosql.sql.planner.optimizations.TransformQuantifiedComparisonApplyToCorrelatedJoin;
import io.prestosql.sql.planner.optimizations.UnaliasSymbolReferences;
import io.prestosql.sql.planner.optimizations.WindowFilterPushDown;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;

public class PlanOptimizers
{
    private final List<PlanOptimizer> optimizers;
    private final RuleStatsRecorder ruleStats = new RuleStatsRecorder();
    private final OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();
    private final MBeanExporter exporter;

    @Inject
    public PlanOptimizers(
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            MBeanExporter exporter,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            @EstimatedExchanges CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator)
    {
        this(metadata,
                typeAnalyzer,
                taskManagerConfig,
                false,
                exporter,
                splitManager,
                pageSourceManager,
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                costComparator,
                taskCountEstimator);
    }

    @PostConstruct
    public void initialize()
    {
        ruleStats.export(exporter);
        optimizerStats.export(exporter);
    }

    @PreDestroy
    public void destroy()
    {
        ruleStats.unexport(exporter);
        optimizerStats.unexport(exporter);
    }

    public PlanOptimizers(
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            boolean forceSingleNode,
            MBeanExporter exporter,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator)
    {
        this.exporter = exporter;
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        Set<Rule<?>> predicatePushDownRules = ImmutableSet.of(
                new MergeFilters(metadata));

        // TODO: Once we've migrated handling all the plan node types, replace uses of PruneUnreferencedOutputs with an IterativeOptimizer containing these rules.
        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneCrossJoinColumns(),
                new PruneFilterColumns(),
                new PruneIndexSourceColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                new PruneMarkDistinctColumns(),
                new PruneOutputColumns(),
                new PruneProjectColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneTopNColumns(),
                new PruneValuesColumns(),
                new PruneWindowColumns(),
                new PruneOffsetColumns(),
                new PruneLimitColumns(),
                new PruneTableScanColumns());

        Set<Rule<?>> projectionPushdownRules = ImmutableSet.of(
                new PushProjectionIntoTableScan(metadata, typeAnalyzer),
                new PushProjectionThroughUnion(),
                new PushProjectionThroughExchange());

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(),
                        new RemoveRedundantIdentityProjections()));

        IterativeOptimizer projectionPushDown = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                projectionPushdownRules);

        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(new SimplifyExpressions(metadata, typeAnalyzer).rules())
                        .addAll(new UnwrapCastInComparison(metadata, typeAnalyzer).rules())
                        .addAll(new RemoveDuplicateConditions(metadata).rules())
                        .addAll(new CanonicalizeExpressions(metadata, typeAnalyzer).rules())
                        .build());

        PlanOptimizer predicatePushDown = new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, typeAnalyzer, true));

        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new DesugarAtTimeZone(metadata, typeAnalyzer).rules())
                                .addAll(new DesugarCurrentUser(metadata).rules())
                                .addAll(new DesugarCurrentPath(metadata).rules())
                                .addAll(new DesugarTryExpression(metadata, typeAnalyzer).rules())
                                .addAll(new DesugarRowSubscript(typeAnalyzer).rules())
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new CanonicalizeExpressions(metadata, typeAnalyzer).rules()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(predicatePushDownRules)
                                .addAll(columnPruningRules)
                                .addAll(projectionPushdownRules)
                                .addAll(ImmutableSet.of(
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroSample(),
                                        new PushOffsetThroughProject(),
                                        new PushLimitThroughOffset(),
                                        new PushLimitThroughProject(),
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
                                        new RemoveRedundantSort(),
                                        new RemoveRedundantTopN(),
                                        new RemoveRedundantDistinctLimit(),
                                        new ImplementFilteredAggregations(metadata),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MultipleDistinctAggregationToMarkDistinct(),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata),
                                        new PruneOrderByInAggregation(metadata),
                                        new RewriteSpatialPartitioningAggregation(metadata)))
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementOffset(),
                                new ImplementLimitWithTies(metadata))),
                simplifyOptimizer,
                new UnaliasSymbolReferences(metadata),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new SetFlatteningOptimizer(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableList.of(new ImplementIntersectAndExceptAsUnion(metadata)),
                        ImmutableSet.of(
                                new ImplementIntersectAsUnion(metadata),
                                new ImplementExceptAsUnion(metadata))),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PruneUnreferencedOutputs(),
                inlineProjections,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        columnPruningRules),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformExistsApplyToCorrelatedJoin(metadata))),
                new TransformQuantifiedComparisonApplyToCorrelatedJoin(metadata),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarSubqueries(),
                                new TransformUncorrelatedSubqueryToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedScalarAggregationToJoin(metadata),
                                new TransformCorrelatedJoinToJoin(metadata))),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(metadata), // must be run after PruneUnreferencedOutputs
                                new TransformCorrelatedScalarSubquery(metadata), // must be run after TransformCorrelatedScalarAggregationToJoin
                                new TransformCorrelatedJoinToJoin(metadata),
                                new ImplementFilteredAggregations(metadata))),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new InlineProjections(),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject(),
                                new RemoveAggregationInSemiJoin())),
                new CheckSubqueryNodesAreRewritten(),
                new StatsRecordingPlanOptimizer(
                        optimizerStats,
                        new PredicatePushDown(metadata, typeAnalyzer, false)),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(projectionPushdownRules)
                                .add(new PushLimitIntoTableScan(metadata))
                                .add(new PushPredicateIntoTableScan(metadata, typeAnalyzer))
                                .add(new PushSampleIntoTableScan(metadata))
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        // Temporary hack: separate optimizer step to avoid the sample node being replaced by filter before pushing
                        // it to table scan node
                        ImmutableSet.of(new ImplementBernoulliSampleAsFilter(metadata))),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin())),
                inlineProjections,
                simplifyOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                projectionPushDown,
                new UnaliasSymbolReferences(metadata), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new SimplifyCountOverConstant(metadata))),
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .build()),
                inlineProjections,
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new MetadataQueryOptimizer(metadata),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins(metadata))), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                predicatePushDown,
                simplifyOptimizer, // Should be always run after PredicatePushDown
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new PushPredicateIntoTableScan(metadata, typeAnalyzer))),
                projectionPushDown,
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),

                // Because ReorderJoins runs only once,
                // PredicatePushDown, PruneUnreferenedOutputpus and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new ReorderJoins(metadata, costComparator))));

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughProject(),
                        new PushTopNThroughOuterJoin(),
                        new PushTopNThroughUnion())));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .addAll(new ExtractSpatialJoins(metadata, splitManager, pageSourceManager, typeAnalyzer).rules())
                        .add(new InlineProjections())
                        .build()));

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new PushDeleteIntoConnector(metadata)))); // Must run before AddExchanges

        if (!forceSingleNode) {
            builder.add(new ReplicateSemiJoinInDelete()); // Must run before AddExchanges
            builder.add((new IterativeOptimizer(
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    ImmutableSet.of(
                            new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                            // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                            // to avoid temporarily having an invalid plan
                            new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator)))));
            builder.add(
                    new IterativeOptimizer(
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(new PushTableWriteThroughUnion()))); // Must run before AddExchanges
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new AddExchanges(metadata, typeAnalyzer)));
        }
        //noinspection UnusedAssignment
        estimatedExchangesCostCalculator = null; // Prevent accidental use after AddExchanges

        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveEmptyDelete()))); // Run RemoveEmptyDelete after table scan is removed by PickTableLayout/AddExchanges

        builder.add(predicatePushDown); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(new RemoveUnsupportedDynamicFilters(metadata));
        builder.add(simplifyOptimizer); // Should be always run after PredicatePushDown
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new RemoveRedundantTableScanPredicate(metadata))));
        builder.add(projectionPushDown);
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences(metadata)); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(new PruneUnreferencedOutputs());

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushRemoteExchangeThroughAssignUniqueId())
                        .add(new InlineProjections())
                        .build()));

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, typeAnalyzer));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new PushPartialAggregationThroughJoin(),
                        new PushPartialAggregationThroughExchange(metadata),
                        new PruneJoinColumns())));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(metadata, typeAnalyzer, taskCountEstimator, taskManagerConfig).rules()));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections())));
        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer(metadata));

        builder.add(new TableDeleteOptimizer(metadata));
        builder.add(new BeginTableWrite(metadata)); // HACK! see comments in BeginTableWrite

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
