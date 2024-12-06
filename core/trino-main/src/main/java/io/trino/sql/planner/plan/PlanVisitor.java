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
package io.trino.sql.planner.plan;

import io.trino.sql.planner.iterative.GroupReference;

public abstract class PlanVisitor<R, C>
{
    protected abstract R visitPlan(PlanNode node, C context);

    public R visitRemoteSource(RemoteSourceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitAggregation(AggregationNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitFilter(FilterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitProject(ProjectNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTopN(TopNNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOutput(OutputNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOffset(OffsetNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitLimit(LimitNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitDistinctLimit(DistinctLimitNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSample(SampleNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableScan(TableScanNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExplainAnalyze(ExplainAnalyzeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitValues(ValuesNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitIndexSource(IndexSourceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitDynamicFilterSource(DynamicFilterSourceNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitJoin(JoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSemiJoin(SemiJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSpatialJoin(SpatialJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitIndexJoin(IndexJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSort(SortNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitWindow(WindowNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitRefreshMaterializedView(RefreshMaterializedViewNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableWriter(TableWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableExecute(TableExecuteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSimpleTableExecuteNode(SimpleTableExecuteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMergeWriter(MergeWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMergeProcessor(MergeProcessorNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableDelete(TableDeleteNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableUpdate(TableUpdateNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableFinish(TableFinishNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitStatisticsWriterNode(StatisticsWriterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitUnion(UnionNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitIntersect(IntersectNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExcept(ExceptNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitUnnest(UnnestNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitMarkDistinct(MarkDistinctNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitGroupId(GroupIdNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitRowNumber(RowNumberNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTopNRanking(TopNRankingNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExchange(ExchangeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitEnforceSingleRow(EnforceSingleRowNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitApply(ApplyNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitAssignUniqueId(AssignUniqueId node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitGroupReference(GroupReference node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitCorrelatedJoin(CorrelatedJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitPatternRecognition(PatternRecognitionNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitChooseAlternativeNode(ChooseAlternativeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitCacheDataPlanNode(CacheDataPlanNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitLoadCachedDataPlanNode(LoadCachedDataPlanNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableFunction(TableFunctionNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableFunctionProcessor(TableFunctionProcessorNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitAdaptivePlanNode(AdaptivePlanNode node, C context)
    {
        return visitPlan(node, context);
    }
}
