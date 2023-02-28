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

import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.CorrelatedJoinNode.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Pattern.typeOf;
import static io.trino.matching.Property.optionalProperty;
import static io.trino.matching.Property.property;
import static io.trino.sql.planner.plan.Patterns.Values.rowCount;

public final class Patterns
{
    private Patterns() {}

    public static Pattern<AssignUniqueId> assignUniqueId()
    {
        return typeOf(AssignUniqueId.class);
    }

    public static Pattern<AggregationNode> aggregation()
    {
        return typeOf(AggregationNode.class);
    }

    public static Pattern<GroupIdNode> groupId()
    {
        return typeOf(GroupIdNode.class);
    }

    public static Pattern<ApplyNode> applyNode()
    {
        return typeOf(ApplyNode.class);
    }

    public static Pattern<TableExecuteNode> tableExecute()
    {
        return typeOf(TableExecuteNode.class);
    }

    public static Pattern<MergeWriterNode> mergeWriter()
    {
        return typeOf(MergeWriterNode.class);
    }

    public static Pattern<ExchangeNode> exchange()
    {
        return typeOf(ExchangeNode.class);
    }

    public static Pattern<ExplainAnalyzeNode> explainAnalyze()
    {
        return typeOf(ExplainAnalyzeNode.class);
    }

    public static Pattern<EnforceSingleRowNode> enforceSingleRow()
    {
        return typeOf(EnforceSingleRowNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<IndexJoinNode> indexJoin()
    {
        return typeOf(IndexJoinNode.class);
    }

    public static Pattern<IndexSourceNode> indexSource()
    {
        return typeOf(IndexSourceNode.class);
    }

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Pattern<DynamicFilterSourceNode> dynamicFilterSource()
    {
        return typeOf(DynamicFilterSourceNode.class);
    }

    public static Pattern<SpatialJoinNode> spatialJoin()
    {
        return typeOf(SpatialJoinNode.class);
    }

    public static Pattern<CorrelatedJoinNode> correlatedJoin()
    {
        return typeOf(CorrelatedJoinNode.class);
    }

    public static Pattern<OffsetNode> offset()
    {
        return typeOf(OffsetNode.class);
    }

    public static Pattern<LimitNode> limit()
    {
        return typeOf(LimitNode.class);
    }

    public static Pattern<MarkDistinctNode> markDistinct()
    {
        return typeOf(MarkDistinctNode.class);
    }

    public static Pattern<OutputNode> output()
    {
        return typeOf(OutputNode.class);
    }

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Pattern<SampleNode> sample()
    {
        return typeOf(SampleNode.class);
    }

    public static Pattern<SemiJoinNode> semiJoin()
    {
        return typeOf(SemiJoinNode.class);
    }

    public static Pattern<SortNode> sort()
    {
        return typeOf(SortNode.class);
    }

    public static Pattern<TableFinishNode> tableFinish()
    {
        return typeOf(TableFinishNode.class);
    }

    public static Pattern<TableScanNode> tableScan()
    {
        return typeOf(TableScanNode.class);
    }

    public static Pattern<TableWriterNode> tableWriterNode()
    {
        return typeOf(TableWriterNode.class);
    }

    public static Pattern<TopNNode> topN()
    {
        return typeOf(TopNNode.class);
    }

    public static Pattern<UnionNode> union()
    {
        return typeOf(UnionNode.class);
    }

    public static Pattern<ValuesNode> values()
    {
        return typeOf(ValuesNode.class);
    }

    public static Pattern<ValuesNode> emptyValues()
    {
        return values().with(rowCount().equalTo(0));
    }

    public static Pattern<UnnestNode> unnest()
    {
        return typeOf(UnnestNode.class);
    }

    public static Pattern<WindowNode> window()
    {
        return typeOf(WindowNode.class);
    }

    public static Pattern<PatternRecognitionNode> patternRecognition()
    {
        return typeOf(PatternRecognitionNode.class);
    }

    public static Pattern<TableFunctionNode> tableFunction()
    {
        return typeOf(TableFunctionNode.class);
    }

    public static Pattern<TableFunctionProcessorNode> tableFunctionProcessor()
    {
        return typeOf(TableFunctionProcessorNode.class);
    }

    public static Pattern<RowNumberNode> rowNumber()
    {
        return typeOf(RowNumberNode.class);
    }

    public static Pattern<TopNRankingNode> topNRanking()
    {
        return typeOf(TopNRankingNode.class);
    }

    public static Pattern<DistinctLimitNode> distinctLimit()
    {
        return typeOf(DistinctLimitNode.class);
    }

    public static Pattern<IntersectNode> intersect()
    {
        return typeOf(IntersectNode.class);
    }

    public static Pattern<ExceptNode> except()
    {
        return typeOf(ExceptNode.class);
    }

    public static Property<PlanNode, Lookup, PlanNode> source()
    {
        return optionalProperty(
                "source",
                (node, lookup) -> {
                    if (node.getSources().size() == 1) {
                        PlanNode source = getOnlyElement(node.getSources());
                        return Optional.of(lookup.resolve(source));
                    }
                    return Optional.empty();
                });
    }

    public static Property<PlanNode, Lookup, List<PlanNode>> sources()
    {
        return property(
                "sources",
                (PlanNode node, Lookup lookup) -> node.getSources().stream()
                        .map(lookup::resolve)
                        .collect(toImmutableList()));
    }

    public static final class Aggregation
    {
        public static Property<AggregationNode, Lookup, List<Symbol>> groupingColumns()
        {
            return property("groupingKeys", AggregationNode::getGroupingKeys);
        }

        public static Property<AggregationNode, Lookup, AggregationNode.Step> step()
        {
            return property("step", AggregationNode::getStep);
        }
    }

    public static final class Apply
    {
        public static Property<ApplyNode, Lookup, List<Symbol>> correlation()
        {
            return property("correlation", ApplyNode::getCorrelation);
        }
    }

    public static final class DistinctLimit
    {
        public static Property<DistinctLimitNode, Lookup, Boolean> isPartial()
        {
            return property("isPartial", DistinctLimitNode::isPartial);
        }
    }

    public static final class Exchange
    {
        public static Property<ExchangeNode, Lookup, ExchangeNode.Scope> scope()
        {
            return property("scope", ExchangeNode::getScope);
        }
    }

    public static final class Join
    {
        public static Property<JoinNode, Lookup, JoinNode.Type> type()
        {
            return property("type", JoinNode::getType);
        }

        public static Property<JoinNode, Lookup, PlanNode> left()
        {
            return property("left", (JoinNode joinNode, Lookup lookup) -> lookup.resolve(joinNode.getLeft()));
        }

        public static Property<JoinNode, Lookup, PlanNode> right()
        {
            return property("right", (JoinNode joinNode, Lookup lookup) -> lookup.resolve(joinNode.getRight()));
        }
    }

    public static final class CorrelatedJoin
    {
        public static Property<CorrelatedJoinNode, Lookup, List<Symbol>> correlation()
        {
            return property("correlation", CorrelatedJoinNode::getCorrelation);
        }

        public static Property<CorrelatedJoinNode, Lookup, PlanNode> subquery()
        {
            return property("subquery", (node, context) -> context.resolve(node.getSubquery()));
        }

        public static Property<CorrelatedJoinNode, Lookup, Expression> filter()
        {
            return property("filter", CorrelatedJoinNode::getFilter);
        }

        public static Property<CorrelatedJoinNode, Lookup, Type> type()
        {
            return property("type", CorrelatedJoinNode::getType);
        }
    }

    public static final class Limit
    {
        public static Property<LimitNode, Lookup, Long> count()
        {
            return property("count", LimitNode::getCount);
        }

        public static Property<LimitNode, Lookup, Boolean> requiresPreSortedInputs()
        {
            return property("requiresPreSortedInputs", LimitNode::requiresPreSortedInputs);
        }
    }

    public static final class Sample
    {
        public static Property<SampleNode, Lookup, Double> sampleRatio()
        {
            return property("sampleRatio", SampleNode::getSampleRatio);
        }

        public static Property<SampleNode, Lookup, SampleNode.Type> sampleType()
        {
            return property("sampleType", SampleNode::getSampleType);
        }
    }

    public static final class TopN
    {
        public static Property<TopNNode, Lookup, TopNNode.Step> step()
        {
            return property("step", TopNNode::getStep);
        }

        public static Property<TopNNode, Lookup, Long> count()
        {
            return property("count", TopNNode::getCount);
        }
    }

    public static final class Values
    {
        public static Property<ValuesNode, Lookup, Optional<List<Expression>>> rows()
        {
            return property("rows", ValuesNode::getRows);
        }

        public static Property<ValuesNode, Lookup, Integer> rowCount()
        {
            return property("rowCount", ValuesNode::getRowCount);
        }
    }

    public static final class SemiJoin
    {
        public static Property<SemiJoinNode, Lookup, PlanNode> getSource()
        {
            return property(
                    "source",
                    (SemiJoinNode semiJoin, Lookup lookup) -> lookup.resolve(semiJoin.getSource()));
        }

        public static Property<SemiJoinNode, Lookup, PlanNode> getFilteringSource()
        {
            return property(
                    "filteringSource",
                    (SemiJoinNode semiJoin, Lookup lookup) -> lookup.resolve(semiJoin.getFilteringSource()));
        }
    }

    public static final class Intersect
    {
        public static Property<IntersectNode, Lookup, Boolean> distinct()
        {
            return property("distinct", IntersectNode::isDistinct);
        }
    }

    public static final class Except
    {
        public static Property<ExceptNode, Lookup, Boolean> distinct()
        {
            return property("distinct", ExceptNode::isDistinct);
        }
    }

    public static final class PatternRecognition
    {
        public static Property<PatternRecognitionNode, Lookup, RowsPerMatch> rowsPerMatch()
        {
            return property("rowsPerMatch", PatternRecognitionNode::getRowsPerMatch);
        }
    }
}
