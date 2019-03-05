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
package io.prestosql.sql.planner.plan;

import io.prestosql.matching.Pattern;
import io.prestosql.matching.Property;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.matching.Pattern.typeOf;
import static io.prestosql.matching.Property.optionalProperty;
import static io.prestosql.matching.Property.property;

public class Patterns
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

    public static Pattern<ApplyNode> applyNode()
    {
        return typeOf(ApplyNode.class);
    }

    public static Pattern<DeleteNode> delete()
    {
        return typeOf(DeleteNode.class);
    }

    public static Pattern<ExchangeNode> exchange()
    {
        return typeOf(ExchangeNode.class);
    }

    public static Pattern<EnforceSingleRowNode> enforceSingleRow()
    {
        return typeOf(EnforceSingleRowNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<IndexSourceNode> indexSource()
    {
        return typeOf(IndexSourceNode.class);
    }

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Pattern<SpatialJoinNode> spatialJoin()
    {
        return typeOf(SpatialJoinNode.class);
    }

    public static Pattern<LateralJoinNode> lateralJoin()
    {
        return typeOf(LateralJoinNode.class);
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

    public static Pattern<WindowNode> window()
    {
        return typeOf(WindowNode.class);
    }

    public static Pattern<RowNumberNode> rowNumber()
    {
        return typeOf(RowNumberNode.class);
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
                        .map(source -> lookup.resolve(source))
                        .collect(toImmutableList()));
    }

    public static class Aggregation
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

    public static class Apply
    {
        public static Property<ApplyNode, Lookup, List<Symbol>> correlation()
        {
            return property("correlation", ApplyNode::getCorrelation);
        }
    }

    public static class Exchange
    {
        public static Property<ExchangeNode, Lookup, ExchangeNode.Scope> scope()
        {
            return property("scope", ExchangeNode::getScope);
        }
    }

    public static class Join
    {
        public static Property<JoinNode, Lookup, JoinNode.Type> type()
        {
            return property("type", JoinNode::getType);
        }
    }

    public static class LateralJoin
    {
        public static Property<LateralJoinNode, Lookup, List<Symbol>> correlation()
        {
            return property("correlation", LateralJoinNode::getCorrelation);
        }

        public static Property<LateralJoinNode, Lookup, PlanNode> subquery()
        {
            return property("subquery", LateralJoinNode::getSubquery);
        }

        public static Property<LateralJoinNode, Lookup, Expression> filter()
        {
            return property("filter", LateralJoinNode::getFilter);
        }
    }

    public static class Limit
    {
        public static Property<LimitNode, Lookup, Long> count()
        {
            return property("count", LimitNode::getCount);
        }
    }

    public static class Sample
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

    public static class TopN
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

    public static class Values
    {
        public static Property<ValuesNode, Lookup, List<List<Expression>>> rows()
        {
            return property("rows", ValuesNode::getRows);
        }
    }

    public static class SemiJoin
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
}
