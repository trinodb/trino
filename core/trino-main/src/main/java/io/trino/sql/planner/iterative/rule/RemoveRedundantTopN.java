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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.Cardinality;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.trino.sql.planner.plan.Patterns.topN;

/**
 * Replace TopN node
 * 1. With a Sort node when the subplan is guaranteed to produce fewer rows than N
 * 2. With its source when the subplan produces only one row
 * 3. With a empty ValuesNode when N is 0
 */
public class RemoveRedundantTopN
        implements Rule<TopNNode>
{
    private static final Pattern<TopNNode> PATTERN = topN();

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode node, Captures captures, Context context)
    {
        if (node.getCount() == 0) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }
        Cardinality sourceCardinality = extractCardinality(node.getSource(), context.getLookup());
        if (sourceCardinality.isScalar()) {
            return Result.ofPlanNode(node.getSource());
        }
        if (sourceCardinality.isAtMost(node.getCount())) {
            return Result.ofPlanNode(new SortNode(context.getIdAllocator().getNextId(), node.getSource(), node.getOrderingScheme(), false));
        }
        return Result.empty();
    }
}
