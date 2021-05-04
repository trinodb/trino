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
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.ValuesNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static io.trino.sql.planner.plan.Patterns.intersect;

/**
 * Converts an Intersect node with at least one empty branch to an empty Values node.
 */
public class EvaluateEmptyIntersect
        implements Rule<IntersectNode>
{
    private static final Pattern<IntersectNode> PATTERN = intersect();

    @Override
    public Pattern<IntersectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(IntersectNode node, Captures captures, Context context)
    {
        boolean hasEmptyBranches = node.getSources().stream().anyMatch(source -> isEmpty(source, context.getLookup()));

        if (hasEmptyBranches) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        return Result.empty();
    }
}
