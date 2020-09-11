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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.ValuesNode;

import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.prestosql.sql.planner.plan.Patterns.limit;

/**
 * Remove Limit node when the subplan is guaranteed to produce fewer rows than the limit and
 * replace the plan with empty values if the limit count is 0.
 */
public class RemoveRedundantLimit
        implements Rule<LimitNode>
{
    // Applies to both LimitNode with ties and LimitNode without ties.
    private static final Pattern<LimitNode> PATTERN = limit();

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode limit, Captures captures, Context context)
    {
        if (limit.getCount() == 0) {
            return Result.ofPlanNode(new ValuesNode(limit.getId(), limit.getOutputSymbols(), ImmutableList.of()));
        }
        if (isAtMost(limit.getSource(), context.getLookup(), limit.getCount())) {
            return Result.ofPlanNode(limit.getSource());
        }
        return Result.empty();
    }
}
