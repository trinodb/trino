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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.ValuesNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.plan.Patterns.offset;

/**
 * Remove Offset node and its subplan when the subplan is guaranteed to produce no more rows than the offset
 * and replace the plan with empty values.
 * Remove Offset node from the plan if the offset is 0.
 */
public class RemoveRedundantOffset
        implements Rule<OffsetNode>
{
    private static final Pattern<OffsetNode> PATTERN = offset();

    @Override
    public Pattern<OffsetNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(OffsetNode offset, Captures captures, Context context)
    {
        if (isAtMost(offset.getSource(), context.getLookup(), offset.getCount())) {
            return Result.ofPlanNode(new ValuesNode(offset.getId(), offset.getOutputSymbols()));
        }
        if (offset.getCount() == 0) {
            return Result.ofPlanNode(offset.getSource());
        }
        return Result.empty();
    }
}
