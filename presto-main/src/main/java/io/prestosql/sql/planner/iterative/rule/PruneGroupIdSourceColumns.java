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

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.GroupIdNode;

import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.groupId;

public class PruneGroupIdSourceColumns
        implements Rule<GroupIdNode>
{
    private static final Pattern<GroupIdNode> PATTERN = groupId();

    @Override
    public Pattern<GroupIdNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(GroupIdNode groupId, Captures captures, Context context)
    {
        return restrictChildOutputs(context.getIdAllocator(), groupId, groupId.getInputSymbols())
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
