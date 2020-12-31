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

import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.DeleteNode;

import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.delete;

public class PruneDeleteSourceColumns
        implements Rule<DeleteNode>
{
    private static final Pattern<DeleteNode> PATTERN = delete();

    @Override
    public Pattern<DeleteNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(DeleteNode deleteNode, Captures captures, Context context)
    {
        return restrictChildOutputs(context.getIdAllocator(), deleteNode, ImmutableSet.of(deleteNode.getRowId()))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
