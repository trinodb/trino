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
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;

import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.prestosql.sql.planner.plan.Patterns.enforceSingleRow;

public class RemoveRedundantEnforceSingleRowNode
        implements Rule<EnforceSingleRowNode>
{
    private static final Pattern<EnforceSingleRowNode> PATTERN = enforceSingleRow();

    @Override
    public Pattern<EnforceSingleRowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(EnforceSingleRowNode node, Captures captures, Context context)
    {
        if (isScalar(node.getSource(), context.getLookup())) {
            return Result.ofPlanNode(node.getSource());
        }
        return Result.empty();
    }
}
