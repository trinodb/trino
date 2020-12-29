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
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.SetOperationNode;

import java.util.Optional;

import static io.trino.sql.planner.plan.Patterns.except;

public class MergeExcept
        implements Rule<ExceptNode>
{
    private static final Pattern<ExceptNode> PATTERN = except();

    @Override
    public Pattern<ExceptNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Rule.Result apply(ExceptNode node, Captures captures, Rule.Context context)
    {
        SetOperationMerge mergeOperation = new SetOperationMerge(node, context);
        Optional<SetOperationNode> result = mergeOperation.mergeFirstSource();
        return result.map(Result::ofPlanNode).orElseGet(Result::empty);
    }
}
