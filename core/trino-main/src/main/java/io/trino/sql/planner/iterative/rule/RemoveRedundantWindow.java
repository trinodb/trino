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
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static io.trino.sql.planner.plan.Patterns.window;

public class RemoveRedundantWindow
        implements Rule<WindowNode>
{
    private static final Pattern<WindowNode> PATTERN = window();

    @Override
    public Pattern<WindowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(WindowNode window, Captures captures, Context context)
    {
        if (isEmpty(window.getSource(), context.getLookup())) {
            return Result.ofPlanNode(new ValuesNode(window.getId(), window.getOutputSymbols(), ImmutableList.of()));
        }
        return Result.empty();
    }
}
