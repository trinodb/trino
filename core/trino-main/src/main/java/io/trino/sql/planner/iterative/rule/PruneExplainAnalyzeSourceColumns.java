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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;

import static io.trino.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.trino.sql.planner.plan.Patterns.explainAnalyze;

public class PruneExplainAnalyzeSourceColumns
        implements Rule<ExplainAnalyzeNode>
{
    private static final Pattern<ExplainAnalyzeNode> PATTERN = explainAnalyze();

    @Override
    public Pattern<ExplainAnalyzeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ExplainAnalyzeNode explainAnalyzeNode, Captures captures, Context context)
    {
        return restrictChildOutputs(
                context.getIdAllocator(),
                explainAnalyzeNode,
                ImmutableSet.copyOf(explainAnalyzeNode.getActualOutputs()))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
