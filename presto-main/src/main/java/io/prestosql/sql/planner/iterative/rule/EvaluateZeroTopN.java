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
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.ValuesNode;

import static io.prestosql.sql.planner.plan.Patterns.TopN.count;
import static io.prestosql.sql.planner.plan.Patterns.topN;

public class EvaluateZeroTopN
        implements Rule<TopNNode>
{
    private static final Pattern<TopNNode> PATTERN = topN()
            .with(count().equalTo(0L));

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode topN, Captures captures, Context context)
    {
        return Result.ofPlanNode(new ValuesNode(topN.getId(), topN.getOutputSymbols(), ImmutableList.of()));
    }
}
