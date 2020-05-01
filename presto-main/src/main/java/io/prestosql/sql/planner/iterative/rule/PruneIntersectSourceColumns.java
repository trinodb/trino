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
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.IntersectNode;

import java.util.Set;

import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.intersect;

public class PruneIntersectSourceColumns
        implements Rule<IntersectNode>
{
    @Override
    public Pattern<IntersectNode> getPattern()
    {
        return intersect();
    }

    @Override
    public Result apply(IntersectNode node, Captures captures, Context context)
    {
        @SuppressWarnings("unchecked")
        Set<Symbol>[] referencedInputs = new Set[node.getSources().size()];
        for (int i = 0; i < node.getSources().size(); i++) {
            referencedInputs[i] = ImmutableSet.copyOf(node.sourceOutputLayout(i));
        }
        return restrictChildOutputs(context.getIdAllocator(), node, referencedInputs)
                .map(Rule.Result::ofPlanNode)
                .orElse(Rule.Result.empty());
    }
}
