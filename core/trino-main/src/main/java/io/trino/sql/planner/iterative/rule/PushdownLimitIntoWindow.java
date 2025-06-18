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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.WindowNode;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isOptimizeTopNRanking;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.Util.toTopNRankingType;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.window;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static java.lang.Math.toIntExact;

public class PushdownLimitIntoWindow
        implements Rule<LimitNode>
{
    private static final Capture<WindowNode> childCapture = newCapture();
    private final Pattern<LimitNode> pattern;

    public PushdownLimitIntoWindow()
    {
        this.pattern = limit()
                .matching(limit -> !limit.isWithTies() &&
                        limit.getCount() != 0 && limit.getCount() <= Integer.MAX_VALUE &&
                        !limit.requiresPreSortedInputs())
                .with(source().matching(window()
                        .matching(window -> window.getOrderingScheme().isPresent())
                        .matching(window -> toTopNRankingType(window).isPresent())
                        .capturedAs(childCapture)));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeTopNRanking(session);
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(LimitNode node, Captures captures, Context context)
    {
        WindowNode source = captures.get(childCapture);

        Optional<RankingType> rankingType = toTopNRankingType(source);

        int limit = toIntExact(node.getCount());
        TopNRankingNode topNRowNumberNode = new TopNRankingNode(
                source.getId(),
                source.getSource(),
                source.getSpecification(),
                rankingType.get(),
                getOnlyElement(source.getWindowFunctions().keySet()),
                limit,
                false);
        if (rankingType.get() == ROW_NUMBER && source.getPartitionBy().isEmpty()) {
            return Result.ofPlanNode(topNRowNumberNode);
        }
        return Result.ofPlanNode(replaceChildren(node, ImmutableList.of(topNRowNumberNode)));
    }
}
