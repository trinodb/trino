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
package io.trino.sql.planner.assertions;

import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;

public class GroupIdMatcher
        implements Matcher
{
    private final List<List<String>> groupingSets;
    private final List<String> aggregationArguments;
    private final String groupIdSymbol;

    public GroupIdMatcher(List<List<String>> groupingSets, List<String> aggregationArguments, String groupIdSymbol)
    {
        this.groupingSets = groupingSets;
        this.aggregationArguments = aggregationArguments;
        this.groupIdSymbol = groupIdSymbol;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof GroupIdNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        GroupIdNode groupIdNode = (GroupIdNode) node;
        List<List<Symbol>> actualGroupingSets = groupIdNode.getGroupingSets();
        List<Symbol> actualAggregationArguments = groupIdNode.getAggregationArguments();

        if (actualGroupingSets.size() != groupingSets.size()) {
            return NO_MATCH;
        }

        for (int i = 0; i < actualGroupingSets.size(); i++) {
            if (!AggregationMatcher.matches(groupingSets.get(i), actualGroupingSets.get(i), symbolAliases)) {
                return NO_MATCH;
            }
        }

        if (!AggregationMatcher.matches(aggregationArguments, actualAggregationArguments, symbolAliases)) {
            return NO_MATCH;
        }

        return match(groupIdSymbol, groupIdNode.getGroupIdSymbol().toSymbolReference());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groupingSets", groupingSets)
                .add("aggregationArguments", aggregationArguments)
                .add("groupIdSymbol", groupIdSymbol)
                .toString();
    }
}
