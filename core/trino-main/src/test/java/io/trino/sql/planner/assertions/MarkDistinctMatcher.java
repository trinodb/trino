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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static java.util.Objects.requireNonNull;

public class MarkDistinctMatcher
        implements Matcher
{
    private final PlanTestSymbol markerSymbol;
    private final List<PlanTestSymbol> distinctSymbols;

    public MarkDistinctMatcher(PlanTestSymbol markerSymbol, List<PlanTestSymbol> distinctSymbols)
    {
        this.markerSymbol = requireNonNull(markerSymbol, "markerSymbol is null");
        this.distinctSymbols = ImmutableList.copyOf(distinctSymbols);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof MarkDistinctNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        MarkDistinctNode markDistinctNode = (MarkDistinctNode) node;

        if (!ImmutableSet.copyOf(markDistinctNode.getDistinctSymbols())
                .equals(distinctSymbols.stream().map(alias -> alias.toSymbol(symbolAliases)).collect(toImmutableSet()))) {
            return NO_MATCH;
        }

        return match(markerSymbol.toString(), markDistinctNode.getMarkerSymbol().toSymbolReference());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("markerSymbol", markerSymbol)
                .add("distinctSymbols", distinctSymbols)
                .toString();
    }
}
