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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.assertions.PlanMatchPattern.Ordering;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.Util.orderingSchemeMatches;
import static java.util.Objects.requireNonNull;

public class LimitMatcher
        implements Matcher
{
    private final long limit;
    private final List<Ordering> tiesResolvers;
    private final boolean partial;
    private final List<SymbolAlias> preSortedInputs;

    public LimitMatcher(long limit, List<Ordering> tiesResolvers, boolean partial, List<SymbolAlias> preSortedInputs)
    {
        this.limit = limit;
        this.tiesResolvers = ImmutableList.copyOf(requireNonNull(tiesResolvers, "tiesResolvers is null"));
        this.partial = partial;
        this.preSortedInputs = ImmutableList.copyOf(requireNonNull(preSortedInputs, "requiresPreSortedInputs is null"));
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof LimitNode limitNode)) {
            return false;
        }

        return limitNode.getCount() == limit
                && limitNode.isWithTies() == !tiesResolvers.isEmpty()
                && limitNode.isPartial() == partial
                && limitNode.requiresPreSortedInputs() == !preSortedInputs.isEmpty();
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node));
        LimitNode limitNode = (LimitNode) node;

        if (!limitNode.isWithTies()) {
            return match();
        }
        OrderingScheme tiesResolvingScheme = limitNode.getTiesResolvingScheme().get();
        if (orderingSchemeMatches(tiesResolvers, tiesResolvingScheme, symbolAliases)) {
            return match();
        }

        if (!limitNode.requiresPreSortedInputs()) {
            return match();
        }
        if (preSortedInputs.stream()
                .map(alias -> alias.toSymbol(symbolAliases))
                .collect(toImmutableSet())
                .equals(ImmutableSet.copyOf(limitNode.getPreSortedInputs()))) {
            return match();
        }
        return NO_MATCH;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("limit", limit)
                .add("tiesResolvers", tiesResolvers)
                .add("partial", partial)
                .add("requiresPreSortedInputs", preSortedInputs)
                .toString();
    }
}
