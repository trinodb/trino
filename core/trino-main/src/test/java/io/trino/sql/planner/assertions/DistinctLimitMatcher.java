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
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static java.util.Objects.requireNonNull;

public class DistinctLimitMatcher
        implements Matcher
{
    private final long limit;
    private final List<PlanTestSymbol> distinctSymbols;
    private final Optional<PlanTestSymbol> hashSymbol;

    public DistinctLimitMatcher(long limit, List<PlanTestSymbol> distinctSymbols, Optional<PlanTestSymbol> hashSymbol)
    {
        this.limit = limit;
        this.distinctSymbols = ImmutableList.copyOf(requireNonNull(distinctSymbols, "distinctSymbols is null"));
        this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof DistinctLimitNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        DistinctLimitNode distinctLimitNode = (DistinctLimitNode) node;

        if (distinctLimitNode.getLimit() != limit) {
            return NO_MATCH;
        }

        if (!distinctLimitNode.getHashSymbol().equals(hashSymbol.map(alias -> alias.toSymbol(symbolAliases)))) {
            return NO_MATCH;
        }

        return new MatchResult(ImmutableSet.copyOf(distinctLimitNode.getDistinctSymbols())
                .equals(distinctSymbols.stream().map(alias -> alias.toSymbol(symbolAliases)).collect(toImmutableSet())));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("limit", limit)
                .add("distinctSymbols", distinctSymbols)
                .add("hashSymbol", hashSymbol)
                .toString();
    }
}
