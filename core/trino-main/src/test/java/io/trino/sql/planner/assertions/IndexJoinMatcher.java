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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static java.util.Objects.requireNonNull;

final class IndexJoinMatcher
        implements Matcher
{
    private final IndexJoinNode.Type type;
    private final List<ExpectedValueProvider<IndexJoinNode.EquiJoinClause>> criteria;
    private final Optional<PlanTestSymbol> probeHashSymbol;
    private final Optional<PlanTestSymbol> indexHashSymbol;

    IndexJoinMatcher(
            IndexJoinNode.Type type,
            List<ExpectedValueProvider<IndexJoinNode.EquiJoinClause>> criteria,
            Optional<PlanTestSymbol> probeHashSymbol,
            Optional<PlanTestSymbol> indexHashSymbol)
    {
        this.type = requireNonNull(type, "type is null");
        this.criteria = requireNonNull(criteria, "criteria is null");
        this.probeHashSymbol = requireNonNull(probeHashSymbol, "probeHashSymbol is null");
        this.indexHashSymbol = requireNonNull(indexHashSymbol, "indexHashSymbol is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof IndexJoinNode indexJoinNode)) {
            return false;
        }

        return indexJoinNode.getType() == type;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        IndexJoinNode indexJoinNode = (IndexJoinNode) node;

        if (indexJoinNode.getCriteria().size() != criteria.size()) {
            return NO_MATCH;
        }
        Set<IndexJoinNode.EquiJoinClause> actualCriteria = ImmutableSet.copyOf(indexJoinNode.getCriteria());
        Set<IndexJoinNode.EquiJoinClause> expectedCriteria = criteria.stream()
                .map(equiClause -> equiClause.getExpectedValue(symbolAliases))
                .collect(toImmutableSet());
        if (!expectedCriteria.equals(actualCriteria)) {
            return NO_MATCH;
        }

        if (!indexJoinNode.getProbeHashSymbol().equals(probeHashSymbol.map(alias -> alias.toSymbol(symbolAliases)))) {
            return NO_MATCH;
        }

        if (!indexJoinNode.getIndexHashSymbol().equals(indexHashSymbol.map(alias -> alias.toSymbol(symbolAliases)))) {
            return NO_MATCH;
        }

        return MatchResult.match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("type", type)
                .add("criteria", criteria)
                .add("probeHashSymbol", probeHashSymbol.orElse(null))
                .add("indexHashSymbol", indexHashSymbol.orElse(null))
                .toString();
    }
}
